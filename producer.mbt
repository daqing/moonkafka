// A simple producer: sends messages to one topic through the accumulator's
// per-partition batches, routed by the configured partitioner. Transport
// failures and leadership changes trigger recovery — reconnect through
// the bootstrap set, refresh metadata, and retry with backoff up to
// `retries` times. Cluster management (connection pool, metadata,
// reconnection) lives in ClusterClient.

///|
pub struct Producer {
  topic : String
  acks : Int
  timeout_ms : Int
  retries : Int
  retry_backoff_ms : Int
  retry_backoff_max_ms : Int
  delivery_timeout_ms : Int
  /// The user's task group; hosts the sender drain loop, which the group
  /// joins on exit (close lets it drain first).
  group : @async.TaskGroup[Unit]
  enable_idempotence : Bool
  /// Idempotence identity, assigned at connect (InitProducerId v5).
  mut producer_id : Int64
  mut producer_epoch : Int
  /// Next sequence number per partition; stamped into fresh batches,
  /// rewound on terminal failures, reset on epoch bumps.
  priv sequences : Map[Int, Int]
  priv cluster : ClusterClient
  mut topic_id : Uuid
  mut partitions : Array[PartitionInfo]
  priv router : PartitionRouter
  priv accumulator : RecordAccumulator
  mut closed : Bool
}

///|
/// Connect to a bootstrap broker, negotiate API versions, resolve the
/// topic's partitions and their leaders, and start the sender drain loop
/// in `group`. `acks` is 0 (fire-and-forget), 1 (leader) or -1 (all
/// in-sync replicas).
pub async fn Producer::connect(
  group~ : @async.TaskGroup[Unit],
  host~ : String,
  port~ : Int,
  topic~ : String,
  acks? : Int = 1,
  timeout_ms? : Int = 30000,
) -> Producer {
  let config = ProducerConfig::new(
    ["\{host}:\{port}"],
    topic,
    acks~,
    request_timeout_ms=timeout_ms,
  )
  Producer::connect_with_config(group~, config)
}

///|
/// Connect using explicit configuration, which is validated first. The
/// cluster client lands on the first bootstrap server that accepts, and
/// the sender task drains the accumulator until close.
pub async fn Producer::connect_with_config(
  group~ : @async.TaskGroup[Unit],
  config : ProducerConfig,
) -> Producer {
  // SASL credentials only apply on SASL security protocols.
  let sasl = match config.common.security_protocol {
    SaslPlaintext | SaslSsl => config.common.sasl
    _ => None
  }
  let use_tls = match config.common.security_protocol {
    Ssl | SaslSsl => true
    _ => false
  }
  let cluster = ClusterClient::connect(
    config.common.bootstrap_addresses(),
    client_id=config.common.client_id,
    request_timeout_ms=config.common.request_timeout_ms,
    metadata_max_age_ms=config.common.metadata_max_age_ms,
    max_in_flight=config.max_in_flight,
    sasl~,
    use_tls~,
    tls=config.common.tls,
  )
  let producer = {
    topic: config.topic,
    acks: config.acks,
    timeout_ms: config.common.request_timeout_ms,
    retries: config.common.retries,
    retry_backoff_ms: config.common.retry_backoff_ms,
    retry_backoff_max_ms: config.common.retry_backoff_max_ms,
    delivery_timeout_ms: config.delivery_timeout_ms,
    group,
    enable_idempotence: config.enable_idempotence,
    producer_id: -1L,
    producer_epoch: -1,
    sequences: Map([]),
    cluster,
    topic_id: Uuid::zero(),
    partitions: [],
    router: PartitionRouter::new(config.partitioner, next_sticky_seed()),
    accumulator: RecordAccumulator::new(
      config.batch_size,
      config.buffer_memory,
      config.linger_ms,
    ),
    closed: false,
  }
  producer.refresh_metadata()
  if config.enable_idempotence {
    // A plain idempotent producer may ask any broker (Java uses the
    // least-loaded node); the transactional flow goes to its coordinator.
    let id = producer.cluster
      .control_conn()
      .init_producer_id(transaction_timeout_ms=config.delivery_timeout_ms)
    producer.producer_id = id.producer_id
    producer.producer_epoch = id.producer_epoch
  }
  // The sender loop joins the group (no_wait=false): close() stops it
  // and the group waits out its final drain before tearing down.
  group.spawn_bg(no_wait=false, allow_failure=false, () => {
    producer.sender_loop()
  })
  producer
}

///|
/// Stop the producer: the sender task force-closes every open batch,
/// drains what is queued (honoring retries and delivery_timeout_ms),
/// resolves every pending send, and tears the cluster connections down
/// on exit. Sends after close raise.
pub fn Producer::close(self : Producer) -> Unit {
  if !self.closed {
    self.closed = true
  }
}

///|
/// Recover after a transport failure: re-dial the bootstrap set and
/// refresh metadata through the cluster client.
async fn Producer::recover(self : Producer) -> Unit {
  self.cluster.reconnect()
  self.refresh_metadata()
}

///|
async fn Producer::refresh_metadata(self : Producer) -> Unit {
  self.cluster.refresh_metadata(Some([self.topic]))
  let topic_meta = match self.cluster.topic(self.topic) {
    Some(t) => t
    None => raise ProtocolError::ProtocolError("topic \{self.topic} not found")
  }
  if topic_meta.partitions.is_empty() {
    raise ProtocolError::ProtocolError(
      "topic \{self.topic} has no partitions with a leader",
    )
  }
  self.partitions = topic_meta.partitions
  self.topic_id = topic_meta.topic_id
}

///|
/// Route a record to a partition: an explicit override wins; otherwise
/// keyed records follow murmur2 (Kafka-compatible — same placement as the
/// Java client and librdkafka's default partitioner, murmur2 masked to 31
/// bits before the modulo like Utils.toPositive) and unkeyed records go
/// sticky or round-robin per the configured strategy.
fn Producer::pick_partition(
  self : Producer,
  key : Bytes?,
  override_partition : Int?,
) -> PartitionInfo raise ProtocolError {
  let index = self.router.pick(
    self.partitions.length(),
    key,
    override_partition,
  )
  self.partitions[index]
}
