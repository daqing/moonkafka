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

///|
/// Send one message and return the offset assigned by the broker.
/// `timestamp` defaults to the current time (ms since epoch). `partition`
/// overrides automatic routing when given and must name an existing
/// partition; `headers` ride along with the record. The record joins the
/// partition's open batch (batch_size, linger_ms) and the sender task
/// puts it on the wire, so concurrent senders coalesce into shared
/// Produce requests and each takes the batch's base offset plus its
/// index. Retries run in the sender, bounded by delivery_timeout_ms.
/// With acks=0 nothing is acknowledged and send returns -1 once the
/// request is written. buffer_memory exhaustion raises BufferExhausted.
pub async fn Producer::send(
  self : Producer,
  key? : Bytes,
  value? : Bytes,
  timestamp? : Int64,
  partition? : Int,
  headers? : Array[(Bytes, Bytes)] = [],
) -> Int64 {
  if self.closed {
    raise ProtocolError::ProtocolError("producer is closed")
  }
  // Opportunistic metadata refresh once the cached snapshot aged out.
  self.cluster.refresh_if_stale()
  let timestamp = timestamp.unwrap_or(@async.now())
  // The batch's encoded bytes belong to one partition, so the pick
  // happens once, before accumulating; retries keep it.
  let p = self.pick_partition(key, partition)
  let n = self.partitions.length()
  let (batch, index, rotated) = self.accumulator.append(
    @async.now(),
    p.index,
    timestamp,
    key,
    value,
    headers,
  )
  if rotated {
    self.router.on_batch_closed(n)
  }
  // The sender task drains the batch and publishes the result.
  self.await_batch(batch, index)
}

///|
/// Wait for the sender task to publish the batch's result and derive
/// this record's offset from the batch's base offset. Every coalesced
/// sender registers its own wakeup under the accumulator lock, so
/// releases racing the registration cannot strand anyone.
async fn Producer::await_batch(
  self : Producer,
  batch : ProducerBatch,
  index : Int,
) -> Int64 {
  let my_done = @async.Semaphore(1, initial_value=0)
  let already_done = self.accumulator.register_waiter(batch, my_done)
  if !already_done {
    my_done.acquire()
  }
  match batch.result {
    Some(BatchOk(base)) => base + index.to_int64()
    Some(BatchError(msg)) => raise ProtocolError::ProtocolError(msg)
    None =>
      raise ProtocolError::ProtocolError(
        "batch finished without a result (moonkafka bug)",
      )
  }
}
