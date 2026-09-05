// A simple producer: sends messages to one topic, one Produce request per
// send. Messages with a key are routed by key hash; messages without a key
// are spread over the partitions round-robin. Transport failures and
// leadership changes trigger recovery — reconnect through the bootstrap
// set, refresh metadata, and retry with backoff up to `retries` times.

///|
pub struct Producer {
  topic : String
  acks : Int
  timeout_ms : Int
  retries : Int
  retry_backoff_ms : Int
  retry_backoff_max_ms : Int
  bootstrap : BootstrapServers
  sasl : SaslConfig?
  use_tls : Bool
  tls_options : TlsClientOptions?
  mut meta_conn : BrokerConnection
  mut brokers : Map[Int, BrokerInfo]
  mut topic_id : Uuid
  leader_conns : Map[Int, BrokerConnection]
  mut partitions : Array[PartitionInfo]
  mut round_robin : Int
  mut closed : Bool
}

///|
/// Connect to a bootstrap broker, negotiate API versions, resolve the
/// topic's partitions and their leaders, and open a connection per leader.
/// `acks` is 1 (leader acknowledgement) or -1 (all in-sync replicas);
/// acks=0 is not supported because this client always reads the response.
pub async fn Producer::connect(
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
  Producer::connect_with_config(config)
}

///|
/// Connect using explicit configuration, which is validated first. The
/// meta connection lands on the first bootstrap server that accepts.
pub async fn Producer::connect_with_config(config : ProducerConfig) -> Producer {
  let bootstrap = BootstrapServers::new(config.common.bootstrap_addresses())
  // SASL credentials only apply on SASL security protocols.
  let sasl = match config.common.security_protocol {
    SaslPlaintext | SaslSsl => config.common.sasl
    _ => None
  }
  let use_tls = match config.common.security_protocol {
    Ssl | SaslSsl => true
    _ => false
  }
  let meta_conn = connect_bootstrap(
    bootstrap,
    config.common.client_id,
    timeout_ms=config.common.request_timeout_ms,
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
    bootstrap,
    sasl,
    use_tls,
    tls_options: config.common.tls,
    meta_conn,
    brokers: Map([]),
    topic_id: Uuid::zero(),
    leader_conns: Map([]),
    partitions: [],
    round_robin: 0,
    closed: false,
  }
  producer.refresh_metadata()
  producer
}

///|
pub fn Producer::close(self : Producer) -> Unit {
  if !self.closed {
    self.closed = true
    self.drop_connections()
  }
}

///|
fn Producer::drop_connections(self : Producer) -> Unit {
  self.meta_conn.close()
  for _, conn in self.leader_conns {
    conn.close()
  }
  self.leader_conns.clear()
}

///|
/// Rebuild every connection after a transport failure: dial a bootstrap
/// server (rotating), then refresh metadata and leader connections.
async fn Producer::recover(self : Producer) -> Unit {
  self.drop_connections()
  self.meta_conn = connect_bootstrap(
    self.bootstrap,
    self.meta_conn.client_id,
    timeout_ms=self.timeout_ms,
    use_tls=self.use_tls,
    tls=self.tls_options,
  )
  match self.sasl {
    Some(sasl) => self.meta_conn.authenticate(sasl, timeout_ms=self.timeout_ms)
    None => ()
  }
  self.refresh_metadata()
}

///|
async fn Producer::refresh_metadata(self : Producer) -> Unit {
  let metadata = self.meta_conn.fetch_metadata(
    self.topic,
    timeout_ms=self.timeout_ms,
  )
  self.brokers = metadata.brokers
  let topic_meta = match metadata.topics.get(0) {
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
  // Open a connection per distinct partition leader.
  for _, conn in self.leader_conns {
    conn.close()
  }
  self.leader_conns.clear()
  for p in self.partitions {
    if !self.leader_conns.contains(p.leader) {
      match self.brokers.get(p.leader) {
        Some(broker) => {
          let tls_opts = match self.tls_options {
            Some(opts) => Some(opts)
            None =>
              if self.use_tls {
                Some(TlsClientOptions::new(broker.host))
              } else {
                None
              }
          }
          let conn = BrokerConnection::connect(
            broker.host,
            broker.port,
            client_id=self.meta_conn.client_id,
            timeout_ms=self.timeout_ms,
            tls=tls_opts,
          )
          match self.sasl {
            Some(sasl) => conn.authenticate(sasl, timeout_ms=self.timeout_ms)
            None => ()
          }
          conn.negotiate_api_versions(timeout_ms=self.timeout_ms)
          self.leader_conns[p.leader] = conn
        }
        None =>
          raise ProtocolError::ProtocolError(
            "no broker info for leader node \{p.leader}",
          )
      }
    }
  }
}

///|
/// Route a message to a partition: @internal.murmur2 hash of the key when present
/// (Kafka-compatible — same placement as the Java client and librdkafka's
/// default partitioner), round-robin otherwise. @internal.murmur2 is masked to 31 bits
/// before the modulo, like Utils.toPositive in the Java client.
fn Producer::pick_partition(self : Producer, key : Bytes?) -> PartitionInfo {
  let n = self.partitions.length()
  let index = match key {
    Some(k) => (@internal.murmur2(k) & 0x7fffffff) % n
    None => {
      let i = self.round_robin % n
      self.round_robin += 1
      i
    }
  }
  self.partitions[index]
}

///|
/// Send one message and return the offset assigned by the broker.
/// `timestamp` defaults to the current time (ms since epoch).
/// Leadership changes and transport failures trigger recovery and a
/// backoff-paced retry, up to `retries` attempts after the first.
pub async fn Producer::send(
  self : Producer,
  key? : Bytes,
  value? : Bytes,
  timestamp? : Int64,
) -> Int64 {
  let timestamp = timestamp.unwrap_or(@async.now())
  let batch = encode_record_batch([
    { offset: 0L, timestamp, key, value, headers: [], },
  ])
  let backoff = Backoff::new(
    base_ms=self.retry_backoff_ms,
    max_ms=self.retry_backoff_max_ms,
  )
  // Cap the materialized attempt count: huge `retries` values (Java-style
  // unbounded) must not overflow the loop bound.
  let attempts = if self.retries > 1000 { 1000 } else { self.retries + 1 }
  for attempt in 0..<attempts {
    let _ = attempt
    let p = self.pick_partition(key)
    match self.attempt_send(p, batch) {
      Some(offset) => return offset
      None => {
        self.recover()
        if attempt + 1 < attempts {
          @async.sleep(backoff.next_ms())
        }
      }
    }
  }
  raise ProtocolError::ProtocolError(
    "send to topic \{self.topic} failed after \{attempts} attempts",
  )
}

///|
/// One produce attempt. Returns None when the failure is recoverable —
/// a dead connection or a leadership change — after recovery has been
/// triggered; permanent broker errors raise.
async fn Producer::attempt_send(
  self : Producer,
  p : PartitionInfo,
  batch : Bytes,
) -> Int64? {
  guard self.leader_conns.get(p.leader) is Some(conn) else { return None }
  let results = conn.produce(
    self.topic,
    topic_id=self.topic_id,
    partitions=[(p.index, batch)],
    acks=self.acks,
    timeout_ms=self.timeout_ms,
  ) catch {
    e =>
      match e {
        TransportError::ConnectionClosed(_) => return None
        TransportError::RequestTimeout(_) => return None
        _ => raise e
      }
  }
  guard results.get(0) is Some(result) else {
    raise ProtocolError::ProtocolError("Produce response has no partitions")
  }
  match result.error_code {
    0 => Some(result.base_offset)
    // LEADER_NOT_AVAILABLE / NOT_LEADER_OR_FOLLOWER / REBOOTSTRAP_REQUIRED:
    // all recover by re-dialing the bootstrap set and refreshing metadata.
    5 | 6 | 129 => None
    code =>
      raise ProtocolError::ProtocolError(
        "Produce failed for partition \{result.partition}: \{error_name(code)}\{produce_error_detail(result)}",
      )
  }
}

///|
/// The broker's own explanation for a dropped batch: the summary message
/// and the offending record indices (KIP-467), when it reported any.
fn produce_error_detail(result : ProducePartitionResult) -> String {
  let detail = match result.error_message {
    Some(msg) => ": \{msg}"
    None => ""
  }
  if result.record_errors.is_empty() {
    return detail
  }
  let indices = result.record_errors
    .map(fn(err) { err.batch_index.to_string() })
    .join(",")
  "\{detail} (records [\{indices}])"
}
