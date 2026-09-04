// A simple producer: sends messages to one topic, one Produce request per
// send. Messages with a key are routed by key hash; messages without a key
// are spread over the partitions round-robin.

///|
pub struct Producer {
  topic : String
  acks : Int
  timeout_ms : Int
  meta_conn : Connection
  mut brokers : Map[Int, BrokerInfo]
  leader_conns : Map[Int, Connection]
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
  if acks != 1 && acks != -1 {
    raise ProtocolError::ProtocolError(
      "acks must be 1 (leader) or -1 (all), got \{acks}",
    )
  }
  let meta_conn = Connection::connect(host, port)
  meta_conn.check_api_versions()
  let producer = {
    topic,
    acks,
    timeout_ms,
    meta_conn,
    brokers: Map([]),
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
    self.meta_conn.close()
    for _, conn in self.leader_conns {
      conn.close()
    }
  }
}

///|
async fn Producer::refresh_metadata(self : Producer) -> Unit {
  let metadata = self.meta_conn.fetch_metadata(self.topic)
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
  // Open a connection per distinct partition leader.
  for _, conn in self.leader_conns {
    conn.close()
  }
  self.leader_conns.clear()
  for p in self.partitions {
    if !self.leader_conns.contains(p.leader) {
      match self.brokers.get(p.leader) {
        Some(broker) =>
          self.leader_conns[p.leader] = Connection::connect(
            broker.host,
            broker.port,
          )
        None =>
          raise ProtocolError::ProtocolError(
            "no broker info for leader node \{p.leader}",
          )
      }
    }
  }
}

///|
/// Route a message to a partition: crc32c hash of the key when present
/// (note: not the Kafka-standard murmur2, so key placement differs from
/// other clients), round-robin otherwise.
fn Producer::pick_partition(self : Producer, key : Bytes?) -> PartitionInfo {
  let n = self.partitions.length()
  let index = match key {
    Some(k) => (crc32c(k) % n.reinterpret_as_uint()).reinterpret_as_int()
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
/// On a leadership change the producer refreshes metadata and retries once.
pub async fn Producer::send(
  self : Producer,
  key? : Bytes,
  value? : Bytes,
  timestamp? : Int64,
) -> Int64 {
  let timestamp = timestamp.unwrap_or(@async.now())
  let batch = encode_record_batch([{ offset: 0L, timestamp, key, value, }])
  for _ in 0..<2 {
    let p = self.pick_partition(key)
    guard self.leader_conns.get(p.leader) is Some(conn) else {
      self.refresh_metadata()
      continue
    }
    let results = conn.produce(
      self.topic,
      [(p.index, batch)],
      acks=self.acks,
      timeout_ms=self.timeout_ms,
    )
    guard results.get(0) is Some(result) else {
      raise ProtocolError::ProtocolError("Produce response has no partitions")
    }
    match result.error_code {
      0 => return result.base_offset
      5 | 6 => self.refresh_metadata() // LEADER_NOT_AVAILABLE / NOT_LEADER_OR_FOLLOWER
      code =>
        raise ProtocolError::ProtocolError(
          "Produce failed for partition \{result.partition}: \{error_name(code)}",
        )
    }
  }
  raise ProtocolError::ProtocolError("Produce failed after metadata refresh")
}
