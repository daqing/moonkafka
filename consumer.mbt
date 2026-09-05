// A simple consumer: fetches all partitions of one topic in a poll loop,
// without consumer group coordination. Offsets are tracked in memory.

///|
pub(all) enum StartFrom {
  Earliest
  Latest
} derive(@debug.Debug)

///|
struct PartitionState {
  info : PartitionInfo
  mut next_offset : Int64
}

///|
pub struct Consumer {
  topic : String
  start_from : StartFrom
  request_timeout_ms : Int
  bootstrap : BootstrapServers
  mut meta_conn : BrokerConnection
  mut brokers : Map[Int, BrokerInfo]
  leader_conns : Map[Int, BrokerConnection]
  partitions : Array[PartitionState]
  mut closed : Bool
}

///|
/// Connect to a bootstrap broker, negotiate API versions, resolve the
/// topic's partitions and their leaders, and initialize fetch offsets.
pub async fn Consumer::connect(
  host~ : String,
  port~ : Int,
  topic~ : String,
  start_from? : StartFrom = Earliest,
) -> Consumer {
  let config = ConsumerConfig::new(["\{host}:\{port}"], topic, start_from~)
  Consumer::connect_with_config(config)
}

///|
/// Connect using explicit configuration, which is validated first. The
/// meta connection lands on the first bootstrap server that accepts.
pub async fn Consumer::connect_with_config(config : ConsumerConfig) -> Consumer {
  let bootstrap = BootstrapServers::new(config.common.bootstrap_addresses())
  let meta_conn = connect_bootstrap(
    bootstrap,
    config.common.client_id,
    timeout_ms=config.common.request_timeout_ms,
  )
  meta_conn.check_api_versions(timeout_ms=config.common.request_timeout_ms)
  let consumer = {
    topic: config.topic,
    start_from: config.start_from,
    request_timeout_ms: config.common.request_timeout_ms,
    bootstrap,
    meta_conn,
    brokers: Map([]),
    leader_conns: Map([]),
    partitions: [],
    closed: false,
  }
  consumer.refresh_metadata()
  consumer.reset_offsets()
  consumer
}

///|
pub fn Consumer::close(self : Consumer) -> Unit {
  if !self.closed {
    self.closed = true
    self.meta_conn.close()
    for _, conn in self.leader_conns {
      conn.close()
    }
  }
}

///|
/// Rebuild every connection after a transport failure: dial a bootstrap
/// server (rotating), then refresh metadata and leader connections. Read
/// positions survive the refresh.
async fn Consumer::recover(self : Consumer) -> Unit {
  self.meta_conn.close()
  for _, conn in self.leader_conns {
    conn.close()
  }
  self.leader_conns.clear()
  self.meta_conn = connect_bootstrap(
    self.bootstrap,
    self.meta_conn.client_id,
    timeout_ms=self.request_timeout_ms,
  )
  self.refresh_metadata()
}

///|
async fn Consumer::refresh_metadata(self : Consumer) -> Unit {
  let metadata = self.meta_conn.fetch_metadata(
    self.topic,
    timeout_ms=self.request_timeout_ms,
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
  // A refresh after a reconnect must keep read positions; fresh connects
  // start from zero and are overwritten by reset_offsets anyway.
  let old : Map[Int, Int64] = Map([])
  for p in self.partitions {
    old[p.info.index] = p.next_offset
  }
  self.partitions.clear()
  for p in topic_meta.partitions {
    let next_offset = old.get(p.index).unwrap_or(0L)
    self.partitions.push({ info: p, next_offset, })
  }
  // Open a connection per distinct partition leader.
  for _, conn in self.leader_conns {
    conn.close()
  }
  self.leader_conns.clear()
  for p in self.partitions {
    let leader = p.info.leader
    if !self.leader_conns.contains(leader) {
      match self.brokers.get(leader) {
        Some(broker) =>
          self.leader_conns[leader] = BrokerConnection::connect(
            broker.host,
            broker.port,
            client_id=self.meta_conn.client_id,
          )
        None =>
          raise ProtocolError::ProtocolError(
            "no broker info for leader node \{leader}",
          )
      }
    }
  }
}

///|
/// Resolve the start offset for every partition (earliest or latest).
async fn Consumer::reset_offsets(self : Consumer) -> Unit {
  let timestamp = match self.start_from {
    Earliest => OFFSET_EARLIEST
    Latest => OFFSET_LATEST
  }
  let infos = self.partitions.map(fn(p) { p.info })
  let offsets = self.meta_conn.list_offsets(
    self.topic,
    infos,
    timestamp,
    timeout_ms=self.request_timeout_ms,
  )
  for p in self.partitions {
    match offsets.get(p.info.index) {
      Some(offset) => p.next_offset = offset
      None =>
        raise ProtocolError::ProtocolError(
          "ListOffsets missing partition \{p.info.index}",
        )
    }
  }
}

///|
/// Poll every partition once. Returns the decoded records (possibly empty;
/// the broker long-polls up to `max_wait_ms` per fetch).
/// On offset or leadership errors the consumer recovers by refreshing
/// metadata or re-resolving offsets on the next poll.
pub async fn Consumer::poll(
  self : Consumer,
  max_wait_ms? : Int = 500,
  max_bytes? : Int = 1048576,
) -> Array[Record] {
  // Group partitions by leader so each leader connection gets one fetch.
  let by_leader : Map[Int, Array[(PartitionInfo, Int64)]] = Map([])
  for p in self.partitions {
    let list = by_leader.get_or_init(p.info.leader, fn() { [] })
    list.push((p.info, p.next_offset))
  }
  let out : Array[Record] = []
  let mut refresh = false
  for leader, request_partitions in by_leader {
    guard self.leader_conns.get(leader) is Some(conn) else {
      refresh = true
      continue
    }
    let results : Array[FetchPartitionResult]? = Some(
      conn.fetch(
        self.topic,
        request_partitions,
        max_wait_ms~,
        max_bytes~,
        timeout_ms=self.request_timeout_ms,
      ),
    ) catch {
      e =>
        match e {
          TransportError::ConnectionClosed(_)
          | TransportError::RequestTimeout(_) => {
            refresh = true
            None
          }
          _ => raise e
        }
    }
    guard results is Some(results) else { continue }
    for result in results {
      match result.error_code {
        0 =>
          if !result.records.is_empty() {
            for record in result.records {
              out.push(record)
            }
            let last = result.records[result.records.length() - 1]
            self.set_next_offset(result.partition, last.offset + 1L)
          }
        1 => self.reset_partition_offset(result.partition) // OFFSET_OUT_OF_RANGE
        _ => refresh = true // NOT_LEADER_OR_FOLLOWER etc.
      }
    }
  }
  if refresh {
    self.recover()
  }
  out
}

///|
fn Consumer::set_next_offset(
  self : Consumer,
  partition : Int,
  offset : Int64,
) -> Unit {
  for p in self.partitions {
    if p.info.index == partition {
      p.next_offset = offset
    }
  }
}

///|
async fn Consumer::reset_partition_offset(
  self : Consumer,
  partition : Int,
) -> Unit {
  let timestamp = match self.start_from {
    Earliest => OFFSET_EARLIEST
    Latest => OFFSET_LATEST
  }
  for p in self.partitions {
    if p.info.index == partition {
      let offsets = self.meta_conn.list_offsets(
        self.topic,
        [p.info],
        timestamp,
        timeout_ms=self.request_timeout_ms,
      )
      match offsets.get(partition) {
        Some(offset) => p.next_offset = offset
        None => ()
      }
    }
  }
}
