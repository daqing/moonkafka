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
  retries : Int
  retry_backoff_ms : Int
  retry_backoff_max_ms : Int
  /// The user's task group; hosts the autocommit loop, which the group
  /// joins on exit (close lets the final commit land first).
  group : @async.TaskGroup[Unit]
  /// Set when the consumer can commit offsets; the group coordinator
  /// node id is cached next to it until NOT_COORDINATOR.
  priv group_id : String?
  priv mut group_coordinator : Int?
  enable_auto_commit : Bool
  auto_commit_interval_ms : Int
  /// Last known committed offset per partition (-1 = none), refreshed
  /// by commit() and committed().
  priv committed_offsets : Map[Int, Int64]
  priv cluster : ClusterClient
  mut topic_id : Uuid
  /// Fetch session per leader, driving incremental fetches.
  fetch_sessions : Map[Int, FetchSession]
  partitions : Array[PartitionState]
  mut closed : Bool
}

///|
/// Connect to a bootstrap broker, negotiate API versions, resolve the
/// topic's partitions and their leaders, and initialize fetch offsets.
/// The autocommit loop (when configured) joins `group`.
pub async fn Consumer::connect(
  group~ : @async.TaskGroup[Unit],
  host~ : String,
  port~ : Int,
  topic~ : String,
  start_from? : StartFrom = Earliest,
) -> Consumer {
  let config = ConsumerConfig::new(["\{host}:\{port}"], topic, start_from~)
  Consumer::connect_with_config(group~, config)
}

///|
/// Connect using explicit configuration, which is validated first. The
/// meta connection lands on the first bootstrap server that accepts.
pub async fn Consumer::connect_with_config(
  group~ : @async.TaskGroup[Unit],
  config : ConsumerConfig,
) -> Consumer {
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
  let consumer = {
    topic: config.topic,
    start_from: config.start_from,
    request_timeout_ms: config.common.request_timeout_ms,
    retries: config.common.retries,
    retry_backoff_ms: config.common.retry_backoff_ms,
    retry_backoff_max_ms: config.common.retry_backoff_max_ms,
    group,
    group_id: config.group_id,
    group_coordinator: None,
    enable_auto_commit: config.enable_auto_commit,
    auto_commit_interval_ms: config.auto_commit_interval_ms,
    committed_offsets: Map([]),
    cluster,
    topic_id: Uuid::zero(),
    fetch_sessions: Map([]),
    partitions: [],
    closed: false,
  }
  consumer.refresh_metadata()
  consumer.reset_offsets()
  if consumer.autocommit_enabled() {
    // The autocommit loop tears the cluster down on exit, so close()
    // only flips the flag and the group joins after the final commit.
    group.spawn_bg(no_wait=false, allow_failure=false, () => {
      consumer.autocommit_loop()
    })
  }
  consumer
}

///|
/// Stop the consumer: the autocommit loop (if any) commits the current
/// positions once more, then tears the cluster connections down before
/// the group joins it.
pub fn Consumer::close(self : Consumer) -> Unit {
  if !self.closed {
    self.closed = true
    if !self.autocommit_enabled() {
      self.cluster.close()
    }
  }
}

///|
fn Consumer::autocommit_enabled(self : Consumer) -> Bool {
  self.group_id is Some(_) && self.enable_auto_commit
}

///|
/// Recover after a transport failure: re-dial the bootstrap set and
/// refresh metadata through the cluster client. Read positions survive
/// the refresh.
async fn Consumer::recover(self : Consumer) -> Unit {
  self.cluster.reconnect()
  self.refresh_metadata()
}

///|
async fn Consumer::refresh_metadata(self : Consumer) -> Unit {
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
  // A refresh after a reconnect must keep read positions; fresh connects
  // start from zero and are overwritten by reset_offsets anyway.
  self.topic_id = topic_meta.topic_id
  let old : Map[Int, Int64] = Map([])
  for p in self.partitions {
    old[p.info.index] = p.next_offset
  }
  self.partitions.clear()
  for p in topic_meta.partitions {
    let next_offset = old.get(p.index).unwrap_or(0L)
    self.partitions.push({ info: p, next_offset, })
  }
  // Leader connections come from the cluster client's pool on demand;
  // fetch sessions die with every refresh.
  self.fetch_sessions.clear()
}

///|
/// Resolve the start offset for every partition (earliest or latest).
async fn Consumer::reset_offsets(self : Consumer) -> Unit {
  let timestamp = match self.start_from {
    Earliest => OFFSET_EARLIEST
    Latest => OFFSET_LATEST
  }
  let infos = self.partitions.map(fn(p) { p.info })
  let offsets = self.cluster
    .control_conn()
    .list_offsets(
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
///
/// Fetches run through per-leader incremental fetch sessions: the first
/// request carries every partition, later ones only partitions whose
/// position changed since the broker last heard (session eviction and
/// INVALID_FETCH_SESSION_EPOCH restart with a full request).
pub async fn Consumer::poll(
  self : Consumer,
  max_wait_ms? : Int = 500,
  max_bytes? : Int = 1048576,
) -> Array[Record] {
  // Group partitions by leader so each leader connection gets one fetch.
  let by_leader : Map[Int, Map[Int, (Int64, Int)]] = Map([])
  for p in self.partitions {
    let wanted = by_leader.get_or_init(p.info.leader, fn() { Map([]) })
    wanted[p.info.index] = (p.next_offset, p.info.leader_epoch)
  }
  let out : Array[Record] = []
  let mut refresh = false
  for leader, wanted in by_leader {
    let conn = self.cluster.connection(leader) catch {
      _ => {
        refresh = true
        continue
      }
    }
    let session = self.fetch_sessions.get_or_init(leader, fn() {
      FetchSession::new()
    })
    let results : FetchResult? = Some(
      self.fetch_from_leader(conn, session, wanted, max_wait_ms, max_bytes),
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
    session.handle_response(results.session_id, results.top_error_code)
    for topic in results.topics {
      for result in topic.partitions {
        match result.error_code {
          0 =>
            if !result.records.is_empty() {
              for record in result.records {
                out.push(record)
              }
              // Offset-safe even when a trailing batch was truncated:
              // only complete batches were decoded.
              let last = result.records[result.records.length() - 1]
              self.set_next_offset(result.partition, last.offset + 1L)
            }
          1 => self.reset_partition_offset(result.partition) // OFFSET_OUT_OF_RANGE
          _ => refresh = true // NOT_LEADER_OR_FOLLOWER etc.
        }
      }
    }
  }
  if refresh {
    self.recover()
  }
  out
}

///|
/// One fetch exchange with a leader, retrying once when the broker
/// evicts the fetch session (FETCH_SESSION_ID_NOT_FOUND /
/// INVALID_FETCH_SESSION_EPOCH): the retry goes out as a full request.
async fn Consumer::fetch_from_leader(
  self : Consumer,
  conn : BrokerConnection,
  session : FetchSession,
  wanted : Map[Int, (Int64, Int)],
  max_wait_ms : Int,
  max_bytes : Int,
) -> FetchResult {
  let request = session.prepare(self.topic, self.topic_id, wanted, max_bytes)
  let result = self.send_fetch(conn, request, max_wait_ms, max_bytes)
  if result.top_error_code == FETCH_SESSION_ID_NOT_FOUND ||
    result.top_error_code == INVALID_FETCH_SESSION_EPOCH {
    // The broker lost the session (failover, eviction, epoch race):
    // re-establish with everything, once.
    session.invalidate()
    let retry = session.prepare(self.topic, self.topic_id, wanted, max_bytes)
    return self.send_fetch(conn, retry, max_wait_ms, max_bytes)
  }
  result
}

///|
/// Send one fetch for the single subscribed topic, mapping the session's
/// forgotten partition indexes onto the topic entry.
async fn Consumer::send_fetch(
  self : Consumer,
  conn : BrokerConnection,
  request : FetchSessionReq,
  max_wait_ms : Int,
  max_bytes : Int,
) -> FetchResult {
  conn.fetch(
    [
      {
        name: self.topic,
        topic_id: self.topic_id,
        partitions: request.partitions,
      },
    ],
    session=request,
    max_wait_ms~,
    max_bytes~,
    timeout_ms=self.request_timeout_ms,
  )
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
      let offsets = self.cluster
        .control_conn()
        .list_offsets(
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
