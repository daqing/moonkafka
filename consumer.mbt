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
  /// Paused partitions are skipped by poll until resumed.
  mut paused : Bool
}

///|
/// True when the manual assignment list contains (topic, partition).
fn manual_has(
  parts : Array[(String, Int)],
  topic : String,
  partition : Int,
) -> Bool {
  for pair in parts {
    if pair.0 == topic && pair.1 == partition {
      return true
    }
  }
  false
}

///|
/// One leader's poll outcome, gathered concurrently and applied
/// sequentially afterwards (cooperative scheduling keeps the swap safe).
priv enum PollOutcome {
  Fetched(FetchResult)
  /// Transport failure: recover and retry on the next poll.
  PollRetry
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
  auto_offset_reset : AutoOffsetReset
  max_poll_records : Int
  max_partition_fetch_bytes : Int
  enable_read_committed : Bool
  /// Last known committed offset per partition (-1 = none), refreshed
  /// by commit() and committed().
  priv committed_offsets : Map[Int, Int64]
  /// KIP-848 membership state, owned by the heartbeat loop.
  priv group_instance_id : String?
  priv mut member_id : String
  priv mut member_epoch : Int
  priv mut subscribed_names : Array[String]?
  priv mut subscribed_regex : String?
  priv mut assignment : Array[(String, Int)]
  mut heartbeat_interval_ms : Int
  session_timeout_ms : Int
  priv mut member_active : Bool
  priv mut member_spawned : Bool
  priv mut member_left : Bool
  priv mut member_error : String?
  /// Manual assignment (assign()); None while group-driven.
  priv mut manual_assignment : Array[(String, Int)]?
  mut last_poll_ms : Int64
  max_poll_interval_ms : Int
  group_protocol : GroupProtocol
  /// Classic-protocol membership state.
  priv mut generation_id : Int
  priv mut classic_assignors : Array[Assignor]
  priv mut member_kind : MemberKind
  priv mut rebalance_listener : RebalanceListener?
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
    auto_offset_reset: config.auto_offset_reset,
    max_poll_records: config.max_poll_records,
    max_partition_fetch_bytes: config.max_partition_fetch_bytes,
    enable_read_committed: config.enable_read_committed,
    committed_offsets: Map([]),
    group_instance_id: config.group_instance_id,
    member_id: "",
    member_epoch: 0,
    subscribed_names: None,
    subscribed_regex: None,
    assignment: [],
    heartbeat_interval_ms: 0,
    session_timeout_ms: config.session_timeout_ms,
    member_active: false,
    member_spawned: false,
    member_left: false,
    member_error: None,
    generation_id: -1,
    classic_assignors: [RangeAssignor],
    member_kind: Kip848Member,
    rebalance_listener: None,
    manual_assignment: None,
    last_poll_ms: 0L,
    max_poll_interval_ms: config.max_poll_interval_ms,
    group_protocol: config.group_protocol,
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
    if !self.autocommit_enabled() && !self.member_spawned {
      // A background loop (autocommit or membership) owns the teardown
      // when one is running; it closes the cluster after its last wire
      // work (final commit / graceful leave).
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
  let topic_meta = self.cluster.wait_for_topic(
    self.topic,
    timeout_ms=self.request_timeout_ms,
  )
  // A refresh after a reconnect must keep read positions; fresh connects
  // start from zero and are overwritten by reset_offsets anyway.
  self.topic_id = topic_meta.topic_id
  let old : Map[Int, (Int64, Bool)] = Map([])
  for p in self.partitions {
    old[p.info.index] = (p.next_offset, p.paused)
  }
  self.partitions.clear()
  for p in topic_meta.partitions {
    match old.get(p.index) {
      Some((next_offset, paused)) =>
        self.partitions.push({ info: p, next_offset, paused, })
      None => self.partitions.push({ info: p, next_offset: 0L, paused: false, })
    }
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
/// Poll the assigned partitions once. Returns up to `max_poll_records`
/// decoded records (possibly empty; the broker long-polls up to
/// `max_wait_ms` per fetch). Paused partitions are skipped.
///
/// Fetches run concurrently per leader through incremental fetch
/// sessions: the first request carries every partition, later ones only
/// partitions whose position changed since the broker last heard (session
/// eviction and INVALID_FETCH_SESSION_EPOCH restart with a full request).
/// Recovery: transport failures refresh metadata; OFFSET_OUT_OF_RANGE
/// follows the auto-reset policy; UNKNOWN/FENCED_LEADER_EPOCH validate
/// the position against the leader's epoch end offset (KIP-320) and
/// rewind on truncation. READ_COMMITTED consumers filter aborted
/// transactions; read positions always advance past whole batches, so
/// markers and aborted ranges are never re-fetched.
pub async fn Consumer::poll(
  self : Consumer,
  max_wait_ms? : Int = 500,
  max_bytes? : Int = 1048576,
) -> Array[Record] {
  self.enforce_poll_interval()
  // Group unpauseed partitions by leader so each leader connection gets
  // one concurrent fetch.
  let by_leader : Map[Int, Map[Int, (Int64, Int)]] = Map([])
  for p in self.partitions {
    if p.paused {
      continue
    }
    if self.member_spawned && !self.is_assigned(p.info.index) {
      // Group membership drives the fetch set: partitions the
      // coordinator has not assigned stay untouched.
      continue
    }
    match self.manual_assignment {
      Some(parts) =>
        if !manual_has(parts, self.topic, p.info.index) {
          continue
        }
      None => ()
    }
    let wanted = by_leader.get_or_init(p.info.leader, fn() { Map([]) })
    wanted[p.info.index] = (p.next_offset, p.info.leader_epoch)
  }
  // One fetch exchange per leader, all in flight at once.
  let outcomes : Array[PollOutcome] = []
  @async.with_task_group(fn(group) {
    for _, wanted in by_leader {
      group.spawn_bg(no_wait=false, allow_failure=true, () => {
        let outcome = self.poll_leader(wanted, max_wait_ms, max_bytes) catch {
          _ => PollRetry
        }
        outcomes.push(outcome)
      })
    }
  })
  // Apply the results sequentially: records, positions, error policies.
  let out : Array[Record] = []
  let mut remaining = self.max_poll_records
  let mut refresh = false
  for outcome in outcomes {
    guard outcome is Fetched(result) else {
      refresh = true
      continue
    }
    for leader_result in result.topics {
      for part in leader_result.partitions {
        match part.error_code {
          0 => {
            // Delivered records honor read_committed; the position
            // advances past the last complete batch (markers and
            // aborted ranges included), so nothing is re-fetched.
            let delivered = if self.enable_read_committed {
              collect_committed(part.batches, part.aborted_transactions)
            } else {
              part.records
            }
            if remaining <= 0 {
              // max_poll_records reached: leave the position alone so
              // the next poll re-fetches what this one didn't return.
              continue
            }
            let mut last_seen : Int64? = None
            if !part.batches.is_empty() && part.records_complete {
              let last_batch = part.batches[part.batches.length() - 1]
              last_seen = Some(last_batch.last_offset + 1L)
            } else if !part.records.is_empty() {
              // Truncated tail: stop at the last complete record.
              let last = part.records[part.records.length() - 1]
              last_seen = Some(last.offset + 1L)
            }
            let taken : Array[Record] = []
            let mut cut = false
            for record in delivered {
              if taken.length() >= remaining {
                cut = true
                break
              }
              taken.push(record)
            }
            for record in taken {
              out.push(record)
            }
            remaining -= taken.length()
            if cut {
              // The cap cut mid-delivery: the position stops at the last
              // returned record so the rest is re-fetched next poll.
              let last_taken = taken[taken.length() - 1]
              self.set_next_offset(part.partition, last_taken.offset + 1L)
              remaining = 0
            } else {
              match last_seen {
                Some(offset) => self.set_next_offset(part.partition, offset)
                None => ()
              }
            }
          }
          1 =>
            // OFFSET_OUT_OF_RANGE: follow the auto-reset policy.
            match self.auto_offset_reset {
              ResetEarliest =>
                self.reset_partition_offset(part.partition, OFFSET_EARLIEST)
              ResetLatest =>
                self.reset_partition_offset(part.partition, OFFSET_LATEST)
              ResetNone =>
                raise ProtocolError::ProtocolError(
                  "offset for partition \{part.partition} is out of range and auto offset reset is disabled",
                )
            }
          74 | 75 => {
            // FENCED/UNKNOWN_LEADER_EPOCH: leadership changed under us.
            // Validate the position against the leader's epoch end and
            // rewind on truncation; otherwise refresh metadata.
            self.validate_leader_epoch(part.partition)
            refresh = true
          }
          _ => refresh = true // NOT_LEADER_OR_FOLLOWER etc.
        }
      }
    }
  }
  if refresh {
    self.recover()
  }
  self.last_poll_ms = @async.now()
  out
}

///|
/// max_poll_interval_ms enforcement: a gap between polls past the
/// window means the application stopped making progress — leave the
/// group (if any) and surface the error instead of ghosting.
async fn Consumer::enforce_poll_interval(self : Consumer) -> Unit {
  let last = self.last_poll_ms
  if last > 0L {
    let gap = @async.now() - last
    if gap > self.max_poll_interval_ms.to_int64() {
      if self.member_active {
        self.unsubscribe()
      }
      raise ProtocolError::ProtocolError(
        "poll gap of \{gap}ms exceeded max_poll_interval_ms (\{self.max_poll_interval_ms}); the group membership was released",
      )
    }
  }
}

///|
/// One leader's fetch exchange with session-eviction retry.
async fn Consumer::poll_leader(
  self : Consumer,
  wanted : Map[Int, (Int64, Int)],
  max_wait_ms : Int,
  max_bytes : Int,
) -> PollOutcome {
  let leader = self.leader_of_any(wanted)
  let conn = self.cluster.connection(leader)
  let session = self.fetch_sessions.get_or_init(leader, fn() {
    FetchSession::new()
  })
  let result = self.fetch_from_leader(
    conn,
    session,
    wanted,
    max_wait_ms,
    max_bytes,
    self.max_partition_fetch_bytes,
  )
  session.handle_response(result.session_id, result.top_error_code)
  Fetched(result)
}

///|
/// The leader of any partition in `wanted` (single-topic consumer: all
/// entries share the topic's leader mapping).
fn Consumer::leader_of_any(
  self : Consumer,
  wanted : Map[Int, (Int64, Int)],
) -> Int raise {
  for p in self.partitions {
    if wanted.contains(p.info.index) {
      return p.info.leader
    }
  }
  raise ProtocolError::ProtocolError("no assigned partition to fetch")
}

///|
/// KIP-320 truncation check after a fenced/unknown leader epoch: ask the
/// leader where the known epoch ends; a position past it rewinds. Any
/// failure leaves recovery to the metadata refresh.
async fn Consumer::validate_leader_epoch(
  self : Consumer,
  partition : Int,
) -> Unit {
  let mut info : PartitionInfo? = None
  for p in self.partitions {
    if p.info.index == partition {
      info = Some(p.info)
    }
  }
  guard info is Some(info) else { return }
  let results = self.cluster
    .connection(info.leader)
    .offset_for_leader_epoch(
      self.topic,
      [(partition, info.leader_epoch, info.leader_epoch)],
      timeout_ms=self.request_timeout_ms,
    ) catch {
      _ => return
    }
  for result in results {
    if result.error_code == 0 &&
      result.end_offset >= 0L &&
      result.end_offset < self.position_of(partition) {
      self.set_next_offset(partition, result.end_offset)
    }
  }
}

///|
fn Consumer::position_of(self : Consumer, partition : Int) -> Int64 {
  for p in self.partitions {
    if p.info.index == partition {
      return p.next_offset
    }
  }
  0L
}

///|
/// One fetch exchange with a leader, retrying once when the broker
/// evicts the fetch session (FETCH_SESSION_ID_NOT_FOUND /
/// INVALID_FETCH_SESSION_EPOCH): the retry goes out as a full request.
/// `partition_max_bytes` caps each partition's response; `max_bytes` is
/// the request-level cap.
async fn Consumer::fetch_from_leader(
  self : Consumer,
  conn : BrokerConnection,
  session : FetchSession,
  wanted : Map[Int, (Int64, Int)],
  max_wait_ms : Int,
  max_bytes : Int,
  partition_max_bytes : Int,
) -> FetchResult {
  let request = session.prepare(
    self.topic,
    self.topic_id,
    wanted,
    partition_max_bytes,
  )
  let result = self.send_fetch(conn, request, max_wait_ms, max_bytes)
  if result.top_error_code == FETCH_SESSION_ID_NOT_FOUND ||
    result.top_error_code == INVALID_FETCH_SESSION_EPOCH {
    // The broker lost the session (failover, eviction, epoch race):
    // re-establish with everything, once.
    session.invalidate()
    let retry = session.prepare(
      self.topic,
      self.topic_id,
      wanted,
      partition_max_bytes,
    )
    return self.send_fetch(conn, retry, max_wait_ms, max_bytes)
  }
  result
}

///|
/// Send one fetch for the single subscribed topic, mapping the session's
/// forgotten partition indexes onto the topic entry. READ_COMMITTED
/// consumers set the isolation level so the broker withholds records
/// past the last stable offset and reports aborted transactions.
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
    isolation_level=if self.enable_read_committed { 1 } else { 0 },
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
/// Re-resolve one partition's position from a ListOffsets sentinel
/// (the auto-reset policy's earliest/latest).
async fn Consumer::reset_partition_offset(
  self : Consumer,
  partition : Int,
  sentinel : Int64,
) -> Unit {
  for p in self.partitions {
    if p.info.index == partition {
      let offsets = self.cluster
        .control_conn()
        .list_offsets(
          self.topic,
          [p.info],
          sentinel,
          timeout_ms=self.request_timeout_ms,
        )
      match offsets.get(partition) {
        Some(offset) => p.next_offset = offset
        None => ()
      }
    }
  }
}

///|
/// Pause fetching from the given partitions (default: all). Paused
/// partitions keep their positions; poll skips them until resume.
pub fn Consumer::pause(self : Consumer, partitions? : Array[Int]) -> Unit {
  for p in self.partitions {
    if self.pause_wanted(partitions, p.info.index) {
      p.paused = true
    }
  }
}

///|
/// Resume fetching from the given partitions (default: all).
pub fn Consumer::resume(self : Consumer, partitions? : Array[Int]) -> Unit {
  for p in self.partitions {
    if self.pause_wanted(partitions, p.info.index) {
      p.paused = false
    }
  }
}

///|
/// The partitions currently paused.
pub fn Consumer::paused(self : Consumer) -> Array[Int] {
  let out : Array[Int] = []
  for p in self.partitions {
    if p.paused {
      out.push(p.info.index)
    }
  }
  out
}

///|
fn Consumer::pause_wanted(
  self : Consumer,
  partitions : Array[Int]?,
  index : Int,
) -> Bool {
  match partitions {
    None => true
    Some(list) => {
      for i in list {
        if i == index {
          return true
        }
      }
      false
    }
  }
}
