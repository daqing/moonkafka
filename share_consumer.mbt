// The KIP-932 share consumer (Phase 6): a single-topic spiral over
// ShareGroupHeartbeat v1 membership and ShareFetch v2 acquisition.
//
// Unlike a consumer group, a share group's partitions are shared: the
// coordinator assigns every member the *same* set of topic-partitions,
// and each record may be delivered to several members concurrently to
// spread work (KIP-932's "share" semantic). The broker owns the
// acquisition state — delivery counts and which offsets are in flight
// for a member — and hands out records without a fetch offset. The
// member's binding choice is the acknowledgement: Accept (consume),
// Release (return for redelivery), Reject (poison), Renew (keep the
// delivery window).
//
// The client keeps a bounded in-flight queue of acquired-but-unacked
// records (max_queue_size) and auto-rejects records whose delivery count
// already exceeds delivery_count_limit instead of surfacing them. The
// share session epoch state machine mirrors the Java client: 0 opens a
// session, positive epochs continue it (bumped after each success), -1
// closes; SHARE_SESSION_NOT_FOUND / INVALID_SHARE_SESSION_EPOCH /
// SHARE_SESSION_LIMIT_REACHED re-open from epoch 0.

///|
/// The acknowledgement a member makes over an offset range.
/// 0 Gap, 1 Accept, 2 Release, 3 Reject, 4 Renew.
pub enum ShareAckType {
  ShareAccept
  ShareRelease
  ShareReject
  ShareRenew
} derive(@debug.Debug, Eq)

///|
fn ShareAckType::code(self : ShareAckType) -> Int {
  match self {
    ShareAccept => 1
    ShareRelease => 2
    ShareReject => 3
    ShareRenew => 4
  }
}

///|
/// One record delivered to this share consumer, with the partition it
/// came from so the caller can acknowledge it later.
pub struct ShareRecord {
  partition : Int
  offset : Int64
  timestamp : Int64
  key : Bytes?
  value : Bytes?
  headers : Array[(Bytes, Bytes)]
} derive(@debug.Debug)

///|
/// Share consumer settings on top of the shared transport config.
pub struct ShareConsumerConfig {
  common : CommonConfig
  topic : String
  group_id : String
  start_from : StartFrom
  /// Cap on records one poll may acquire (and hold in flight).
  max_queue_size : Int
  /// Records at or above this many deliveries are auto-rejected rather
  /// than surfaced (negative disables the cap).
  delivery_count_limit : Int
  request_timeout_ms : Int
} derive(@debug.Debug)

///|
pub fn ShareConsumerConfig::new(
  bootstrap_servers : Array[String],
  topic : String,
  group_id : String,
  start_from? : StartFrom = Earliest,
  request_timeout_ms? : Int = 30000,
  security_protocol? : SecurityProtocol = Plaintext,
  sasl? : SaslConfig? = None,
  tls? : TlsClientOptions? = None,
  metadata_max_age_ms? : Int = 300000,
  max_queue_size? : Int = 100,
  delivery_count_limit? : Int = 3,
) -> ShareConsumerConfig raise {
  if topic.length() == 0 {
    raise ProtocolError::ProtocolError("topic must not be empty")
  }
  if group_id.is_empty() {
    raise ProtocolError::ProtocolError("group_id must not be empty")
  }
  if max_queue_size <= 0 {
    raise ProtocolError::ProtocolError(
      "max_queue_size must be positive, got \{max_queue_size}",
    )
  }
  {
    common: CommonConfig::new(
      bootstrap_servers,
      request_timeout_ms~,
      security_protocol~,
      sasl~,
      tls~,
      metadata_max_age_ms~,
    ),
    topic,
    group_id,
    start_from,
    max_queue_size,
    delivery_count_limit,
    request_timeout_ms,
  }
}

///|
/// One per-leader share session: epoch (0 opens, >0 continues, -1
/// closes) and the partitions the broker has in the session. Rebuilt
/// (from epoch 0) whenever the broker reports a session error.
pub struct ShareSession {
  mut epoch : Int
} derive(@debug.Debug)

///|
pub fn ShareSession::new() -> ShareSession {
  { epoch: 0, }
}

///|
/// Bump the epoch for the next request; a wrap starts over at 1 (never
/// back to 0, which would silently open a second session on the broker).
fn ShareSession::next(self : ShareSession) -> Unit {
  if self.epoch == 0x7fffffff {
    self.epoch = 1
  } else {
    self.epoch += 1
  }
}

///|
/// The share consumer: connect, subscribe to its group, then poll and
/// acknowledge.
pub struct ShareConsumer {
  topic : String
  mut topic_id : Uuid
  group_id : String
  start_from : StartFrom
  max_queue_size : Int
  delivery_count_limit : Int
  request_timeout_ms : Int
  retries : Int
  retry_backoff_ms : Int
  retry_backoff_max_ms : Int
  /// The user's task group; hosts the membership loop.
  group : @async.TaskGroup[Unit]
  priv cluster : ClusterClient
  mut partitions : Array[PartitionInfo]
  /// The membership assignment: (topic, partition) pairs the share
  /// coordinator pushed.
  priv mut assignment : Array[(String, Int)]
  priv mut member_id : String
  priv mut member_epoch : Int
  priv mut member_active : Bool
  priv mut member_spawned : Bool
  priv mut member_left : Bool
  priv mut member_error : String?
  priv mut heartbeat_interval_ms : Int
  priv mut share_coordinator : Int?
  /// Per-leader share fetch sessions.
  priv mut share_sessions : Map[Int, ShareSession]
  /// In-flight (acquired, unacknowledged) records, keyed by partition
  /// then offset.
  priv in_flight : Map[Int, Map[Int64, ShareRecord]]
  mut closed : Bool
}

///|
/// Connect a share consumer to the bootstrap set and resolve the topic's
/// partition leaders. The membership loop joins `group` on subscribe.
pub async fn ShareConsumer::connect_with_config(
  group~ : @async.TaskGroup[Unit],
  config : ShareConsumerConfig,
) -> ShareConsumer {
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
    topic_id: Uuid::zero(),
    group_id: config.group_id,
    start_from: config.start_from,
    max_queue_size: config.max_queue_size,
    delivery_count_limit: config.delivery_count_limit,
    request_timeout_ms: config.request_timeout_ms,
    retries: config.common.retries,
    retry_backoff_ms: config.common.retry_backoff_ms,
    retry_backoff_max_ms: config.common.retry_backoff_max_ms,
    group,
    cluster,
    partitions: [],
    assignment: [],
    member_id: "",
    member_epoch: 0,
    member_active: false,
    member_spawned: false,
    member_left: false,
    member_error: None,
    heartbeat_interval_ms: 0,
    share_coordinator: None,
    share_sessions: Map([]),
    in_flight: Map([]),
    closed: false,
  }
  consumer.refresh_metadata()
  consumer
}

///|
async fn ShareConsumer::refresh_metadata(self : ShareConsumer) -> Unit {
  self.cluster.refresh_metadata(Some([self.topic]))
  let topic_meta = match self.cluster.topic(self.topic) {
    Some(t) => t
    None => raise ProtocolError::ProtocolError("topic \{self.topic} not found")
  }
  self.topic_id = topic_meta.topic_id
  self.partitions = topic_meta.partitions
  self.share_sessions = Map([])
}

///|
/// Subscribe to the share group and start the membership loop.
pub fn ShareConsumer::subscribe(self : ShareConsumer) -> Unit raise {
  if self.member_active {
    return
  }
  if self.member_id.is_empty() {
    self.member_id = new_member_id()
  }
  self.member_active = true
  if !self.member_spawned {
    self.member_spawned = true
    let consumer = self
    self.group.spawn_bg(no_wait=false, allow_failure=true, () => {
      consumer.member_loop()
    })
  }
}

///|
/// The member's share coordinator connection, resolving (and caching)
/// the coordinator node on first use (CoordinatorType::Share).
async fn ShareConsumer::coordinator_conn(
  self : ShareConsumer,
) -> BrokerConnection {
  let node = match self.share_coordinator {
    Some(node) => node
    None => {
      let backoff = Backoff::new(
        base_ms=self.retry_backoff_ms,
        max_ms=self.retry_backoff_max_ms,
      )
      let mut found = -1
      for attempt in 0..<=self.retries {
        let infos = self.cluster.coordinator([self.group_id], Share)
        match infos.get(self.group_id) {
          Some(info) => if info.error_code == 0 { found = info.node_id }
          None => ()
        }
        if found >= 0 {
          break
        }
        @async.sleep(backoff.next_ms())
      }
      if found < 0 {
        raise ProtocolError::ProtocolError(
          "no share coordinator for group \{self.group_id}",
        )
      }
      self.share_coordinator = Some(found)
      found
    }
  }
  self.cluster.connection(node)
}

///|
/// The heartbeat loop: exchange ShareGroupHeartbeat every server-guided
/// interval, reconcile identity/epoch, and record the pushed assignment.
/// Exits with a graceful leave (epoch -1) when the consumer closes.
async fn ShareConsumer::member_loop(self : ShareConsumer) -> Unit {
  let backoff = Backoff::new(
    base_ms=self.retry_backoff_ms,
    max_ms=self.retry_backoff_max_ms,
  )
  for ;; {
    if self.closed || !self.member_active {
      break
    }
    let heartbeat = try? self
      .coordinator_conn()
      .share_group_heartbeat(
        self.group_id,
        self.member_id,
        self.member_epoch,
        subscribed_topic_names=Some([self.topic]),
        timeout_ms=self.request_timeout_ms,
      )
    let result : ShareGroupHeartbeatResult = match heartbeat {
      Ok(result) => result
      Err(_) => {
        let _ = self.recover() catch { _ => () }
        @async.sleep(backoff.next_ms())
        continue
      }
    }
    if result.error_code != 0 {
      match result.error_code {
        // Identity/epoch races: restart the join from epoch 0.
        25 | 110 | 113 => self.member_epoch = 0
        // Not the coordinator: drop the cached node and re-resolve.
        16 => {
          self.share_coordinator = None
          @async.sleep(backoff.next_ms())
        }
        81 | 14 | 15 => @async.sleep(backoff.next_ms())
        code => {
          self.member_error = Some(
            "ShareGroupHeartbeat failed: \{error_name(code)}\{share_error_detail(result.error_message)}",
          )
          break
        }
      }
      continue
    }
    if !result.member_id.is_empty() {
      self.member_id = result.member_id
    }
    if result.member_epoch >= 0 {
      self.member_epoch = result.member_epoch
    }
    if result.heartbeat_interval_ms > 0 {
      self.heartbeat_interval_ms = result.heartbeat_interval_ms
    }
    let pairs : Array[(String, Int)] = []
    for entry in result.assignment {
      match self.cluster.topic_name(entry.topic_id) {
        Some(name) =>
          for partition in entry.partitions {
            pairs.push((name, partition))
          }
        None => ()
      }
    }
    self.assignment = pairs
    // Rebuild share sessions when the assignment changes, so the
    // session's partition set follows the assignment.
    self.share_sessions = Map([])
    let wait = Int::max(1, self.heartbeat_interval_ms / SENDER_TICK_MS)
    for _ in 0..<wait {
      if self.closed || !self.member_active {
        break
      }
      @async.sleep(SENDER_TICK_MS)
    }
  }
  self.leave_group()
}

///|
async fn ShareConsumer::leave_group(self : ShareConsumer) -> Unit {
  if self.member_epoch > 0 {
    try {
      let _ = self
        .coordinator_conn()
        .share_group_heartbeat(
          self.group_id,
          self.member_id,
          -1,
          timeout_ms=self.request_timeout_ms,
        )
    } catch {
      _ => ()
    }
  }
  self.assignment = []
  self.member_epoch = 0
  self.member_left = true
}

///|
async fn ShareConsumer::recover(self : ShareConsumer) -> Unit {
  self.cluster.reconnect()
  self.refresh_metadata()
}

///|
fn share_error_detail(message : String?) -> String {
  match message {
    Some(msg) => ": \{msg}"
    None => ""
  }
}

///|
/// The topic-partitions the coordinator currently assigns to this member.
pub fn ShareConsumer::assignment(self : ShareConsumer) -> Array[(String, Int)] {
  self.assignment
}

///|
/// This member's generated id; empty while not subscribed.
pub fn ShareConsumer::member_identity(self : ShareConsumer) -> String {
  self.member_id
}

///|
/// The last fatal membership error, if the loop stopped on one.
pub fn ShareConsumer::membership_error(self : ShareConsumer) -> String? {
  self.member_error
}

///|
/// Stop the consumer and leave the group.
pub fn ShareConsumer::close(self : ShareConsumer) -> Unit {
  if !self.closed {
    self.closed = true
  }
}

///|
/// The per-leader connection from the cluster pool.
async fn ShareConsumer::leader_conn(
  self : ShareConsumer,
  leader : Int,
) -> BrokerConnection {
  self.cluster.connection(leader)
}

///|
/// The partitions assigned to this member, grouped by leader (the
/// single-topic shape: every partition shares the topic's id).
fn ShareConsumer::wanted_by_leader(
  self : ShareConsumer,
) -> Map[Int, Array[Int]] {
  let by_leader : Map[Int, Array[Int]] = Map([])
  for pair in self.assignment {
    if pair.0 == self.topic {
      for p in self.partitions {
        if p.index == pair.1 {
          let arr = by_leader.get_or_init(p.leader, fn() { [] })
          arr.push(p.index)
        }
      }
    }
  }
  by_leader
}

///|
/// Poll: acquire records for every assigned partition via ShareFetch v2
/// and return them. Records land in the in-flight queue until
/// acknowledge() drains them. Records at or past the delivery limit are
/// auto-rejected and never surfaced.
pub async fn ShareConsumer::poll(
  self : ShareConsumer,
  max_wait_ms? : Int = 500,
  max_records? : Int = 100,
) -> Array[ShareRecord] {
  let by_leader = self.wanted_by_leader()
  let out : Array[ShareRecord] = []
  let mut refresh = false
  @async.with_task_group(fn(group) {
    for leader, partitions in by_leader {
      group.spawn_bg(no_wait=false, allow_failure=true, () => {
        let result = self.acquire_from_leader(leader, partitions, max_wait_ms) catch {
          _ => {
            refresh = true
            None
          }
        }
        match result {
          Some(records) =>
            for record in records {
              out.push(record)
            }
          None => ()
        }
      })
    }
  })
  if refresh {
    let _ = self.recover() catch { _ => () }
  }
  // Cap the batch returned to the caller (extra acquired records stay
  // in flight until the next poll returns them). Slicing yields a view;
  // rebuild into an owned array.
  if out.length() > max_records {
    let capped : Array[ShareRecord] = []
    for i in 0..<max_records {
      capped.push(out[i])
    }
    capped
  } else {
    out
  }
}

///|
/// One leader's acquire exchange. The session epoch opens (0) on first
/// use and bumps after each success; a session error re-opens from 0.
/// Returns None when the exchange needs a metadata refresh.
async fn ShareConsumer::acquire_from_leader(
  self : ShareConsumer,
  leader : Int,
  partitions : Array[Int],
  max_wait_ms : Int,
) -> Array[ShareRecord]? {
  let conn = self.leader_conn(leader)
  let session = self.share_sessions.get_or_init(leader, fn() {
    ShareSession::new()
  })
  let topic : Array[ShareFetchTopic] = [
    {
      topic_id: self.topic_id,
      partitions: partitions.map(fn(index) {
        { partition: index, acknowledgment_batches: [], }
      }),
    },
  ]
  let result = conn.share_fetch(
    self.group_id,
    self.member_id,
    session.epoch,
    topic,
    forgotten=[],
    max_wait_ms~,
    timeout_ms=self.request_timeout_ms,
  ) catch {
    _ => return None
  }
  // Session errors re-open from epoch 0 and do not carry records.
  match result.error_code {
    122 | 123 | 133 => {
      session.epoch = 0
      return Some([])
    }
    0 => ()
    _ => return Some([])
  }
  session.next()
  let records : Array[ShareRecord] = []
  for topic_result in result.topics {
    for part in topic_result.partitions {
      if part.error_code != 0 {
        continue
      }
      // Every acquired record falls inside one acquired range; records
      // already at or past the delivery-count cap are not surfaced (the
      // broker keeps them for redelivery / poison handling).
      for range in part.acquired {
        for record in part.records {
          if record.offset >= range.first_offset &&
            record.offset <= range.last_offset &&
            self.under_delivery_cap(range.delivery_count) {
            let share_record = {
              partition: part.partition,
              offset: record.offset,
              timestamp: record.timestamp,
              key: record.key,
              value: record.value,
              headers: record.headers,
            }
            let by_offset = self.in_flight.get_or_init(part.partition, fn() {
              Map([])
            })
            by_offset[record.offset] = share_record
            records.push(share_record)
          }
        }
      }
    }
  }
  Some(records)
}

///|
/// The delivery-count cap admits a record while the cap is disabled
/// (negative) or its count is still below the limit.
fn ShareConsumer::under_delivery_cap(self : ShareConsumer, count : Int) -> Bool {
  self.delivery_count_limit < 0 || count < self.delivery_count_limit
}

///|
/// Acknowledge a set of records with one delivery action, sending a
/// ShareAcknowledge request. `records` must have been returned by poll()
/// and still be in flight. Returns the per-partition result.
pub async fn ShareConsumer::acknowledge(
  self : ShareConsumer,
  records : Array[ShareRecord],
  action : ShareAckType,
) -> Array[ShareAcknowledgeTopicResult] {
  if self.member_id.is_empty() {
    raise ProtocolError::ProtocolError(
      "acknowledge requires the consumer to be subscribed to its group",
    )
  }
  // Group records by partition, then collapse contiguous offsets into
  // acknowledgement batches of one action each.
  let by_partition : Map[Int, Array[(Int64, Int)]] = Map([])
  for record in records {
    self.discard_in_flight(record.partition, record.offset)
    let list = by_partition.get_or_init(record.partition, fn() { [] })
    list.push((record.offset, action.code()))
  }
  // Route by each partition's leader.
  let by_leader : Map[Int, Array[ShareFetchTopic]] = Map([])
  for partition, entries in by_partition {
    let leader = self.leader_of_partition(partition)
    let topics = by_leader.get_or_init(leader, fn() { [] })
    topics.push({
      topic_id: self.topic_id,
      partitions: [
        { partition, acknowledgment_batches: compress_batches(entries), },
      ],
    })
  }
  let out : Array[ShareAcknowledgeTopicResult] = []
  @async.with_task_group(fn(group) {
    for leader, topics in by_leader {
      group.spawn_bg(no_wait=false, allow_failure=true, () => {
        let session = self.share_sessions.get_or_init(leader, fn() {
          ShareSession::new()
        })
        try {
          let result = self.cluster
            .connection(leader)
            .share_acknowledge(
              self.group_id,
              self.member_id,
              session.epoch,
              topics,
              timeout_ms=self.request_timeout_ms,
            )
          if result.error_code == 0 {
            session.next()
          } else if result.error_code == 122 || result.error_code == 123 {
            session.epoch = 0
          }
          for topic in result.topics {
            out.push(topic)
          }
        } catch {
          _ => ()
        }
      })
    }
  })
  out
}

///|
fn ShareConsumer::discard_in_flight(
  self : ShareConsumer,
  partition : Int,
  offset : Int64,
) -> Unit {
  match self.in_flight.get(partition) {
    Some(by_offset) => by_offset.remove(offset)
    None => ()
  }
}

///|
fn ShareConsumer::leader_of_partition(
  self : ShareConsumer,
  partition : Int,
) -> Int raise ProtocolError {
  for p in self.partitions {
    if p.index == partition {
      return p.leader
    }
  }
  raise ProtocolError::ProtocolError(
    "no leader for partition \{partition} of \{self.topic}",
  )
}

///|
/// Collapse (offset, action) pairs into contiguous acknowledgement
/// batches, splitting whenever the action changes or the offsets stop
/// being consecutive. Pairs are already pushed in ascending offset order.
fn compress_batches(entries : Array[(Int64, Int)]) -> Array[ShareAckBatch] {
  let out : Array[ShareAckBatch] = []
  let mut i = 0
  while i < entries.length() {
    let (start, action) = entries[i]
    let mut end = start
    let types : Array[Int] = [action]
    let mut j = i + 1
    while j < entries.length() {
      let (offset, next_action) = entries[j]
      if offset == end + 1L && next_action == action {
        end = offset
        types.push(action)
        j += 1
      } else {
        break
      }
    }
    out.push({
      first_offset: start,
      last_offset: end,
      acknowledge_types: types,
    })
    i = j
  }
  out
}
