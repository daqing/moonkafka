// The classic consumer-group member (Phase 4 compat path): JoinGroup →
// SyncGroup → Heartbeat with the assignment strategies riding the
// protocol name. The leader deserializes every member's
// ConsumerProtocolSubscription, runs the agreed assignor over the
// topics' partition counts, and hands out ConsumerProtocolAssignment
// bytes; followers sync empty and receive their own.
//
// Cooperative-sticky runs the KIP-429 two-round protocol: when any
// member owns partitions the target moves away, the leader's round
// sends only keep-sets (revocations), the members that revoked rejoin,
// and the next round delivers the full target. Members report their
// owned partitions in the subscription's userData slot.

///|
/// Subscribe with the classic protocol: JoinGroup/SyncGroup under the
/// given assignment strategies (most-preferred first; the group runs on
/// the one the coordinator settles on).
pub fn Consumer::subscribe_classic(
  self : Consumer,
  topics : Array[String],
  assignors? : Array[Assignor] = [RangeAssignor],
  listener? : RebalanceListener?,
) -> Unit raise {
  self.guard_group_subscription()
  if topics.is_empty() {
    raise ProtocolError::ProtocolError(
      "subscribe_classic requires at least one topic",
    )
  }
  if assignors.is_empty() {
    raise ProtocolError::ProtocolError(
      "subscribe_classic requires at least one assignment strategy",
    )
  }
  self.rebalance_listener = listener.unwrap_or(self.rebalance_listener)
  self.subscribed_names = Some(topics)
  self.subscribed_regex = None
  self.classic_assignors = assignors
  self.member_kind = ClassicMember
  self.start_membership()
}

///|
/// The supported protocol entries for the current subscription: one per
/// configured assignor, metadata = ConsumerProtocolSubscription with the
/// owned partitions reported in userData for cooperative stickiness.
fn Consumer::classic_protocols(self : Consumer) -> Array[JoinGroupProtocol] {
  let topics = self.expanded_subscription()
  let owned : Array[(String, Array[Int])] = []
  // Group this member's current assignment per topic for the userData.
  let per_topic : Map[String, Array[Int]] = Map([])
  for pair in self.assignment_pairs() {
    let partitions = per_topic.get_or_init(pair.0, fn() { [] })
    partitions.push(pair.1)
  }
  for topic, partitions in per_topic {
    owned.push((topic, partitions))
  }
  self.classic_assignors.map(fn(assignor) {
    let metadata = match assignor {
      CooperativeStickyAssignor =>
        encode_cooperative_subscription(topics, owned)
      _ => encode_consumer_protocol_subscription(topics)
    }
    { name: assignor.name(), metadata, }
  })
}

///|
/// Serialize a cooperative subscription: topics plus the owned
/// partitions riding the userData slot (our own assignment encoding —
/// an interop caveat against Java's private userData format).
fn encode_cooperative_subscription(
  topics : Array[String],
  owned : Array[(String, Array[Int])],
) -> Bytes {
  let body = @buf.Encoder::new()
  body.write_i16(CONSUMER_PROTOCOL_SUBSCRIPTION_VERSION)
  body.write_i16(topics.length())
  for topic in topics {
    let bytes = @utf8.encode(topic)
    body.write_i16(bytes.length())
    body.write_bytes(bytes)
  }
  let user_data = encode_consumer_protocol_assignment(owned)
  body.write_i16(user_data.length())
  body.write_bytes(user_data)
  body.to_bytes()
}

///|
/// Decode a subscription's topics and, when present, its owned
/// partitions (cooperative userData).
fn decode_subscription_metadata(
  metadata : Bytes,
) -> (Array[String], Array[(String, Array[Int])]) raise {
  let d = @buf.Decoder::new(metadata)
  let version = d.read_i16()
  guard version == CONSUMER_PROTOCOL_SUBSCRIPTION_VERSION else {
    raise ProtocolError::ProtocolError(
      "unsupported ConsumerProtocolSubscription version \{version}",
    )
  }
  let n = d.read_i16()
  let topics : Array[String] = []
  for _ in 0..<n {
    let len = d.read_i16()
    topics.push(@utf8.decode_lossy(d.read_bytes(len)[:]))
  }
  let mut owned : Array[(String, Array[Int])] = []
  let user_len = d.read_i16()
  if user_len > 0 {
    owned = decode_consumer_protocol_assignment(d.read_bytes(user_len))
  }
  (topics, owned)
}

///|
/// The classic member loop: join, sync, apply, heartbeat until the next
/// rebalance, and leave gracefully on exit.
async fn Consumer::classic_loop(self : Consumer) -> Unit {
  let backoff = Backoff::new(
    base_ms=self.retry_backoff_ms,
    max_ms=self.retry_backoff_max_ms,
  )
  for ;; {
    if self.closed || !self.member_active {
      break
    }
    let rejoin = self.classic_join_round() catch {
      e => {
        // Transport (or unexpected protocol) failure: record it for
        // diagnosis, reconnect, and pace the next attempt.
        self.member_error = Some("\{e}")
        let _ = self.recover() catch { _ => () }
        @async.sleep(backoff.next_ms())
        continue
      }
    }
    match rejoin {
      ClassicOk => {
        // Heartbeat until the group rebalances or we are done.
        let mut leave_now = false
        for ;; {
          let wait = Int::max(1, self.heartbeat_interval_ms / SENDER_TICK_MS)
          for _ in 0..<wait {
            if self.closed || !self.member_active {
              break
            }
            @async.sleep(SENDER_TICK_MS)
          }
          if self.closed || !self.member_active {
            leave_now = true
            break
          }
          let code = try? self
            .group_coordinator_conn()
            .group_heartbeat(
              self.group_id.unwrap_or(""),
              self.generation_id,
              self.member_id,
              group_instance_id=self.group_instance_id,
              timeout_ms=self.request_timeout_ms,
            )
          match code {
            Ok(0) => continue
            Ok(27) => break // rebalance in progress: rejoin
            Ok(22) | Ok(25) => {
              // Illegal generation / unknown member: start over.
              self.generation_id = -1
              self.member_id = ""
              self.set_assignment([])
              break
            }
            Ok(82) => {
              self.member_error = Some(
                "the static member was fenced: FENCED_INSTANCE_ID",
              )
              leave_now = true
              break
            }
            Ok(16) => {
              self.group_coordinator = None
              @async.sleep(backoff.next_ms())
            }
            Ok(_) | Err(_) => @async.sleep(backoff.next_ms())
          }
        }
        if leave_now {
          break
        }
      }
      ClassicRejoin => continue
      ClassicFatal(msg) => {
        self.member_error = Some(msg)
        break
      }
    }
  }
  self.classic_leave()
  if !self.autocommit_enabled() {
    self.cluster.close()
  }
}

///|
priv enum ClassicRound {
  /// Member synced into a stable generation.
  ClassicOk
  /// Rebalance or identity reset: run another join round immediately.
  ClassicRejoin
  /// Fatal error the caller must surface.
  ClassicFatal(String)
}

///|
/// One join round: JoinGroup, (leader) assignment computation, SyncGroup,
/// assignment application.
async fn Consumer::classic_join_round(self : Consumer) -> ClassicRound {
  let conn = self.group_coordinator_conn()
  let join = conn.join_group(
    self.group_id.unwrap_or(""),
    self.session_timeout_ms,
    self.request_timeout_ms, // rebalance timeout
    self.member_id,
    group_instance_id=self.group_instance_id,
    protocols=self.classic_protocols(),
    timeout_ms=self.request_timeout_ms,
  )
  match join.error_code {
    0 => ()
    25 => {
      // The coordinator forgot us: join fresh.
      self.member_id = ""
      self.generation_id = -1
      return ClassicRejoin
    }
    27 => return ClassicRejoin
    14 | 15 => return ClassicRejoin
    16 => {
      self.group_coordinator = None
      return ClassicRejoin
    }
    82 =>
      return ClassicFatal("the static member was fenced: FENCED_INSTANCE_ID")
    code => return ClassicFatal("JoinGroup failed: \{error_name(code)}")
  }
  self.member_id = join.member_id
  self.generation_id = join.generation_id
  let protocol_name = join.protocol_name
  let assignor = match protocol_name {
    "roundrobin" => RoundRobinAssignor
    "sticky" => StickyAssignor
    "cooperative-sticky" => CooperativeStickyAssignor
    _ => RangeAssignor
  }
  // Leader: compute the assignment over every member's subscription.
  let assignments : Array[SyncGroupAssignment] = []
  let mut target : Map[String, Array[(String, Int)]] = Map([])
  let mut cooperative = assignor is CooperativeStickyAssignor
  if join.member_id == join.leader_id && !join.members.is_empty() {
    let members : Array[Assignee] = []
    let owned : Map[String, Array[(String, Int)]] = Map([])
    let topics : Array[String] = []
    for member in join.members {
      let (subscribed, member_owned) = decode_subscription_metadata(
        member.metadata,
      )
      members.push({ member_id: member.member_id, topics: subscribed, })
      owned[member.member_id] = flatten_pairs(member_owned)
      for topic in subscribed {
        let mut known = false
        for t in topics {
          if t == topic {
            known = true
          }
        }
        if !known {
          topics.push(topic)
        }
      }
    }
    // Partition counts from fresh metadata for every subscribed topic.
    let _ = self.cluster.refresh_metadata(Some(topics)) catch { _ => () }
    let counts : Map[String, Int] = Map([])
    for topic in topics {
      let count = match self.cluster.topic(topic) {
        Some(meta) => meta.partitions.length()
        None => 0
      }
      counts[topic] = count
    }
    target = compute_assignment(assignor, members, counts, owned~)
    // The cooperative two-round: revocations first, additions next.
    let mut revocations = false
    if cooperative {
      for member in members {
        let owned_pairs = owned.get(member.member_id).unwrap_or([])
        let target_pairs = target.get(member.member_id).unwrap_or([])
        for pair in owned_pairs {
          if !any_pair(target_pairs, pair.0, pair.1) {
            revocations = true
          }
        }
      }
    }
    if cooperative && revocations {
      // Round 1: keep-sets only.
      for member in members {
        let owned_pairs = owned.get(member.member_id).unwrap_or([])
        let target_pairs = target.get(member.member_id).unwrap_or([])
        let keep : Array[(String, Int)] = []
        for pair in owned_pairs {
          if any_pair(target_pairs, pair.0, pair.1) {
            keep.push(pair)
          }
        }
        assignments.push({
          member_id: member.member_id,
          assignment: encode_consumer_protocol_assignment(
            group_pairs_by_topic(keep),
          ),
        })
      }
    } else {
      // Eager round (or cooperative with nothing to revoke): full target.
      for member in members {
        assignments.push({
          member_id: member.member_id,
          assignment: encode_consumer_protocol_assignment(
            group_pairs_by_topic(target.get(member.member_id).unwrap_or([])),
          ),
        })
      }
      cooperative = false
    }
  }
  let sync = conn.sync_group(
    self.group_id.unwrap_or(""),
    self.generation_id,
    self.member_id,
    group_instance_id=self.group_instance_id,
    protocol_name~,
    assignments~,
    timeout_ms=self.request_timeout_ms,
  )
  match sync.error_code {
    0 => ()
    27 => return ClassicRejoin
    22 | 25 => {
      self.generation_id = -1
      self.member_id = ""
      return ClassicRejoin
    }
    82 =>
      return ClassicFatal("the static member was fenced: FENCED_INSTANCE_ID")
    code => return ClassicFatal("SyncGroup failed: \{error_name(code)}")
  }
  let before = self.assignment_pairs()
  let pairs : Array[(String, Int)] = []
  for entry in decode_consumer_protocol_assignment(sync.assignment) {
    let (topic, partitions) = entry
    for partition in partitions {
      pairs.push((topic, partition))
    }
  }
  let revoked = count_lost(before, pairs)
  self.set_assignment(pairs)
  // Cooperative two-round: a member that just revoked rejoins at once so
  // the next round delivers the additions.
  if cooperative && revoked > 0 {
    return ClassicRejoin
  }
  ClassicOk
}

///|
async fn Consumer::classic_leave(self : Consumer) -> Unit {
  if self.generation_id >= 0 && !self.member_id.is_empty() {
    try {
      let _ = self
        .group_coordinator_conn()
        .leave_group(
          self.group_id.unwrap_or(""),
          self.member_id,
          group_instance_id=self.group_instance_id,
          timeout_ms=self.request_timeout_ms,
        )
    } catch {
      _ => ()
    }
  }
  if !self.assignment_pairs().is_empty() {
    self.fire_revoke(self.assignment_pairs())
  }
  self.assignment = []
  self.generation_id = -1
  self.member_id = ""
}

///|
/// How many (topic, partition) pairs of `before` are missing from `after`.
fn count_lost(
  before : Array[(String, Int)],
  after : Array[(String, Int)],
) -> Int {
  let mut lost = 0
  for pair in before {
    if !any_pair(after, pair.0, pair.1) {
      lost += 1
    }
  }
  lost
}

///|
/// Group (topic, partition) pairs per topic for the assignment encoding.
fn group_pairs_by_topic(
  pairs : Array[(String, Int)],
) -> Array[(String, Array[Int])] {
  let per_topic : Map[String, Array[Int]] = Map([])
  let order : Array[String] = []
  for pair in pairs {
    let partitions = per_topic.get_or_init(pair.0, fn() {
      order.push(pair.0)
      []
    })
    partitions.push(pair.1)
  }
  let out : Array[(String, Array[Int])] = []
  for topic in order {
    match per_topic.get(topic) {
      Some(partitions) => out.push((topic, partitions))
      None => ()
    }
  }
  out
}

///|
/// Flatten (topic, partitions) entries into (topic, partition) pairs.
fn flatten_pairs(entries : Array[(String, Array[Int])]) -> Array[(String, Int)] {
  let out : Array[(String, Int)] = []
  for entry in entries {
    let (topic, partitions) = entry
    for partition in partitions {
      out.push((topic, partition))
    }
  }
  out
}
