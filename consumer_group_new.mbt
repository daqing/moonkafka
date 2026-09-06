// The KIP-848 consumer group member (Phase 4): the primary group path.
// One ConsumerGroupHeartbeat exchange drives join, reconcile, and leave;
// the coordinator owns the assignment and pushes it in the response.
// The member generates its own id (a uuid kept for the process
// lifetime), joins with epoch 0, applies server assignments atomically
// (revoke-set, swap, assign-set), and leaves with epoch -1.
//
// Error policy: UNKNOWN_MEMBER_ID / STALE_MEMBER_EPOCH / FENCED_MEMBER_
// EPOCH restart the join (epoch back to 0); GROUP_MAX_SIZE_REACHED and
// coordinator hiccups retry with backoff; UNSUPPORTED_ASSIGNOR /
// UNRELEASED_INSTANCE_ID / INVALID_REGULAR_EXPRESSION are fatal
// configuration errors recorded on the consumer. Static members
// (`group_instance_id`) keep their identity across restarts.

///|
/// Which membership protocol the member loop runs.
priv enum MemberKind {
  Kip848Member
  ClassicMember
}

///|
/// Rebalance hooks fired around assignment changes: on_revoke with the
// revoked topic-partitions before they are dropped, on_assign with the
// newly gained ones after they are added.
pub struct RebalanceListener {
  on_assign : (Array[(String, Int)]) -> Unit
  on_revoke : (Array[(String, Int)]) -> Unit
}

///|
pub fn RebalanceListener::new(
  on_assign~ : (Array[(String, Int)]) -> Unit,
  on_revoke~ : (Array[(String, Int)]) -> Unit,
) -> RebalanceListener {
  { on_assign, on_revoke, }
}

///|
pub fn RebalanceListener::default() -> RebalanceListener {
  { on_assign: fn(_partitions) { () }, on_revoke: fn(_partitions) { () }, }
}

///|
/// Glob-style topic matcher for subscription expansion: `*` matches any
/// run of characters, `?`/`.` a single one (covering the common
/// "prefix.*" patterns); full Java regex arrives with the polish pass.
fn wildcard_match(pattern : String, name : String) -> Bool {
  let p : Array[Char] = []
  for c in pattern {
    p.push(c)
  }
  let n : Array[Char] = []
  for c in name {
    n.push(c)
  }
  let mut pi = 0
  let mut ni = 0
  let mut star = -1
  let mut mark = 0
  for ;; {
    if ni >= n.length() {
      break
    }
    if pi < p.length() && (p[pi] == '?' || p[pi] == '.' || p[pi] == n[ni]) {
      pi = pi + 1
      ni = ni + 1
    } else if pi < p.length() && p[pi] == '*' {
      star = pi
      mark = ni
      pi = pi + 1
    } else if star >= 0 {
      pi = star + 1
      mark = mark + 1
      ni = mark
    } else {
      return false
    }
  }
  for ;; {
    if pi >= p.length() || p[pi] != '*' {
      break
    }
    pi = pi + 1
  }
  pi == p.length()
}

///|
/// A client-generated member id: 32 hex chars from the clock-seeded
/// xorshift, unique enough across processes (correctness does not rest
/// on cryptographic randomness — the coordinator arbitrates identity).
fn new_member_id() -> String {
  let mut seed = xorshift64(
    @async.now().reinterpret_as_uint64() ^ 0x9E3779B97F4A7C15UL,
  )
  let digits = "0123456789abcdef"
  let sb = StringBuilder()
  for _ in 0..<4 {
    seed = xorshift64(seed)
    for shift in [60, 56, 52, 48, 44, 40, 36, 32] {
      let nibble = ((seed >> shift) & 0xFUL).to_int()
      sb.write_char(digits[nibble].to_char().unwrap_or('0'))
    }
  }
  sb.to_string()
}

///|
/// Subscribe to explicit topics and start (or join) the group membership
/// loop in the consumer's task group. Requires a group_id.
pub fn Consumer::subscribe(
  self : Consumer,
  topics : Array[String],
  listener? : RebalanceListener?,
) -> Unit raise {
  self.guard_group_subscription()
  if topics.is_empty() {
    raise ProtocolError::ProtocolError("subscribe requires at least one topic")
  }
  self.rebalance_listener = listener.unwrap_or(self.rebalance_listener)
  self.subscribed_names = Some(topics)
  self.subscribed_regex = None
  self.member_kind = Kip848Member
  self.start_membership()
}

///|
/// Subscribe with a topic pattern: the pattern rides the heartbeat
/// (v1) and the client expands it against metadata to drive fetches.
pub async fn Consumer::subscribe_regex(
  self : Consumer,
  pattern : String,
  listener? : RebalanceListener?,
) -> Unit {
  self.guard_group_subscription()
  if pattern.is_empty() {
    raise ProtocolError::ProtocolError("subscribe_regex requires a pattern")
  }
  self.rebalance_listener = listener.unwrap_or(self.rebalance_listener)
  self.subscribed_regex = Some(pattern)
  self.subscribed_names = Some([])
  self.member_kind = Kip848Member
  // Load the topic catalog once; the loop re-expands as it refreshes.
  let _ = self.cluster.refresh_metadata(None) catch { _ => () }
  self.start_membership()
}

///|
fn Consumer::guard_group_subscription(self : Consumer) -> Unit raise {
  if self.group_id is None {
    raise ProtocolError::ProtocolError(
      "subscribing requires a group_id on the consumer config",
    )
  }
}

///|
fn Consumer::start_membership(self : Consumer) -> Unit raise {
  if self.member_active {
    // The loop picks the new subscription up on its next heartbeat.
    return
  }
  if self.member_id.is_empty() {
    self.member_id = new_member_id()
  }
  self.member_active = true
  if !self.member_spawned {
    self.member_spawned = true
    let kind = self.member_kind
    self.group.spawn_bg(no_wait=false, allow_failure=true, () => {
      match kind {
        Kip848Member => self.member_loop()
        ClassicMember => self.classic_loop()
      }
    })
  }
}

///|
/// Leave the group gracefully: a final heartbeat with epoch -1, revoke
/// the assignment, and stop the loop.
pub async fn Consumer::unsubscribe(self : Consumer) -> Unit {
  if !self.member_active {
    return
  }
  self.member_active = false
  // The loop performs the actual leave; give it a moment to land.
  for _ in 0..<200 {
    if !self.member_spawned || self.member_left {
      break
    }
    @async.sleep(5)
  }
}

///|
/// The topic partitions currently assigned to this member.
pub fn Consumer::assignment(self : Consumer) -> Array[(String, Int)] {
  self.assignment_pairs()
}

///|
/// This member's id (client-generated, stable for the consumer's
/// lifetime); empty while not subscribed.
pub fn Consumer::member_identity(self : Consumer) -> String {
  self.member_id
}

///|
/// The last fatal membership error, if the loop stopped on one.
pub fn Consumer::membership_error(self : Consumer) -> String? {
  self.member_error
}

///|
/// The heartbeat loop: exchange heartbeats every server-guided interval,
/// reconcile the identity and epoch, and apply pushed assignments.
/// Exits (with a graceful leave) when the consumer closes or the member
/// unsubscribes.
async fn Consumer::member_loop(self : Consumer) -> Unit {
  let backoff = Backoff::new(
    base_ms=self.retry_backoff_ms,
    max_ms=self.retry_backoff_max_ms,
  )
  let mut iterations = 0
  for ;; {
    if self.closed || !self.member_active {
      break
    }
    iterations += 1
    if self.subscribed_regex is Some(_) && iterations % 10 == 0 {
      // Re-expand the regex against fresh metadata so newly created
      // topics join the subscription.
      let _ = self.cluster.refresh_metadata(None) catch { _ => () }
    }
    let names = self.expanded_subscription()
    let heartbeat = try? self
      .group_coordinator_conn()
      .consumer_group_heartbeat(
        self.group_id.unwrap_or(""),
        self.member_id,
        self.member_epoch,
        instance_id=self.group_instance_id,
        rebalance_timeout_ms=self.request_timeout_ms,
        subscribed_topic_names=Some(names),
        subscribed_topic_regex=self.subscribed_regex,
        timeout_ms=self.request_timeout_ms,
      )
    let result : ConsumerGroupHeartbeatResult = match heartbeat {
      Ok(result) => result
      Err(_) => {
        // Transport failure: reconnect and pace the next attempt.
        let _ = self.recover() catch { _ => () }
        @async.sleep(backoff.next_ms())
        continue
      }
    }
    if result.error_code != 0 {
      match result.error_code {
        // Identity/epoch races: restart the join from epoch 0.
        25 | 110 | 113 => {
          self.member_epoch = 0
          self.set_assignment([])
        }
        // A static member re-announces with epoch -2 (KIP-848).
        111 => self.member_epoch = -2
        81 | 14 | 15 => @async.sleep(backoff.next_ms())
        16 => {
          self.group_coordinator = None
          @async.sleep(backoff.next_ms())
        }
        code => {
          // Fatal configuration errors surface to the caller.
          self.member_error = Some(
            "ConsumerGroupHeartbeat failed: \{error_name(code)}\{heartbeat_error_detail(result.error_message)}",
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
    self.set_assignment(pairs)
    // Sleep the server's interval in tick-sized steps so close()
    // lands promptly.
    let wait = Int::max(1, self.heartbeat_interval_ms / SENDER_TICK_MS)
    for _ in 0..<wait {
      if self.closed || !self.member_active {
        break
      }
      @async.sleep(SENDER_TICK_MS)
    }
  }
  self.leave_group()
  if !self.autocommit_enabled() {
    // The member loop owns the teardown when autocommit does not.
    self.cluster.close()
  }
}

///|
async fn Consumer::leave_group(self : Consumer) -> Unit {
  let had_assignment = !self.assignment_pairs().is_empty()
  if self.member_epoch > 0 {
    // Graceful leave (epoch -1); best effort — a dead coordinator is
    // not the caller's problem at this point.
    try {
      let _ = self
        .group_coordinator_conn()
        .consumer_group_heartbeat(
          self.group_id.unwrap_or(""),
          self.member_id,
          -1,
          instance_id=self.group_instance_id,
          timeout_ms=self.request_timeout_ms,
        )
    } catch {
      _ => ()
    }
  }
  if had_assignment {
    self.fire_revoke(self.assignment_pairs())
  }
  self.assignment = []
  self.member_epoch = 0
  self.member_left = true
}

///|
/// The subscribed topic names: explicit names, or the regex expanded
/// against the cached topic catalog.
fn Consumer::expanded_subscription(self : Consumer) -> Array[String] {
  match self.subscribed_regex {
    Some(pattern) => {
      let out : Array[String] = []
      for name in self.cluster.topics_snapshot() {
        if wildcard_match(pattern, name) {
          out.push(name)
        }
      }
      out
    }
    None => self.subscribed_names.unwrap_or([])
  }
}

///|
/// Apply a server assignment atomically: compute the revoke/assign
/// deltas against the current assignment, fire the listener's
/// on_revoke, swap the assignment in, then fire on_assign.
fn Consumer::set_assignment(
  self : Consumer,
  pairs : Array[(String, Int)],
) -> Unit {
  let current = self.assignment_pairs()
  let revoked : Array[(String, Int)] = []
  for pair in current {
    let mut kept = false
    for other in pairs {
      if other.0 == pair.0 && other.1 == pair.1 {
        kept = true
      }
    }
    if !kept {
      revoked.push(pair)
    }
  }
  let assigned : Array[(String, Int)] = []
  for pair in pairs {
    let mut known = false
    for other in current {
      if other.0 == pair.0 && other.1 == pair.1 {
        known = true
      }
    }
    if !known {
      assigned.push(pair)
    }
  }
  if !revoked.is_empty() {
    self.fire_revoke(revoked)
  }
  self.assignment = pairs
  if !assigned.is_empty() {
    match self.rebalance_listener {
      Some(listener) => (listener.on_assign)(assigned)
      None => ()
    }
  }
}

///|
fn Consumer::fire_revoke(self : Consumer, pairs : Array[(String, Int)]) -> Unit {
  match self.rebalance_listener {
    Some(listener) => (listener.on_revoke)(pairs)
    None => ()
  }
}

///|
fn Consumer::assignment_pairs(self : Consumer) -> Array[(String, Int)] {
  self.assignment
}

///|
fn heartbeat_error_detail(message : String?) -> String {
  match message {
    Some(msg) => ": \{msg}"
    None => ""
  }
}

///|
/// True when (topic, partition) is in the member's assignment.
fn Consumer::is_assigned(self : Consumer, partition : Int) -> Bool {
  for pair in self.assignment {
    if pair.0 == self.topic && pair.1 == partition {
      return true
    }
  }
  false
}
