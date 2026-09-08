// Consumer-group partition assignors (Phase 4 classic path): pure
// functions from the group's subscriptions and the topics' partition
// counts to per-member assignments. Range and round-robin match the Java
// assignors' placement; sticky keeps prior assignments where balance
// allows (a deterministic greedy variant of Java's algorithm — same
// balance and stickiness properties, not the same tie-breaking order);
// cooperative-sticky adds the incremental revoke semantics on top.

///|
pub(all) enum Assignor {
  RangeAssignor
  RoundRobinAssignor
  StickyAssignor
  CooperativeStickyAssignor
} derive(@debug.Debug, Eq)

///|
/// The wire protocol names the brokers and Java clients use.
pub fn Assignor::name(self : Assignor) -> String {
  match self {
    RangeAssignor => "range"
    RoundRobinAssignor => "roundrobin"
    StickyAssignor => "sticky"
    CooperativeStickyAssignor => "cooperative-sticky"
  }
}

///|
/// One member's input to an assignment: its id and subscribed topics.
pub(all) struct Assignee {
  member_id : String
  topics : Array[String]
} derive(@debug.Debug)

///|
/// Compute a full assignment: member id -> (topic, partition) pairs.
/// `owned` carries the members' current assignments for stickiness
/// (empty for range/round-robin; cooperative revocation handled by the
/// caller).
pub fn compute_assignment(
  assignor : Assignor,
  members : Array[Assignee],
  topic_partitions : Map[String, Int],
  owned? : Map[String, Array[(String, Int)]] = Map([]),
) -> Map[String, Array[(String, Int)]] {
  let out : Map[String, Array[(String, Int)]] = Map([])
  for member in members {
    out[member.member_id] = []
  }
  match assignor {
    RangeAssignor => assign_range(members, topic_partitions, out)
    RoundRobinAssignor => assign_round_robin(members, topic_partitions, out)
    StickyAssignor | CooperativeStickyAssignor =>
      assign_sticky(members, topic_partitions, owned, out)
  }
  out
}

///|
/// Per-topic contiguous ranges: members sorted by id each take a
/// consecutive chunk of the partition range (Java's RangeAssignor).
fn assign_range(
  members : Array[Assignee],
  topic_partitions : Map[String, Int],
  out : Map[String, Array[(String, Int)]],
) -> Unit {
  let topics : Array[String] = []
  for topic, _ in topic_partitions {
    topics.push(topic)
  }
  topics.sort()
  for topic in topics {
    let count = topic_partitions.get(topic).unwrap_or(0)
    // Members subscribed to this topic, sorted by id.
    let subscribed : Array[String] = []
    for member in members {
      if member.topics.contains(topic) {
        subscribed.push(member.member_id)
      }
    }
    subscribed.sort()
    if subscribed.is_empty() {
      continue
    }
    let base = count / subscribed.length()
    let extra = count % subscribed.length()
    let mut next = 0
    for i in 0..<subscribed.length() {
      let take = base + (if i < extra { 1 } else { 0 })
      for _ in 0..<take {
        out[subscribed[i]].push((topic, next))
        next += 1
      }
    }
  }
}

///|
/// One circle over every subscribed (topic, partition) in sorted order,
/// handed to subscribed members in id order (Java's RoundRobinAssignor).
fn assign_round_robin(
  members : Array[Assignee],
  topic_partitions : Map[String, Int],
  out : Map[String, Array[(String, Int)]],
) -> Unit {
  let entries : Array[(String, Int)] = []
  let topics : Array[String] = []
  for topic, _ in topic_partitions {
    topics.push(topic)
  }
  topics.sort()
  for topic in topics {
    let count = topic_partitions.get(topic).unwrap_or(0)
    for p in 0..<count {
      entries.push((topic, p))
    }
  }
  let ordered : Array[String] = []
  for member in members {
    ordered.push(member.member_id)
  }
  ordered.sort()
  let subscribed = fn(member_id : String, topic : String) -> Bool {
    for member in members {
      if member.member_id == member_id && member.topics.contains(topic) {
        return true
      }
    }
    false
  }
  let mut turn = 0
  for entry in entries {
    let (topic, _) = entry
    let mut cursor = turn
    for ;; {
      let candidate = ordered[cursor % ordered.length()]
      if subscribed(candidate, topic) {
        out[candidate].push(entry)
        turn = cursor + 1
        break
      }
      cursor += 1
    }
  }
}

///|
/// Sticky assignment: keep prior assignments where the member is still
/// subscribed, then fill the unassigned partitions into the least-loaded
/// eligible members, moving from over-loaded members when needed. Same
/// balance/stickiness properties as Java's StickyAssignor, with
/// deterministic (id-ordered) tie-breaking instead of its random order.
fn assign_sticky(
  members : Array[Assignee],
  topic_partitions : Map[String, Int],
  owned : Map[String, Array[(String, Int)]],
  out : Map[String, Array[(String, Int)]],
) -> Unit {
  for member in members {
    match owned.get(member.member_id) {
      Some(pairs) =>
        for pair in pairs {
          let (topic, partition) = pair
          let exists = topic_partitions.get(topic).unwrap_or(0) > partition
          let subscribed = member.topics.contains(topic)
          // Keep the partition only when nobody claimed it yet.
          let mut claimed = false
          for _, mine in out {
            for pair2 in mine {
              if pair2.0 == topic && pair2.1 == partition {
                claimed = true
              }
            }
          }
          if exists && subscribed && !claimed {
            out[member.member_id].push(pair)
          }
        }
      None => ()
    }
  }
  // Every (topic, partition) with at least one subscribed member must
  // end up assigned: fill gaps greedily into the least-loaded member.
  let topics : Array[String] = []
  for topic, _ in topic_partitions {
    topics.push(topic)
  }
  topics.sort()
  for topic in topics {
    let count = topic_partitions.get(topic).unwrap_or(0)
    for partition in 0..<count {
      let mut taker : String? = None
      for member in members {
        if !member.topics.contains(topic) {
          continue
        }
        let has = any_pair(out[member.member_id], topic, partition)
        if has {
          taker = Some(member.member_id)
          break
        }
      }
      match taker {
        Some(_) => ()
        None => {
          // Least-loaded subscribed member, id-ordered tie-break.
          let mut best : String? = None
          let mut best_load = 0
          for member in members {
            if !member.topics.contains(topic) {
              continue
            }
            let load = out[member.member_id].length()
            match best {
              Some(_) =>
                if load < best_load ||
                  (load == best_load && member.member_id < best.unwrap_or("")) {
                  best = Some(member.member_id)
                  best_load = load
                }
              None => {
                best = Some(member.member_id)
                best_load = load
              }
            }
          }
          match best {
            Some(member_id) => out[member_id].push((topic, partition))
            None => ()
          }
        }
      }
    }
  }
  balance_sticky(members, topic_partitions, out)
}

///|
/// Move partitions from over-loaded members to the least-loaded eligible
/// member until no move improves the spread (Java's sticky assignor
/// balances first; our pass runs after the keep+fill instead).
fn balance_sticky(
  members : Array[Assignee],
  topic_partitions : Map[String, Int],
  out : Map[String, Array[(String, Int)]],
) -> Unit {
  let total : Int = {
    let mut n = 0
    for _, pairs in out {
      n += pairs.length()
    }
    n
  }
  let count = members.length()
  if count < 2 || total == 0 {
    return
  }
  if count < 2 || total == 0 {
    return
  }
  for ;; {
    // Most-loaded member (id-ordered tie-break).
    let mut heaviest : String? = None
    let mut heavy_load = 0
    for member in members {
      let load = out[member.member_id].length()
      match heaviest {
        Some(_) =>
          if load > heavy_load ||
            (load == heavy_load && member.member_id < heaviest.unwrap_or("")) {
            heaviest = Some(member.member_id)
            heavy_load = load
          }
        None => {
          heaviest = Some(member.member_id)
          heavy_load = load
        }
      }
    }
    guard heaviest is Some(from) else { break }
    // The lightest member overall: stop once the spread is minimal.
    let mut lightest = heavy_load
    for member in members {
      let load = out[member.member_id].length()
      if load < lightest {
        lightest = load
      }
    }
    if heavy_load - lightest <= 1 {
      break
    }
    ignore(heavy_load)
    // The least-loaded subscribed member that may take one partition.
    let pairs = out[from]
    if pairs.is_empty() {
      break
    }
    // Give away the newest partition the lightest eligible member wants.
    let mut moved = false
    let mut i = pairs.length() - 1
    for ;; {
      if i < 0 {
        break
      }
      let pair = pairs[i]
      // Lightest member subscribed to this topic (excluding taker).
      let mut best : String? = None
      let mut best_load = 0
      for member in members {
        if member.member_id == from || !member.topics.contains(pair.0) {
          continue
        }
        let load = out[member.member_id].length()
        match best {
          Some(_) =>
            if load < best_load ||
              (load == best_load && member.member_id < best.unwrap_or("")) {
              best = Some(member.member_id)
              best_load = load
            }
          None => {
            best = Some(member.member_id)
            best_load = load
          }
        }
      }
      match best {
        Some(to) =>
          if best_load + 2 <= heavy_load {
            let removed = pairs.remove(i)
            out[to].push(removed)
            moved = true
            break
          } else {
            // No move improves the spread.
            break
          }
        None => i -= 1
      }
    }
    if !moved {
      break
    }
  }
}

///|
/// True when (topic, partition) sits in the pair list.
fn any_pair(
  pairs : Array[(String, Int)],
  topic : String,
  partition : Int,
) -> Bool {
  for pair in pairs {
    if pair.0 == topic && pair.1 == partition {
      return true
    }
  }
  false
}
