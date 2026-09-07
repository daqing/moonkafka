// The consumer's public surface helpers (Phase 4): manual assignment,
// position reads, and group metadata introspection.

///|
/// The consumer group's identity as this member sees it: None for a
/// group-less consumer; an empty member id and generation -1 before the
/// first join.
pub(all) struct GroupMetadata {
  group_id : String
  member_id : String
  generation_id : Int
} derive(@debug.Debug)

///|
/// Manually assign topic partitions (the subscribe-less path): the
/// consumer fetches exactly these partitions of its topic without group
/// coordination. Positions start from the committed offsets when a
/// group_id is configured, else from the auto-offset-reset sentinel.
/// Mutually exclusive with subscribe().
pub async fn Consumer::assign(
  self : Consumer,
  partitions : Array[(String, Int)],
) -> Unit {
  if self.member_active {
    raise ProtocolError::ProtocolError(
      "assign() is mutually exclusive with subscribe()",
    )
  }
  let known : Array[Int] = []
  for pair in partitions {
    let (topic, partition) = pair
    if topic != self.topic {
      raise ProtocolError::ProtocolError(
        "assign: topic \\{topic} is outside this consumer's topic (\\{self.topic})",
      )
    }
    known.push(partition)
  }
  self.manual_assignment = Some(partitions)
  // Resolve positions: committed offsets first, then the reset policy.
  let committed : Map[Int, Int64] = Map([])
  if self.group_id is Some(_) {
    try {
      let map = self.committed(partitions=Some(known))
      for partition, offset in map {
        committed[partition] = offset
      }
    } catch {
      _ => ()
    }
  }
  for p in self.partitions {
    if !manual_has(partitions, self.topic, p.info.index) {
      continue
    }
    match committed.get(p.info.index) {
      Some(offset) =>
        if offset >= 0L {
          p.next_offset = offset
          continue
        }
      None => ()
    }
    // No committed offset: fall back to the reset policy.
    let sentinel = match self.auto_offset_reset {
      ResetEarliest => OFFSET_EARLIEST
      ResetLatest => OFFSET_LATEST
      ResetNone => continue
    }
    let offsets = self.cluster
      .control_conn()
      .list_offsets(
        self.topic,
        [p.info],
        sentinel,
        timeout_ms=self.request_timeout_ms,
      )
    match offsets.get(p.info.index) {
      Some(offset) => p.next_offset = offset
      None => ()
    }
  }
}

///|
/// Drop the manual assignment; poll stops fetching until a new one is
/// set.
pub fn Consumer::unassign(self : Consumer) -> Unit {
  self.manual_assignment = None
}

///|
/// The current read position of one partition, if it is part of the
/// consumer's metadata view.
pub fn Consumer::position(self : Consumer, partition : Int) -> Int64? {
  for p in self.partitions {
    if p.info.index == partition {
      return Some(p.next_offset)
    }
  }
  None
}

///|
/// The group identity this consumer participates in, if configured.
pub fn Consumer::group_metadata(self : Consumer) -> GroupMetadata? {
  match self.group_id {
    Some(group_id) =>
      Some({
        group_id,
        member_id: self.member_id,
        generation_id: self.generation_id,
      })
    None => None
  }
}
