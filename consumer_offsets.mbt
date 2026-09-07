// Consumer offset management (Phase 4): committed-offset tracking on the
// group coordinator (OffsetCommit/OffsetFetch), the seek family over
// ListOffsets, and the autocommit background loop. Commit semantics
// mirror the Java client: COORDINATOR_LOAD_IN_PROGRESS /
// COORDINATOR_NOT_AVAILABLE / NOT_COORDINATOR / REBALANCE_IN_PROGRESS
// (transient while the group rebalances) retry with backoff; member
// identity errors (UNKNOWN_MEMBER_ID, ILLEGAL_GENERATION, FENCED_) raise
// for the group layer to handle.

///|
/// Commit error codes worth retrying: the coordinator is loading (14),
/// unavailable (15), moved (16), or a rebalance is in flight (27) — the
/// Java client re-issues the commit in all four cases.
fn commit_retriable(code : Int) -> Bool {
  code == 14 || code == 15 || code == 16 || code == 27
}

///|
/// The group coordinator's pooled connection, resolving (and caching)
/// the coordinator node on first use. Requires a group id.
async fn Consumer::group_coordinator_conn(self : Consumer) -> BrokerConnection {
  guard self.group_id is Some(group_id) else {
    raise ProtocolError::ProtocolError(
      "the consumer was not configured with a group_id",
    )
  }
  let node = match self.group_coordinator {
    Some(node) => node
    None => {
      let backoff = Backoff::new(
        base_ms=self.retry_backoff_ms,
        max_ms=self.retry_backoff_max_ms,
      )
      let mut found = -1
      for attempt in 0..<=self.retries {
        let infos = self.cluster.coordinator([group_id], Group)
        match infos.get(group_id) {
          Some(info) =>
            if info.error_code == 0 {
              found = info.node_id
              self.group_coordinator = Some(info.node_id)
              break
            } else if commit_retriable(info.error_code) &&
              attempt < self.retries {
              @async.sleep(backoff.next_ms())
            } else {
              raise ProtocolError::ProtocolError(
                "FindCoordinator(\{group_id}) failed: \{error_name(info.error_code)}",
              )
            }
          None =>
            raise ProtocolError::ProtocolError(
              "FindCoordinator(\{group_id}) returned no entry",
            )
        }
      }
      if found < 0 {
        raise ProtocolError::ProtocolError(
          "FindCoordinator(\{group_id}) gave up after \{self.retries} retries",
        )
      }
      found
    }
  }
  self.cluster.connection(node) catch {
    _ => {
      // The coordinator node may not be in the cached metadata yet.
      self.cluster.refresh_metadata(None)
      self.cluster.connection(node)
    }
  }
}

///|
/// Snapshot the current read positions per partition.
fn Consumer::positions(self : Consumer) -> Map[Int, Int64] {
  let out : Map[Int, Int64] = Map([])
  for p in self.partitions {
    out[p.info.index] = p.next_offset
  }
  out
}

///|
/// Commit offsets to the group coordinator: `offsets` explicitly, or
/// every partition's current position when omitted. Retriable
/// coordinator/rebalance errors retry with backoff; success refreshes
/// the committed-offset cache.
pub async fn Consumer::commit(
  self : Consumer,
  offsets? : Map[Int, Int64],
) -> Unit {
  guard self.group_id is Some(group_id) else {
    raise ProtocolError::ProtocolError(
      "commit requires a group_id on the consumer config",
    )
  }
  let to_commit = match offsets {
    Some(map) => map
    None => self.positions()
  }
  if to_commit.is_empty() {
    return
  }
  let entries : Array[(Int, Int64, Int)] = []
  for p in self.partitions {
    match to_commit.get(p.info.index) {
      Some(offset) => entries.push((p.info.index, offset, p.info.leader_epoch))
      None => ()
    }
  }
  let backoff = Backoff::new(
    base_ms=self.retry_backoff_ms,
    max_ms=self.retry_backoff_max_ms,
  )
  for attempt in 0..<=self.retries {
    let conn = self.group_coordinator_conn()
    let results = conn.offset_commit(
      group_id,
      -1, // generation / member epoch: group-less commit
      "",
      topics=[
        { name: self.topic, topic_id: self.topic_id, partitions: entries, },
      ],
      timeout_ms=self.request_timeout_ms,
    )
    let mut code = 0
    for result in results {
      if result.error_code != 0 {
        code = result.error_code
      }
    }
    if code == 0 {
      for entry in entries {
        self.committed_offsets[entry.0] = entry.1
      }
      return
    }
    if !commit_retriable(code) {
      raise ProtocolError::ProtocolError(
        "OffsetCommit failed: \{error_name(code)}",
      )
    }
    if code == 16 {
      self.group_coordinator = None
    }
    if attempt < self.retries {
      @async.sleep(backoff.next_ms())
    }
  }
  raise ProtocolError::ProtocolError(
    "OffsetCommit gave up after \{self.retries} retries",
  )
}

///|
/// Commit in the background: the commit runs in the consumer's task
/// group and `on_complete` receives None on success or the error
/// message. Nothing is awaited.
pub fn Consumer::commit_async(
  self : Consumer,
  offsets? : Map[Int, Int64],
  on_complete? : (String?) -> Unit = fn(_msg) { () },
) -> Unit raise {
  guard self.group_id is Some(_) else {
    raise ProtocolError::ProtocolError(
      "commit_async requires a group_id on the consumer config",
    )
  }
  self.group.spawn_bg(no_wait=false, allow_failure=true, () => {
    let mut failed : String? = None
    try {
      match offsets {
        Some(map) => self.commit(offsets=map)
        None => self.commit()
      }
    } catch {
      e => failed = Some("\{e}")
    }
    match failed {
      Some(msg) => on_complete(Some(msg))
      None => on_complete(None)
    }
  })
}

///|
/// Fetch the last committed offsets for the topic's partitions (or the
/// given ones) from the coordinator, refresh the cache, and return the
/// map. Partitions without a commit carry -1.
pub async fn Consumer::committed(
  self : Consumer,
  partitions? : Array[Int]? = None,
) -> Map[Int, Int64] {
  guard self.group_id is Some(group_id) else {
    raise ProtocolError::ProtocolError(
      "committed() requires a group_id on the consumer config",
    )
  }
  let wanted = match partitions {
    Some(list) => list
    None => self.partitions.map(fn(p) { p.info.index })
  }
  if wanted.is_empty() {
    return Map([])
  }
  let by_index : Map[Int, PartitionInfo] = Map([])
  for p in self.partitions {
    by_index[p.info.index] = p.info
  }
  let backoff = Backoff::new(
    base_ms=self.retry_backoff_ms,
    max_ms=self.retry_backoff_max_ms,
  )
  for attempt in 0..<=self.retries {
    let conn = self.group_coordinator_conn()
    let groups = conn.offset_fetch(
      group_id,
      topics=[
        { name: self.topic, topic_id: self.topic_id, partitions: wanted, },
      ],
      timeout_ms=self.request_timeout_ms,
    )
    let mut code = 0
    let out : Map[Int, Int64] = Map([])
    for group in groups {
      if group.error_code != 0 {
        code = group.error_code
      }
      for partition in group.partitions {
        if partition.error_code != 0 {
          code = partition.error_code
        }
        out[partition.partition] = partition.offset
      }
    }
    if code == 0 {
      for partition, offset in out {
        self.committed_offsets[partition] = offset
      }
      return out
    }
    if !commit_retriable(code) {
      raise ProtocolError::ProtocolError(
        "OffsetFetch failed: \{error_name(code)}",
      )
    }
    if code == 16 {
      self.group_coordinator = None
    }
    if attempt < self.retries {
      @async.sleep(backoff.next_ms())
    }
  }
  raise ProtocolError::ProtocolError(
    "OffsetFetch gave up after \{self.retries} retries",
  )
}

///|
/// The cached committed offset for one partition, if known locally.
/// Call `committed()` to refresh from the coordinator.
pub fn Consumer::committed_cached(self : Consumer, partition : Int) -> Int64? {
  self.committed_offsets.get(partition)
}

///|
/// Move the read position of one partition to `offset` (no validation
/// against the log; the next poll surfaces OFFSET_OUT_OF_RANGE if it is
/// outside).
pub fn Consumer::seek(
  self : Consumer,
  partition : Int,
  offset : Int64,
) -> Unit raise {
  let mut known = false
  for p in self.partitions {
    if p.info.index == partition {
      known = true
    }
  }
  guard known else {
    raise ProtocolError::ProtocolError(
      "seek: partition \{partition} is not part of the assignment",
    )
  }
  self.set_next_offset(partition, offset)
}

///|
/// Resolve the given partitions (default: all) to the log start offset.
pub async fn Consumer::seek_to_beginning(
  self : Consumer,
  partitions? : Array[Int]? = None,
) -> Unit {
  self.seek_to_sentinel(partitions, OFFSET_EARLIEST)
}

///|
/// Resolve the given partitions (default: all) to the log end offset.
pub async fn Consumer::seek_to_end(
  self : Consumer,
  partitions? : Array[Int]? = None,
) -> Unit {
  self.seek_to_sentinel(partitions, OFFSET_LATEST)
}

///|
async fn Consumer::seek_to_sentinel(
  self : Consumer,
  partitions : Array[Int]?,
  sentinel : Int64,
) -> Unit {
  let wanted = self.seek_partitions(partitions)
  if wanted.is_empty() {
    return
  }
  let infos = wanted.map(fn(p) { p.info })
  let offsets = self.cluster
    .control_conn()
    .list_offsets(
      self.topic,
      infos,
      sentinel,
      timeout_ms=self.request_timeout_ms,
    )
  for p in wanted {
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
/// Move one partition's read position to the offset of the first record
/// whose timestamp is >= `timestamp` (ListOffsets by timestamp).
pub async fn Consumer::seek_by_timestamp(
  self : Consumer,
  partition : Int,
  timestamp : Int64,
) -> Unit {
  let wanted = self.seek_partitions(Some([partition]))
  guard wanted.length() == 1 else {
    raise ProtocolError::ProtocolError(
      "seek_by_timestamp: partition \{partition} is not part of the assignment",
    )
  }
  let offsets = self.cluster
    .control_conn()
    .list_offsets(
      self.topic,
      [wanted[0].info],
      timestamp,
      timeout_ms=self.request_timeout_ms,
    )
  match offsets.get(partition) {
    Some(offset) => wanted[0].next_offset = offset
    None =>
      raise ProtocolError::ProtocolError(
        "ListOffsets missing partition \{partition}",
      )
  }
}

///|
fn Consumer::seek_partitions(
  self : Consumer,
  partitions : Array[Int]?,
) -> Array[PartitionState] {
  match partitions {
    None => self.partitions.copy()
    Some(list) => {
      let out : Array[PartitionState] = []
      for p in self.partitions {
        let mut wanted = false
        for i in list {
          if i == p.info.index {
            wanted = true
          }
        }
        if wanted {
          out.push(p)
        }
      }
      out
    }
  }
}

///|
/// The autocommit loop: commit the current positions every
/// auto_commit_interval_ms and once more on close, then tear the
/// cluster down before the group joins this task.
async fn Consumer::autocommit_loop(self : Consumer) -> Unit {
  for ;; {
    // Poll the closed flag at most once per interval; a finer wakeup
    // lands with the D6 background-task polish.
    for _ in 0..<Int::max(1, self.auto_commit_interval_ms / SENDER_TICK_MS) {
      if self.closed {
        break
      }
      @async.sleep(SENDER_TICK_MS)
    }
    if self.closed || !self.autocommit_enabled() {
      break
    }
    let _ = self.commit() catch { _ => () }
  }
  // Commit-on-close: best effort, then own the cluster teardown.
  if self.autocommit_enabled() {
    let _ = self.commit() catch { _ => () }
  }
  self.cluster.close()
}
