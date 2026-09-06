// The transaction manager (Phase 3): begin/commit/abort around the
// producer's drain loop, transactional offset commits, and the
// AddPartitionsToTxn bookkeeping the sender performs before producing to
// a partition. State lives on the Producer; the coordinator connection
// resolves through the cluster client and is cached until a
// NOT_COORDINATOR moves it.
//
// Error policy (mirroring the Java client's abortable model): coordinator
// hiccups (loading/unavailable/moved/concurrent) retry with backoff;
// fencing (INVALID_PRODUCER_EPOCH / PRODUCER_FENCED) and other broker
// errors raise. A terminal send failure inside an open transaction does
// not raise from the transaction itself — commit_transaction refuses to
// commit and the caller aborts (abort-on-error).

///|
/// Start a transaction: records appended after this point are stamped
/// into batches the sender routes through AddPartitionsToTxn, and
/// commit_transaction makes them visible atomically.
pub async fn Producer::begin_transaction(self : Producer) -> Unit {
  self.guard_transactional()
  if self.closed {
    raise ProtocolError::ProtocolError("producer is closed")
  }
  if self.in_transaction {
    raise ProtocolError::ProtocolError("a transaction is already in progress")
  }
  self.in_transaction = true
  self.txn_added = Map([])
  self.txn_failed_base = self.accumulator.failed_count()
}

///|
/// Commit the ongoing transaction: wait for every appended record to
/// reach the log, then ask the coordinator to commit. Raises when any
/// record failed since begin (abort instead) or the coordinator rejects
/// the commit; a successful EndTxn v5 adopts the coordinator-bumped
/// epoch (KIP-890 part 2) and continues the sequence numbers.
pub async fn Producer::commit_transaction(self : Producer) -> Unit {
  self.guard_transactional()
  if !self.in_transaction {
    raise ProtocolError::ProtocolError("no transaction in progress to commit")
  }
  self.flush_transaction()
  // The abort-on-error policy: a failed release inside the transaction
  // leaves records' fate unknown to the coordinator — refuse the commit.
  if self.accumulator.failed_count() > self.txn_failed_base {
    raise ProtocolError::ProtocolError(
      "records failed during the transaction; abort_transaction is required",
    )
  }
  let id = self.transactional_id.unwrap_or("")
  let result = self.end_txn_with_retries(id, true)
  self.finish_transaction(result)
}

///|
/// Abort the ongoing transaction: drain what is queued, then ask the
/// coordinator to abort, discarding the records appended since begin.
pub async fn Producer::abort_transaction(self : Producer) -> Unit {
  self.guard_transactional()
  if !self.in_transaction {
    raise ProtocolError::ProtocolError("no transaction in progress to abort")
  }
  self.flush_transaction()
  let id = self.transactional_id.unwrap_or("")
  let result = self.end_txn_with_retries(id, false)
  self.finish_transaction(result)
}

///|
/// Commit offsets collected by a consumer to `group_id` inside the
/// ongoing transaction: AddOffsetsToTxn registers the group with the
/// transaction coordinator, then TxnOffsetCommit lands the offsets at
/// the group coordinator. They become visible with the transaction's
/// commit, not before.
pub async fn Producer::send_offsets_to_transaction(
  self : Producer,
  offsets : Array[(String, Array[TxnOffset])],
  group_id : String,
) -> Unit {
  self.guard_transactional()
  if !self.in_transaction {
    raise ProtocolError::ProtocolError(
      "send_offsets_to_transaction requires an active transaction",
    )
  }
  if offsets.is_empty() {
    return
  }
  let id = self.transactional_id.unwrap_or("")
  // Register the group at the transaction coordinator (retried).
  let backoff = Backoff::new(
    base_ms=self.retry_backoff_ms,
    max_ms=self.retry_backoff_max_ms,
  )
  let mut registered = false
  for attempt in 0..<=self.retries {
    let conn = self.txn_coordinator_conn()
    let code = conn.add_offsets_to_txn(
      id,
      self.producer_id,
      self.producer_epoch,
      group_id,
      timeout_ms=self.timeout_ms,
    )
    if code == 0 {
      registered = true
      break
    }
    classify_txn_code(code)
    if attempt < self.retries {
      if code == 16 {
        self.txn_coordinator = None
      }
      @async.sleep(backoff.next_ms())
    }
  }
  if !registered {
    raise ProtocolError::ProtocolError(
      "AddOffsetsToTxn gave up after \{self.retries} retries",
    )
  }
  // Commit the offsets at the group coordinator (resolved per call, so
  // NOT_COORDINATOR heals on the next attempt without caching).
  for attempt in 0..<=self.retries {
    let conn = self.group_coordinator_conn(group_id)
    let results = conn.txn_offset_commit(
      id,
      group_id,
      self.producer_id,
      self.producer_epoch,
      offsets,
      timeout_ms=self.timeout_ms,
    )
    let mut code = 0
    for result in results {
      if result.error_code != 0 {
        code = result.error_code
      }
    }
    if code == 0 {
      return
    }
    classify_txn_code(code)
    if attempt < self.retries {
      @async.sleep(backoff.next_ms())
    }
  }
  raise ProtocolError::ProtocolError(
    "TxnOffsetCommit gave up after \{self.retries} retries",
  )
}

///|
/// Connect-time InitProducerId for a transactional producer: resolve the
/// coordinator, register the transaction timeout, and adopt the assigned
/// identity. Coordinator hiccups retry with backoff.
async fn Producer::init_transactional(
  self : Producer,
  id : String,
  transaction_timeout_ms : Int,
) -> Unit {
  let backoff = Backoff::new(
    base_ms=self.retry_backoff_ms,
    max_ms=self.retry_backoff_max_ms,
  )
  for attempt in 0..<=self.retries {
    let conn = self.txn_coordinator_conn()
    let result = conn.init_producer_id_txn(
      id,
      transaction_timeout_ms,
      self.timeout_ms,
    )
    if result.error_code == 0 {
      self.producer_id = result.producer_id
      self.producer_epoch = result.producer_epoch
      return
    }
    if !txn_retriable_code(result.error_code) {
      raise ProtocolError::ProtocolError(
        "InitProducerId failed: \{error_name(result.error_code)}",
      )
    }
    if attempt < self.retries {
      if result.error_code == 16 {
        self.txn_coordinator = None
      }
      @async.sleep(backoff.next_ms())
    }
  }
  raise ProtocolError::ProtocolError(
    "InitProducerId gave up after \{self.retries} retries",
  )
}

///|
/// Common guard: transactional operations on a non-transactional
/// producer are a programming error.
fn Producer::guard_transactional(self : Producer) -> Unit raise {
  if self.transactional_id is None {
    raise ProtocolError::ProtocolError(
      "the producer was not configured with a transactional_id",
    )
  }
}

///|
/// Reset per-transaction state and adopt the coordinator's post-EndTxn
/// identity (v5 bumps the epoch on every transaction; sequence numbers
/// continue under KIP-890 part 2 state preservation).
fn Producer::finish_transaction(self : Producer, result : EndTxnResult) -> Unit {
  self.in_transaction = false
  self.txn_added = Map([])
  if result.producer_epoch >= 0 {
    self.producer_id = result.producer_id
    self.producer_epoch = result.producer_epoch
  }
}

///|
/// EndTxn with the coordinator-retry loop shared by commit and abort.
async fn Producer::end_txn_with_retries(
  self : Producer,
  transactional_id : String,
  committed : Bool,
) -> EndTxnResult {
  let backoff = Backoff::new(
    base_ms=self.retry_backoff_ms,
    max_ms=self.retry_backoff_max_ms,
  )
  let verb = if committed { "commit" } else { "abort" }
  for attempt in 0..<=self.retries {
    let conn = self.txn_coordinator_conn()
    let result = conn.end_txn(
      transactional_id,
      self.producer_id,
      self.producer_epoch,
      committed,
      timeout_ms=self.timeout_ms,
    )
    if result.error_code == 0 {
      return result
    }
    classify_txn_code(result.error_code)
    if attempt < self.retries {
      if result.error_code == 16 {
        self.txn_coordinator = None
      }
      @async.sleep(backoff.next_ms())
    }
  }
  raise ProtocolError::ProtocolError(
    "transaction \{verb} gave up after \{self.retries} retries",
  )
}

///|
/// Classify a broker error from a coordinator call: retriable codes
/// return for the retry loop, fenced codes and everything else raise.
fn classify_txn_code(code : Int) -> Unit raise {
  if txn_retriable_code(code) {
    return
  }
  if txn_fenced_code(code) {
    raise ProtocolError::ProtocolError(
      "the transactional producer was fenced: \{error_name(code)}",
    )
  }
  raise ProtocolError::ProtocolError(
    "transaction coordinator call failed: \{error_name(code)}",
  )
}

///|
/// The transaction coordinator's pooled connection, resolving (and
/// caching) the coordinator node on first use.
async fn Producer::txn_coordinator_conn(self : Producer) -> BrokerConnection {
  let node = match self.txn_coordinator {
    Some(node) => node
    None => self.resolve_txn_coordinator()
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
/// FindCoordinator(Transaction) with retries while the coordinator
/// loads; the result node id is cached until NOT_COORDINATOR.
async fn Producer::resolve_txn_coordinator(self : Producer) -> Int {
  guard self.transactional_id is Some(id) else {
    raise ProtocolError::ProtocolError("no transactional id configured")
  }
  let backoff = Backoff::new(
    base_ms=self.retry_backoff_ms,
    max_ms=self.retry_backoff_max_ms,
  )
  for attempt in 0..<=self.retries {
    let infos = self.cluster.coordinator([id], Transaction)
    match infos.get(id) {
      Some(info) =>
        if info.error_code == 0 {
          self.txn_coordinator = Some(info.node_id)
          return info.node_id
        } else if txn_retriable_code(info.error_code) && attempt < self.retries {
          @async.sleep(backoff.next_ms())
        } else {
          raise ProtocolError::ProtocolError(
            "FindCoordinator(\{id}) failed: \{error_name(info.error_code)}",
          )
        }
      None =>
        raise ProtocolError::ProtocolError(
          "FindCoordinator(\{id}) returned no entry",
        )
    }
  }
  raise ProtocolError::ProtocolError(
    "FindCoordinator(\{id}) gave up after \{self.retries} retries",
  )
}

///|
/// The group coordinator's pooled connection for `group_id`, resolved
/// fresh on every call (group coordinators move; transactional offset
/// commits are rare).
async fn Producer::group_coordinator_conn(
  self : Producer,
  group_id : String,
) -> BrokerConnection {
  let backoff = Backoff::new(
    base_ms=self.retry_backoff_ms,
    max_ms=self.retry_backoff_max_ms,
  )
  for attempt in 0..<=self.retries {
    let infos = self.cluster.coordinator([group_id], Group)
    match infos.get(group_id) {
      Some(info) =>
        if info.error_code == 0 {
          return self.cluster.connection(info.node_id) catch {
            _ => {
              self.cluster.refresh_metadata(None)
              self.cluster.connection(info.node_id)
            }
          }
        } else if txn_retriable_code(info.error_code) && attempt < self.retries {
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
  raise ProtocolError::ProtocolError(
    "FindCoordinator(\{group_id}) gave up after \{self.retries} retries",
  )
}

///|
/// Wait until every appended record reached a terminal result — the
/// precondition for commit/abort, matching the Java client's flush.
/// Bounded by the delivery window plus a margin for the drain ticks.
async fn Producer::flush_transaction(self : Producer) -> Unit {
  let deadline = @async.now() + self.delivery_timeout_ms.to_int64() + 10000L
  for ;; {
    if self.accumulator.outstanding() == 0 {
      return
    }
    if @async.now() > deadline {
      raise ProtocolError::ProtocolError(
        "timed out waiting for queued records before ending the transaction",
      )
    }
    @async.sleep(SENDER_TICK_MS)
  }
}

///|
/// AddPartitionsToTxn for the sender's round: registers the round's
/// not-yet-added partitions with the transaction coordinator (its own
/// connection, cached) before the Produce goes out — the broker rejects
/// produces to partitions outside the transaction. Retriable coordinator
/// errors requeue the round.
async fn Producer::add_round_partitions(
  self : Producer,
  entries : Array[(Int, ProducerBatch)],
) -> SendRound {
  let need : Array[Int] = []
  for entry in entries {
    if !self.txn_added.get(entry.0).unwrap_or(false) {
      need.push(entry.0)
    }
  }
  if need.is_empty() {
    return RoundOk
  }
  let id = self.transactional_id.unwrap_or("")
  let conn = self.txn_coordinator_conn()
  let results = conn.add_partitions_to_txn(
    id,
    self.producer_id,
    self.producer_epoch,
    [(self.topic, need)],
    timeout_ms=self.timeout_ms,
  )
  let mut code = 0
  for result in results {
    if result.error_code != 0 {
      code = result.error_code
    } else {
      self.txn_added[result.partition] = true
    }
  }
  if code == 0 {
    return RoundOk
  }
  if code == 16 {
    self.txn_coordinator = None
  }
  if txn_fenced_code(code) || !txn_retriable_code(code) {
    // The transaction is fenced or broken: fail the round's batches
    // now (their release poisons the commit) instead of retrying into
    // a dead transaction.
    for entry in entries {
      self.rewind_sequence(entry.1)
      self.accumulator.release(
        entry.1,
        BatchError(
          "AddPartitionsToTxn failed for partition \{entry.0}: \{error_name(code)}",
        ),
      )
    }
    return RoundOk
  }
  RoundRetry
}
