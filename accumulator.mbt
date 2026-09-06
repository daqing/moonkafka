// The producer's record accumulator: per-partition queues of in-flight
// batches with batch_size / linger_ms / buffer_memory accounting. Records
// land in the partition's open batch until it is full (batch_size); the
// linger window lets concurrent senders coalesce. One flusher claims each
// batch, puts it on the wire, and publishes the broker's base offset, from
// which every coalesced record derives its own offset (base + index).
//
// Root scope for now — folds into the producer/ package split with the
// rest of the sender work.

///|
/// Terminal outcome of one produce batch. Coalesced senders wait on the
/// batch's done semaphore and read this.
priv enum BatchResult {
  BatchOk(Int64) // base offset assigned by the broker
  BatchError(String)
}

///|

///|
priv struct ProducerBatch {
  /// Unique within its accumulator; identity for queue-order comparisons.
  id : Int
  partition : Int
  builder : RecordBatchBuilder
  created_ms : Int64
  /// Closed batches accept no more records and are ready to send.
  mut closed : Bool
  /// Send attempts so far, reported in delivery-timeout errors.
  mut attempts : Int
  /// Set when the sticky boundary for this batch already fired (append
  /// closed it by rotation); the sender fires it otherwise.
  mut boundary_fired : Bool
  /// Estimated wire size; also the batch's share of buffer_memory.
  mut size_estimate : Int
  mut result : BatchResult?
  /// One semaphore per waiting sender, registered under the accumulator
  /// lock; release() wakes every waiter exactly once.
  waiters : Array[@async.Semaphore]
}

///|
/// When the batch becomes ready under the linger setting.
fn ProducerBatch::ready_at(self : ProducerBatch, linger_ms : Int) -> Int64 {
  self.created_ms + linger_ms.to_int64()
}

///|
priv struct RecordAccumulator {
  batch_size : Int
  buffer_memory : Int
  linger_ms : Int
  lock : @async.Mutex
  /// Per-partition queues; the last batch of a queue is the open one.
  batches : Map[Int, Array[ProducerBatch]]
  /// Sum of queued batches' size estimates, capped by buffer_memory.
  mut pool_bytes : Int
  mut next_id : Int
}

///|
fn RecordAccumulator::new(
  batch_size : Int,
  buffer_memory : Int,
  linger_ms : Int,
) -> RecordAccumulator {
  {
    batch_size,
    buffer_memory,
    linger_ms,
    lock: @async.Mutex(),
    batches: Map([]),
    pool_bytes: 0,
    next_id: 0,
  }
}

///|
fn RecordAccumulator::queue(
  self : RecordAccumulator,
  partition : Int,
) -> Array[ProducerBatch] {
  match self.batches.get(partition) {
    Some(q) => q
    None => {
      let q : Array[ProducerBatch] = []
      self.batches[partition] = q
      q
    }
  }
}

///|
/// Append one record to the partition's open batch, closing it and
/// rotating to a fresh batch when the record does not fit (batch_size).
/// Returns the batch, the record's index within it (its offset is the
/// batch's base offset plus the index), and whether a full batch was
/// closed — the sticky partitioner's boundary. A record larger than
/// batch_size goes out alone in an oversized batch; there is no room for
/// a new batch within buffer_memory, BufferExhausted is raised.
async fn RecordAccumulator::append(
  self : RecordAccumulator,
  now_ms : Int64,
  partition : Int,
  timestamp : Int64,
  key : Bytes?,
  value : Bytes?,
  headers : Array[(Bytes, Bytes)],
) -> (ProducerBatch, Int, Bool) {
  self.lock.acquire()
  defer self.lock.release()
  let queue = self.queue(partition)
  let open = match queue.last() {
    Some(b) => if b.closed { None } else { Some(b) }
    None => None
  }
  let rec_size = match open {
    Some(b) =>
      b.builder.estimated_append_size(timestamp, key~, value~, headers~)
    None => {
      let probe = RecordBatchBuilder::new()
      probe.estimated_append_size(timestamp, key~, value~, headers~)
    }
  }
  match open {
    Some(b) =>
      if b.builder.estimated_size() + rec_size <= self.batch_size {
        b.builder.append(timestamp, key~, value~, headers~)
        b.size_estimate += rec_size
        self.pool_bytes += rec_size
        return (b, b.builder.count() - 1, false)
      }
    None => ()
  }
  // Rotate: the open batch cannot fit the record, so it is full.
  let rotated = open is Some(_)
  match open {
    Some(b) => {
      b.closed = true
      b.boundary_fired = true
    }
    None => ()
  }
  let fresh = RECORD_BATCH_HEADER_SIZE + rec_size
  if self.pool_bytes + fresh > self.buffer_memory {
    raise ProtocolError::BufferExhausted(
      "buffer_memory \{self.buffer_memory} exhausted (\{self.pool_bytes} bytes queued, \{fresh} more needed for partition \{partition})",
    )
  }
  self.next_id += 1
  let id = self.next_id
  let batch : ProducerBatch = {
    id,
    partition,
    builder: RecordBatchBuilder::new(),
    created_ms: now_ms,
    closed: false,
    attempts: 0,
    boundary_fired: false,
    size_estimate: 0,
    result: None,
    waiters: [],
  }
  batch.builder.append(timestamp, key~, value~, headers~)
  batch.size_estimate = fresh
  self.pool_bytes += fresh
  queue.push(batch)
  (batch, 0, rotated)
}

///|
/// Force-close every open batch: the producer is closing and its sender
/// must drain and resolve everything still queued.
async fn RecordAccumulator::force_close_all(self : RecordAccumulator) -> Unit {
  self.lock.acquire()
  defer self.lock.release()
  for _, queue in self.batches {
    for batch in queue {
      batch.closed = true
    }
  }
}

///|
/// Pop the front batch of every partition whose head is ready: closed
/// (full or force-closed) or past its linger deadline. Batches past
/// delivery_timeout are released with an error instead of being handed
/// out. At most one batch per partition per round keeps per-partition
/// order trivially; requeued batches return to the front.
async fn RecordAccumulator::drain_ready(
  self : RecordAccumulator,
  now_ms : Int64,
  delivery_timeout_ms : Int,
) -> Array[ProducerBatch] {
  self.lock.acquire()
  defer self.lock.release()
  let out : Array[ProducerBatch] = []
  for _, queue in self.batches {
    if queue.is_empty() {
      continue
    }
    let batch = queue[0]
    if now_ms >= batch.created_ms + delivery_timeout_ms.to_int64() {
      // The batch never reached the wire in time: expire it.
      ignore(queue.remove(0))
      self.resolve_locked(
        batch,
        BatchError(
          "batch for partition \{batch.partition} exceeded delivery_timeout_ms (\{delivery_timeout_ms}) after \{batch.attempts} attempts",
        ),
      )
      continue
    }
    if batch.closed || now_ms >= batch.ready_at(self.linger_ms) {
      batch.closed = true
      ignore(queue.remove(0))
      out.push(batch)
    }
  }
  out
}

///|
/// Put a batch back at the front of its partition queue after a
/// recoverable failure, preserving per-partition order; the sender
/// retries it.
async fn RecordAccumulator::requeue(
  self : RecordAccumulator,
  batch : ProducerBatch,
) -> Unit {
  self.lock.acquire()
  defer self.lock.release()
  self.queue(batch.partition).insert(0, batch)
}

///|
/// Publish the batch's terminal result, wake every coalesced sender, and
/// return its bytes to the buffer pool.
async fn RecordAccumulator::release(
  self : RecordAccumulator,
  batch : ProducerBatch,
  result : BatchResult,
) -> Unit {
  self.lock.acquire()
  defer self.lock.release()
  self.resolve_locked(batch, result)
}

///|
fn RecordAccumulator::resolve_locked(
  self : RecordAccumulator,
  batch : ProducerBatch,
  result : BatchResult,
) -> Unit {
  batch.result = Some(result)
  self.pool_bytes -= batch.size_estimate
  // Waiters never re-register once the result is set.
  for waiter in batch.waiters {
    waiter.release()
  }
}

///|
/// Register a completion waiter under the accumulator lock. Returns true
/// when the batch already reached a terminal result — no wait needed.
/// Locking here makes a release racing the registration unable to lose
/// the wakeup.
async fn RecordAccumulator::register_waiter(
  self : RecordAccumulator,
  batch : ProducerBatch,
  waiter : @async.Semaphore,
) -> Bool {
  self.lock.acquire()
  defer self.lock.release()
  match batch.result {
    Some(_) => true
    None => {
      batch.waiters.push(waiter)
      false
    }
  }
}
