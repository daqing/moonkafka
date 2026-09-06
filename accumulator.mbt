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
priv struct ProducerBatch {
  /// Unique within its accumulator; identity for queue-order comparisons.
  id : Int
  partition : Int
  builder : RecordBatchBuilder
  created_ms : Int64
  /// Closed batches accept no more records and are ready to send.
  mut closed : Bool
  /// Claimed by the flusher that alone puts the batch on the wire; stays
  /// claimed across requeue-and-retry until a terminal result.
  mut claimed : Bool
  /// Estimated wire size; also the batch's share of buffer_memory.
  mut size_estimate : Int
  mut result : BatchResult?
  /// Released exactly once when the result lands; coalesced senders wait.
  done : @async.Semaphore
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
    Some(b) => b.closed = true
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
    claimed: false,
    size_estimate: 0,
    result: None,
    done: @async.Semaphore(1, initial_value=0),
  }
  batch.builder.append(timestamp, key~, value~, headers~)
  batch.size_estimate = fresh
  self.pool_bytes += fresh
  queue.push(batch)
  (batch, 0, rotated)
}

///|
/// Close the batch (if still open) and take the flusher role if free.
/// Returns whether this call closed it — the sticky partitioner's
/// boundary — and whether this caller is the flusher.
async fn RecordAccumulator::close_and_claim(
  self : RecordAccumulator,
  batch : ProducerBatch,
) -> (Bool, Bool) {
  self.lock.acquire()
  defer self.lock.release()
  let closed_now = !batch.closed
  batch.closed = true
  let mut claimed = false
  if !batch.claimed {
    batch.claimed = true
    claimed = true
  }
  (closed_now, claimed)
}

///|
/// Queued batches ordered ahead of `batch` in its partition that have
/// not reached a terminal result yet. A flusher must put these on the
/// wire (or wait out their owner) first, so per-partition order survives
/// coalescing — the inline stand-in for the sender task's front-first
/// drain.
async fn RecordAccumulator::predecessors(
  self : RecordAccumulator,
  batch : ProducerBatch,
) -> Array[ProducerBatch] {
  self.lock.acquire()
  defer self.lock.release()
  let out : Array[ProducerBatch] = []
  match self.batches.get(batch.partition) {
    Some(q) =>
      for b in q {
        if b.id == batch.id {
          break
        }
        if b.result is None {
          out.push(b)
        }
      }
    None => ()
  }
  out
}

///|
/// Put a claimed batch back at the front of its partition queue after a
/// recoverable failure, preserving per-partition order; its claimer
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
  batch.result = Some(result)
  self.pool_bytes -= batch.size_estimate
  self.lock.release()
  batch.done.release()
}
