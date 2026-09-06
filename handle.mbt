// The producer's public send surface: `send` for blocking sends,
// `send_handle` for a cancelable future-like handle, `send_all` for
// batched appends, and optional completion callbacks running as tasks in
// the producer's task group.

///|
/// A cancelable handle to one record's produce result. `await` returns
/// the broker-assigned offset (or -1 with acks=0). `cancel` stops the
/// wait and makes `await` raise — the record itself still goes out, since
/// batches are shared with other senders and records are not retracted.
pub struct SendHandle {
  priv producer : Producer
  priv batch : ProducerBatch
  priv index : Int
  priv mut waiter : @async.Semaphore?
  priv mut cancelled : Bool
}

///|
/// Terminal outcome delivered to a completion callback.
pub(all) enum SendResult {
  /// The record's broker-assigned offset (-1 with acks=0).
  Sent(Int64)
  /// Why the record's batch failed (delivery timeout, terminal broker
  /// error, ...).
  Failed(String)
} derive(@debug.Debug)

///|
/// Wait for the sender task to resolve the record. Raises when the
/// handle was cancelled before completion, or with the batch's error.
pub async fn SendHandle::await(self : SendHandle) -> Int64 {
  if self.cancelled {
    raise ProtocolError::ProtocolError("send cancelled")
  }
  let my_done = @async.Semaphore(1, initial_value=0)
  // Publish the waiter before registering so a concurrent cancel's
  // release is never lost (semaphores count up).
  self.waiter = Some(my_done)
  let already_done = self.producer.accumulator.register_waiter(
    self.batch,
    my_done,
  )
  if !already_done {
    my_done.acquire()
  }
  if self.cancelled && self.batch.result is None {
    raise ProtocolError::ProtocolError("send cancelled")
  }
  match self.batch.result {
    Some(BatchOk(base)) => base + self.index.to_int64()
    Some(BatchError(msg)) => raise ProtocolError::ProtocolError(msg)
    None =>
      raise ProtocolError::ProtocolError(
        "batch finished without a result (moonkafka bug)",
      )
  }
}

///|
/// Stop waiting for this record: a running or later `await` raises
/// immediately. The record is still delivered if its batch was already
/// in flight — cancellation here means "I no longer care", not "retract".
pub fn SendHandle::cancel(self : SendHandle) -> Unit {
  self.cancelled = true
  match self.waiter {
    Some(waiter) => waiter.release()
    None => ()
  }
}

///|
/// Run `callback` with the record's terminal outcome, as a background
/// task in the producer's task group. Fire-and-forget: failures inside
/// the callback do not fail the group.
pub fn SendHandle::on_complete(
  self : SendHandle,
  callback : async (SendResult) -> Unit,
) -> Unit {
  self.producer.group.spawn_bg(no_wait=true, allow_failure=true, () => {
    let offset = self.await() catch {
      e => {
        callback(Failed("\{e}"))
        return
      }
    }
    callback(Sent(offset))
  })
}

///|
/// Append one record without waiting for its result; the returned handle
/// awaits, cancels, or attaches a callback.
pub async fn Producer::send_handle(
  self : Producer,
  key? : Bytes,
  value? : Bytes,
  timestamp? : Int64,
  partition? : Int,
  headers? : Array[(Bytes, Bytes)] = [],
) -> SendHandle {
  let (batch, index) = self.append_record(
    key, value, timestamp, partition, headers,
  )
  { producer: self, batch, index, waiter: None, cancelled: false, }
}

///|
/// Send one record and block until its offset is known.
pub async fn Producer::send(
  self : Producer,
  key? : Bytes,
  value? : Bytes,
  timestamp? : Int64,
  partition? : Int,
  headers? : Array[(Bytes, Bytes)] = [],
) -> Int64 {
  let (batch, index) = self.append_record(
    key, value, timestamp, partition, headers,
  )
  let handle : SendHandle = {
    producer: self,
    batch,
    index,
    waiter: None,
    cancelled: false,
  }
  handle.await()
}

///|
/// One record for `send_all`.
pub(all) struct SendRecord {
  key : Bytes?
  value : Bytes?
  /// None stamps the record with the current wall clock.
  timestamp : Int64?
  partition : Int?
  headers : Array[(Bytes, Bytes)]
}

///|
/// Defaults for a SendRecord: no key, no headers, wall-clock timestamp.
pub fn SendRecord::of(value~ : Bytes) -> SendRecord {
  {
    key: None,
    value: Some(value),
    timestamp: None,
    partition: None,
    headers: [],
  }
}

///|
/// Append a batch of records in one call and return their handles (in
/// input order); each routes independently — by key, partitioner, or its
/// `partition` override.
pub async fn Producer::send_all(
  self : Producer,
  records : Array[SendRecord],
) -> Array[SendHandle] {
  let out : Array[SendHandle] = []
  for record in records {
    let (batch, index) = self.append_record(
      record.key,
      record.value,
      record.timestamp,
      record.partition,
      record.headers,
    )
    out.push({ producer: self, batch, index, waiter: None, cancelled: false, })
  }
  out
}

///|
/// Shared append path for send/send_handle/send_all: route to a
/// partition, accumulate, fire the sticky boundary on rotation.
async fn Producer::append_record(
  self : Producer,
  key : Bytes?,
  value : Bytes?,
  timestamp : Int64?,
  partition : Int?,
  headers : Array[(Bytes, Bytes)],
) -> (ProducerBatch, Int) {
  if self.closed {
    raise ProtocolError::ProtocolError("producer is closed")
  }
  // Opportunistic metadata refresh once the cached snapshot aged out.
  self.cluster.refresh_if_stale()
  let timestamp = timestamp.unwrap_or(@async.now())
  // The batch's encoded bytes belong to one partition, so the pick
  // happens once, before accumulating; retries keep it.
  let p = self.pick_partition(key, partition)
  let n = self.partitions.length()
  let (batch, index, rotated) = self.accumulator.append(
    @async.now(),
    p.index,
    timestamp,
    key,
    value,
    headers,
  )
  if rotated {
    self.router.on_batch_closed(n)
  }
  (batch, index)
}
