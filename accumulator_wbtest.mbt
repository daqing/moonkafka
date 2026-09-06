// Whitebox tests for the record accumulator: size estimates, batch_size
// rotation, buffer_memory exhaustion, claim/requeue/release semantics,
// and per-partition ordering snapshots. Accumulator methods take the
// mutex, so these run as async tests.

///|
fn acc(
  batch_size~ : Int,
  buffer_memory~ : Int,
  linger_ms? : Int = 0,
) -> RecordAccumulator {
  RecordAccumulator::new(batch_size, buffer_memory, linger_ms)
}

///|
test "estimated append size matches actual builder growth" {
  let b = RecordBatchBuilder::new()
  let est = b.estimated_append_size(1000L, key=Some(b"k"), value=Some(b"v"))
  b.append(1000L, key=Some(b"k"), value=Some(b"v"))
  assert_eq(est, b.estimated_size() - RECORD_BATCH_HEADER_SIZE)
  let est2 = b.estimated_append_size(1001L, value=Some(b"hello"))
  b.append(1001L, value=Some(b"hello"))
  assert_eq(est2, b.estimated_size() - RECORD_BATCH_HEADER_SIZE - est)
  assert_eq(b.count(), 2)
}

///|
async test "append coalesces records while they fit batch_size" {
  let a = acc(batch_size=100_000, buffer_memory=1_000_000)
  let (b1, i1, r1) = a.append(0L, 0, 0L, None, Some(b"one"), [])
  assert_true(!r1)
  assert_eq(i1, 0)
  let (b2, i2, r2) = a.append(1L, 0, 1L, None, Some(b"two"), [])
  assert_true(b2.id == b1.id)
  assert_eq(i2, 1)
  assert_true(!r2)
  // the bytes decode back to both records
  let batch = b2.builder.to_bytes()
  let (records, _) = decode_record_batches_ex(batch) catch {
    e => {
      ignore(e)
      abort("decode failed")
    }
  }
  assert_eq(records.length(), 2)
}

///|
async test "append rotates to a fresh batch when the record does not fit" {
  // "AAAA" encodes to 10 bytes (body + varint) and "A" to 8; header is
  // 61. With batch_size=78: one "AAAA" (71) leaves no room for another
  // "AAAA" but the fresh batch then hosts two "A" records (77).
  let a = acc(batch_size=78, buffer_memory=1_000_000)
  let (b1, _, r1) = a.append(0L, 0, 0L, None, Some(b"AAAA"), [])
  assert_true(!r1)
  let (b2, _, r2) = a.append(1L, 0, 1L, None, Some(b"A"), [])
  assert_true(r2)
  assert_true(b2.id != b1.id)
  assert_true(b1.closed)
  assert_true(!b2.closed)
  // a third record lands in the newest open batch
  let (b3, _, r3) = a.append(2L, 0, 2L, None, Some(b"A"), [])
  assert_true(b3.id == b2.id)
  assert_true(!r3)
}

///|
async fn append_exhausted(a : RecordAccumulator) -> Bool {
  let _ = a.append(1L, 0, 1L, None, Some(b"AAAA"), []) catch {
    BufferExhausted(_) => return true
    e => raise e
  }
  false
}

///|
async test "append raises BufferExhausted when buffer_memory is out" {
  // one batch of 72 bytes queued; the rotation needs another 72
  let a = acc(batch_size=82, buffer_memory=112)
  let _ = a.append(0L, 0, 0L, None, Some(b"AAAA"), [])
  assert_true(append_exhausted(a))
}

///|
async test "release publishes the result and frees the buffer pool" {
  let a = acc(batch_size=1000, buffer_memory=10_000)
  let (b, _, _) = a.append(0L, 0, 0L, None, Some(b"x"), [])
  assert_true(a.pool_bytes > 0)
  a.release(b, BatchOk(42L))
  assert_eq(a.pool_bytes, 0)
  assert_true(b.result is Some(BatchOk(42L)))
}

///|
async test "drain_ready hands out closed batches one per partition in order" {
  // all three records coalesce into one open batch that lingers
  let a = acc(batch_size=1000, buffer_memory=10_000, linger_ms=10_000)
  let (b1, _, _) = a.append(0L, 0, 0L, None, Some(b"x"), [])
  let (b2, _, _) = a.append(1L, 0, 1L, None, Some(b"y"), [])
  let (b3, _, _) = a.append(2L, 0, 2L, None, Some(b"z"), [])
  assert_true(b2.id == b1.id)
  assert_true(b3.id == b1.id)
  // nothing is ready while the batch is open and linger has not passed
  assert_true(a.drain_ready(5_000L, 100_000).is_empty())
  // past the linger deadline the open batch becomes ready...
  let ready = a.drain_ready(10_500L, 100_000)
  assert_eq(ready.length(), 1)
  assert_eq(ready[0].id, b1.id)
  // ...and the next record after the old batch left opens a fresh open
  // batch (nothing to rotate), which force-close then makes ready
  let (b4, _, rotated) = a.append(10_600L, 0, 10_600L, None, Some(b"w"), [])
  assert_true(!rotated)
  assert_true(b4.id != b1.id)
  a.force_close_all()
  let next = a.drain_ready(10_600L, 100_000)
  assert_eq(next.length(), 1)
  assert_eq(next[0].id, b4.id)
  assert_true(a.drain_ready(10_600L, 100_000).is_empty())
}

///|
async test "drain_ready releases batches past the delivery timeout" {
  let a = acc(batch_size=1000, buffer_memory=10_000)
  let (b, _, _) = a.append(0L, 0, 0L, None, Some(b"x"), [])
  // 5_000 ms old with a 1_000 ms delivery budget: expire, don't hand out
  let ready = a.drain_ready(5_000L, 1_000)
  assert_true(ready.is_empty())
  assert_eq(a.pool_bytes, 0)
  assert_true(b.result is Some(_))
  let msg = match b.result {
    Some(BatchError(msg)) => msg
    _ => abort("expected a delivery error")
  }
  assert_true(msg.contains("delivery_timeout_ms"))
}

///|
async test "requeue moves a failed batch back to the front" {
  let a = acc(batch_size=82, buffer_memory=1_000_000)
  let (b1, _, _) = a.append(0L, 0, 0L, None, Some(b"AAAA"), [])
  let (b2, _, _) = a.append(1L, 0, 1L, None, Some(b"AAAA"), [])
  a.requeue(b2)
  let q = a.batches.get(0).unwrap()
  assert_eq(q[0].id, b2.id)
  assert_eq(q[1].id, b1.id)
}

///|
async test "ready_at adds the linger window to the creation time" {
  let a = acc(batch_size=1000, buffer_memory=10_000, linger_ms=250)
  let (b, _, _) = a.append(5_000L, 0, 0L, None, Some(b"x"), [])
  assert_eq(b.ready_at(a.linger_ms), 5_250L)
  let zero = acc(batch_size=1000, buffer_memory=10_000)
  let (b2, _, _) = zero.append(5_000L, 0, 0L, None, Some(b"x"), [])
  assert_eq(b2.ready_at(zero.linger_ms), 5_000L)
}
