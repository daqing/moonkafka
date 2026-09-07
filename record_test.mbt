// Tests for the record layer: header en/decode, batch flags, the
// read_committed filter primitive, and the incremental batch builder.

///|
fn header_record() -> @moonkafka.Record {
  {
    offset: 0L,
    timestamp: 1700000000000L,
    key: Some(b"k"),
    value: Some(b"v"),
    headers: [(b"h1", b"value-1"), (b"h2", b""), (b"", b"empty-key")],
  }
}

///|
test "record headers roundtrip through the batch codec" {
  let batch = @moonkafka.encode_record_batch([header_record()])
  let decoded = @moonkafka.decode_record_batches(batch)
  assert_eq(decoded.length(), 1)
  assert_eq(decoded[0].headers.length(), 3)
  assert_eq(decoded[0].headers[0].0, b"h1")
  assert_eq(decoded[0].headers[0].1, b"value-1")
  assert_eq(decoded[0].headers[1].1, b"") // empty header value
  assert_eq(decoded[0].headers[2].0, b"") // empty header key
  assert_eq(decoded[0].headers[2].1, b"empty-key")
}

///|
/// A batch with the transactional and/or control attribute bits set.
fn flagged_batch(transactional~ : Bool, control~ : Bool) -> Bytes {
  let batch = @moonkafka.encode_record_batch([header_record()])
  let patched : Array[Byte] = []
  for b in batch {
    patched.push(b)
  }
  let mut attributes = 0
  if transactional {
    attributes = attributes | 0x10
  }
  if control {
    attributes = attributes | 0x20
  }
  patched[21] = (attributes >> 8).to_byte()
  patched[22] = (attributes & 0xFF).to_byte()
  // The CRC covers bytes [21, end): recompute after the attribute flip.
  let unrepaired = Bytes::from_array(patched)
  let crc = @internal.crc32c(unrepaired, start=21)
  for i in 0..<4 {
    patched[17 + i] = ((crc >> (24 - 8 * i)) & 0xFFU).to_byte()
  }
  Bytes::from_array(patched)
}

///|
test "decode exposes control and transactional batch flags" {
  let plain = @moonkafka.decode_record_batches_detailed(
      @moonkafka.encode_record_batch([header_record()]),
    ).0[0]
  assert_true(!plain.is_control)
  assert_true(!plain.is_transactional)
  assert_eq(plain.producer_id, -1L)
  let txn = @moonkafka.decode_record_batches_detailed(
      flagged_batch(transactional=true, control=false),
    ).0[0]
  assert_true(txn.is_transactional)
  assert_true(!txn.is_control)
  assert_eq(txn.records.length(), 1)
  let control = @moonkafka.decode_record_batches_detailed(
      flagged_batch(transactional=true, control=true),
    ).0[0]
  assert_true(control.is_control)
  // The detailed view keeps marker records (flagged); the flat view and
  // collect_committed are what hide them from consumers.
  assert_eq(control.records.length(), 1)
}

///|
test "collect_committed drops aborted transactions and control batches" {
  let committed = @moonkafka.encode_record_batch([
    { offset: 0L, timestamp: 1L, key: None, value: Some(b"ok"), headers: [], },
  ])
  let aborted_batch = flagged_batch(transactional=true, control=false)
  let control_batch = flagged_batch(transactional=false, control=true)
  // Splice the three batches together.
  let joined : Array[Byte] = []
  for bytes in [committed, aborted_batch, control_batch] {
    for i in 0..<bytes.length() {
      joined.push(bytes[i])
    }
  }
  let (batches, truncated) = @moonkafka.decode_record_batches_detailed(
    Bytes::from_array(joined),
  )
  assert_eq(batches.length(), 3)
  assert_true(!truncated)
  // Nothing aborted: control batch still dropped, data kept.
  let all = @moonkafka.collect_committed(batches, [])
  assert_eq(all.length(), 2)
  // Abort producer -1 from offset 0: the transactional batch's record
  // (offset 0) disappears; the non-transactional record stays.
  let filtered = @moonkafka.collect_committed(batches, [
    { producer_id: -1L, first_offset: 0L, },
  ])
  assert_eq(filtered.length(), 1)
  assert_eq(filtered[0].value, Some(b"ok"))
}

///|
test "batch builder tracks size and offsets incrementally" {
  let builder = @moonkafka.RecordBatchBuilder::new(base_offset=10L)
  assert_eq(builder.count(), 0)
  assert_eq(builder.last_assigned_offset(), 9L)
  builder.append(1000L, key=Some(b"a"), value=Some(b"aaa"))
  builder.append(1001L, value=Some(b"bb"), headers=[(b"h", b"1")])
  assert_eq(builder.count(), 2)
  assert_eq(builder.last_assigned_offset(), 11L)
  // The estimate covers the fixed header plus the encoded bodies.
  assert_true(builder.estimated_size() >= 61 + 2)
  let bytes = builder.to_bytes()
  let decoded = @moonkafka.decode_record_batches(bytes)
  assert_eq(decoded.length(), 2)
  assert_eq(decoded[0].offset, 10L)
  assert_eq(decoded[0].timestamp, 1000L)
  assert_eq(decoded[1].offset, 11L)
  assert_eq(decoded[1].timestamp, 1001L)
  assert_eq(decoded[1].headers, [(b"h", b"1")])
  // Same input as the one-shot encoder: same bytes.
  let direct = @moonkafka.encode_record_batch([
    {
      offset: 0L,
      timestamp: 1000L,
      key: Some(b"a"),
      value: Some(b"aaa"),
      headers: [],
    },
    {
      offset: 0L,
      timestamp: 1001L,
      key: None,
      value: Some(b"bb"),
      headers: [(b"h", b"1")],
    },
  ])
  assert_eq(bytes.length(), direct.length())
}

///|
test "truncated trailing batch is reported by the detailed decoder" {
  let batch = @moonkafka.encode_record_batch([
    { offset: 0L, timestamp: 1L, key: None, value: Some(b"x"), headers: [], },
  ])
  let cut = batch.length() - 5
  let partial : Array[Byte] = []
  for i in 0..<cut {
    partial.push(batch[i])
  }
  let (batches, truncated) = @moonkafka.decode_record_batches_detailed(
    Bytes::from_array(partial),
  )
  assert_eq(batches.length(), 0)
  assert_true(truncated)
}
