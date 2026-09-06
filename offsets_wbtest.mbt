// Whitebox ListOffsets request tests: PartitionInfo literals are
// package-internal.

///|

///|
test "list offsets v11 request encodes sentinel and timeout" {
  let req = encode_list_offsets_request(
    11,
    "events",
    [{ index: 0, leader: 1, leader_epoch: 7, }],
    OFFSET_MAX_TIMESTAMP,
    timeout_ms=5000,
  )
  let d = @buf.Decoder::new(req)
  assert_eq(d.read_i32(), -1) // replica_id: consumer
  assert_eq(d.read_i8(), 0) // isolation level
  assert_eq(d.read_compact_len(), 1) // one topic
  assert_eq(d.read_compact_string(), "events")
  assert_eq(d.read_compact_len(), 1) // one partition
  assert_eq(d.read_i32(), 0) // index
  assert_eq(d.read_i32(), 7) // current leader epoch
  assert_eq(d.read_i64(), -3L) // max timestamp sentinel
  d.skip_tag_buffer() // entry tag buffer
  d.skip_tag_buffer() // topic tag buffer
  assert_eq(d.read_i32(), 5000) // timeout_ms (v10+)
  assert_eq(d.remaining(), 1) // body tag buffer
}

///|
test "list offsets v10 request defaults the timeout to zero" {
  let req = encode_list_offsets_request(
    10,
    "events",
    [{ index: 0, leader: 1, leader_epoch: 7, }],
    OFFSET_EARLIEST_PENDING_UPLOAD,
  )
  let d = @buf.Decoder::new(req)
  let mut saw_timeout = 0
  // Skip to the last four bytes before the tag buffer.
  let bytes = req.length()
  saw_timeout = @buf.Decoder::new(req, start=bytes - 5).read_i32()
  ignore(d)
  assert_eq(saw_timeout, 0)
  // Sentinels keep their stable numbers.
  assert_eq(OFFSET_EARLIEST, -2L)
  assert_eq(OFFSET_LATEST, -1L)
  assert_eq(OFFSET_LOCAL_LOG_START, -4L)
  assert_eq(OFFSET_LAST_TIERED, -5L)
  assert_eq(OFFSET_EARLIEST_PENDING_UPLOAD, -6L)
}

///|
test "list offsets request refuses versions without codecs" {
  let mut refused = false
  ignore(
    encode_list_offsets_request(
      7,
      "events",
      [{ index: 0, leader: 1, leader_epoch: 7, }],
      OFFSET_LATEST,
    ),
  ) catch {
    ProtocolError(_) => refused = true
    _ => fail("expected ProtocolError")
  }
  assert_true(refused)
}
