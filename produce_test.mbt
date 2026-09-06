// Codec tests for Produce v12/v13: hand-built response bodies per the
// 4.3 ProduceResponse.json schema, decoded and checked field by field;
// the request encoder verified by decoding its bytes back.

///|
/// A produce response body for one topic and one partition, parameterized
/// over the topic addressing (name for v12, id for v13) and record errors.
fn produce_body(by_id~ : Bool) -> Bytes {
  let e = @buf.Encoder::new()
  e.write_compact_len(1) // one topic
  if by_id {
    e.write_bytes(Bytes::make(16, b'\x2a')) // topic id
  } else {
    e.write_compact_string("orders")
  }
  e.write_compact_len(1) // one partition
  e.write_i32(3) // partition index
  e.write_i16(39) // non-zero error code, surfaced via error_name by callers
  e.write_i64(100L) // base offset
  e.write_i64(1700000000000L) // log append time
  e.write_i64(5L) // log start offset
  e.write_compact_len(2) // two record errors
  e.write_i32(0)
  e.write_compact_nullable_string(Some("record too large"))
  e.write_tag_buffer()
  e.write_i32(2)
  e.write_compact_nullable_string(None)
  e.write_tag_buffer()
  e.write_compact_nullable_string(Some("batch dropped"))
  e.write_tag_buffer()
  e.write_tag_buffer() // topic tag buffer
  e.write_i32(7) // throttle
  e.write_tag_buffer()
  e.to_bytes()
}

///|
test "produce v12 response decodes record errors and per-batch fields" {
  let d = @buf.Decoder::new(produce_body(by_id=false))
  let (results, throttle) = @moonkafka.decode_produce_response(12, d)
  assert_eq(throttle, 7)
  assert_eq(results.length(), 1)
  let r = results[0]
  assert_eq(r.partition, 3)
  assert_eq(r.error_code, 39)
  assert_eq(r.base_offset, 100L)
  assert_eq(r.log_append_time, 1700000000000L)
  assert_eq(r.log_start_offset, 5L)
  assert_eq(r.error_message, Some("batch dropped"))
  assert_eq(r.record_errors.length(), 2)
  assert_eq(r.record_errors[0].batch_index, 0)
  assert_eq(r.record_errors[0].message, Some("record too large"))
  assert_eq(r.record_errors[1].batch_index, 2)
  assert_eq(r.record_errors[1].message, None)
  assert_eq(d.remaining(), 0)
}

///|
test "produce v13 response addresses topics by id" {
  let d = @buf.Decoder::new(produce_body(by_id=true))
  let (results, throttle) = @moonkafka.decode_produce_response(13, d)
  assert_eq(throttle, 7)
  assert_eq(results.length(), 1)
  assert_eq(results[0].partition, 3)
  assert_eq(d.remaining(), 0)
}

///|
test "produce decode refuses versions without codecs" {
  let mut refused = false
  ignore(@moonkafka.decode_produce_response(11, @buf.Decoder::new(b""))) catch {
    @moonkafka.ProtocolError(_) => refused = true
    _ => fail("expected ProtocolError")
  }
  assert_true(refused)
}

///|
test "produce v12 request encodes topic name and null transactional id" {
  let batch = @moonkafka.encode_record_batch([
    { offset: 0L, timestamp: 1L, key: None, value: Some(b"x"), headers: [], },
  ])
  let req = @moonkafka.encode_produce_request(
    12,
    "orders",
    @moonkafka.Uuid::zero(),
    [(3, batch)],
    acks=-1,
    timeout_ms=1500,
  )
  let d = @buf.Decoder::new(req)
  assert_eq(d.read_compact_len(), -1) // transactional_id: null
  assert_eq(d.read_i16(), -1) // acks
  assert_eq(d.read_i32(), 1500) // timeout
  assert_eq(d.read_compact_len(), 1) // one topic
  assert_eq(d.read_compact_string(), "orders") // v12: by name
  assert_eq(d.read_compact_len(), 1) // one partition
  assert_eq(d.read_i32(), 3) // partition index
  assert_eq(d.read_compact_len(), batch.length())
  assert_eq(d.remaining(), batch.length() + 3) // batch + 3 tag buffers (partition, topic, body)
}

///|
test "produce v13 request encodes the topic id and rejects zero ids" {
  let id = @moonkafka.Uuid::parse("AAAAAAAAAAAAAAAAAAAAAQ")
  let batch = b"\x00\x01"
  let req = @moonkafka.encode_produce_request(
    13,
    "orders",
    id,
    [(0, batch)],
    acks=1,
    timeout_ms=1,
  )
  let d = @buf.Decoder::new(req)
  assert_eq(d.read_compact_len(), -1) // transactional_id: null
  assert_eq(d.read_i16(), 1) // acks
  assert_eq(d.read_i32(), 1) // timeout
  assert_eq(d.read_compact_len(), 1) // one topic
  assert_eq(d.read_bytes(16), id.to_bytes()) // v13: by id, no name
  assert_eq(d.read_compact_len(), 1) // one partition
  assert_eq(d.read_i32(), 0)
  assert_eq(d.read_compact_len(), 2)
  assert_eq(d.remaining(), 2 + 3) // batch bytes + 3 tag buffers
  // A zero topic id is "unknown" to the broker on v13: fail fast.
  let mut refused = false
  ignore(
    @moonkafka.encode_produce_request(
      13,
      "orders",
      @moonkafka.Uuid::zero(),
      [(0, batch)],
      acks=1,
      timeout_ms=1,
    ),
  ) catch {
    @moonkafka.ProtocolError(_) => refused = true
    _ => fail("expected ProtocolError")
  }
  assert_true(refused)
}
