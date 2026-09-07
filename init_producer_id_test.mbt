// Blackbox tests for the InitProducerId v5 codecs.

///|
test "init producer id v5 request encodes identity and bump fields" {
  let plain = @moonkafka.encode_init_producer_id_request(None, 60000, -1L, -1)
  let d = @buf.Decoder::new(plain)
  assert_eq(d.read_compact_nullable_string(), None)
  assert_eq(d.read_i32(), 60000)
  assert_eq(d.read_i64(), -1L)
  assert_eq(d.read_i16(), -1)
  assert_eq(d.remaining(), 1) // tag buffer
  let bump = @moonkafka.encode_init_producer_id_request(
    Some("tx-1"),
    30000,
    500L,
    2,
  )
  let d2 = @buf.Decoder::new(bump)
  assert_eq(d2.read_compact_nullable_string(), Some("tx-1"))
  assert_eq(d2.read_i32(), 30000)
  assert_eq(d2.read_i64(), 500L)
  assert_eq(d2.read_i16(), 2)
}

///|
test "init producer id v5 response decodes the assigned identity" {
  let e = @buf.Encoder::new()
  e.write_i32(3) // throttle
  e.write_i16(0) // no error
  e.write_i64(500L)
  e.write_i16(1)
  e.write_tag_buffer()
  let (result, throttle) = @moonkafka.decode_init_producer_id_response(
    @buf.Decoder::new(e.to_bytes()),
  )
  assert_eq(throttle, 3)
  assert_eq(result.producer_id, 500L)
  assert_eq(result.producer_epoch, 1)
  assert_eq(result.error_code, 0)
}
