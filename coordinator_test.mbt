// Blackbox tests for the FindCoordinator v4 codecs.

///|
test "find coordinator v4 request carries type then batched keys" {
  let body = @moonkafka.encode_find_coordinator_request(
    ["orders", "payments"],
    @moonkafka.Transaction,
  )
  let d = @buf.Decoder::new(body)
  assert_eq(d.read_i8(), 1) // key type: transaction
  assert_eq(d.read_compact_len(), 2)
  assert_eq(d.read_compact_string(), "orders")
  assert_eq(d.read_compact_string(), "payments")
  assert_eq(d.remaining(), 1) // trailing tag buffer
}

///|
test "find coordinator v4 response decodes one entry per key" {
  let e = @buf.Encoder::new()
  e.write_i32(7) // throttle
  e.write_compact_len(2)
  // entry 1: healthy
  e.write_compact_string("orders")
  e.write_i32(3) // node id
  e.write_compact_string("broker3.example.com")
  e.write_i32(9092)
  e.write_i16(0)
  e.write_compact_nullable_string(None)
  e.write_tag_buffer()
  // entry 2: broker-side failure
  e.write_compact_string("payments")
  e.write_i32(-1)
  e.write_compact_string("")
  e.write_i32(-1)
  e.write_i16(15) // COORDINATOR_NOT_AVAILABLE
  e.write_compact_nullable_string(Some("try later"))
  e.write_tag_buffer()
  e.write_tag_buffer()
  let (infos, throttle) = @moonkafka.decode_find_coordinator_response(
    @buf.Decoder::new(e.to_bytes()),
  )
  assert_eq(throttle, 7)
  assert_eq(infos.length(), 2)
  assert_eq(infos[0].key, "orders")
  assert_eq(infos[0].node_id, 3)
  assert_eq(infos[0].host, "broker3.example.com")
  assert_eq(infos[0].port, 9092)
  assert_eq(infos[0].error_code, 0)
  assert_true(infos[0].error_message is None)
  assert_eq(infos[1].key, "payments")
  assert_eq(infos[1].error_code, 15)
  assert_eq(infos[1].error_message.unwrap(), "try later")
}
