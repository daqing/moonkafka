// Codec tests for ListOffsets v10/v11, pinned to the 4.3
// ListOffsetsRequest.json / ListOffsetsResponse.json schemas.

///|
fn offsets_partition(index : Int, error : Int) -> Bytes {
  let e = @buf.Encoder::new()
  e.write_i32(index)
  e.write_i16(error)
  e.write_i64(1700000000000L) // timestamp of the returned offset
  e.write_i64(100L) // offset
  e.write_i32(7) // leader epoch
  e.write_tag_buffer()
  e.to_bytes()
}

///|
fn offsets_body(partitions : Array[Bytes]) -> Bytes {
  let e = @buf.Encoder::new()
  e.write_i32(4) // throttle
  e.write_compact_len(1) // one topic
  e.write_compact_string("events")
  e.write_compact_len(partitions.length())
  for partition in partitions {
    e.write_bytes(partition)
  }
  e.write_tag_buffer() // topic
  e.write_tag_buffer() // body
  e.to_bytes()
}

///|
test "list offsets response decodes offsets and raises on errors" {
  let ok = offsets_body([offsets_partition(0, 0)])
  let d = @buf.Decoder::new(ok)
  let (offsets, throttle) = @moonkafka.decode_list_offsets_response(d)
  assert_eq(throttle, 4)
  assert_eq(offsets.get(0), Some(100L))
  assert_eq(d.remaining(), 0)
  let errored = offsets_body([offsets_partition(1, 1)])
  let mut matched = false
  ignore(@moonkafka.decode_list_offsets_response(@buf.Decoder::new(errored))) catch {
    @moonkafka.BrokerError(1, _) => matched = true
    _ => fail("expected BrokerError 1")
  }
  assert_true(matched)
}
