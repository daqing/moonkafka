///|
test "fake api versions response decodes with zero throttle" {
  let body = fake_api_versions_response(0)
  let d = @buf.Decoder::new(body)
  let (versions, throttle) = @moonkafka.decode_api_versions_response(d)
  assert_eq(throttle, 0)
  assert_eq(d.remaining(), 0)
  // The fake broker's advertised ranges negotiate the driver's implemented
  // versions (each matrix row is a single version today).
  assert_eq(versions.pick(@moonkafka.API_PRODUCE), 11)
  assert_eq(versions.pick(@moonkafka.API_FETCH), 12)
  assert_eq(versions.pick(@moonkafka.API_LIST_OFFSETS), 7)
  assert_eq(versions.pick(@moonkafka.API_METADATA), 12)
  assert_eq(versions.pick(@moonkafka.API_API_VERSIONS), 3)
}
