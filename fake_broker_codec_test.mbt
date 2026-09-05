///|
test "fake api versions response decodes with zero throttle" {
  let body = fake_api_versions_response(0)
  let d = @buf.Decoder::new(body)
  let throttle = @moonkafka.decode_api_versions_response(d)
  assert_eq(throttle, 0)
  assert_eq(d.remaining(), 0)
}
