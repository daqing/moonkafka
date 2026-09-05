///|
test "fake api versions response decodes with zero throttle" {
  let body = fake_api_versions_response(0)
  let d = @buf.Decoder::new(body)
  let (versions, throttle) = @moonkafka.decode_api_versions_response(d)
  assert_eq(throttle, 0)
  assert_eq(d.remaining(), 0)
  // The fake broker's advertised ranges negotiate the driver's implemented
  // versions; Produce and Metadata top out at the v13 codecs.
  assert_eq(versions.pick(@moonkafka.API_PRODUCE), 13)
  assert_eq(versions.pick(@moonkafka.API_FETCH), 12)
  assert_eq(versions.pick(@moonkafka.API_LIST_OFFSETS), 7)
  assert_eq(versions.pick(@moonkafka.API_METADATA), 13)
  assert_eq(versions.pick(@moonkafka.API_API_VERSIONS), 3)
  assert_eq(versions.pick(@moonkafka.API_DESCRIBE_TOPIC_PARTITIONS), 0)
}
