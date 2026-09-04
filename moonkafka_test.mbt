// Blackbox tests run against the package's public API. Use `@moonkafka` to import.
// Prefer high-level behaviors over implementation details.
// Run `moon test` to execute and `moon test --update` to refresh snapshots.

///|
test "record batch encode/decode roundtrip" {
  let records : Array[@moonkafka.Record] = [
    {
      offset: 0L,
      timestamp: 1700000000000L,
      key: Some(@utf8.encode("key-1")),
      value: Some(@utf8.encode("value-1")),
    },
    {
      offset: 0L,
      timestamp: 1700000001234L,
      key: None,
      value: Some(b"\x00\xff"),
    },
    { offset: 0L, timestamp: 1700000001234L, key: None, value: None, },
  ]
  let decoded = @moonkafka.decode_record_batches(
    @moonkafka.encode_record_batch(records),
  )
  assert_eq(decoded.length(), 3)
  for i, record in decoded {
    assert_eq(record.offset, i.to_int64())
    assert_eq(record.timestamp, records[i].timestamp)
    assert_eq(record.key, records[i].key)
    assert_eq(record.value, records[i].value)
  }
}
