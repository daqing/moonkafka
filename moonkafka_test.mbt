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

///|
test "crc32c known vectors" {
  // Reference values from RFC 3720 / the Castagnoli CRC test vectors.
  assert_eq(@moonkafka.crc32c(b""), 0U)
  assert_eq(@moonkafka.crc32c(@utf8.encode("abc")), 0x364B3FB7U)
  assert_eq(@moonkafka.crc32c(@utf8.encode("123456789")), 0xE3069283U)
}

///|
test "uvarint roundtrip" {
  let enc = @moonkafka.Encoder::new()
  for v in [0U, 1U, 127U, 128U, 300U, 0x7FFFFFFFU] {
    enc.write_uvarint(v)
  }
  let d = @moonkafka.Decoder::new(enc.to_bytes())
  for v in [0, 1, 127, 128, 300, 0x7FFFFFFF] {
    assert_eq(d.read_uvarint(), v)
  }
}

///|
test "zigzag varint roundtrip" {
  let enc = @moonkafka.Encoder::new()
  for v in [0, -1, 1, -64, 63, -2147483648, 2147483647] {
    enc.write_varint(v)
  }
  let d = @moonkafka.Decoder::new(enc.to_bytes())
  for v in [0, -1, 1, -64, 63, -2147483648, 2147483647] {
    assert_eq(d.read_varint(), v)
  }
}

///|
test "zigzag varlong roundtrip" {
  let enc = @moonkafka.Encoder::new()
  for v in [0L, -1L, 1L, 123456789012345L, -9223372036854775808L] {
    enc.write_varlong(v)
  }
  let d = @moonkafka.Decoder::new(enc.to_bytes())
  for v in [0L, -1L, 1L, 123456789012345L, -9223372036854775808L] {
    assert_eq(d.read_varlong(), v)
  }
}

///|
test "compact string roundtrip" {
  let enc = @moonkafka.Encoder::new()
  enc.write_compact_string("hello kafka")
  enc.write_compact_nullable_string(None)
  enc.write_compact_nullable_string(Some(""))
  let d = @moonkafka.Decoder::new(enc.to_bytes())
  assert_eq(d.read_compact_string(), "hello kafka")
  assert_eq(d.read_compact_nullable_string(), None)
  assert_eq(d.read_compact_nullable_string(), Some(""))
}
