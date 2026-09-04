// Blackbox tests for the wire codecs. Run `moon test` to execute.

///|
test "uvarint roundtrip" {
  let enc = @buf.Encoder::new()
  for v in [0U, 1U, 127U, 128U, 300U, 0x7FFFFFFFU] {
    enc.write_uvarint(v)
  }
  let d = @buf.Decoder::new(enc.to_bytes())
  for v in [0, 1, 127, 128, 300, 0x7FFFFFFF] {
    assert_eq(d.read_uvarint(), v)
  }
}

///|
test "zigzag varint roundtrip" {
  let enc = @buf.Encoder::new()
  for v in [0, -1, 1, -64, 63, -2147483648, 2147483647] {
    enc.write_varint(v)
  }
  let d = @buf.Decoder::new(enc.to_bytes())
  for v in [0, -1, 1, -64, 63, -2147483648, 2147483647] {
    assert_eq(d.read_varint(), v)
  }
}

///|
test "zigzag varlong roundtrip" {
  let enc = @buf.Encoder::new()
  for v in [0L, -1L, 1L, 123456789012345L, -9223372036854775808L] {
    enc.write_varlong(v)
  }
  let d = @buf.Decoder::new(enc.to_bytes())
  for v in [0L, -1L, 1L, 123456789012345L, -9223372036854775808L] {
    assert_eq(d.read_varlong(), v)
  }
}

///|
test "compact string roundtrip" {
  let enc = @buf.Encoder::new()
  enc.write_compact_string("hello kafka")
  enc.write_compact_nullable_string(None)
  enc.write_compact_nullable_string(Some(""))
  let d = @buf.Decoder::new(enc.to_bytes())
  assert_eq(d.read_compact_string(), "hello kafka")
  assert_eq(d.read_compact_nullable_string(), None)
  assert_eq(d.read_compact_nullable_string(), Some(""))
}

///|
test "tagged fields roundtrip in ascending order" {
  let enc = @buf.Encoder::new()
  enc.write_tagged_fields([(5, b"five"), (1, b"one"), (300, b"big-tag")])
  let d = @buf.Decoder::new(enc.to_bytes())
  assert_eq(d.read_uvarint(), 3)
  assert_eq(d.read_uvarint(), 1)
  assert_eq(d.read_bytes(d.read_uvarint()), b"one")
  assert_eq(d.read_uvarint(), 5)
  assert_eq(d.read_bytes(d.read_uvarint()), b"five")
  assert_eq(d.read_uvarint(), 300)
  assert_eq(d.read_bytes(d.read_uvarint()), b"big-tag")
}

///|
test "empty tag forms both read as zero count" {
  let enc = @buf.Encoder::new()
  enc.write_tag_buffer()
  enc.write_tagged_fields([])
  let d = @buf.Decoder::new(enc.to_bytes())
  assert_eq(d.read_uvarint(), 0)
  assert_eq(d.read_uvarint(), 0)
  // and skip_tag_buffer consumes a populated buffer in one go
  let enc2 = @buf.Encoder::new()
  enc2.write_tagged_fields([(7, b"\x00\xff")])
  enc2.write_tag_buffer()
  let d2 = @buf.Decoder::new(enc2.to_bytes())
  d2.skip_tag_buffer()
  d2.skip_tag_buffer()
  assert_eq(d2.remaining(), 0)
}
