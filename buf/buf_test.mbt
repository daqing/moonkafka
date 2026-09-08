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

///|
test "huge compact lengths raise instead of panicking" {
  // A compact string claiming ~2GB must raise, not overflow the pos+n
  // bounds check into a negative and index out of bounds mid-read; a byte
  // is consumed first so the addition would wrap on 32-bit ints.
  let d = @buf.Decoder::new(b"\x00\xff\xff\xff\xff\x07") // uvarint 0x7FFFFFFF
  ignore(d.read_byte())
  let mut eof = false
  ignore(d.read_compact_nullable_string()) catch {
    @buf.UnexpectedEof => eof = true
    _ => fail("expected UnexpectedEof")
  }
  assert_true(eof)
}

///|
test "negative skip is rejected without rewinding the position" {
  let d = @buf.Decoder::new(b"abcd")
  ignore(d.read_i16()) // consume two bytes
  let mut rejected = false
  ignore(d.skip(-1)) catch {
    @buf.UnexpectedEof => rejected = true
    _ => fail("expected UnexpectedEof")
  }
  assert_true(rejected)
  assert_eq(d.remaining(), 2)
}

///|
test "read_bytes_view is a zero-copy slice of the backing buffer" {
  let backing = b"\x00\x01\x02\x03\x04\x05"
  let d = @buf.Decoder::new(backing)
  ignore(d.read_bytes(3))
  let view = d.read_bytes_view(2) catch { _ => fail("unexpected decode error") }
  // A view shares the backing bytes rather than copying them.
  assert_eq(view.length(), 2)
  assert_eq(view[0], b'\x03')
  assert_eq(view[1], b'\x04')
  assert_eq(d.read_bytes(1), b"\x05")
  assert_eq(d.remaining(), 0)
}

///|
test "with_capacity preallocates without changing output bytes" {
  let e = @buf.Encoder::with_capacity(8)
  e.write_compact_string("moonkafka")
  e.write_i32(42)
  e.write_tag_buffer()
  // A default encoder must produce the identical bytes.
  let d = @buf.Encoder::new()
  d.write_compact_string("moonkafka")
  d.write_i32(42)
  d.write_tag_buffer()
  assert_eq(e.to_bytes(), d.to_bytes())
}
