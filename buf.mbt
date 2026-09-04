// Kafka wire protocol primitive codecs.
//
// Fixed-width integers are big-endian. RPC-layer compact lengths and tag
// buffers use unsigned LEB128 varints; record-batch internals use zig-zag
// varints (see record.mbt).

///|
pub(all) suberror DecodeError {
  UnexpectedEof
  Malformed(String)
}

///|
pub struct Encoder {
  buf : Array[Byte]
}

///|
pub fn Encoder::new() -> Encoder {
  { buf: [], }
}

///|
pub fn Encoder::to_bytes(self : Encoder) -> Bytes {
  Bytes::from_array(self.buf)
}

///|
pub fn Encoder::write_byte(self : Encoder, b : Byte) -> Unit {
  self.buf.push(b)
}

///|
pub fn Encoder::write_bool(self : Encoder, v : Bool) -> Unit {
  self.buf.push(if v { b'\x01' } else { b'\x00' })
}

///|
pub fn Encoder::write_i8(self : Encoder, v : Int) -> Unit {
  self.buf.push(v.to_byte())
}

///|
pub fn Encoder::write_i16(self : Encoder, v : Int) -> Unit {
  self.buf.push((v >> 8).to_byte())
  self.buf.push(v.to_byte())
}

///|
pub fn Encoder::write_i32(self : Encoder, v : Int) -> Unit {
  for shift in [24, 16, 8, 0] {
    self.buf.push((v >> shift).to_byte())
  }
}

///|
pub fn Encoder::write_i64(self : Encoder, v : Int64) -> Unit {
  for shift in [56, 48, 40, 32, 24, 16, 8, 0] {
    self.buf.push((v >> shift).to_byte())
  }
}

///|
pub fn Encoder::write_bytes(self : Encoder, b : Bytes) -> Unit {
  for byte in b {
    self.buf.push(byte)
  }
}

///|
/// Unsigned LEB128 varint.
pub fn Encoder::write_uvarint(self : Encoder, v : UInt) -> Unit {
  let mut v = v
  while v >= 0x80U {
    self.buf.push((v | 0x80U).to_byte())
    v = v >> 7
  }
  self.buf.push(v.to_byte())
}

///|
/// Zig-zag varint (used inside record batches).
pub fn Encoder::write_varint(self : Encoder, v : Int) -> Unit {
  self.write_uvarint(((v << 1) ^ (v >> 31)).reinterpret_as_uint())
}

///|
/// Zig-zag varlong (used inside record batches).
pub fn Encoder::write_varlong(self : Encoder, v : Int64) -> Unit {
  let mut n = ((v << 1) ^ (v >> 63)).reinterpret_as_uint64()
  while n >= 0x80UL {
    self.buf.push((n | 0x80UL).to_byte())
    n = n >> 7
  }
  self.buf.push(n.to_byte())
}

///|
/// Compact array/bytes length: count + 1 as unsigned varint.
pub fn Encoder::write_compact_len(self : Encoder, n : Int) -> Unit {
  self.write_uvarint((n + 1).reinterpret_as_uint())
}

///|
/// Legacy NULLABLE_STRING: INT16 length, -1 = null.
pub fn Encoder::write_nullable_string(self : Encoder, s : String?) -> Unit {
  match s {
    None => self.write_i16(-1)
    Some(s) => {
      let b = @utf8.encode(s)
      self.write_i16(b.length())
      self.write_bytes(b)
    }
  }
}

///|
pub fn Encoder::write_compact_string(self : Encoder, s : String) -> Unit {
  let b = @utf8.encode(s)
  self.write_compact_len(b.length())
  self.write_bytes(b)
}

///|
pub fn Encoder::write_compact_nullable_string(
  self : Encoder,
  s : String?,
) -> Unit {
  match s {
    None => self.write_uvarint(0U)
    Some(s) => self.write_compact_string(s)
  }
}

///|
/// Empty tag buffer.
pub fn Encoder::write_tag_buffer(self : Encoder) -> Unit {
  self.buf.push(b'\x00')
}

///|
pub struct Decoder {
  data : Bytes
  mut pos : Int
}

///|
pub fn Decoder::new(data : Bytes, start? : Int = 0) -> Decoder {
  { data, pos: start, }
}

///|
pub fn Decoder::remaining(self : Decoder) -> Int {
  self.data.length() - self.pos
}

///|
fn Decoder::need(self : Decoder, n : Int) -> Unit raise DecodeError {
  if self.pos + n > self.data.length() {
    raise DecodeError::UnexpectedEof
  }
}

///|
pub fn Decoder::read_byte(self : Decoder) -> Byte raise DecodeError {
  self.need(1)
  let b = self.data[self.pos]
  self.pos += 1
  b
}

///|
pub fn Decoder::read_bool(self : Decoder) -> Bool raise DecodeError {
  self.read_byte() != b'\x00'
}

///|
pub fn Decoder::read_i8(self : Decoder) -> Int raise DecodeError {
  let v = self.read_byte().to_int()
  if v >= 128 {
    v - 256
  } else {
    v
  }
}

///|
pub fn Decoder::read_i16(self : Decoder) -> Int raise DecodeError {
  self.need(2)
  let v = (self.data[self.pos].to_int() << 8) | self.data[self.pos + 1].to_int()
  self.pos += 2
  Int16::from_int(v).to_int()
}

///|
pub fn Decoder::read_i32(self : Decoder) -> Int raise DecodeError {
  self.need(4)
  let mut v = 0
  for _ in 0..<4 {
    v = (v << 8) | self.data[self.pos].to_int()
    self.pos += 1
  }
  v
}

///|
pub fn Decoder::read_i64(self : Decoder) -> Int64 raise DecodeError {
  self.need(8)
  let mut v = 0L
  for _ in 0..<8 {
    v = (v << 8) | Int64::from_int(self.data[self.pos].to_int())
    self.pos += 1
  }
  v
}

///|
/// Skip n bytes, raising on overrun.
pub fn Decoder::skip(self : Decoder, n : Int) -> Unit raise DecodeError {
  self.need(n)
  self.pos += n
}

///|
pub fn Decoder::read_bytes(self : Decoder, n : Int) -> Bytes raise DecodeError {
  self.need(n)
  let out : Array[Byte] = Array::new(capacity=n)
  for _ in 0..<n {
    out.push(self.data[self.pos])
    self.pos += 1
  }
  Bytes::from_array(out)
}

///|
fn Decoder::read_raw_uvarint(self : Decoder) -> UInt raise DecodeError {
  let mut result = 0U
  for shift = 0; ; shift = shift + 7 {
    if shift >= 35 {
      raise DecodeError::Malformed("uvarint too long")
    }
    let b = self.read_byte().to_int().reinterpret_as_uint()
    result = result | ((b & 0x7FU) << shift)
    if (b & 0x80U) == 0U {
      break
    }
  }
  result
}

///|
/// Unsigned LEB128 varint.
pub fn Decoder::read_uvarint(self : Decoder) -> Int raise DecodeError {
  let v = self.read_raw_uvarint()
  if v > 0x7FFFFFFFU {
    raise DecodeError::Malformed("uvarint overflows int32")
  }
  v.reinterpret_as_int()
}

///|
/// Zig-zag varint.
pub fn Decoder::read_varint(self : Decoder) -> Int raise DecodeError {
  let u = self.read_raw_uvarint()
  ((u >> 1) ^ (0U - (u & 1U))).reinterpret_as_int()
}

///|
/// Zig-zag varlong.
pub fn Decoder::read_varlong(self : Decoder) -> Int64 raise DecodeError {
  let mut result = 0UL
  for shift = 0; ; shift = shift + 7 {
    if shift >= 70 {
      raise DecodeError::Malformed("uvarint too long")
    }
    let b = self.read_byte().to_int64().reinterpret_as_uint64()
    result = result | ((b & 0x7FUL) << shift)
    if (b & 0x80UL) == 0UL {
      break
    }
  }
  ((result >> 1) ^ (0UL - (result & 1UL))).reinterpret_as_int64()
}

///|
/// Compact length prefix (value + 1); returns -1 for the null marker.
pub fn Decoder::read_compact_len(self : Decoder) -> Int raise DecodeError {
  self.read_uvarint() - 1
}

///|
pub fn Decoder::read_compact_string(self : Decoder) -> String raise DecodeError {
  match self.read_compact_nullable_string() {
    Some(s) => s
    None => raise DecodeError::Malformed("unexpected null compact string")
  }
}

///|
pub fn Decoder::read_compact_nullable_string(
  self : Decoder,
) -> String? raise DecodeError {
  let len = self.read_compact_len()
  if len < 0 {
    None
  } else {
    Some(@utf8.decode_lossy(self.read_bytes(len)[:]))
  }
}

///|
/// Read and discard a tag buffer.
pub fn Decoder::skip_tag_buffer(self : Decoder) -> Unit raise DecodeError {
  let count = self.read_uvarint()
  for _ in 0..<count {
    let _tag = self.read_uvarint()
    let size = self.read_uvarint()
    self.skip(size)
  }
}
