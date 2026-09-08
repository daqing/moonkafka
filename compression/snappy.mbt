// Snappy raw block format plus Kafka's xerial chunk framing.
//
// Raw block: varint uncompressed length, then a sequence of elements
// tagged by their low two bits — literals (00), 1-byte-offset copies
// (01), 2-byte-offset copies (10), 4-byte-offset copies (11). Copies
// reference earlier output and may overlap (byte-by-byte copy).
//
// Xerial framing (what Kafka's producer emits via snappy-java): magic
// 0x82 "SNAPPY", int32 version, int32 compatibility version, then
// chunks — each an int32 BE compressed length followed by one raw block
// that decompresses to up to 32 KiB of input.

///|
fn read_snappy_uvarint(d : @buf.Decoder) -> Int raise @buf.DecodeError {
  let mut result = 0
  let mut shift = 0
  while shift <= 28 {
    let b = d.read_byte().to_int()
    result = result | ((b & 0x7F) << shift)
    if (b & 0x80) == 0 {
      return result
    }
    shift += 7
  }
  raise @buf.Malformed("snappy length varint too long")
}

///|
fn write_snappy_uvarint(e : @buf.Encoder, n : Int) -> Unit {
  let mut n = n
  while n >= 0x80 {
    e.write_byte(((n & 0x7F) | 0x80).to_byte())
    n = n >> 7
  }
  e.write_byte(n.to_byte())
}

///|
/// Decode a snappy raw block to its original bytes.
pub fn snappy_decode_block(data : Bytes) -> Bytes raise @buf.DecodeError {
  let d = @buf.Decoder::new(data)
  let expected = read_snappy_uvarint(d)
  if expected < 0 {
    raise @buf.Malformed("snappy negative output length")
  }
  let out : Array[Byte] = Array::new(capacity=expected)
  while d.remaining() > 0 {
    let tag = d.read_byte().to_int()
    match tag & 0x03 {
      0 => {
        // Literal: upper six bits +1 is the length (60-63 escape).
        let mut len = (tag >> 2) + 1
        if len > 60 {
          // Values 60-63 escape into 1-4 extra little-endian bytes, and
          // like the small encoding the stored value is length - 1.
          let extra = len - 60
          len = 0
          for i in 0..<extra {
            len = len | (d.read_byte().to_int() << (8 * i))
          }
          len += 1
        }
        if len < 0 || len > d.remaining() {
          raise @buf.Malformed("snappy literal overruns input")
        }
        for _ in 0..<len {
          out.push(d.read_byte())
        }
      }
      kind => {
        // Copies: length and offset encoding varies by tag kind.
        let (len, offset) = match kind {
          1 => {
            let len = ((tag >> 2) & 0x07) + 4
            let offset = (tag >> 5 << 8) | d.read_byte().to_int()
            (len, offset)
          }
          2 => {
            let len = (tag >> 2) + 1
            let lo = d.read_byte().to_int()
            let hi = d.read_byte().to_int()
            (len, lo | (hi << 8))
          }
          _ => {
            let len = (tag >> 2) + 1
            let mut offset = 0
            for i in 0..<4 {
              offset = offset | (d.read_byte().to_int() << (8 * i))
            }
            (len, offset)
          }
        }
        if offset <= 0 || offset > out.length() {
          raise @buf.Malformed("snappy copy offset out of range")
        }
        // Overlapping copies replicate byte-by-byte on purpose.
        let mut src = out.length() - offset
        for _ in 0..<len {
          out.push(out[src])
          src += 1
        }
      }
    }
  }
  if out.length() != expected {
    raise @buf.Malformed(
      "snappy decoded \{out.length()} bytes, header promised \{expected}",
    )
  }
  Bytes::from_array(out)
}

///|
/// Encode a snappy raw block: a greedy hash-chain matcher over 4-byte
/// sequences with literal fallback. Produces valid blocks (decode
/// round-trips); throughput and ratio are not production targets.
pub fn snappy_encode_block(data : Bytes) -> Bytes {
  let n = data.length()
  let e = @buf.Encoder::new()
  write_snappy_uvarint(e, n)
  // 14-bit hash table of candidate positions; 0 means empty (we offset
  // positions by 1).
  let table_size = 1 << 14
  let table : Array[Int] = Array::make(table_size, 0)
  let hash = fn(i : Int) -> Int {
    let v = (data[i].to_int() << 24) |
      (data[i + 1].to_int() << 16) |
      (data[i + 2].to_int() << 8) |
      data[i + 3].to_int()
    ((v * 0x1E35A7BD) >> (32 - 14)) & (table_size - 1)
  }
  let mut anchor = 0
  let mut i = 0
  let limit = n - 4
  while i <= limit {
    let candidate = table[hash(i)] - 1
    table[hash(i)] = i + 1
    if candidate >= 0 &&
      candidate < i &&
      data[candidate] == data[i] &&
      data[candidate + 1] == data[i + 1] &&
      data[candidate + 2] == data[i + 2] &&
      data[candidate + 3] == data[i + 3] {
      // Extend the match as far as it goes.
      let mut len = 4
      while i + len < n && data[candidate + len] == data[i + len] && len < 64 {
        len += 1
      }
      // Emit the pending literal run first.
      if i > anchor {
        write_snappy_literal(e, data, anchor, i - anchor)
      }
      let offset = i - candidate
      if len < 12 && offset < 2048 {
        // Copy1: 3-bit length, 11-bit offset.
        e.write_byte((0x01 | ((len - 4) << 2) | (offset >> 8 << 5)).to_byte())
        e.write_byte((offset & 0xFF).to_byte())
      } else if len <= 64 {
        // Copy2: 6-bit length, 16-bit offset.
        e.write_byte((0x02 | ((len - 1) << 2)).to_byte())
        e.write_byte((offset & 0xFF).to_byte())
        e.write_byte(((offset >> 8) & 0xFF).to_byte())
      } else {
        // Copy4.
        e.write_byte((0x03 | ((len - 1) << 2)).to_byte())
        for shift in [0, 8, 16, 24] {
          e.write_byte(((offset >> shift) & 0xFF).to_byte())
        }
      }
      i += len
      anchor = i
    } else {
      i += 1
    }
  }
  if anchor < n {
    write_snappy_literal(e, data, anchor, n - anchor)
  }
  e.to_bytes()
}

///|
fn write_snappy_literal(
  e : @buf.Encoder,
  data : Bytes,
  start : Int,
  len : Int,
) -> Unit {
  let mut pos = start
  let mut remaining = len
  while remaining > 0 {
    // Chunks of up to 60 bytes fit their length in the tag; larger runs
    // spend 1-2 extra length bytes (4-byte escapes are never needed at
    // these chunk sizes).
    let (take, escape) = if remaining <= 60 {
      (remaining, 0)
    } else if remaining <= 0xFF {
      (Int::min(0xFF, remaining), 1)
    } else {
      (Int::min(0xFFFF, remaining), 2)
    }
    if escape == 0 {
      e.write_byte((0x00 | ((take - 1) << 2)).to_byte())
    } else if escape == 1 {
      e.write_byte((0x00 | (60 << 2)).to_byte())
      e.write_byte(take.to_byte())
    } else {
      e.write_byte((0x00 | (61 << 2)).to_byte())
      e.write_byte((take & 0xFF).to_byte())
      e.write_byte(((take >> 8) & 0xFF).to_byte())
    }
    for i in 0..<take {
      e.write_byte(data[pos + i])
    }
    pos += take
    remaining -= take
  }
}

///|
/// Decode Kafka's xerial-framed snappy payload (chunked).
pub fn snappy_decompress(data : Bytes) -> Bytes raise @buf.DecodeError {
  if data.length() < 16 {
    raise @buf.Malformed("xerial payload too short")
  }
  // Magic: 0x82 "SNAPPY"
  let magic_ok = data[0] == b'\x82' &&
    data[1] == b'S' &&
    data[2] == b'N' &&
    data[3] == b'A' &&
    data[4] == b'P' &&
    data[5] == b'P' &&
    data[6] == b'Y'
  if !magic_ok {
    raise @buf.Malformed("xerial magic mismatch")
  }
  let d = @buf.Decoder::new(data, start=15) // 1 magic byte + "SNAPPY" + int32 version + int32 compat
  let out : Array[Byte] = []
  while d.remaining() > 0 {
    if d.remaining() < 4 {
      raise @buf.Malformed("xerial truncated chunk length")
    }
    let chunk_len = d.read_i32()
    if chunk_len < 0 || chunk_len > d.remaining() {
      raise @buf.Malformed("xerial chunk length overruns payload")
    }
    let chunk = snappy_decode_block(d.read_bytes(chunk_len))
    for b in chunk {
      out.push(b)
    }
  }
  Bytes::from_array(out)
}

///|
/// Encode with xerial framing: independent 32 KiB chunks like
/// snappy-java's SnappyOutputStream.
pub fn snappy_compress(data : Bytes) -> Bytes {
  let e = @buf.Encoder::new()
  e.write_byte(b'\x82')
  e.write_byte(b'S')
  e.write_byte(b'N')
  e.write_byte(b'A')
  e.write_byte(b'P')
  e.write_byte(b'P')
  e.write_byte(b'Y')
  e.write_i32(1) // version
  e.write_i32(1) // compatible version
  let chunk_size = 32768
  let mut pos = 0
  if data.length() == 0 {
    e.write_i32(0) // one empty chunk, like snappy-java
  }
  while pos < data.length() {
    let take = if pos + chunk_size > data.length() {
      data.length() - pos
    } else {
      chunk_size
    }
    let slice : Array[Byte] = Array::new(capacity=take)
    for i in 0..<take {
      slice.push(data[pos + i])
    }
    let compressed = snappy_encode_block(Bytes::from_array(slice))
    e.write_i32(compressed.length())
    e.write_bytes(compressed)
    pos += take
  }
  e.to_bytes()
}
