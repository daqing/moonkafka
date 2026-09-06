// LZ4 block format plus Kafka's LZ4 frame framing.
//
// Frame (what Kafka's producer emits): magic 0x184D2204 (little-endian
// on the wire), frame descriptor — FLG byte (bits: reserved 0-1,
// contentChecksum 2, contentSize 3, blockChecksum 4, blockIndependence
// 5, version 6-7 = 1), BD byte (bits 4-6: max block size, 4=64KB up to
// 7=4MB), HC byte (XXH32 of the descriptor >> 8) — then data blocks
// (int32 LE size; bit 31 set means stored uncompressed), each followed
// by an optional XXH32 block checksum, an end mark (size 0), and an
// optional XXH32 content checksum.
//
// Block: sequences of a token byte (high nibble literal length, low
// nibble match length - 4), extended lengths, literals, and matches
// referenced by little-endian offsets. The block ends with a
// literals-only sequence covering the tail (the last 12 bytes never
// start a match, per the end-of-block conditions).

///|
/// XXH32 (the 32-bit xxHash) as the LZ4 frame spec requires it.
pub fn xxh32(data : Bytes, seed : UInt) -> UInt {
  let prime1 : UInt = 2654435761U
  let prime2 : UInt = 2246822519U
  let prime3 : UInt = 3266489917U
  let prime4 : UInt = 668265263U
  let prime5 : UInt = 374761393U
  let n = data.length()
  let mut i = 0
  let mut acc : UInt = if n >= 16 {
    i = 16
    let a = read_u32_le(data, 0).add(seed).mul(prime1)
    let b = read_u32_le(data, 4).add(seed).mul(prime1)
    let c = read_u32_le(data, 8).add(seed).mul(prime1)
    let dd = read_u32_le(data, 12).add(seed).mul(prime1)
    ((a << 13) | (a >> 29))
    .mul(prime2)
    .add(((b << 13) | (b >> 29)).mul(prime2))
    .add(((c << 13) | (c >> 29)).mul(prime2))
    .add(((dd << 13) | (dd >> 29)).mul(prime2))
  } else {
    seed.add(prime5)
  }
  acc = acc.add(n.reinterpret_as_uint())
  while i + 4 <= n {
    acc = acc.add(read_u32_le(data, i).mul(prime3))
    acc = ((acc << 17) | (acc >> 15)).mul(prime4)
    i += 4
  }
  while i < n {
    acc = acc.add(data[i].to_uint().mul(prime5))
    acc = ((acc << 11) | (acc >> 21)).mul(prime1)
    i += 1
  }
  acc = acc ^ (acc >> 15)
  acc = acc.mul(prime2)
  acc = acc ^ (acc >> 13)
  acc = acc.mul(prime3)
  acc ^ (acc >> 16)
}

///|
fn read_u32_le(data : Bytes, at : Int) -> UInt {
  data[at].to_uint() |
  (data[at + 1].to_uint() << 8) |
  (data[at + 2].to_uint() << 16) |
  (data[at + 3].to_uint() << 24)
}

///|
fn write_u32_le(e : @buf.Encoder, v : UInt) -> Unit {
  for shift in [0, 8, 16, 24] {
    e.write_byte(((v >> shift) & 0xFFU).to_byte())
  }
}

///|
fn lz4_expect(data : Bytes, pos : Int, n : Int) -> Unit raise @buf.DecodeError {
  if pos > data.length() - n {
    raise @buf.Malformed("lz4 input truncated")
  }
}

///|
/// Decode an LZ4 block. `expected` bounds the output; pass -1 when the
/// size is unknown (frame blocks without a content-size header) and the
/// equality check is skipped.
pub fn lz4_decode_block(
  data : Bytes,
  expected : Int,
) -> Bytes raise @buf.DecodeError {
  let n = data.length()
  let cap = if expected < 0 { 64 } else { expected }
  let out : Array[Byte] = Array::new(capacity=cap)
  let mut p = 0
  while p < n {
    lz4_expect(data, p, 1)
    let token = data[p].to_int()
    p += 1
    // Literal length: high nibble, 15 escapes into more bytes.
    let mut lit_len = token >> 4
    if lit_len == 15 {
      let mut b = 255
      while b == 255 {
        lz4_expect(data, p, 1)
        b = data[p].to_int()
        p += 1
        lit_len += b
      }
    }
    if lit_len < 0 || lit_len > n - p {
      raise @buf.Malformed("lz4 literals overrun block")
    }
    for i in 0..<lit_len {
      out.push(data[p + i])
    }
    p += lit_len
    if p >= n {
      break // the last sequence carries no match
    }
    lz4_expect(data, p, 2)
    let offset = data[p].to_int() | (data[p + 1].to_int() << 8)
    p += 2
    if offset == 0 || offset > out.length() {
      raise @buf.Malformed("lz4 match offset out of range")
    }
    let mut match_len = (token & 0x0F) + 4
    if (token & 0x0F) == 15 {
      let mut b = 255
      while b == 255 {
        lz4_expect(data, p, 1)
        b = data[p].to_int()
        p += 1
        match_len += b
      }
    }
    // Overlapping matches replicate byte-by-byte on purpose.
    let mut src = out.length() - offset
    for _ in 0..<match_len {
      out.push(out[src])
      src += 1
    }
  }
  if expected >= 0 && out.length() != expected {
    raise @buf.Malformed(
      "lz4 decoded \{out.length()} bytes, expected \{expected}",
    )
  }
  Bytes::from_array(out)
}

///|
/// Encode an LZ4 block: greedy 4-byte hash matcher (64KB window,
/// matches of 4+ bytes), literals-only tail sequence. Valid blocks;
/// compression ratio is not a design target.
pub fn lz4_encode_block(data : Bytes) -> Bytes {
  let n = data.length()
  let e = @buf.Encoder::new()
  if n == 0 {
    return e.to_bytes()
  }
  let table_size = 1 << 16
  let table : Array[Int] = Array::make(table_size, 0)
  let hash = fn(i : Int) -> Int {
    let v = data[i].to_uint() |
      (data[i + 1].to_uint() << 8) |
      (data[i + 2].to_uint() << 16) |
      (data[i + 3].to_uint() << 24)
    ((v * 2654435761U) >> 16).reinterpret_as_int() & (table_size - 1)
  }
  let mut anchor = 0
  let mut i = 0
  // Matches never start within 12 bytes of the end (MFLIMIT rule).
  let limit = n - 12
  while i < limit {
    let candidate = table[hash(i)] - 1
    table[hash(i)] = i + 1
    if candidate >= 0 &&
      i - candidate <= 65535 &&
      data[candidate] == data[i] &&
      data[candidate + 1] == data[i + 1] &&
      data[candidate + 2] == data[i + 2] &&
      data[candidate + 3] == data[i + 3] {
      let mut len = 4
      // Matches must end at least 5 bytes before the end.
      while i + len < n - 5 && data[candidate + len] == data[i + len] {
        len += 1
      }
      // One sequence: [extended] literal run, then the match.
      let lit_len = i - anchor
      let mut token = 0
      if lit_len < 15 {
        token = lit_len << 4
      } else {
        token = 15 << 4
      }
      if len - 4 < 15 {
        token = token | (len - 4)
      } else {
        token = token | 15
      }
      e.write_byte(token.to_byte())
      if lit_len >= 15 {
        let mut extra = lit_len - 15
        while extra >= 255 {
          e.write_byte(b'\xff')
          extra -= 255
        }
        e.write_byte(extra.to_byte())
      }
      for k in 0..<lit_len {
        e.write_byte(data[anchor + k])
      }
      let offset = i - candidate
      e.write_byte((offset & 0xFF).to_byte())
      e.write_byte(((offset >> 8) & 0xFF).to_byte())
      if len - 4 >= 15 {
        let mut extra = len - 4 - 15
        while extra >= 255 {
          e.write_byte(b'\xff')
          extra -= 255
        }
        e.write_byte(extra.to_byte())
      }
      i += len
      anchor = i
    } else {
      i += 1
    }
  }
  // Tail: a literals-only sequence (low nibble 0, no offset).
  let tail = n - anchor
  if tail < 15 {
    e.write_byte((tail << 4).to_byte())
  } else {
    e.write_byte((15 << 4).to_byte())
    let mut extra = tail - 15
    while extra >= 255 {
      e.write_byte(b'\xff')
      extra -= 255
    }
    e.write_byte(extra.to_byte())
  }
  for k in 0..<tail {
    e.write_byte(data[anchor + k])
  }
  e.to_bytes()
}

///|
/// Decode Kafka's LZ4 frame payload: header, blocks, optional checksums.
pub fn lz4_decompress(data : Bytes) -> Bytes raise @buf.DecodeError {
  let n = data.length()
  if n < 7 {
    raise @buf.Malformed("lz4 frame too short")
  }
  if read_u32_le(data, 0) != 0x184D2204U {
    raise @buf.Malformed("lz4 frame magic mismatch")
  }
  let mut p = 4
  let flg = data[4].to_int()
  let _bd = data[5].to_int()
  let _hc = data[6]
  p = 7
  let version = (flg >> 6) & 0x03
  if version != 1 {
    raise @buf.Malformed("lz4 frame version \{version} unsupported")
  }
  let has_content_checksum = ((flg >> 2) & 1) == 1
  let has_content_size = ((flg >> 3) & 1) == 1
  let has_block_checksum = ((flg >> 4) & 1) == 1
  let mut expected = -1
  if has_content_size {
    lz4_expect(data, p, 8)
    let mut size = 0L
    for i in 0..<8 {
      size = size | (data[p + i].to_int64() << (8 * i))
    }
    p += 8
    expected = size.to_int()
  }
  let out : Array[Byte] = []
  for ;; {
    lz4_expect(data, p, 4)
    let word = read_u32_le(data, p)
    p += 4
    if word == 0U {
      break // end mark
    }
    let uncompressed_flag = ((word >> 31) & 1U) == 1U
    let size = (word & 0x7FFFFFFFU).reinterpret_as_int()
    if size > n - p {
      raise @buf.Malformed("lz4 frame block overruns payload")
    }
    let block = data[p:p + size].to_owned()
    p += size
    if uncompressed_flag {
      for b in block {
        out.push(b)
      }
    } else {
      let decoded = lz4_decode_block(block, -1)
      for b in decoded {
        out.push(b)
      }
    }
    if has_block_checksum {
      p += 4
    }
  }
  if has_content_checksum {
    p += 4
  }
  if expected >= 0 && out.length() != expected {
    raise @buf.Malformed(
      "lz4 frame declared \{expected} bytes, decoded \{out.length()}",
    )
  }
  ignore(n)
  Bytes::from_array(out)
}

///|
/// Encode with Kafka's frame settings: block-independent, block max
/// 64KB, no content/block checksums, XXH32 descriptor checksum.
pub fn lz4_compress(data : Bytes) -> Bytes {
  let e = @buf.Encoder::new()
  write_u32_le(e, 0x184D2204U)
  // FLG: version 1, block independent, no checksums, no content size.
  let flg : Int = (1 << 6) | (1 << 5)
  let bd : Int = 0x40 // 64KB max block size
  e.write_byte(flg.to_byte())
  e.write_byte(bd.to_byte())
  // HC: XXH32 over (FLG, BD), folded to one byte.
  let descriptor : Array[Byte] = [flg.to_byte(), bd.to_byte()]
  let hc = xxh32(Bytes::from_array(descriptor), 0U) >> 8
  e.write_byte((hc & 0xFFU).to_byte())
  let chunk = 65536
  let mut pos = 0
  if data.length() == 0 {
    write_u32_le(e, 0U) // end mark only
  }
  while pos < data.length() {
    let take = if pos + chunk > data.length() {
      data.length() - pos
    } else {
      chunk
    }
    let slice : Array[Byte] = Array::new(capacity=take)
    for i in 0..<take {
      slice.push(data[pos + i])
    }
    let block = lz4_encode_block(Bytes::from_array(slice))
    write_u32_le(e, block.length().reinterpret_as_uint())
    e.write_bytes(block)
    pos += take
  }
  write_u32_le(e, 0U) // end mark
  e.to_bytes()
}
