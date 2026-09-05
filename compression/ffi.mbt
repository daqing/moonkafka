// Native FFI for gzip (system zlib) and zstd (vendored decompressor).
// Gated to the native backend — this module is native-only.

///|
/// Inflate gzip data into dest. Returns bytes written, -2 when dest is
/// too small, -1 on malformed input.
#borrow(src)
#borrow(dest)
extern "c" fn gzip_decompress_into(
  src : Bytes,
  src_len : Int,
  dest : FixedArray[Byte],
  dest_cap : Int,
) -> Int = "moonkafka_gzip_decompress_into"

///|
/// Deflate into a gzip member. Same return conventions.
#borrow(src)
#borrow(dest)
extern "c" fn gzip_compress_into(
  src : Bytes,
  src_len : Int,
  dest : FixedArray[Byte],
  dest_cap : Int,
) -> Int = "moonkafka_gzip_compress_into"

///|
/// Bound zlib reports for the compressed size, plus gzip framing slack.
extern "c" fn gzip_compress_bound(src_len : Int) -> Int = "moonkafka_gzip_compress_bound"

///|
#borrow(src)
#borrow(dest)
extern "c" fn zstd_decompress_into(
  src : Bytes,
  src_len : Int,
  dest : FixedArray[Byte],
  dest_cap : Int,
) -> Int = "moonkafka_zstd_decompress_into"

///|
/// Frame-declared output size: >= 0 known, -1 unknown, -2 corrupt.
#borrow(src)
extern "c" fn zstd_frame_content_size(src : Bytes, src_len : Int) -> Int64 = "moonkafka_zstd_frame_content_size"

///|
const GZIP_ATTEMPTS : Int = 24

///|
pub fn gzip_decompress(data : Bytes) -> Bytes raise @buf.DecodeError {
  let mut cap = 65536
  for _ in 0..<GZIP_ATTEMPTS {
    let dest : FixedArray[Byte] = FixedArray::make(cap, b'\x00')
    let n = gzip_decompress_into(data, data.length(), dest, cap)
    if n >= 0 {
      return Bytes::from_array(dest[0:n].to_owned())
    }
    if n == -2 {
      cap = cap * 2
      continue
    }
    raise @buf.Malformed("gzip decompression failed")
  }
  raise @buf.Malformed("gzip output exceeds \{cap} bytes")
}

///|
pub fn gzip_compress(data : Bytes) -> Bytes raise @buf.DecodeError {
  let cap = gzip_compress_bound(data.length())
  let dest : FixedArray[Byte] = FixedArray::make(cap, b'\x00')
  let n = gzip_compress_into(data, data.length(), dest, cap)
  if n < 0 {
    raise @buf.Malformed("gzip compression failed")
  }
  Bytes::from_array(dest[0:n].to_owned())
}

///|
/// Zstd frames declare their output size; reject unknown-size frames
/// rather than guessing (Kafka's producers always declare it).
pub fn zstd_decompress(data : Bytes) -> Bytes raise @buf.DecodeError {
  let declared = zstd_frame_content_size(data, data.length())
  if declared < 0 {
    raise @buf.Malformed(
      "zstd frame content size unknown or corrupt (\{declared})",
    )
  }
  if declared > 1 << 30 {
    raise @buf.Malformed("zstd frame declares over 1GB of output")
  }
  let cap = declared.to_int()
  let dest : FixedArray[Byte] = FixedArray::make(cap, b'\x00')
  let n = zstd_decompress_into(data, data.length(), dest, cap)
  if n < 0 {
    raise @buf.Malformed("zstd decompression failed")
  }
  Bytes::from_array(dest[0:n].to_owned())
}

///|
/// Zstd compression arrives with the producer's codec config in Phase 3
/// (the vendored decompressor is decode-only).
pub fn zstd_compress(data : Bytes) -> Bytes raise @buf.DecodeError {
  ignore(data)
  raise @buf.Malformed("zstd compression is not supported yet")
}
