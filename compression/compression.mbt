// Compression codecs for RecordBatch v2 (plan decision D4). The
// attribute bits 0-2 of the batch header select the codec:
//   0 none, 1 gzip, 2 snappy (xerial framing), 3 lz4 (frame format),
//   4 zstd. Batches are compressed whole — never per record.
//
// Snappy and LZ4 are pure MoonBit; gzip wraps the system zlib and zstd
// wraps the vendored zstd decompressor via native FFI (this module is
// native-only). gzip/zstd compression support lands with the producer's
// codec config in Phase 3.

///|
pub(all) struct Codec {
  /// RecordBatch attribute-bit value (0-4).
  id : Int
  name : String
  compress : (Bytes) -> Bytes raise @buf.DecodeError
  decompress : (Bytes) -> Bytes raise @buf.DecodeError
}

///|
/// The codecs every consumer must understand, in attribute-bit order.
pub fn codecs() -> Array[Codec] {
  [
    { id: 0, name: "none", compress: data => data, decompress: data => data, },
    {
      id: 1,
      name: "gzip",
      compress: data => gzip_compress(data),
      decompress: data => gzip_decompress(data),
    },
    {
      id: 2,
      name: "snappy",
      compress: data => snappy_compress(data),
      decompress: data => snappy_decompress(data),
    },
    {
      id: 3,
      name: "lz4",
      compress: data => lz4_compress(data),
      decompress: data => lz4_decompress(data),
    },
    {
      id: 4,
      name: "zstd",
      compress: data => zstd_compress(data),
      decompress: data => zstd_decompress(data),
    },
  ]
}

///|
/// The codec for a RecordBatch attribute-bit value, if one exists
/// (values above 4 are reserved).
pub fn codec_by_id(id : Int) -> Codec? {
  for codec in codecs() {
    if codec.id == id {
      return Some(codec)
    }
  }
  None
}
