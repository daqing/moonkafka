// Codec tests: round-trips for every codec, golden decodes of streams
// produced by independent compressors (see test/golden/generate.py),
// and whole-batch decoding of foreign-compressed record regions.

///|
test "xxh32 matches the canonical spec vectors" {
  // Published xxHash test vectors.
  assert_eq(@compression.xxh32(b"", 0U), 0x02CC5D05U)
  assert_eq(@compression.xxh32(b"a", 0U), 0x550D7456U)
  assert_eq(@compression.xxh32(b"abc", 0U), 0x32D153FFU)
}

///|
test "codec registry maps attribute bits" {
  assert_eq(codec_by_id(0).unwrap().name, "none")
  assert_eq(codec_by_id(4).unwrap().name, "zstd")
  assert_true(codec_by_id(5) is None)
  assert_eq(codecs().length(), 5)
}
