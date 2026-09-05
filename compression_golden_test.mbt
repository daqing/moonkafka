// Golden decode tests: streams produced by independent compressors
// (cramjam's Rust codecs + Python gzip; see test/golden/generate.py),
// decoded through moonkafka's codecs and whole-batch pipeline.

///|
test "golden streams decode to the exact payload" {
  assert_eq(@compression.gzip_decompress(golden_gzip), golden_payload)
  assert_eq(@compression.snappy_decompress(golden_snappy), golden_payload)
  assert_eq(@compression.lz4_decompress(golden_lz4), golden_payload)
  assert_eq(@compression.zstd_decompress(golden_zstd), golden_payload)
}

///|
test "batches with foreign-compressed regions decode" {
  // GOLDEN_BATCH_* are whole RecordBatch v2s whose records region was
  // compressed by foreign implementations and spliced in with a
  // repaired CRC — what a codec-configured producer puts on the wire.
  let batches = [
    (golden_batch_gzip, 1),
    (golden_batch_snappy, 2),
    (golden_batch_lz4, 3),
    (golden_batch_zstd, 4),
  ]
  for pair in batches {
    let (batch, codec_id) = pair
    assert_eq(@compression.codec_by_id(codec_id).unwrap().name != "", true)
    let records = @moonkafka.decode_record_batches(batch)
    assert_eq(records.length(), 1)
    assert_eq(records[0].offset, 0L)
    assert_eq(records[0].key, Some(b"k"))
    assert_eq(records[0].value, Some(b"v"))
  }
}
