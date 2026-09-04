// Blackbox tests for murmur2. Expected values are the vectors from Apache
// Kafka's UtilsTest#testMurmur2; passing them proves byte-for-byte
// compatibility with the Java client's default partitioner.

///|
test "murmur2 matches Kafka's UtilsTest vectors" {
  assert_eq(@moonkafka.murmur2(@utf8.encode("21")), -973932308)
  assert_eq(@moonkafka.murmur2(@utf8.encode("foobar")), -790332482)
  assert_eq(
    @moonkafka.murmur2(@utf8.encode("a-little-bit-long-string")),
    -985981536,
  )
  assert_eq(
    @moonkafka.murmur2(@utf8.encode("a-little-bit-longer-string")),
    -1486304829,
  )
  assert_eq(
    @moonkafka.murmur2(
      @utf8.encode("lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8"),
    ),
    -58897971,
  )
  assert_eq(@moonkafka.murmur2(b"abc"), 479470107)
}

///|
test "murmur2 handles byte lengths not divisible by 4" {
  // 0, 1, 2, 3, 5 and 7 byte inputs exercise every tail branch; values are
  // cross-checked against an independent reference implementation of
  // Utils.murmur2.
  assert_eq(@moonkafka.murmur2(b""), 275646681)
  assert_eq(@moonkafka.murmur2(b"x"), 2121525046)
  assert_eq(@moonkafka.murmur2(b"ab"), 316155434)
  assert_eq(@moonkafka.murmur2(b"abc"), 479470107)
  assert_eq(@moonkafka.murmur2(b"abcd"), -1323649548)
  assert_eq(@moonkafka.murmur2(b"abcde"), 461995741)
  assert_eq(@moonkafka.murmur2(b"abcdefg"), -346467175)
}
