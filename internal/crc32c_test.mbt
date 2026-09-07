// CRC-32C vectors from RFC 3720 / the Castagnoli CRC test set.

///|
test "crc32c known vectors" {
  // Reference values from RFC 3720 / the Castagnoli CRC test vectors.
  assert_eq(@internal.crc32c(b""), 0U)
  assert_eq(@internal.crc32c(@utf8.encode("abc")), 0x364B3FB7U)
  assert_eq(@internal.crc32c(@utf8.encode("123456789")), 0xE3069283U)
}
