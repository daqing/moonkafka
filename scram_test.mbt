// Blackbox tests for the SCRAM client primitives. PBKDF2 vectors are from
// RFC 7914 section 11; the HMAC vector follows RFC 4231 case 2; the SCRAM
// exchange replays the RFC 7677 section 3 example (user / pencil), with the
// expected proof and server signature cross-computed independently.

///|
fn hex_to_bytes(hex : String) -> Bytes {
  let out : Array[Byte] = []
  let chars : Array[Char] = []
  for c in hex {
    chars.push(c)
  }
  fn nibble(c : Char) -> Int raise {
    let v = c.to_int()
    if v >= 48 && v <= 57 {
      v - 48
    } else if v >= 97 && v <= 102 {
      v - 87
    } else {
      abort("bad hex")
    }
  }

  for i in 0..<(chars.length() / 2) {
    let hi = nibble(chars[i * 2]) catch { _ => 0 }
    let lo = nibble(chars[i * 2 + 1]) catch { _ => 0 }
    out.push(((hi << 4) | lo).to_byte())
  }
  Bytes::from_array(out)
}

///|
test "pbkdf2-hmac-sha256 matches RFC 7914 vectors" {
  let password = @utf8.encode("password")
  let salt = @utf8.encode("salt")
  assert_eq(
    @moonkafka.scram_pbkdf2_hmac(@crypto.SHA256::new(), password, salt, 1, 32),
    hex_to_bytes(
      "120fb6cffcf8b32c43e7225256c4f837a86548c92ccc35480805987cb70be17b",
    ),
  )
  assert_eq(
    @moonkafka.scram_pbkdf2_hmac(@crypto.SHA256::new(), password, salt, 2, 32),
    hex_to_bytes(
      "ae4d0c95af6b46d32d0adff928f06dd02a303f8ef3c251dfd6e2d85a95474c43",
    ),
  )
  assert_eq(
    @moonkafka.scram_pbkdf2_hmac(
      @crypto.SHA256::new(),
      password,
      salt,
      4096,
      32,
    ),
    hex_to_bytes(
      "c5e478d59288c841aa530db6845c4c8d962893a001ce4e11a4963873aa98134a",
    ),
  )
}

///|
test "pbkdf2-hmac-sha512 matches reference vectors" {
  let password = @utf8.encode("password")
  let salt = @utf8.encode("salt")
  assert_eq(
    @moonkafka.scram_pbkdf2_hmac(@crypto.SHA512::new(), password, salt, 1, 64),
    hex_to_bytes(
      "867f70cf1ade02cff3752599a3a53dc4af34c7a669815ae5d513554e1c8cf252c02d470a285a0501bad999bfe943c08f050235d7d68b1da55e63f73b60a57fce",
    ),
  )
  assert_eq(
    @moonkafka.scram_pbkdf2_hmac(@crypto.SHA512::new(), password, salt, 2, 64),
    hex_to_bytes(
      "e1d9c16aa681708a45f5c7c4e215ceb66e011a2e9f0040713f18aefdb866d53cf76cab2868a39b9f7840edce4fef5a82be67335c77a6068e04112754f27ccf4e",
    ),
  )
}

///|
test "hmac-sha256 matches RFC 4231 vector" {
  let mac = @crypto.hmac(
    @crypto.SHA256::new(),
    @utf8.encode("key")[:],
    @utf8.encode("The quick brown fox jumps over the lazy dog")[:],
  )
  assert_eq(
    @crypto.bytes_to_hex_string(mac),
    "f7bc83f430538424b13298e6aa6fb143ef4d59a14946175997479dbc2d1a3cd8",
  )
}

///|
test "scram client final replays the RFC 7677 exchange" {
  // RFC 7677 section 3: user / pencil, 4096 iterations.
  let salt = @base64.decode("QSXCR+Q6sek8bf92"[:])
  let salted = @moonkafka.scram_salted_password(
    false,
    @utf8.encode("pencil"),
    salt,
    4096,
  )
  let client_first_bare = "n=user,r=fyko+d2lbbFgONRv9qkxdawL"
  let server_first = "r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,i=4096"
  let client_final_without_proof = "c=biws,r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j"
  let auth_message = @moonkafka.scram_auth_message(
    client_first_bare, server_first, client_final_without_proof,
  )
  assert_eq(
    @moonkafka.scram_client_proof_b64(false, salted, auth_message),
    "qQRLRHGPDGjB+7iVAE7NNi5xEoHKHuLCHPNQ8BTmvds=",
  )
  assert_eq(
    @moonkafka.scram_server_signature_b64(false, salted, auth_message),
    "XKW6VuW1FANROQabnJBz1KaeCnQL/HZByQtX/iU+o30=",
  )
}

///|
test "scram server-first parsing" {
  let parsed = @moonkafka.ScramServerFirst::parse(
    "r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,i=4096",
  )
  assert_eq(parsed.nonce, "fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j")
  assert_eq(parsed.iterations, 4096)
  assert_eq(parsed.salt, @base64.decode("QSXCR+Q6sek8bf92"[:]))
}

///|
test "scram escapes special characters in usernames" {
  assert_eq(@moonkafka.scram_escape_username("a=b,c"), "a=3Db=2Cc")
  assert_eq(@moonkafka.scram_escape_username("plain"), "plain")
}
