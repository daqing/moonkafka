///|
test "fake sasl auth bytes parser" {
  let body = @moonkafka.encode_sasl_authenticate_request(
    @utf8.encode("n,,n=admin,r=nonce"),
  )
  let e = @buf.Encoder::new()
  e.write_i16(36)
  e.write_i16(2)
  e.write_i32(7)
  e.write_nullable_string(Some("moonkafka"))
  e.write_tag_buffer()
  e.write_bytes(body)
  let auth = fake_sasl_auth_bytes(e.to_bytes())
  assert_eq(auth, @utf8.encode("n,,n=admin,r=nonce"))
}

///|
test "fake handshake response lists scram mechanisms" {
  let d = @buf.Decoder::new(fake_sasl_handshake_response())
  let result = @moonkafka.decode_sasl_handshake_response(d)
  assert_eq(result.error_code, 0)
  assert_eq(result.mechanisms.length(), 3)
}
