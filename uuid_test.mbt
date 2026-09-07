// Blackbox tests for Uuid. String forms were computed with a standard
// base64url implementation and match Java Uuid.toString() semantics
// (unpadded URL-safe Base64 of the 16 bytes).

///|
test "uuid string form matches base64url vectors" {
  let forms : Array[(@moonkafka.Uuid, String)] = [
    (@moonkafka.Uuid::zero(), "AAAAAAAAAAAAAAAAAAAAAA"),
    (
      @moonkafka.Uuid::from_bytes(
        b"\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f",
      ),
      "AAECAwQFBgcICQoLDA0ODw",
    ),
    (
      @moonkafka.Uuid::from_bytes(
        b"\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff",
      ),
      "_____________________w",
    ),
    (@moonkafka.Uuid::from_bytes(b"moonkafka-uuid!!"), "bW9vbmthZmthLXV1aWQhIQ"),
  ]
  for pair in forms {
    let (uuid, form) = pair
    assert_eq(uuid.to_string(), form)
  }
}

///|
test "uuid parse is the inverse of to_string" {
  let forms = [
    "AAAAAAAAAAAAAAAAAAAAAA", "AAECAwQFBgcICQoLDA0ODw", "_____________________w",
    "bW9vbmthZmthLXV1aWQhIQ",
  ]
  for form in forms {
    let uuid = @moonkafka.Uuid::parse(form)
    assert_eq(uuid.to_string(), form)
  }
  try @moonkafka.Uuid::parse("AAAAAAAAAAAAAAAAAAAAAA") catch {
    _ => fail("zero form must parse")
  } noraise {
    uuid => assert_eq(uuid, @moonkafka.Uuid::zero())
  }
}

///|
test "uuid parse rejects bad input" {
  try @moonkafka.Uuid::parse("tooshort") catch {
    _ => ()
  } noraise {
    _ => fail("expected parse to raise on short input")
  }
  try @moonkafka.Uuid::parse("!!!!!!!!!!!!!!!!!!!!!!") catch {
    _ => ()
  } noraise {
    _ => fail("expected parse to raise on invalid characters")
  }
}

///|
test "uuid from_bytes validates length" {
  try @moonkafka.Uuid::from_bytes(b"short") catch {
    _ => ()
  } noraise {
    _ => fail("expected from_bytes to raise on short input")
  }
  try @moonkafka.Uuid::from_bytes(b"0123456789abcdef") catch {
    _ => fail("16 bytes must be accepted")
  } noraise {
    uuid => assert_eq(uuid.to_string(), "MDEyMzQ1Njc4OWFiY2RlZg")
  }
}

///|
test "uuid equality and distinctness" {
  let a = @moonkafka.Uuid::from_bytes(b"0123456789abcdef")
  let a_again = @moonkafka.Uuid::parse("MDEyMzQ1Njc4OWFiY2RlZg")
  let b = @moonkafka.Uuid::zero()
  assert_eq(a.to_bytes().length(), 16)
  assert_eq(a, a_again)
  assert_false(a == b)
  assert_eq(a.to_string().length(), 22)
}
