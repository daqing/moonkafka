// Kafka Uuid (org.apache.kafka.common.Uuid): 128-bit identifiers used for
// topic ids on the wire and, later, client-generated consumer-group member
// ids. Canonical string form is Java's: 22-char unpadded URL-safe Base64.

///|
pub struct Uuid {
  bytes : Bytes
} derive(Eq, Compare, Hash, @debug.Debug)

///|
/// The all-zero uuid, Kafka's sentinel for "no id".
pub fn Uuid::zero() -> Uuid {
  { bytes: Bytes::make(16, b'\x00'), }
}

///|
/// Wrap exactly 16 bytes; anything else is a wire-level violation.
pub fn Uuid::from_bytes(bytes : Bytes) -> Uuid raise @buf.DecodeError {
  if bytes.length() != 16 {
    raise @buf.Malformed("uuid must be 16 bytes, got \{bytes.length()}")
  }
  { bytes, }
}

///|
pub fn Uuid::to_bytes(self : Uuid) -> Bytes {
  self.bytes
}

///|
/// Canonical 22-char unpadded URL-safe Base64, identical to Java
/// Uuid.toString(): five 3-byte groups (20 chars) plus one trailing byte
/// (2 chars, low 4 bits zero-padded).
pub impl Show for Uuid with fn output(self, logger) {
  let sb = StringBuilder()
  let mut acc : Int = 0
  let mut count : Int = 0
  for b in self.bytes {
    acc = ((acc << 8) | b.to_int()) & 0xFFFFFF
    count = count + 1
    if count == 3 {
      sb.write_string(b64url_chars[(acc >> 18) & 0x3F])
      sb.write_string(b64url_chars[(acc >> 12) & 0x3F])
      sb.write_string(b64url_chars[(acc >> 6) & 0x3F])
      sb.write_string(b64url_chars[acc & 0x3F])
      count = 0
    }
  }
  // Exactly one trailing byte remains (16 = 5*3 + 1).
  sb.write_string(b64url_chars[(acc >> 2) & 0x3F])
  sb.write_string(b64url_chars[(acc << 4) & 0x3F])
  logger <+ "\{sb.to_string()}"
}

///|
let b64url_chars : Array[String] = [
  "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P",
  "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z", "a", "b", "c", "d", "e", "f",
  "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v",
  "w", "x", "y", "z", "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "-", "_",
]

///|
fn uuid_char_value(c : Char) -> Int raise @buf.DecodeError {
  let v : Int = c.to_int()
  if v >= 65 && v <= 90 {
    v - 65 // A-Z
  } else if v >= 97 && v <= 122 {
    v - 71 // a-z
  } else if v >= 48 && v <= 57 {
    v + 4 // 0-9
  } else if v == 45 {
    62 // -
  } else if v == 95 {
    63 // _
  } else {
    raise @buf.Malformed("invalid base64url character code \{v}")
  }
}

///|
/// Parse the canonical 22-char form (as produced by Show). The zero padding
/// in the trailing character's low bits is ignored, like Java's decoder.
pub fn Uuid::parse(s : String) -> Uuid raise @buf.DecodeError {
  if s.length() != 22 {
    raise @buf.Malformed("uuid string must be 22 chars, got \{s.length()}")
  }
  let out : Array[Byte] = Array::new(capacity=16)
  let mut acc : Int = 0
  let mut bits : Int = 0
  for c in s {
    acc = ((acc << 6) | uuid_char_value(c)) & 0xFFFFFF
    bits = bits + 6
    if bits == 24 {
      out.push(((acc >> 16) & 0xFF).to_byte())
      out.push(((acc >> 8) & 0xFF).to_byte())
      out.push((acc & 0xFF).to_byte())
      bits = 0
    }
  }
  // 22 chars = 5 full groups (15 bytes) + 2 chars holding the final byte
  // in their top 8 of 12 bits.
  out.push(((acc >> 4) & 0xFF).to_byte())
  { bytes: Bytes::from_array(out), }
}
