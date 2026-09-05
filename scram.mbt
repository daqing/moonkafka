// SCRAM client mechanism (RFC 5802; SCRAM-SHA-256 per RFC 7677).
//
// Hashing and HMAC come from moonbitlang/x/crypto; Hi() is PBKDF2-HMAC,
// implemented here as a thin loop. SCRAM nonces need uniqueness, not
// secrecy, so the client nonce derives from a hash of the wall clock and
// the client id.

///|
/// PBKDF2-HMAC (RFC 2898) producing exactly dk_len bytes; SCRAM always
/// uses dk_len equal to the hash output length, i.e. a single block.
pub fn[H : @crypto.CryptoHasher] scram_pbkdf2_hmac(
  h : H,
  password : Bytes,
  salt : Bytes,
  iterations : Int,
  dk_len : Int,
) -> Bytes {
  let hlen = h.size()
  let mut u : FixedArray[Byte] = FixedArray::make(hlen, b'\x00')
  // U1 = HMAC(password, salt || INT(block index 1))
  let msg : Array[Byte] = []
  for b in salt {
    msg.push(b)
  }
  msg.push(b'\x00')
  msg.push(b'\x00')
  msg.push(b'\x00')
  msg.push(b'\x01')
  let u1 = @crypto.hmac(h, password[:], Bytes::from_array(msg)[:])
  for i in 0..<dk_len {
    u[i] = u1[i]
  }
  let out = FixedArray::make(dk_len, b'\x00')
  for i in 0..<dk_len {
    out[i] = u[i]
  }
  // Ui = HMAC(password, U(i-1)), XORed into the result
  for _ in 1..<iterations {
    u = @crypto.hmac(h, password[:], scram_fixed_to_bytes(u)[:])
    for i in 0..<dk_len {
      out[i] = out[i] ^ u[i]
    }
  }
  scram_fixed_to_bytes(out)
}

///|
fn scram_fixed_to_bytes(fa : FixedArray[Byte]) -> Bytes {
  let out = Array::new(capacity=fa.length())
  for i in 0..<fa.length() {
    out.push(fa[i])
  }
  Bytes::from_array(out)
}

///|
fn scram_hmac(sha512 : Bool, key : Bytes, message : Bytes) -> FixedArray[Byte] {
  if sha512 {
    @crypto.hmac(@crypto.SHA512::new(), key[:], message[:])
  } else {
    @crypto.hmac(@crypto.SHA256::new(), key[:], message[:])
  }
}

///|
fn scram_hash(sha512 : Bool, data : Bytes) -> FixedArray[Byte] {
  if sha512 {
    @crypto.sha512(data)
  } else {
    @crypto.sha256(data)
  }
}

///|
fn scram_xor(a : FixedArray[Byte], b : FixedArray[Byte]) -> FixedArray[Byte] {
  FixedArray::makei(a.length(), fn(i) { a[i] ^ b[i] })
}

///|
/// SaltedPassword = Hi(password, salt, iterations), i.e. PBKDF2-HMAC.
pub fn scram_salted_password(
  sha512 : Bool,
  password : Bytes,
  salt : Bytes,
  iterations : Int,
) -> Bytes {
  let hlen = if sha512 { 64 } else { 32 }
  if sha512 {
    scram_pbkdf2_hmac(@crypto.SHA512::new(), password, salt, iterations, hlen)
  } else {
    scram_pbkdf2_hmac(@crypto.SHA256::new(), password, salt, iterations, hlen)
  }
}

///|
/// AuthMessage = client-first-bare "," server-first "," client-final-without-proof.
pub fn scram_auth_message(
  client_first_bare : String,
  server_first : String,
  client_final_without_proof : String,
) -> String {
  "\{client_first_bare},\{server_first},\{client_final_without_proof}"
}

///|
/// ClientProof = ClientKey XOR ClientSignature, base64-encoded.
pub fn scram_client_proof_b64(
  sha512 : Bool,
  salted_password : Bytes,
  auth_message : String,
) -> String {
  let client_key = scram_hmac(
    sha512,
    salted_password,
    @utf8.encode("Client Key"),
  )
  let stored_key = scram_hash(sha512, scram_fixed_to_bytes(client_key))
  let client_signature = scram_hmac(
    sha512,
    scram_fixed_to_bytes(stored_key),
    @utf8.encode(auth_message),
  )
  @base64.encode(
    scram_fixed_to_bytes(scram_xor(client_key, client_signature))[:],
  )
}

///|
/// Server-side check of a client proof: recompute the stored key from the
/// known salted password and confirm proof XOR ClientSignature hashes to it.
pub fn scram_verify_client_proof(
  sha512 : Bool,
  salted_password : Bytes,
  auth_message : String,
  proof_b64 : String,
) -> Bool {
  let proof = @base64.decode(proof_b64[:]) catch { _ => return false }
  let stored_key = scram_hash(
    sha512,
    scram_fixed_to_bytes(
      scram_hmac(sha512, salted_password, @utf8.encode("Client Key")),
    ),
  )
  let client_signature = scram_hmac(
    sha512,
    scram_fixed_to_bytes(stored_key),
    @utf8.encode(auth_message),
  )
  if proof.length() != client_signature.length() {
    return false
  }
  let client_key = FixedArray::make(proof.length(), b'\x00')
  for i in 0..<proof.length() {
    client_key[i] = proof[i] ^ client_signature[i]
  }
  let recomputed = scram_hash(sha512, scram_fixed_to_bytes(client_key))
  let mut equal = true
  for i in 0..<recomputed.length() {
    if recomputed[i] != stored_key[i] {
      equal = false
    }
  }
  equal
}

///|
/// ServerSignature = HMAC(ServerKey, AuthMessage), base64-encoded; the
/// client verifies the broker's server-final "v=" against it.
pub fn scram_server_signature_b64(
  sha512 : Bool,
  salted_password : Bytes,
  auth_message : String,
) -> String {
  let server_key = scram_hmac(
    sha512,
    salted_password,
    @utf8.encode("Server Key"),
  )
  let server_signature = scram_hmac(
    sha512,
    scram_fixed_to_bytes(server_key),
    @utf8.encode(auth_message),
  )
  @base64.encode(scram_fixed_to_bytes(server_signature)[:])
}

///|
/// Parse a server-first-message into its nonce (which must extend the
/// client nonce), salt, and iteration count.
pub fn ScramServerFirst::parse(msg : String) -> ScramServerFirst raise {
  let mut nonce = ""
  let mut salt_b64 = ""
  let mut iterations = -1
  let mut seen_nonce = false
  let mut seen_salt = false
  let mut seen_iterations = false
  for part in scram_split(msg) {
    let (tag, value) = scram_attr(part)
    match tag {
      'r' => {
        nonce = value
        seen_nonce = true
      }
      's' => {
        salt_b64 = value
        seen_salt = true
      }
      'i' => {
        iterations = scram_parse_iterations(value)
        seen_iterations = true
      }
      _ => raise SaslError("unknown SCRAM attribute in server-first-message")
    }
  }
  if !(seen_nonce && seen_salt && seen_iterations) {
    raise SaslError("server-first-message is missing required attributes")
  }
  let salt = @base64.decode(salt_b64[:]) catch {
    _ => raise SaslError("server-first-message salt is not valid base64")
  }
  { nonce, salt, iterations, }
}

///|
pub(all) struct ScramServerFirst {
  nonce : String
  salt : Bytes
  iterations : Int
} derive(@debug.Debug)

///|
fn scram_split(msg : String) -> Array[String] {
  let parts : Array[String] = []
  let mut sb = StringBuilder()
  for c in msg {
    if c == ',' {
      parts.push(sb.to_string())
      sb = StringBuilder()
    } else {
      sb.write_char(c)
    }
  }
  parts.push(sb.to_string())
  parts
}

///|
fn scram_attr(part : String) -> (Char, String) raise {
  let chars : Array[Char] = []
  for c in part {
    chars.push(c)
  }
  if chars.length() < 2 || chars[1] != '=' {
    raise SaslError("malformed SCRAM attribute \"\{part}\"")
  }
  let rest = chars_to_string(chars, 2, chars.length())
  (chars[0], rest)
}

///|
fn scram_parse_iterations(s : String) -> Int raise {
  let mut v = 0
  for c in s {
    let d = c.to_int() - 48
    if d < 0 || d > 9 {
      raise SaslError("SCRAM iteration count is not a number")
    }
    v = v * 10 + d
    if v > 1_000_000_000 {
      raise SaslError("SCRAM iteration count is unreasonably large")
    }
  }
  if v <= 0 {
    raise SaslError("SCRAM iteration count must be positive")
  }
  v
}

///|
/// Escape the username per the SCRAM spec: '=' and ',' are sent as =3D
/// and =2C. (Full SASLprep normalization is not applied; usernames made
/// of printable ASCII are unaffected.)
pub fn scram_escape_username(username : String) -> String {
  let sb = StringBuilder()
  for c in username {
    if c == '=' {
      sb.write_string("=3D")
    } else if c == ',' {
      sb.write_string("=2C")
    } else {
      sb.write_char(c)
    }
  }
  sb.to_string()
}

///|
/// The client nonce must be unique per authentication exchange, so it is
/// derived from a hash of the wall clock and the client id. Hex output is
/// printable ASCII without commas, exactly what SCRAM requires.
fn scram_nonce(client_id : String) -> String {
  let material = "\{client_id}:\{@async.now()}"
  @crypto.bytes_to_hex_string(@crypto.sha256(@utf8.encode(material)))
}
