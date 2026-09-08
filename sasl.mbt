// SASL negotiation: SaslHandshake v1 (non-flexible framing!) and
// SaslAuthenticate v2 (flexible). PLAIN is implemented; SCRAM and
// OAUTHBEARER arrive with their crypto/callback spikes.
// API keys live in versions.mbt. SASL versions are fixed, not negotiated:
// authentication runs before the ApiVersions handshake could inform a
// choice, and v1/v2 are the only versions 4.x brokers serve.

///|
const SASL_HANDSHAKE_VERSION : Int = 1

///|
const SASL_AUTHENTICATE_VERSION : Int = 2

///|
/// SaslHandshake v1 body: the mechanism name as a NON-compact string.
pub fn encode_sasl_handshake_request(mechanism : String) -> Bytes {
  let body = @buf.Encoder::new()
  let bytes = @utf8.encode(mechanism)
  body.write_i16(bytes.length())
  body.write_bytes(bytes)
  body.to_bytes()
}

///|
pub(all) struct SaslHandshakeResult {
  error_code : Int
  mechanisms : Array[String]
}

///|
/// SaslHandshake v1 response: non-flexible, so the mechanisms array uses an
/// int32 count and int16-length strings.
pub fn decode_sasl_handshake_response(
  d : @buf.Decoder,
) -> SaslHandshakeResult raise {
  let error_code = d.read_i16()
  let n = d.read_i32()
  let mechanisms : Array[String] = []
  for _ in 0..<n {
    let len = d.read_i16()
    mechanisms.push(@utf8.decode_lossy(d.read_bytes(len)[:]))
  }
  { error_code, mechanisms, }
}

///|
/// SaslAuthenticate v2 body: auth bytes as a compact byte array.
pub fn encode_sasl_authenticate_request(auth_bytes : Bytes) -> Bytes {
  let body = @buf.Encoder::new()
  body.write_compact_len(auth_bytes.length())
  body.write_bytes(auth_bytes)
  body.to_bytes()
}

///|
pub(all) struct SaslAuthenticateResult {
  error_code : Int
  error_message : String?
  auth_bytes : Bytes?
  session_lifetime_ms : Int64
}

///|
pub fn decode_sasl_authenticate_response(
  d : @buf.Decoder,
) -> SaslAuthenticateResult raise {
  let error_code = d.read_i16()
  let error_message = d.read_compact_nullable_string()
  let auth_len = d.read_compact_len()
  let auth_bytes = if auth_len < 0 {
    None
  } else {
    Some(d.read_bytes(auth_len))
  }
  let session_lifetime_ms = d.read_i64()
  d.skip_tag_buffer()
  { error_code, error_message, auth_bytes, session_lifetime_ms, }
}

///|
/// PLAIN: authzid NUL username NUL password, all UTF-8.
fn plain_auth_bytes(sasl : SaslConfig) -> Bytes {
  let username = @utf8.encode(sasl.username)
  let password = @utf8.encode(sasl.password)
  let buf : Array[Byte] = [b'\x00']
  for b in username {
    buf.push(b)
  }
  buf.push(b'\x00')
  for b in password {
    buf.push(b)
  }
  Bytes::from_array(buf)
}

///|
fn sasl_mechanism_name(mechanism : SaslMechanism) -> String {
  match mechanism {
    Plain => "PLAIN"
    ScramSha256 => "SCRAM-SHA-256"
    ScramSha512 => "SCRAM-SHA-512"
    OAuthBearer => "OAUTHBEARER"
  }
}

///|
/// Run the SASL handshake and authentication exchange on an established
/// connection. OAUTHBEARER and the SCRAM family raise until their
/// implementations land.
pub async fn BrokerConnection::authenticate(
  self : BrokerConnection,
  sasl : SaslConfig,
  timeout_ms? : Int = 30000,
) -> Unit {
  let mechanism = sasl_mechanism_name(sasl.mechanism)
  let d = self.request_raw(
    API_SASL_HANDSHAKE,
    SASL_HANDSHAKE_VERSION,
    encode_sasl_handshake_request(mechanism),
    timeout_ms~,
    flexible=false,
  )
  let handshake = decode_sasl_handshake_response(d)
  if handshake.error_code != 0 {
    raise SaslError(
      "handshake failed for \{mechanism}: \{error_name(handshake.error_code)}",
    )
  }
  if !handshake.mechanisms.contains(mechanism) {
    raise SaslError("broker does not enable \{mechanism}")
  }
  match sasl.mechanism {
    Plain => self.authenticate_plain(sasl, timeout_ms)
    ScramSha256 => self.authenticate_scram(sasl, false, timeout_ms)
    ScramSha512 => self.authenticate_scram(sasl, true, timeout_ms)
    OAuthBearer => self.authenticate_oauth(sasl, timeout_ms)
  }
}

///|
async fn BrokerConnection::authenticate_plain(
  self : BrokerConnection,
  sasl : SaslConfig,
  timeout_ms : Int,
) -> Unit {
  let result = self.sasl_authenticate(plain_auth_bytes(sasl), timeout_ms)
  if result.error_code != 0 {
    raise sasl_auth_error("PLAIN", result)
  }
}

///|
/// SCRAM-SHA-256 / SCRAM-SHA-512 (RFC 5802 / 7677): two SaslAuthenticate
/// round trips carrying the client-first, server-first, client-final, and
/// server-final messages. The broker's server signature is verified before
/// the exchange is considered successful.
async fn BrokerConnection::authenticate_scram(
  self : BrokerConnection,
  sasl : SaslConfig,
  sha512 : Bool,
  timeout_ms : Int,
) -> Unit {
  let client_nonce = scram_nonce(self.client_id)
  let client_first_bare = "n=\{scram_escape_username(sasl.username)},r=\{client_nonce}"
  let client_first = "n,,\{client_first_bare}"
  let first = self.sasl_authenticate(@utf8.encode(client_first), timeout_ms)
  if first.error_code != 0 {
    raise sasl_auth_error("SCRAM", first)
  }
  let server_first = match first.auth_bytes {
    Some(bytes) => @utf8.decode_lossy(bytes[:])
    None => raise SaslError("broker sent no server-first-message")
  }
  let parsed = ScramServerFirst::parse(server_first)
  if !scram_starts_with(parsed.nonce, client_nonce) {
    raise SaslError("server nonce does not extend the client nonce")
  }
  let salted = scram_salted_password(
    sha512,
    @utf8.encode(sasl.password),
    parsed.salt,
    parsed.iterations,
  )
  let client_final_without_proof = "c=biws,r=\{parsed.nonce}"
  let auth_message = scram_auth_message(
    client_first_bare, server_first, client_final_without_proof,
  )
  let proof_b64 = scram_client_proof_b64(sha512, salted, auth_message)
  let client_final = "\{client_final_without_proof},p=\{proof_b64}"
  let second = self.sasl_authenticate(@utf8.encode(client_final), timeout_ms)
  if second.error_code != 0 {
    raise sasl_auth_error("SCRAM", second)
  }
  let server_final = match second.auth_bytes {
    Some(bytes) => @utf8.decode_lossy(bytes[:])
    None => raise SaslError("broker sent no server-final-message")
  }
  let expected = scram_server_signature_b64(sha512, salted, auth_message)
  if server_final != "v=\{expected}" {
    raise SaslError("broker server signature mismatch")
  }
}

///|
/// OAUTHBEARER (RFC 7628 voucher over the Kafka SASL exchange): the token
/// provider supplies the current bearer token, sent as a single voucher;
/// the broker's success/error response decides the outcome.
async fn BrokerConnection::authenticate_oauth(
  self : BrokerConnection,
  sasl : SaslConfig,
  timeout_ms : Int,
) -> Unit {
  let provider = match sasl.oauth_token_provider {
    Some(provider) => provider
    None => raise SaslError("OAUTHBEARER requires a token provider")
  }
  let token = provider() catch {
    e => raise SaslError("token provider failed: \{e}")
  }
  if token.length() == 0 {
    raise SaslError("OAUTHBEARER token is empty")
  }
  let voucher = "n,\u{1}auth=Bearer \{token}\u{1}\u{1}"
  let result = self.sasl_authenticate(@utf8.encode(voucher), timeout_ms)
  if result.error_code != 0 {
    raise sasl_auth_error("OAUTHBEARER", result)
  }
}

///|
async fn BrokerConnection::sasl_authenticate(
  self : BrokerConnection,
  auth_bytes : Bytes,
  timeout_ms : Int,
) -> SaslAuthenticateResult {
  let d = self.request_raw(
    API_SASL_AUTHENTICATE,
    SASL_AUTHENTICATE_VERSION,
    encode_sasl_authenticate_request(auth_bytes),
    timeout_ms~,
  )
  decode_sasl_authenticate_response(d)
}

///|
fn sasl_auth_error(
  mechanism : String,
  result : SaslAuthenticateResult,
) -> SaslError {
  let detail = match result.error_message {
    Some(msg) => ": \{msg}"
    None => ""
  }
  SaslError(
    "authentication failed for \{mechanism}: \{error_name(result.error_code)}\{detail}",
  )
}

///|
fn scram_starts_with(s : String, prefix : String) -> Bool {
  if s.length() < prefix.length() {
    return false
  }
  let s_chars : Array[Char] = []
  let prefix_chars : Array[Char] = []
  for c in s {
    s_chars.push(c)
  }
  for c in prefix {
    prefix_chars.push(c)
  }
  for i in 0..<prefix_chars.length() {
    if s_chars[i] != prefix_chars[i] {
      return false
    }
  }
  true
}

///|
/// The broker's server signature, verified against the server-final
/// message; also used by the fake-broker tests from the server side.
pub fn scram_server_final(
  sha512 : Bool,
  salted_password : Bytes,
  auth_message : String,
) -> String {
  "v=\{scram_server_signature_b64(sha512, salted_password, auth_message)}"
}
