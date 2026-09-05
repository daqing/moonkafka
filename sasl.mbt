// SASL negotiation: SaslHandshake v1 (non-flexible framing!) and
// SaslAuthenticate v2 (flexible). PLAIN is implemented; SCRAM and
// OAUTHBEARER arrive with their crypto/callback spikes.

///|
const API_SASL_HANDSHAKE : Int = 17

///|
const API_SASL_AUTHENTICATE : Int = 36

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
  let auth_bytes = match sasl.mechanism {
    Plain => plain_auth_bytes(sasl)
    mechanism =>
      raise SaslError(
        "mechanism \{sasl_mechanism_name(mechanism)} not implemented yet",
      )
  }
  let d2 = self.request_raw(
    API_SASL_AUTHENTICATE,
    SASL_AUTHENTICATE_VERSION,
    encode_sasl_authenticate_request(auth_bytes),
    timeout_ms~,
  )
  let result = decode_sasl_authenticate_response(d2)
  if result.error_code != 0 {
    let detail = match result.error_message {
      Some(msg) => ": \{msg}"
      None => ""
    }
    raise SaslError(
      "authentication failed for \{mechanism}: \{error_name(result.error_code)}\{detail}",
    )
  }
  // session_lifetime_ms drives re-authentication scheduling (tracked in
  // Phase 2); zero means the broker never expires the session.
}
