// Client configuration: transport/security settings shared by every client
// surface plus per-surface (producer/consumer) shells. Validation happens at
// construction; Phase 1 wires the security fields into the transport.

///|
/// Transport security. SASL layers authenticate right after connecting;
/// only plaintext is wired up so far.
pub(all) enum SecurityProtocol {
  Plaintext
  Ssl
  SaslPlaintext
  SaslSsl
} derive(@debug.Debug)

///|
/// SASL mechanism. OAUTHBEARER token callbacks arrive with the Phase 1 SASL
/// work; PLAIN and SCRAM use username/password.
pub(all) enum SaslMechanism {
  Plain
  ScramSha256
  ScramSha512
  OAuthBearer
} derive(@debug.Debug)

///|
pub(all) struct SaslConfig {
  mechanism : SaslMechanism
  username : String
  password : String
  /// OAUTHBEARER only: returns the current bearer token (raw token, without
  /// the "Bearer " prefix); called at each authentication.
  oauth_token_provider : (() -> String raise)?
}

///|
/// Manual Debug: the token-provider function field cannot be derived, and
/// credentials are never rendered.
pub impl @debug.Debug for SaslConfig with fn to_repr(self : SaslConfig) -> Repr {
  Repr::literal(
    "SaslConfig(mechanism: \{sasl_mechanism_name(self.mechanism)}, username: \{self.username})",
  )
}

///|
/// One "host:port" (or "[ipv6]:port") entry of the bootstrap list.
pub(all) struct HostPort {
  host : String
  port : Int
} derive(Eq, Compare, Hash, @debug.Debug)

///|
const DEFAULT_KAFKA_PORT : Int = 9092

///|
/// Parse a single bootstrap entry. The port defaults to 9092 when omitted;
/// bare IPv6 addresses must be bracketed.
pub fn HostPort::parse(entry : String) -> HostPort raise ProtocolError {
  if entry.length() == 0 {
    raise ProtocolError::ProtocolError("empty bootstrap server entry")
  }
  let chars : Array[Char] = []
  for c in entry {
    chars.push(c)
  }
  let n = chars.length()
  if chars[0] == '[' {
    // [ipv6] or [ipv6]:port
    let mut close = -1
    for i in 1..<n {
      if chars[i] == ']' {
        close = i
        break
      }
    }
    guard close > 0 else {
      raise ProtocolError::ProtocolError(
        "bootstrap entry \{entry} has an unterminated [ipv6] bracket",
      )
    }
    let host = chars_to_string(chars, 1, close)
    if close + 1 == n {
      return { host, port: DEFAULT_KAFKA_PORT, }
    }
    if chars[close + 1] == ':' {
      let port = parse_port(chars, close + 2, entry)
      return { host, port, }
    }
    raise ProtocolError::ProtocolError(
      "bootstrap entry \{entry} has trailing garbage after [ipv6]",
    )
  }
  // host or host:port — split on the last colon
  let mut colon = -1
  for i in 0..<n {
    if chars[i] == ':' {
      colon = i
    }
  }
  if colon < 0 {
    let host = chars_to_string(chars, 0, n)
    if host.length() == 0 {
      raise ProtocolError::ProtocolError("bootstrap entry \{entry} has no host")
    }
    { host, port: DEFAULT_KAFKA_PORT, }
  } else {
    let host = chars_to_string(chars, 0, colon)
    if host.length() == 0 {
      raise ProtocolError::ProtocolError("bootstrap entry \{entry} has no host")
    }
    let port = parse_port(chars, colon + 1, entry)
    { host, port, }
  }
}

///|
fn chars_to_string(chars : Array[Char], start : Int, end : Int) -> String {
  let sb = StringBuilder()
  for i in start..<end {
    sb.write_char(chars[i])
  }
  sb.to_string()
}

///|
fn parse_port(
  chars : Array[Char],
  start : Int,
  entry : String,
) -> Int raise ProtocolError {
  if start >= chars.length() {
    raise ProtocolError::ProtocolError(
      "bootstrap entry \{entry} has an empty port",
    )
  }
  let mut port = 0
  for i in start..<chars.length() {
    let v = chars[i].to_int()
    if v < 48 || v > 57 {
      raise ProtocolError::ProtocolError(
        "bootstrap entry \{entry} has a non-numeric port",
      )
    }
    port = port * 10 + (v - 48)
    if port > 65535 {
      raise ProtocolError::ProtocolError(
        "bootstrap entry \{entry} has a port above 65535",
      )
    }
  }
  if port < 1 {
    raise ProtocolError::ProtocolError(
      "bootstrap entry \{entry} port must be >= 1",
    )
  }
  port
}

///|
/// Settings shared by every client surface.
pub struct CommonConfig {
  bootstrap_servers : Array[String]
  client_id : String
  request_timeout_ms : Int
  connection_max_idle_ms : Int
  retries : Int
  retry_backoff_ms : Int
  retry_backoff_max_ms : Int
  security_protocol : SecurityProtocol
  sasl : SaslConfig?
  /// TLS settings for Ssl / SaslSsl protocols; None synthesizes defaults
  /// whose server name is the dialed host.
  tls : TlsClientOptions?
} derive(@debug.Debug)

///|
/// Defaults mirror the Java client where sensible: 30s request timeout,
/// 9-minute idle connection limit. Retries are bounded at 10 for now —
/// the Java default is effectively unbounded but is gated by
/// delivery.timeout.ms, which the batching producer (Phase 3) introduces.
pub fn CommonConfig::new(
  bootstrap_servers : Array[String],
  request_timeout_ms? : Int = 30000,
  security_protocol? : SecurityProtocol = Plaintext,
  sasl? : SaslConfig? = None,
  tls? : TlsClientOptions? = None,
) -> CommonConfig raise {
  let config : CommonConfig = {
    bootstrap_servers,
    client_id: "moonkafka",
    request_timeout_ms,
    connection_max_idle_ms: 540000,
    retries: 10,
    retry_backoff_ms: 100,
    retry_backoff_max_ms: 1000,
    security_protocol,
    sasl,
    tls,
  }
  config.validate()
  config
}

///|
/// Reject values that would make a client misbehave; called by `new`.
pub fn CommonConfig::validate(self : CommonConfig) -> Unit raise ProtocolError {
  if self.bootstrap_servers.is_empty() {
    raise ProtocolError::ProtocolError("bootstrap_servers must not be empty")
  }
  ignore(self.bootstrap_addresses())
  if self.client_id.length() == 0 {
    raise ProtocolError::ProtocolError("client_id must not be empty")
  }
  if self.request_timeout_ms <= 0 {
    raise ProtocolError::ProtocolError(
      "request_timeout_ms must be positive, got \{self.request_timeout_ms}",
    )
  }
  if self.connection_max_idle_ms <= 0 {
    raise ProtocolError::ProtocolError(
      "connection_max_idle_ms must be positive, got \{self.connection_max_idle_ms}",
    )
  }
  if self.retries < 0 {
    raise ProtocolError::ProtocolError("retries must be >= 0")
  }
  if self.retry_backoff_ms <= 0 ||
    self.retry_backoff_max_ms < self.retry_backoff_ms {
    raise ProtocolError::ProtocolError(
      "retry backoff needs backoff_ms > 0 and max_ms >= backoff_ms, got \{self.retry_backoff_ms}/\{self.retry_backoff_max_ms}",
    )
  }
  match (self.security_protocol, self.sasl) {
    (SaslPlaintext, None) | (SaslSsl, None) =>
      raise ProtocolError::ProtocolError(
        "SASL security protocols require sasl credentials",
      )
    (_, Some(sasl)) =>
      if sasl.mechanism is OAuthBearer {
        // the token provider carries the credential
      } else if sasl.username.length() == 0 {
        raise ProtocolError::ProtocolError("sasl username must not be empty")
      }
    _ => ()
  }
}

///|
/// Parse every bootstrap entry; raises on the first malformed one.
pub fn CommonConfig::bootstrap_addresses(
  self : CommonConfig,
) -> Array[HostPort] raise ProtocolError {
  let out = []
  for entry in self.bootstrap_servers {
    out.push(HostPort::parse(entry))
  }
  out
}

///|
pub struct ProducerConfig {
  common : CommonConfig
  topic : String
  acks : Int
} derive(@debug.Debug)

///|
/// `acks` is 1 (leader acknowledgement) or -1 (all in-sync replicas);
/// 0 becomes valid with the Phase 3 sender that does not read responses.
pub fn ProducerConfig::new(
  bootstrap_servers : Array[String],
  topic : String,
  acks? : Int = 1,
  request_timeout_ms? : Int = 30000,
  security_protocol? : SecurityProtocol = Plaintext,
  sasl? : SaslConfig? = None,
  tls? : TlsClientOptions? = None,
) -> ProducerConfig raise {
  if acks != 1 && acks != -1 {
    raise ProtocolError::ProtocolError(
      "acks must be 1 (leader) or -1 (all), got \{acks}",
    )
  }
  if topic.length() == 0 {
    raise ProtocolError::ProtocolError("topic must not be empty")
  }
  {
    common: CommonConfig::new(
      bootstrap_servers,
      request_timeout_ms~,
      security_protocol~,
      sasl~,
      tls~,
    ),
    topic,
    acks,
  }
}

///|
pub struct ConsumerConfig {
  common : CommonConfig
  topic : String
  start_from : StartFrom
} derive(@debug.Debug)

///|
pub fn ConsumerConfig::new(
  bootstrap_servers : Array[String],
  topic : String,
  start_from? : StartFrom = Earliest,
  request_timeout_ms? : Int = 30000,
  security_protocol? : SecurityProtocol = Plaintext,
  sasl? : SaslConfig? = None,
  tls? : TlsClientOptions? = None,
) -> ConsumerConfig raise {
  if topic.length() == 0 {
    raise ProtocolError::ProtocolError("topic must not be empty")
  }
  {
    common: CommonConfig::new(
      bootstrap_servers,
      request_timeout_ms~,
      security_protocol~,
      sasl~,
      tls~,
    ),
    topic,
    start_from,
  }
}
