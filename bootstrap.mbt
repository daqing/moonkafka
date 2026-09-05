// Bootstrap-server dialing: rotate over the configured entries so repeated
// connects (and reconnects) spread across the listed brokers, and keep
// trying until one accepts.

///|
/// Rotating view over parsed bootstrap server addresses.
pub struct BootstrapServers {
  addrs : Array[HostPort]
  mut cursor : Int
} derive(@debug.Debug)

///|
pub fn BootstrapServers::new(addrs : Array[HostPort]) -> BootstrapServers raise {
  if addrs.is_empty() {
    raise ProtocolError::ProtocolError(
      "bootstrap server list must not be empty",
    )
  }
  { addrs, cursor: 0, }
}

///|
/// Next address in round-robin order.
pub fn BootstrapServers::next(self : BootstrapServers) -> HostPort {
  let addr = self.addrs[self.cursor % self.addrs.length()]
  self.cursor += 1
  addr
}

///|
/// Dial the bootstrap servers in rotating order; the first connection that
/// succeeds wins. Raises the last failure if every entry refuses.
pub async fn connect_bootstrap(
  servers : BootstrapServers,
  client_id : String,
  timeout_ms? : Int = 10000,
  sasl? : SaslConfig? = None,
  use_tls? : Bool = false,
  tls? : TlsClientOptions? = None,
) -> BrokerConnection {
  let n = servers.addrs.length()
  let mut last_error : Error = ProtocolError::ProtocolError(
    "no bootstrap servers",
  )
  for _ in 0..<n {
    let addr = servers.next()
    match dial(addr, client_id, timeout_ms, sasl, use_tls, tls) {
      Ok(conn) => return conn
      Err(e) => last_error = e
    }
  }
  raise TransportError::ConnectionClosed(
    "all \{n} bootstrap servers failed; last error: \{last_error}",
  )
}

///|
/// The Result escapes to the caller as data, so Ok/Err wrapping is the
/// point of this helper rather than a mechanical try? replacement.
async fn dial(
  addr : HostPort,
  client_id : String,
  timeout_ms : Int,
  sasl : SaslConfig?,
  use_tls : Bool,
  tls : TlsClientOptions?,
) -> Result[BrokerConnection, Error] {
  // Without explicit TLS options, a TLS protocol still gets defaults whose
  // server name is the dialed host.
  let tls_opts = match tls {
    Some(opts) => Some(opts)
    None => if use_tls { Some(TlsClientOptions::new(addr.host)) } else { None }
  }
  try
    @async.with_timeout(timeout_ms, () => {
      BrokerConnection::connect(
        addr.host,
        addr.port,
        client_id~,
        sasl~,
        timeout_ms~,
        tls=tls_opts,
      )
    })
    |> Ok
  catch {
    e => Err(e)
  }
}
