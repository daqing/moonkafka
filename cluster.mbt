// The cluster layer (the last Phase 1 item): one client's view of the
// cluster — bootstrap dialing, a BrokerConnection pool keyed by node id,
// cached metadata with expiry and error-triggered refresh, the topic-id
// map, and batched coordinator lookups. Producer and consumer delegate
// their cluster management here.
//
// Root scope for now; the plan's cluster/ package move takes this whole
// layer once the sender task (Phase 3) and fetcher (Phase 4) depend on it.

///|
pub struct ClusterClient {
  bootstrap : BootstrapServers
  client_id : String
  request_timeout_ms : Int
  metadata_max_age_ms : Int
  max_in_flight : Int
  sasl : SaslConfig?
  use_tls : Bool
  tls_options : TlsClientOptions?
  lock : @async.Mutex
  /// Any-broker connection carrying metadata and coordinator requests.
  priv mut control : BrokerConnection
  mut brokers : Map[Int, BrokerInfo]
  /// Latest metadata per topic name; TopicMetadata carries the topic id.
  mut topics : Map[String, TopicMetadata]
  /// topic id → name (KIP-516); the reverse view of topics.
  mut topic_ids : Map[Uuid, String]
  /// Wall-clock ms of the last successful refresh; -1 = never.
  mut refreshed_ms : Int64
  conns : Map[Int, BrokerConnection]
}

///|
/// Dial the bootstrap set (rotating, SASL, TLS, ApiVersions negotiation)
/// and return the client; metadata starts empty and stale, so the first
/// refresh_metadata is on the caller.
pub async fn ClusterClient::connect(
  bootstrap_addresses : Array[HostPort],
  client_id? : String = "moonkafka",
  request_timeout_ms? : Int = 30000,
  metadata_max_age_ms? : Int = 300000,
  max_in_flight? : Int = 5,
  sasl? : SaslConfig? = None,
  use_tls? : Bool = false,
  tls? : TlsClientOptions? = None,
) -> ClusterClient {
  let bootstrap = BootstrapServers::new(bootstrap_addresses)
  let control = connect_bootstrap(
    bootstrap,
    client_id,
    timeout_ms=request_timeout_ms,
    sasl~,
    use_tls~,
    tls~,
  )
  {
    bootstrap,
    client_id,
    request_timeout_ms,
    metadata_max_age_ms,
    max_in_flight,
    sasl,
    use_tls,
    tls_options: tls,
    lock: @async.Mutex(),
    control,
    brokers: Map([]),
    topics: Map([]),
    topic_ids: Map([]),
    refreshed_ms: -1L,
    conns: Map([]),
  }
}

///|
/// The any-broker control connection, for direct cluster-level requests.
pub fn ClusterClient::control_conn(self : ClusterClient) -> BrokerConnection {
  self.control
}

///|
/// Tear down the control connection and the whole pool.
pub fn ClusterClient::close(self : ClusterClient) -> Unit {
  self.control.close()
  for _, conn in self.conns {
    conn.close()
  }
  self.conns.clear()
}

///|
/// Re-dial the bootstrap set after a transport failure: the control
/// connection and every pooled connection are dropped; metadata stays
/// stale until the caller refreshes. Callers keep their per-client state
/// (positions, accumulator) across the swap.
pub async fn ClusterClient::reconnect(self : ClusterClient) -> Unit {
  self.lock.acquire()
  defer self.lock.release()
  self.control.close()
  for _, conn in self.conns {
    conn.close()
  }
  self.conns.clear()
  self.control = connect_bootstrap(
    self.bootstrap,
    self.client_id,
    timeout_ms=self.request_timeout_ms,
    sasl=self.sasl,
    use_tls=self.use_tls,
    tls=self.tls_options,
  )
}

///|
/// Fetch metadata (None asks for all topics) and refresh the broker map,
/// topic map, topic-id map, and the refresh clock. Errors leave the old
/// snapshot in place.
pub async fn ClusterClient::refresh_metadata(
  self : ClusterClient,
  topics : Array[String]?,
) -> Unit {
  let metadata = self.control.fetch_metadata(
    topics,
    timeout_ms=self.request_timeout_ms,
  )
  self.lock.acquire()
  defer self.lock.release()
  self.brokers = metadata.brokers
  let topic_map : Map[String, TopicMetadata] = Map([])
  let ids : Map[Uuid, String] = Map([])
  for t in metadata.topics {
    topic_map[t.name] = t
    // Error topics carry the zero id; only real ids enter the map.
    if t.topic_id != Uuid::zero() {
      ids[t.topic_id] = t.name
    }
  }
  self.topics = topic_map
  self.topic_ids = ids
  self.refreshed_ms = @async.now()
}

///|
/// Opportunistic refresh when the cached snapshot aged out. Refresh
/// failures are swallowed — staleness surfaces on use, where callers run
/// their own recovery; Java's background refresher lands with the sender
/// task (D6).
pub async fn ClusterClient::refresh_if_stale(self : ClusterClient) -> Unit {
  if self.refreshed_ms < 0L ||
    @async.now() - self.refreshed_ms >= self.metadata_max_age_ms.to_int64() {
    let _ = self.refresh_metadata(None) catch { _ => () }
  }
}

///|
/// Force the next refresh_if_stale to treat the cache as cold (the
/// error-triggered half of refresh scheduling; a retry path may simply
/// call refresh_metadata instead).
pub fn ClusterClient::invalidate_metadata(self : ClusterClient) -> Unit {
  self.refreshed_ms = -1L
}

///|
/// A pooled connection to the broker behind `node_id`, dialed (with SASL
/// and ApiVersions negotiation) on first use. Dial and authentication
/// errors propagate; the pool stays consistent.
pub async fn ClusterClient::connection(
  self : ClusterClient,
  node_id : Int,
) -> BrokerConnection {
  self.lock.acquire()
  defer self.lock.release()
  match self.conns.get(node_id) {
    Some(conn) => return conn
    None => ()
  }
  match self.brokers.get(node_id) {
    Some(broker) => {
      let tls_opts = match self.tls_options {
        Some(opts) => Some(opts)
        None =>
          if self.use_tls {
            Some(TlsClientOptions::new(broker.host))
          } else {
            None
          }
      }
      let conn = BrokerConnection::connect(
        broker.host,
        broker.port,
        client_id=self.client_id,
        max_in_flight=self.max_in_flight,
        timeout_ms=self.request_timeout_ms,
        tls=tls_opts,
      )
      match self.sasl {
        Some(sasl) =>
          conn.authenticate(sasl, timeout_ms=self.request_timeout_ms)
        None => ()
      }
      conn.negotiate_api_versions(timeout_ms=self.request_timeout_ms)
      self.conns[node_id] = conn
      conn
    }
    None =>
      raise ProtocolError::ProtocolError(
        "no broker info for node \{node_id}; refresh metadata first",
      )
  }
}

///|
fn ClusterClient::broker(self : ClusterClient, node_id : Int) -> BrokerInfo? {
  self.brokers.get(node_id)
}

///|
/// The cached metadata snapshot for one topic, if known.
pub fn ClusterClient::topic(
  self : ClusterClient,
  name : String,
) -> TopicMetadata? {
  self.topics.get(name)
}

///|
/// The topic name behind a topic id (KIP-516), when in the cache.
pub fn ClusterClient::topic_name(self : ClusterClient, id : Uuid) -> String? {
  self.topic_ids.get(id)
}

///|
/// Names of every topic in the cached metadata snapshot (subscription
/// expansion for regex consumers).
pub fn ClusterClient::topics_snapshot(self : ClusterClient) -> Array[String] {
  let out : Array[String] = []
  for name, _ in self.topics {
    out.push(name)
  }
  out
}

///|
/// Sum of the throttle hints received across the control connection and
/// every pooled connection.
pub fn ClusterClient::total_throttle_ms(self : ClusterClient) -> Int64 {
  // Unlocked reads: counters are advisory and the cooperative scheduler
  // makes a torn read impossible on native Int64 writes.
  let mut total = self.control.throttle_total_ms()
  for _, conn in self.conns {
    total += conn.throttle_total_ms()
  }
  total
}

///|
/// Batched coordinator lookup through the control connection, keyed by
/// the requested key. Per-key broker errors come back inside the entries
/// (error_code); transport failures propagate.
pub async fn ClusterClient::coordinator(
  self : ClusterClient,
  keys : Array[String],
  coordinator_type : CoordinatorType,
) -> Map[String, CoordinatorInfo] {
  let infos = self.control.find_coordinator(
    keys,
    coordinator_type,
    timeout_ms=self.request_timeout_ms,
  )
  let out : Map[String, CoordinatorInfo] = Map([])
  for info in infos {
    out[info.key] = info
  }
  out
}
