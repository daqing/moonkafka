// KIP-714 telemetry client (Phase 6): subscribes to broker telemetry and
// pushes the driver's own metrics on a cadence, then terminates. A slave
// of the pluggable MetricsProvider — the client never looks inside the
// payload, so callers render whatever counters they want.
//
// Low priority by design: telemetry only runs when something consumes
// it, which here is the driver exposing its own counters. The provider
// is a plain function, so a caller can capture a Producer/Consumer and
// render a fresh snapshot on every push.

///|
/// Telemetry client settings on top of the shared transport config.
pub struct TelemetryClientConfig {
  common : CommonConfig
  /// The client's instance id; zero asks the broker to assign one.
  client_instance_id : Uuid
  /// Cap the payload the client renders (the broker's budget wins if
  /// the subscription reports a smaller telemetry_max_bytes).
  max_push_bytes : Int
  /// Renders the metrics payload pushed on each interval.
  metrics_provider : MetricsProvider
}

///|
pub fn TelemetryClientConfig::new(
  bootstrap_servers : Array[String],
  client_instance_id? : Uuid = Uuid::zero(),
  max_push_bytes? : Int = 16384,
  metrics_provider~ : MetricsProvider,
  request_timeout_ms? : Int = 30000,
  security_protocol? : SecurityProtocol = Plaintext,
  sasl? : SaslConfig? = None,
  tls? : TlsClientOptions? = None,
  metadata_max_age_ms? : Int = 300000,
) -> TelemetryClientConfig raise {
  {
    common: CommonConfig::new(
      bootstrap_servers,
      request_timeout_ms~,
      security_protocol~,
      sasl~,
      tls~,
      metadata_max_age_ms~,
    ),
    client_instance_id,
    max_push_bytes,
    metrics_provider,
  }
}

///|
pub struct TelemetryClient {
  cluster : ClusterClient
  /// The user's task group; hosts the push loop.
  group : @async.TaskGroup[Unit]
  priv mut client_instance_id : Uuid
  max_push_bytes : Int
  metrics_provider : MetricsProvider
  priv mut subscription_id : Int
  priv mut active : Bool
  priv mut spawned : Bool
  priv mut closed : Bool
  priv mut push_interval_ms : Int
  priv mut telemetry_max_bytes : Int
}

///|
/// Connect the telemetry client and fetch its first subscription. The
/// push loop joins `group` once it runs.
pub async fn TelemetryClient::connect_with_config(
  group~ : @async.TaskGroup[Unit],
  config : TelemetryClientConfig,
) -> TelemetryClient {
  let sasl = match config.common.security_protocol {
    SaslPlaintext | SaslSsl => config.common.sasl
    _ => None
  }
  let use_tls = match config.common.security_protocol {
    Ssl | SaslSsl => true
    _ => false
  }
  let cluster = ClusterClient::connect(
    config.common.bootstrap_addresses(),
    client_id=config.common.client_id,
    request_timeout_ms=config.common.request_timeout_ms,
    metadata_max_age_ms=config.common.metadata_max_age_ms,
    sasl~,
    use_tls~,
    tls=config.common.tls,
  )
  let client = {
    cluster,
    group,
    client_instance_id: config.client_instance_id,
    max_push_bytes: config.max_push_bytes,
    metrics_provider: config.metrics_provider,
    subscription_id: 0,
    push_interval_ms: 0,
    telemetry_max_bytes: 0,
    active: false,
    spawned: false,
    closed: false,
  }
  client
}

///|
/// Start pushing metrics: fetch the subscription (best effort) and run
/// the push loop in `group`.
pub async fn TelemetryClient::run(self : TelemetryClient) -> Unit {
  self.spawned = true
  self.active = true
  let client = self
  self.group.spawn_bg(no_wait=false, allow_failure=true, () => {
    client.push_loop()
  })
}

///|
/// Push metrics until close(): refresh the subscription when it lapses,
/// render the provider's payload inside the byte budget, and push.
async fn TelemetryClient::push_loop(self : TelemetryClient) -> Unit {
  for ;; {
    if self.closed || !self.active {
      break
    }
    let terminated = self.closed
    // Re-subscribe on first iteration or when the broker lost us.
    if self.subscription_id == 0 || terminated {
      let subscribed = self.refresh_subscription() catch { _ => false }
      if !subscribed {
        @async.sleep(SENDER_TICK_MS * 20)
        continue
      }
    }
    let (metrics, truncated) = self.render_payload()
    if truncated {
      // A payload over the budget would be rejected; skip this round
      // and let the provider size down on the next one.
      @async.sleep(SENDER_TICK_MS * 4)
      continue
    }
    let error_code = self.cluster
      .control_conn()
      .push_telemetry(
        self.client_instance_id,
        self.subscription_id,
        metrics,
        terminating=terminated,
      ) catch {
        // Transport failure: drop the subscription and retry later.
        _ => {
          self.subscription_id = 0
          continue
        }
      }
    if error_code != 0 {
      self.subscription_id = 0 // re-fetch the subscription next round.
    }
    let wait = Int::max(1, self.push_interval_ms / SENDER_TICK_MS)
    for _ in 0..<wait {
      if self.closed || !self.active {
        break
      }
      @async.sleep(SENDER_TICK_MS)
    }
  }
}

///|
/// Fetch (or re-fetch) the telemetry subscription. False when the broker
/// refused it — the caller paces the retry.
async fn TelemetryClient::refresh_subscription(self : TelemetryClient) -> Bool {
  let (sub, _throttle) = self.cluster
    .control_conn()
    .get_telemetry_subscriptions(self.client_instance_id)
  if sub.error_code != 0 {
    return false
  }
  self.client_instance_id = sub.client_instance_id
  self.subscription_id = sub.subscription_id
  if sub.push_interval_ms > 0 {
    self.push_interval_ms = sub.push_interval_ms
  } else if self.push_interval_ms <= 0 {
    self.push_interval_ms = 60000 // Kafka's default telemetry interval.
  }
  self.telemetry_max_bytes = if sub.telemetry_max_bytes > 0 {
    sub.telemetry_max_bytes
  } else {
    self.max_push_bytes
  }
  true
}

///|
/// Render the provider's payload, capped at the byte budget. Returns
/// true when the render had to be truncated (too large to send).
fn TelemetryClient::render_payload(self : TelemetryClient) -> (Bytes, Bool) {
  let budget = Int::min(self.telemetry_max_bytes, self.max_push_bytes)
  let full = (self.metrics_provider)()
  if full.length() > budget {
    // Copy the first budget bytes into an owned payload.
    let e = @buf.Encoder::new()
    for i in 0..<budget {
      e.write_byte(full[i])
    }
    (e.to_bytes(), true)
  } else {
    (full, false)
  }
}

///|
/// Stop the telemetry client and send the terminating push.
pub fn TelemetryClient::close(self : TelemetryClient) -> Unit {
  if !self.closed {
    self.closed = true
    self.active = false
  }
}
