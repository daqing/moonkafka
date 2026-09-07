// Telemetry (KIP-714, Phase 6): GetTelemetrySubscriptions v0 (key 71)
// and PushTelemetry v0 (key 72). Pinned from the matching Request/
// Response.json files in the Kafka 4.3 tree; flexible since v0.
//
// The client first asks the broker for a telemetry subscription — its
// subscription id, the accepted compression types, the push cadence and
// byte budget, and the metric names the broker wants. It then pushes an
// opaque metrics payload (the driver renders its own counters) inside
// that budget until it terminates the subscription.
//
// The metrics payload is opaque to the broker (a byte array); a
// TelemetryClient drives it from a pluggable provider so callers can
// expose their own counters.

///|
pub struct TelemetrySubscription {
  error_code : Int
  client_instance_id : Uuid
  subscription_id : Int
  accepted_compression_types : Array[Int]
  push_interval_ms : Int
  telemetry_max_bytes : Int
  delta_temporality : Bool
  requested_metrics : Array[String]
} derive(@debug.Debug)

///|
/// Encode a GetTelemetrySubscriptions v0 request body: the client's
/// instance id (zero until the broker assigns one).
pub fn encode_get_telemetry_subscriptions_request(
  client_instance_id : Uuid,
) -> Bytes {
  let body = @buf.Encoder::new()
  body.write_bytes(client_instance_id.to_bytes())
  body.write_tag_buffer()
  body.to_bytes()
}

///|
/// Decode a GetTelemetrySubscriptions v0 response body.
pub fn decode_get_telemetry_subscriptions_response(
  d : @buf.Decoder,
) -> (TelemetrySubscription, Int) raise {
  let throttle = d.read_i32()
  let error_code = d.read_i16()
  let client_instance_id = Uuid::from_bytes(d.read_bytes(16))
  let subscription_id = d.read_i32()
  let accepted_compression_types : Array[Int] = []
  let n_compression = d.read_compact_len()
  for _ in 0..<n_compression {
    accepted_compression_types.push(d.read_i8())
  }
  let push_interval_ms = d.read_i32()
  let telemetry_max_bytes = d.read_i32()
  let delta_temporality = d.read_bool()
  let requested_metrics : Array[String] = []
  let n_metrics = d.read_compact_len()
  for _ in 0..<n_metrics {
    requested_metrics.push(d.read_compact_string())
  }
  d.skip_tag_buffer()
  (
    {
      error_code,
      client_instance_id,
      subscription_id,
      accepted_compression_types,
      push_interval_ms,
      telemetry_max_bytes,
      delta_temporality,
      requested_metrics,
    },
    throttle,
  )
}

///|
pub async fn BrokerConnection::get_telemetry_subscriptions(
  self : BrokerConnection,
  client_instance_id : Uuid,
  timeout_ms? : Int = 30000,
) -> (TelemetrySubscription, Int) {
  let version = self.api_version(API_GET_TELEMETRY_SUBSCRIPTIONS)
  let d = self.request(
    API_GET_TELEMETRY_SUBSCRIPTIONS,
    version,
    encode_get_telemetry_subscriptions_request(client_instance_id),
    timeout_ms~,
  )
  let (result, throttle) = decode_get_telemetry_subscriptions_response(d)
  self.note_throttle(throttle)
  (result, throttle)
}

///|
/// Encode a PushTelemetry v0 request body. `metrics` is the opaque
/// payload rendered by the client's metrics provider.
pub fn encode_push_telemetry_request(
  client_instance_id : Uuid,
  subscription_id : Int,
  terminating : Bool,
  compression_type : Int,
  metrics : Bytes,
) -> Bytes {
  let body = @buf.Encoder::new()
  body.write_bytes(client_instance_id.to_bytes())
  body.write_i32(subscription_id)
  body.write_bool(terminating)
  body.write_i8(compression_type)
  body.write_compact_len(metrics.length())
  body.write_bytes(metrics)
  body.write_tag_buffer()
  body.to_bytes()
}

///|
/// Decode a PushTelemetry v0 response body: the throttle hint plus the
/// top-level error code.
pub fn decode_push_telemetry_response(d : @buf.Decoder) -> (Int, Int) raise {
  let throttle = d.read_i32()
  let error_code = d.read_i16()
  d.skip_tag_buffer()
  (error_code, throttle)
}

///|
pub async fn BrokerConnection::push_telemetry(
  self : BrokerConnection,
  client_instance_id : Uuid,
  subscription_id : Int,
  metrics : Bytes,
  terminating? : Bool = false,
  compression_type? : Int = 0,
  timeout_ms? : Int = 30000,
) -> Int {
  let version = self.api_version(API_PUSH_TELEMETRY)
  let d = self.request(
    API_PUSH_TELEMETRY,
    version,
    encode_push_telemetry_request(
      client_instance_id, subscription_id, terminating, compression_type, metrics,
    ),
    timeout_ms~,
  )
  let (error_code, throttle) = decode_push_telemetry_response(d)
  self.note_throttle(throttle)
  error_code
}

///|
/// The pluggable metrics source: renders the driver's counters into the
/// opaque payload pushed to the broker (a Plain-Text/OTLP-style render
/// is up to the provider).
pub type MetricsProvider = () -> Bytes

///|
/// Render a simple "key=value\\n" text payload from a key/value snapshot
/// — a convenient default provider body for exposing driver counters.
pub fn render_metrics(snapshot : Array[(String, Int64)]) -> Bytes {
  let e = @buf.Encoder::new()
  for pair in snapshot {
    let (key, value) = pair
    let line = "\{key}=\{value}\n"
    let b = @utf8.encode(line)
    e.write_bytes(b)
  }
  e.to_bytes()
}
