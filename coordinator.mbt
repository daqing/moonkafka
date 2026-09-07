// FindCoordinator v4 — the coordinator lookup the group/transaction
// clients (P3/P4) build on. Pinned from FindCoordinatorRequest.json /
// FindCoordinatorResponse.json in the Kafka 4.3 tree: v4 (KIP-699)
// carries the batched coordinator_keys request and returns the batched
// coordinators array; flexible since v3.

///|
/// What kind of coordinator to look up. Group and Transaction cover the
/// consumer-group and transaction flows; Share arrives with KIP-932.
pub(all) enum CoordinatorType {
  Group
  Transaction
  Share
} derive(@debug.Debug, Eq)

///|
fn CoordinatorType::code(self : CoordinatorType) -> Int {
  match self {
    Group => 0
    Transaction => 1
    Share => 2
  }
}

///|
/// One coordinator lookup result; error_code names per-key failures
/// (COORDINATOR_NOT_AVAILABLE, GROUP_AUTHORIZATION_FAILED, ...).
pub struct CoordinatorInfo {
  key : String
  node_id : Int
  host : String
  port : Int
  error_code : Int
  error_message : String?
} derive(@debug.Debug)

///|
/// Encode a FindCoordinator v4 request body: the batched keys plus the
/// coordinator type (field order per the v4 schema).
pub fn encode_find_coordinator_request(
  keys : Array[String],
  coordinator_type : CoordinatorType,
) -> Bytes {
  let body = @buf.Encoder::new()
  body.write_i8(coordinator_type.code())
  body.write_compact_len(keys.length())
  for key in keys {
    body.write_compact_string(key)
  }
  body.write_tag_buffer()
  body.to_bytes()
}

///|
/// Decode a FindCoordinator v4 response body: one entry per requested
/// key, plus the throttle hint.
pub fn decode_find_coordinator_response(
  d : @buf.Decoder,
) -> (Array[CoordinatorInfo], Int) raise {
  let throttle = d.read_i32()
  let n = d.read_compact_len()
  let out : Array[CoordinatorInfo] = []
  for _ in 0..<n {
    let key = d.read_compact_string()
    let node_id = d.read_i32()
    let host = d.read_compact_string()
    let port = d.read_i32()
    let error_code = d.read_i16()
    let error_message = d.read_compact_nullable_string()
    d.skip_tag_buffer()
    out.push({ key, node_id, host, port, error_code, error_message, })
  }
  d.skip_tag_buffer()
  (out, throttle)
}

///|
pub async fn BrokerConnection::find_coordinator(
  self : BrokerConnection,
  keys : Array[String],
  coordinator_type : CoordinatorType,
  timeout_ms? : Int = 30000,
) -> Array[CoordinatorInfo] {
  let version = self.api_version(API_FIND_COORDINATOR)
  let d = self.request(
    API_FIND_COORDINATOR,
    version,
    encode_find_coordinator_request(keys, coordinator_type),
    timeout_ms~,
  )
  let (coordinators, throttle) = decode_find_coordinator_response(d)
  self.note_throttle(throttle)
  coordinators
}
