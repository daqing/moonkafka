// The classic consumer-group coordination APIs (Phase 4 compat path):
// JoinGroup v9 (key 11), SyncGroup v5 (key 14), Heartbeat v4 (key 12),
// LeaveGroup v5 (key 13). Field order pinned from the message/*.json
// schemas in the Kafka 4.3 tree; all four are flexible in range and
// carry the group-instance id for static membership.

///|
/// One protocol entry of a JoinGroup request: the assignor name and its
/// serialized ConsumerProtocolSubscription.
pub(all) struct JoinGroupProtocol {
  name : String
  metadata : Bytes
} derive(@debug.Debug)

///|
/// One member's entry in a JoinGroup response.
pub struct JoinGroupMember {
  member_id : String
  group_instance_id : String?
  metadata : Bytes
} derive(@debug.Debug)

///|
pub struct JoinGroupResult {
  error_code : Int
  generation_id : Int
  protocol_name : String
  leader_id : String
  member_id : String
  /// Only the leader receives the other members' subscriptions.
  members : Array[JoinGroupMember]
} derive(@debug.Debug)

///|
/// Encode a JoinGroup v9 request body. `member_id` empty joins fresh;
/// `group_instance_id` enables static membership. Protocols ride the
/// assignor names this member supports with subscription metadata.
pub fn encode_join_group_request(
  group_id : String,
  session_timeout_ms : Int,
  rebalance_timeout_ms : Int,
  member_id : String,
  group_instance_id? : String? = None,
  protocols~ : Array[JoinGroupProtocol],
) -> Bytes {
  let body = @buf.Encoder::new()
  body.write_compact_string(group_id)
  body.write_i32(session_timeout_ms)
  body.write_i32(rebalance_timeout_ms)
  body.write_compact_string(member_id)
  body.write_compact_nullable_string(group_instance_id)
  body.write_compact_string("consumer") // protocol type
  body.write_compact_len(protocols.length())
  for protocol in protocols {
    body.write_compact_string(protocol.name)
    body.write_compact_len(protocol.metadata.length())
    body.write_bytes(protocol.metadata)
    body.write_tag_buffer()
  }
  body.write_compact_nullable_string(None) // reason (v8+)
  body.write_tag_buffer()
  body.to_bytes()
}

///|
/// Decode a JoinGroup v9 response body.
pub fn decode_join_group_response(
  d : @buf.Decoder,
) -> (JoinGroupResult, Int) raise {
  let throttle = d.read_i32()
  let error_code = d.read_i16()
  let generation_id = d.read_i32()
  let _protocol_type = d.read_compact_nullable_string() // v7+
  let protocol_name = d.read_compact_nullable_string().unwrap_or("")
  let leader_id = d.read_compact_string()
  ignore(d.read_bool()) // skip_assignment (v9+)
  let member_id = d.read_compact_string()
  let n = d.read_compact_len()
  let members : Array[JoinGroupMember] = []
  for _ in 0..<n {
    let id = d.read_compact_string()
    let instance = d.read_compact_nullable_string()
    let len = d.read_compact_len()
    let metadata = d.read_bytes(len)
    d.skip_tag_buffer()
    members.push({ member_id: id, group_instance_id: instance, metadata, })
  }
  d.skip_tag_buffer()
  (
    { error_code, generation_id, protocol_name, leader_id, member_id, members, },
    throttle,
  )
}

///|
pub async fn BrokerConnection::join_group(
  self : BrokerConnection,
  group_id : String,
  session_timeout_ms : Int,
  rebalance_timeout_ms : Int,
  member_id : String,
  group_instance_id? : String? = None,
  protocols~ : Array[JoinGroupProtocol],
  timeout_ms? : Int = 30000,
) -> JoinGroupResult {
  let version = self.api_version(API_JOIN_GROUP)
  let d = self.request(
    API_JOIN_GROUP,
    version,
    encode_join_group_request(
      group_id,
      session_timeout_ms,
      rebalance_timeout_ms,
      member_id,
      group_instance_id~,
      protocols~,
    ),
    timeout_ms~,
  )
  let (result, throttle) = decode_join_group_response(d)
  self.note_throttle(throttle)
  result
}

///|
/// One member's assignment in a SyncGroup request (the leader sends all).
pub(all) struct SyncGroupAssignment {
  member_id : String
  assignment : Bytes
} derive(@debug.Debug)

///|
pub struct SyncGroupResult {
  error_code : Int
  protocol_name : String
  assignment : Bytes
} derive(@debug.Debug)

///|
/// Encode a SyncGroup v5 request body: the leader carries every member's
/// assignment; followers send an empty list and receive their own.
pub fn encode_sync_group_request(
  group_id : String,
  generation_id : Int,
  member_id : String,
  group_instance_id? : String? = None,
  protocol_name? : String = "",
  assignments~ : Array[SyncGroupAssignment],
) -> Bytes {
  let body = @buf.Encoder::new()
  body.write_compact_string(group_id)
  body.write_i32(generation_id)
  body.write_compact_string(member_id)
  body.write_compact_nullable_string(group_instance_id)
  body.write_compact_nullable_string(Some("consumer")) // protocol type (v5+)
  body.write_compact_nullable_string(
    if protocol_name.is_empty() {
      None
    } else {
      Some(protocol_name)
    },
  )
  body.write_compact_len(assignments.length())
  for entry in assignments {
    body.write_compact_string(entry.member_id)
    body.write_compact_len(entry.assignment.length())
    body.write_bytes(entry.assignment)
    body.write_tag_buffer()
  }
  body.write_tag_buffer()
  body.to_bytes()
}

///|
/// Decode a SyncGroup v5 response body.
pub fn decode_sync_group_response(
  d : @buf.Decoder,
) -> (SyncGroupResult, Int) raise {
  let throttle = d.read_i32()
  let error_code = d.read_i16()
  let _protocol_type = d.read_compact_nullable_string()
  let protocol_name = d.read_compact_nullable_string().unwrap_or("")
  let len = d.read_compact_len()
  let assignment = d.read_bytes(len)
  d.skip_tag_buffer()
  ({ error_code, protocol_name, assignment, }, throttle)
}

///|
pub async fn BrokerConnection::sync_group(
  self : BrokerConnection,
  group_id : String,
  generation_id : Int,
  member_id : String,
  group_instance_id? : String? = None,
  protocol_name? : String = "",
  assignments~ : Array[SyncGroupAssignment],
  timeout_ms? : Int = 30000,
) -> SyncGroupResult {
  let version = self.api_version(API_SYNC_GROUP)
  let d = self.request(
    API_SYNC_GROUP,
    version,
    encode_sync_group_request(
      group_id,
      generation_id,
      member_id,
      group_instance_id~,
      protocol_name~,
      assignments~,
    ),
    timeout_ms~,
  )
  let (result, throttle) = decode_sync_group_response(d)
  self.note_throttle(throttle)
  result
}

///|
/// Encode a Heartbeat v4 request body.
pub fn encode_heartbeat_request(
  group_id : String,
  generation_id : Int,
  member_id : String,
  group_instance_id? : String? = None,
) -> Bytes {
  let body = @buf.Encoder::new()
  body.write_compact_string(group_id)
  body.write_i32(generation_id)
  body.write_compact_string(member_id)
  body.write_compact_nullable_string(group_instance_id)
  body.write_tag_buffer()
  body.to_bytes()
}

///|
/// Decode a Heartbeat v4 response body: just the error code.
pub fn decode_heartbeat_response(d : @buf.Decoder) -> (Int, Int) raise {
  let throttle = d.read_i32()
  let error_code = d.read_i16()
  d.skip_tag_buffer()
  (error_code, throttle)
}

///|
pub async fn BrokerConnection::group_heartbeat(
  self : BrokerConnection,
  group_id : String,
  generation_id : Int,
  member_id : String,
  group_instance_id? : String? = None,
  timeout_ms? : Int = 30000,
) -> Int {
  let version = self.api_version(API_HEARTBEAT)
  let d = self.request(
    API_HEARTBEAT,
    version,
    encode_heartbeat_request(
      group_id,
      generation_id,
      member_id,
      group_instance_id~,
    ),
    timeout_ms~,
  )
  let (error_code, throttle) = decode_heartbeat_response(d)
  self.note_throttle(throttle)
  error_code
}

///|
pub struct LeaveGroupMemberResult {
  member_id : String
  error_code : Int
} derive(@debug.Debug)

///|
/// Encode a LeaveGroup v5 request body: this member (and, for static
/// membership, the instance id) leaving the group.
pub fn encode_leave_group_request(
  group_id : String,
  member_id : String,
  group_instance_id? : String? = None,
) -> Bytes {
  let body = @buf.Encoder::new()
  body.write_compact_string(group_id)
  body.write_compact_len(1) // members: this one
  body.write_compact_string(member_id)
  body.write_compact_nullable_string(group_instance_id)
  body.write_compact_nullable_string(None) // reason
  body.write_tag_buffer()
  body.write_tag_buffer()
  body.to_bytes()
}

///|
/// Decode a LeaveGroup v5 response body: the group-level error plus this
/// member's per-member entry.
pub fn decode_leave_group_response(
  d : @buf.Decoder,
) -> (Int, Array[LeaveGroupMemberResult], Int) raise {
  let throttle = d.read_i32()
  let error_code = d.read_i16()
  let n = d.read_compact_len()
  let members : Array[LeaveGroupMemberResult] = []
  for _ in 0..<n {
    let member_id = d.read_compact_string()
    ignore(d.read_compact_nullable_string()) // group instance id
    let member_error = d.read_i16()
    d.skip_tag_buffer()
    members.push({ member_id, error_code: member_error, })
  }
  d.skip_tag_buffer()
  (error_code, members, throttle)
}

///|
pub async fn BrokerConnection::leave_group(
  self : BrokerConnection,
  group_id : String,
  member_id : String,
  group_instance_id? : String? = None,
  timeout_ms? : Int = 30000,
) -> Int {
  let version = self.api_version(API_LEAVE_GROUP)
  let d = self.request(
    API_LEAVE_GROUP,
    version,
    encode_leave_group_request(group_id, member_id, group_instance_id~),
    timeout_ms~,
  )
  let (error_code, _members, throttle) = decode_leave_group_response(d)
  self.note_throttle(throttle)
  error_code
}
