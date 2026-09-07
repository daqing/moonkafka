// ShareGroupHeartbeat v1 (key 76) — the KIP-932 share group membership
// RPC. Mirror of the consumer-group heartbeat: one request drives join
// (epoch 0), reconcile (next epochs), and leave (epoch -1); the share
// coordinator owns the assignment and pushes it in the response. v1 is
// the initial GA shape (the only supported version). Pinned from
// ShareGroupHeartbeatRequest.json / ShareGroupHeartbeatResponse.json in
// the Kafka 4.3 tree; flexible since v0.

///|
/// The share coordinator's pushed assignment: topic-id addressed
/// partitions per topic.
pub(all) struct ShareHeartbeatTopicPartitions {
  topic_id : Uuid
  partitions : Array[Int]
} derive(@debug.Debug)

///|
/// Encode a ShareGroupHeartbeat v1 request body. `member_epoch` 0 joins,
/// -1 leaves; a positive value reconciles. Rack and subscription ride
/// along only when they change (None keeps the broker's state).
pub fn encode_share_group_heartbeat_request(
  version : Int,
  group_id : String,
  member_id : String,
  member_epoch : Int,
  rack_id? : String? = None,
  subscribed_topic_names? : Array[String]? = None,
) -> Bytes raise {
  if version != 1 {
    raise ProtocolError::ProtocolError(
      "moonkafka implements ShareGroupHeartbeat v1, got v\{version}",
    )
  }
  let body = @buf.Encoder::new()
  body.write_compact_string(group_id)
  body.write_compact_string(member_id)
  body.write_i32(member_epoch)
  body.write_compact_nullable_string(rack_id)
  match subscribed_topic_names {
    Some(names) => {
      body.write_compact_len(names.length())
      for name in names {
        body.write_compact_string(name)
      }
    }
    None => body.write_compact_len(-1)
  }
  body.write_tag_buffer()
  body.to_bytes()
}

///|
/// One share-group heartbeat outcome: identity/epoch reconciliation,
/// the server's heartbeat interval, and the (possibly empty) topic
/// assignment.
pub struct ShareGroupHeartbeatResult {
  error_code : Int
  error_message : String?
  member_id : String
  member_epoch : Int
  heartbeat_interval_ms : Int
  assignment : Array[ShareHeartbeatTopicPartitions]
} derive(@debug.Debug)

///|
/// Decode a ShareGroupHeartbeat response body.
pub fn decode_share_group_heartbeat_response(
  d : @buf.Decoder,
) -> (ShareGroupHeartbeatResult, Int) raise {
  let throttle = d.read_i32()
  let error_code = d.read_i16()
  let error_message = d.read_compact_nullable_string()
  let member_id = d.read_compact_nullable_string().unwrap_or("")
  let member_epoch = d.read_i32()
  let heartbeat_interval_ms = d.read_i32()
  let assignment : Array[ShareHeartbeatTopicPartitions] = []
  if d.read_i8() >= 0 {
    let topics = d.read_compact_len()
    for _ in 0..<topics {
      let topic_id = Uuid::from_bytes(d.read_bytes(16))
      let n = d.read_compact_len()
      let partitions : Array[Int] = []
      for _ in 0..<n {
        partitions.push(d.read_i32())
      }
      d.skip_tag_buffer()
      assignment.push({ topic_id, partitions, })
    }
    d.skip_tag_buffer()
  }
  d.skip_tag_buffer()
  (
    {
      error_code,
      error_message,
      member_id,
      member_epoch,
      heartbeat_interval_ms,
      assignment,
    },
    throttle,
  )
}

///|
pub async fn BrokerConnection::share_group_heartbeat(
  self : BrokerConnection,
  group_id : String,
  member_id : String,
  member_epoch : Int,
  rack_id? : String? = None,
  subscribed_topic_names? : Array[String]? = None,
  timeout_ms? : Int = 30000,
) -> ShareGroupHeartbeatResult {
  let version = self.api_version(API_SHARE_GROUP_HEARTBEAT)
  let d = self.request(
    API_SHARE_GROUP_HEARTBEAT,
    version,
    encode_share_group_heartbeat_request(
      version,
      group_id,
      member_id,
      member_epoch,
      rack_id~,
      subscribed_topic_names~,
    ),
    timeout_ms~,
  )
  let (result, throttle) = decode_share_group_heartbeat_response(d)
  self.note_throttle(throttle)
  result
}
