// ConsumerGroupHeartbeat v0/v1 (key 68) — the single KIP-848 group
// membership RPC: join, reconcile, and leave all ride one request the
// member re-issues every heartbeat interval. Pinned from
// ConsumerGroupHeartbeatRequest.json / ConsumerGroupHeartbeatResponse.json
// in the Kafka 4.3 tree: v1 adds SubscribedTopicRegex and the
// INVALID_REGULAR_EXPRESSION error; flexible since v0.
//
// Member semantics: epoch 0 joins the group (the coordinator answers with
// the member id and the next epoch), -1 leaves gracefully, -2 marks a
// static member that will rejoin. The client generates its member id and
// keeps it for the process lifetime.

///|
/// One topic's partitions in a heartbeat assignment (topic-id addressed).
pub(all) struct HeartbeatTopicPartitions {
  topic_id : Uuid
  partitions : Array[Int]
} derive(@debug.Debug)

///|
/// Encode a ConsumerGroupHeartbeat request body. Optional fields the
/// member leaves unchanged send None (the server keeps its state); the
/// owned-partition list is sent as null (unchanged), so the server
/// drives assignment.
pub fn encode_consumer_group_heartbeat_request(
  version : Int,
  group_id : String,
  member_id : String,
  member_epoch : Int,
  instance_id? : String? = None,
  rack_id? : String? = None,
  rebalance_timeout_ms? : Int = -1,
  subscribed_topic_names? : Array[String]? = None,
  subscribed_topic_regex? : String? = None,
  server_assignor? : String? = None,
) -> Bytes raise {
  if version < 0 || version > 1 {
    raise ProtocolError::ProtocolError(
      "moonkafka implements ConsumerGroupHeartbeat v0-v1, got v\{version}",
    )
  }
  let body = @buf.Encoder::new()
  body.write_compact_string(group_id)
  body.write_compact_string(member_id)
  body.write_i32(member_epoch)
  body.write_compact_nullable_string(instance_id)
  body.write_compact_nullable_string(rack_id)
  body.write_i32(rebalance_timeout_ms)
  match subscribed_topic_names {
    Some(names) => {
      body.write_compact_len(names.length())
      for name in names {
        body.write_compact_string(name)
      }
    }
    None => body.write_compact_len(-1)
  }
  if version >= 1 {
    body.write_compact_nullable_string(subscribed_topic_regex)
  }
  body.write_compact_nullable_string(server_assignor)
  // Owned topic partitions: a nullable struct — null ("unchanged") is a
  // single -1 byte, not an array length.
  body.write_i8(-1)
  body.write_tag_buffer()
  body.to_bytes()
}

///|
/// One member heartbeat outcome: identity/epoch reconciliation, the
/// server's heartbeat interval, and the (possibly empty) assignment.
pub struct ConsumerGroupHeartbeatResult {
  error_code : Int
  error_message : String?
  member_id : String
  member_epoch : Int
  heartbeat_interval_ms : Int
  assignment : Array[HeartbeatTopicPartitions]
} derive(@debug.Debug)

///|
/// Decode a ConsumerGroupHeartbeat response body.
pub fn decode_consumer_group_heartbeat_response(
  d : @buf.Decoder,
) -> (ConsumerGroupHeartbeatResult, Int) raise {
  let throttle = d.read_i32()
  let error_code = d.read_i16()
  let error_message = d.read_compact_nullable_string()
  let member_id = d.read_compact_nullable_string().unwrap_or("")
  let member_epoch = d.read_i32()
  let heartbeat_interval_ms = d.read_i32()
  let assignment : Array[HeartbeatTopicPartitions] = []
  let has_assignment = d.read_i8()
  if has_assignment >= 0 {
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
pub async fn BrokerConnection::consumer_group_heartbeat(
  self : BrokerConnection,
  group_id : String,
  member_id : String,
  member_epoch : Int,
  instance_id? : String? = None,
  rebalance_timeout_ms? : Int = -1,
  subscribed_topic_names? : Array[String]? = None,
  subscribed_topic_regex? : String? = None,
  server_assignor? : String? = None,
  timeout_ms? : Int = 30000,
) -> ConsumerGroupHeartbeatResult {
  let version = self.api_version(API_CONSUMER_GROUP_HEARTBEAT)
  let d = self.request(
    API_CONSUMER_GROUP_HEARTBEAT,
    version,
    encode_consumer_group_heartbeat_request(
      version,
      group_id,
      member_id,
      member_epoch,
      instance_id~,
      rebalance_timeout_ms~,
      subscribed_topic_names~,
      subscribed_topic_regex~,
      server_assignor~,
    ),
    timeout_ms~,
  )
  let (result, throttle) = decode_consumer_group_heartbeat_response(d)
  self.note_throttle(throttle)
  result
}
