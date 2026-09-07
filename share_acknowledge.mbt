// ShareAcknowledge v1/v2 (key 79) — the KIP-932 acknowledgement RPC.
// Several share consumers may hold a copy of the same acquired record;
// acknowledgement is the member's binding choice — Accept (consume for
// good), Release (return for redelivery), Reject (poison), Renew
// (keep the delivery window). Pinned from ShareAcknowledgeRequest.json /
// ShareAcknowledgeResponse.json in the Kafka 4.3 tree; flexible since v0.
// v1 is the initial shape; v2 adds the tagged IsRenewAck flag (defaulted).

///|
/// Encode a ShareAcknowledge v1/v2 request body. `member_id` may be
/// empty (None on the wire) for admin-only acknowledgements on older
/// flows; session_epoch 0 opens, -1 closes a share session.
pub fn encode_share_acknowledge_request(
  version : Int,
  group_id : String,
  member_id : String,
  session_epoch : Int,
  topics : Array[ShareFetchTopic],
) -> Bytes raise {
  if version < 1 || version > 2 {
    raise ProtocolError::ProtocolError(
      "moonkafka implements ShareAcknowledge v1-v2, got v\{version}",
    )
  }
  let body = @buf.Encoder::new()
  body.write_compact_nullable_string(Some(group_id))
  body.write_compact_nullable_string(
    if member_id.is_empty() {
      None
    } else {
      Some(member_id)
    },
  )
  body.write_i32(session_epoch)
  body.write_compact_len(topics.length())
  for topic in topics {
    body.write_bytes(topic.topic_id.to_bytes())
    body.write_compact_len(topic.partitions.length())
    for p in topic.partitions {
      body.write_i32(p.partition)
      write_ack_batches(body, p.acknowledgment_batches)
      body.write_tag_buffer()
    }
    body.write_tag_buffer()
  }
  // IsRenewAck (v2, tagged): false by default.
  body.write_tag_buffer()
  body.to_bytes()
}

///|
/// One topic's acknowledgement outcome.
pub struct ShareAcknowledgeTopicResult {
  topic_id : Uuid
  partitions : Array[ShareAcknowledgePartitionResult]
} derive(@debug.Debug)

///|
pub struct ShareAcknowledgePartitionResult {
  partition : Int
  error_code : Int
  error_message : String?
  leader_id : Int
  leader_epoch : Int
} derive(@debug.Debug)

///|
pub struct ShareAcknowledgeResult {
  error_code : Int
  error_message : String?
  acquisition_lock_timeout_ms : Int
  topics : Array[ShareAcknowledgeTopicResult]
} derive(@debug.Debug)

///|
/// Decode a ShareAcknowledge v1/v2 response body.
pub fn decode_share_acknowledge_response(
  version : Int,
  d : @buf.Decoder,
) -> (ShareAcknowledgeResult, Int) raise {
  if version < 1 || version > 2 {
    raise ProtocolError::ProtocolError(
      "moonkafka implements ShareAcknowledge v1-v2, got v\{version}",
    )
  }
  let throttle = d.read_i32()
  let error_code = d.read_i16()
  let error_message = d.read_compact_nullable_string()
  // AcquisitionLockTimeoutMs arrives in v2 only.
  let acquisition_lock_timeout_ms = if version >= 2 { d.read_i32() } else { 0 }
  let topics : Array[ShareAcknowledgeTopicResult] = []
  let topic_count = d.read_compact_len()
  for _ in 0..<topic_count {
    let topic_id = Uuid::from_bytes(d.read_bytes(16))
    let partitions : Array[ShareAcknowledgePartitionResult] = []
    let part_count = d.read_compact_len()
    for _ in 0..<part_count {
      let partition = d.read_i32()
      let part_error = d.read_i16()
      let part_error_message = d.read_compact_nullable_string()
      let leader_id = d.read_i32()
      let leader_epoch = d.read_i32()
      d.skip_tag_buffer()
      partitions.push({
        partition,
        error_code: part_error,
        error_message: part_error_message,
        leader_id,
        leader_epoch,
      })
    }
    d.skip_tag_buffer()
    topics.push({ topic_id, partitions, })
  }
  d.skip_tag_buffer()
  (
    { error_code, error_message, acquisition_lock_timeout_ms, topics, },
    throttle,
  )
}

///|
pub async fn BrokerConnection::share_acknowledge(
  self : BrokerConnection,
  group_id : String,
  member_id : String,
  session_epoch : Int,
  topics : Array[ShareFetchTopic],
  timeout_ms? : Int = 30000,
) -> ShareAcknowledgeResult {
  let version = self.api_version(API_SHARE_ACKNOWLEDGE)
  let d = self.request(
    API_SHARE_ACKNOWLEDGE,
    version,
    encode_share_acknowledge_request(
      version, group_id, member_id, session_epoch, topics,
    ),
    timeout_ms~,
  )
  let (result, throttle) = decode_share_acknowledge_response(version, d)
  self.note_throttle(throttle)
  result
}
