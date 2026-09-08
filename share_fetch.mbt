// ShareFetch v1/v2 (key 78) — the KIP-932 fetch RPC for share groups.
// Pinned from ShareFetchRequest.json / ShareFetchResponse.json in the
// Kafka 4.3 tree; flexible since v0.
//
// A share fetch borrows the consumer incremental-session model (epoch 0
// opens the session, -1 closes it, positive counts advance it) and
// couples acquisition with acknowledgement: each per-partition request
// may carry acknowledgement batches for records the member already holds
// (accept/release/reject), so a single round-trip both acknowledges
// previously acquired records and acquires new ones.
//
// v1 (GA): MaxRecords/BatchSize and AcquisitionLockTimeoutMs; v2 adds
// ShareAcquireMode and IsRenewAck (tagged-ignorable fields we default).
// The PartitionMaxBytes field is pinned to version 0 only, so the v1/v2
// request omits it entirely.

///|
/// One offset range of records to acknowledge, with one delivery type
/// per record offset: 0 Gap, 1 Accept, 2 Release, 3 Reject, 4 Renew.
pub(all) struct ShareAckBatch {
  first_offset : Int64
  last_offset : Int64
  acknowledge_types : Array[Int]
} derive(@debug.Debug)

///|
/// One partition's acknowledgment batches in a share-fetch or
/// share-acknowledge request.
pub(all) struct ShareFetchPartition {
  partition : Int
  acknowledgment_batches : Array[ShareAckBatch]
} derive(@debug.Debug)

///|
/// One topic to share-fetch: topic-id addressed, so the cluster must
/// resolve id=name from metadata before building the request.
pub(all) struct ShareFetchTopic {
  topic_id : Uuid
  partitions : Array[ShareFetchPartition]
} derive(@debug.Debug)

///|
/// One topic's partitions to drop from the established share session.
pub(all) struct ShareForgottenTopic {
  topic_id : Uuid
  partitions : Array[Int]
} derive(@debug.Debug)

///|
/// Encode a ShareFetch v1/v2 request body. `acknowledge` carries the
/// batches to send on each partition (may be empty; the array is then a
/// single -1 null marker). v2's tagged ShareAcquireMode/IsRenewAck stay
/// at their defaults (batch-optimized, no renew acks).
pub fn encode_share_fetch_request(
  version : Int,
  group_id : String,
  member_id : String,
  session_epoch : Int,
  topics : Array[ShareFetchTopic],
  forgotten~ : Array[ShareForgottenTopic],
  max_wait_ms~ : Int,
  min_bytes? : Int = 1,
  max_bytes? : Int = 0x7fffffff,
  max_records? : Int = 100,
  batch_size? : Int = 10,
) -> Bytes raise {
  if version < 1 || version > 2 {
    raise ProtocolError::ProtocolError(
      "moonkafka implements ShareFetch v1-v2, got v\{version}",
    )
  }
  let body = @buf.Encoder::new()
  body.write_compact_nullable_string(Some(group_id))
  body.write_compact_nullable_string(Some(member_id))
  body.write_i32(session_epoch)
  body.write_i32(max_wait_ms)
  body.write_i32(min_bytes)
  body.write_i32(max_bytes)
  body.write_i32(max_records)
  body.write_i32(batch_size)
  // v2 tagged fields defaulted: ShareAcquireMode 0, IsRenewAck false.
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
  body.write_compact_len(forgotten.length())
  for topic in forgotten {
    body.write_bytes(topic.topic_id.to_bytes())
    body.write_compact_len(topic.partitions.length())
    for p in topic.partitions {
      body.write_i32(p)
    }
    body.write_tag_buffer()
  }
  body.write_tag_buffer()
  body.to_bytes()
}

///|
fn write_ack_batches(
  body : @buf.Encoder,
  batches : Array[ShareAckBatch],
) -> Unit {
  if batches.is_empty() {
    body.write_compact_len(-1)
    return
  }
  body.write_compact_len(batches.length())
  for batch in batches {
    body.write_i64(batch.first_offset)
    body.write_i64(batch.last_offset)
    body.write_compact_len(batch.acknowledge_types.length())
    for t in batch.acknowledge_types {
      body.write_i8(t)
    }
  }
}

///|
/// One share fetch response body. Records decode through the shared
/// record-batch machinery; acquired ranges carry their delivery count.
pub struct ShareFetchResult {
  error_code : Int
  error_message : String?
  acquisition_lock_timeout_ms : Int
  topics : Array[ShareFetchTopicResult]
} derive(@debug.Debug)

///|
pub struct ShareFetchTopicResult {
  topic_id : Uuid
  partitions : Array[ShareFetchPartitionResult]
} derive(@debug.Debug)

///|
pub struct ShareFetchPartitionResult {
  partition : Int
  error_code : Int
  error_message : String?
  acknowledge_error_code : Int
  acknowledge_error_message : String?
  leader_id : Int
  leader_epoch : Int
  records : Array[Record]
  batches : Array[DecodedBatch]
  records_complete : Bool
  acquired : Array[ShareAcquiredRange]
} derive(@debug.Debug)

///|
/// One acquired offset range in a share fetch response.
pub(all) struct ShareAcquiredRange {
  first_offset : Int64
  last_offset : Int64
  delivery_count : Int
} derive(@debug.Debug)

///|
/// Decode a ShareFetch v1/v2 response body. The tagged per-partition
/// fields and top-level NodeEndpoints are skipped; acquired ranges and
/// records are decoded.
pub fn decode_share_fetch_response(
  version : Int,
  d : @buf.Decoder,
) -> (ShareFetchResult, Int) raise {
  if version < 1 || version > 2 {
    raise ProtocolError::ProtocolError(
      "moonkafka implements ShareFetch v1-v2, got v\{version}",
    )
  }
  let throttle = d.read_i32()
  let error_code = d.read_i16()
  let error_message = d.read_compact_nullable_string()
  let acquisition_lock_timeout_ms = d.read_i32()
  let topics : Array[ShareFetchTopicResult] = []
  let topic_count = d.read_compact_len()
  for _ in 0..<topic_count {
    let topic_id = Uuid::from_bytes(d.read_bytes(16))
    let partitions : Array[ShareFetchPartitionResult] = []
    let part_count = d.read_compact_len()
    for _ in 0..<part_count {
      let partition = d.read_i32()
      let part_error = d.read_i16()
      let part_error_message = d.read_compact_nullable_string()
      let ack_error = d.read_i16()
      let ack_error_message = d.read_compact_nullable_string()
      let leader_id = d.read_i32()
      let leader_epoch = d.read_i32()
      let records_len = d.read_compact_len() // nullable records; -1 = null
      let (records, batches, truncated) = if records_len > 0 {
        let (detailed, cut) = decode_record_batches_detailed(
          d.read_bytes(records_len),
        )
        let flat : Array[Record] = []
        for batch in detailed {
          if batch.is_control {
            continue
          }
          for record in batch.records {
            flat.push(record)
          }
        }
        (flat, detailed, cut)
      } else {
        ([], [], false)
      }
      let acquired : Array[ShareAcquiredRange] = []
      let acq_count = d.read_compact_len() // nullable; -1 = null
      if acq_count > 0 {
        for _ in 0..<acq_count {
          let first_offset = d.read_i64()
          let last_offset = d.read_i64()
          let delivery_count = d.read_i16()
          acquired.push({ first_offset, last_offset, delivery_count, })
        }
      }
      d.skip_tag_buffer()
      partitions.push({
        partition,
        error_code: part_error,
        error_message: part_error_message,
        acknowledge_error_code: ack_error,
        acknowledge_error_message: ack_error_message,
        leader_id,
        leader_epoch,
        records,
        batches,
        records_complete: !truncated,
        acquired,
      })
    }
    d.skip_tag_buffer()
    topics.push({ topic_id, partitions, })
  }
  // NodeEndpoints: skipper the tagged top-level array.
  d.skip_tag_buffer()
  (
    { error_code, error_message, acquisition_lock_timeout_ms, topics, },
    throttle,
  )
}

///|
pub async fn BrokerConnection::share_fetch(
  self : BrokerConnection,
  group_id : String,
  member_id : String,
  session_epoch : Int,
  topics : Array[ShareFetchTopic],
  forgotten? : Array[ShareForgottenTopic] = [],
  max_wait_ms~ : Int,
  timeout_ms? : Int = 30000,
) -> ShareFetchResult {
  let version = self.api_version(API_SHARE_FETCH)
  let d = self.request(
    API_SHARE_FETCH,
    version,
    encode_share_fetch_request(
      version,
      group_id,
      member_id,
      session_epoch,
      topics,
      forgotten~,
      max_wait_ms~,
    ),
    timeout_ms~,
  )
  let (result, throttle) = decode_share_fetch_response(version, d)
  self.note_throttle(throttle)
  result
}
