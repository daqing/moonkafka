// OffsetCommit v8/v9/v10 and OffsetFetch v8/v9/v10 — the committed-offset
// store (Phase 4). Field order pinned from OffsetCommitRequest/Response.json
// and OffsetFetchRequest/Response.json in the Kafka 4.3 tree:
//
// - OffsetCommit v9 is the first KIP-848-capable version (wire shape equal
//   to v8; new error codes); v10 swaps topic names for topic ids.
// - OffsetFetch v8 carries the batched multi-group request; v9 adds the
//   member identity; v10 swaps names for ids. RequireStable rides along
//   since v7.
//
// Broker error codes travel as values; the consumer classifies and retries.

///|
/// One partition's commit outcome: (partition index, error code).
pub struct OffsetCommitPartitionResult {
  partition : Int
  error_code : Int
} derive(@debug.Debug)

///|
/// One fetched committed offset.
pub(all) struct CommittedOffset {
  topic : String
  partition : Int
  /// -1 when the partition has no committed offset.
  offset : Int64
  leader_epoch : Int
  error_code : Int
} derive(@debug.Debug)

///|
/// One group's OffsetFetch result.
pub struct OffsetFetchGroupResult {
  group_id : String
  error_code : Int
  partitions : Array[CommittedOffset]
} derive(@debug.Debug)

///|
/// One topic's partitions to commit: (name, topic id, entries) where an
/// entry is (partition, offset, committed leader epoch). The encoder
/// picks name or id addressing from the negotiated version.
pub(all) struct CommitTopic {
  name : String
  topic_id : Uuid
  partitions : Array[(Int, Int64, Int)]
} derive(@debug.Debug)

///|
/// Encode an OffsetCommit v8/v9/v10 request body. `generation_id_or_
/// member_epoch` is -1 and `member_id` empty for group-less commits;
/// KIP-848 members send their epoch.
pub fn encode_offset_commit_request(
  version : Int,
  group_id : String,
  generation_id_or_member_epoch : Int,
  member_id : String,
  group_instance_id : String?,
  topics : Array[CommitTopic],
) -> Bytes raise {
  if version < 8 || version > 10 {
    raise ProtocolError::ProtocolError(
      "moonkafka implements OffsetCommit v8-v10, got v\{version}",
    )
  }
  let body = @buf.Encoder::new()
  body.write_compact_string(group_id)
  body.write_i32(generation_id_or_member_epoch)
  body.write_compact_string(member_id)
  body.write_compact_nullable_string(group_instance_id)
  body.write_compact_len(topics.length())
  for topic in topics {
    if version >= 10 {
      body.write_bytes(topic.topic_id.to_bytes())
    } else {
      body.write_compact_string(topic.name)
    }
    body.write_compact_len(topic.partitions.length())
    for entry in topic.partitions {
      let (partition, offset, leader_epoch) = entry
      body.write_i32(partition)
      body.write_i64(offset)
      body.write_i32(leader_epoch)
      body.write_compact_nullable_string(None) // committed metadata
      body.write_tag_buffer()
    }
    body.write_tag_buffer()
  }
  body.write_tag_buffer()
  body.to_bytes()
}

///|
/// Decode an OffsetCommit response body (same shape for v8-v10, with the
/// topic name present through v9 and the topic id from v10): flat
/// per-partition results plus the throttle hint.
pub fn decode_offset_commit_response(
  version : Int,
  d : @buf.Decoder,
) -> (Array[OffsetCommitPartitionResult], Int) raise {
  let throttle = d.read_i32()
  let out : Array[OffsetCommitPartitionResult] = []
  let n = d.read_compact_len()
  for _ in 0..<n {
    if version >= 10 {
      d.skip(16) // topic id
    } else {
      ignore(d.read_compact_string())
    }
    let parts = d.read_compact_len()
    for _ in 0..<parts {
      let partition = d.read_i32()
      let error_code = d.read_i16()
      d.skip_tag_buffer()
      out.push({ partition, error_code, })
    }
    d.skip_tag_buffer()
  }
  d.skip_tag_buffer()
  (out, throttle)
}

///|
pub async fn BrokerConnection::offset_commit(
  self : BrokerConnection,
  group_id : String,
  generation_id_or_member_epoch : Int,
  member_id : String,
  group_instance_id? : String? = None,
  topics~ : Array[CommitTopic],
  timeout_ms? : Int = 30000,
) -> Array[OffsetCommitPartitionResult] {
  let version = self.api_version(API_OFFSET_COMMIT)
  let d = self.request(
    API_OFFSET_COMMIT,
    version,
    encode_offset_commit_request(
      version, group_id, generation_id_or_member_epoch, member_id, group_instance_id,
      topics,
    ),
    timeout_ms~,
  )
  let (results, throttle) = decode_offset_commit_response(version, d)
  self.note_throttle(throttle)
  results
}

///|
/// One topic's wanted partitions for an OffsetFetch: (name, topic id,
/// partition indexes). The encoder picks name or id addressing from the
/// negotiated version.
pub(all) struct FetchOffsetTopic {
  name : String
  topic_id : Uuid
  partitions : Array[Int]
} derive(@debug.Debug)

///|
/// Encode an OffsetFetch v8/v9/v10 request body for one group.
/// `member_id`/`member_epoch` identify a KIP-848 member (v9+); a
/// group-less fetch sends ""/-1.
pub fn encode_offset_fetch_request(
  version : Int,
  group_id : String,
  member_id : String,
  member_epoch : Int,
  topics : Array[FetchOffsetTopic],
  require_stable? : Bool = false,
) -> Bytes raise {
  if version < 8 || version > 10 {
    raise ProtocolError::ProtocolError(
      "moonkafka implements OffsetFetch v8-v10, got v\{version}",
    )
  }
  let body = @buf.Encoder::new()
  body.write_compact_len(1) // groups
  body.write_compact_string(group_id)
  if version >= 9 {
    body.write_compact_nullable_string(
      if member_id.is_empty() {
        None
      } else {
        Some(member_id)
      },
    )
    body.write_i32(member_epoch)
  }
  body.write_compact_len(topics.length())
  for topic in topics {
    if version >= 10 {
      body.write_bytes(topic.topic_id.to_bytes())
    } else {
      body.write_compact_string(topic.name)
    }
    body.write_compact_len(topic.partitions.length())
    for p in topic.partitions {
      body.write_i32(p)
    }
    body.write_tag_buffer()
  }
  body.write_bool(require_stable)
  body.write_tag_buffer()
  body.to_bytes()
}

///|
/// Decode an OffsetFetch v8/v9/v10 response body: per-group results
/// (partitions flattened across topics) plus the throttle hint.
pub fn decode_offset_fetch_response(
  version : Int,
  d : @buf.Decoder,
) -> (Array[OffsetFetchGroupResult], Int) raise {
  let throttle = d.read_i32()
  let out : Array[OffsetFetchGroupResult] = []
  let groups = d.read_compact_len()
  for _ in 0..<groups {
    let group_id = d.read_compact_string()
    let partitions : Array[CommittedOffset] = []
    let topics = d.read_compact_len()
    for _ in 0..<topics {
      let name = if version >= 10 {
        d.skip(16)
        ""
      } else {
        d.read_compact_string()
      }
      let n = d.read_compact_len()
      for _ in 0..<n {
        let partition = d.read_i32()
        let offset = d.read_i64()
        let leader_epoch = d.read_i32()
        let _metadata = d.read_compact_nullable_string()
        let error_code = d.read_i16()
        d.skip_tag_buffer()
        partitions.push({
          topic: name,
          partition,
          offset,
          leader_epoch,
          error_code,
        })
      }
      d.skip_tag_buffer()
    }
    let error_code = d.read_i16()
    d.skip_tag_buffer()
    out.push({ group_id, error_code, partitions, })
  }
  d.skip_tag_buffer()
  (out, throttle)
}

///|
pub async fn BrokerConnection::offset_fetch(
  self : BrokerConnection,
  group_id : String,
  member_id? : String = "",
  member_epoch? : Int = -1,
  topics~ : Array[FetchOffsetTopic],
  require_stable? : Bool = false,
  timeout_ms? : Int = 30000,
) -> Array[OffsetFetchGroupResult] {
  let version = self.api_version(API_OFFSET_FETCH)
  let d = self.request(
    API_OFFSET_FETCH,
    version,
    encode_offset_fetch_request(
      version,
      group_id,
      member_id,
      member_epoch,
      topics,
      require_stable~,
    ),
    timeout_ms~,
  )
  let (results, throttle) = decode_offset_fetch_response(version, d)
  self.note_throttle(throttle)
  results
}
