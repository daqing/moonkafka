// Share-group offset admin (KIP-932): DescribeShareGroupOffsets v0/v1
// (key 90), AlterShareGroupOffsets v0 (key 91) and DeleteShareGroupOffsets
// v0 (key 92). Pinned from the matching Request/Response.json files in
// the Kafka 4.3 tree; flexible since v0. Error codes travel as values;
// the Admin's shared retry policy classifies retriable ones.

///|
/// One topic wanted in a DescribeShareGroupOffsets request: its name and
/// the partitions to describe (empty = all).
pub(all) struct ShareOffsetDescribeTopic {
  name : String
  partitions : Array[Int]
} derive(@debug.Debug)

///|
/// One group in a DescribeShareGroupOffsets request; None topics means
/// all topic-partitions.
pub(all) struct ShareOffsetDescribeGroup {
  group_id : String
  topics : Array[ShareOffsetDescribeTopic]?
} derive(@debug.Debug)

///|
/// Encode a DescribeShareGroupOffsets v0/v1 request body.
pub fn encode_describe_share_group_offsets_request(
  groups : Array[ShareOffsetDescribeGroup],
) -> Bytes {
  let body = @buf.Encoder::new()
  body.write_compact_len(groups.length())
  for group in groups {
    body.write_compact_string(group.group_id)
    match group.topics {
      Some(topics) => {
        body.write_compact_len(topics.length())
        for topic in topics {
          body.write_compact_string(topic.name)
          body.write_compact_len(topic.partitions.length())
          for p in topic.partitions {
            body.write_i32(p)
          }
          body.write_tag_buffer()
        }
      }
      None => body.write_compact_len(-1)
    }
    body.write_tag_buffer()
  }
  body.write_tag_buffer()
  body.to_bytes()
}

///|
/// One described partition's share start offset.
pub(all) struct ShareOffsetDescribePartition {
  partition : Int
  start_offset : Int64
  leader_epoch : Int
  lag : Int64
  error_code : Int
  error_message : String?
} derive(@debug.Debug)

///|
pub(all) struct ShareOffsetDescribeTopicResult {
  name : String
  topic_id : Uuid
  partitions : Array[ShareOffsetDescribePartition]
} derive(@debug.Debug)

///|
pub(all) struct ShareOffsetDescribeGroupResult {
  group_id : String
  topics : Array[ShareOffsetDescribeTopicResult]
  error_code : Int
  error_message : String?
} derive(@debug.Debug)

///|
/// Decode a DescribeShareGroupOffsets v0/v1 response body. Lag defaults
/// to -1 in v0.
pub fn decode_describe_share_group_offsets_response(
  version : Int,
  d : @buf.Decoder,
) -> (Array[ShareOffsetDescribeGroupResult], Int) raise {
  if version < 0 || version > 1 {
    raise ProtocolError::ProtocolError(
      "moonkafka implements DescribeShareGroupOffsets v0-v1, got v\{version}",
    )
  }
  let throttle = d.read_i32()
  let groups : Array[ShareOffsetDescribeGroupResult] = []
  let n = d.read_compact_len()
  for _ in 0..<n {
    let group_id = d.read_compact_string()
    let topics : Array[ShareOffsetDescribeTopicResult] = []
    let topic_count = d.read_compact_len()
    for _ in 0..<topic_count {
      let name = d.read_compact_string()
      let topic_id = Uuid::from_bytes(d.read_bytes(16))
      let partitions : Array[ShareOffsetDescribePartition] = []
      let part_count = d.read_compact_len()
      for _ in 0..<part_count {
        let partition = d.read_i32()
        let start_offset = d.read_i64()
        let leader_epoch = d.read_i32()
        let lag = if version >= 1 { d.read_i64() } else { -1L }
        let error_code = d.read_i16()
        let error_message = d.read_compact_nullable_string()
        d.skip_tag_buffer()
        partitions.push({
          partition,
          start_offset,
          leader_epoch,
          lag,
          error_code,
          error_message,
        })
      }
      d.skip_tag_buffer()
      topics.push({ name, topic_id, partitions, })
    }
    let error_code = d.read_i16()
    let error_message = d.read_compact_nullable_string()
    d.skip_tag_buffer()
    groups.push({ group_id, topics, error_code, error_message, })
  }
  d.skip_tag_buffer()
  (groups, throttle)
}

///|
pub async fn BrokerConnection::describe_share_group_offsets(
  self : BrokerConnection,
  groups : Array[ShareOffsetDescribeGroup],
  timeout_ms? : Int = 30000,
) -> Array[ShareOffsetDescribeGroupResult] {
  let version = self.api_version(API_DESCRIBE_SHARE_GROUP_OFFSETS)
  let d = self.request(
    API_DESCRIBE_SHARE_GROUP_OFFSETS,
    version,
    encode_describe_share_group_offsets_request(groups),
    timeout_ms~,
  )
  let (results, throttle) = decode_describe_share_group_offsets_response(
    version, d,
  )
  self.note_throttle(throttle)
  results
}

///|
/// One partition's start offset to alter.
pub(all) struct ShareOffsetAlterPartition {
  partition : Int
  start_offset : Int64
} derive(@debug.Debug)

///|
/// One topic to alter share offsets for.
pub(all) struct ShareOffsetAlterTopic {
  name : String
  partitions : Array[ShareOffsetAlterPartition]
} derive(@debug.Debug)

///|
/// Encode an AlterShareGroupOffsets v0 request body.
pub fn encode_alter_share_group_offsets_request(
  group_id : String,
  topics : Array[ShareOffsetAlterTopic],
) -> Bytes {
  let body = @buf.Encoder::new()
  body.write_compact_string(group_id)
  body.write_compact_len(topics.length())
  for topic in topics {
    body.write_compact_string(topic.name)
    body.write_compact_len(topic.partitions.length())
    for p in topic.partitions {
      body.write_i32(p.partition)
      body.write_i64(p.start_offset)
      body.write_tag_buffer()
    }
    body.write_tag_buffer()
  }
  body.write_tag_buffer()
  body.to_bytes()
}

///|
pub(all) struct ShareOffsetAlterPartitionResult {
  partition : Int
  error_code : Int
  error_message : String?
} derive(@debug.Debug)

///|
pub(all) struct ShareOffsetAlterTopicResult {
  name : String
  topic_id : Uuid
  partitions : Array[ShareOffsetAlterPartitionResult]
} derive(@debug.Debug)

///|
pub struct ShareOffsetAlterResult {
  error_code : Int
  error_message : String?
  topics : Array[ShareOffsetAlterTopicResult]
} derive(@debug.Debug)

///|
/// Decode an AlterShareGroupOffsets v0 response body.
pub fn decode_alter_share_group_offsets_response(
  d : @buf.Decoder,
) -> (ShareOffsetAlterResult, Int) raise {
  let throttle = d.read_i32()
  let error_code = d.read_i16()
  let error_message = d.read_compact_nullable_string()
  let topics : Array[ShareOffsetAlterTopicResult] = []
  let n = d.read_compact_len()
  for _ in 0..<n {
    let name = d.read_compact_string()
    let topic_id = Uuid::from_bytes(d.read_bytes(16))
    let partitions : Array[ShareOffsetAlterPartitionResult] = []
    let part_count = d.read_compact_len()
    for _ in 0..<part_count {
      let partition = d.read_i32()
      let part_error = d.read_i16()
      let part_error_message = d.read_compact_nullable_string()
      d.skip_tag_buffer()
      partitions.push({
        partition,
        error_code: part_error,
        error_message: part_error_message,
      })
    }
    d.skip_tag_buffer()
    topics.push({ name, topic_id, partitions, })
  }
  d.skip_tag_buffer()
  ({ error_code, error_message, topics, }, throttle)
}

///|
pub async fn BrokerConnection::alter_share_group_offsets(
  self : BrokerConnection,
  group_id : String,
  topics : Array[ShareOffsetAlterTopic],
  timeout_ms? : Int = 30000,
) -> ShareOffsetAlterResult {
  let version = self.api_version(API_ALTER_SHARE_GROUP_OFFSETS)
  let d = self.request(
    API_ALTER_SHARE_GROUP_OFFSETS,
    version,
    encode_alter_share_group_offsets_request(group_id, topics),
    timeout_ms~,
  )
  let (result, throttle) = decode_alter_share_group_offsets_response(d)
  self.note_throttle(throttle)
  result
}

///|
/// Encode a DeleteShareGroupOffsets v0 request body.
pub fn encode_delete_share_group_offsets_request(
  group_id : String,
  topics : Array[String],
) -> Bytes {
  let body = @buf.Encoder::new()
  body.write_compact_string(group_id)
  body.write_compact_len(topics.length())
  for name in topics {
    body.write_compact_string(name)
    body.write_tag_buffer()
  }
  body.write_tag_buffer()
  body.to_bytes()
}

///|
pub(all) struct ShareOffsetDeleteTopicResult {
  name : String
  topic_id : Uuid
  error_code : Int
  error_message : String?
} derive(@debug.Debug)

///|
pub struct ShareOffsetDeleteResult {
  error_code : Int
  error_message : String?
  topics : Array[ShareOffsetDeleteTopicResult]
} derive(@debug.Debug)

///|
/// Decode a DeleteShareGroupOffsets v0 response body.
pub fn decode_delete_share_group_offsets_response(
  d : @buf.Decoder,
) -> (ShareOffsetDeleteResult, Int) raise {
  let throttle = d.read_i32()
  let error_code = d.read_i16()
  let error_message = d.read_compact_nullable_string()
  let topics : Array[ShareOffsetDeleteTopicResult] = []
  let n = d.read_compact_len()
  for _ in 0..<n {
    let name = d.read_compact_string()
    let topic_id = Uuid::from_bytes(d.read_bytes(16))
    let topic_error = d.read_i16()
    let topic_error_message = d.read_compact_nullable_string()
    d.skip_tag_buffer()
    topics.push({
      name,
      topic_id,
      error_code: topic_error,
      error_message: topic_error_message,
    })
  }
  d.skip_tag_buffer()
  ({ error_code, error_message, topics, }, throttle)
}

///|
pub async fn BrokerConnection::delete_share_group_offsets(
  self : BrokerConnection,
  group_id : String,
  topics : Array[String],
  timeout_ms? : Int = 30000,
) -> ShareOffsetDeleteResult {
  let version = self.api_version(API_DELETE_SHARE_GROUP_OFFSETS)
  let d = self.request(
    API_DELETE_SHARE_GROUP_OFFSETS,
    version,
    encode_delete_share_group_offsets_request(group_id, topics),
    timeout_ms~,
  )
  let (result, throttle) = decode_delete_share_group_offsets_response(d)
  self.note_throttle(throttle)
  result
}

///|
/// Describe share-group offsets for one group, optionally narrowed to a
/// set of topics (empty describes all topic-partitions). Retriable group
/// errors re-issue the whole call under the admin retry policy.
pub async fn Admin::describe_share_group_offsets(
  self : Admin,
  group_id : String,
  topics : Array[ShareOffsetDescribeTopic],
) -> Array[ShareOffsetDescribeGroupResult] {
  self.with_retries(
    fn(results : Array[ShareOffsetDescribeGroupResult]) {
      let mut retry = false
      for result in results {
        if admin_error_retriable(result.error_code) {
          retry = true
        }
      }
      retry
    },
    async fn() {
      let conn = self.any_conn()
      conn.describe_share_group_offsets(
        [{ group_id, topics: Some(topics), }],
        timeout_ms=self.request_timeout_ms,
      )
    },
  )
}

///|
/// Set share-group partition start offsets. Per-partition error codes
/// come back as values; a retriable top-level error re-issues the call.
pub async fn Admin::alter_share_group_offsets(
  self : Admin,
  group_id : String,
  topics : Array[ShareOffsetAlterTopic],
) -> ShareOffsetAlterResult {
  self.with_retries(
    fn(result : ShareOffsetAlterResult) {
      admin_error_retriable(result.error_code)
    },
    async fn() {
      let conn = self.any_conn()
      conn.alter_share_group_offsets(
        group_id,
        topics,
        timeout_ms=self.request_timeout_ms,
      )
    },
  )
}

///|
/// Delete the committed share offsets of the given topics for a group.
pub async fn Admin::delete_share_group_offsets(
  self : Admin,
  group_id : String,
  topics : Array[String],
) -> ShareOffsetDeleteResult {
  self.with_retries(
    fn(result : ShareOffsetDeleteResult) {
      admin_error_retriable(result.error_code)
    },
    async fn() {
      let conn = self.any_conn()
      conn.delete_share_group_offsets(
        group_id,
        topics,
        timeout_ms=self.request_timeout_ms,
      )
    },
  )
}
