// Admin topic operations (Phase 5): CreateTopics v7 (configs +
// replication assignment), DeleteTopics v6 (by name or topic id —
// topic-id-first addressing, KIP-516), CreatePartitions v3,
// DeleteRecords v2, and OffsetDelete v0 (the one non-flexible API in
// the set — header v0 framing). Field order pinned from the message
// schemas in the Kafka 4.3 tree. All results carry per-topic error
// codes as values; the Admin retry policy re-issues retriable ones.

///|
/// One topic to create: either num_partitions/replication_factor (use
/// -1 for broker defaults) or explicit replica assignments, plus
/// config overrides.
pub(all) struct CreatableTopic {
  name : String
  num_partitions : Int
  replication_factor : Int
  assignments : Array[(Int, Array[Int])]
  configs : Array[(String, String?)]
} derive(@debug.Debug)

///|
pub struct CreateTopicsResult {
  name : String
  topic_id : Uuid
  error_code : Int
  error_message : String?
} derive(@debug.Debug)

///|
/// Encode a CreateTopics v7 request body.
pub fn encode_create_topics_request(
  topics : Array[CreatableTopic],
  timeout_ms : Int,
  validate_only : Bool,
) -> Bytes {
  let body = @buf.Encoder::new()
  body.write_compact_len(topics.length())
  for topic in topics {
    body.write_compact_string(topic.name)
    body.write_i32(topic.num_partitions)
    body.write_i16(topic.replication_factor)
    body.write_compact_len(topic.assignments.length())
    for entry in topic.assignments {
      let (partition, brokers) = entry
      body.write_i32(partition)
      body.write_compact_len(brokers.length())
      for broker in brokers {
        body.write_i32(broker)
      }
      body.write_tag_buffer()
    }
    body.write_compact_len(topic.configs.length())
    for entry in topic.configs {
      let (name, value) = entry
      body.write_compact_string(name)
      body.write_compact_nullable_string(value)
      body.write_tag_buffer()
    }
    body.write_tag_buffer()
  }
  body.write_i32(timeout_ms)
  body.write_bool(validate_only)
  body.write_tag_buffer()
  body.to_bytes()
}

///|
/// Decode a CreateTopics v7 response body: per-topic results (the
/// echoed config block is skipped — the topic configs come back through
/// DescribeConfigs).
pub fn decode_create_topics_response(
  d : @buf.Decoder,
) -> (Array[CreateTopicsResult], Int) raise {
  let throttle = d.read_i32()
  let out : Array[CreateTopicsResult] = []
  let n = d.read_compact_len()
  for _ in 0..<n {
    let name = d.read_compact_string()
    let topic_id = Uuid::from_bytes(d.read_bytes(16))
    let error_code = d.read_i16()
    let error_message = d.read_compact_nullable_string()
    ignore(d.read_i16()) // topic config error code (v5+)
    ignore(d.read_i32()) // num partitions (v5+)
    ignore(d.read_i16()) // replication factor (v5+)
    let configs = d.read_compact_len() // nullable configs (v5+)
    for _ in 0..<configs {
      ignore(d.read_compact_string())
      ignore(d.read_compact_nullable_string())
      ignore(d.read_bool())
      ignore(d.read_i8())
      ignore(d.read_bool())
      d.skip_tag_buffer()
    }
    d.skip_tag_buffer()
    out.push({ name, topic_id, error_code, error_message, })
  }
  d.skip_tag_buffer()
  (out, throttle)
}

///|
pub async fn BrokerConnection::create_topics(
  self : BrokerConnection,
  topics : Array[CreatableTopic],
  timeout_ms : Int,
  validate_only? : Bool = false,
) -> Array[CreateTopicsResult] {
  let version = self.api_version(API_CREATE_TOPICS)
  let d = self.request(
    API_CREATE_TOPICS,
    version,
    encode_create_topics_request(topics, timeout_ms, validate_only),
    timeout_ms~,
  )
  let (results, throttle) = decode_create_topics_response(d)
  self.note_throttle(throttle)
  results
}

///|
pub struct DeleteTopicsResult {
  name : String?
  topic_id : Uuid
  error_code : Int
  error_message : String?
} derive(@debug.Debug)

///|
/// Encode a DeleteTopics v6 request body: topics addressed by name, by
/// topic id, or mixed (topic-id-first addressing, KIP-516 — a null name
/// with a set id deletes by id alone).
pub fn encode_delete_topics_request(
  names : Array[String],
  topic_ids : Array[Uuid],
  timeout_ms : Int,
) -> Bytes {
  let body = @buf.Encoder::new()
  body.write_compact_len(names.length() + topic_ids.length())
  for name in names {
    body.write_compact_nullable_string(Some(name))
    body.write_bytes(Uuid::zero().to_bytes())
    body.write_tag_buffer()
  }
  for topic_id in topic_ids {
    body.write_compact_nullable_string(None)
    body.write_bytes(topic_id.to_bytes())
    body.write_tag_buffer()
  }
  body.write_i32(timeout_ms)
  body.write_tag_buffer()
  body.to_bytes()
}

///|
/// Decode a DeleteTopics v6 response body.
pub fn decode_delete_topics_response(
  d : @buf.Decoder,
) -> (Array[DeleteTopicsResult], Int) raise {
  let throttle = d.read_i32()
  let out : Array[DeleteTopicsResult] = []
  let n = d.read_compact_len()
  for _ in 0..<n {
    let name = d.read_compact_nullable_string()
    let topic_id = Uuid::from_bytes(d.read_bytes(16))
    let error_code = d.read_i16()
    let error_message = d.read_compact_nullable_string()
    d.skip_tag_buffer()
    out.push({ name, topic_id, error_code, error_message, })
  }
  d.skip_tag_buffer()
  (out, throttle)
}

///|
pub async fn BrokerConnection::delete_topics(
  self : BrokerConnection,
  names : Array[String],
  topic_ids : Array[Uuid],
  timeout_ms : Int,
) -> Array[DeleteTopicsResult] {
  let version = self.api_version(API_DELETE_TOPICS)
  let d = self.request(
    API_DELETE_TOPICS,
    version,
    encode_delete_topics_request(names, topic_ids, timeout_ms),
    timeout_ms~,
  )
  let (results, throttle) = decode_delete_topics_response(d)
  self.note_throttle(throttle)
  results
}

///|
/// One topic to grow: the new partition count, optionally with replica
/// assignments for the new partitions (new-assignments-first order).
pub(all) struct CreatePartitionsSpec {
  name : String
  count : Int
  /// Replica broker lists for each NEW partition, or None for
  /// broker-assigned placement.
  assignments : Array[Array[Int]]?
} derive(@debug.Debug)

///|
pub struct CreatePartitionsResult {
  name : String
  error_code : Int
  error_message : String?
} derive(@debug.Debug)

///|
/// Encode a CreatePartitions v3 request body.
pub fn encode_create_partitions_request(
  topics : Array[CreatePartitionsSpec],
  timeout_ms : Int,
  validate_only : Bool,
) -> Bytes {
  let body = @buf.Encoder::new()
  body.write_compact_len(topics.length())
  for topic in topics {
    body.write_compact_string(topic.name)
    body.write_i32(topic.count)
    match topic.assignments {
      Some(assignments) => {
        body.write_compact_len(assignments.length())
        for brokers in assignments {
          body.write_compact_len(brokers.length())
          for broker in brokers {
            body.write_i32(broker)
          }
          body.write_tag_buffer()
        }
      }
      None => body.write_compact_len(-1)
    }
    body.write_tag_buffer()
  }
  body.write_i32(timeout_ms)
  body.write_bool(validate_only)
  body.write_tag_buffer()
  body.to_bytes()
}

///|
/// Decode a CreatePartitions v3 response body.
pub fn decode_create_partitions_response(
  d : @buf.Decoder,
) -> (Array[CreatePartitionsResult], Int) raise {
  let throttle = d.read_i32()
  let out : Array[CreatePartitionsResult] = []
  let n = d.read_compact_len()
  for _ in 0..<n {
    let name = d.read_compact_string()
    let error_code = d.read_i16()
    let error_message = d.read_compact_nullable_string()
    d.skip_tag_buffer()
    out.push({ name, error_code, error_message, })
  }
  d.skip_tag_buffer()
  (out, throttle)
}

///|
pub async fn BrokerConnection::create_partitions(
  self : BrokerConnection,
  topics : Array[CreatePartitionsSpec],
  timeout_ms : Int,
  validate_only? : Bool = false,
) -> Array[CreatePartitionsResult] {
  let version = self.api_version(API_CREATE_PARTITIONS)
  let d = self.request(
    API_CREATE_PARTITIONS,
    version,
    encode_create_partitions_request(topics, timeout_ms, validate_only),
    timeout_ms~,
  )
  let (results, throttle) = decode_create_partitions_response(d)
  self.note_throttle(throttle)
  results
}

///|
/// One deleted-records result: the new low watermark, or the error.
pub struct DeleteRecordsResult {
  topic : String
  partition : Int
  low_watermark : Int64
  error_code : Int
} derive(@debug.Debug)

///|
/// Encode a DeleteRecords v2 request body: records up to (and
/// including) each offset are deleted; OFFSET_START/-1 means "delete
/// everything".
pub fn encode_delete_records_request(
  topics : Array[(String, Array[(Int, Int64)])],
  timeout_ms : Int,
) -> Bytes {
  let body = @buf.Encoder::new()
  body.write_compact_len(topics.length())
  for entry in topics {
    let (name, partitions) = entry
    body.write_compact_string(name)
    body.write_compact_len(partitions.length())
    for partition in partitions {
      let (index, offset) = partition
      body.write_i32(index)
      body.write_i64(offset)
      body.write_tag_buffer()
    }
    body.write_tag_buffer()
  }
  body.write_i32(timeout_ms)
  body.write_tag_buffer()
  body.to_bytes()
}

///|
/// Decode a DeleteRecords v2 response body.
pub fn decode_delete_records_response(
  d : @buf.Decoder,
) -> (Array[DeleteRecordsResult], Int) raise {
  let throttle = d.read_i32()
  let out : Array[DeleteRecordsResult] = []
  let n = d.read_compact_len()
  for _ in 0..<n {
    let topic = d.read_compact_string()
    let parts = d.read_compact_len()
    for _ in 0..<parts {
      let partition = d.read_i32()
      let low_watermark = d.read_i64()
      let error_code = d.read_i16()
      d.skip_tag_buffer()
      out.push({ topic, partition, low_watermark, error_code, })
    }
    d.skip_tag_buffer()
  }
  d.skip_tag_buffer()
  (out, throttle)
}

///|
pub async fn BrokerConnection::delete_records(
  self : BrokerConnection,
  topics : Array[(String, Array[(Int, Int64)])],
  timeout_ms : Int,
) -> Array[DeleteRecordsResult] {
  let version = self.api_version(API_DELETE_RECORDS)
  let d = self.request(
    API_DELETE_RECORDS,
    version,
    encode_delete_records_request(topics, timeout_ms),
    timeout_ms~,
  )
  let (results, throttle) = decode_delete_records_response(d)
  self.note_throttle(throttle)
  results
}

///|
pub struct OffsetDeleteResult {
  /// The group-level error, or 0.
  error_code : Int
  partitions : Array[OffsetDeletePartitionResult]
} derive(@debug.Debug)

///|
pub struct OffsetDeletePartitionResult {
  topic : String
  partition : Int
  error_code : Int
} derive(@debug.Debug)

///|
/// Encode an OffsetDelete v0 request body: the committed offsets to
/// delete for a group. v0 is not flexible — header v0 framing.
pub fn encode_offset_delete_request(
  group_id : String,
  topics : Array[(String, Array[Int])],
) -> Bytes {
  // Non-flexible strings: int16 length + UTF-8 bytes.
  let body = @buf.Encoder::new()
  let group = @utf8.encode(group_id)
  body.write_i16(group.length())
  body.write_bytes(group)
  body.write_i32(topics.length())
  for entry in topics {
    let (name, partitions) = entry
    let bytes = @utf8.encode(name)
    body.write_i16(bytes.length())
    body.write_bytes(bytes)
    body.write_i32(partitions.length())
    for partition in partitions {
      body.write_i32(partition)
    }
  }
  body.to_bytes()
}

///|
/// Decode an OffsetDelete v0 response body (non-flexible: no tag
/// buffers anywhere).
pub fn decode_offset_delete_response(
  d : @buf.Decoder,
) -> (OffsetDeleteResult, Int) raise {
  let error_code = d.read_i16()
  let throttle = d.read_i32()
  let partitions : Array[OffsetDeletePartitionResult] = []
  let n = d.read_i32()
  for _ in 0..<n {
    let name_len = d.read_i16()
    let topic = @utf8.decode_lossy(d.read_bytes(name_len)[:])
    let parts = d.read_i32()
    for _ in 0..<parts {
      let partition = d.read_i32()
      let partition_error = d.read_i16()
      partitions.push({ topic, partition, error_code: partition_error, })
    }
  }
  ({ error_code, partitions, }, throttle)
}

///|
pub async fn BrokerConnection::offset_delete(
  self : BrokerConnection,
  group_id : String,
  topics : Array[(String, Array[Int])],
  timeout_ms : Int,
) -> OffsetDeleteResult {
  // OffsetDelete v0 is not flexible: header v0 framing on both sides.
  let d = self.request_raw(
    API_OFFSET_DELETE,
    0,
    encode_offset_delete_request(group_id, topics),
    timeout_ms~,
    flexible=false,
  )
  let (result, throttle) = decode_offset_delete_response(d)
  self.note_throttle(throttle)
  result
}

///|
/// Create topics: configs and replica assignments per topic, results
/// per topic. Retriable per-topic errors re-issue under the policy.
pub async fn Admin::create_topics(
  self : Admin,
  topics : Array[CreatableTopic],
  validate_only? : Bool = false,
) -> Array[CreateTopicsResult] {
  self.with_retries(
    fn(results : Array[CreateTopicsResult]) {
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
      conn.create_topics(topics, self.request_timeout_ms, validate_only~)
    },
  )
}

///|
/// Delete topics by name, by topic id, or mixed (topic-id-first when
/// only an id is given).
pub async fn Admin::delete_topics(
  self : Admin,
  names : Array[String],
  topic_ids : Array[Uuid],
) -> Array[DeleteTopicsResult] {
  self.with_retries(
    fn(results : Array[DeleteTopicsResult]) {
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
      conn.delete_topics(names, topic_ids, self.request_timeout_ms)
    },
  )
}

///|
/// Grow topics to new partition counts, optionally with explicit
/// replica placement for the new partitions.
pub async fn Admin::create_partitions(
  self : Admin,
  topics : Array[CreatePartitionsSpec],
  validate_only? : Bool = false,
) -> Array[CreatePartitionsResult] {
  self.with_retries(
    fn(results : Array[CreatePartitionsResult]) {
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
      conn.create_partitions(topics, self.request_timeout_ms, validate_only~)
    },
  )
}

///|
/// Delete records up to the given offsets; results carry the new low
/// watermarks.
pub async fn Admin::delete_records(
  self : Admin,
  topics : Array[(String, Array[(Int, Int64)])],
) -> Array[DeleteRecordsResult] {
  self.with_retries(
    fn(results : Array[DeleteRecordsResult]) {
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
      conn.delete_records(topics, self.request_timeout_ms)
    },
  )
}

///|
/// Delete committed offsets of a consumer group for the given topic
/// partitions.
pub async fn Admin::offset_delete(
  self : Admin,
  group_id : String,
  topics : Array[(String, Array[Int])],
) -> OffsetDeleteResult {
  self.with_retries(
    fn(result : OffsetDeleteResult) { admin_error_retriable(result.error_code) },
    async fn() {
      let conn = self.any_conn()
      conn.offset_delete(group_id, topics, self.request_timeout_ms)
    },
  )
}
