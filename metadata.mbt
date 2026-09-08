// Metadata v12/v13 and DescribeTopicPartitions v0 — cluster topology
// discovery for the data plane, the admin client (P5), and regex
// subscription expansion (P4). Field order is pinned from the Kafka 4.3
// message schemas: MetadataRequest/Response.json and
// DescribeTopicPartitionsRequest/Response.json under
// clients/src/main/resources/common/message/.

///|
pub struct BrokerInfo {
  node_id : Int
  host : String
  port : Int
}

///|
pub struct PartitionInfo {
  index : Int
  leader : Int
  leader_epoch : Int
} derive(@debug.Debug)

///|
pub struct TopicMetadata {
  name : String
  /// The broker's per-topic error code (0 = present); admin callers
  /// treat codes as values, the metadata path raises on non-zero.
  error_code : Int
  topic_id : Uuid
  is_internal : Bool
  partitions : Array[PartitionInfo]
}

///|
pub struct Metadata {
  brokers : Map[Int, BrokerInfo]
  /// The controller's node id (controller-routed admin ops).
  controller_id : Int
  topics : Array[TopicMetadata]
}

///|
/// Skip a compact array of int32 node ids (replicas, ISR, offline, ELR);
/// nullable (-1) arrays contribute nothing.
fn skip_int32_array(d : @buf.Decoder) -> Unit raise {
  let n = d.read_compact_len()
  if n > 0 {
    d.skip(n * 4)
  }
}

///|
/// Encode a Metadata v12/v13 request body (the request shape is identical
/// in both versions). `topics` None asks for all topics; entries are
/// addressed by name with a zero topic id.
pub fn encode_metadata_request(topics : Array[String]?) -> Bytes {
  let body = @buf.Encoder::new()
  match topics {
    None => body.write_compact_len(-1) // null array: all topics
    Some(topics) => {
      body.write_compact_len(topics.length())
      for topic in topics {
        body.write_bytes(Uuid::zero().to_bytes()) // topic_id: by name
        body.write_compact_nullable_string(Some(topic))
        body.write_tag_buffer()
      }
    }
  }
  body.write_bool(true) // allow_auto_topic_creation
  body.write_bool(false) // include_topic_authorized_operations
  body.write_tag_buffer()
  body.to_bytes()
}

///|
/// Decode a Metadata v12 or v13 response body. v13 appends a top-level
/// error code after the topics array. Returns the metadata plus the
/// throttle hint in milliseconds.
pub fn decode_metadata_response(
  version : Int,
  d : @buf.Decoder,
) -> (Metadata, Int) raise {
  if version < 12 || version > 13 {
    raise ProtocolError::ProtocolError(
      "moonkafka implements Metadata v12-v13, got v\{version}",
    )
  }
  let throttle = d.read_i32() // throttle_time_ms
  let broker_count = d.read_compact_len()
  let brokers : Map[Int, BrokerInfo] = Map([])
  for _ in 0..<broker_count {
    let node_id = d.read_i32()
    let host = d.read_compact_string()
    let port = d.read_i32()
    let _rack = d.read_compact_nullable_string()
    d.skip_tag_buffer()
    brokers[node_id] = { node_id, host, port, }
  }
  let _cluster_id = d.read_compact_nullable_string()
  let controller_id = d.read_i32()
  let topic_count = d.read_compact_len()
  let topics : Array[TopicMetadata] = []
  for _ in 0..<topic_count {
    let error_code = d.read_i16()
    let name = match d.read_compact_nullable_string() {
      Some(name) => name
      None => raise @buf.Malformed("metadata topic name is null")
    }
    let topic_id = Uuid::from_bytes(d.read_bytes(16))
    let is_internal = d.read_bool()
    let partition_count = d.read_compact_len()
    let partitions : Array[PartitionInfo] = []
    for _ in 0..<partition_count {
      let partition_error = d.read_i16()
      let index = d.read_i32()
      let leader = d.read_i32()
      let leader_epoch = d.read_i32()
      skip_int32_array(d) // replica_nodes
      skip_int32_array(d) // isr_nodes
      skip_int32_array(d) // offline_replicas
      d.skip_tag_buffer()
      if partition_error == 0 {
        partitions.push({ index, leader, leader_epoch, })
      }
    }
    d.skip(4) // topic_authorized_operations
    d.skip_tag_buffer()
    if error_code != 0 {
      raise BrokerError(error_code, "Metadata failed for topic \{name}")
    }
    topics.push({ name, error_code, topic_id, is_internal, partitions, })
  }
  if version >= 13 {
    let top_level_error = d.read_i16()
    if top_level_error != 0 {
      raise BrokerError(top_level_error, "Metadata failed")
    }
  }
  d.skip_tag_buffer()
  ({ brokers, controller_id, topics, }, throttle)
}

///|
/// Cursor for paginated DescribeTopicPartitions responses: the topic and
/// partition index the next request resumes from.
pub(all) struct TopicPartitionCursor {
  topic_name : String
  partition_index : Int
} derive(@debug.Debug)

///|
/// One page of DescribeTopicPartitions results. `next_cursor` is set when
/// the broker truncated the response at the partition limit.
pub struct DescribeTopicPartitions {
  topics : Array[TopicMetadata]
  next_cursor : TopicPartitionCursor?
}

///|
/// Encode a DescribeTopicPartitions v0 request body. An empty `topics`
/// list describes all topics (what regex subscription expands against);
/// `cursor` resumes a previous page. A nullable struct on the wire is a
/// signed byte marker, -1 = null, 1 = present (per the Java generator).
pub fn encode_describe_topic_partitions_request(
  topics : Array[String],
  response_partition_limit : Int,
  cursor? : TopicPartitionCursor? = None,
) -> Bytes {
  let body = @buf.Encoder::new()
  body.write_compact_len(topics.length())
  for topic in topics {
    body.write_compact_string(topic)
    body.write_tag_buffer()
  }
  body.write_i32(response_partition_limit)
  match cursor {
    None => body.write_i8(-1)
    Some(cursor) => {
      body.write_i8(1)
      body.write_compact_string(cursor.topic_name)
      body.write_i32(cursor.partition_index)
      body.write_tag_buffer()
    }
  }
  body.write_tag_buffer()
  body.to_bytes()
}

///|
/// Decode a DescribeTopicPartitions v0 response body. Partition entries
/// carry two nullable ELR arrays (skipped); a null next cursor means the
/// listing is complete. Returns the page plus the throttle hint.
pub fn decode_describe_topic_partitions_response(
  d : @buf.Decoder,
) -> (DescribeTopicPartitions, Int) raise {
  let throttle = d.read_i32() // throttle_time_ms
  let topic_count = d.read_compact_len()
  let topics : Array[TopicMetadata] = []
  for _ in 0..<topic_count {
    let error_code = d.read_i16()
    let name = match d.read_compact_nullable_string() {
      Some(name) => name
      None => raise @buf.Malformed("describe topic name is null")
    }
    let topic_id = Uuid::from_bytes(d.read_bytes(16))
    let is_internal = d.read_bool()
    let partition_count = d.read_compact_len()
    let partitions : Array[PartitionInfo] = []
    for _ in 0..<partition_count {
      let partition_error = d.read_i16()
      let index = d.read_i32()
      let leader = d.read_i32()
      let leader_epoch = d.read_i32()
      skip_int32_array(d) // replica_nodes
      skip_int32_array(d) // isr_nodes
      skip_int32_array(d) // eligible_leader_replicas (nullable)
      skip_int32_array(d) // last_known_elr (nullable)
      skip_int32_array(d) // offline_replicas
      d.skip_tag_buffer()
      if partition_error == 0 {
        partitions.push({ index, leader, leader_epoch, })
      }
    }
    d.skip(4) // topic_authorized_operations
    d.skip_tag_buffer()
    // Per-topic error codes travel as values: the admin client surfaces
    // them per topic instead of failing the whole page.
    topics.push({ name, error_code, topic_id, is_internal, partitions, })
  }
  let next_cursor = if d.read_i8() >= 0 {
    let topic_name = d.read_compact_string()
    let partition_index = d.read_i32()
    d.skip_tag_buffer()
    Some({ topic_name, partition_index, })
  } else {
    None
  }
  d.skip_tag_buffer()
  ({ topics, next_cursor, }, throttle)
}
