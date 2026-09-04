// Kafka RPC layer: request framing and the four APIs used by the simple
// consumer. Targets Kafka 4.x brokers (flexible message versions only).

///|
pub const API_FETCH : Int = 1

///|
pub const API_LIST_OFFSETS : Int = 2

///|
pub const API_METADATA : Int = 3

///|
pub const API_API_VERSIONS : Int = 18

///|
const API_VERSIONS_VERSION : Int = 3

///|
const METADATA_VERSION : Int = 12

///|
const LIST_OFFSETS_VERSION : Int = 7

///|
// Fetch v12 is the last version that addresses topics by name;
// v13+ requires topic UUIDs from Metadata.
const FETCH_VERSION : Int = 12

///|
pub(all) suberror ProtocolError {
  ProtocolError(String)
}

///|
/// Encode a full request frame: INT32 size prefix + header v2 + body.
/// Header v2 keeps client_id as a legacy NULLABLE_STRING (see
/// RequestHeader.json) followed by an empty tag buffer.
pub fn encode_request(
  api_key : Int,
  api_version : Int,
  correlation_id : Int,
  client_id : String,
  body : Bytes,
) -> Bytes {
  let header = Encoder::new()
  header.write_i16(api_key)
  header.write_i16(api_version)
  header.write_i32(correlation_id)
  header.write_nullable_string(Some(client_id))
  header.write_tag_buffer()
  let frame = Encoder::new()
  frame.write_i32(header.buf.length() + body.length())
  frame.write_bytes(header.to_bytes())
  frame.write_bytes(body)
  frame.to_bytes()
}

///|
pub fn encode_api_versions_request() -> Bytes {
  let body = Encoder::new()
  body.write_compact_string("moonkafka")
  body.write_compact_string("0.1.0")
  body.write_tag_buffer()
  body.to_bytes()
}

///|
/// Check the ApiVersions v3 response; response header is v0 (no tag buffer).
/// Raises unless the broker supports the API versions this client uses.
pub fn decode_api_versions_response(d : Decoder) -> Unit raise {
  let error_code = d.read_i16()
  if error_code != 0 {
    raise ProtocolError::ProtocolError(
      "ApiVersions failed: \{error_name(error_code)}",
    )
  }
  let count = d.read_compact_len()
  for _ in 0..<count {
    let key = d.read_i16()
    let min_version = d.read_i16()
    let max_version = d.read_i16()
    d.skip_tag_buffer()
    let required = match key {
      API_FETCH => FETCH_VERSION
      API_METADATA => METADATA_VERSION
      API_LIST_OFFSETS => LIST_OFFSETS_VERSION
      _ => continue
    }
    if required < min_version || required > max_version {
      raise ProtocolError::ProtocolError(
        "broker does not support API key \{key} version \{required} (range \{min_version}-\{max_version}); only Kafka 4.x is supported",
      )
    }
  }
  // throttle_time_ms, top-level tag buffer
  d.skip(4)
  d.skip_tag_buffer()
}

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
}

///|
pub struct TopicMetadata {
  name : String
  partitions : Array[PartitionInfo]
}

///|
pub struct Metadata {
  brokers : Map[Int, BrokerInfo]
  topics : Array[TopicMetadata]
}

///|
pub fn encode_metadata_request(topic : String) -> Bytes {
  let body = Encoder::new()
  body.write_compact_len(1) // one topic
  body.write_bytes(Bytes::make(16, b'\x00')) // topic_id: unused
  body.write_compact_nullable_string(Some(topic))
  body.write_tag_buffer()
  body.write_bool(true) // allow_auto_topic_creation
  body.write_bool(false) // include_topic_authorized_operations
  body.write_tag_buffer()
  body.to_bytes()
}

///|
/// Decode a Metadata v12 response (after the v1 response header).
pub fn decode_metadata_response(d : Decoder) -> Metadata raise {
  d.skip(4) // throttle_time_ms
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
  let _controller_id = d.read_i32()
  let topic_count = d.read_compact_len()
  let topics : Array[TopicMetadata] = []
  for _ in 0..<topic_count {
    let error_code = d.read_i16()
    let name = match d.read_compact_nullable_string() {
      Some(name) => name
      None => raise DecodeError::Malformed("metadata topic name is null")
    }
    d.skip(16) // topic_id
    let _is_internal = d.read_bool()
    let partition_count = d.read_compact_len()
    let partitions : Array[PartitionInfo] = []
    for _ in 0..<partition_count {
      let partition_error = d.read_i16()
      let index = d.read_i32()
      let leader = d.read_i32()
      let leader_epoch = d.read_i32()
      for _ in 0..<3 { // replica_nodes, isr_nodes, offline_replicas
        let n = d.read_compact_len()
        d.skip(n * 4)
      }
      d.skip_tag_buffer()
      if partition_error == 0 {
        partitions.push({ index, leader, leader_epoch, })
      }
    }
    d.skip(4) // topic_authorized_operations
    d.skip_tag_buffer()
    if error_code != 0 {
      raise ProtocolError::ProtocolError(
        "Metadata failed for topic \{name}: \{error_name(error_code)}",
      )
    }
    topics.push({ name, partitions, })
  }
  d.skip_tag_buffer()
  { brokers, topics, }
}

///|
pub fn encode_list_offsets_request(
  topic : String,
  partitions : Array[PartitionInfo],
  timestamp : Int64,
) -> Bytes {
  let body = Encoder::new()
  body.write_i32(-1) // replica_id: consumer
  body.write_i8(0) // isolation_level: READ_UNCOMMITTED
  body.write_compact_len(1) // one topic
  body.write_compact_string(topic)
  body.write_compact_len(partitions.length())
  for p in partitions {
    body.write_i32(p.index)
    body.write_i32(p.leader_epoch)
    body.write_i64(timestamp)
    body.write_tag_buffer()
  }
  body.write_tag_buffer()
  body.write_tag_buffer()
  body.to_bytes()
}

///|
/// Decode a ListOffsets v7 response; returns partition index -> offset.
pub fn decode_list_offsets_response(d : Decoder) -> Map[Int, Int64] raise {
  d.skip(4) // throttle_time_ms
  let offsets : Map[Int, Int64] = Map([])
  let topic_count = d.read_compact_len()
  for _ in 0..<topic_count {
    let _name = d.read_compact_string()
    let partition_count = d.read_compact_len()
    for _ in 0..<partition_count {
      let index = d.read_i32()
      let error_code = d.read_i16()
      let _timestamp = d.read_i64()
      let offset = d.read_i64()
      let _leader_epoch = d.read_i32()
      d.skip_tag_buffer()
      if error_code != 0 {
        raise ProtocolError::ProtocolError(
          "ListOffsets failed for partition \{index}: \{error_name(error_code)}",
        )
      }
      offsets[index] = offset
    }
    d.skip_tag_buffer()
  }
  d.skip_tag_buffer()
  offsets
}

///|
pub fn encode_fetch_request(
  topic : String,
  partitions : Array[(PartitionInfo, Int64)],
  max_wait_ms : Int,
  max_bytes : Int,
) -> Bytes {
  let body = Encoder::new()
  body.write_i32(-1) // replica_id: consumer
  body.write_i32(max_wait_ms)
  body.write_i32(1) // min_bytes
  body.write_i32(max_bytes)
  body.write_i8(0) // isolation_level: READ_UNCOMMITTED
  body.write_i32(0) // session_id: no incremental fetch session
  body.write_i32(-1) // session_epoch
  body.write_compact_len(1) // one topic
  body.write_compact_string(topic)
  body.write_compact_len(partitions.length())
  for entry in partitions {
    let (p, offset) = entry
    body.write_i32(p.index)
    body.write_i32(p.leader_epoch)
    body.write_i64(offset)
    body.write_i32(-1) // last_fetched_epoch
    body.write_i64(-1L) // log_start_offset
    body.write_i32(max_bytes) // partition_max_bytes
    body.write_tag_buffer()
  }
  body.write_tag_buffer()
  body.write_compact_len(0) // forgotten_topics_data: empty
  body.write_compact_string("") // rack_id
  body.write_tag_buffer()
  body.to_bytes()
}

///|
pub struct FetchPartitionResult {
  partition : Int
  error_code : Int
  high_watermark : Int64
  records : Array[Record]
}

///|
/// Decode a Fetch v12 response (after the v1 response header).
pub fn decode_fetch_response(d : Decoder) -> Array[FetchPartitionResult] raise {
  d.skip(4) // throttle_time_ms
  let error_code = d.read_i16()
  if error_code != 0 {
    raise ProtocolError::ProtocolError(
      "Fetch failed: \{error_name(error_code)}",
    )
  }
  let _session_id = d.read_i32()
  let results : Array[FetchPartitionResult] = []
  let topic_count = d.read_compact_len()
  for _ in 0..<topic_count {
    let _topic = d.read_compact_string()
    let partition_count = d.read_compact_len()
    for _ in 0..<partition_count {
      let partition = d.read_i32()
      let partition_error = d.read_i16()
      let high_watermark = d.read_i64()
      let _last_stable_offset = d.read_i64()
      let _log_start_offset = d.read_i64()
      let aborted_count = d.read_compact_len() // nullable array; -1 = null
      if aborted_count > 0 {
        for _ in 0..<aborted_count {
          d.skip(8 + 8) // producer_id, first_offset
          d.skip_tag_buffer()
        }
      }
      d.skip(4) // preferred_read_replica
      let records_len = d.read_compact_len() // nullable records; -1 = null
      let records = if records_len > 0 {
        decode_record_batches(d.read_bytes(records_len))
      } else {
        []
      }
      d.skip_tag_buffer()
      results.push({
        partition,
        error_code: partition_error,
        high_watermark,
        records,
      })
    }
    d.skip_tag_buffer()
  }
  d.skip_tag_buffer()
  results
}

///|
/// Names for the error codes a simple consumer can plausibly hit.
pub fn error_name(code : Int) -> String {
  match code {
    0 => "NONE"
    1 => "OFFSET_OUT_OF_RANGE"
    3 => "UNKNOWN_TOPIC_OR_PARTITION"
    5 => "LEADER_NOT_AVAILABLE"
    6 => "NOT_LEADER_OR_FOLLOWER"
    29 => "TOPIC_AUTHORIZATION_FAILED"
    35 => "UNSUPPORTED_VERSION"
    _ => "error code \{code}"
  }
}

///|
const OFFSET_EARLIEST : Int64 = -2L

///|
const OFFSET_LATEST : Int64 = -1L
