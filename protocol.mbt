// Kafka RPC layer: request framing and the data-plane APIs used by the
// simple producer and consumer. Targets Kafka 4.x brokers (flexible
// message versions only). API keys and the implemented-version matrix
// live in versions.mbt; the Metadata and DescribeTopicPartitions codecs
// live in metadata.mbt.

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
  flexible? : Bool = true,
) -> Bytes {
  let header = @buf.Encoder::new()
  header.write_i16(api_key)
  header.write_i16(api_version)
  header.write_i32(correlation_id)
  header.write_nullable_string(Some(client_id))
  if flexible {
    header.write_tag_buffer()
  }
  let frame = @buf.Encoder::new()
  frame.write_i32(header.buf.length() + body.length())
  frame.write_bytes(header.to_bytes())
  frame.write_bytes(body)
  frame.to_bytes()
}

///|
pub fn encode_api_versions_request() -> Bytes {
  let body = @buf.Encoder::new()
  body.write_compact_string("moonkafka")
  body.write_compact_string("0.1.0")
  body.write_tag_buffer()
  body.to_bytes()
}

///|
/// Decode an ApiVersions v3 response; response header is v0 (no tag buffer).
/// Returns the broker's advertised version ranges plus the throttle hint
/// in milliseconds. Version validation is not done here: requests
/// negotiate against the ranges via BrokerVersions::pick.
pub fn decode_api_versions_response(
  d : @buf.Decoder,
) -> (BrokerVersions, Int) raise {
  let error_code = d.read_i16()
  if error_code != 0 {
    raise BrokerError(error_code, "ApiVersions failed")
  }
  let count = d.read_compact_len()
  let ranges : Map[Int, (Int, Int)] = Map([])
  for _ in 0..<count {
    let key = d.read_i16()
    let min_version = d.read_i16()
    let max_version = d.read_i16()
    d.skip_tag_buffer()
    ranges[key] = (min_version, max_version)
  }
  // throttle_time_ms, top-level tag buffer
  let throttle = d.read_i32()
  d.skip_tag_buffer()
  ({ ranges, }, throttle)
}

///|
pub fn encode_list_offsets_request(
  topic : String,
  partitions : Array[PartitionInfo],
  timestamp : Int64,
) -> Bytes {
  let body = @buf.Encoder::new()
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
/// Decode a ListOffsets v7 response; returns partition index -> offset
/// plus the throttle hint in milliseconds.
pub fn decode_list_offsets_response(
  d : @buf.Decoder,
) -> (Map[Int, Int64], Int) raise {
  let throttle = d.read_i32() // throttle_time_ms
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
        raise BrokerError(
          error_code,
          "ListOffsets failed for partition \{index}",
        )
      }
      offsets[index] = offset
    }
    d.skip_tag_buffer()
  }
  d.skip_tag_buffer()
  (offsets, throttle)
}

///|
/// Encode a Produce v11 request: one topic, one record batch per partition.
pub fn encode_produce_request(
  topic : String,
  partitions : Array[(Int, Bytes)],
  acks : Int,
  timeout_ms : Int,
) -> Bytes {
  let body = @buf.Encoder::new()
  body.write_compact_nullable_string(None) // transactional_id
  body.write_i16(acks)
  body.write_i32(timeout_ms)
  body.write_compact_len(1) // one topic
  body.write_compact_string(topic)
  body.write_compact_len(partitions.length())
  for entry in partitions {
    let (index, batch) = entry
    body.write_i32(index)
    body.write_compact_len(batch.length()) // records: COMPACT_BYTES
    body.write_bytes(batch)
    body.write_tag_buffer()
  }
  body.write_tag_buffer()
  body.write_tag_buffer()
  body.to_bytes()
}

///|
pub struct ProducePartitionResult {
  partition : Int
  error_code : Int
  base_offset : Int64
}

///|
/// Decode a Produce v11 response (after the v1 response header).
pub fn decode_produce_response(
  d : @buf.Decoder,
) -> (Array[ProducePartitionResult], Int) raise {
  let results : Array[ProducePartitionResult] = []
  let topic_count = d.read_compact_len()
  for _ in 0..<topic_count {
    let _name = d.read_compact_string()
    let partition_count = d.read_compact_len()
    for _ in 0..<partition_count {
      let partition = d.read_i32()
      let error_code = d.read_i16()
      let base_offset = d.read_i64()
      let _log_append_time = d.read_i64()
      let _log_start_offset = d.read_i64()
      let record_error_count = d.read_compact_len()
      for _ in 0..<record_error_count {
        let _batch_index = d.read_i32()
        let _message = d.read_compact_nullable_string()
        d.skip_tag_buffer()
      }
      let _error_message = d.read_compact_nullable_string()
      d.skip_tag_buffer()
      results.push({ partition, error_code, base_offset, })
    }
    d.skip_tag_buffer()
  }
  let throttle = d.read_i32() // throttle_time_ms
  d.skip_tag_buffer()
  (results, throttle)
}

///|
pub fn encode_fetch_request(
  topic : String,
  partitions : Array[(PartitionInfo, Int64)],
  max_wait_ms : Int,
  max_bytes : Int,
) -> Bytes {
  let body = @buf.Encoder::new()
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
pub fn decode_fetch_response(
  d : @buf.Decoder,
) -> (Array[FetchPartitionResult], Int) raise {
  let throttle = d.read_i32() // throttle_time_ms
  let error_code = d.read_i16()
  if error_code != 0 {
    raise BrokerError(error_code, "Fetch failed")
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
  (results, throttle)
}

///|
const OFFSET_EARLIEST : Int64 = -2L

///|
const OFFSET_LATEST : Int64 = -1L
