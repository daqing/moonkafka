// ListOffsets v10/v11 — offset resolution by timestamp or sentinel.
// Field order is pinned from the Kafka 4.3 message schemas
// ListOffsetsRequest.json / ListOffsetsResponse.json. v10 appends a
// timeout_ms for tiered-storage reads (KIP-1075); v11 adds the earliest
// pending upload sentinel (KIP-1023) — same wire shape. The response is
// unchanged since v7. A timestamp query (OffsetForTimestamp) is this API
// with a wall-clock timestamp instead of a sentinel.

///|
/// Sentinel timestamps understood by ListOffsets.
pub const OFFSET_EARLIEST : Int64 = -2L

///|
pub const OFFSET_LATEST : Int64 = -1L

///|
/// Offset of the record with the largest timestamp (KIP-734, v7+).
pub const OFFSET_MAX_TIMESTAMP : Int64 = -3L

///|
/// Earliest log start offset still in the local log (KIP-405, v8+).
pub const OFFSET_LOCAL_LOG_START : Int64 = -4L

///|
/// Last tiered offset (KIP-1005, v9+).
pub const OFFSET_LAST_TIERED : Int64 = -5L

///|
/// Earliest pending upload offset (KIP-1023, v11+).
pub const OFFSET_EARLIEST_PENDING_UPLOAD : Int64 = -6L

///|
/// Encode a ListOffsets v10/v11 request body: one topic, one entry per
/// partition. `timestamp` is a sentinel or a wall-clock query.
pub fn encode_list_offsets_request(
  version : Int,
  topic : String,
  partitions : Array[PartitionInfo],
  timestamp : Int64,
  timeout_ms? : Int = 0,
) -> Bytes raise {
  if version < 10 || version > 11 {
    raise ProtocolError::ProtocolError(
      "moonkafka implements ListOffsets v10-v11, got v\{version}",
    )
  }
  let body = @buf.Encoder::new()
  body.write_i32(-1) // replica_id: consumer
  body.write_i8(0) // isolation_level: READ_UNCOMMITTED
  body.write_compact_len(1) // one topic
  body.write_compact_string(topic)
  body.write_compact_len(partitions.length())
  for p in partitions {
    body.write_i32(p.index)
    body.write_i32(p.leader_epoch) // current_leader_epoch
    body.write_i64(timestamp)
    body.write_tag_buffer()
  }
  body.write_tag_buffer()
  body.write_i32(timeout_ms)
  body.write_tag_buffer()
  body.to_bytes()
}

///|
/// Decode a ListOffsets v10/v11 response body (the shape is unchanged
/// since v7). Returns partition index -> offset plus the throttle hint
/// in milliseconds.
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
