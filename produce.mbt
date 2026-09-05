// Produce v12/v13 — the produce data path for the simple producer.
// Field order is pinned from the Kafka 4.3 message schemas
// ProduceRequest.json / ProduceResponse.json. v13 (KIP-516) replaces the
// topic name with the topic id in both directions; note KIP-890: when a
// transactional producer without txn v2 is added in Phase 3, it must cap
// at v11 inside transactions — the negotiated version is not usable there
// unconditionally.

///|
pub(all) struct ProduceRecordError {
  batch_index : Int
  message : String?
} derive(@debug.Debug)

///|
pub struct ProducePartitionResult {
  partition : Int
  error_code : Int
  base_offset : Int64
  log_append_time : Int64
  log_start_offset : Int64
  record_errors : Array[ProduceRecordError]
  error_message : String?
}

///|
/// Encode a Produce v12/v13 request body: one topic, one record batch per
/// partition. v12 addresses the topic by name; v13 by id (a zero topic id
/// would mean "unknown" to the broker, so it raises). `transactional_id`
/// is None for the non-transactional producer.
pub fn encode_produce_request(
  version : Int,
  topic : String,
  topic_id : Uuid,
  partitions : Array[(Int, Bytes)],
  transactional_id? : String? = None,
  acks~ : Int,
  timeout_ms~ : Int,
) -> Bytes raise {
  if version != 12 && version != 13 {
    raise ProtocolError::ProtocolError(
      "moonkafka implements Produce v12-v13, got v\{version}",
    )
  }
  if version == 13 && topic_id == Uuid::zero() {
    raise ProtocolError::ProtocolError(
      "Produce v13 needs the topic id; refresh metadata first",
    )
  }
  let body = @buf.Encoder::new()
  body.write_compact_nullable_string(transactional_id)
  body.write_i16(acks)
  body.write_i32(timeout_ms)
  body.write_compact_len(1) // one topic
  match version {
    13 => body.write_bytes(topic_id.to_bytes())
    _ => body.write_compact_string(topic)
  }
  body.write_compact_len(partitions.length())
  for entry in partitions {
    let (index, batch) = entry
    body.write_i32(index)
    body.write_compact_len(batch.length()) // records: COMPACT_RECORDS
    body.write_bytes(batch)
    body.write_tag_buffer()
  }
  body.write_tag_buffer()
  body.write_tag_buffer()
  body.to_bytes()
}

///|
/// Decode a Produce v12 or v13 response body. v13 identifies topics by
/// id; everything else matches, including per-record errors (KIP-467).
/// Returns the per-partition results plus the throttle hint.
pub fn decode_produce_response(
  version : Int,
  d : @buf.Decoder,
) -> (Array[ProducePartitionResult], Int) raise {
  if version != 12 && version != 13 {
    raise ProtocolError::ProtocolError(
      "moonkafka implements Produce v12-v13, got v\{version}",
    )
  }
  let results : Array[ProducePartitionResult] = []
  let topic_count = d.read_compact_len()
  for _ in 0..<topic_count {
    if version == 13 {
      d.skip(16) // topic_id
    } else {
      let _name = d.read_compact_string()
    }
    let partition_count = d.read_compact_len()
    for _ in 0..<partition_count {
      let partition = d.read_i32()
      let error_code = d.read_i16()
      let base_offset = d.read_i64()
      let log_append_time = d.read_i64()
      let log_start_offset = d.read_i64()
      let record_errors : Array[ProduceRecordError] = []
      let record_error_count = d.read_compact_len()
      for _ in 0..<record_error_count {
        let batch_index = d.read_i32()
        let message = d.read_compact_nullable_string()
        d.skip_tag_buffer()
        record_errors.push({ batch_index, message, })
      }
      let error_message = d.read_compact_nullable_string()
      d.skip_tag_buffer()
      results.push({
        partition,
        error_code,
        base_offset,
        log_append_time,
        log_start_offset,
        record_errors,
        error_message,
      })
    }
    d.skip_tag_buffer()
  }
  let throttle = d.read_i32() // throttle_time_ms
  d.skip_tag_buffer()
  (results, throttle)
}
