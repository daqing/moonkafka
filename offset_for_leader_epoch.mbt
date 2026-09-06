// OffsetForLeaderEpoch v4 — leader-epoch end offsets, the truncation
// detection the fetcher (Phase 4) runs after UNKNOWN_LEADER_EPOCH /
// FENCED_LEADER_EPOCH fetch errors. Pinned from
// OffsetForLeaderEpochRequest.json / OffsetForLeaderEpochResponse.json in
// the Kafka 4.3 tree: v4 is the first flexible version; consumers send
// ReplicaId -1 and address topics by name at every in-range version.

///|
/// One partition's epoch end offset.
pub struct EpochEndOffset {
  partition : Int
  error_code : Int
  leader_epoch : Int
  end_offset : Int64
} derive(@debug.Debug)

///|
/// Encode an OffsetForLeaderEpoch v4 request body: one topic, one entry
/// per partition, asking for the last offset of `leader_epoch` (the
/// consumer sends its current leader epoch for fencing).
pub fn encode_offset_for_leader_epoch_request(
  topic : String,
  partitions : Array[(Int, Int, Int)],
  timeout_ms? : Int = 0,
) -> Bytes {
  let body = @buf.Encoder::new()
  body.write_i32(-1) // replica_id: consumer
  body.write_compact_len(1) // one topic
  body.write_compact_string(topic)
  body.write_compact_len(partitions.length())
  for entry in partitions {
    let (partition, current_leader_epoch, leader_epoch) = entry
    body.write_i32(partition)
    body.write_i32(current_leader_epoch)
    body.write_i32(leader_epoch)
    body.write_tag_buffer()
  }
  body.write_tag_buffer()
  body.write_tag_buffer()
  ignore(timeout_ms)
  body.to_bytes()
}

///|
/// Decode an OffsetForLeaderEpoch v4 response body: per-partition epoch
/// end offsets plus the throttle hint. UNDEFINED_EPOCH end offset (-1)
/// means the broker has no data for the requested epoch.
pub fn decode_offset_for_leader_epoch_response(
  d : @buf.Decoder,
) -> (Array[EpochEndOffset], Int) raise {
  let throttle = d.read_i32()
  let out : Array[EpochEndOffset] = []
  let n = d.read_compact_len()
  for _ in 0..<n {
    ignore(d.read_compact_string()) // topic
    let parts = d.read_compact_len()
    for _ in 0..<parts {
      let error_code = d.read_i16()
      let partition = d.read_i32()
      let leader_epoch = d.read_i32()
      let end_offset = d.read_i64()
      d.skip_tag_buffer()
      out.push({ partition, error_code, leader_epoch, end_offset, })
    }
    d.skip_tag_buffer()
  }
  d.skip_tag_buffer()
  (out, throttle)
}

///|
pub async fn BrokerConnection::offset_for_leader_epoch(
  self : BrokerConnection,
  topic : String,
  partitions : Array[(Int, Int, Int)],
  timeout_ms? : Int = 30000,
) -> Array[EpochEndOffset] {
  let version = self.api_version(API_OFFSET_FOR_LEADER_EPOCH)
  let d = self.request(
    API_OFFSET_FOR_LEADER_EPOCH,
    version,
    encode_offset_for_leader_epoch_request(topic, partitions),
    timeout_ms~,
  )
  let (results, throttle) = decode_offset_for_leader_epoch_response(d)
  self.note_throttle(throttle)
  results
}
