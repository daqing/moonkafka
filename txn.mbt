// The transaction APIs (Phase 3). All four codecs are pinned from the
// message/*.json schemas in the Kafka 4.3 tree:
//
// - AddPartitionsToTxn v3 (key 24, flexible 3+): the client shape — the
//   v4+ Transactions array is the broker-batch API (KIP-890) and is not
//   used by producers.
// - AddOffsetsToTxn v4 (key 25, flexible 3+): v4 adds the
//   TRANSACTION_ABORTABLE error.
// - EndTxn v5 (key 26, flexible 3+): v5 is the KIP-890 part 2 shape —
//   the coordinator bumps the epoch on every transaction and returns the
//   new identity, which the producer adopts for its next batches.
// - TxnOffsetCommit v4 (key 28, flexible 3+): v5 is wire-identical, and
//   the schema itself caps transactional (non-2PC) clients at v4, so the
//   matrix pins 4.
//
// Broker error codes travel as plain values here: the transaction
// manager classifies them (retry at the coordinator, fatal fencing,
// otherwise raise) instead of the wrappers raising on every non-zero.

///|
/// One partition's outcome from AddPartitionsToTxn / TxnOffsetCommit.
pub struct TxnPartitionResult {
  topic : String
  partition : Int
  error_code : Int
} derive(@debug.Debug)

///|
/// EndTxn v5 outcome: the broker error code plus the (possibly bumped)
/// producer identity the coordinator returns (v5, KIP-890 part 2).
pub struct EndTxnResult {
  error_code : Int
  producer_id : Int64
  producer_epoch : Int
} derive(@debug.Debug)

///|
/// One offset destined for TxnOffsetCommit within a transaction.
pub(all) struct TxnOffset {
  partition : Int
  offset : Int64
} derive(@debug.Debug)

///|
/// Encode an AddPartitionsToTxn v3 request body (client shape): the
/// transactional id, producer identity, and the topics with the
/// partitions being added to the ongoing transaction.
pub fn encode_add_partitions_to_txn_request(
  transactional_id : String,
  producer_id : Int64,
  producer_epoch : Int,
  topics : Array[(String, Array[Int])],
) -> Bytes {
  let body = @buf.Encoder::new()
  body.write_compact_string(transactional_id)
  body.write_i64(producer_id)
  body.write_i16(producer_epoch)
  body.write_compact_len(topics.length())
  for entry in topics {
    let (name, partitions) = entry
    body.write_compact_string(name)
    body.write_compact_len(partitions.length())
    for p in partitions {
      body.write_i32(p)
    }
    body.write_tag_buffer()
  }
  body.write_tag_buffer()
  body.to_bytes()
}

///|
/// Decode an AddPartitionsToTxn v3 response body: per-partition error
/// codes under the v3-and-below topic results (v4+ restructures the
/// response for the broker-batch API), plus the throttle hint.
pub fn decode_add_partitions_to_txn_response(
  d : @buf.Decoder,
) -> (Array[TxnPartitionResult], Int) raise {
  let throttle = d.read_i32()
  let n = d.read_compact_len()
  let out : Array[TxnPartitionResult] = []
  for _ in 0..<n {
    let topic = d.read_compact_string()
    let parts = d.read_compact_len()
    for _ in 0..<parts {
      let partition = d.read_i32()
      let error_code = d.read_i16()
      d.skip_tag_buffer()
      out.push({ topic, partition, error_code, })
    }
    d.skip_tag_buffer()
  }
  d.skip_tag_buffer()
  (out, throttle)
}

///|
pub async fn BrokerConnection::add_partitions_to_txn(
  self : BrokerConnection,
  transactional_id : String,
  producer_id : Int64,
  producer_epoch : Int,
  topics : Array[(String, Array[Int])],
  timeout_ms? : Int = 30000,
) -> Array[TxnPartitionResult] {
  let version = self.api_version(API_ADD_PARTITIONS_TO_TXN)
  let d = self.request(
    API_ADD_PARTITIONS_TO_TXN,
    version,
    encode_add_partitions_to_txn_request(
      transactional_id, producer_id, producer_epoch, topics,
    ),
    timeout_ms~,
  )
  let (results, throttle) = decode_add_partitions_to_txn_response(d)
  self.note_throttle(throttle)
  results
}

///|
/// Encode an AddOffsetsToTxn v4 request body: registers the consumer
/// group whose offsets are about to be committed inside the transaction.
pub fn encode_add_offsets_to_txn_request(
  transactional_id : String,
  producer_id : Int64,
  producer_epoch : Int,
  group_id : String,
) -> Bytes {
  let body = @buf.Encoder::new()
  body.write_compact_string(transactional_id)
  body.write_i64(producer_id)
  body.write_i16(producer_epoch)
  body.write_compact_string(group_id)
  body.write_tag_buffer()
  body.to_bytes()
}

///|
/// Decode an AddOffsetsToTxn v4 response body: the top-level error code
/// plus the throttle hint.
pub fn decode_add_offsets_to_txn_response(d : @buf.Decoder) -> (Int, Int) raise {
  let throttle = d.read_i32()
  let error_code = d.read_i16()
  d.skip_tag_buffer()
  (error_code, throttle)
}

///|
pub async fn BrokerConnection::add_offsets_to_txn(
  self : BrokerConnection,
  transactional_id : String,
  producer_id : Int64,
  producer_epoch : Int,
  group_id : String,
  timeout_ms? : Int = 30000,
) -> Int {
  let version = self.api_version(API_ADD_OFFSETS_TO_TXN)
  let d = self.request(
    API_ADD_OFFSETS_TO_TXN,
    version,
    encode_add_offsets_to_txn_request(
      transactional_id, producer_id, producer_epoch, group_id,
    ),
    timeout_ms~,
  )
  let (error_code, throttle) = decode_add_offsets_to_txn_response(d)
  self.note_throttle(throttle)
  error_code
}

///|
/// Encode a TxnOffsetCommit v4 request body: the offsets to commit for
/// `group_id` as part of the transaction. v3+ carries the consumer
/// member identity; a standalone (non-joiner) commit sends the defaults
/// generation -1 and empty member id, like the Java client.
pub fn encode_txn_offset_commit_request(
  transactional_id : String,
  group_id : String,
  producer_id : Int64,
  producer_epoch : Int,
  generation_id : Int,
  member_id : String,
  topics : Array[(String, Array[TxnOffset])],
) -> Bytes {
  let body = @buf.Encoder::new()
  body.write_compact_string(transactional_id)
  body.write_compact_string(group_id)
  body.write_i64(producer_id)
  body.write_i16(producer_epoch)
  body.write_i32(generation_id)
  body.write_compact_string(member_id)
  body.write_compact_nullable_string(None) // group instance id
  body.write_compact_len(topics.length())
  for entry in topics {
    let (name, offsets) = entry
    body.write_compact_string(name)
    body.write_compact_len(offsets.length())
    for offset in offsets {
      body.write_i32(offset.partition)
      body.write_i64(offset.offset)
      body.write_i32(-1) // committed leader epoch: unknown
      body.write_compact_nullable_string(None) // committed metadata
      body.write_tag_buffer()
    }
    body.write_tag_buffer()
  }
  body.write_tag_buffer()
  body.to_bytes()
}

///|
/// Decode a TxnOffsetCommit v4 response body: per-partition error codes
/// plus the throttle hint.
pub fn decode_txn_offset_commit_response(
  d : @buf.Decoder,
) -> (Array[TxnPartitionResult], Int) raise {
  let throttle = d.read_i32()
  let n = d.read_compact_len()
  let out : Array[TxnPartitionResult] = []
  for _ in 0..<n {
    let topic = d.read_compact_string()
    let parts = d.read_compact_len()
    for _ in 0..<parts {
      let partition = d.read_i32()
      let error_code = d.read_i16()
      d.skip_tag_buffer()
      out.push({ topic, partition, error_code, })
    }
    d.skip_tag_buffer()
  }
  d.skip_tag_buffer()
  (out, throttle)
}

///|
pub async fn BrokerConnection::txn_offset_commit(
  self : BrokerConnection,
  transactional_id : String,
  group_id : String,
  producer_id : Int64,
  producer_epoch : Int,
  topics : Array[(String, Array[TxnOffset])],
  timeout_ms? : Int = 30000,
) -> Array[TxnPartitionResult] {
  let version = self.api_version(API_TXN_OFFSET_COMMIT)
  let d = self.request(
    API_TXN_OFFSET_COMMIT,
    version,
    encode_txn_offset_commit_request(
      transactional_id, group_id, producer_id, producer_epoch, -1, "", topics,
    ),
    timeout_ms~,
  )
  let (results, throttle) = decode_txn_offset_commit_response(d)
  self.note_throttle(throttle)
  results
}

///|
/// Encode an EndTxn v5 request body: commit or abort the ongoing
/// transaction.
pub fn encode_end_txn_request(
  transactional_id : String,
  producer_id : Int64,
  producer_epoch : Int,
  committed : Bool,
) -> Bytes {
  let body = @buf.Encoder::new()
  body.write_compact_string(transactional_id)
  body.write_i64(producer_id)
  body.write_i16(producer_epoch)
  body.write_bool(committed)
  body.write_tag_buffer()
  body.to_bytes()
}

///|
/// Decode an EndTxn v5 response body: the error code and the identity
/// the coordinator returns — v5 bumps the epoch on every transaction
/// (KIP-890 part 2) and echoes the new (pid, epoch).
pub fn decode_end_txn_response(d : @buf.Decoder) -> (EndTxnResult, Int) raise {
  let throttle = d.read_i32()
  let error_code = d.read_i16()
  let producer_id = d.read_i64()
  let producer_epoch = d.read_i16()
  d.skip_tag_buffer()
  ({ error_code, producer_id, producer_epoch, }, throttle)
}

///|
pub async fn BrokerConnection::end_txn(
  self : BrokerConnection,
  transactional_id : String,
  producer_id : Int64,
  producer_epoch : Int,
  committed : Bool,
  timeout_ms? : Int = 30000,
) -> EndTxnResult {
  let version = self.api_version(API_END_TXN)
  let d = self.request(
    API_END_TXN,
    version,
    encode_end_txn_request(
      transactional_id, producer_id, producer_epoch, committed,
    ),
    timeout_ms~,
  )
  let (result, throttle) = decode_end_txn_response(d)
  self.note_throttle(throttle)
  result
}

///|
/// InitProducerId at the transaction coordinator without raising on
/// broker error codes: the connect flow classifies and retries them.
async fn BrokerConnection::init_producer_id_txn(
  self : BrokerConnection,
  transactional_id : String,
  transaction_timeout_ms : Int,
  timeout_ms : Int,
) -> InitProducerIdResult {
  let version = self.api_version(API_INIT_PRODUCER_ID)
  let d = self.request(
    API_INIT_PRODUCER_ID,
    version,
    encode_init_producer_id_request(
      Some(transactional_id),
      transaction_timeout_ms,
      -1L,
      -1,
    ),
    timeout_ms~,
  )
  let (result, throttle) = decode_init_producer_id_response(d)
  self.note_throttle(throttle)
  result
}

///|
/// Coordinator-call error codes worth retrying: the coordinator is
/// loading (14), unavailable (15), moved (16), or a concurrent
/// transaction holds the lock (51). Note the protocol's own
/// "retriable" flag does not apply here — fencing (47/90) is marked
/// retriable for Produce but is fatal for the transaction manager.
fn txn_retriable_code(code : Int) -> Bool {
  code == 14 || code == 15 || code == 16 || code == 51
}

///|
/// Fatal transaction-manager codes: the producer was fenced by a newer
/// instance or epoch (the caller must not retry into a live fence).
fn txn_fenced_code(code : Int) -> Bool {
  code == 47 || code == 90
}
