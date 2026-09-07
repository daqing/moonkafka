// InitProducerId v5 — the idempotence handshake (Phase 3) and, with a
// transactional id, the transaction-manager entry point (KIP-98; epoch
// bumping after UNKNOWN_PRODUCER_ID reuses it with pid/epoch set, the
// v3 fields). Pinned from InitProducerIdRequest/Response.json in the
// Kafka 4.3 tree: v5 adds the TRANSACTION_ABORTABLE error (KIP-890);
// v6 (2PC, KIP-939) is unstable and out of scope.

///|
pub struct InitProducerIdResult {
  producer_id : Int64
  producer_epoch : Int
  error_code : Int
} derive(@debug.Debug)

///|
/// Encode an InitProducerId v5 request body. `transactional_id` None
/// means a plain idempotent producer; a set pid/epoch asks the broker to
/// bump the epoch for the existing identity.
pub fn encode_init_producer_id_request(
  transactional_id : String?,
  transaction_timeout_ms : Int,
  producer_id : Int64,
  producer_epoch : Int,
) -> Bytes {
  let body = @buf.Encoder::new()
  body.write_compact_nullable_string(transactional_id)
  body.write_i32(transaction_timeout_ms)
  body.write_i64(producer_id)
  body.write_i16(producer_epoch)
  body.write_tag_buffer()
  body.to_bytes()
}

///|
/// Decode an InitProducerId v5 response body: the assigned (or bumped)
/// producer identity plus the throttle hint.
pub fn decode_init_producer_id_response(
  d : @buf.Decoder,
) -> (InitProducerIdResult, Int) raise {
  let throttle = d.read_i32()
  let error_code = d.read_i16()
  let producer_id = d.read_i64()
  let producer_epoch = d.read_i16()
  d.skip_tag_buffer()
  ({ producer_id, producer_epoch, error_code, }, throttle)
}

///|
pub async fn BrokerConnection::init_producer_id(
  self : BrokerConnection,
  transactional_id? : String? = None,
  transaction_timeout_ms? : Int = 60000,
  producer_id? : Int64 = -1L,
  producer_epoch? : Int = -1,
  timeout_ms? : Int = 30000,
) -> InitProducerIdResult {
  let version = self.api_version(API_INIT_PRODUCER_ID)
  let d = self.request(
    API_INIT_PRODUCER_ID,
    version,
    encode_init_producer_id_request(
      transactional_id, transaction_timeout_ms, producer_id, producer_epoch,
    ),
    timeout_ms~,
  )
  let (result, throttle) = decode_init_producer_id_response(d)
  self.note_throttle(throttle)
  if result.error_code != 0 {
    raise ProtocolError::ProtocolError(
      "InitProducerId failed: \{error_name(result.error_code)}",
    )
  }
  result
}
