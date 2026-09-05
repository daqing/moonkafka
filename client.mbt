// High-level RPC helpers on BrokerConnection: one method per API the
// simple producer and consumer use. Timeouts come from the caller's
// request_timeout_ms; the fetch helper long-polls up to max_wait_ms, which
// the default request timeout comfortably exceeds.

///|
pub async fn BrokerConnection::check_api_versions(
  self : BrokerConnection,
  timeout_ms? : Int = 30000,
) -> Unit {
  let d = self.request(
    API_API_VERSIONS,
    API_VERSIONS_VERSION,
    encode_api_versions_request(),
    timeout_ms~,
  )
  decode_api_versions_response(d)
}

///|
pub async fn BrokerConnection::fetch_metadata(
  self : BrokerConnection,
  topic : String,
  timeout_ms? : Int = 30000,
) -> Metadata {
  let d = self.request(
    API_METADATA,
    METADATA_VERSION,
    encode_metadata_request(topic),
    timeout_ms~,
  )
  decode_metadata_response(d)
}

///|
pub async fn BrokerConnection::list_offsets(
  self : BrokerConnection,
  topic : String,
  partitions : Array[PartitionInfo],
  timestamp : Int64,
  timeout_ms? : Int = 30000,
) -> Map[Int, Int64] {
  let d = self.request(
    API_LIST_OFFSETS,
    LIST_OFFSETS_VERSION,
    encode_list_offsets_request(topic, partitions, timestamp),
    timeout_ms~,
  )
  decode_list_offsets_response(d)
}

///|
pub async fn BrokerConnection::produce(
  self : BrokerConnection,
  topic : String,
  partitions : Array[(Int, Bytes)],
  acks~ : Int,
  timeout_ms~ : Int,
) -> Array[ProducePartitionResult] {
  let d = self.request(
    API_PRODUCE,
    PRODUCE_VERSION,
    encode_produce_request(topic, partitions, acks, timeout_ms),
    timeout_ms~,
  )
  decode_produce_response(d)
}

///|
pub async fn BrokerConnection::fetch(
  self : BrokerConnection,
  topic : String,
  partitions : Array[(PartitionInfo, Int64)],
  max_wait_ms~ : Int,
  max_bytes~ : Int,
  timeout_ms? : Int = 30000,
) -> Array[FetchPartitionResult] {
  let d = self.request(
    API_FETCH,
    FETCH_VERSION,
    encode_fetch_request(topic, partitions, max_wait_ms, max_bytes),
    timeout_ms~,
  )
  decode_fetch_response(d)
}
