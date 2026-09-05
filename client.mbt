// High-level RPC helpers on BrokerConnection: one method per API the
// simple producer and consumer use. Every request negotiates its version
// against the ranges stored by negotiate_api_versions. Timeouts come from
// the caller's request_timeout_ms; the fetch helper long-polls up to
// max_wait_ms, which the default request timeout comfortably exceeds.

///|
/// Run the ApiVersions handshake and store the broker's advertised ranges;
/// later requests pick their version from them (api_version).
pub async fn BrokerConnection::negotiate_api_versions(
  self : BrokerConnection,
  timeout_ms? : Int = 30000,
) -> Unit {
  let d = self.request(
    API_API_VERSIONS,
    API_VERSIONS_VERSION,
    encode_api_versions_request(),
    timeout_ms~,
  )
  let (versions, throttle) = decode_api_versions_response(d)
  self.versions = Some(versions)
  self.note_throttle(throttle)
}

///|
pub async fn BrokerConnection::fetch_metadata(
  self : BrokerConnection,
  topic : String,
  timeout_ms? : Int = 30000,
) -> Metadata {
  let version = self.api_version(API_METADATA)
  let d = self.request(
    API_METADATA,
    version,
    encode_metadata_request(Some([topic])),
    timeout_ms~,
  )
  let (metadata, throttle) = decode_metadata_response(version, d)
  self.note_throttle(throttle)
  metadata
}

///|
/// Describe topics via DescribeTopicPartitions v0, one page per call:
/// returns the page plus the cursor to resume with when the broker
/// truncated the response at `response_partition_limit`. An empty `topics`
/// list describes all topics.
pub async fn BrokerConnection::describe_topic_partitions(
  self : BrokerConnection,
  topics : Array[String],
  response_partition_limit? : Int = 2000,
  cursor? : TopicPartitionCursor? = None,
  timeout_ms? : Int = 30000,
) -> DescribeTopicPartitions {
  let version = self.api_version(API_DESCRIBE_TOPIC_PARTITIONS)
  let d = self.request(
    API_DESCRIBE_TOPIC_PARTITIONS,
    version,
    encode_describe_topic_partitions_request(
      topics,
      response_partition_limit,
      cursor~,
    ),
    timeout_ms~,
  )
  let (page, throttle) = decode_describe_topic_partitions_response(d)
  self.note_throttle(throttle)
  page
}

///|
pub async fn BrokerConnection::list_offsets(
  self : BrokerConnection,
  topic : String,
  partitions : Array[PartitionInfo],
  timestamp : Int64,
  timeout_ms? : Int = 30000,
) -> Map[Int, Int64] {
  let version = self.api_version(API_LIST_OFFSETS)
  let d = self.request(
    API_LIST_OFFSETS,
    version,
    encode_list_offsets_request(topic, partitions, timestamp),
    timeout_ms~,
  )
  let (offsets, throttle) = decode_list_offsets_response(d)
  self.note_throttle(throttle)
  offsets
}

///|
/// Produce one batch per partition to a single topic. `topic_id` comes
/// from metadata and is mandatory once the negotiated version is 13. The
/// acks validation (1 or -1) lives in ProducerConfig.
pub async fn BrokerConnection::produce(
  self : BrokerConnection,
  topic : String,
  topic_id? : Uuid = Uuid::zero(),
  partitions~ : Array[(Int, Bytes)],
  acks~ : Int,
  timeout_ms~ : Int,
) -> Array[ProducePartitionResult] {
  let version = self.api_version(API_PRODUCE)
  let d = self.request(
    API_PRODUCE,
    version,
    encode_produce_request(
      version,
      topic,
      topic_id,
      partitions,
      acks~,
      timeout_ms~,
    ),
    timeout_ms~,
  )
  let (results, throttle) = decode_produce_response(version, d)
  self.note_throttle(throttle)
  results
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
  let version = self.api_version(API_FETCH)
  let d = self.request(
    API_FETCH,
    version,
    encode_fetch_request(topic, partitions, max_wait_ms, max_bytes),
    timeout_ms~,
  )
  let (results, throttle) = decode_fetch_response(d)
  self.note_throttle(throttle)
  results
}
