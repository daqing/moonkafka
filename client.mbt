// TCP connection to a single Kafka broker, with request/response framing.

///|
pub struct Connection {
  tcp : @socket.Tcp
  client_id : String
  mut correlation_id : Int
}

///|
pub async fn Connection::connect(
  host : String,
  port : Int,
  client_id? : String = "moonkafka",
) -> Connection {
  let tcp = @socket.Tcp::connect_to_host(host, port~)
  { tcp, client_id, correlation_id: 0, }
}

///|
pub fn Connection::close(self : Connection) -> Unit {
  self.tcp.close()
}

///|
/// Send a request (flexible version, header v2) and return a Decoder
/// positioned at the response body. ApiVersions is special: its response
/// header is v0 (no tag buffer) even for flexible versions.
pub async fn Connection::request(
  self : Connection,
  api_key : Int,
  api_version : Int,
  body : Bytes,
) -> Decoder {
  self.correlation_id += 1
  let frame = encode_request(
    api_key,
    api_version,
    self.correlation_id,
    self.client_id,
    body,
  )
  self.tcp.write(frame)
  let size_bytes = self.tcp.read_exactly(4)
  let size = Decoder::new(size_bytes).read_i32() catch {
    _ => raise DecodeError::Malformed("bad frame size")
  }
  let data = self.tcp.read_exactly(size)
  let d = Decoder::new(data)
  let correlation_id = d.read_i32()
  if correlation_id != self.correlation_id {
    raise ProtocolError::ProtocolError(
      "correlation id mismatch: sent \{self.correlation_id}, got \{correlation_id}",
    )
  }
  if api_key != API_API_VERSIONS {
    d.skip_tag_buffer() // response header v1
  }
  d
}

///|
pub async fn Connection::check_api_versions(self : Connection) -> Unit {
  let d = self.request(
    API_API_VERSIONS,
    API_VERSIONS_VERSION,
    encode_api_versions_request(),
  )
  decode_api_versions_response(d)
}

///|
pub async fn Connection::fetch_metadata(
  self : Connection,
  topic : String,
) -> Metadata {
  let d = self.request(
    API_METADATA,
    METADATA_VERSION,
    encode_metadata_request(topic),
  )
  decode_metadata_response(d)
}

///|
pub async fn Connection::list_offsets(
  self : Connection,
  topic : String,
  partitions : Array[PartitionInfo],
  timestamp : Int64,
) -> Map[Int, Int64] {
  let d = self.request(
    API_LIST_OFFSETS,
    LIST_OFFSETS_VERSION,
    encode_list_offsets_request(topic, partitions, timestamp),
  )
  decode_list_offsets_response(d)
}

///|
pub async fn Connection::produce(
  self : Connection,
  topic : String,
  partitions : Array[(Int, Bytes)],
  acks~ : Int,
  timeout_ms~ : Int,
) -> Array[ProducePartitionResult] {
  let d = self.request(
    API_PRODUCE,
    PRODUCE_VERSION,
    encode_produce_request(topic, partitions, acks, timeout_ms),
  )
  decode_produce_response(d)
}

///|
pub async fn Connection::fetch(
  self : Connection,
  topic : String,
  partitions : Array[(PartitionInfo, Int64)],
  max_wait_ms~ : Int,
  max_bytes~ : Int,
) -> Array[FetchPartitionResult] {
  let d = self.request(
    API_FETCH,
    FETCH_VERSION,
    encode_fetch_request(topic, partitions, max_wait_ms, max_bytes),
  )
  decode_fetch_response(d)
}
