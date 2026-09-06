// Kafka RPC layer: request framing and the data-plane APIs used by the
// simple producer and consumer. Targets Kafka 4.x brokers (flexible
// message versions only). API keys and the implemented-version matrix
// live in versions.mbt; the Metadata and DescribeTopicPartitions codecs
// live in metadata.mbt.

///|
pub(all) suberror ProtocolError {
  ProtocolError(String)
  /// The producer's buffer_memory is exhausted and the record could not
  /// be queued (Java's BufferExhaustedException).
  BufferExhausted(String)
}

///|
/// Encode a full request frame: INT32 size prefix + header + body.
/// Flexible versions use header v2 (client_id as a COMPACT_NULLABLE_STRING
/// followed by an empty tag buffer, per RequestHeader.json); the rest use
/// header v1 with a legacy NULLABLE_STRING (SaslHandshake v1 today).
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
  if flexible {
    header.write_compact_nullable_string(Some(client_id))
    header.write_tag_buffer()
  } else {
    header.write_nullable_string(Some(client_id))
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
