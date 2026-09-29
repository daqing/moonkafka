// The embedded consumer-group protocol payloads (Phase 4 classic path):
// ConsumerProtocolSubscription rides JoinGroup metadata and
// ConsumerProtocolAssignment rides SyncGroup assignments. Both are
// non-flexible data messages (flexibleVersions: none) behind a bare
// int16 version prefix: strings are int16-length-prefixed, but array
// counts and byte buffers are int32. The broker parses our join metadata
// with its generated reader, so the widths are not ours to choose — int16
// counts made ClassicGroup.computeSubscribedTopics throw
// "Tried to allocate a collection of size 65544, but there are only 10
// bytes remaining" and the join never completed.

///|
pub const CONSUMER_PROTOCOL_SUBSCRIPTION_VERSION : Int = 0

///|
pub const CONSUMER_PROTOCOL_ASSIGNMENT_VERSION : Int = 0

///|
/// The highest ConsumerProtocol version this client reads. Both messages
/// are `validVersions: 0-3` and Kafka's own reader parses a newer version
/// with the fields it knows, so the version we emit (0) and the versions
/// we accept (0..3) are not the same thing. Everything read here leads the
/// message — Topics for the subscription, AssignedPartitions for the
/// assignment — and has not moved across those versions; the fields that
/// follow (OwnedPartitions, GenerationId, RackId, UserData) are left
/// unread.
const CONSUMER_PROTOCOL_MAX_VERSION : Int = 3

///|
/// Serialize a ConsumerProtocolSubscription: the subscribed topic names
/// (userData stays empty).
pub fn encode_consumer_protocol_subscription(topics : Array[String]) -> Bytes {
  let body = @buf.Encoder::new()
  body.write_i16(CONSUMER_PROTOCOL_SUBSCRIPTION_VERSION)
  body.write_i32(topics.length())
  for topic in topics {
    let bytes = @utf8.encode(topic)
    body.write_i16(bytes.length())
    body.write_bytes(bytes)
  }
  body.write_i32(-1) // userData: null
  body.to_bytes()
}

///|
/// Parse a ConsumerProtocolSubscription: the subscribed topic names.
pub fn decode_consumer_protocol_subscription(
  data : Bytes,
) -> Array[String] raise {
  let d = @buf.Decoder::new(data)
  let version = d.read_i16()
  guard 0 <= version && version <= CONSUMER_PROTOCOL_MAX_VERSION else {
    raise ProtocolError::ProtocolError(
      "unsupported ConsumerProtocolSubscription version \{version}",
    )
  }
  let n = d.read_i32()
  let topics : Array[String] = []
  for _ in 0..<n {
    let len = d.read_i16()
    topics.push(@utf8.decode_lossy(d.read_bytes(len)[:]))
  }
  topics
}

///|
/// Serialize a ConsumerProtocolAssignment: topic partitions per member.
pub fn encode_consumer_protocol_assignment(
  assignment : Array[(String, Array[Int])],
) -> Bytes {
  let body = @buf.Encoder::new()
  body.write_i16(CONSUMER_PROTOCOL_ASSIGNMENT_VERSION)
  body.write_i32(assignment.length())
  for entry in assignment {
    let (topic, partitions) = entry
    let bytes = @utf8.encode(topic)
    body.write_i16(bytes.length())
    body.write_bytes(bytes)
    body.write_i32(partitions.length())
    for partition in partitions {
      body.write_i32(partition)
    }
  }
  body.write_i32(-1) // userData: null
  body.to_bytes()
}

///|
/// Parse a ConsumerProtocolAssignment: topic partitions per member.
pub fn decode_consumer_protocol_assignment(
  data : Bytes,
) -> Array[(String, Array[Int])] raise {
  let d = @buf.Decoder::new(data)
  let version = d.read_i16()
  guard 0 <= version && version <= CONSUMER_PROTOCOL_MAX_VERSION else {
    raise ProtocolError::ProtocolError(
      "unsupported ConsumerProtocolAssignment version \{version}",
    )
  }
  let n = d.read_i32()
  let out : Array[(String, Array[Int])] = []
  for _ in 0..<n {
    let len = d.read_i16()
    let topic = @utf8.decode_lossy(d.read_bytes(len)[:])
    let parts = d.read_i32()
    let partitions : Array[Int] = []
    for _ in 0..<parts {
      partitions.push(d.read_i32())
    }
    out.push((topic, partitions))
  }
  out
}
