// Codec tests for Metadata v12/v13 and DescribeTopicPartitions v0:
// hand-built response bodies per the 4.3 schemas, decoded and checked
// field by field; request encoders verified byte-wise.

///|
/// A two-broker, one-topic Metadata response body. `top_error` writes the
/// v13-only top-level error slot.
fn metadata_body(top_error : Int?) -> Bytes {
  let e = @buf.Encoder::new()
  e.write_i32(9) // throttle_time_ms
  e.write_compact_len(2) // two brokers
  e.write_i32(1)
  e.write_compact_string("broker-a")
  e.write_i32(9092)
  e.write_compact_nullable_string(Some("rack-1"))
  e.write_tag_buffer()
  e.write_i32(2)
  e.write_compact_string("broker-b")
  e.write_i32(9093)
  e.write_compact_nullable_string(None)
  e.write_tag_buffer()
  e.write_compact_nullable_string(Some("cluster-1"))
  e.write_i32(1) // controller id
  e.write_compact_len(1) // one topic
  e.write_i16(0) // no error
  e.write_compact_string("orders")
  e.write_bytes(Bytes::make(16, b'\x2a')) // topic id
  e.write_bool(true) // is_internal
  e.write_compact_len(2)
  // partition 0: replicas [1,2], isr [1], no offline replicas
  e.write_i16(0)
  e.write_i32(0)
  e.write_i32(1) // leader
  e.write_i32(7) // leader epoch
  e.write_compact_len(2)
  e.write_i32(1)
  e.write_i32(2)
  e.write_compact_len(1)
  e.write_i32(1)
  e.write_compact_len(0)
  e.write_tag_buffer()
  // partition 1: errored partitions are dropped from the result
  e.write_i16(5) // LEADER_NOT_AVAILABLE
  e.write_i32(1)
  e.write_i32(-1)
  e.write_i32(-1)
  e.write_compact_len(0)
  e.write_compact_len(0)
  e.write_compact_len(0)
  e.write_tag_buffer()
  e.write_i32(0) // topic_authorized_operations
  e.write_tag_buffer()
  match top_error {
    Some(code) => e.write_i16(code)
    None => ()
  }
  e.write_tag_buffer()
  e.to_bytes()
}

///|
test "metadata v12 response decodes brokers, topic id, and internal flag" {
  let d = @buf.Decoder::new(metadata_body(None))
  let (metadata, throttle) = @moonkafka.decode_metadata_response(12, d)
  assert_eq(throttle, 9)
  assert_eq(metadata.brokers.length(), 2)
  let broker_a = metadata.brokers.get(1).unwrap()
  assert_eq(broker_a.host, "broker-a")
  assert_eq(broker_a.port, 9092)
  let broker_b = metadata.brokers.get(2).unwrap()
  assert_eq(broker_b.host, "broker-b")
  assert_eq(metadata.topics.length(), 1)
  let topic = metadata.topics[0]
  assert_eq(topic.name, "orders")
  assert_eq(
    topic.topic_id,
    @moonkafka.Uuid::from_bytes(Bytes::make(16, b'\x2a')),
  )
  assert_true(topic.is_internal)
  assert_eq(topic.partitions.length(), 1)
  assert_eq(topic.partitions[0].index, 0)
  assert_eq(topic.partitions[0].leader, 1)
  assert_eq(topic.partitions[0].leader_epoch, 7)
  assert_eq(d.remaining(), 0)
}

///|
test "metadata v13 response decodes the top-level error slot" {
  let d = @buf.Decoder::new(metadata_body(Some(0)))
  let (metadata, throttle) = @moonkafka.decode_metadata_response(13, d)
  assert_eq(throttle, 9)
  assert_eq(metadata.topics.length(), 1)
  assert_eq(d.remaining(), 0)
  let mut matched = false
  let errored = @buf.Decoder::new(metadata_body(Some(35)))
  ignore(@moonkafka.decode_metadata_response(13, errored)) catch {
    @moonkafka.BrokerError(35, _) => matched = true
    e => fail("expected BrokerError 35, got \{e}")
  }
  assert_true(matched)
}

///|
test "metadata decode refuses versions without codecs" {
  let mut refused = false
  ignore(@moonkafka.decode_metadata_response(14, @buf.Decoder::new(b""))) catch {
    @moonkafka.ProtocolError(_) => refused = true
    e => fail("expected ProtocolError, got \{e}")
  }
  assert_true(refused)
}

///|
test "metadata request null topics lists all topics" {
  let all = @moonkafka.encode_metadata_request(None)
  assert_eq(all.length(), 4)
  assert_eq(all[0], b'\x00') // null compact array marker
  assert_eq(all[1], b'\x01') // allow_auto_topic_creation
  assert_eq(all[2], b'\x00') // include_topic_authorized_operations
  assert_eq(all[3], b'\x00') // tag buffer
  // one named topic: array length, zero topic id, compact string, entry
  // tag buffer, the two booleans, and the body tag buffer
  let named = @moonkafka.encode_metadata_request(Some(["a"]))
  assert_eq(named[0], b'\x02') // one topic + 1
  assert_eq(named.length(), 1 + 16 + 2 + 1 + 2 + 1)
}

///|
test "describe topic partitions request encodes cursor and limit" {
  let req = @moonkafka.encode_describe_topic_partitions_request(
    ["events"],
    100,
    cursor=Some({ topic_name: "events", partition_index: 3, }),
  )
  let d = @buf.Decoder::new(req)
  assert_eq(d.read_compact_len(), 1) // one topic
  assert_eq(d.read_compact_string(), "events")
  d.skip_tag_buffer()
  assert_eq(d.read_i32(), 100) // response_partition_limit
  assert_eq(d.read_i8(), 1) // cursor present
  assert_eq(d.read_compact_string(), "events")
  assert_eq(d.read_i32(), 3)
  d.skip_tag_buffer()
  d.skip_tag_buffer()
  assert_eq(d.remaining(), 0)
  // without a cursor the nullable struct marker is -1
  let fresh = @moonkafka.encode_describe_topic_partitions_request([], 2000)
  let d2 = @buf.Decoder::new(fresh)
  assert_eq(d2.read_compact_len(), 0) // all topics
  assert_eq(d2.read_i32(), 2000)
  assert_eq(d2.read_i8(), -1)
  d2.skip_tag_buffer()
  assert_eq(d2.remaining(), 0)
}

///|
test "describe topic partitions response decodes topics and next cursor" {
  let e = @buf.Encoder::new()
  e.write_i32(0) // throttle
  e.write_compact_len(2)
  // topic one: one partition whose nullable ELR arrays are null
  e.write_i16(0)
  e.write_compact_string("alpha")
  e.write_bytes(Bytes::make(16, b'\x01'))
  e.write_bool(true)
  e.write_compact_len(1)
  e.write_i16(0)
  e.write_i32(4) // index
  e.write_i32(2) // leader
  e.write_i32(9) // leader epoch
  e.write_compact_len(0) // replicas
  e.write_compact_len(0) // isr
  e.write_compact_len(-1) // eligible leader replicas: null
  e.write_compact_len(-1) // last known elr: null
  e.write_compact_len(0) // offline replicas
  e.write_tag_buffer()
  e.write_i32(0) // authorized operations
  e.write_tag_buffer()
  // topic two: no partitions
  e.write_i16(0)
  e.write_compact_string("beta")
  e.write_bytes(Bytes::make(16, b'\x02'))
  e.write_bool(false)
  e.write_compact_len(0)
  e.write_i32(0)
  e.write_tag_buffer()
  // next cursor resumes at beta
  e.write_i8(1)
  e.write_compact_string("beta")
  e.write_i32(0)
  e.write_tag_buffer()
  e.write_tag_buffer()
  let (page, throttle) = @moonkafka.decode_describe_topic_partitions_response(
    @buf.Decoder::new(e.to_bytes()),
  )
  assert_eq(throttle, 0)
  assert_eq(page.topics.length(), 2)
  assert_eq(page.topics[0].name, "alpha")
  assert_true(page.topics[0].is_internal)
  assert_eq(page.topics[0].partitions.length(), 1)
  assert_eq(page.topics[0].partitions[0].index, 4)
  assert_eq(page.topics[0].partitions[0].leader, 2)
  assert_eq(page.topics[0].partitions[0].leader_epoch, 9)
  assert_eq(page.topics[1].name, "beta")
  assert_eq(page.topics[1].partitions.length(), 0)
  guard page.next_cursor is Some(cursor) else {
    abort("expected a next cursor")
  }
  assert_eq(cursor.topic_name, "beta")
  assert_eq(cursor.partition_index, 0)
}

///|
test "describe topic partitions raises per-topic broker errors" {
  let e = @buf.Encoder::new()
  e.write_i32(0) // throttle
  e.write_compact_len(1)
  e.write_i16(3) // UNKNOWN_TOPIC_OR_NOT_FOUND
  e.write_compact_string("gone")
  e.write_bytes(Bytes::make(16, b'\x00'))
  e.write_bool(false)
  e.write_compact_len(0)
  e.write_i32(0)
  e.write_tag_buffer()
  e.write_i8(-1) // no next cursor
  e.write_tag_buffer()
  // Per-topic error codes surface as values for the admin client.
  let (page, _throttle) = @moonkafka.decode_describe_topic_partitions_response(
    @buf.Decoder::new(e.to_bytes()),
  )
  assert_eq(page.topics[0].error_code, 3)
  assert_eq(page.topics[0].name, "gone")
}
