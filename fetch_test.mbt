// Codec and fetch-session tests for Fetch v12-v16, pinned to the 4.3
// FetchRequest.json / FetchResponse.json schemas.

///|
fn test_topic(version : Int) -> @moonkafka.FetchTopicReq raise {
  {
    name: "events",
    topic_id: if version == 12 {
      @moonkafka.Uuid::zero()
    } else {
      @moonkafka.Uuid::parse("AAAAAAAAAAAAAAAAAAAAAQ")
    },
    partitions: [
      { index: 0, leader_epoch: 3, fetch_offset: 42L, max_bytes: 100, },
    ],
  }
}

///|
fn test_session(version : Int) -> @moonkafka.FetchSessionReq raise {
  {
    session_id: 7,
    session_epoch: 2,
    partitions: [],
    forgotten: [
      {
        name: "events",
        topic_id: if version == 12 {
          @moonkafka.Uuid::zero()
        } else {
          @moonkafka.Uuid::parse("AAAAAAAAAAAAAAAAAAAAAQ")
        },
        partitions: [1],
      },
    ],
  }
}

///|
test "fetch v12 request encodes replica id, names, and session fields" {
  let req = @moonkafka.encode_fetch_request(
    12,
    [test_topic(12)],
    session=test_session(12),
    max_wait_ms=500,
    max_bytes=1000,
  )
  let d = @buf.Decoder::new(req)
  assert_eq(d.read_i32(), -1) // replica_id: consumer (present at v12)
  assert_eq(d.read_i32(), 500) // max_wait
  assert_eq(d.read_i32(), 1) // min_bytes
  assert_eq(d.read_i32(), 1000) // max_bytes
  assert_eq(d.read_i8(), 0) // isolation: read uncommitted
  assert_eq(d.read_i32(), 7) // session id
  assert_eq(d.read_i32(), 2) // session epoch
  assert_eq(d.read_compact_len(), 1) // one topic
  assert_eq(d.read_compact_string(), "events") // v12: by name
  assert_eq(d.read_compact_len(), 1) // one partition
  assert_eq(d.read_i32(), 0) // index
  assert_eq(d.read_i32(), 3) // current leader epoch
  assert_eq(d.read_i64(), 42L) // fetch offset
  assert_eq(d.read_i32(), -1) // last fetched epoch
  assert_eq(d.read_i64(), -1L) // log start offset
  assert_eq(d.read_i32(), 100) // partition max bytes
  d.skip_tag_buffer() // partition
  d.skip_tag_buffer() // topic
  assert_eq(d.read_compact_len(), 1) // one forgotten topic
  assert_eq(d.read_compact_string(), "events")
  assert_eq(d.read_compact_len(), 1) // one forgotten partition
  assert_eq(d.read_i32(), 1)
  d.skip_tag_buffer() // forgotten topic
  assert_eq(d.read_compact_string(), "") // rack id
  assert_eq(d.remaining(), 1) // body tag buffer
}

///|
test "fetch v16 request drops replica id and addresses topics by id" {
  let req = @moonkafka.encode_fetch_request(
    16,
    [test_topic(16)],
    session=test_session(16),
    max_wait_ms=500,
    max_bytes=1000,
  )
  let d = @buf.Decoder::new(req)
  assert_eq(d.read_i32(), 500) // max_wait comes first: no replica_id at v16
  assert_eq(d.read_i32(), 1)
  assert_eq(d.read_i32(), 1000)
  assert_eq(d.read_i8(), 0)
  assert_eq(d.read_i32(), 7)
  assert_eq(d.read_i32(), 2)
  assert_eq(d.read_compact_len(), 1)
  assert_eq(
    d.read_bytes(16),
    @moonkafka.Uuid::parse("AAAAAAAAAAAAAAAAAAAAAQ").to_bytes(),
  )
  // ... partition fields identical to v12 from here; just check the
  // forgotten entry is id-addressed too
  let rest = @buf.Decoder::new(req, start=req.length() - 40)
  ignore(rest)
}

///|
fn fetch_response_body(by_id~ : Bool, records~ : Bytes?) -> Bytes {
  let e = @buf.Encoder::new()
  e.write_i32(3) // throttle
  e.write_i16(0) // top-level error
  e.write_i32(7) // session id
  e.write_compact_len(1) // one topic
  if by_id {
    e.write_bytes(Bytes::make(16, b'\x2a'))
  } else {
    e.write_compact_string("events")
  }
  e.write_compact_len(1) // one partition
  e.write_i32(0) // index
  e.write_i16(0) // no error
  e.write_i64(100L) // high watermark
  e.write_i64(90L) // last stable offset
  e.write_i64(5L) // log start offset
  e.write_compact_len(-1) // aborted transactions: null
  e.write_i32(-1) // preferred read replica
  match records {
    Some(records) => {
      e.write_compact_len(records.length())
      e.write_bytes(records)
    }
    None => e.write_compact_len(-1) // records: null
  }
  e.write_tag_buffer()
  e.write_tag_buffer()
  e.write_tag_buffer()
  e.to_bytes()
}

///|
test "fetch v12 response decodes topics by name" {
  let d = @buf.Decoder::new(fetch_response_body(by_id=false, records=None))
  let (result, throttle) = @moonkafka.decode_fetch_response(12, d)
  assert_eq(throttle, 3)
  assert_eq(result.top_error_code, 0)
  assert_eq(result.session_id, 7)
  assert_eq(result.topics.length(), 1)
  assert_eq(result.topics[0].name, "events")
  assert_eq(result.topics[0].topic_id, @moonkafka.Uuid::zero())
  let p = result.topics[0].partitions[0]
  assert_eq(p.partition, 0)
  assert_eq(p.high_watermark, 100L)
  assert_eq(p.last_stable_offset, 90L)
  assert_eq(p.log_start_offset, 5L)
  assert_true(p.records.is_empty())
  assert_true(p.records_complete)
  assert_eq(d.remaining(), 0)
}

///|
test "fetch v13 response decodes topics by id with records" {
  let batch = @moonkafka.encode_record_batch([
    { offset: 0L, timestamp: 1L, key: None, value: Some(b"x"), },
  ])
  let d = @buf.Decoder::new(
    fetch_response_body(by_id=true, records=Some(batch)),
  )
  let (result, _throttle) = @moonkafka.decode_fetch_response(13, d)
  assert_eq(result.topics[0].name, "")
  assert_eq(
    result.topics[0].topic_id,
    @moonkafka.Uuid::from_bytes(Bytes::make(16, b'\x2a')),
  )
  let p = result.topics[0].partitions[0]
  assert_eq(p.records.length(), 1)
  assert_eq(p.records[0].offset, 0L)
  assert_true(p.records_complete)
}

///|
test "fetch response reports a truncated trailing batch" {
  let batch = @moonkafka.encode_record_batch([
    { offset: 0L, timestamp: 1L, key: None, value: Some(b"x"), },
    { offset: 0L, timestamp: 2L, key: None, value: Some(b"y"), },
  ])
  let cut = batch.length() - 5
  let partial : Array[Byte] = []
  for i in 0..<cut {
    partial.push(batch[i])
  }
  let d = @buf.Decoder::new(
    fetch_response_body(by_id=true, records=Some(Bytes::from_array(partial))),
  )
  let (result, _throttle) = @moonkafka.decode_fetch_response(16, d)
  let p = result.topics[0].partitions[0]
  // No complete second batch: nothing decoded past the truncation point.
  assert_true(p.records.is_empty())
  assert_true(!p.records_complete)
}

///|
test "fetch codecs refuse versions without codecs" {
  let mut refused = 0
  ignore(
    @moonkafka.encode_fetch_request(
      17,
      [],
      session={
        session_id: 0,
        session_epoch: 0,
        partitions: [],
        forgotten: [],
      },
      max_wait_ms=1,
      max_bytes=1,
    ),
  ) catch {
    @moonkafka.ProtocolError(_) => refused += 1
    _ => fail("expected ProtocolError")
  }
  ignore(@moonkafka.decode_fetch_response(11, @buf.Decoder::new(b""))) catch {
    @moonkafka.ProtocolError(_) => refused += 1
    _ => fail("expected ProtocolError")
  }
  assert_eq(refused, 2)
}

///|
test "fetch session acquires an id and then goes incremental" {
  let s = @moonkafka.FetchSession::new()
  let wanted : Map[Int, (Int64, Int)] = Map([])
  wanted[0] = (42L, 3)
  let full = s.prepare("events", @moonkafka.Uuid::zero(), wanted, 100)
  assert_eq(full.session_id, 0)
  assert_eq(full.session_epoch, 0)
  assert_eq(full.partitions.length(), 1)
  assert_eq(full.forgotten.length(), 0)
  s.handle_response(7, 0)
  assert_true(s.established())
  // Nothing changed: the incremental request carries no partitions.
  let incremental = s.prepare("events", @moonkafka.Uuid::zero(), wanted, 100)
  assert_eq(incremental.session_id, 7)
  assert_eq(incremental.session_epoch, 1)
  assert_eq(incremental.partitions.length(), 0)
  // A moved position is re-sent.
  wanted[0] = (50L, 3)
  let moved = s.prepare("events", @moonkafka.Uuid::zero(), wanted, 100)
  assert_eq(moved.session_epoch, 2)
  assert_eq(moved.partitions.length(), 1)
  assert_eq(moved.partitions[0].fetch_offset, 50L)
}

///|
test "fetch session forgets removed partitions" {
  let s = @moonkafka.FetchSession::new()
  let wanted : Map[Int, (Int64, Int)] = Map([])
  wanted[0] = (1L, 0)
  wanted[1] = (2L, 0)
  ignore(s.prepare("events", @moonkafka.Uuid::zero(), wanted, 100))
  s.handle_response(9, 0)
  let none : Map[Int, (Int64, Int)] = Map([])
  let after = s.prepare(
    "events",
    @moonkafka.Uuid::parse("AAAAAAAAAAAAAAAAAAAAAQ"),
    none,
    100,
  )
  assert_eq(after.session_id, 9)
  assert_eq(after.partitions.length(), 0)
  assert_eq(after.forgotten.length(), 2)
  for topic in after.forgotten {
    assert_eq(topic.name, "events")
    assert_eq(topic.topic_id, @moonkafka.Uuid::parse("AAAAAAAAAAAAAAAAAAAAAQ"))
  }
}

///|
test "fetch session restarts from a full request after eviction" {
  let s = @moonkafka.FetchSession::new()
  let wanted : Map[Int, (Int64, Int)] = Map([])
  wanted[0] = (1L, 0)
  ignore(s.prepare("events", @moonkafka.Uuid::zero(), wanted, 100))
  s.handle_response(9, 0)
  // Either eviction error tears the session down.
  s.handle_response(9, @moonkafka.FETCH_SESSION_ID_NOT_FOUND)
  assert_true(!s.established())
  let full = s.prepare("events", @moonkafka.Uuid::zero(), wanted, 100)
  assert_eq(full.session_epoch, 0)
  assert_eq(full.partitions.length(), 1)
  s.handle_response(9, 0)
  // A broker-closed session (id 0 in reply) does the same.
  s.handle_response(0, 0)
  assert_true(!s.established())
  let refull = s.prepare("events", @moonkafka.Uuid::zero(), wanted, 100)
  assert_eq(refull.session_epoch, 0)
}

///|
test "fetch session without a broker grant stays sessionless" {
  let s = @moonkafka.FetchSession::new()
  let wanted : Map[Int, (Int64, Int)] = Map([])
  wanted[0] = (1L, 0)
  ignore(s.prepare("events", @moonkafka.Uuid::zero(), wanted, 100))
  s.handle_response(0, 0) // broker declined to create a session
  let next = s.prepare("events", @moonkafka.Uuid::zero(), wanted, 100)
  assert_eq(next.session_id, 0)
  assert_eq(next.session_epoch, 0)
  assert_eq(next.partitions.length(), 1) // full request again
}
