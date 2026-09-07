// An in-process fake broker serving real ApiVersions, SASL, Metadata, and
// Produce responses (decodable by this library's own decoders), used to
// exercise the full producer flow — including SASL and reconnect after a
// crash — without a Kafka installation.

///|
fn fake_api_versions_response(
  throttle_ms : Int,
  hide_apis? : Array[Int] = [],
) -> Bytes {
  let e = @buf.Encoder::new()
  e.write_i16(0) // error_code
  let all_apis : Array[(Int, Int)] = [
    (0, 13),
    (1, 16),
    (2, 11),
    (3, 13),
    (8, 10),
    (9, 10),
    (10, 4),
    (23, 4),
    (68, 1),
    (18, 3),
    (22, 5),
    (24, 3),
    (25, 4),
    (26, 5),
    (28, 4),
    (75, 0),
    (11, 9),
    (12, 4),
    (13, 5),
    (14, 5),
    (19, 7),
    (20, 6),
    (21, 2),
    (37, 3),
    (47, 0),
    (29, 3),
    (30, 3),
    (31, 3),
    (48, 1),
    (49, 1),
    (50, 0),
    (51, 0),
    (60, 2),
    (32, 4),
    (44, 1),
    (33, 2),
    (74, 1),
    (35, 5),
    (43, 2),
    (45, 1),
    (46, 0),
    (64, 0),
    (55, 2),
    (57, 2),
  ]
  let apis = all_apis.filter(fn(pair) { !hide_apis.contains(pair.0) })
  e.write_compact_len(apis.length())
  for pair in apis {
    let (key, max) = pair
    e.write_i16(key)
    e.write_i16(0)
    e.write_i16(max)
    e.write_tag_buffer()
  }
  e.write_i32(throttle_ms) // throttle
  e.write_tag_buffer()
  e.to_bytes()
}

///|
/// Mechanism name from a SaslHandshake request (non-flexible v1 request:
/// no tag buffer between the header and the mechanism string).
fn fake_sasl_handshake_mechanism(rest : Bytes) -> String raise {
  let d = @buf.Decoder::new(rest)
  ignore(d.read_i16()) // api key
  ignore(d.read_i16()) // api version
  ignore(d.read_i32()) // correlation id
  let id_len = d.read_i16()
  ignore(d.read_bytes(id_len))
  let len = d.read_i16()
  @utf8.decode_lossy(d.read_bytes(len)[:])
}

///|
/// auth_bytes from a SaslAuthenticate request (flexible v2: the compact
/// byte array follows the v2 request header's tag buffer).
fn fake_sasl_auth_bytes(rest : Bytes) -> Bytes raise {
  let d = @buf.Decoder::new(rest)
  ignore(d.read_i16()) // api key
  ignore(d.read_i16()) // api version
  ignore(d.read_i32()) // correlation id
  ignore(d.read_compact_string()) // client id
  d.skip_tag_buffer()
  let n = d.read_compact_len()
  d.read_bytes(n)
}

///|
/// SaslAuthenticate response envelope (flexible v2) wrapping a SCRAM
/// message as auth bytes.
fn fake_sasl_scram_response(message : String) -> Bytes {
  let e = @buf.Encoder::new()
  e.write_i16(0) // error_code
  e.write_compact_nullable_string(None) // error message
  let bytes = @utf8.encode(message)
  e.write_compact_len(bytes.length())
  e.write_bytes(bytes) // auth bytes
  e.write_i64(0L) // session lifetime
  e.write_tag_buffer()
  e.to_bytes()
}

///|
let scram_test_salt : Bytes = b"fake-broker-salt"

///|
/// Strip the gs2 header from a client-first-message: everything after the
/// second comma.
fn scram_bare_message(msg : String) -> String {
  let chars : Array[Char] = []
  for c in msg {
    chars.push(c)
  }
  let mut commas = 0
  let mut start = 0
  for i in 0..<chars.length() {
    if chars[i] == ',' {
      commas += 1
      if commas == 2 {
        start = i + 1
        break
      }
    }
  }
  let sb = StringBuilder()
  for i in start..<chars.length() {
    sb.write_char(chars[i])
  }
  sb.to_string()
}

///|
/// Value of an "x=..." attribute; returns None when the attribute is absent.
fn scram_attr_value(msg : String, want : Char) -> String? {
  let part : Array[Char] = []
  for c in msg {
    if c == ',' {
      if part.length() >= 2 && part[0] == want && part[1] == '=' {
        let sb = StringBuilder()
        for i in 2..<part.length() {
          sb.write_char(part[i])
        }
        return Some(sb.to_string())
      }
      part.clear()
    } else {
      part.push(c)
    }
  }
  if part.length() >= 2 && part[0] == want && part[1] == '=' {
    let sb = StringBuilder()
    for i in 2..<part.length() {
      sb.write_char(part[i])
    }
    return Some(sb.to_string())
  }
  None
}

///|
/// client-final-message minus the ",p=proof" suffix.
fn scram_final_without_proof(msg : String) -> String {
  let chars : Array[Char] = []
  for c in msg {
    chars.push(c)
  }
  let sb = StringBuilder()
  for i in 0..<chars.length() {
    if chars[i] == ',' &&
      i + 2 < chars.length() &&
      chars[i + 1] == 'p' &&
      chars[i + 2] == '=' {
      break
    }
    sb.write_char(chars[i])
  }
  sb.to_string()
}

///|
fn fake_sasl_handshake_response() -> Bytes {
  let e = @buf.Encoder::new()
  e.write_i16(0) // error_code
  let mechanisms : Array[String] = [
    "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512", "OAUTHBEARER",
  ]
  e.write_i32(mechanisms.length())
  for mechanism in mechanisms {
    let bytes = @utf8.encode(mechanism)
    e.write_i16(bytes.length())
    e.write_bytes(bytes)
  }
  e.to_bytes()
}

///|
fn fake_sasl_authenticate_response(ok : Bool) -> Bytes {
  let e = @buf.Encoder::new()
  e.write_i16(if ok { 0 } else { 58 }) // SASL_AUTHENTICATION_FAILED
  e.write_compact_nullable_string(
    if ok {
      None
    } else {
      Some("bad credentials")
    },
  )
  e.write_compact_len(0) // auth bytes: empty
  e.write_i64(0L) // session lifetime
  e.write_tag_buffer()
  e.to_bytes()
}

///|
/// The requested coordinator keys of a FindCoordinator v4 request.
fn fake_find_coordinator_request_keys(rest : Bytes) -> Array[String] raise {
  let d = @buf.Decoder::new(rest)
  ignore(d.read_i16()) // api key
  ignore(d.read_i16()) // api version
  ignore(d.read_i32()) // correlation id
  ignore(d.read_compact_string()) // client id
  d.skip_tag_buffer()
  ignore(d.read_i8()) // key type
  let n = d.read_compact_len()
  let keys : Array[String] = []
  for _ in 0..<n {
    keys.push(d.read_compact_string())
  }
  keys
}

///|
/// FindCoordinator v4 response body: one entry per requested key, all
/// pointing at node 0.
fn fake_find_coordinator_response(keys : Array[String], port : Int) -> Bytes {
  let e = @buf.Encoder::new()
  e.write_i32(0) // throttle
  e.write_compact_len(keys.length())
  for key in keys {
    e.write_compact_string(key)
    e.write_i32(0) // node id
    e.write_compact_string("127.0.0.1")
    e.write_i32(port)
    e.write_i16(0) // no error
    e.write_compact_nullable_string(None)
    e.write_tag_buffer()
  }
  e.write_tag_buffer()
  e.to_bytes()
}

///|
/// Parse an InitProducerId v5 request: (transactional id, timeout,
/// producer id, producer epoch).
fn fake_init_producer_id_request(rest : Bytes) -> (String?, Int64, Int) raise {
  let d = @buf.Decoder::new(rest)
  ignore(d.read_i16()) // api key
  ignore(d.read_i16()) // api version
  ignore(d.read_i32()) // correlation id
  ignore(d.read_compact_string()) // client id
  d.skip_tag_buffer()
  let transactional_id = d.read_compact_nullable_string()
  ignore(d.read_i32()) // transaction timeout
  let producer_id = d.read_i64()
  let producer_epoch = d.read_i16()
  (transactional_id, producer_id, producer_epoch)
}

///|
/// InitProducerId v5 response: a fresh identity gets pid 500 epoch 0;
/// a bump request keeps the pid and increments the epoch.
fn fake_init_producer_id_response(
  producer_id : Int64,
  producer_epoch : Int,
) -> Bytes {
  let e = @buf.Encoder::new()
  e.write_i32(0) // throttle
  e.write_i16(0) // no error
  e.write_i64(producer_id)
  e.write_i16(producer_epoch)
  e.write_tag_buffer()
  e.to_bytes()
}

///|
/// Metadata response body; v13 appends a top-level error code after the
/// topics array.
fn fake_metadata_response(
  topic : String,
  broker_port : Int,
  version : Int,
  partition_count? : Int = 1,
) -> Bytes {
  let e = @buf.Encoder::new()
  e.write_i32(0) // throttle
  e.write_compact_len(1) // one broker, node 0, which is also the leader
  e.write_i32(0)
  e.write_compact_string("127.0.0.1")
  e.write_i32(broker_port)
  e.write_compact_nullable_string(None) // rack
  e.write_tag_buffer()
  e.write_compact_nullable_string(None) // cluster id
  e.write_i32(0) // controller
  e.write_compact_len(1) // one topic
  e.write_i16(0) // no error
  e.write_compact_string(topic)
  e.write_bytes(fake_topic_id) // topic id
  e.write_bool(false) // is_internal
  e.write_compact_len(partition_count)
  for p in 0..<partition_count {
    e.write_i16(0) // no error
    e.write_i32(p) // index
    e.write_i32(0) // leader node
    e.write_i32(0) // leader epoch
    e.write_compact_len(0) // replicas
    e.write_compact_len(0) // isr
    e.write_compact_len(0) // offline replicas
    e.write_tag_buffer()
  }
  e.write_i32(0) // authorized operations (skipped by the decoder)
  e.write_tag_buffer()
  if version >= 13 {
    e.write_i16(0) // top-level error code
  }
  e.write_tag_buffer()
  e.to_bytes()
}

///|
/// The fake topic's id; metadata reports it and Produce v13 responses
/// echo it (KIP-516 topic-id addressing).
let fake_topic_id : Bytes = Bytes::make(16, b'\x2a')

///|
/// Parse the first partition index and record count of a Produce v12/v13
/// request: skip the fixed header ints, client id + tag buffer, then the
/// request body down to the first topic's first partition entry; the
/// count sits in the batch's v2 header (i32 at bytes 57..61).
fn fake_produce_request_info(
  rest : Bytes,
  version : Int,
) -> (Int, Int, Int64, Int, Int) raise {
  let d = @buf.Decoder::new(rest)
  ignore(d.read_i16()) // api key
  ignore(d.read_i16()) // api version
  ignore(d.read_i32()) // correlation id
  ignore(d.read_compact_string()) // client id
  d.skip_tag_buffer()
  ignore(d.read_compact_nullable_string()) // transactional id
  ignore(d.read_i16()) // acks
  ignore(d.read_i32()) // timeout
  ignore(d.read_compact_len()) // topics: one
  match version {
    13 => d.skip(16) // topic id
    _ => ignore(d.read_compact_string()) // topic name
  }
  ignore(d.read_compact_len()) // partitions
  let partition = d.read_i32()
  let n = d.read_compact_len() // records: one batch's bytes
  let batch = d.read_bytes(n)
  let count = i32_at(batch, 57)
  let producer_id = (batch[43].to_int64() << 56) |
    (batch[44].to_int64() << 48) |
    (batch[45].to_int64() << 40) |
    (batch[46].to_int64() << 32) |
    (batch[47].to_int64() << 24) |
    (batch[48].to_int64() << 16) |
    (batch[49].to_int64() << 8) |
    batch[50].to_int64()
  let epoch = (batch[51].to_int() << 8) | batch[52].to_int()
  let base_sequence = i32_at(batch, 53)
  (partition, count, producer_id, epoch, base_sequence)
}

///|
fn i32_at(data : Bytes, at : Int) -> Int {
  (data[at].to_int() << 24) |
  (data[at + 1].to_int() << 16) |
  (data[at + 2].to_int() << 8) |
  data[at + 3].to_int()
}

///|
fn fake_produce_response(
  topic : String,
  version : Int,
  base_offset : Int64,
  error_code? : Int = 0,
) -> Bytes {
  let e = @buf.Encoder::new()
  e.write_compact_len(1) // topics
  if version >= 13 {
    e.write_bytes(fake_topic_id)
  } else {
    e.write_compact_string(topic)
  }
  e.write_compact_len(1) // partitions
  e.write_i32(0) // index
  e.write_i16(error_code)
  e.write_i64(base_offset)
  e.write_i64(-1L) // log_append_time
  e.write_i64(-1L) // log_start_offset
  e.write_compact_len(0) // record errors
  e.write_compact_nullable_string(None) // error message
  e.write_tag_buffer()
  e.write_tag_buffer()
  e.write_i32(0) // throttle
  e.write_tag_buffer()
  e.to_bytes()
}

///|
/// ListOffsets v7 response body: every partition at the given offset.
fn fake_list_offsets_response(
  topic : String,
  offset : Int64,
  partition_count? : Int = 1,
) -> Bytes {
  let e = @buf.Encoder::new()
  e.write_i32(0) // throttle
  e.write_compact_len(1) // one topic
  e.write_compact_string(topic)
  e.write_compact_len(partition_count)
  for p in 0..<partition_count {
    e.write_i32(p) // index
    e.write_i16(0) // no error
    e.write_i64(-1L) // timestamp
    e.write_i64(offset)
    e.write_i32(-1) // leader epoch
    e.write_tag_buffer()
  }
  e.write_tag_buffer()
  e.write_tag_buffer()
  e.to_bytes()
}

///|
/// session_epoch of a Fetch request at v15+ (no replica_id field): the
/// value sits after max_wait/min/max/isolation and session_id.
fn fake_fetch_request_session_epoch(rest : Bytes) -> Int raise {
  let d = @buf.Decoder::new(rest)
  ignore(d.read_i16()) // api key
  ignore(d.read_i16()) // api version
  ignore(d.read_i32()) // correlation id
  ignore(d.read_compact_string()) // client id
  d.skip_tag_buffer()
  ignore(d.read_i32()) // max_wait_ms
  ignore(d.read_i32()) // min_bytes
  ignore(d.read_i32()) // max_bytes
  ignore(d.read_i8()) // isolation level
  ignore(d.read_i32()) // session id
  d.read_i32() // session epoch
}

///|
/// Shared state of the fake classic group coordinator. Wrapped in a
/// single-entry array so every connection task mutates the same cell.
struct FakeCoordinator {
  mut next_member : Int
  /// member id -> instance id ("" = none)
  members : Map[String, String]
  /// Current completed round == the group's generation.
  mut round : Int
  /// member -> round of its last join.
  arrived : Map[String, Int]
  /// member -> subscription metadata (first protocol entry).
  subscriptions : Map[String, Bytes]
  /// member -> assignment bytes the leader synced.
  assignments : Map[String, Bytes]
  /// member -> generation of the stored assignment.
  sync_generations : Map[String, Int]
  mut protocol : String
  joins : Array[(String, Int)]
  syncs : Array[(String, Int)]
  leaves : Array[String]
}

///|
fn FakeCoordinator::new() -> FakeCoordinator {
  {
    next_member: 0,
    members: Map([]),
    round: 0,
    arrived: Map([]),
    subscriptions: Map([]),
    assignments: Map([]),
    sync_generations: Map([]),
    protocol: "",
    joins: [],
    syncs: [],
    leaves: [],
  }
}

///|
/// Parse a JoinGroup v9 request: (group, member id, instance id,
/// protocol entries).
fn fake_join_group_request(
  rest : Bytes,
) -> (String, String, String?, Array[(String, Bytes)]) raise {
  let d = @buf.Decoder::new(rest)
  ignore(d.read_i16()) // api key
  ignore(d.read_i16()) // api version
  ignore(d.read_i32()) // correlation id
  ignore(d.read_compact_string()) // client id
  d.skip_tag_buffer()
  let group_id = d.read_compact_string()
  ignore(d.read_i32()) // session timeout
  ignore(d.read_i32()) // rebalance timeout
  let member_id = d.read_compact_string()
  let instance_id = d.read_compact_nullable_string()
  ignore(d.read_compact_string()) // protocol type
  let n = d.read_compact_len()
  let protocols : Array[(String, Bytes)] = []
  for _ in 0..<n {
    let name = d.read_compact_string()
    let len = d.read_compact_len()
    let metadata = d.read_bytes(len)
    d.skip_tag_buffer()
    protocols.push((name, metadata))
  }
  (group_id, member_id, instance_id, protocols)
}

///|
/// JoinGroup v9 response body.
fn fake_join_group_response(
  error_code : Int,
  generation : Int,
  protocol : String,
  leader : String,
  member_id : String,
  members : Array[(String, Bytes)],
) -> Bytes {
  let e = @buf.Encoder::new()
  e.write_i32(0) // throttle
  e.write_i16(error_code)
  e.write_i32(generation)
  e.write_compact_nullable_string(Some("consumer")) // protocol type
  e.write_compact_string(protocol)
  e.write_compact_string(leader)
  e.write_bool(false) // skip assignment
  e.write_compact_string(member_id)
  e.write_compact_len(members.length())
  for member in members {
    let (id, metadata) = member
    e.write_compact_string(id)
    e.write_compact_nullable_string(None)
    e.write_compact_len(metadata.length())
    e.write_bytes(metadata)
    e.write_tag_buffer()
  }
  e.write_tag_buffer()
  e.to_bytes()
}

///|
/// Parse a SyncGroup v5 request: (member id, generation, assignments).
fn fake_sync_group_request(
  rest : Bytes,
) -> (String, Int, Array[(String, Bytes)]) raise {
  let d = @buf.Decoder::new(rest)
  ignore(d.read_i16()) // api key
  ignore(d.read_i16()) // api version
  ignore(d.read_i32()) // correlation id
  ignore(d.read_compact_string()) // client id
  d.skip_tag_buffer()
  ignore(d.read_compact_string()) // group
  let generation = d.read_i32()
  let member_id = d.read_compact_string()
  ignore(d.read_compact_nullable_string()) // instance
  ignore(d.read_compact_nullable_string()) // protocol type
  ignore(d.read_compact_nullable_string()) // protocol name
  let n = d.read_compact_len()
  let assignments : Array[(String, Bytes)] = []
  for _ in 0..<n {
    let id = d.read_compact_string()
    let len = d.read_compact_len()
    let bytes = d.read_bytes(len)
    d.skip_tag_buffer()
    assignments.push((id, bytes))
  }
  (member_id, generation, assignments)
}

///|
/// SyncGroup v5 response body.
fn fake_sync_group_response(assignment : Bytes) -> Bytes {
  let e = @buf.Encoder::new()
  e.write_i32(0) // throttle
  e.write_i16(0) // no error
  e.write_compact_nullable_string(None) // protocol type
  e.write_compact_nullable_string(None) // protocol name
  e.write_compact_len(assignment.length())
  e.write_bytes(assignment)
  e.write_tag_buffer()
  e.to_bytes()
}

///|
/// Parse a Heartbeat v4 request: (member id, generation).
fn fake_classic_heartbeat_request(rest : Bytes) -> (String, Int) raise {
  let d = @buf.Decoder::new(rest)
  ignore(d.read_i16()) // api key
  ignore(d.read_i16()) // api version
  ignore(d.read_i32()) // correlation id
  ignore(d.read_compact_string()) // client id
  d.skip_tag_buffer()
  ignore(d.read_compact_string()) // group
  let generation = d.read_i32()
  let member_id = d.read_compact_string()
  (member_id, generation)
}

///|
/// Heartbeat v4 response body.
fn fake_classic_heartbeat_response(error_code : Int) -> Bytes {
  let e = @buf.Encoder::new()
  e.write_i32(0) // throttle
  e.write_i16(error_code)
  e.write_tag_buffer()
  e.to_bytes()
}

///|
/// Parse a LeaveGroup v5 request: the leaving member id.
fn fake_leave_group_request(rest : Bytes) -> String raise {
  let d = @buf.Decoder::new(rest)
  ignore(d.read_i16()) // api key
  ignore(d.read_i16()) // api version
  ignore(d.read_i32()) // correlation id
  ignore(d.read_compact_string()) // client id
  d.skip_tag_buffer()
  ignore(d.read_compact_string()) // group
  let n = d.read_compact_len()
  let mut member_id = ""
  for _ in 0..<n {
    member_id = d.read_compact_string()
    ignore(d.read_compact_nullable_string()) // instance
    ignore(d.read_compact_nullable_string()) // reason
    d.skip_tag_buffer()
  }
  member_id
}

///|
/// LeaveGroup v5 response body.
fn fake_leave_group_response(member_id : String) -> Bytes {
  let e = @buf.Encoder::new()
  e.write_i32(0) // throttle
  e.write_i16(0) // no error
  e.write_compact_len(1) // members
  e.write_compact_string(member_id)
  e.write_compact_nullable_string(None) // instance
  e.write_i16(0) // no error
  e.write_tag_buffer()
  e.write_tag_buffer()
  e.to_bytes()
}

///|
/// Run one classic-group JoinGroup through the mini coordinator: fresh
/// members start a new round, the round completes when every registered
/// member has arrived, and the generation equals the round number.
async fn fake_classic_join(
  coord : FakeCoordinator,
  member_id : String,
  protocols : Array[(String, Bytes)],
) -> Bytes {
  let mut id = member_id
  if id.is_empty() {
    coord.next_member += 1
    id = "classic-member-\{coord.next_member}"
  }
  // A fresh member, or any join landing on a settled group, starts a
  // new rebalance round (a new generation).
  if !coord.members.contains(id) || coord.round_complete() {
    coord.round += 1
    coord.arrived.clear()
  }
  coord.members[id] = ""
  if !protocols.is_empty() {
    coord.subscriptions[id] = protocols[0].1
    let leader = coord.leader()
    if id == leader && coord.protocol.is_empty() {
      coord.protocol = protocols[0].0
    }
  }
  coord.arrived[id] = coord.round
  let deadline = @async.now() + 3000L
  for ;; {
    if coord.round_complete() || @async.now() > deadline {
      break
    }
    // Round completion runs on the event loop; yield to it.
    @async.sleep(1)
  }
  if !coord.round_complete() {
    // The round stalled: tell the member to retry rather than let it
    // compute against a partial group.
    coord.arrived[id] = -1
    return fake_join_group_response(15, -1, "", "", id, [])
  }
  coord.joins.push((id, coord.round))
  let leader = coord.leader()
  let members : Array[(String, Bytes)] = []
  if id == leader {
    for m, _ in coord.members {
      match coord.subscriptions.get(m) {
        Some(metadata) => members.push((m, metadata))
        None => members.push((m, Bytes::make(0, 0)))
      }
    }
  }
  fake_join_group_response(0, coord.round, coord.protocol, leader, id, members)
}

///|
fn FakeCoordinator::leader(self : FakeCoordinator) -> String {
  let mut best = ""
  for m, _ in self.members {
    if best.is_empty() || m < best {
      best = m
    }
  }
  best
}

///|
fn FakeCoordinator::round_complete(self : FakeCoordinator) -> Bool {
  for m, _ in self.members {
    if self.arrived.get(m) != Some(self.round) {
      return false
    }
  }
  true
}

///|
/// Parse a CreateTopics v7 request: (names, requested partition counts,
/// replication factors, timeout, validate-only).
fn fake_create_topics_request(
  rest : Bytes,
) -> (Array[(String, Int, Int)], Bool) raise {
  let d = @buf.Decoder::new(rest)
  ignore(d.read_i16()) // api key
  ignore(d.read_i16()) // api version
  ignore(d.read_i32()) // correlation id
  ignore(d.read_compact_string()) // client id
  d.skip_tag_buffer()
  let n = d.read_compact_len()
  let topics : Array[(String, Int, Int)] = []
  for _ in 0..<n {
    let name = d.read_compact_string()
    let num_partitions = d.read_i32()
    let replication_factor = d.read_i16()
    let assignments = d.read_compact_len()
    for _ in 0..<assignments {
      ignore(d.read_i32())
      let brokers = d.read_compact_len()
      d.skip(brokers * 4)
      d.skip_tag_buffer()
    }
    let configs = d.read_compact_len()
    for _ in 0..<configs {
      ignore(d.read_compact_string())
      ignore(d.read_compact_nullable_string())
      d.skip_tag_buffer()
    }
    d.skip_tag_buffer()
    topics.push((name, num_partitions, replication_factor))
  }
  ignore(d.read_i32()) // timeout
  let validate_only = d.read_bool()
  (topics, validate_only)
}

///|
/// CreateTopics v7 response body: per-topic success with generated ids.
fn fake_create_topics_response(
  results : Array[(String, Bytes, Int, String?)],
) -> Bytes {
  let e = @buf.Encoder::new()
  e.write_i32(0) // throttle
  e.write_compact_len(results.length())
  for result in results {
    let (name, id, error_code, message) = result
    e.write_compact_string(name)
    e.write_bytes(id)
    e.write_i16(error_code)
    e.write_compact_nullable_string(message)
    e.write_i16(0) // topic config error
    e.write_i32(-1) // num partitions
    e.write_i16(-1) // replication factor
    e.write_compact_len(-1) // configs: null
    e.write_tag_buffer()
  }
  e.write_tag_buffer()
  e.to_bytes()
}

///|
/// Parse a DeleteTopics v6 request: (names, topic ids).
fn fake_delete_topics_request(
  rest : Bytes,
) -> (Array[String], Array[Bytes]) raise {
  let d = @buf.Decoder::new(rest)
  ignore(d.read_i16()) // api key
  ignore(d.read_i16()) // api version
  ignore(d.read_i32()) // correlation id
  ignore(d.read_compact_string()) // client id
  d.skip_tag_buffer()
  let n = d.read_compact_len()
  let names : Array[String] = []
  let ids : Array[Bytes] = []
  for _ in 0..<n {
    let name = d.read_compact_nullable_string()
    let id = d.read_bytes(16)
    d.skip_tag_buffer()
    match name {
      Some(name) => names.push(name)
      None => ids.push(id)
    }
  }
  ignore(d.read_i32()) // timeout
  (names, ids)
}

///|
/// DeleteTopics v6 response body.
fn fake_delete_topics_response(
  names : Array[String],
  ids : Array[Bytes],
) -> Bytes {
  let e = @buf.Encoder::new()
  e.write_i32(0) // throttle
  e.write_compact_len(names.length() + ids.length())
  for name in names {
    e.write_compact_nullable_string(Some(name))
    e.write_bytes(Bytes::make(16, 0))
    e.write_i16(0)
    e.write_compact_nullable_string(None)
    e.write_tag_buffer()
  }
  for id in ids {
    e.write_compact_nullable_string(None)
    e.write_bytes(id)
    e.write_i16(0)
    e.write_compact_nullable_string(None)
    e.write_tag_buffer()
  }
  e.write_tag_buffer()
  e.to_bytes()
}

///|
/// Parse a DeleteRecords v2 request: ((topic, partition, offset) triples,
/// timeout).
fn fake_delete_records_request(
  rest : Bytes,
) -> (Array[(String, Array[(Int, Int64)])], Int) raise {
  let d = @buf.Decoder::new(rest)
  ignore(d.read_i16()) // api key
  ignore(d.read_i16()) // api version
  ignore(d.read_i32()) // correlation id
  ignore(d.read_compact_string()) // client id
  d.skip_tag_buffer()
  let n = d.read_compact_len()
  let topics : Array[(String, Array[(Int, Int64)])] = []
  for _ in 0..<n {
    let name = d.read_compact_string()
    let parts = d.read_compact_len()
    let partitions : Array[(Int, Int64)] = []
    for _ in 0..<parts {
      let index = d.read_i32()
      let offset = d.read_i64()
      d.skip_tag_buffer()
      partitions.push((index, offset))
    }
    d.skip_tag_buffer()
    topics.push((name, partitions))
  }
  let timeout = d.read_i32()
  (topics, timeout)
}

///|
/// DeleteRecords v2 response body: per-partition low watermarks.
fn fake_delete_records_response(results : Array[(Int, Int64, Int)]) -> Bytes {
  let e = @buf.Encoder::new()
  e.write_i32(0) // throttle
  e.write_compact_len(1) // one topic
  e.write_compact_string("events")
  e.write_compact_len(results.length())
  for result in results {
    let (index, watermark, error_code) = result
    e.write_i32(index)
    e.write_i64(watermark)
    e.write_i16(error_code)
    e.write_tag_buffer()
  }
  e.write_tag_buffer()
  e.write_tag_buffer()
  e.to_bytes()
}

///|
/// Parse a CreatePartitions v3 request: (names, counts, timeout).
fn fake_create_partitions_request(
  rest : Bytes,
) -> (Array[(String, Int, Int)], Bool) raise {
  let d = @buf.Decoder::new(rest)
  ignore(d.read_i16()) // api key
  ignore(d.read_i16()) // api version
  ignore(d.read_i32()) // correlation id
  ignore(d.read_compact_string()) // client id
  d.skip_tag_buffer()
  let n = d.read_compact_len()
  let topics : Array[(String, Int, Int)] = []
  for _ in 0..<n {
    let name = d.read_compact_string()
    let count = d.read_i32()
    let assignments = d.read_compact_len()
    for _ in 0..<assignments {
      let brokers = d.read_compact_len()
      d.skip(brokers * 4)
      d.skip_tag_buffer()
    }
    d.skip_tag_buffer()
    topics.push((name, count, 0))
  }
  ignore(d.read_i32()) // timeout
  let validate_only = d.read_bool()
  (topics, validate_only)
}

///|
/// CreatePartitions v3 response body.
fn fake_create_partitions_response(
  results : Array[(String, Int, String?)],
) -> Bytes {
  let e = @buf.Encoder::new()
  e.write_i32(0) // throttle
  e.write_compact_len(results.length())
  for result in results {
    let (name, error_code, message) = result
    e.write_compact_string(name)
    e.write_i16(error_code)
    e.write_compact_nullable_string(message)
    e.write_tag_buffer()
  }
  e.write_tag_buffer()
  e.to_bytes()
}

///|
/// Parse an OffsetDelete v0 request (non-flexible): (group, (topic,
/// partition) pairs).
fn fake_offset_delete_request(
  rest : Bytes,
) -> (String, Array[(String, Array[Int])]) raise {
  let d = @buf.Decoder::new(rest)
  ignore(d.read_i16()) // api key
  ignore(d.read_i16()) // api version
  ignore(d.read_i32()) // correlation id
  let client_len = d.read_i16()
  ignore(d.read_bytes(client_len)) // client id (no tag buffer: v0 header)
  let group_len = d.read_i16()
  let group_id = @utf8.decode_lossy(d.read_bytes(group_len)[:])
  let n = d.read_i32()
  let topics : Array[(String, Array[Int])] = []
  for _ in 0..<n {
    let name_len = d.read_i16()
    let name = @utf8.decode_lossy(d.read_bytes(name_len)[:])
    let parts = d.read_i32()
    let partitions : Array[Int] = []
    for _ in 0..<parts {
      partitions.push(d.read_i32())
    }
    topics.push((name, partitions))
  }
  (group_id, topics)
}

///|
/// OffsetDelete v0 response body (non-flexible framing).
fn fake_offset_delete_response(results : Array[(String, Int, Int)]) -> Bytes {
  let e = @buf.Encoder::new()
  e.write_i16(0) // top-level error
  e.write_i32(0) // throttle
  e.write_i32(1) // one topic
  let name = @utf8.encode("events")
  e.write_i16(name.length())
  e.write_bytes(name)
  e.write_i32(results.length())
  for result in results {
    let (_, index, error_code) = result
    e.write_i32(index)
    e.write_i16(error_code)
  }
  e.to_bytes()
}

///|
/// Capture arrays and canned knobs for the cluster/config admin ops.
/// Handlers append what they receive so tests can assert on the wire
/// bytes they produced.
struct FakeClusterOps {
  describe_cluster : Array[Bool] // include-authorized-operations flags
  describe_configs : Array[(Int, String, Array[String]?)]
  incremental_alters : Array[(Int, String, Array[(String, Int, String?)])]
  alter_configs : Array[(Int, String, Array[(String, String?)])]
  list_config_resources : Array[Array[Int]] // requested type filters
  log_dirs : Array[Array[(String, Array[Int])]?] // requested topics
  elect_leaders : Array[(Int, Array[(String, Array[Int])]?)] // (type, topics)
  alter_reassignments : Array[(String, Int, Array[Int]?)] // (topic, index, replicas)
  list_reassignments : Array[Array[(String, Array[Int])]?]
  unregister_broker : Array[Int]
  quorum_requests : Array[(String, Int)]
  update_features : Array[(String, Int, Int, Bool)] // (feature, level, type, validate-only)
  mut update_features_error : Int
  describe_acls : Array[@moonkafka.AclFilter]
  create_acls : Array[@moonkafka.AclCreation]
  delete_acls : Array[@moonkafka.AclFilter]
  describe_quotas : Array[@moonkafka.ClientQuotaFilter]
  alter_quotas : Array[@moonkafka.ClientQuotaAlteration]
  alter_quotas_validate_only : Array[Bool]
  describe_scram : Array[Array[String]?]
  alter_scram_deletions : Array[@moonkafka.ScramCredentialDeletion]
  alter_scram_upsertions : Array[@moonkafka.ScramCredentialUpsertion]
}

///|
fn FakeClusterOps::new() -> FakeClusterOps {
  {
    describe_cluster: [],
    describe_configs: [],
    incremental_alters: [],
    alter_configs: [],
    list_config_resources: [],
    log_dirs: [],
    elect_leaders: [],
    alter_reassignments: [],
    list_reassignments: [],
    unregister_broker: [],
    quorum_requests: [],
    update_features: [],
    update_features_error: 0,
    describe_acls: [],
    create_acls: [],
    delete_acls: [],
    describe_quotas: [],
    alter_quotas: [],
    alter_quotas_validate_only: [],
    describe_scram: [],
    alter_scram_deletions: [],
    alter_scram_upsertions: [],
  }
}

///|
/// Flexible request header: skip api key/version/correlation/client id.
fn fake_skip_flexible_header(d : @buf.Decoder) -> Unit raise {
  ignore(d.read_i16()) // api key
  ignore(d.read_i16()) // api version
  ignore(d.read_i32()) // correlation id
  ignore(d.read_compact_string()) // client id
  d.skip_tag_buffer()
}

///|
/// Parse a DescribeCluster v0-v2 request: the include-operations flag
/// (the endpoint type is fixed at brokers, fenced brokers at v2+).
fn fake_describe_cluster_request(rest : Bytes) -> Bool raise {
  let d = @buf.Decoder::new(rest)
  fake_skip_flexible_header(d)
  d.read_bool()
}

///|
/// DescribeCluster v0-v2 response body: one broker (node 0), the fake
/// cluster id, node 0 as controller.
fn fake_describe_cluster_response(port : Int, version : Int) -> Bytes {
  let e = @buf.Encoder::new()
  e.write_i32(0) // throttle
  e.write_i16(0) // error
  e.write_compact_nullable_string(None) // error message
  if version >= 1 {
    e.write_i8(1) // endpoint type: brokers
  }
  e.write_compact_string("fake-cluster")
  e.write_i32(0) // controller id
  e.write_compact_len(1)
  e.write_i32(0) // broker id
  e.write_compact_string("127.0.0.1")
  e.write_i32(port)
  e.write_compact_nullable_string(None) // rack
  if version >= 2 {
    e.write_bool(false) // is_fenced
  }
  e.write_tag_buffer()
  e.write_i32(-2147483648) // cluster authorized operations
  e.write_tag_buffer()
  e.to_bytes()
}

///|
/// Parse a DescribeConfigs v4 request: (type, name, keys-or-null) per
/// resource plus the include flags.
fn fake_describe_configs_request(
  rest : Bytes,
) -> (Array[(Int, String, Array[String]?)], Bool, Bool) raise {
  let d = @buf.Decoder::new(rest)
  fake_skip_flexible_header(d)
  let resources : Array[(Int, String, Array[String]?)] = []
  let n = d.read_compact_len()
  for _ in 0..<n {
    let resource_type = d.read_i8()
    let resource_name = d.read_compact_string()
    // Null arrays carry the compact-length marker 0 (read back as -1).
    let keys : Array[String]? = match d.read_compact_len() {
      -1 => None
      count => {
        let out : Array[String] = []
        for _ in 0..<count {
          out.push(d.read_compact_string())
        }
        Some(out)
      }
    }
    d.skip_tag_buffer()
    resources.push((resource_type, resource_name, keys))
  }
  let include_synonyms = d.read_bool()
  let include_documentation = d.read_bool()
  (resources, include_synonyms, include_documentation)
}

///|
/// DescribeConfigs v4 response body: per resource, one config entry per
/// requested key (or two canned entries for the null key filter).
fn fake_describe_configs_response(
  resources : Array[(Int, String, Array[String]?)],
) -> Bytes {
  let e = @buf.Encoder::new()
  e.write_i32(0) // throttle
  e.write_compact_len(resources.length())
  for resource in resources {
    let (resource_type, resource_name, keys) = resource
    e.write_i16(0) // error
    e.write_compact_nullable_string(None)
    e.write_i8(resource_type)
    e.write_compact_string(resource_name)
    let names : Array[String] = match keys {
      Some(keys) => keys
      None => ["retention.ms", "cleanup.policy"]
    }
    e.write_compact_len(names.length())
    for name in names {
      e.write_compact_string(name)
      e.write_compact_nullable_string(Some("604800000"))
      e.write_bool(false) // read only
      e.write_i8(2) // config source
      e.write_bool(false) // sensitive
      e.write_compact_len(0) // synonyms
      e.write_i8(2) // config type
      e.write_compact_nullable_string(None) // documentation
      e.write_tag_buffer()
    }
    e.write_tag_buffer()
  }
  e.write_tag_buffer()
  e.to_bytes()
}

///|
/// Parse an IncrementalAlterConfigs v1 request: (type, name, (key, op,
/// value)) per resource plus the validate-only flag.
fn fake_incremental_alter_configs_request(
  rest : Bytes,
) -> (Array[(Int, String, Array[(String, Int, String?)])], Bool) raise {
  let d = @buf.Decoder::new(rest)
  fake_skip_flexible_header(d)
  let resources : Array[(Int, String, Array[(String, Int, String?)])] = []
  let n = d.read_compact_len()
  for _ in 0..<n {
    let resource_type = d.read_i8()
    let resource_name = d.read_compact_string()
    let configs : Array[(String, Int, String?)] = []
    let count = d.read_compact_len()
    for _ in 0..<count {
      let name = d.read_compact_string()
      let operation = d.read_i8()
      let value = d.read_compact_nullable_string()
      d.skip_tag_buffer()
      configs.push((name, operation, value))
    }
    d.skip_tag_buffer()
    resources.push((resource_type, resource_name, configs))
  }
  let validate_only = d.read_bool()
  (resources, validate_only)
}

///|
/// Parse an AlterConfigs v2 request: (type, name, (key, value)) per
/// resource plus the validate-only flag.
fn fake_alter_configs_request(
  rest : Bytes,
) -> (Array[(Int, String, Array[(String, String?)])], Bool) raise {
  let d = @buf.Decoder::new(rest)
  fake_skip_flexible_header(d)
  let resources : Array[(Int, String, Array[(String, String?)])] = []
  let n = d.read_compact_len()
  for _ in 0..<n {
    let resource_type = d.read_i8()
    let resource_name = d.read_compact_string()
    let configs : Array[(String, String?)] = []
    let count = d.read_compact_len()
    for _ in 0..<count {
      let name = d.read_compact_string()
      let value = d.read_compact_nullable_string()
      d.skip_tag_buffer()
      configs.push((name, value))
    }
    d.skip_tag_buffer()
    resources.push((resource_type, resource_name, configs))
  }
  let validate_only = d.read_bool()
  (resources, validate_only)
}

///|
/// AlterConfigs/IncrementalAlterConfigs response body: per-resource
/// success echoing (type, name).
fn fake_alter_configs_response(resources : Array[(Int, String)]) -> Bytes {
  let e = @buf.Encoder::new()
  e.write_i32(0) // throttle
  e.write_compact_len(resources.length())
  for resource in resources {
    let (resource_type, resource_name) = resource
    e.write_i16(0) // error
    e.write_compact_nullable_string(None)
    e.write_i8(resource_type)
    e.write_compact_string(resource_name)
    e.write_tag_buffer()
  }
  e.write_tag_buffer()
  e.to_bytes()
}

///|
/// Parse a ListConfigResources v0/v1 request: the requested type filter
/// (empty at v0, which carries no filter field).
fn fake_list_config_resources_request(rest : Bytes) -> Array[Int] raise {
  let d = @buf.Decoder::new(rest)
  fake_skip_flexible_header(d)
  let version = rest[3].to_int()
  let types : Array[Int] = []
  if version >= 1 {
    let n = d.read_compact_len()
    for _ in 0..<n {
      types.push(d.read_i8())
    }
  }
  types
}

///|
/// ListConfigResources v0/v1 response body: a topic and a broker
/// resource (types only ride v1+).
fn fake_list_config_resources_response(version : Int) -> Bytes {
  let e = @buf.Encoder::new()
  e.write_i32(0) // throttle
  e.write_i16(0) // error
  e.write_compact_len(2)
  e.write_compact_string("events")
  if version >= 1 {
    e.write_i8(2) // topic
  }
  e.write_tag_buffer()
  e.write_compact_string("1")
  if version >= 1 {
    e.write_i8(4) // broker
  }
  e.write_tag_buffer()
  e.write_tag_buffer()
  e.to_bytes()
}

///|
/// Parse a DescribeLogDirs request: the requested topics (None = all).
fn fake_describe_log_dirs_request(
  rest : Bytes,
) -> Array[(String, Array[Int])]? raise {
  let d = @buf.Decoder::new(rest)
  fake_skip_flexible_header(d)
  // Null arrays carry the compact-length marker 0 (read back as -1).
  let topics : Array[(String, Array[Int])]? = match d.read_compact_len() {
    -1 => None
    n => {
      let out : Array[(String, Array[Int])] = []
      for _ in 0..<n {
        let name = d.read_compact_string()
        let parts = d.read_compact_len()
        let partitions : Array[Int] = []
        for _ in 0..<parts {
          partitions.push(d.read_i32())
        }
        d.skip_tag_buffer()
        out.push((name, partitions))
      }
      Some(out)
    }
  }
  topics
}

///|
/// DescribeLogDirs v2-v5 response body: one log dir holding the
/// requested topics (or both fake partitions of "events" when the
/// request asked for all), sizes echoing the partition index.
fn fake_describe_log_dirs_response(
  topics : Array[(String, Array[Int])],
  version : Int,
) -> Bytes {
  let e = @buf.Encoder::new()
  e.write_i32(0) // throttle
  if version >= 3 {
    e.write_i16(0) // top-level error
  }
  e.write_compact_len(1) // one log dir
  e.write_i16(0) // error
  e.write_compact_string("/tmp/kafka-logs")
  e.write_compact_len(topics.length())
  for entry in topics {
    let (name, partitions) = entry
    e.write_compact_string(name)
    e.write_compact_len(partitions.length())
    for partition in partitions {
      e.write_i32(partition)
      e.write_i64(100L + partition.to_int64()) // size
      e.write_i64(0L) // lag
      e.write_bool(false) // future
      e.write_tag_buffer()
    }
    e.write_tag_buffer()
  }
  if version >= 4 {
    e.write_i64(1000000L) // total bytes
    e.write_i64(500000L) // usable bytes
  }
  if version >= 5 {
    e.write_bool(false) // is_cordoned
  }
  e.write_tag_buffer()
  e.write_tag_buffer()
  e.to_bytes()
}

///|
/// Parse an ElectLeaders v2 request: (election type, topics-or-null,
/// timeout).
fn fake_elect_leaders_request(
  rest : Bytes,
) -> (Int, Array[(String, Array[Int])]?) raise {
  let d = @buf.Decoder::new(rest)
  fake_skip_flexible_header(d)
  let election_type = d.read_i8()
  // Null arrays carry the compact-length marker 0 (read back as -1).
  let topics : Array[(String, Array[Int])]? = match d.read_compact_len() {
    -1 => None
    n => {
      let out : Array[(String, Array[Int])] = []
      for _ in 0..<n {
        let name = d.read_compact_string()
        let parts = d.read_compact_len()
        let partitions : Array[Int] = []
        for _ in 0..<parts {
          partitions.push(d.read_i32())
        }
        d.skip_tag_buffer()
        out.push((name, partitions))
      }
      Some(out)
    }
  }
  ignore(d.read_i32()) // timeout
  (election_type, topics)
}

///|
/// ElectLeaders v2 response body: per-partition success for the
/// requested (or canned all) partitions.
fn fake_elect_leaders_response(topics : Array[(String, Array[Int])]) -> Bytes {
  let e = @buf.Encoder::new()
  e.write_i32(0) // throttle
  e.write_i16(0) // top-level error
  e.write_compact_len(topics.length())
  for entry in topics {
    let (name, partitions) = entry
    e.write_compact_string(name)
    e.write_compact_len(partitions.length())
    for partition in partitions {
      e.write_i32(partition)
      e.write_i16(0) // error
      e.write_compact_nullable_string(None)
      e.write_tag_buffer()
    }
    e.write_tag_buffer()
  }
  e.write_tag_buffer()
  e.to_bytes()
}

///|
/// Parse an AlterPartitionReassignments request: (topic, partition,
/// replicas-or-null) triples.
fn fake_alter_reassignments_request(
  rest : Bytes,
) -> Array[(String, Int, Array[Int]?)] raise {
  let d = @buf.Decoder::new(rest)
  fake_skip_flexible_header(d)
  ignore(d.read_i32()) // timeout
  if rest[3].to_int() >= 1 {
    ignore(d.read_bool()) // allow replication factor change (v1+)
  }
  let out : Array[(String, Int, Array[Int]?)] = []
  let n = d.read_compact_len()
  for _ in 0..<n {
    let name = d.read_compact_string()
    let count = d.read_compact_len()
    for _ in 0..<count {
      let index = d.read_i32()
      // Null arrays carry the compact-length marker 0 (read back as -1).
      let replicas : Array[Int]? = match d.read_compact_len() {
        -1 => None
        parts => {
          let ids : Array[Int] = []
          for _ in 0..<parts {
            ids.push(d.read_i32())
          }
          Some(ids)
        }
      }
      d.skip_tag_buffer()
      out.push((name, index, replicas))
    }
    d.skip_tag_buffer()
  }
  out
}

///|
/// AlterPartitionReassignments response body: success per (topic,
/// partition, message-null).
fn fake_alter_reassignments_response(
  reassignments : Array[(String, Int, Array[Int]?)],
) -> Bytes {
  let e = @buf.Encoder::new()
  e.write_i32(0) // throttle
  e.write_bool(true) // allow replication factor change echo (v1+)
  e.write_i16(0) // top-level error
  e.write_compact_nullable_string(None)
  let by_topic : Map[String, Array[Int]] = Map([])
  let order : Array[String] = []
  for entry in reassignments {
    let (name, index, _) = entry
    match by_topic.get(name) {
      Some(indexes) => indexes.push(index)
      None => {
        by_topic[name] = [index]
        order.push(name)
      }
    }
  }
  e.write_compact_len(order.length())
  for name in order {
    e.write_compact_string(name)
    let indexes = by_topic[name]
    e.write_compact_len(indexes.length())
    for index in indexes {
      e.write_i32(index)
      e.write_i16(0) // error
      e.write_compact_nullable_string(None)
      e.write_tag_buffer()
    }
    e.write_tag_buffer()
  }
  e.write_tag_buffer()
  e.to_bytes()
}

///|
/// Parse a ListPartitionReassignments request: the requested topic
/// filter (None = all).
fn fake_list_reassignments_request(
  rest : Bytes,
) -> Array[(String, Array[Int])]? raise {
  let d = @buf.Decoder::new(rest)
  fake_skip_flexible_header(d)
  ignore(d.read_i32()) // timeout
  // Null arrays carry the compact-length marker 0 (read back as -1).
  let topics : Array[(String, Array[Int])]? = match d.read_compact_len() {
    -1 => None
    n => {
      let out : Array[(String, Array[Int])] = []
      for _ in 0..<n {
        let name = d.read_compact_string()
        let parts = d.read_compact_len()
        let partitions : Array[Int] = []
        for _ in 0..<parts {
          partitions.push(d.read_i32())
        }
        d.skip_tag_buffer()
        out.push((name, partitions))
      }
      Some(out)
    }
  }
  topics
}

///|
/// ListPartitionReassignments response body: one ongoing reassignment
/// of "events" partition 0 (empty when the request filters others out).
fn fake_list_reassignments_response(
  filter : Array[(String, Array[Int])]?,
) -> Bytes {
  let e = @buf.Encoder::new()
  e.write_i32(0) // throttle
  e.write_i16(0) // top-level error
  e.write_compact_nullable_string(None)
  let wanted = match filter {
    None => true
    Some(topics) => topics.get(0) is Some((name, _)) && name == "events"
  }
  if wanted {
    e.write_compact_len(1)
    e.write_compact_string("events")
    e.write_compact_len(1)
    e.write_i32(0) // partition
    e.write_compact_len(2)
    e.write_i32(1)
    e.write_i32(2)
    e.write_compact_len(1)
    e.write_i32(2)
    e.write_compact_len(1)
    e.write_i32(1)
    e.write_tag_buffer()
    e.write_tag_buffer()
  } else {
    e.write_compact_len(0)
  }
  e.write_tag_buffer()
  e.to_bytes()
}

///|
/// Parse an UnregisterBroker request: the broker id.
fn fake_unregister_broker_request(rest : Bytes) -> Int raise {
  let d = @buf.Decoder::new(rest)
  fake_skip_flexible_header(d)
  d.read_i32()
}

///|
/// UnregisterBroker response body.
fn fake_unregister_broker_response() -> Bytes {
  let e = @buf.Encoder::new()
  e.write_i32(0) // throttle
  e.write_i16(0) // error
  e.write_compact_nullable_string(None)
  e.write_tag_buffer()
  e.to_bytes()
}

///|
/// Parse a DescribeQuorum request: (topic, partition) pairs.
fn fake_describe_quorum_request(rest : Bytes) -> Array[(String, Int)] raise {
  let d = @buf.Decoder::new(rest)
  fake_skip_flexible_header(d)
  let out : Array[(String, Int)] = []
  let n = d.read_compact_len()
  for _ in 0..<n {
    let name = d.read_compact_string()
    let count = d.read_compact_len()
    for _ in 0..<count {
      let index = d.read_i32()
      d.skip_tag_buffer()
      out.push((name, index))
    }
    d.skip_tag_buffer()
  }
  out
}

///|
/// DescribeQuorum v2 response body: node 0 leading with one voter
/// (itself), plus the quorum node/listener map. No throttle field in
/// this API.
fn fake_describe_quorum_response(requests : Array[(String, Int)]) -> Bytes {
  let e = @buf.Encoder::new()
  e.write_i16(0) // top-level error
  e.write_compact_nullable_string(None)
  e.write_compact_len(requests.length())
  for entry in requests {
    let (name, index) = entry
    e.write_compact_string(name)
    e.write_compact_len(1)
    e.write_i32(index)
    e.write_i16(0) // error
    e.write_compact_nullable_string(None)
    e.write_i32(0) // leader id
    e.write_i32(5) // leader epoch
    e.write_i64(42L) // high watermark
    e.write_compact_len(1) // voters
    e.write_i32(0) // replica id
    e.write_bytes(Bytes::make(16, 0)) // directory id
    e.write_i64(42L) // log end offset
    e.write_i64(-1L) // last fetch
    e.write_i64(-1L) // last caught up
    e.write_tag_buffer()
    e.write_compact_len(0) // observers
    e.write_tag_buffer()
    e.write_tag_buffer()
  }
  e.write_compact_len(1) // nodes
  e.write_i32(0) // node id
  e.write_compact_len(1)
  e.write_compact_string("CONTROLLER")
  e.write_compact_string("127.0.0.1")
  e.write_i16(9093) // port (uint16)
  e.write_tag_buffer()
  e.write_tag_buffer()
  e.write_tag_buffer()
  e.to_bytes()
}

///|
/// Parse an UpdateFeatures v2 request: (feature, level, upgrade type,
/// validate-only).
fn fake_update_features_request(
  rest : Bytes,
) -> (Array[(String, Int, Int)], Bool) raise {
  let d = @buf.Decoder::new(rest)
  fake_skip_flexible_header(d)
  ignore(d.read_i32()) // timeout
  let updates : Array[(String, Int, Int)] = []
  let n = d.read_compact_len()
  for _ in 0..<n {
    let feature = d.read_compact_string()
    let level = d.read_i16()
    let upgrade_type = d.read_i8()
    d.skip_tag_buffer()
    updates.push((feature, level, upgrade_type))
  }
  let validate_only = d.read_bool()
  (updates, validate_only)
}

///|
/// UpdateFeatures v2 response body: top-level error only (v2 dropped
/// the per-feature results).
fn fake_update_features_response(error_code : Int) -> Bytes {
  let e = @buf.Encoder::new()
  e.write_i32(0) // throttle
  e.write_i16(error_code)
  e.write_compact_nullable_string(None)
  e.write_tag_buffer()
  e.to_bytes()
}

///|
/// Parse one AclFilter from the current decoder position.
fn fake_acl_filter(d : @buf.Decoder) -> @moonkafka.AclFilter raise {
  let resource_type = d.read_i8()
  let resource_name = d.read_compact_nullable_string()
  let pattern_type = d.read_i8()
  let principal = d.read_compact_nullable_string()
  let host = d.read_compact_nullable_string()
  let operation = d.read_i8()
  let permission_type = d.read_i8()
  d.skip_tag_buffer()
  {
    resource_type,
    resource_name,
    pattern_type,
    principal,
    host,
    operation,
    permission_type,
  }
}

///|
/// Parse a DescribeAcls request: one filter.
fn fake_describe_acls_request(rest : Bytes) -> @moonkafka.AclFilter raise {
  let d = @buf.Decoder::new(rest)
  fake_skip_flexible_header(d)
  fake_acl_filter(d)
}

///|
/// Parse a CreateAcls request: the creations.
fn fake_create_acls_request(
  rest : Bytes,
) -> Array[@moonkafka.AclCreation] raise {
  let d = @buf.Decoder::new(rest)
  fake_skip_flexible_header(d)
  let out : Array[@moonkafka.AclCreation] = []
  let n = d.read_compact_len()
  for _ in 0..<n {
    let resource_type = d.read_i8()
    let resource_name = d.read_compact_string()
    let pattern_type = d.read_i8()
    let principal = d.read_compact_string()
    let host = d.read_compact_string()
    let operation = d.read_i8()
    let permission_type = d.read_i8()
    d.skip_tag_buffer()
    out.push({
      resource_type,
      resource_name,
      pattern_type,
      principal,
      host,
      operation,
      permission_type,
    })
  }
  out
}

///|
/// Parse a DeleteAcls request: the filters.
fn fake_delete_acls_request(rest : Bytes) -> Array[@moonkafka.AclFilter] raise {
  let d = @buf.Decoder::new(rest)
  fake_skip_flexible_header(d)
  let out : Array[@moonkafka.AclFilter] = []
  let n = d.read_compact_len()
  for _ in 0..<n {
    out.push(fake_acl_filter(d))
  }
  out
}

///|
/// DescribeAcls response body: the "events" topic (literal) carrying
/// one ALLOW/READ ACL for User:alice.
fn fake_describe_acls_response() -> Bytes {
  let e = @buf.Encoder::new()
  e.write_i32(0) // throttle
  e.write_i16(0) // error
  e.write_compact_nullable_string(None)
  e.write_compact_len(1) // one resource
  e.write_i8(2) // topic
  e.write_compact_string("events")
  e.write_i8(3) // literal
  e.write_compact_len(1) // one acl
  e.write_compact_string("User:alice")
  e.write_compact_string("*")
  e.write_i8(3) // read
  e.write_i8(3) // allow
  e.write_tag_buffer()
  e.write_tag_buffer()
  e.write_tag_buffer()
  e.to_bytes()
}

///|
/// CreateAcls response body: success per creation.
fn fake_create_acls_response(count : Int) -> Bytes {
  let e = @buf.Encoder::new()
  e.write_i32(0) // throttle
  e.write_compact_len(count)
  for _ in 0..<count {
    e.write_i16(0) // error
    e.write_compact_nullable_string(None)
    e.write_tag_buffer()
  }
  e.write_tag_buffer()
  e.to_bytes()
}

///|
/// DeleteAcls response body: per filter, the canned "events" ACL as the
/// only match (deleted successfully).
fn fake_delete_acls_response(count : Int) -> Bytes {
  let e = @buf.Encoder::new()
  e.write_i32(0) // throttle
  e.write_compact_len(count)
  for _ in 0..<count {
    e.write_i16(0) // filter error
    e.write_compact_nullable_string(None)
    e.write_compact_len(1) // one matching acl
    e.write_i16(0) // deletion error
    e.write_compact_nullable_string(None)
    e.write_i8(2) // topic
    e.write_compact_string("events")
    e.write_i8(3) // literal
    e.write_compact_string("User:alice")
    e.write_compact_string("*")
    e.write_i8(3) // read
    e.write_i8(3) // allow
    e.write_tag_buffer()
    e.write_tag_buffer()
  }
  e.write_tag_buffer()
  e.to_bytes()
}

///|
/// Quota/SCRAM request and response shapes: the quota APIs negotiate
/// v0 (legacy lengths, no tag buffers) or v1 (compact + tag buffers),
/// so their fake helpers shape by version like the real codecs.
fn fake_quotas_len(d : @buf.Decoder, version : Int) -> Int raise {
  if version >= 1 {
    d.read_compact_len()
  } else {
    d.read_i32()
  }
}

///|
fn fake_write_quotas_len(e : @buf.Encoder, n : Int, version : Int) -> Unit {
  if version >= 1 {
    e.write_compact_len(n)
  } else {
    e.write_i32(n)
  }
}

///|
fn fake_quotas_string(d : @buf.Decoder, version : Int) -> String raise {
  if version >= 1 {
    d.read_compact_string()
  } else {
    let len = d.read_i16()
    @utf8.decode_lossy(d.read_bytes(len)[:])
  }
}

///|
fn fake_quotas_nullable_string(
  d : @buf.Decoder,
  version : Int,
) -> String? raise {
  if version >= 1 {
    d.read_compact_nullable_string()
  } else {
    let len = d.read_i16()
    if len < 0 {
      None
    } else {
      Some(@utf8.decode_lossy(d.read_bytes(len)[:]))
    }
  }
}

///|
fn fake_write_quotas_string(
  e : @buf.Encoder,
  s : String,
  version : Int,
) -> Unit {
  if version >= 1 {
    e.write_compact_string(s)
  } else {
    let b = @utf8.encode(s)
    e.write_i16(b.length())
    e.write_bytes(b)
  }
}

///|
fn fake_write_quotas_nullable_string(
  e : @buf.Encoder,
  s : String?,
  version : Int,
) -> Unit {
  if version >= 1 {
    e.write_compact_nullable_string(s)
  } else {
    e.write_nullable_string(s)
  }
}

///|
fn fake_write_quotas_tag(e : @buf.Encoder, version : Int) -> Unit {
  if version >= 1 {
    e.write_tag_buffer()
  }
}

///|
/// Skip the request header: v0 has the legacy shape (no tag buffer).
fn fake_skip_quotas_header(d : @buf.Decoder, version : Int) -> Unit raise {
  ignore(d.read_i16()) // api key
  ignore(d.read_i16()) // api version
  ignore(d.read_i32()) // correlation id
  if version >= 1 {
    ignore(d.read_compact_string()) // client id
    d.skip_tag_buffer()
  } else {
    let client_len = d.read_i16()
    ignore(d.read_bytes(client_len)) // client id
  }
}

///|
/// Skip a struct's tag buffer: none at v0, one at v1+.
fn fake_skip_quotas_tag(d : @buf.Decoder, version : Int) -> Unit raise {
  if version >= 1 {
    d.skip_tag_buffer()
  }
}

///|
/// Parse a DescribeClientQuotas v0-v1 request: the filter.
fn fake_describe_client_quotas_request(
  rest : Bytes,
  version : Int,
) -> @moonkafka.ClientQuotaFilter raise {
  let d = @buf.Decoder::new(rest)
  fake_skip_quotas_header(d, version)
  let n = fake_quotas_len(d, version)
  let components : Array[@moonkafka.ClientQuotaFilterComponent] = []
  for _ in 0..<n {
    let entity_type = fake_quotas_string(d, version)
    let match_type = d.read_i8()
    let matches = fake_quotas_nullable_string(d, version)
    fake_skip_quotas_tag(d, version)
    components.push({ entity_type, match_type, matches, })
  }
  let strict = d.read_bool()
  { components, strict, }
}

///|
/// DescribeClientQuotas response body: one entry for the user "alice"
/// with the two rate quotas set.
fn fake_describe_client_quotas_response(version : Int) -> Bytes {
  let e = @buf.Encoder::new()
  e.write_i32(0) // throttle
  e.write_i16(0) // error
  fake_write_quotas_nullable_string(e, None, version)
  fake_write_quotas_len(e, 1, version) // one entry
  fake_write_quotas_len(e, 1, version) // one entity
  fake_write_quotas_string(e, "user", version)
  fake_write_quotas_nullable_string(e, Some("alice"), version)
  fake_write_quotas_tag(e, version)
  fake_write_quotas_len(e, 2, version) // two values
  fake_write_quotas_string(e, "producer_byte_rate", version)
  e.write_f64(1024.0)
  fake_write_quotas_tag(e, version)
  fake_write_quotas_string(e, "consumer_byte_rate", version)
  e.write_f64(2048.0)
  fake_write_quotas_tag(e, version)
  fake_write_quotas_tag(e, version)
  fake_write_quotas_tag(e, version)
  e.to_bytes()
}

///|
/// Parse an AlterClientQuotas v0-v1 request: the alterations and the
/// validate-only flag.
fn fake_alter_client_quotas_request(
  rest : Bytes,
  version : Int,
) -> (Array[@moonkafka.ClientQuotaAlteration], Bool) raise {
  let d = @buf.Decoder::new(rest)
  fake_skip_quotas_header(d, version)
  let n = fake_quotas_len(d, version)
  let out : Array[@moonkafka.ClientQuotaAlteration] = []
  for _ in 0..<n {
    let entity_len = fake_quotas_len(d, version)
    let entity : Array[@moonkafka.QuotaEntity] = []
    for _ in 0..<entity_len {
      let entity_type = fake_quotas_string(d, version)
      let entity_name = fake_quotas_nullable_string(d, version)
      fake_skip_quotas_tag(d, version)
      entity.push({ entity_type, entity_name, })
    }
    let ops_len = fake_quotas_len(d, version)
    let ops : Array[@moonkafka.ClientQuotaOp] = []
    for _ in 0..<ops_len {
      let key = fake_quotas_string(d, version)
      let value = d.read_f64()
      let remove = d.read_bool()
      fake_skip_quotas_tag(d, version)
      ops.push({ key, value, remove, })
    }
    fake_skip_quotas_tag(d, version)
    out.push({ entity, ops, })
  }
  let validate_only = d.read_bool()
  (out, validate_only)
}

///|
/// AlterClientQuotas response body: success per alteration.
fn fake_alter_client_quotas_response(
  entries : Array[@moonkafka.ClientQuotaAlteration],
  version : Int,
) -> Bytes {
  let e = @buf.Encoder::new()
  e.write_i32(0) // throttle
  fake_write_quotas_len(e, entries.length(), version)
  for entry in entries {
    e.write_i16(0) // error
    fake_write_quotas_nullable_string(e, None, version)
    fake_write_quotas_len(e, entry.entity.length(), version)
    for part in entry.entity {
      fake_write_quotas_string(e, part.entity_type, version)
      fake_write_quotas_nullable_string(e, part.entity_name, version)
      fake_write_quotas_tag(e, version)
    }
    fake_write_quotas_tag(e, version)
  }
  fake_write_quotas_tag(e, version)
  e.to_bytes()
}

///|
/// Parse a DescribeUserScramCredentials v0 request: the user names, or
/// None for "all users" (a null array).
fn fake_describe_user_scram_request(rest : Bytes) -> Array[String]? raise {
  let d = @buf.Decoder::new(rest)
  fake_skip_flexible_header(d)
  let n = d.read_compact_len()
  if n < 0 {
    None
  } else {
    let users : Array[String] = []
    for _ in 0..<n {
      users.push(d.read_compact_string())
      d.skip_tag_buffer()
    }
    Some(users)
  }
}

///|
/// DescribeUserScramCredentials response body: "alice" with one
/// SCRAM-SHA-512 credential at 8192 iterations.
fn fake_describe_user_scram_response() -> Bytes {
  let e = @buf.Encoder::new()
  e.write_i32(0) // throttle
  e.write_i16(0) // message-level error
  e.write_compact_nullable_string(None)
  e.write_compact_len(1) // one user
  e.write_compact_string("alice")
  e.write_i16(0) // user-level error
  e.write_compact_nullable_string(None)
  e.write_compact_len(1) // one credential
  e.write_i8(2) // SCRAM-SHA-512
  e.write_i32(8192) // iterations
  e.write_tag_buffer()
  e.write_tag_buffer()
  e.write_tag_buffer()
  e.to_bytes()
}

///|
/// Parse an AlterUserScramCredentials v0 request: the deletions and
/// upsertions.
fn fake_alter_user_scram_request(
  rest : Bytes,
) -> (
  Array[@moonkafka.ScramCredentialDeletion],
  Array[@moonkafka.ScramCredentialUpsertion],
) raise {
  let d = @buf.Decoder::new(rest)
  fake_skip_flexible_header(d)
  let deletions : Array[@moonkafka.ScramCredentialDeletion] = []
  let dn = d.read_compact_len()
  for _ in 0..<dn {
    let name = d.read_compact_string()
    let mechanism = d.read_i8()
    d.skip_tag_buffer()
    deletions.push({ name, mechanism, })
  }
  let upsertions : Array[@moonkafka.ScramCredentialUpsertion] = []
  let un = d.read_compact_len()
  for _ in 0..<un {
    let name = d.read_compact_string()
    let mechanism = d.read_i8()
    let iterations = d.read_i32()
    let salt_len = d.read_compact_len()
    let salt = d.read_bytes(salt_len)
    let salted_len = d.read_compact_len()
    let salted_password = d.read_bytes(salted_len)
    d.skip_tag_buffer()
    upsertions.push({ name, mechanism, iterations, salt, salted_password, })
  }
  (deletions, upsertions)
}

///|
/// AlterUserScramCredentials response body: success per deletion and
/// upsertion, keyed by user name.
fn fake_alter_user_scram_response(
  deletions : Array[@moonkafka.ScramCredentialDeletion],
  upsertions : Array[@moonkafka.ScramCredentialUpsertion],
) -> Bytes {
  let e = @buf.Encoder::new()
  e.write_i32(0) // throttle
  e.write_compact_len(deletions.length() + upsertions.length())
  for deletion in deletions {
    e.write_compact_string(deletion.name)
    e.write_i16(0) // error
    e.write_compact_nullable_string(None)
    e.write_tag_buffer()
  }
  for upsertion in upsertions {
    e.write_compact_string(upsertion.name)
    e.write_i16(0) // error
    e.write_compact_nullable_string(None)
    e.write_tag_buffer()
  }
  e.write_tag_buffer()
  e.to_bytes()
}

///|
/// Parse a ConsumerGroupHeartbeat v1 request: (group, member id, epoch,
/// instance id, subscribed regex).
fn fake_consumer_group_heartbeat_request(
  rest : Bytes,
) -> (String, String, Int, String?, String?) raise {
  let d = @buf.Decoder::new(rest)
  ignore(d.read_i16()) // api key
  ignore(d.read_i16()) // api version
  ignore(d.read_i32()) // correlation id
  ignore(d.read_compact_string()) // client id
  d.skip_tag_buffer()
  let group_id = d.read_compact_string()
  let member_id = d.read_compact_string()
  let member_epoch = d.read_i32()
  let instance_id = d.read_compact_nullable_string()
  ignore(d.read_compact_nullable_string()) // rack id
  ignore(d.read_i32()) // rebalance timeout
  let names = d.read_compact_len()
  for _ in 0..<names {
    ignore(d.read_compact_string())
  }
  let regex = d.read_compact_nullable_string()
  ignore(d.read_compact_nullable_string()) // server assignor
  ignore(d.read_i8()) // owned topic partitions: null
  (group_id, member_id, member_epoch, instance_id, regex)
}

///|
/// ConsumerGroupHeartbeat response body: the confirmed identity, the
/// next epoch, a 20ms heartbeat interval, and the configured assignment.
fn fake_consumer_group_heartbeat_response(
  error_code : Int,
  member_id : String,
  member_epoch : Int,
  partitions : Array[Int],
) -> Bytes {
  let e = @buf.Encoder::new()
  e.write_i32(0) // throttle
  e.write_i16(error_code)
  e.write_compact_nullable_string(None) // error message
  e.write_compact_string(member_id)
  e.write_i32(member_epoch)
  e.write_i32(20) // heartbeat interval
  if error_code == 0 && !partitions.is_empty() {
    e.write_i8(1) // assignment present
    e.write_compact_len(1) // one topic
    e.write_bytes(fake_topic_id)
    e.write_compact_len(partitions.length())
    for p in partitions {
      e.write_i32(p)
    }
    e.write_tag_buffer()
    e.write_tag_buffer()
  } else {
    e.write_i8(-1) // assignment null
  }
  e.write_tag_buffer()
  e.to_bytes()
}

///|
/// Isolation level of a Fetch request (0 read_uncommitted, 1
/// read_committed).
fn fake_fetch_request_isolation(rest : Bytes) -> Int raise {
  let d = @buf.Decoder::new(rest)
  ignore(d.read_i16()) // api key
  ignore(d.read_i16()) // api version
  ignore(d.read_i32()) // correlation id
  ignore(d.read_compact_string()) // client id
  d.skip_tag_buffer()
  ignore(d.read_i32()) // max_wait_ms
  ignore(d.read_i32()) // min_bytes
  ignore(d.read_i32()) // max_bytes
  d.read_i8() // isolation level
}

///|
/// A READ_COMMITTED fetch response: the two records ride in a
/// transactional batch from producer 500, reported aborted from offset
/// 42 — read_committed consumers must filter both out.
fn fake_fetch_response_committed(
  topic : String,
  version : Int,
  session_id : Int,
) -> Bytes {
  let builder = @moonkafka.RecordBatchBuilder::new(base_offset=42L)
  builder.set_idempotence(500L, 0, 0)
  builder.set_transactional()
  builder.append(1000L, value=Some(b"one"))
  builder.append(2000L, value=Some(b"two"))
  let batch = builder.to_bytes()
  let e = @buf.Encoder::new()
  e.write_i32(0) // throttle
  e.write_i16(0) // top-level error
  e.write_i32(session_id)
  e.write_compact_len(1) // one topic
  if version >= 13 {
    e.write_bytes(fake_topic_id)
  } else {
    e.write_compact_string(topic)
  }
  e.write_compact_len(1) // one partition
  e.write_i32(0) // index
  e.write_i16(0) // no error
  e.write_i64(100L) // high watermark
  e.write_i64(44L) // last stable offset
  e.write_i64(-1L) // log start offset
  e.write_compact_len(1) // aborted transactions
  e.write_i64(500L) // producer id
  e.write_i64(42L) // first offset
  e.write_tag_buffer()
  e.write_i32(-1) // preferred read replica
  e.write_compact_len(batch.length()) // records
  e.write_bytes(batch)
  e.write_tag_buffer()
  e.write_tag_buffer()
  e.write_tag_buffer()
  e.to_bytes()
}

///|
/// OffsetForLeaderEpoch v4 response body: one partition at the given
/// epoch end offset.
fn fake_offset_for_leader_epoch_response(end_offset : Int64) -> Bytes {
  let e = @buf.Encoder::new()
  e.write_i32(0) // throttle
  e.write_compact_len(1) // one topic
  e.write_compact_string("events")
  e.write_compact_len(1) // one partition
  e.write_i16(0) // no error
  e.write_i32(0) // partition
  e.write_i32(0) // leader epoch
  e.write_i64(end_offset)
  e.write_tag_buffer()
  e.write_tag_buffer()
  e.write_tag_buffer()
  e.to_bytes()
}

///|
/// Partition index of the first requested partition.
fn fake_fetch_request_first_partition(rest : Bytes) -> Int raise {
  let d = @buf.Decoder::new(rest)
  ignore(d.read_i16()) // api key
  ignore(d.read_i16()) // api version
  ignore(d.read_i32()) // correlation id
  ignore(d.read_compact_string()) // client id
  d.skip_tag_buffer()
  ignore(d.read_i32()) // max_wait_ms
  ignore(d.read_i32()) // min_bytes
  ignore(d.read_i32()) // max_bytes
  ignore(d.read_i8()) // isolation level
  ignore(d.read_i32()) // session id
  ignore(d.read_i32()) // session epoch
  ignore(d.read_compact_len()) // topics
  d.skip(16) // topic id
  ignore(d.read_compact_len()) // partitions
  d.read_i32() // partition index
}

///|
/// Fetch offset of the first requested partition (same for v12-v16: the
/// offset sits after the partition index and current leader epoch).
fn fake_fetch_request_first_offset(rest : Bytes) -> Int64 raise {
  let d = @buf.Decoder::new(rest)
  ignore(d.read_i16()) // api key
  ignore(d.read_i16()) // api version
  ignore(d.read_i32()) // correlation id
  ignore(d.read_compact_string()) // client id
  d.skip_tag_buffer()
  ignore(d.read_i32()) // max_wait_ms
  ignore(d.read_i32()) // min_bytes
  ignore(d.read_i32()) // max_bytes
  ignore(d.read_i8()) // isolation level
  ignore(d.read_i32()) // session id
  ignore(d.read_i32()) // session epoch
  ignore(d.read_compact_len()) // topics
  d.skip(16) // topic id
  ignore(d.read_compact_len()) // partitions
  ignore(d.read_i32()) // partition index
  ignore(d.read_i32()) // current leader epoch
  d.read_i64() // fetch offset
}

///|
/// A Fetch response body (any v12-v16: the shape matches) carrying two
/// records at base_offset 42 and the given session id. The record batch's
/// base offset is patched after encoding (it is outside the CRC).
fn fake_fetch_response(
  topic : String,
  version : Int,
  session_id : Int,
  error_code? : Int = 0,
  partition_index? : Int = 0,
) -> Bytes {
  let batch = @moonkafka.encode_record_batch([
    {
      offset: 0L,
      timestamp: 1000L,
      key: None,
      value: Some(b"one"),
      headers: [],
    },
    {
      offset: 0L,
      timestamp: 2000L,
      key: None,
      value: Some(b"two"),
      headers: [],
    },
  ])
  let patched_arr : Array[Byte] = [
    b'\x00', b'\x00', b'\x00', b'\x00', b'\x00', b'\x00', b'\x00', b'\x2a',
  ]
  for i in 8..<batch.length() {
    patched_arr.push(batch[i])
  }
  let patched = Bytes::from_array(patched_arr)
  let e = @buf.Encoder::new()
  e.write_i32(0) // throttle
  e.write_i16(0) // top-level error
  e.write_i32(session_id)
  e.write_compact_len(1) // one topic
  if version >= 13 {
    e.write_bytes(fake_topic_id)
  } else {
    e.write_compact_string(topic)
  }
  e.write_compact_len(1) // one partition
  e.write_i32(partition_index)
  e.write_i16(error_code)
  e.write_i64(100L) // high watermark
  e.write_i64(-1L) // last stable offset
  e.write_i64(-1L) // log start offset
  e.write_compact_len(-1) // aborted transactions: null
  e.write_i32(-1) // preferred read replica
  e.write_compact_len(patched.length()) // records
  e.write_bytes(patched)
  e.write_tag_buffer()
  e.write_tag_buffer()
  e.write_tag_buffer()
  e.to_bytes()
}

///|
/// Cursor from a DescribeTopicPartitions request (flexible v0: header v2
/// with its tag buffer, then topics array, response limit, cursor struct).
fn fake_dtp_request_cursor(rest : Bytes) -> TopicPartitionCursor? raise {
  let d = @buf.Decoder::new(rest)
  ignore(d.read_i16()) // api key
  ignore(d.read_i16()) // api version
  ignore(d.read_i32()) // correlation id
  ignore(d.read_compact_string()) // client id
  d.skip_tag_buffer()
  let topic_count = d.read_compact_len()
  for _ in 0..<topic_count {
    ignore(d.read_compact_string())
    d.skip_tag_buffer()
  }
  ignore(d.read_i32()) // response_partition_limit
  if d.read_i8() < 0 {
    None
  } else {
    let topic_name = d.read_compact_string()
    let partition_index = d.read_i32()
    d.skip_tag_buffer()
    Some({ topic_name, partition_index, })
  }
}

///|
/// DescribeTopicPartitions response page for one topic: the given
/// partitions plus an optional resume cursor.
fn fake_describe_topic_partitions_response(
  topic : String,
  partitions : Array[Int],
  next_cursor : TopicPartitionCursor?,
  error_code? : Int = 0,
) -> Bytes {
  let e = @buf.Encoder::new()
  e.write_i32(0) // throttle
  e.write_compact_len(1) // one topic
  e.write_i16(error_code)
  e.write_compact_string(topic)
  e.write_bytes(Bytes::make(16, b'\x07')) // topic id
  e.write_bool(false) // is_internal
  e.write_compact_len(partitions.length())
  for p in partitions {
    e.write_i16(0) // no error
    e.write_i32(p) // index
    e.write_i32(0) // leader node
    e.write_i32(0) // leader epoch
    e.write_compact_len(0) // replicas
    e.write_compact_len(0) // isr
    e.write_compact_len(-1) // eligible leader replicas: null
    e.write_compact_len(-1) // last known elr: null
    e.write_compact_len(0) // offline replicas
    e.write_tag_buffer()
  }
  e.write_i32(0) // authorized operations
  e.write_tag_buffer()
  match next_cursor {
    None => e.write_i8(-1) // nullable struct marker
    Some(cursor) => {
      e.write_i8(1)
      e.write_compact_string(cursor.topic_name)
      e.write_i32(cursor.partition_index)
      e.write_tag_buffer()
    }
  }
  e.write_tag_buffer()
  e.to_bytes()
}

///|
/// Parse an AddPartitionsToTxn v3 request (client shape):
/// (transactional id, producer id, epoch, topics with partitions).
fn fake_add_partitions_to_txn_request(
  rest : Bytes,
) -> (String, Int64, Int, Array[(String, Array[Int])]) raise {
  let d = @buf.Decoder::new(rest)
  ignore(d.read_i16()) // api key
  ignore(d.read_i16()) // api version
  ignore(d.read_i32()) // correlation id
  ignore(d.read_compact_string()) // client id
  d.skip_tag_buffer()
  let transactional_id = d.read_compact_string()
  let producer_id = d.read_i64()
  let producer_epoch = d.read_i16()
  let n = d.read_compact_len()
  let topics : Array[(String, Array[Int])] = []
  for _ in 0..<n {
    let name = d.read_compact_string()
    let parts = d.read_compact_len()
    let partitions : Array[Int] = []
    for _ in 0..<parts {
      partitions.push(d.read_i32())
    }
    d.skip_tag_buffer()
    topics.push((name, partitions))
  }
  (transactional_id, producer_id, producer_epoch, topics)
}

///|
/// AddPartitionsToTxn v3 response body: per-partition results under the
/// v3-and-below topic array.
fn fake_add_partitions_to_txn_response(
  topics : Array[(String, Array[Int])],
  error_code : Int,
) -> Bytes {
  let e = @buf.Encoder::new()
  e.write_i32(0) // throttle
  e.write_compact_len(topics.length())
  for topic in topics {
    let (name, partitions) = topic
    e.write_compact_string(name)
    e.write_compact_len(partitions.length())
    for p in partitions {
      e.write_i32(p)
      e.write_i16(error_code)
      e.write_tag_buffer()
    }
    e.write_tag_buffer()
  }
  e.write_tag_buffer()
  e.to_bytes()
}

///|
/// Parse an AddOffsetsToTxn v4 request:
/// (transactional id, producer id, epoch, group id).
fn fake_add_offsets_to_txn_request(
  rest : Bytes,
) -> (String, Int64, Int, String) raise {
  let d = @buf.Decoder::new(rest)
  ignore(d.read_i16()) // api key
  ignore(d.read_i16()) // api version
  ignore(d.read_i32()) // correlation id
  ignore(d.read_compact_string()) // client id
  d.skip_tag_buffer()
  let transactional_id = d.read_compact_string()
  let producer_id = d.read_i64()
  let producer_epoch = d.read_i16()
  let group_id = d.read_compact_string()
  (transactional_id, producer_id, producer_epoch, group_id)
}

///|
/// AddOffsetsToTxn v4 response body.
fn fake_add_offsets_to_txn_response(error_code : Int) -> Bytes {
  let e = @buf.Encoder::new()
  e.write_i32(0) // throttle
  e.write_i16(error_code)
  e.write_tag_buffer()
  e.to_bytes()
}

///|
/// Parse a TxnOffsetCommit v4 request: (transactional id, group id,
/// producer id, epoch, committed (partition, offset) pairs).
fn fake_txn_offset_commit_request(
  rest : Bytes,
) -> (String, String, Int64, Int, Array[(Int, Int64)]) raise {
  let d = @buf.Decoder::new(rest)
  ignore(d.read_i16()) // api key
  ignore(d.read_i16()) // api version
  ignore(d.read_i32()) // correlation id
  ignore(d.read_compact_string()) // client id
  d.skip_tag_buffer()
  let transactional_id = d.read_compact_string()
  let group_id = d.read_compact_string()
  let producer_id = d.read_i64()
  let producer_epoch = d.read_i16()
  ignore(d.read_i32()) // generation id
  ignore(d.read_compact_string()) // member id
  ignore(d.read_compact_nullable_string()) // group instance id
  let n = d.read_compact_len()
  let offsets : Array[(Int, Int64)] = []
  for _ in 0..<n {
    ignore(d.read_compact_string()) // topic name
    let parts = d.read_compact_len()
    for _ in 0..<parts {
      let partition = d.read_i32()
      let offset = d.read_i64()
      ignore(d.read_i32()) // committed leader epoch
      ignore(d.read_compact_nullable_string()) // metadata
      d.skip_tag_buffer()
      offsets.push((partition, offset))
    }
    d.skip_tag_buffer()
  }
  (transactional_id, group_id, producer_id, producer_epoch, offsets)
}

///|
/// TxnOffsetCommit v4 response body.
fn fake_txn_offset_commit_response(
  offsets : Array[(Int, Int64)],
  error_code : Int,
) -> Bytes {
  let e = @buf.Encoder::new()
  e.write_i32(0) // throttle
  e.write_compact_len(1) // one topic
  e.write_compact_string("events")
  e.write_compact_len(offsets.length())
  for entry in offsets {
    let (partition, _) = entry
    e.write_i32(partition)
    e.write_i16(error_code)
    e.write_tag_buffer()
  }
  e.write_tag_buffer()
  e.write_tag_buffer()
  e.to_bytes()
}

///|
/// Parse an EndTxn v5 request:
/// (transactional id, producer id, epoch, committed).
fn fake_end_txn_request(rest : Bytes) -> (String, Int64, Int, Bool) raise {
  let d = @buf.Decoder::new(rest)
  ignore(d.read_i16()) // api key
  ignore(d.read_i16()) // api version
  ignore(d.read_i32()) // correlation id
  ignore(d.read_compact_string()) // client id
  d.skip_tag_buffer()
  let transactional_id = d.read_compact_string()
  let producer_id = d.read_i64()
  let producer_epoch = d.read_i16()
  let committed = d.read_bool()
  (transactional_id, producer_id, producer_epoch, committed)
}

///|
/// EndTxn v5 response body: success carries the KIP-890 part 2
/// post-transaction identity (the fake bumps the epoch like a v2
/// transaction coordinator).
fn fake_end_txn_response(
  error_code : Int,
  producer_id : Int64,
  producer_epoch : Int,
) -> Bytes {
  let e = @buf.Encoder::new()
  e.write_i32(0) // throttle
  e.write_i16(error_code)
  e.write_i64(producer_id)
  e.write_i16(producer_epoch)
  e.write_tag_buffer()
  e.to_bytes()
}

///|
/// Parse an OffsetCommit v10 request (topic-id addressing): (group id,
/// generation/epoch, member id, (partition, offset) commits).
fn fake_offset_commit_request(
  rest : Bytes,
) -> (String, Int, String, Array[(Int, Int64)]) raise {
  let d = @buf.Decoder::new(rest)
  ignore(d.read_i16()) // api key
  ignore(d.read_i16()) // api version
  ignore(d.read_i32()) // correlation id
  ignore(d.read_compact_string()) // client id
  d.skip_tag_buffer()
  let group_id = d.read_compact_string()
  let generation = d.read_i32()
  let member_id = d.read_compact_string()
  ignore(d.read_compact_nullable_string()) // group instance id
  let commits : Array[(Int, Int64)] = []
  let n = d.read_compact_len()
  for _ in 0..<n {
    d.skip(16) // topic id
    let parts = d.read_compact_len()
    for _ in 0..<parts {
      let partition = d.read_i32()
      let offset = d.read_i64()
      ignore(d.read_i32()) // committed leader epoch
      ignore(d.read_compact_nullable_string()) // metadata
      d.skip_tag_buffer()
      commits.push((partition, offset))
    }
    d.skip_tag_buffer()
  }
  (group_id, generation, member_id, commits)
}

///|
/// OffsetCommit v10 response body: one topic entry with the given
/// per-partition error code.
fn fake_offset_commit_response(
  commits : Array[(Int, Int64)],
  error_code : Int,
) -> Bytes {
  let e = @buf.Encoder::new()
  e.write_i32(0) // throttle
  e.write_compact_len(1) // one topic
  e.write_bytes(fake_topic_id)
  e.write_compact_len(commits.length())
  for entry in commits {
    let (partition, _) = entry
    e.write_i32(partition)
    e.write_i16(error_code)
    e.write_tag_buffer()
  }
  e.write_tag_buffer()
  e.write_tag_buffer()
  e.to_bytes()
}

///|
/// Parse an OffsetFetch v10 request: (group id, wanted partitions).
fn fake_offset_fetch_request(rest : Bytes) -> (String, Array[Int]) raise {
  let d = @buf.Decoder::new(rest)
  ignore(d.read_i16()) // api key
  ignore(d.read_i16()) // api version
  ignore(d.read_i32()) // correlation id
  ignore(d.read_compact_string()) // client id
  d.skip_tag_buffer()
  ignore(d.read_compact_len()) // groups
  let group_id = d.read_compact_string()
  ignore(d.read_compact_nullable_string()) // member id
  ignore(d.read_i32()) // member epoch
  let partitions : Array[Int] = []
  let n = d.read_compact_len()
  for _ in 0..<n {
    d.skip(16) // topic id
    let parts = d.read_compact_len()
    for _ in 0..<parts {
      partitions.push(d.read_i32())
    }
    d.skip_tag_buffer()
  }
  ignore(d.read_bool()) // require stable
  (group_id, partitions)
}

///|
/// OffsetFetch v10 response body: the group's committed offsets from the
/// fake's store (-1 when none).
fn fake_offset_fetch_response(
  group_id : String,
  partitions : Array[Int],
  committed : Map[Int, Int64],
) -> Bytes {
  let e = @buf.Encoder::new()
  e.write_i32(0) // throttle
  e.write_compact_len(1) // groups
  e.write_compact_string(group_id)
  e.write_compact_len(1) // topics
  e.write_bytes(fake_topic_id)
  e.write_compact_len(partitions.length())
  for p in partitions {
    e.write_i32(p)
    e.write_i64(committed.get(p).unwrap_or(-1L))
    e.write_i32(-1) // leader epoch
    e.write_compact_nullable_string(None) // metadata
    e.write_i16(0) // no error
    e.write_tag_buffer()
  }
  e.write_tag_buffer()
  e.write_i16(0) // group error
  e.write_tag_buffer()
  e.write_tag_buffer()
  e.to_bytes()
}

///|
/// Answer requests forever on one client connection; exits when the client
/// goes away. Every API answers with offset 42.
async fn fake_serve_conn(
  stream : @moonkafka.BrokerStream,
  topic : String,
  broker_port : Int,
  api_throttle_ms? : Int = 0,
  sasl_ok? : Bool = true,
  scram_password? : String = "secret",
  scram_sha512? : Bool = false,
  fetch_epochs? : Array[Int] = [],
  produce_partitions? : Array[Int] = [],
  produce_record_counts? : Array[Int] = [],
  metadata_requests? : Array[Int] = [],
  produce_error_rounds? : Int = 0,
  produce_identities? : Array[(Int64, Int, Int)] = [],
  unknown_producer_at? : Int = 0,
  add_txn_requests? : Array[(String, Array[(String, Array[Int])])] = [],
  add_offsets_requests? : Array[(String, String)] = [],
  txn_offset_requests? : Array[(String, String, Array[(Int, Int64)])] = [],
  end_txn_requests? : Array[(String, Int64, Int, Bool)] = [],
  txn_error_code? : Int = 0,
  end_txn_error_code? : Int = 0,
  end_txn_error_rounds? : Int = 0,
  produce_error_code? : Int = 0,
  offset_commit_requests? : Array[(String, Int, Int64)] = [],
  committed_store? : Map[Int, Int64] = Map([]),
  offset_commit_error_rounds? : Int = 0,
  fetch_offsets? : Array[Int64] = [],
  fetch_error_rounds? : Int = 0,
  fetch_error_code? : Int = 0,
  epoch_end_offset? : Int64 = 42L,
  heartbeat_requests? : Array[(String, String, Int, String?)] = [],
  heartbeat_regexes? : Array[String?] = [],
  heartbeat_errors? : Array[Int] = [],
  heartbeat_partitions? : Array[Int] = [0],
  coordinator? : FakeCoordinator? = None,
  metadata_partitions? : Int = 1,
  hide_apis? : Array[Int] = [],
  dtp_requests? : Array[Int] = [],
  dtp_error_rounds? : Int = 0,
  create_topics? : Array[(String, Bytes)] = [],
  delete_topics? : Array[(String?, Bytes)] = [],
  create_partitions? : Array[(String, Int)] = [],
  delete_records? : Array[(String, Int, Int64)] = [],
  offset_deletes? : Array[(String, String, Int)] = [],
  cluster_ops? : FakeClusterOps = FakeClusterOps::new(),
) -> Unit {
  // per-connection SASL state
  let mut dtp_frames = 0
  let mut sasl_mechanism : String? = None
  let mut scram_client_first_bare : String? = None
  let mut scram_server_first : String? = None
  for ;; {
    let size_bytes = stream.read_exactly(4)
    let size = @buf.Decoder::new(size_bytes).read_i32()
    let rest = stream.read_exactly(size)
    let key = (rest[0].to_int() << 8) | rest[1].to_int()
    let version = (rest[2].to_int() << 8) | rest[3].to_int()
    let corr = (rest[4].to_int() << 24) |
      (rest[5].to_int() << 16) |
      (rest[6].to_int() << 8) |
      rest[7].to_int()
    let body = match key {
      18 => fake_api_versions_response(api_throttle_ms, hide_apis~)
      17 => {
        sasl_mechanism = Some(fake_sasl_handshake_mechanism(rest))
        fake_sasl_handshake_response()
      }
      0 => {
        let (idx, count, pid, epoch, base_seq) = fake_produce_request_info(
          rest, version,
        )
        produce_partitions.push(idx)
        produce_record_counts.push(count)
        produce_identities.push((pid, epoch, base_seq))
        // The shared partitions array doubles as the round counter, so
        // re-dialed connections keep counting: the first
        // `produce_error_rounds` produce requests (across all of them)
        // answer LEADER_NOT_AVAILABLE — retriable, the sender must
        // requeue and recover.
        if unknown_producer_at > 0 &&
          produce_partitions.length() == unknown_producer_at {
          // UNKNOWN_PRODUCER_ID: the sender must bump the epoch and
          // restart sequences
          fake_produce_response(topic, version, 42L, error_code=59)
        } else if produce_partitions.length() <= produce_error_rounds {
          fake_produce_response(topic, version, 42L, error_code=5)
        } else if produce_error_code != 0 {
          fake_produce_response(
            topic,
            version,
            42L,
            error_code=produce_error_code,
          )
        } else {
          fake_produce_response(topic, version, 42L)
        }
      }
      2 =>
        fake_list_offsets_response(
          topic,
          42L,
          partition_count=metadata_partitions,
        )
      1 => {
        fetch_epochs.push(fake_fetch_request_session_epoch(rest))
        fetch_offsets.push(fake_fetch_request_first_offset(rest))
        if fake_fetch_request_isolation(rest) == 1 {
          fake_fetch_response_committed(topic, version, 7)
        } else if fetch_offsets.length() <= fetch_error_rounds {
          fake_fetch_response(
            topic,
            version,
            7,
            error_code=fetch_error_code,
            partition_index=fake_fetch_request_first_partition(rest),
          )
        } else {
          fake_fetch_response(
            topic,
            version,
            7,
            partition_index=fake_fetch_request_first_partition(rest),
          )
        }
      }
      23 => fake_offset_for_leader_epoch_response(epoch_end_offset)
      11 => {
        // Classic JoinGroup through the mini coordinator.
        let coord = coordinator.unwrap_or(FakeCoordinator::new())
        let (_, member, _, protocols) = fake_join_group_request(rest)
        fake_classic_join(coord, member, protocols)
      }
      12 => {
        let coord = coordinator.unwrap_or(FakeCoordinator::new())
        let (member, _) = fake_classic_heartbeat_request(rest)
        // Rebalance signal: members below the current round rejoin.
        let code = if coord.arrived.get(member) == Some(coord.round) {
          0
        } else {
          27
        }
        fake_classic_heartbeat_response(code)
      }
      13 => {
        let coord = coordinator.unwrap_or(FakeCoordinator::new())
        let member = fake_leave_group_request(rest)
        let _ = coord.members.remove(member)
        let _ = coord.arrived.remove(member)
        coord.leaves.push(member)
        coord.round += 1
        coord.arrived.clear()
        fake_leave_group_response(member)
      }
      14 => {
        let coord = coordinator.unwrap_or(FakeCoordinator::new())
        let (member, generation, assigns) = fake_sync_group_request(rest)
        coord.syncs.push((member, generation))
        // The leader's sync registers everyone's assignment (tagged
        // with the generation); a member waits for its generation to
        // be registered before reading its assignment back.
        for entry in assigns {
          let (id, bytes) = entry
          coord.assignments[id] = bytes
          coord.sync_generations[id] = generation
        }
        if !assigns.is_empty() {
          fake_sync_group_response(
            coord.assignments.get(member).unwrap_or(Bytes::make(0, 0)),
          )
        } else {
          let deadline = @async.now() + 3000L
          for ;; {
            if coord.sync_generations.get(member) == Some(generation) {
              break
            }
            if @async.now() > deadline {
              break
            }
            @async.sleep(2)
          }
          fake_sync_group_response(
            coord.assignments.get(member).unwrap_or(Bytes::make(0, 0)),
          )
        }
      }
      19 => {
        let (topics, _) = fake_create_topics_request(rest)
        let results : Array[(String, Bytes, Int, String?)] = []
        let mut next_id = 1
        for entry in topics {
          let (name, _, _) = entry
          let id = Bytes::make(16, next_id.to_byte())
          next_id += 1
          create_topics.push((name, id))
          results.push((name, id, 0, None))
        }
        fake_create_topics_response(results)
      }
      20 => {
        let (names, ids) = fake_delete_topics_request(rest)
        for name in names {
          delete_topics.push((Some(name), Bytes::make(16, 0)))
        }
        for id in ids {
          delete_topics.push((None, id))
        }
        fake_delete_topics_response(names, ids)
      }
      21 => {
        let (topics, _) = fake_delete_records_request(rest)
        let results : Array[(Int, Int64, Int)] = []
        for entry in topics {
          let (name, partitions) = entry
          for part in partitions {
            let (index, offset) = part
            delete_records.push((name, index, offset))
            results.push((index, offset, 0))
          }
        }
        fake_delete_records_response(results)
      }
      37 => {
        let (topics, _) = fake_create_partitions_request(rest)
        let results : Array[(String, Int, String?)] = []
        for entry in topics {
          let (name, count, _) = entry
          create_partitions.push((name, count))
          results.push((name, 0, None))
        }
        fake_create_partitions_response(results)
      }
      47 => {
        let (group, topics) = fake_offset_delete_request(rest)
        let results : Array[(String, Int, Int)] = []
        for entry in topics {
          let (name, partitions) = entry
          for part in partitions {
            let index = part
            offset_deletes.push((group, name, index))
            results.push((name, index, 0))
          }
        }
        fake_offset_delete_response(results)
      }
      68 => {
        let (group, member, epoch, instance, regex) = fake_consumer_group_heartbeat_request(
          rest,
        )
        heartbeat_requests.push((group, member, epoch, instance))
        heartbeat_regexes.push(regex)
        if heartbeat_requests.length() <= heartbeat_errors.length() {
          fake_consumer_group_heartbeat_response(
            heartbeat_errors[heartbeat_requests.length() - 1],
            member,
            epoch,
            [],
          )
        } else {
          let next_epoch = if epoch >= 0 { epoch + 1 } else { epoch }
          fake_consumer_group_heartbeat_response(
            0, member, next_epoch, heartbeat_partitions,
          )
        }
      }
      3 => {
        metadata_requests.push(1)
        fake_metadata_response(
          topic,
          broker_port,
          version,
          partition_count=metadata_partitions,
        )
      }
      10 =>
        fake_find_coordinator_response(
          fake_find_coordinator_request_keys(rest),
          broker_port,
        )
      22 => {
        let (_, pid, epoch) = fake_init_producer_id_request(rest)
        if pid < 0 {
          fake_init_producer_id_response(500L, 0)
        } else {
          fake_init_producer_id_response(pid, epoch + 1)
        }
      }
      24 => {
        let (tid, _, _, topics) = fake_add_partitions_to_txn_request(rest)
        add_txn_requests.push((tid, topics))
        fake_add_partitions_to_txn_response(topics, txn_error_code)
      }
      25 => {
        let (tid, _, _, group) = fake_add_offsets_to_txn_request(rest)
        add_offsets_requests.push((tid, group))
        fake_add_offsets_to_txn_response(0)
      }
      26 => {
        let (tid, pid, epoch, committed) = fake_end_txn_request(rest)
        end_txn_requests.push((tid, pid, epoch, committed))
        // The shared array doubles as the round counter: the first
        // `end_txn_error_rounds` EndTxn calls answer the injected error;
        // success bumps the epoch like a KIP-890 part 2 coordinator.
        if end_txn_requests.length() <= end_txn_error_rounds {
          fake_end_txn_response(end_txn_error_code, pid, epoch)
        } else {
          fake_end_txn_response(0, pid, epoch + 1)
        }
      }
      28 => {
        let (tid, group, _, _, offsets) = fake_txn_offset_commit_request(rest)
        txn_offset_requests.push((tid, group, offsets))
        fake_txn_offset_commit_response(offsets, 0)
      }
      8 => {
        let (group, _, _, commits) = fake_offset_commit_request(rest)
        for entry in commits {
          let (partition, offset) = entry
          offset_commit_requests.push((group, partition, offset))
          committed_store[partition] = offset
        }
        // The shared array doubles as the round counter: the first
        // `offset_commit_error_rounds` commits answer
        // COORDINATOR_LOAD_IN_PROGRESS — retriable for the consumer.
        let code = if offset_commit_requests.length() <=
          offset_commit_error_rounds {
          14
        } else {
          0
        }
        fake_offset_commit_response(commits, code)
      }
      9 => {
        let (group, partitions) = fake_offset_fetch_request(rest)
        fake_offset_fetch_response(group, partitions, committed_store)
      }
      75 => {
        dtp_requests.push(1)
        dtp_frames += 1
        let error = if dtp_frames <= dtp_error_rounds { 5 } else { 0 }
        // Two pages: a fresh request returns partition 0 with a cursor at
        // partition 1; a resumed request returns partition 1 and completes.
        match fake_dtp_request_cursor(rest) {
          None =>
            fake_describe_topic_partitions_response(
              topic,
              [0],
              Some({ topic_name: topic, partition_index: 1, }),
              error_code=error,
            )
          Some(_) =>
            fake_describe_topic_partitions_response(
              topic,
              [1],
              None,
              error_code=error,
            )
        }
      }
      36 => {
        let mechanism = sasl_mechanism.unwrap_or("")
        if !sasl_ok {
          fake_sasl_authenticate_response(false)
        } else if mechanism == "PLAIN" {
          fake_sasl_authenticate_response(true)
        } else if mechanism == "OAUTHBEARER" {
          // RFC 7628 voucher: "n,<SOH>auth=Bearer <token><SOH><SOH>"
          let voucher = @utf8.decode_lossy(fake_sasl_auth_bytes(rest)[:])
          let prefix = "n,\u{1}auth=Bearer "
          let suffix = "\u{1}\u{1}"
          if voucher.has_prefix(prefix) && voucher.has_suffix(suffix) {
            fake_sasl_authenticate_response(true)
          } else {
            fake_sasl_authenticate_response(false)
          }
        } else if mechanism != "SCRAM-SHA-256" && mechanism != "SCRAM-SHA-512" {
          fake_sasl_authenticate_response(false)
        } else {
          let msg = @utf8.decode_lossy(fake_sasl_auth_bytes(rest)[:])
          match scram_client_first_bare {
            None => {
              // client-first-message: strip the "n,," gs2 header
              let bare = scram_bare_message(msg)
              let client_nonce = match scram_attr_value(bare, 'r') {
                Some(n) => n
                None => abort("client-first-message has no nonce")
              }
              let server_nonce = "\{client_nonce}srv"
              let server_first = "r=\{server_nonce},s=\{@base64.encode(scram_test_salt[:])},i=4096"
              scram_client_first_bare = Some(bare)
              scram_server_first = Some(server_first)
              fake_sasl_scram_response(server_first)
            }
            Some(client_first_bare) => {
              // client-final-message: verify the proof, answer with v=
              let server_first = scram_server_first.unwrap_or("")
              let without_proof = scram_final_without_proof(msg)
              match scram_attr_value(msg, 'p') {
                Some(proof_b64) => {
                  let auth_message = @moonkafka.scram_auth_message(
                    client_first_bare, server_first, without_proof,
                  )
                  let salted = @moonkafka.scram_salted_password(
                    scram_sha512,
                    @utf8.encode(scram_password),
                    scram_test_salt,
                    4096,
                  )
                  if @moonkafka.scram_verify_client_proof(
                      scram_sha512, salted, auth_message, proof_b64,
                    ) {
                    let signature = @moonkafka.scram_server_final(
                      scram_sha512, salted, auth_message,
                    )
                    fake_sasl_scram_response(signature)
                  } else {
                    fake_sasl_authenticate_response(false)
                  }
                }
                None => fake_sasl_authenticate_response(false)
              }
            }
          }
        }
      }
      29 => {
        cluster_ops.describe_acls.push(fake_describe_acls_request(rest))
        fake_describe_acls_response()
      }
      30 => {
        let creations = fake_create_acls_request(rest)
        for creation in creations {
          cluster_ops.create_acls.push(creation)
        }
        fake_create_acls_response(creations.length())
      }
      31 => {
        let filters = fake_delete_acls_request(rest)
        for filter in filters {
          cluster_ops.delete_acls.push(filter)
        }
        fake_delete_acls_response(filters.length())
      }
      48 => {
        let filter = fake_describe_client_quotas_request(rest, version)
        cluster_ops.describe_quotas.push(filter)
        fake_describe_client_quotas_response(version)
      }
      49 => {
        let (entries, validate_only) = fake_alter_client_quotas_request(
          rest, version,
        )
        for entry in entries {
          cluster_ops.alter_quotas.push(entry)
          cluster_ops.alter_quotas_validate_only.push(validate_only)
        }
        fake_alter_client_quotas_response(entries, version)
      }
      50 => {
        cluster_ops.describe_scram.push(fake_describe_user_scram_request(rest))
        fake_describe_user_scram_response()
      }
      51 => {
        let (deletions, upsertions) = fake_alter_user_scram_request(rest)
        for deletion in deletions {
          cluster_ops.alter_scram_deletions.push(deletion)
        }
        for upsertion in upsertions {
          cluster_ops.alter_scram_upsertions.push(upsertion)
        }
        fake_alter_user_scram_response(deletions, upsertions)
      }
      60 => {
        cluster_ops.describe_cluster.push(fake_describe_cluster_request(rest))
        fake_describe_cluster_response(broker_port, version)
      }
      32 => {
        let (resources, _, _) = fake_describe_configs_request(rest)
        for resource in resources {
          let (resource_type, resource_name, keys) = resource
          cluster_ops.describe_configs.push(
            (resource_type, resource_name, keys),
          )
        }
        fake_describe_configs_response(resources)
      }
      44 => {
        let (resources, _) = fake_incremental_alter_configs_request(rest)
        for resource in resources {
          let (resource_type, resource_name, configs) = resource
          cluster_ops.incremental_alters.push(
            (resource_type, resource_name, configs),
          )
        }
        fake_alter_configs_response(
          resources.map(fn(r) {
            let (t, n, _) = r
            (t, n)
          }),
        )
      }
      33 => {
        let (resources, _) = fake_alter_configs_request(rest)
        for resource in resources {
          let (resource_type, resource_name, configs) = resource
          cluster_ops.alter_configs.push(
            (resource_type, resource_name, configs),
          )
        }
        fake_alter_configs_response(
          resources.map(fn(r) {
            let (t, n, _) = r
            (t, n)
          }),
        )
      }
      74 => {
        cluster_ops.list_config_resources.push(
          fake_list_config_resources_request(rest),
        )
        fake_list_config_resources_response(version)
      }
      35 => {
        let topics = fake_describe_log_dirs_request(rest)
        cluster_ops.log_dirs.push(topics)
        let effective = match topics {
          Some(topics) => topics
          None => [("events", [0, 1])]
        }
        fake_describe_log_dirs_response(effective, version)
      }
      43 => {
        let (election_type, topics) = fake_elect_leaders_request(rest)
        cluster_ops.elect_leaders.push((election_type, topics))
        let effective = match topics {
          Some(topics) => topics
          None => [("events", [0, 1])]
        }
        fake_elect_leaders_response(effective)
      }
      45 => {
        let reassignments = fake_alter_reassignments_request(rest)
        for entry in reassignments {
          let (name, index, replicas) = entry
          cluster_ops.alter_reassignments.push((name, index, replicas))
        }
        fake_alter_reassignments_response(reassignments)
      }
      46 => {
        let filter = fake_list_reassignments_request(rest)
        cluster_ops.list_reassignments.push(filter)
        fake_list_reassignments_response(filter)
      }
      64 => {
        cluster_ops.unregister_broker.push(fake_unregister_broker_request(rest))
        fake_unregister_broker_response()
      }
      55 => {
        let requests = fake_describe_quorum_request(rest)
        for entry in requests {
          let (name, index) = entry
          cluster_ops.quorum_requests.push((name, index))
        }
        fake_describe_quorum_response(requests)
      }
      57 => {
        let (updates, validate_only) = fake_update_features_request(rest)
        for update in updates {
          let (feature, level, upgrade_type) = update
          cluster_ops.update_features.push(
            (feature, level, upgrade_type, validate_only),
          )
        }
        fake_update_features_response(cluster_ops.update_features_error)
      }
      _ => continue
    }
    // ApiVersions and SaslHandshake v1 responses use header v0: no tag
    // buffer after the correlation id. The rest are flexible (header v1).
    let has_tag = key != 18 && key != 17 && key != 47
    let frame = @buf.Encoder::new()
    frame.write_i32(4 + (if has_tag { 1 } else { 0 }) + body.length())
    frame.write_i32(corr)
    if has_tag {
      frame.write_tag_buffer()
    }
    frame.write_bytes(body)
    stream.write(frame.to_bytes())
  }
}

///|
/// Accept connections forever, serving each one concurrently.
async fn fake_broker(
  server : @socket.TcpServer,
  root : @async.TaskGroup[Unit],
  topic : String,
  accepted : Array[@socket.Tcp],
  api_throttle_ms? : Int = 0,
  sasl_ok? : Bool = true,
  scram_password? : String = "secret",
  scram_sha512? : Bool = false,
  wrap_tls? : Bool = false,
  fetch_epochs? : Array[Int] = [],
  produce_partitions? : Array[Int] = [],
  produce_record_counts? : Array[Int] = [],
  metadata_requests? : Array[Int] = [],
  produce_error_rounds? : Int = 0,
  produce_identities? : Array[(Int64, Int, Int)] = [],
  unknown_producer_at? : Int = 0,
  add_txn_requests? : Array[(String, Array[(String, Array[Int])])] = [],
  add_offsets_requests? : Array[(String, String)] = [],
  txn_offset_requests? : Array[(String, String, Array[(Int, Int64)])] = [],
  end_txn_requests? : Array[(String, Int64, Int, Bool)] = [],
  txn_error_code? : Int = 0,
  end_txn_error_code? : Int = 0,
  end_txn_error_rounds? : Int = 0,
  produce_error_code? : Int = 0,
  offset_commit_requests? : Array[(String, Int, Int64)] = [],
  committed_store? : Map[Int, Int64] = Map([]),
  offset_commit_error_rounds? : Int = 0,
  fetch_offsets? : Array[Int64] = [],
  fetch_error_rounds? : Int = 0,
  fetch_error_code? : Int = 0,
  epoch_end_offset? : Int64 = 42L,
  heartbeat_requests? : Array[(String, String, Int, String?)] = [],
  heartbeat_regexes? : Array[String?] = [],
  heartbeat_errors? : Array[Int] = [],
  heartbeat_partitions? : Array[Int] = [0],
  coordinator? : FakeCoordinator? = None,
  metadata_partitions? : Int = 1,
  hide_apis? : Array[Int] = [],
  dtp_requests? : Array[Int] = [],
  dtp_error_rounds? : Int = 0,
  create_topics? : Array[(String, Bytes)] = [],
  delete_topics? : Array[(String?, Bytes)] = [],
  create_partitions? : Array[(String, Int)] = [],
  delete_records? : Array[(String, Int, Int64)] = [],
  offset_deletes? : Array[(String, String, Int)] = [],
  cluster_ops? : FakeClusterOps = FakeClusterOps::new(),
) -> Unit {
  for ;; {
    let (tcp, _) = server.accept()
    accepted.push(tcp)
    root.spawn_bg(no_wait=true, allow_failure=true) <| () => {
      let stream = if wrap_tls {
        @moonkafka.Secure(
          @tls.Tls::server_from_pair(
            tcp,
            tcp,
            private_key_file="test_keys/key.pem",
            private_key_type=PEM,
            certificate_file="test_keys/cert.pem",
            certificate_type=PEM,
          ),
        )
      } else {
        @moonkafka.Plain(tcp)
      }
      fake_serve_conn(
        stream,
        topic,
        server.addr.port(),
        api_throttle_ms~,
        sasl_ok~,
        scram_password~,
        scram_sha512~,
        fetch_epochs~,
        produce_partitions~,
        produce_record_counts~,
        metadata_requests~,
        produce_error_rounds~,
        produce_identities~,
        unknown_producer_at~,
        add_txn_requests~,
        add_offsets_requests~,
        txn_offset_requests~,
        end_txn_requests~,
        txn_error_code~,
        end_txn_error_code~,
        end_txn_error_rounds~,
        produce_error_code~,
        offset_commit_requests~,
        committed_store~,
        offset_commit_error_rounds~,
        fetch_offsets~,
        fetch_error_rounds~,
        fetch_error_code~,
        epoch_end_offset~,
        heartbeat_requests~,
        heartbeat_regexes~,
        heartbeat_errors~,
        heartbeat_partitions~,
        coordinator~,
        metadata_partitions~,
        hide_apis~,
        dtp_requests~,
        dtp_error_rounds~,
        create_topics~,
        delete_topics~,
        create_partitions~,
        delete_records~,
        offset_deletes~,
        cluster_ops~,
      )
    }
  }
}

///|
async test "describe topic partitions follows the pagination cursor" {
  @async.with_task_group() <| root => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    let accepted : Array[@socket.Tcp] = []
    root.spawn_bg(no_wait=true, allow_failure=true) <| () => {
      fake_broker(server, root, "events", accepted)
    }
    let conn = @moonkafka.BrokerConnection::connect(
      "127.0.0.1",
      server.addr.port(),
      timeout_ms=5000,
    )
    defer conn.close()
    conn.negotiate_api_versions(timeout_ms=5000)
    // Page one returns partition 0 plus a cursor at partition 1; page two
    // returns partition 1 and completes the listing.
    let page1 = conn.describe_topic_partitions(
      ["events"],
      response_partition_limit=1,
      timeout_ms=5000,
    )
    let topic1 = page1.topics[0]
    assert_eq(topic1.name, "events")
    assert_eq(topic1.partitions.length(), 1)
    assert_eq(topic1.partitions[0].index, 0)
    guard page1.next_cursor is Some(cursor) else {
      abort("page one must carry a next cursor")
    }
    assert_eq(cursor.topic_name, "events")
    assert_eq(cursor.partition_index, 1)
    let page2 = conn.describe_topic_partitions(
      ["events"],
      cursor=Some(cursor),
      timeout_ms=5000,
    )
    let topic2 = page2.topics[0]
    assert_eq(topic2.partitions.length(), 1)
    assert_eq(topic2.partitions[0].index, 1)
    assert_true(page2.next_cursor is None)
    server.close()
  }
}

///|
async test "producer authenticates with SCRAM-SHA-256" {
  @async.with_task_group() <| root => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    let accepted : Array[@socket.Tcp] = []
    root.spawn_bg(no_wait=true, allow_failure=true) <| () => {
      fake_broker(server, root, "events", accepted)
    }
    let producer = @moonkafka.Producer::connect_with_config(
      group=root,
      @moonkafka.ProducerConfig::new(
        ["127.0.0.1:\{server.addr.port()}"],
        "events",
        acks=1,
        request_timeout_ms=10000,
        security_protocol=SaslPlaintext,
        sasl=Some({
          mechanism: ScramSha256,
          username: "admin",
          password: "secret",
          oauth_token_provider: None,
        }),
      ),
    )
    defer producer.close()
    try {
      let offset = producer.send(value=b"hello")
      assert_eq(offset, 42L)
    } catch {
      @moonkafka.SaslError(msg) => abort("SCRAM send failed: \{msg}")
      e => abort("SCRAM send failed: \{e}")
    }
    server.close()
  }
}

///|
async test "consumer polls over an incremental fetch session" {
  @async.with_task_group() <| root => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    let accepted : Array[@socket.Tcp] = []
    let fetch_epochs : Array[Int] = []
    root.spawn_bg(no_wait=true, allow_failure=true) <| () => {
      fake_broker(server, root, "events", accepted, fetch_epochs~)
    }
    let consumer = @moonkafka.Consumer::connect_with_config(
      group=root,
      @moonkafka.ConsumerConfig::new(
        ["127.0.0.1:\{server.addr.port()}"],
        "events",
        start_from=Earliest,
        request_timeout_ms=10000,
      ),
    )
    defer consumer.close()
    // First poll: sessionless full fetch (epoch 0) at the resolved offset.
    let first = consumer.poll(max_wait_ms=200)
    assert_eq(first.length(), 2)
    assert_eq(first[0].offset, 42L)
    assert_eq(first[1].offset, 43L)
    // Second poll: the position moved, so an incremental request (epoch 1)
    // carries the partition again under the granted session id.
    let second = consumer.poll(max_wait_ms=200)
    assert_eq(second.length(), 2)
    assert_eq(fetch_epochs, [0, 1])
    server.close()
  }
}

///|
async test "producer manual partition override reaches the broker" {
  @async.with_task_group() <| root => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    let accepted : Array[@socket.Tcp] = []
    let produce_partitions : Array[Int] = []
    root.spawn_bg(no_wait=true, allow_failure=true) <| () => {
      fake_broker(server, root, "events", accepted, produce_partitions~)
    }
    let producer = @moonkafka.Producer::connect_with_config(
      group=root,
      @moonkafka.ProducerConfig::new(
        ["127.0.0.1:\{server.addr.port()}"],
        "events",
        acks=1,
        request_timeout_ms=10000,
      ),
    )
    defer producer.close()
    // The fake topic has a single partition (index 0): the override pins
    // the record to it, and an out-of-range one fails before the wire.
    assert_eq(producer.send(value=b"pinned", partition=0), 42L)
    assert_eq(produce_partitions.length(), 1)
    assert_eq(produce_partitions[0], 0)
    try {
      let _ = producer.send(value=b"x", partition=3)
      abort("out-of-range partition override must fail")
    } catch {
      _ => ()
    }
    assert_eq(produce_partitions.length(), 1)
    server.close()
  }
}

///|
async test "accumulator coalesces concurrent sends into one batch" {
  @async.with_task_group() <| root => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    let accepted : Array[@socket.Tcp] = []
    let produce_partitions : Array[Int] = []
    let produce_record_counts : Array[Int] = []
    root.spawn_bg(no_wait=true, allow_failure=true) <| () => {
      fake_broker(
        server,
        root,
        "events",
        accepted,
        produce_partitions~,
        produce_record_counts~,
      )
    }
    let producer = @moonkafka.Producer::connect_with_config(
      group=root,
      @moonkafka.ProducerConfig::new(
        ["127.0.0.1:\{server.addr.port()}"],
        "events",
        acks=1,
        request_timeout_ms=10000,
        linger_ms=50,
      ),
    )
    defer producer.close()
    // Two concurrent sends land in the same open batch during the linger
    // window and leave as one Produce request; each takes base + index.
    let offsets : Array[Int64] = []
    root.spawn_bg(no_wait=true, allow_failure=true) <| () => {
      offsets.push(producer.send(value=b"first"))
    }
    root.spawn_bg(no_wait=true, allow_failure=true) <| () => {
      offsets.push(producer.send(value=b"second"))
    }
    for _ in 0..<1000 {
      if offsets.length() == 2 {
        break
      }
      @async.sleep(5)
    }
    assert_eq(offsets.length(), 2)
    offsets.sort()
    assert_eq(offsets[0], 42L)
    assert_eq(offsets[1], 43L)
    assert_eq(produce_partitions.length(), 1)
    assert_eq(produce_partitions[0], 0)
    assert_eq(produce_record_counts.length(), 1)
    assert_eq(produce_record_counts[0], 2)
    server.close()
  }
}

///|
async test "cluster client resolves coordinators, pools connections, refreshes on staleness" {
  @async.with_task_group() <| root => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    let accepted : Array[@socket.Tcp] = []
    let metadata_requests : Array[Int] = []
    root.spawn_bg(no_wait=true, allow_failure=true) <| () => {
      fake_broker(server, root, "events", accepted, metadata_requests~)
    }
    let cluster = @moonkafka.ClusterClient::connect(
      [@moonkafka.HostPort::parse("127.0.0.1:\{server.addr.port()}")],
      request_timeout_ms=10000,
      metadata_max_age_ms=60000,
    )
    defer cluster.close()
    // Batched coordinator lookup: one entry per requested key, all on
    // the fake cluster's node 0 at the broker port.
    let coords = cluster.coordinator(["orders", "payments"], @moonkafka.Group)
    assert_eq(coords.length(), 2)
    assert_eq(coords.get("orders").unwrap().node_id, 0)
    assert_eq(coords.get("payments").unwrap().key, "payments")
    assert_eq(coords.get("payments").unwrap().port, server.addr.port())
    // Metadata first: the pool refuses to dial unknown node ids.
    cluster.refresh_metadata(Some(["events"]))
    assert_true(cluster.topic("events") is Some(_))
    // Pooled leader connection: node 0 dials once, the second request
    // reuses it — two accepts total with the control connection.
    let _ = cluster.connection(0)
    let _ = cluster.connection(0)
    assert_eq(accepted.length(), 2)
    // refresh_if_stale on a fresh cache skips; an invalidated (error
    // trigger) cache refreshes through the existing control connection.
    cluster.refresh_if_stale()
    assert_eq(metadata_requests.length(), 1)
    cluster.invalidate_metadata()
    cluster.refresh_if_stale()
    assert_eq(metadata_requests.length(), 2)
    cluster.refresh_if_stale()
    assert_eq(metadata_requests.length(), 2)
    // no extra dials from any of it
    assert_eq(accepted.length(), 2)
    server.close()
  }
}

///|
async test "sender retries retriable produce errors within the delivery window" {
  @async.with_task_group() <| root => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    let accepted : Array[@socket.Tcp] = []
    let produce_partitions : Array[Int] = []
    root.spawn_bg(no_wait=true, allow_failure=true) <| () => {
      fake_broker(
        server,
        root,
        "events",
        accepted,
        produce_partitions~,
        produce_error_rounds=2,
      )
    }
    let producer = @moonkafka.Producer::connect_with_config(
      group=root,
      @moonkafka.ProducerConfig::new(
        ["127.0.0.1:\{server.addr.port()}"],
        "events",
        acks=1,
        request_timeout_ms=10000,
      ),
    )
    defer producer.close()
    // The first two rounds answer LEADER_NOT_AVAILABLE; the sender
    // requeues and recovers, and the third round acknowledges.
    assert_eq(producer.send(value=b"after retries"), 42L)
    assert_eq(produce_partitions.length(), 3)
    server.close()
  }
}

///|
async test "sender writes acks=0 produce without reading a response" {
  @async.with_task_group() <| root => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    let accepted : Array[@socket.Tcp] = []
    let produce_partitions : Array[Int] = []
    let produce_record_counts : Array[Int] = []
    root.spawn_bg(no_wait=true, allow_failure=true) <| () => {
      fake_broker(
        server,
        root,
        "events",
        accepted,
        produce_partitions~,
        produce_record_counts~,
      )
    }
    let producer = @moonkafka.Producer::connect_with_config(
      group=root,
      @moonkafka.ProducerConfig::new(
        ["127.0.0.1:\{server.addr.port()}"],
        "events",
        acks=0,
        request_timeout_ms=10000,
      ),
    )
    defer producer.close()
    // acks=0 has no broker-assigned offset: send resolves with -1 once
    // the sender wrote the frame.
    assert_eq(producer.send(value=b"fire and forget"), -1L)
    for _ in 0..<1000 {
      if produce_partitions.length() == 1 {
        break
      }
      @async.sleep(5)
    }
    assert_eq(produce_partitions[0], 0)
    assert_eq(produce_record_counts[0], 1)
    server.close()
  }
}

///|
/// True when awaiting the handle raises (cancellation surfaces as an error).
async fn await_raises(handle : @moonkafka.SendHandle) -> Bool {
  let _ = handle.await() catch {
    @moonkafka.ProtocolError(_) => return true
    e => raise e
  }
  false
}

///|
async test "send handles await, batch, cancel, and call back" {
  @async.with_task_group() <| root => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    let accepted : Array[@socket.Tcp] = []
    let produce_record_counts : Array[Int] = []
    root.spawn_bg(no_wait=true, allow_failure=true) <| () => {
      fake_broker(server, root, "events", accepted, produce_record_counts~)
    }
    let producer = @moonkafka.Producer::connect_with_config(
      group=root,
      @moonkafka.ProducerConfig::new(
        ["127.0.0.1:\{server.addr.port()}"],
        "events",
        acks=1,
        request_timeout_ms=10000,
        linger_ms=20,
      ),
    )
    defer producer.close()
    // send_all: three records appended in one call, resolved by handle
    let handles = producer.send_all([
      @moonkafka.SendRecord::of(value=b"a"),
      @moonkafka.SendRecord::of(value=b"b"),
      @moonkafka.SendRecord::of(value=b"c"),
    ])
    assert_eq(handles.length(), 3)
    let offsets : Array[Int64] = []
    for handle in handles {
      offsets.push(handle.await())
    }
    offsets.sort()
    assert_eq(offsets, [42L, 43L, 44L])
    // coalesced into one produce request
    assert_eq(produce_record_counts.length(), 1)
    assert_eq(produce_record_counts[0], 3)
    // completion callback fires with the sent offset
    let callback_results : Array[@moonkafka.SendResult] = []
    let late = producer.send_handle(value=b"cb")
    late.on_complete(fn(result) { callback_results.push(result) })
    for _ in 0..<1000 {
      if callback_results.length() == 1 {
        break
      }
      @async.sleep(5)
    }
    assert_eq(callback_results.length(), 1)
    assert_true(callback_results[0] is @moonkafka.Sent(_))
    // cancel stops the wait: await raises, the record still drains at close
    let doomed = producer.send_handle(value=b"doomed")
    doomed.cancel()
    assert_true(await_raises(doomed))
    server.close()
  }
}

///|
async test "idempotent producer stamps pid/epoch/sequence and recovers epoch" {
  @async.with_task_group() <| root => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    let accepted : Array[@socket.Tcp] = []
    let produce_identities : Array[(Int64, Int, Int)] = []
    root.spawn_bg(no_wait=true, allow_failure=true) <| () => {
      fake_broker(
        server,
        root,
        "events",
        accepted,
        produce_identities~,
        unknown_producer_at=2,
      )
    }
    let producer = @moonkafka.Producer::connect_with_config(
      group=root,
      @moonkafka.ProducerConfig::new(
        ["127.0.0.1:\{server.addr.port()}"],
        "events",
        acks=-1,
        request_timeout_ms=10000,
        enable_idempotence=Some(true),
      ),
    )
    defer producer.close()
    // connect ran InitProducerId: the fake assigns pid 500, epoch 0
    assert_eq(producer.producer_id, 500L)
    assert_eq(producer.producer_epoch, 0)
    // first record: batch header carries (500, 0, seq 0)
    assert_eq(producer.send(value=b"one"), 42L)
    // second record's produce answers UNKNOWN_PRODUCER_ID: the sender
    // bumps the epoch (fake returns epoch 1), resets sequences to zero,
    // re-stamps, and retries
    assert_eq(producer.send(value=b"two"), 42L)
    assert_eq(producer.producer_epoch, 1)
    assert_eq(produce_identities.length(), 3)
    assert_eq(produce_identities[0], (500L, 0, 0))
    assert_eq(produce_identities[1], (500L, 0, 1))
    assert_eq(produce_identities[2], (500L, 1, 0))
    server.close()
  }
}

///|
async test "transactional producer commits and keeps the identity moving" {
  @async.with_task_group() <| root => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    let accepted : Array[@socket.Tcp] = []
    let produce_identities : Array[(Int64, Int, Int)] = []
    let add_txn_requests : Array[(String, Array[(String, Array[Int])])] = []
    let end_txn_requests : Array[(String, Int64, Int, Bool)] = []
    root.spawn_bg(no_wait=true, allow_failure=true) <| () => {
      fake_broker(
        server,
        root,
        "events",
        accepted,
        produce_identities~,
        add_txn_requests~,
        end_txn_requests~,
      )
    }
    let producer = @moonkafka.Producer::connect_with_config(
      group=root,
      @moonkafka.ProducerConfig::new(
        ["127.0.0.1:\{server.addr.port()}"],
        "events",
        acks=-1,
        request_timeout_ms=10000,
        transactional_id=Some("tid-1"),
        transaction_timeout_ms=30000,
      ),
    )
    defer producer.close()
    // connect resolved the coordinator and ran InitProducerId there
    assert_eq(producer.producer_id, 500L)
    assert_eq(producer.producer_epoch, 0)
    // sends outside a transaction are a programming error
    try {
      let _ = producer.send(value=b"outside")
      abort("send before begin_transaction must fail")
    } catch {
      _ => ()
    }
    // first transaction: AddPartitionsToTxn precedes the produce, EndTxn
    // commits, and the coordinator bumps the epoch (fake mirrors KIP-890
    // part 2 by returning epoch + 1)
    producer.begin_transaction()
    assert_eq(producer.send(value=b"one"), 42L)
    assert_eq(producer.send(value=b"two"), 42L)
    producer.commit_transaction()
    assert_eq(add_txn_requests, [("tid-1", [("events", [0])])])
    assert_eq(end_txn_requests, [("tid-1", 500L, 0, true)])
    assert_eq(produce_identities.length(), 2)
    assert_eq(produce_identities[0], (500L, 0, 0))
    assert_eq(produce_identities[1], (500L, 0, 1))
    assert_eq(producer.producer_epoch, 1)
    // sends outside a transaction stay rejected after commit
    try {
      let _ = producer.send(value=b"outside")
      abort("send after commit must fail")
    } catch {
      _ => ()
    }
    // second transaction: the identity continues (epoch 1, sequence 2)
    producer.begin_transaction()
    assert_eq(producer.send(value=b"three"), 42L)
    producer.commit_transaction()
    assert_eq(add_txn_requests.length(), 2)
    assert_eq(add_txn_requests[1], ("tid-1", [("events", [0])]))
    assert_eq(end_txn_requests[1], ("tid-1", 500L, 1, true))
    assert_eq(produce_identities.length(), 3)
    assert_eq(produce_identities[2], (500L, 1, 2))
    assert_eq(producer.producer_epoch, 2)
    server.close()
  }
}

///|
async test "transactional producer aborts discard the records" {
  @async.with_task_group() <| root => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    let accepted : Array[@socket.Tcp] = []
    let end_txn_requests : Array[(String, Int64, Int, Bool)] = []
    root.spawn_bg(no_wait=true, allow_failure=true) <| () => {
      fake_broker(server, root, "events", accepted, end_txn_requests~)
    }
    let producer = @moonkafka.Producer::connect_with_config(
      group=root,
      @moonkafka.ProducerConfig::new(
        ["127.0.0.1:\{server.addr.port()}"],
        "events",
        acks=-1,
        request_timeout_ms=10000,
        transactional_id=Some("tid-2"),
      ),
    )
    defer producer.close()
    producer.begin_transaction()
    assert_eq(producer.send(value=b"doomed"), 42L)
    producer.abort_transaction()
    assert_eq(end_txn_requests, [("tid-2", 500L, 0, false)])
    // the aborted transaction released the identity bump too
    assert_eq(producer.producer_epoch, 1)
    // and the producer is ready for the next transaction
    producer.begin_transaction()
    producer.commit_transaction()
    assert_eq(end_txn_requests.length(), 2)
    assert_eq(end_txn_requests[1], ("tid-2", 500L, 1, true))
    server.close()
  }
}

///|
async test "failed sends poison the commit; abort still lands" {
  @async.with_task_group() <| root => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    let accepted : Array[@socket.Tcp] = []
    let end_txn_requests : Array[(String, Int64, Int, Bool)] = []
    root.spawn_bg(no_wait=true, allow_failure=true) <| () => {
      fake_broker(
        server,
        root,
        "events",
        accepted,
        end_txn_requests~,
        produce_error_code=47, // INVALID_PRODUCER_EPOCH: terminal
      )
    }
    let producer = @moonkafka.Producer::connect_with_config(
      group=root,
      @moonkafka.ProducerConfig::new(
        ["127.0.0.1:\{server.addr.port()}"],
        "events",
        acks=-1,
        request_timeout_ms=10000,
        transactional_id=Some("tid-3"),
      ),
    )
    defer producer.close()
    producer.begin_transaction()
    try {
      let _ = producer.send(value=b"lost")
      abort("send against a fenced broker must fail")
    } catch {
      _ => ()
    }
    // the abort-on-error policy: commit refuses to end the transaction
    // while a record failed inside it
    try {
      producer.commit_transaction()
      abort("commit after a failed send must fail")
    } catch {
      _ => ()
    }
    assert_true(end_txn_requests.is_empty())
    // abort ends the transaction cleanly
    producer.abort_transaction()
    assert_eq(end_txn_requests, [("tid-3", 500L, 0, false)])
    server.close()
  }
}

///|
async test "commit retries a moved coordinator and raises on fencing" {
  @async.with_task_group() <| root => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    let accepted : Array[@socket.Tcp] = []
    let end_txn_requests : Array[(String, Int64, Int, Bool)] = []
    root.spawn_bg(no_wait=true, allow_failure=true) <| () => {
      fake_broker(
        server,
        root,
        "events",
        accepted,
        end_txn_requests~,
        end_txn_error_code=16, // NOT_COORDINATOR
        end_txn_error_rounds=1,
      )
    }
    let producer = @moonkafka.Producer::connect_with_config(
      group=root,
      @moonkafka.ProducerConfig::new(
        ["127.0.0.1:\{server.addr.port()}"],
        "events",
        acks=-1,
        request_timeout_ms=10000,
        transactional_id=Some("tid-4"),
      ),
    )
    defer producer.close()
    producer.begin_transaction()
    assert_eq(producer.send(value=b"retry"), 42L)
    // the first EndTxn answers NOT_COORDINATOR; the retry re-resolves and
    // commits
    producer.commit_transaction()
    assert_eq(end_txn_requests.length(), 2)
    assert_eq(end_txn_requests[1], ("tid-4", 500L, 0, true))
    server.close()
  }
}

///|
async test "transactional offset commits register the group and land at it" {
  @async.with_task_group() <| root => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    let accepted : Array[@socket.Tcp] = []
    let end_txn_requests : Array[(String, Int64, Int, Bool)] = []
    let add_offsets_requests : Array[(String, String)] = []
    let txn_offset_requests : Array[(String, String, Array[(Int, Int64)])] = []
    root.spawn_bg(no_wait=true, allow_failure=true) <| () => {
      fake_broker(
        server,
        root,
        "events",
        accepted,
        add_offsets_requests~,
        txn_offset_requests~,
        end_txn_requests~,
      )
    }
    let producer = @moonkafka.Producer::connect_with_config(
      group=root,
      @moonkafka.ProducerConfig::new(
        ["127.0.0.1:\{server.addr.port()}"],
        "events",
        acks=-1,
        request_timeout_ms=10000,
        transactional_id=Some("tid-5"),
      ),
    )
    defer producer.close()
    // offset commits need an active transaction
    try {
      producer.send_offsets_to_transaction(
        [("events", [{ partition: 0, offset: 42L, }])],
        "gid-1",
      )
      abort("send_offsets_to_transaction before begin must fail")
    } catch {
      _ => ()
    }
    producer.begin_transaction()
    producer.send_offsets_to_transaction(
      [("events", [{ partition: 0, offset: 42L, }])],
      "gid-1",
    )
    assert_eq(add_offsets_requests, [("tid-5", "gid-1")])
    assert_eq(txn_offset_requests, [("tid-5", "gid-1", [(0, 42L)])])
    producer.commit_transaction()
    assert_eq(end_txn_requests, [("tid-5", 500L, 0, true)])
    server.close()
  }
}

///|
async test "consumer commits positions and fetches them back" {
  @async.with_task_group() <| root => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    let accepted : Array[@socket.Tcp] = []
    let offset_commit_requests : Array[(String, Int, Int64)] = []
    let committed_store : Map[Int, Int64] = Map([])
    root.spawn_bg(no_wait=true, allow_failure=true) <| () => {
      fake_broker(
        server,
        root,
        "events",
        accepted,
        offset_commit_requests~,
        committed_store~,
      )
    }
    let consumer = @moonkafka.Consumer::connect_with_config(
      group=root,
      @moonkafka.ConsumerConfig::new(
        ["127.0.0.1:\{server.addr.port()}"],
        "events",
        request_timeout_ms=10000,
        group_id=Some("gid-1"),
        enable_auto_commit=false,
      ),
    )
    defer consumer.close()
    // Nothing committed yet: the fetch reports -1 per partition.
    assert_eq(consumer.committed().get(0), Some(-1L))
    // Poll resolves records at 42/43; the position lands on 44 and the
    // explicit commit sends exactly that.
    let records = consumer.poll(max_wait_ms=50)
    assert_eq(records.length(), 2)
    consumer.commit()
    assert_eq(offset_commit_requests, [("gid-1", 0, 44L)])
    assert_eq(consumer.committed_cached(0), Some(44L))
    // The coordinator now answers 44 for partition 0.
    assert_eq(consumer.committed().get(0), Some(44L))
    // An explicit offset map overrides the positions.
    let manual : Map[Int, Int64] = Map([(0, 30L)])
    consumer.commit(offsets=manual)
    assert_eq(offset_commit_requests.length(), 2)
    assert_eq(offset_commit_requests[1], ("gid-1", 0, 30L))
    server.close()
  }
}

///|
async test "commit retries coordinator load and async commit calls back" {
  @async.with_task_group() <| root => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    let accepted : Array[@socket.Tcp] = []
    let offset_commit_requests : Array[(String, Int, Int64)] = []
    let committed_store : Map[Int, Int64] = Map([])
    root.spawn_bg(no_wait=true, allow_failure=true) <| () => {
      fake_broker(
        server,
        root,
        "events",
        accepted,
        offset_commit_requests~,
        committed_store~,
        offset_commit_error_rounds=1,
      )
    }
    let consumer = @moonkafka.Consumer::connect_with_config(
      group=root,
      @moonkafka.ConsumerConfig::new(
        ["127.0.0.1:\{server.addr.port()}"],
        "events",
        request_timeout_ms=10000,
        group_id=Some("gid-2"),
        enable_auto_commit=false,
      ),
    )
    defer consumer.close()
    let _ = consumer.poll(max_wait_ms=50)
    // The first commit answers COORDINATOR_LOAD_IN_PROGRESS; the retry
    // succeeds without surfacing an error.
    consumer.commit()
    assert_eq(offset_commit_requests.length(), 2)
    assert_eq(committed_store.get(0), Some(44L))
    // The async commit runs in the consumer's task group and reports
    // success through the callback.
    let outcomes : Array[String?] = []
    consumer.commit_async(on_complete=fn(msg) { outcomes.push(msg) })
    for _ in 0..<1000 {
      if outcomes.length() == 1 {
        break
      }
      @async.sleep(5)
    }
    assert_eq(outcomes, [None])
    server.close()
  }
}

///|
async test "seeks move the fetch position via offsets and sentinels" {
  @async.with_task_group() <| root => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    let accepted : Array[@socket.Tcp] = []
    let fetch_offsets : Array[Int64] = []
    root.spawn_bg(no_wait=true, allow_failure=true) <| () => {
      fake_broker(server, root, "events", accepted, fetch_offsets~)
    }
    let consumer = @moonkafka.Consumer::connect_with_config(
      group=root,
      @moonkafka.ConsumerConfig::new(
        ["127.0.0.1:\{server.addr.port()}"],
        "events",
        request_timeout_ms=10000,
      ),
    )
    defer consumer.close()
    // Explicit seek: the next fetch asks for that offset.
    consumer.seek(0, 100L)
    let _ = consumer.poll(max_wait_ms=50)
    assert_eq(fetch_offsets.length(), 1)
    assert_eq(fetch_offsets[0], 100L)
    // Seek by timestamp resolves through ListOffsets (fake: 42).
    consumer.seek_by_timestamp(0, 1700000000000L)
    let _ = consumer.poll(max_wait_ms=50)
    assert_eq(fetch_offsets[1], 42L)
    // A different explicit seek moves again (the incremental fetch
    // session only re-sends partitions whose position changed).
    consumer.seek(0, 1234L)
    let _ = consumer.poll(max_wait_ms=50)
    assert_eq(fetch_offsets.length(), 3)
    assert_eq(fetch_offsets[2], 1234L)
    // Seek to beginning resolves through ListOffsets to 42, which
    // differs from the last-sent 1234, so the fetch goes out.
    consumer.seek_to_beginning()
    let records = consumer.poll(max_wait_ms=50)
    assert_eq(fetch_offsets.length(), 4)
    assert_eq(fetch_offsets[3], 42L)
    assert_eq(records.length(), 2)
    // Seek to end resolves to the same 42 the session already heard, so
    // the next poll sends nothing (a broken seek would re-send and grow
    // the log). The empty result proves the position moved to 42.
    consumer.seek_to_end()
    let empty = consumer.poll(max_wait_ms=50)
    assert_eq(fetch_offsets.length(), 4)
    assert_eq(empty.length(), 0)
    // Unknown partitions are rejected before touching the wire.
    try {
      consumer.seek(3, 0L)
      abort("seek to unknown partition must fail")
    } catch {
      _ => ()
    }
    server.close()
  }
}

///|
async test "autocommit commits on the interval and on close" {
  @async.with_task_group() <| root => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    let accepted : Array[@socket.Tcp] = []
    let offset_commit_requests : Array[(String, Int, Int64)] = []
    let committed_store : Map[Int, Int64] = Map([])
    root.spawn_bg(no_wait=true, allow_failure=true) <| () => {
      fake_broker(
        server,
        root,
        "events",
        accepted,
        offset_commit_requests~,
        committed_store~,
      )
    }
    let consumer = @moonkafka.Consumer::connect_with_config(
      group=root,
      @moonkafka.ConsumerConfig::new(
        ["127.0.0.1:\{server.addr.port()}"],
        "events",
        request_timeout_ms=10000,
        group_id=Some("gid-3"),
        enable_auto_commit=true,
        auto_commit_interval_ms=60,
      ),
    )
    // Poll gives the loop something to commit.
    let _ = consumer.poll(max_wait_ms=50)
    // Wait out one interval: a background commit lands on its own.
    for _ in 0..<1000 {
      if offset_commit_requests.length() >= 1 {
        break
      }
      @async.sleep(5)
    }
    assert_eq(offset_commit_requests.length(), 1)
    // Close triggers the final commit before the cluster is torn down.
    consumer.close()
    for _ in 0..<1000 {
      if offset_commit_requests.length() >= 2 {
        break
      }
      @async.sleep(5)
    }
    assert_eq(offset_commit_requests.length(), 2)
    assert_eq(committed_store.get(0), Some(44L))
    server.close()
  }
}

///|
async test "pause skips partitions until resume" {
  @async.with_task_group() <| root => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    let accepted : Array[@socket.Tcp] = []
    let fetch_offsets : Array[Int64] = []
    root.spawn_bg(no_wait=true, allow_failure=true) <| () => {
      fake_broker(server, root, "events", accepted, fetch_offsets~)
    }
    let consumer = @moonkafka.Consumer::connect_with_config(
      group=root,
      @moonkafka.ConsumerConfig::new(
        ["127.0.0.1:\{server.addr.port()}"],
        "events",
        request_timeout_ms=10000,
      ),
    )
    defer consumer.close()
    // Paused partitions are not fetched at all.
    consumer.pause()
    assert_eq(consumer.paused(), [0])
    let empty = consumer.poll(max_wait_ms=50)
    assert_eq(empty.length(), 0)
    assert_eq(fetch_offsets.length(), 0)
    // Resume puts the partition back on the wire at its kept position.
    consumer.resume()
    assert_eq(consumer.paused().length(), 0)
    let records = consumer.poll(max_wait_ms=50)
    assert_eq(records.length(), 2)
    assert_eq(fetch_offsets.length(), 1)
    server.close()
  }
}

///|
async test "max_poll_records caps the delivery and rewinds the position" {
  @async.with_task_group() <| root => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    let accepted : Array[@socket.Tcp] = []
    let fetch_offsets : Array[Int64] = []
    root.spawn_bg(no_wait=true, allow_failure=true) <| () => {
      fake_broker(server, root, "events", accepted, fetch_offsets~)
    }
    let consumer = @moonkafka.Consumer::connect_with_config(
      group=root,
      @moonkafka.ConsumerConfig::new(
        ["127.0.0.1:\{server.addr.port()}"],
        "events",
        request_timeout_ms=10000,
        max_poll_records=1,
      ),
    )
    defer consumer.close()
    // The fake's two records (42/43) arrive in one batch; the cap takes
    // one per poll and rewinds the position to the unconsumed record.
    let first = consumer.poll(max_wait_ms=50)
    assert_eq(first.length(), 1)
    assert_eq(first[0].offset, 42L)
    let second = consumer.poll(max_wait_ms=50)
    assert_eq(second.length(), 1)
    assert_eq(second[0].offset, 42L)
    // The second fetch asked for position 43, not 44: the rewind kept
    // the capped record from being skipped.
    assert_eq(fetch_offsets, [42L, 43L])
    server.close()
  }
}

///|
async test "read_committed filters the aborted transactional batch" {
  @async.with_task_group() <| root => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    let accepted : Array[@socket.Tcp] = []
    root.spawn_bg(no_wait=true, allow_failure=true) <| () => {
      fake_broker(server, root, "events", accepted)
    }
    // Without the flag the records arrive untouched.
    let plain = @moonkafka.Consumer::connect_with_config(
      group=root,
      @moonkafka.ConsumerConfig::new(
        ["127.0.0.1:\{server.addr.port()}"],
        "events",
        request_timeout_ms=10000,
      ),
    )
    defer plain.close()
    assert_eq(plain.poll(max_wait_ms=50).length(), 2)
    plain.close()
    // With read_committed the same fetch comes back empty: the fake
    // answers READ_COMMITTED requests with an aborted transactional
    // batch (producer 500, aborted from offset 42).
    let committed = @moonkafka.Consumer::connect_with_config(
      group=root,
      @moonkafka.ConsumerConfig::new(
        ["127.0.0.1:\{server.addr.port()}"],
        "events",
        request_timeout_ms=10000,
        enable_read_committed=true,
      ),
    )
    defer committed.close()
    assert_eq(committed.poll(max_wait_ms=50).length(), 0)
    server.close()
  }
}

///|
async test "leader epoch errors validate the position and rewind on truncation" {
  @async.with_task_group() <| root => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    let accepted : Array[@socket.Tcp] = []
    let fetch_offsets : Array[Int64] = []
    root.spawn_bg(no_wait=true, allow_failure=true) <| () => {
      fake_broker(
        server,
        root,
        "events",
        accepted,
        fetch_offsets~,
        fetch_error_rounds=1,
        fetch_error_code=75, // UNKNOWN_LEADER_EPOCH
        epoch_end_offset=30L,
      )
    }
    let consumer = @moonkafka.Consumer::connect_with_config(
      group=root,
      @moonkafka.ConsumerConfig::new(
        ["127.0.0.1:\{server.addr.port()}"],
        "events",
        request_timeout_ms=10000,
      ),
    )
    defer consumer.close()
    // The first fetch (position 42 after the earliest reset) answers
    // UNKNOWN_LEADER_EPOCH; the epoch check learns the epoch ends at 30
    // and rewinds, so the retry fetches from 30.
    let _ = consumer.poll(max_wait_ms=50)
    let records = consumer.poll(max_wait_ms=50)
    assert_eq(fetch_offsets, [42L, 30L])
    assert_eq(records.length(), 2)
    server.close()
  }
}

///|
async test "KIP-848 member joins, reconciles assignment, and leaves" {
  @async.with_task_group() <| root => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    let accepted : Array[@socket.Tcp] = []
    let heartbeat_requests : Array[(String, String, Int, String?)] = []
    let assigned : Array[(String, Int)] = []
    let revoked : Array[(String, Int)] = []
    root.spawn_bg(no_wait=true, allow_failure=true) <| () => {
      fake_broker(server, root, "events", accepted, heartbeat_requests~)
    }
    let consumer = @moonkafka.Consumer::connect_with_config(
      group=root,
      @moonkafka.ConsumerConfig::new(
        ["127.0.0.1:\{server.addr.port()}"],
        "events",
        request_timeout_ms=10000,
        group_id=Some("gid-9"),
        enable_auto_commit=false,
      ),
    )
    defer consumer.close()
    // The client generates the member id before the first heartbeat.
    consumer.subscribe(
      ["events"],
      listener=Some(
        @moonkafka.RebalanceListener::new(
          on_assign=fn(pairs) {
            for pair in pairs {
              assigned.push(pair)
            }
          },
          on_revoke=fn(pairs) {
            for pair in pairs {
              revoked.push(pair)
            }
          },
        ),
      ),
    )
    assert_true(consumer.member_identity().length() > 0)
    // The coordinator's assignment lands through the heartbeat loop and
    // fires on_assign.
    for _ in 0..<1000 {
      if assigned.length() == 1 {
        break
      }
      @async.sleep(5)
    }
    assert_eq(assigned, [("events", 0)])
    // Join rides epoch 0; the follow-ups carry the server's epochs and
    // the same member id throughout. Wait out the first interval so the
    // reconciliation heartbeat is on record.
    for _ in 0..<1000 {
      if heartbeat_requests.length() >= 2 {
        break
      }
      @async.sleep(5)
    }
    let epochs = heartbeat_requests.map(fn(r) { r.2 })
    assert_eq(epochs[0], 0)
    assert_true(epochs[1] >= 1)
    assert_eq(heartbeat_requests[0].1, heartbeat_requests[1].1)
    // poll fetches only the assigned partition.
    assert_eq(consumer.poll(max_wait_ms=50).length(), 2)
    // Graceful leave: a final heartbeat with epoch -1 and the revoke.
    consumer.unsubscribe()
    for _ in 0..<1000 {
      if heartbeat_requests[heartbeat_requests.length() - 1].2 == -1 {
        break
      }
      @async.sleep(5)
    }
    assert_eq(heartbeat_requests[heartbeat_requests.length() - 1].2, -1)
    assert_eq(revoked, [("events", 0)])
    server.close()
  }
}

///|
async test "fenced member epoch restarts the join" {
  @async.with_task_group() <| root => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    let accepted : Array[@socket.Tcp] = []
    let heartbeat_requests : Array[(String, String, Int, String?)] = []
    let assigned : Array[(String, Int)] = []
    root.spawn_bg(no_wait=true, allow_failure=true) <| () => {
      fake_broker(
        server,
        root,
        "events",
        accepted,
        heartbeat_requests~,
        heartbeat_errors=[110], // FENCED_MEMBER_EPOCH on the first call
      )
    }
    let consumer = @moonkafka.Consumer::connect_with_config(
      group=root,
      @moonkafka.ConsumerConfig::new(
        ["127.0.0.1:\{server.addr.port()}"],
        "events",
        request_timeout_ms=10000,
        group_id=Some("gid-10"),
        enable_auto_commit=false,
      ),
    )
    defer consumer.close()
    consumer.subscribe(
      ["events"],
      listener=Some(
        @moonkafka.RebalanceListener::new(
          on_assign=fn(pairs) {
            for pair in pairs {
              assigned.push(pair)
            }
          },
          on_revoke=fn(_pairs) { () },
        ),
      ),
    )
    for _ in 0..<1000 {
      if heartbeat_requests.length() >= 3 {
        break
      }
      @async.sleep(5)
    }
    assert_eq(assigned, [("events", 0)])
    assert_true(consumer.membership_error() is None)
    // The join restarted from epoch 0 after the fence and reconciled.
    let epochs = heartbeat_requests.map(fn(r) { r.2 })
    assert_eq(epochs, [0, 0, 1])
    server.close()
  }
}

///|
async test "static membership rides the instance id" {
  @async.with_task_group() <| root => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    let accepted : Array[@socket.Tcp] = []
    let heartbeat_requests : Array[(String, String, Int, String?)] = []
    let assigned : Array[(String, Int)] = []
    root.spawn_bg(no_wait=true, allow_failure=true) <| () => {
      fake_broker(server, root, "events", accepted, heartbeat_requests~)
    }
    let consumer = @moonkafka.Consumer::connect_with_config(
      group=root,
      @moonkafka.ConsumerConfig::new(
        ["127.0.0.1:\{server.addr.port()}"],
        "events",
        request_timeout_ms=10000,
        group_id=Some("gid-11"),
        enable_auto_commit=false,
        group_instance_id=Some("inst-9"),
      ),
    )
    defer consumer.close()
    consumer.subscribe(
      ["events"],
      listener=Some(
        @moonkafka.RebalanceListener::new(
          on_assign=fn(pairs) {
            for pair in pairs {
              assigned.push(pair)
            }
          },
          on_revoke=fn(_pairs) { () },
        ),
      ),
    )
    for _ in 0..<1000 {
      if assigned.length() == 1 {
        break
      }
      @async.sleep(5)
    }
    assert_eq(assigned, [("events", 0)])
    for request in heartbeat_requests {
      assert_eq(request.3, Some("inst-9"))
    }
    server.close()
  }
}

///|
async test "regex subscription rides the heartbeat and expands locally" {
  @async.with_task_group() <| root => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    let accepted : Array[@socket.Tcp] = []
    let heartbeat_requests : Array[(String, String, Int, String?)] = []
    let heartbeat_regexes : Array[String?] = []
    let assigned : Array[(String, Int)] = []
    root.spawn_bg(no_wait=true, allow_failure=true) <| () => {
      fake_broker(
        server,
        root,
        "events",
        accepted,
        heartbeat_requests~,
        heartbeat_regexes~,
      )
    }
    let consumer = @moonkafka.Consumer::connect_with_config(
      group=root,
      @moonkafka.ConsumerConfig::new(
        ["127.0.0.1:\{server.addr.port()}"],
        "events",
        request_timeout_ms=10000,
        group_id=Some("gid-12"),
        enable_auto_commit=false,
      ),
    )
    defer consumer.close()
    consumer.subscribe_regex(
      "ev*",
      listener=Some(
        @moonkafka.RebalanceListener::new(
          on_assign=fn(pairs) {
            for pair in pairs {
              assigned.push(pair)
            }
          },
          on_revoke=fn(_pairs) { () },
        ),
      ),
    )
    for _ in 0..<1000 {
      if assigned.length() == 1 {
        break
      }
      @async.sleep(5)
    }
    // The raw pattern rides v1 heartbeats; the assignment for "events"
    // (expanded from the cached catalog) still lands.
    assert_true(heartbeat_regexes[0] == Some("ev*"))
    assert_eq(assigned, [("events", 0)])
    server.close()
  }
}

///|
async test "classic member joins, syncs the range assignment, and leaves" {
  @async.with_task_group() <| root => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    let accepted : Array[@socket.Tcp] = []
    let coord = FakeCoordinator::new()
    let assigned : Array[(String, Int)] = []
    root.spawn_bg(no_wait=true, allow_failure=true) <| () => {
      fake_broker(server, root, "events", accepted, coordinator=Some(coord))
    }
    let consumer = @moonkafka.Consumer::connect_with_config(
      group=root,
      @moonkafka.ConsumerConfig::new(
        ["127.0.0.1:\{server.addr.port()}"],
        "events",
        request_timeout_ms=10000,
        group_id=Some("gid-c1"),
        enable_auto_commit=false,
        heartbeat_interval_ms=20,
      ),
    )
    defer consumer.close()
    consumer.subscribe_classic(
      ["events"],
      listener=Some(
        @moonkafka.RebalanceListener::new(
          on_assign=fn(pairs) {
            for pair in pairs {
              assigned.push(pair)
            }
          },
          on_revoke=fn(_pairs) { () },
        ),
      ),
    )
    for _ in 0..<1000 {
      if assigned.length() == 1 {
        break
      }
      @async.sleep(5)
    }
    assert_true(consumer.membership_error() is None)
    assert_eq(assigned, [("events", 0)])
    // The member joined at generation 1 under the range protocol and
    // synced before heartbeating.
    assert_eq(coord.joins[0].1, 1)
    assert_eq(coord.protocol, "range")
    assert_true(coord.syncs.length() >= 1)
    assert_eq(coord.syncs[0].1, 1)
    // Graceful leave lands at the coordinator (capture the identity
    // first: unsubscribe clears it once the leave lands).
    let member_id = consumer.member_identity()
    consumer.unsubscribe()
    for _ in 0..<1000 {
      if coord.leaves.length() == 1 {
        break
      }
      @async.sleep(5)
    }
    assert_eq(coord.leaves, [member_id])
    server.close()
  }
}

///|
async test "two classic members split partitions and rebalance on join" {
  @async.with_task_group() <| root => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    let accepted : Array[@socket.Tcp] = []
    let coord = FakeCoordinator::new()
    let assigned_a : Array[(String, Int)] = []
    let revoked_a : Array[(String, Int)] = []
    let assigned_b : Array[(String, Int)] = []
    root.spawn_bg(no_wait=true, allow_failure=true) <| () => {
      fake_broker(
        server,
        root,
        "events",
        accepted,
        coordinator=Some(coord),
        metadata_partitions=2,
      )
    }
    let listener = fn(
      on_assign : Array[((String, Int)) -> Unit],
    ) -> @moonkafka.RebalanceListener {
      ignore(on_assign)
      @moonkafka.RebalanceListener::default()
    }
    ignore(listener)
    let a = @moonkafka.Consumer::connect_with_config(
      group=root,
      @moonkafka.ConsumerConfig::new(
        ["127.0.0.1:\{server.addr.port()}"],
        "events",
        request_timeout_ms=10000,
        group_id=Some("gid-c2"),
        enable_auto_commit=false,
        heartbeat_interval_ms=20,
      ),
    )
    defer a.close()
    a.subscribe_classic(
      ["events"],
      listener=Some(
        @moonkafka.RebalanceListener::new(
          on_assign=fn(pairs) {
            for pair in pairs {
              assigned_a.push(pair)
            }
          },
          on_revoke=fn(pairs) {
            for pair in pairs {
              revoked_a.push(pair)
            }
          },
        ),
      ),
    )
    for _ in 0..<1000 {
      if assigned_a.length() == 2 {
        break
      }
      @async.sleep(5)
    }
    // The lone member takes everything.
    assert_eq(assigned_a, [("events", 0), ("events", 1)])
    // A second member joins: the range assignment splits one each.
    let b = @moonkafka.Consumer::connect_with_config(
      group=root,
      @moonkafka.ConsumerConfig::new(
        ["127.0.0.1:\{server.addr.port()}"],
        "events",
        request_timeout_ms=10000,
        group_id=Some("gid-c2"),
        enable_auto_commit=false,
        heartbeat_interval_ms=20,
      ),
    )
    defer b.close()
    b.subscribe_classic(
      ["events"],
      listener=Some(
        @moonkafka.RebalanceListener::new(
          on_assign=fn(pairs) {
            for pair in pairs {
              assigned_b.push(pair)
            }
          },
          on_revoke=fn(_pairs) { () },
        ),
      ),
    )
    for _ in 0..<1000 {
      if assigned_b.length() == 1 {
        break
      }
      @async.sleep(5)
    }
    // The split is order-agnostic: one partition each, no overlap, and
    // "a" surrendered one of its original two. Wait for the group to
    // converge (transient transport chaos may force extra rounds; the
    // heartbeat loop heals them).
    for _ in 0..<2000 {
      if a.assignment().length() == 1 &&
        b.assignment().length() == 1 &&
        a.assignment()[0].1 != b.assignment()[0].1 {
        break
      }
      @async.sleep(5)
    }
    assert_true(assigned_b.length() >= 1)
    assert_eq(a.assignment().length(), 1)
    assert_eq(b.assignment().length(), 1)
    assert_true(a.assignment()[0].1 != b.assignment()[0].1)
    assert_true(revoked_a.length() >= 1)
    assert_true(revoked_a[0].1 != a.assignment()[0].1)
    server.close()
  }
}

///|
async test "cooperative sticky revokes first and assigns in a second round" {
  @async.with_task_group() <| root => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    let accepted : Array[@socket.Tcp] = []
    let coord = FakeCoordinator::new()
    let assigned_a : Array[(String, Int)] = []
    let revoked_a : Array[(String, Int)] = []
    let assigned_b : Array[(String, Int)] = []
    root.spawn_bg(no_wait=true, allow_failure=true) <| () => {
      fake_broker(
        server,
        root,
        "events",
        accepted,
        coordinator=Some(coord),
        metadata_partitions=2,
      )
    }
    let a = @moonkafka.Consumer::connect_with_config(
      group=root,
      @moonkafka.ConsumerConfig::new(
        ["127.0.0.1:\{server.addr.port()}"],
        "events",
        request_timeout_ms=10000,
        group_id=Some("gid-c3"),
        enable_auto_commit=false,
        heartbeat_interval_ms=20,
      ),
    )
    defer a.close()
    a.subscribe_classic(
      ["events"],
      assignors=[@moonkafka.CooperativeStickyAssignor],
      listener=Some(
        @moonkafka.RebalanceListener::new(
          on_assign=fn(pairs) {
            for pair in pairs {
              assigned_a.push(pair)
            }
          },
          on_revoke=fn(pairs) {
            for pair in pairs {
              revoked_a.push(pair)
            }
          },
        ),
      ),
    )
    for _ in 0..<1000 {
      if assigned_a.length() == 2 {
        break
      }
      @async.sleep(5)
    }
    assert_eq(assigned_a, [("events", 0), ("events", 1)])
    let b = @moonkafka.Consumer::connect_with_config(
      group=root,
      @moonkafka.ConsumerConfig::new(
        ["127.0.0.1:\{server.addr.port()}"],
        "events",
        request_timeout_ms=10000,
        group_id=Some("gid-c3"),
        enable_auto_commit=false,
        heartbeat_interval_ms=20,
      ),
    )
    defer b.close()
    b.subscribe_classic(
      ["events"],
      assignors=[@moonkafka.CooperativeStickyAssignor],
      listener=Some(
        @moonkafka.RebalanceListener::new(
          on_assign=fn(pairs) {
            for pair in pairs {
              assigned_b.push(pair)
            }
          },
          on_revoke=fn(_pairs) { () },
        ),
      ),
    )
    // The cooperative protocol converges: "a" revokes exactly one
    // partition (a separate on_revoke), "b" gains exactly one in the
    // follow-up round, and the final assignment covers both partitions
    // without overlap.
    for _ in 0..<2000 {
      if assigned_b.length() >= 1 &&
        revoked_a.length() >= 1 &&
        a.assignment().length() == 1 &&
        b.assignment().length() == 1 &&
        a.assignment()[0].1 != b.assignment()[0].1 {
        break
      }
      @async.sleep(5)
    }
    assert_true(assigned_b.length() >= 1)
    assert_true(revoked_a.length() >= 1)
    assert_eq(a.assignment().length(), 1)
    assert_eq(b.assignment().length(), 1)
    assert_true(a.assignment()[0].1 != b.assignment()[0].1)
    assert_true(a.membership_error() is None)
    assert_true(b.membership_error() is None)
    server.close()
  }
}

///|
async test "assign pins the fetch set and position tracks reads" {
  @async.with_task_group() <| root => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    let accepted : Array[@socket.Tcp] = []
    let fetch_offsets : Array[Int64] = []
    root.spawn_bg(no_wait=true, allow_failure=true) <| () => {
      fake_broker(
        server,
        root,
        "events",
        accepted,
        fetch_offsets~,
        metadata_partitions=2,
      )
    }
    let consumer = @moonkafka.Consumer::connect_with_config(
      group=root,
      @moonkafka.ConsumerConfig::new(
        ["127.0.0.1:\{server.addr.port()}"],
        "events",
        request_timeout_ms=10000,
      ),
    )
    defer consumer.close()
    // Group-less manual assignment of one partition.
    consumer.assign([("events", 1)])
    let records = consumer.poll(max_wait_ms=50)
    assert_eq(records.length(), 2)
    // Only partition 1 went out on the wire.
    assert_eq(fetch_offsets.length(), 1)
    assert_eq(fetch_offsets[0], 42L)
    assert_eq(consumer.position(1), Some(44L))
    // Unassigning drops the restriction: the whole topic is fetched again.
    consumer.unassign()
    assert_eq(consumer.poll(max_wait_ms=50).length(), 2)
    assert_eq(fetch_offsets.length(), 2)
    server.close()
  }
}

///|
async test "group metadata tracks the member identity" {
  @async.with_task_group() <| root => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    let accepted : Array[@socket.Tcp] = []
    let coord = FakeCoordinator::new()
    let assigned : Array[(String, Int)] = []
    root.spawn_bg(no_wait=true, allow_failure=true) <| () => {
      fake_broker(server, root, "events", accepted, coordinator=Some(coord))
    }
    let consumer = @moonkafka.Consumer::connect_with_config(
      group=root,
      @moonkafka.ConsumerConfig::new(
        ["127.0.0.1:\{server.addr.port()}"],
        "events",
        request_timeout_ms=10000,
        group_id=Some("gid-c9"),
        enable_auto_commit=false,
        heartbeat_interval_ms=20,
      ),
    )
    defer consumer.close()
    // Before subscribing: the group is known, membership is not.
    let empty_metadata : @moonkafka.GroupMetadata? = consumer.group_metadata()
    guard empty_metadata is Some(before) else {
      abort("group_metadata must report the configured group")
    }
    assert_eq(before.group_id, "gid-c9")
    assert_eq(before.member_id, "")
    assert_eq(before.generation_id, -1)
    consumer.subscribe_classic(["events"])
    for _ in 0..<1000 {
      if assigned.length() == 1 {
        break
      }
      @async.sleep(5)
    }
    guard consumer.group_metadata() is Some(after) else {
      abort("group_metadata must stay present after subscribing")
    }
    assert_eq(after.group_id, "gid-c9")
    assert_eq(after.member_id, consumer.member_identity())
    assert_eq(after.generation_id, 1)
    server.close()
  }
}

///|
async test "poll gap past max_poll_interval leaves the group and raises" {
  @async.with_task_group() <| root => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    let accepted : Array[@socket.Tcp] = []
    let coord = FakeCoordinator::new()
    let assigned : Array[(String, Int)] = []
    root.spawn_bg(no_wait=true, allow_failure=true) <| () => {
      fake_broker(server, root, "events", accepted, coordinator=Some(coord))
    }
    let consumer = @moonkafka.Consumer::connect_with_config(
      group=root,
      @moonkafka.ConsumerConfig::new(
        ["127.0.0.1:\{server.addr.port()}"],
        "events",
        request_timeout_ms=10000,
        group_id=Some("gid-c10"),
        enable_auto_commit=false,
        heartbeat_interval_ms=20,
        max_poll_interval_ms=100,
      ),
    )
    defer consumer.close()
    consumer.subscribe_classic(["events"])
    for _ in 0..<1000 {
      if assigned.length() == 1 {
        break
      }
      @async.sleep(5)
    }
    // A poll gap past the window releases the membership and raises:
    // one poll anchors the interval, the long gap blows it.
    let _ = consumer.poll(max_wait_ms=50)
    @async.sleep(200)
    let mut raised = false
    try {
      let _ = consumer.poll(max_wait_ms=50)
    } catch {
      @moonkafka.ProtocolError(_) => raised = true
      e => raise e
    }
    assert_true(raised)
    for _ in 0..<1000 {
      if coord.leaves.length() == 1 {
        break
      }
      @async.sleep(5)
    }
    assert_eq(coord.leaves.length(), 1)
    server.close()
  }
}

///|
async test "records decode through the utf8 helpers" {
  @async.with_task_group() <| root => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    let accepted : Array[@socket.Tcp] = []
    root.spawn_bg(no_wait=true, allow_failure=true) <| () => {
      fake_broker(server, root, "events", accepted)
    }
    let consumer = @moonkafka.Consumer::connect_with_config(
      group=root,
      @moonkafka.ConsumerConfig::new(
        ["127.0.0.1:\{server.addr.port()}"],
        "events",
        request_timeout_ms=10000,
      ),
    )
    defer consumer.close()
    let records = consumer.poll(max_wait_ms=50)
    assert_eq(records.length(), 2)
    assert_eq(records[0].value_utf8(), Some("one"))
    assert_eq(records[1].value_utf8(), Some("two"))
    assert_true(records[0].key_utf8() is None)
    server.close()
  }
}

///|
async test "group_protocol classic routes subscribe through JoinGroup" {
  @async.with_task_group() <| root => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    let accepted : Array[@socket.Tcp] = []
    let coord = FakeCoordinator::new()
    let assigned : Array[(String, Int)] = []
    root.spawn_bg(no_wait=true, allow_failure=true) <| () => {
      fake_broker(server, root, "events", accepted, coordinator=Some(coord))
    }
    let consumer = @moonkafka.Consumer::connect_with_config(
      group=root,
      @moonkafka.ConsumerConfig::new(
        ["127.0.0.1:\{server.addr.port()}"],
        "events",
        request_timeout_ms=10000,
        group_id=Some("gid-p1"),
        enable_auto_commit=false,
        heartbeat_interval_ms=20,
        group_protocol=@moonkafka.ClassicProtocol,
      ),
    )
    defer consumer.close()
    // The plain subscribe() entry honors the configured protocol.
    consumer.subscribe(["events"])
    for _ in 0..<1000 {
      if consumer.assignment().length() == 1 {
        break
      }
      @async.sleep(5)
    }
    assert_eq(consumer.assignment(), [("events", 0)])
    assert_eq(coord.joins[0].1, 1)
    assert_eq(coord.protocol, "range")
    server.close()
  }
}

///|
async test "fallback prefers consumer and degrades to classic" {
  @async.with_task_group() <| root => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    let accepted : Array[@socket.Tcp] = []
    let coord = FakeCoordinator::new()
    let heartbeat_requests : Array[(String, String, Int, String?)] = []
    let assigned : Array[(String, Int)] = []
    root.spawn_bg(no_wait=true, allow_failure=true) <| () => {
      fake_broker(server, root, "events", accepted, coordinator=Some(coord), hide_apis=[
        68,
      ])
    }
    let consumer = @moonkafka.Consumer::connect_with_config(
      group=root,
      @moonkafka.ConsumerConfig::new(
        ["127.0.0.1:\{server.addr.port()}"],
        "events",
        request_timeout_ms=10000,
        group_id=Some("gid-p2"),
        enable_auto_commit=false,
        heartbeat_interval_ms=20,
        group_protocol=@moonkafka.PreferConsumer,
      ),
    )
    defer consumer.close()
    // The broker hides ConsumerGroupHeartbeat: the member degrades to
    // the classic path and still gets its assignment.
    consumer.subscribe(["events"])
    for _ in 0..<1000 {
      if consumer.assignment().length() == 1 {
        break
      }
      @async.sleep(5)
    }
    assert_eq(consumer.assignment(), [("events", 0)])
    assert_true(coord.joins.length() >= 1)
    assert_true(heartbeat_requests.is_empty())
    server.close()
  }
}

///|
async test "fallback prefers consumer over an advertising broker" {
  @async.with_task_group() <| root => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    let accepted : Array[@socket.Tcp] = []
    let coord = FakeCoordinator::new()
    let heartbeat_requests : Array[(String, String, Int, String?)] = []
    let assigned : Array[(String, Int)] = []
    root.spawn_bg(no_wait=true, allow_failure=true) <| () => {
      fake_broker(
        server,
        root,
        "events",
        accepted,
        coordinator=Some(coord),
        heartbeat_requests~,
      )
    }
    let consumer = @moonkafka.Consumer::connect_with_config(
      group=root,
      @moonkafka.ConsumerConfig::new(
        ["127.0.0.1:\{server.addr.port()}"],
        "events",
        request_timeout_ms=10000,
        group_id=Some("gid-p3"),
        enable_auto_commit=false,
        heartbeat_interval_ms=20,
        group_protocol=@moonkafka.PreferConsumer,
      ),
    )
    defer consumer.close()
    // The broker advertises KIP-848: the consumer protocol wins.
    consumer.subscribe(["events"])
    for _ in 0..<1000 {
      if heartbeat_requests.length() >= 1 {
        break
      }
      @async.sleep(5)
    }
    assert_true(heartbeat_requests.length() >= 1)
    assert_true(coord.joins.is_empty())
    server.close()
  }
}

///|
async test "consumer protocol raises without KIP-848 broker support" {
  @async.with_task_group() <| root => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    let accepted : Array[@socket.Tcp] = []
    root.spawn_bg(no_wait=true, allow_failure=true) <| () => {
      fake_broker(server, root, "events", accepted, hide_apis=[68])
    }
    let consumer = @moonkafka.Consumer::connect_with_config(
      group=root,
      @moonkafka.ConsumerConfig::new(
        ["127.0.0.1:\{server.addr.port()}"],
        "events",
        request_timeout_ms=10000,
        group_id=Some("gid-p4"),
        group_protocol=@moonkafka.ConsumerProtocol,
      ),
    )
    defer consumer.close()
    try {
      consumer.subscribe(["events"])
      abort("subscribe must fail without broker KIP-848 support")
    } catch {
      _ => ()
    }
    server.close()
  }
}

///|
async test "admin describes topics and retries retriable results" {
  @async.with_task_group() <| root => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    let accepted : Array[@socket.Tcp] = []
    root.spawn_bg(no_wait=true, allow_failure=true) <| () => {
      fake_broker(server, root, "events", accepted)
    }
    let admin = @moonkafka.Admin::connect(
      group=root,
      host="127.0.0.1",
      port=server.addr.port(),
    )
    defer admin.close()
    // The two-page fake topic merges into one description.
    let topics = admin.describe_topic_partitions(["events"])
    assert_eq(topics.length(), 1)
    assert_eq(topics[0].name, "events")
    assert_eq(topics[0].error_code, 0)
    assert_eq(topics[0].partitions.length(), 2)
    server.close()
  }
}

///|
async test "admin retries retriable topic errors under the policy" {
  @async.with_task_group() <| root => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    let accepted : Array[@socket.Tcp] = []
    let dtp_requests : Array[Int] = []
    root.spawn_bg(no_wait=true, allow_failure=true) <| () => {
      fake_broker(
        server,
        root,
        "events",
        accepted,
        dtp_requests~,
        dtp_error_rounds=2,
      )
    }
    let admin = @moonkafka.Admin::connect(
      group=root,
      host="127.0.0.1",
      port=server.addr.port(),
    )
    defer admin.close()
    // The first two responses carry a retriable per-topic error; the
    // retry policy re-issues until the topic resolves.
    let topics = admin.describe_topic_partitions(["events"])
    assert_eq(topics.length(), 1)
    assert_eq(topics[0].error_code, 0)
    assert_true(dtp_requests.length() >= 3)
    server.close()
  }
}

///|
async test "admin topic lifecycle: create, grow, truncate, delete offsets, delete" {
  @async.with_task_group() <| root => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    let accepted : Array[@socket.Tcp] = []
    let create_topics : Array[(String, Bytes)] = []
    let delete_topics : Array[(String?, Bytes)] = []
    let create_partitions : Array[(String, Int)] = []
    let delete_records : Array[(String, Int, Int64)] = []
    let offset_deletes : Array[(String, String, Int)] = []
    root.spawn_bg(no_wait=true, allow_failure=true) <| () => {
      fake_broker(
        server,
        root,
        "events",
        accepted,
        create_topics~,
        delete_topics~,
        create_partitions~,
        delete_records~,
        offset_deletes~,
      )
    }
    let admin = @moonkafka.Admin::connect(
      group=root,
      host="127.0.0.1",
      port=server.addr.port(),
    )
    defer admin.close()
    // Create with a config override; the fake assigns a fresh topic id.
    let created = admin.create_topics([
      {
        name: "events2",
        num_partitions: 3,
        replication_factor: 1,
        assignments: [],
        configs: [("retention.ms", Some("86400000"))],
      },
    ])
    assert_eq(created.length(), 1)
    assert_eq(created[0].error_code, 0)
    assert_eq(created[0].name, "events2")
    assert_true(created[0].topic_id != @moonkafka.Uuid::zero())
    assert_eq(create_topics.length(), 1)
    // Grow the topic to 5 partitions.
    let grown = admin.create_partitions([
      { name: "events2", count: 5, assignments: None, },
    ])
    assert_eq(grown[0].error_code, 0)
    assert_eq(create_partitions, [("events2", 5)])
    // Truncate partition 0 up to offset 10: the low watermark echoes it.
    let truncated = admin.delete_records([("events2", [(0, 10L)])])
    assert_eq(truncated.length(), 1)
    assert_eq(truncated[0].partition, 0)
    assert_eq(truncated[0].low_watermark, 10L)
    assert_eq(truncated[0].error_code, 0)
    assert_eq(delete_records, [("events2", 0, 10L)])
    // Delete the group's committed offsets for one partition.
    let offsets = admin.offset_delete("gid-x", [("events2", [0, 1])])
    assert_eq(offsets.error_code, 0)
    assert_eq(offsets.partitions.length(), 2)
    assert_eq(offset_deletes, [("gid-x", "events2", 0), ("gid-x", "events2", 1)])
    // Delete by name, then topic-id-first (name null, id set).
    let deleted = admin.delete_topics(["events2"], [])
    assert_eq(deleted.length(), 1)
    assert_eq(deleted[0].name, Some("events2"))
    assert_eq(deleted[0].error_code, 0)
    let deleted_by_id = admin.delete_topics([], [created[0].topic_id])
    assert_eq(deleted_by_id.length(), 1)
    assert_true(deleted_by_id[0].name is None)
    assert_eq(deleted_by_id[0].topic_id, created[0].topic_id)
    assert_eq(deleted_by_id[0].error_code, 0)
    assert_eq(delete_topics.length(), 2)
    server.close()
  }
}

///|
async test "admin describes the cluster and its brokers" {
  @async.with_task_group() <| root => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    let accepted : Array[@socket.Tcp] = []
    let ops = FakeClusterOps::new()
    root.spawn_bg(no_wait=true, allow_failure=true) <| () => {
      fake_broker(server, root, "events", accepted, cluster_ops=ops)
    }
    let admin = @moonkafka.Admin::connect(
      group=root,
      host="127.0.0.1",
      port=server.addr.port(),
    )
    defer admin.close()
    let described = admin.describe_cluster(
      include_authorized_operations=true,
      include_fenced_brokers=true,
    )
    assert_eq(described.error_code, 0)
    assert_eq(described.cluster_id, Some("fake-cluster"))
    assert_eq(described.controller_id, 0)
    assert_eq(described.endpoint_type, @moonkafka.ENDPOINT_TYPE_BROKERS)
    assert_eq(described.brokers.length(), 1)
    assert_eq(described.brokers[0].node_id, 0)
    assert_eq(described.brokers[0].host, "127.0.0.1")
    assert_eq(described.brokers[0].port, server.addr.port())
    assert_true(described.brokers[0].rack is None)
    assert_true(!described.brokers[0].is_fenced)
    // The fake serves the requested-operations bitfield verbatim.
    assert_eq(described.cluster_authorized_operations, -2147483648)
    assert_eq(ops.describe_cluster, [true])
    server.close()
  }
}

///|
async test "admin describes and alters configs" {
  @async.with_task_group() <| root => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    let accepted : Array[@socket.Tcp] = []
    let ops = FakeClusterOps::new()
    root.spawn_bg(no_wait=true, allow_failure=true) <| () => {
      fake_broker(server, root, "events", accepted, cluster_ops=ops)
    }
    let admin = @moonkafka.Admin::connect(
      group=root,
      host="127.0.0.1",
      port=server.addr.port(),
    )
    defer admin.close()
    // Filtered describe of one topic config key.
    let filtered = admin.describe_configs([
      {
        resource_type: @moonkafka.CONFIG_RESOURCE_TOPIC,
        resource_name: "events",
        config_keys: Some(["retention.ms"]),
      },
    ])
    assert_eq(filtered.length(), 1)
    assert_eq(filtered[0].error_code, 0)
    assert_eq(filtered[0].resource_name, "events")
    assert_eq(filtered[0].configs.length(), 1)
    assert_eq(filtered[0].configs[0].name, "retention.ms")
    assert_eq(filtered[0].configs[0].value, Some("604800000"))
    assert_true(!filtered[0].configs[0].read_only)
    assert_true(!filtered[0].configs[0].is_sensitive)
    // A null key filter asks for every key; the fake serves two.
    let all = admin.describe_configs([
      {
        resource_type: @moonkafka.CONFIG_RESOURCE_TOPIC,
        resource_name: "events",
        config_keys: None,
      },
    ])
    assert_eq(all[0].configs.length(), 2)
    assert_eq(ops.describe_configs.length(), 2)
    assert_true(ops.describe_configs[1].2 is None)
    // Incremental set + delete ride AlterableConfig ops.
    let altered = admin.incremental_alter_configs([
      {
        resource_type: @moonkafka.CONFIG_RESOURCE_TOPIC,
        resource_name: "events",
        configs: [
          {
            name: "retention.ms",
            operation: @moonkafka.CONFIG_OP_SET,
            value: Some("86400000"),
          },
          {
            name: "cleanup.policy",
            operation: @moonkafka.CONFIG_OP_DELETE,
            value: None,
          },
        ],
      },
    ])
    assert_eq(altered.length(), 1)
    assert_eq(altered[0].error_code, 0)
    assert_eq(altered[0].resource_type, @moonkafka.CONFIG_RESOURCE_TOPIC)
    assert_eq(ops.incremental_alters.length(), 1)
    assert_eq(ops.incremental_alters[0].2.length(), 2)
    let (key, op, value) = ops.incremental_alters[0].2[0]
    assert_eq(key, "retention.ms")
    assert_eq(op, @moonkafka.CONFIG_OP_SET)
    assert_eq(value, Some("86400000"))
    // Legacy full-replace alter.
    let legacy = admin.alter_configs([
      {
        resource_type: @moonkafka.CONFIG_RESOURCE_BROKER,
        resource_name: "1",
        configs: [{ name: "log.retention.hours", value: Some("48"), }],
      },
    ])
    assert_eq(legacy[0].error_code, 0)
    assert_eq(ops.alter_configs, [
      (
        @moonkafka.CONFIG_RESOURCE_BROKER,
        "1",
        [("log.retention.hours", Some("48"))],
      ),
    ])
    // Config resource listings, unfiltered and broker-only.
    let listed = admin.list_config_resources()
    assert_eq(listed.error_code, 0)
    assert_eq(listed.resources.length(), 2)
    assert_eq(listed.resources[0].resource_name, "events")
    assert_eq(
      listed.resources[0].resource_type,
      @moonkafka.CONFIG_RESOURCE_TOPIC,
    )
    let brokers_only = admin.list_config_resources(resource_types=[
      @moonkafka.CONFIG_RESOURCE_BROKER,
    ])
    assert_eq(brokers_only.error_code, 0)
    assert_eq(ops.list_config_resources, [
      [],
      [@moonkafka.CONFIG_RESOURCE_BROKER],
    ])
    server.close()
  }
}

///|
async test "admin describes log dirs, elects leaders, and reassigns" {
  @async.with_task_group() <| root => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    let accepted : Array[@socket.Tcp] = []
    let ops = FakeClusterOps::new()
    root.spawn_bg(no_wait=true, allow_failure=true) <| () => {
      fake_broker(server, root, "events", accepted, cluster_ops=ops)
    }
    let admin = @moonkafka.Admin::connect(
      group=root,
      host="127.0.0.1",
      port=server.addr.port(),
    )
    defer admin.close()
    // All topics: the fake serves both partitions with sizes.
    let dirs = admin.describe_log_dirs()
    assert_eq(dirs.error_code, 0)
    assert_eq(dirs.results.length(), 1)
    assert_eq(dirs.results[0].log_dir, "/tmp/kafka-logs")
    assert_eq(dirs.results[0].topics.length(), 1)
    assert_eq(dirs.results[0].topics[0].partitions.length(), 2)
    assert_eq(dirs.results[0].topics[0].partitions[1].partition_size, 101L)
    assert_eq(dirs.results[0].total_bytes, 1000000L)
    assert_eq(dirs.results[0].usable_bytes, 500000L)
    assert_true(!dirs.results[0].is_cordoned)
    // Filtered describe carries the requested partitions on the wire.
    let filtered = admin.describe_log_dirs(topics=Some([("events", [0])]))
    assert_eq(filtered.results[0].topics[0].partitions.length(), 1)
    assert_eq(ops.log_dirs.length(), 2)
    assert_true(ops.log_dirs[0] is None)
    // Preferred leader election for explicit partitions.
    let elected = admin.elect_leaders(
      @moonkafka.ELECTION_PREFERRED,
      topic_partitions=Some([("events", [0, 1])]),
    )
    assert_eq(elected.error_code, 0)
    assert_eq(elected.results.length(), 1)
    assert_eq(elected.results[0].partitions.length(), 2)
    assert_eq(elected.results[0].partitions[1].error_code, 0)
    // Unclean election with a null topic list elects everywhere.
    let unclean = admin.elect_leaders(@moonkafka.ELECTION_UNCLEAN)
    assert_eq(unclean.error_code, 0)
    assert_eq(ops.elect_leaders, [
      (@moonkafka.ELECTION_PREFERRED, Some([("events", [0, 1])])),
      (@moonkafka.ELECTION_UNCLEAN, None),
    ])
    // Move partition 0 to replicas [1, 2] and cancel partition 1.
    let reassigned = admin.alter_partition_reassignments([
      {
        name: "events",
        partitions: [
          { partition_index: 0, replicas: Some([1, 2]), },
          { partition_index: 1, replicas: None, },
        ],
      },
    ])
    assert_eq(reassigned.error_code, 0)
    assert_eq(reassigned.responses.length(), 1)
    assert_eq(reassigned.responses[0].1.length(), 2)
    assert_eq(ops.alter_reassignments, [
      ("events", 0, Some([1, 2])),
      ("events", 1, None),
    ])
    // Ongoing reassignments: unfiltered shows the fake's in-flight one.
    let ongoing = admin.list_partition_reassignments()
    assert_eq(ongoing.error_code, 0)
    assert_eq(ongoing.topics.length(), 1)
    let (_, ongoing_partitions) = ongoing.topics[0]
    assert_eq(ongoing_partitions[0].partition_index, 0)
    assert_eq(ongoing_partitions[0].replicas, [1, 2])
    assert_eq(ongoing_partitions[0].adding_replicas, [2])
    assert_eq(ongoing_partitions[0].removing_replicas, [1])
    // A filter that misses the fake's topic yields no results.
    let none = admin.list_partition_reassignments(topics=Some([("other", [0])]))
    assert_eq(none.topics.length(), 0)
    assert_eq(ops.list_reassignments.length(), 2)
    server.close()
  }
}

///|
async test "admin reads the quorum, updates features, unregisters brokers" {
  @async.with_task_group() <| root => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    let accepted : Array[@socket.Tcp] = []
    let ops = FakeClusterOps::new()
    root.spawn_bg(no_wait=true, allow_failure=true) <| () => {
      fake_broker(server, root, "events", accepted, cluster_ops=ops)
    }
    let admin = @moonkafka.Admin::connect(
      group=root,
      host="127.0.0.1",
      port=server.addr.port(),
    )
    defer admin.close()
    let quorum = admin.describe_quorum()
    assert_eq(quorum.error_code, 0)
    assert_eq(quorum.topics.length(), 1)
    assert_eq(quorum.topics[0].topic_name, "__cluster_metadata")
    let partition = quorum.topics[0].partitions[0]
    assert_eq(partition.partition_index, 0)
    assert_eq(partition.leader_id, 0)
    assert_eq(partition.leader_epoch, 5)
    assert_eq(partition.high_watermark, 42L)
    assert_eq(partition.current_voters.length(), 1)
    assert_eq(partition.current_voters[0].replica_id, 0)
    assert_eq(partition.current_voters[0].log_end_offset, 42L)
    assert_eq(partition.current_voters[0].last_fetch_timestamp, -1L)
    assert_eq(quorum.nodes.length(), 1)
    assert_eq(quorum.nodes[0].listeners, [("CONTROLLER", "127.0.0.1", 9093)])
    assert_eq(ops.quorum_requests, [("__cluster_metadata", 0)])
    // Feature update rides the controller route.
    let updated = admin.update_features([
      {
        feature: "metadata.version",
        max_version_level: 22,
        upgrade_type: @moonkafka.FEATURE_UPGRADE,
      },
    ])
    assert_eq(updated.error_code, 0)
    assert_eq(ops.update_features, [
      ("metadata.version", 22, @moonkafka.FEATURE_UPGRADE, false),
    ])
    // A terminal broker error surfaces as a value.
    ops.update_features_error = 95
    let failed = admin.update_features([
      {
        feature: "metadata.version",
        max_version_level: 21,
        upgrade_type: @moonkafka.FEATURE_SAFE_DOWNGRADE,
      },
    ])
    assert_eq(failed.error_code, 95)
    ops.update_features_error = 0
    // Unregistering a fenced broker rides the controller route too.
    let unregistered = admin.unregister_broker(5)
    assert_eq(unregistered.error_code, 0)
    assert_eq(ops.unregister_broker, [5])
    server.close()
  }
}

///|
async test "admin creates, describes, and deletes ACLs" {
  @async.with_task_group() <| root => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    let accepted : Array[@socket.Tcp] = []
    let ops = FakeClusterOps::new()
    root.spawn_bg(no_wait=true, allow_failure=true) <| () => {
      fake_broker(server, root, "events", accepted, cluster_ops=ops)
    }
    let admin = @moonkafka.Admin::connect(
      group=root,
      host="127.0.0.1",
      port=server.addr.port(),
    )
    defer admin.close()
    // Grant alice read on the topic from any host, and alice consume on
    // the group with a prefixed pattern.
    let created = admin.create_acls([
      {
        resource_type: @moonkafka.ACL_RESOURCE_TOPIC,
        resource_name: "events",
        pattern_type: @moonkafka.ACL_PATTERN_LITERAL,
        principal: "User:alice",
        host: "*",
        operation: @moonkafka.ACL_OPERATION_READ,
        permission_type: @moonkafka.ACL_PERMISSION_ALLOW,
      },
      {
        resource_type: @moonkafka.ACL_RESOURCE_GROUP,
        resource_name: "gid-",
        pattern_type: @moonkafka.ACL_PATTERN_PREFIXED,
        principal: "User:alice",
        host: "*",
        operation: @moonkafka.ACL_OPERATION_READ,
        permission_type: @moonkafka.ACL_PERMISSION_ALLOW,
      },
    ])
    assert_eq(created.length(), 2)
    assert_eq(created[0].error_code, 0)
    assert_eq(created[1].error_code, 0)
    assert_eq(ops.create_acls.length(), 2)
    let first = ops.create_acls[0]
    assert_eq(first.resource_type, @moonkafka.ACL_RESOURCE_TOPIC)
    assert_eq(first.resource_name, "events")
    assert_eq(first.pattern_type, @moonkafka.ACL_PATTERN_LITERAL)
    assert_eq(first.principal, "User:alice")
    assert_eq(first.host, "*")
    assert_eq(first.operation, @moonkafka.ACL_OPERATION_READ)
    assert_eq(first.permission_type, @moonkafka.ACL_PERMISSION_ALLOW)
    // Describe the topic's ACLs: any principal/host, any operation.
    let described = admin.describe_acls({
      resource_type: @moonkafka.ACL_RESOURCE_TOPIC,
      resource_name: Some("events"),
      pattern_type: @moonkafka.ACL_PATTERN_LITERAL,
      principal: None,
      host: None,
      operation: @moonkafka.ACL_OPERATION_ANY,
      permission_type: @moonkafka.ACL_PERMISSION_ANY,
    })
    assert_eq(described.error_code, 0)
    assert_eq(described.resources.length(), 1)
    assert_eq(
      described.resources[0].resource_type,
      @moonkafka.ACL_RESOURCE_TOPIC,
    )
    assert_eq(described.resources[0].resource_name, "events")
    assert_eq(
      described.resources[0].pattern_type,
      @moonkafka.ACL_PATTERN_LITERAL,
    )
    assert_eq(described.resources[0].acls.length(), 1)
    assert_eq(described.resources[0].acls[0].principal, "User:alice")
    assert_eq(described.resources[0].acls[0].host, "*")
    assert_eq(
      described.resources[0].acls[0].operation,
      @moonkafka.ACL_OPERATION_READ,
    )
    assert_eq(
      described.resources[0].acls[0].permission_type,
      @moonkafka.ACL_PERMISSION_ALLOW,
    )
    let listed_filter = ops.describe_acls[0]
    assert_eq(listed_filter.resource_type, @moonkafka.ACL_RESOURCE_TOPIC)
    assert_eq(listed_filter.resource_name, Some("events"))
    assert_true(listed_filter.principal is None)
    assert_true(listed_filter.host is None)
    // Delete alice's bindings: the filter matches the canned ACL.
    let deleted = admin.delete_acls([
      {
        resource_type: @moonkafka.ACL_RESOURCE_ANY,
        resource_name: None,
        pattern_type: @moonkafka.ACL_PATTERN_ANY,
        principal: Some("User:alice"),
        host: None,
        operation: @moonkafka.ACL_OPERATION_ANY,
        permission_type: @moonkafka.ACL_PERMISSION_ANY,
      },
    ])
    assert_eq(deleted.length(), 1)
    assert_eq(deleted[0].error_code, 0)
    assert_eq(deleted[0].matching_acls.length(), 1)
    let removed = deleted[0].matching_acls[0]
    assert_eq(removed.error_code, 0)
    assert_eq(removed.binding.resource_name, "events")
    assert_eq(removed.binding.principal, "User:alice")
    assert_eq(ops.delete_acls.length(), 1)
    assert_eq(ops.delete_acls[0].principal, Some("User:alice"))
    server.close()
  }
}

///|
async test "admin describes and alters client quotas and scram credentials" {
  @async.with_task_group() <| root => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    let accepted : Array[@socket.Tcp] = []
    let ops = FakeClusterOps::new()
    root.spawn_bg(no_wait=true, allow_failure=true) <| () => {
      fake_broker(server, root, "events", accepted, cluster_ops=ops)
    }
    let admin = @moonkafka.Admin::connect(
      group=root,
      host="127.0.0.1",
      port=server.addr.port(),
    )
    defer admin.close()
    // Describe the quota of user alice (exact match on the user entity).
    let described = admin.describe_client_quotas({
      components: [
        {
          entity_type: @moonkafka.QUOTA_ENTITY_USER,
          match_type: @moonkafka.QUOTA_MATCH_EXACT,
          matches: Some("alice"),
        },
      ],
      strict: true,
    })
    assert_eq(described.error_code, 0)
    assert_eq(described.entries.length(), 1)
    assert_eq(described.entries[0].entity.length(), 1)
    assert_eq(
      described.entries[0].entity[0].entity_type,
      @moonkafka.QUOTA_ENTITY_USER,
    )
    assert_eq(described.entries[0].entity[0].entity_name, Some("alice"))
    assert_eq(described.entries[0].values.length(), 2)
    assert_eq(described.entries[0].values[0].key, "producer_byte_rate")
    assert_eq(described.entries[0].values[0].value, 1024.0)
    assert_eq(described.entries[0].values[1].key, "consumer_byte_rate")
    assert_eq(described.entries[0].values[1].value, 2048.0)
    let filter = ops.describe_quotas[0]
    assert_eq(filter.components.length(), 1)
    assert_eq(filter.components[0].entity_type, "user")
    assert_eq(filter.components[0].match_type, @moonkafka.QUOTA_MATCH_EXACT)
    assert_eq(filter.components[0].matches, Some("alice"))
    assert_true(filter.strict)
    // Throttle alice's produce rate, then remove it.
    let altered = admin.alter_client_quotas([
      {
        entity: [{ entity_type: "user", entity_name: Some("alice"), }],
        ops: [
          { key: "producer_byte_rate", value: 4096.0, remove: false, },
          { key: "request_percentage", value: 0.0, remove: true, },
        ],
      },
    ])
    assert_eq(altered.length(), 1)
    assert_eq(altered[0].error_code, 0)
    assert_eq(altered[0].entity.length(), 1)
    assert_eq(altered[0].entity[0].entity_name, Some("alice"))
    let alteration = ops.alter_quotas[0]
    assert_eq(alteration.ops.length(), 2)
    assert_eq(alteration.ops[0].key, "producer_byte_rate")
    assert_eq(alteration.ops[0].value, 4096.0)
    assert_true(!alteration.ops[0].remove)
    assert_eq(alteration.ops[1].key, "request_percentage")
    assert_true(alteration.ops[1].remove)
    assert_true(!ops.alter_quotas_validate_only[0])
    // Describe all users' SCRAM credentials.
    let scram = admin.describe_user_scram(None)
    assert_eq(scram.error_code, 0)
    assert_eq(scram.results.length(), 1)
    assert_eq(scram.results[0].user, "alice")
    assert_eq(scram.results[0].credential_infos.length(), 1)
    assert_eq(
      scram.results[0].credential_infos[0].mechanism,
      @moonkafka.SCRAM_MECHANISM_SHA_512,
    )
    assert_eq(scram.results[0].credential_infos[0].iterations, 8192)
    assert_true(ops.describe_scram[0] is None)
    // Delete alice's SHA-256 credential and set one for bob from a
    // plaintext password.
    let results = admin.alter_user_scram(
      [{ name: "alice", mechanism: @moonkafka.SCRAM_MECHANISM_SHA_256, }],
      [
        @moonkafka.ScramCredentialUpsertion::from_password(
          "bob",
          @moonkafka.SCRAM_MECHANISM_SHA_256,
          @utf8.encode("s3cret"),
        ),
      ],
    )
    assert_eq(results.length(), 2)
    assert_eq(results[0].user, "alice")
    assert_eq(results[0].error_code, 0)
    assert_eq(results[1].user, "bob")
    assert_eq(results[1].error_code, 0)
    assert_eq(ops.alter_scram_deletions.length(), 1)
    assert_eq(ops.alter_scram_deletions[0].name, "alice")
    assert_eq(
      ops.alter_scram_deletions[0].mechanism,
      @moonkafka.SCRAM_MECHANISM_SHA_256,
    )
    assert_eq(ops.alter_scram_upsertions.length(), 1)
    let upsertion = ops.alter_scram_upsertions[0]
    assert_eq(upsertion.name, "bob")
    assert_eq(upsertion.iterations, 4096)
    assert_true(upsertion.salt.length() > 0)
    assert_true(upsertion.salted_password.length() > 0)
    server.close()
  }
}
