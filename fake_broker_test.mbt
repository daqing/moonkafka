// An in-process fake broker serving real ApiVersions, SASL, Metadata, and
// Produce responses (decodable by this library's own decoders), used to
// exercise the full producer flow — including SASL and reconnect after a
// crash — without a Kafka installation.

///|
fn fake_api_versions_response(throttle_ms : Int) -> Bytes {
  let e = @buf.Encoder::new()
  e.write_i16(0) // error_code
  e.write_compact_len(6)
  let apis : Array[(Int, Int)] = [
    (0, 13),
    (1, 16),
    (2, 11),
    (3, 13),
    (18, 3),
    (75, 0),
  ]
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
  let id_len = d.read_i16()
  ignore(d.read_bytes(id_len))
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
/// Metadata response body; v13 appends a top-level error code after the
/// topics array.
fn fake_metadata_response(
  topic : String,
  broker_port : Int,
  version : Int,
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
  e.write_compact_len(1) // one partition
  e.write_i16(0) // no error
  e.write_i32(0) // index
  e.write_i32(0) // leader node
  e.write_i32(0) // leader epoch
  e.write_compact_len(0) // replicas
  e.write_compact_len(0) // isr
  e.write_compact_len(0) // offline replicas
  e.write_tag_buffer()
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
fn fake_produce_response(
  topic : String,
  version : Int,
  base_offset : Int64,
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
  e.write_i16(0) // no error
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
/// ListOffsets v7 response body: one partition at the given offset.
fn fake_list_offsets_response(topic : String, offset : Int64) -> Bytes {
  let e = @buf.Encoder::new()
  e.write_i32(0) // throttle
  e.write_compact_len(1) // one topic
  e.write_compact_string(topic)
  e.write_compact_len(1) // one partition
  e.write_i32(0) // index
  e.write_i16(0) // no error
  e.write_i64(-1L) // timestamp
  e.write_i64(offset)
  e.write_i32(-1) // leader epoch
  e.write_tag_buffer()
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
  let id_len = d.read_i16()
  ignore(d.read_bytes(id_len)) // client id
  d.skip_tag_buffer()
  ignore(d.read_i32()) // max_wait_ms
  ignore(d.read_i32()) // min_bytes
  ignore(d.read_i32()) // max_bytes
  ignore(d.read_i8()) // isolation level
  ignore(d.read_i32()) // session id
  d.read_i32() // session epoch
}

///|
/// A Fetch response body (any v12-v16: the shape matches) carrying two
/// records at base_offset 42 and the given session id. The record batch's
/// base offset is patched after encoding (it is outside the CRC).
fn fake_fetch_response(
  topic : String,
  version : Int,
  session_id : Int,
) -> Bytes {
  let batch = @moonkafka.encode_record_batch([
    { offset: 0L, timestamp: 1000L, key: None, value: Some(b"one"), },
    { offset: 0L, timestamp: 2000L, key: None, value: Some(b"two"), },
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
  e.write_i32(0) // index
  e.write_i16(0) // no error
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
  let id_len = d.read_i16()
  ignore(d.read_bytes(id_len))
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
) -> Bytes {
  let e = @buf.Encoder::new()
  e.write_i32(0) // throttle
  e.write_compact_len(1) // one topic
  e.write_i16(0) // no error
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
/// Answer requests forever on one client connection; exits when the client
/// goes away. Every API answers with offset 42.
async fn fake_serve_conn(
  stream : BrokerStream,
  topic : String,
  broker_port : Int,
  api_throttle_ms? : Int = 0,
  sasl_ok? : Bool = true,
  scram_password? : String = "secret",
  scram_sha512? : Bool = false,
  fetch_epochs? : Array[Int] = [],
) -> Unit {
  // per-connection SASL state
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
      18 => fake_api_versions_response(api_throttle_ms)
      17 => {
        sasl_mechanism = Some(fake_sasl_handshake_mechanism(rest))
        fake_sasl_handshake_response()
      }
      0 => fake_produce_response(topic, version, 42L)
      2 => fake_list_offsets_response(topic, 42L)
      1 => {
        fetch_epochs.push(fake_fetch_request_session_epoch(rest))
        fake_fetch_response(topic, version, 7)
      }
      3 => fake_metadata_response(topic, broker_port, version)
      75 =>
        // Two pages: a fresh request returns partition 0 with a cursor at
        // partition 1; a resumed request returns partition 1 and completes.
        match fake_dtp_request_cursor(rest) {
          None =>
            fake_describe_topic_partitions_response(
              topic,
              [0],
              Some({ topic_name: topic, partition_index: 1, }),
            )
          Some(_) => fake_describe_topic_partitions_response(topic, [1], None)
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
      _ => continue
    }
    // ApiVersions and SaslHandshake v1 responses use header v0: no tag
    // buffer after the correlation id. The rest are flexible (header v1).
    let has_tag = key != 18 && key != 17
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
