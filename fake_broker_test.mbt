// An in-process fake broker serving real ApiVersions, Metadata, and
// Produce responses (decodable by this library's own decoders), used to
// exercise the full producer flow — including reconnect after a crash —
// without a Kafka installation.

///|
fn fake_api_versions_response(throttle_ms : Int) -> Bytes {
  let e = @buf.Encoder::new()
  e.write_i16(0) // error_code
  e.write_compact_len(5)
  let apis : Array[(Int, Int)] = [(0, 11), (1, 12), (2, 7), (3, 12), (18, 3)]
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
/// SaslHandshake v1: non-flexible body, int32 count, int16-length names.
fn fake_sasl_handshake_response() -> Bytes {
  let e = @buf.Encoder::new()
  e.write_i16(0) // error_code
  e.write_i32(1) // mechanism count
  let plain = @utf8.encode("PLAIN")
  e.write_i16(plain.length())
  e.write_bytes(plain)
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
fn fake_metadata_response(topic : String, broker_port : Int) -> Bytes {
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
  e.write_bytes(Bytes::make(16, b'\x00')) // topic id
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
  e.write_tag_buffer()
  e.to_bytes()
}

///|
fn fake_produce_response(topic : String, base_offset : Int64) -> Bytes {
  let e = @buf.Encoder::new()
  e.write_compact_len(1) // topics
  e.write_compact_string(topic)
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
/// Answer requests forever on one client connection; exits when the client
/// goes away. Every API answers with offset 42.
async fn fake_serve_conn(
  tcp : @socket.Tcp,
  topic : String,
  broker_port : Int,
  api_throttle_ms? : Int = 0,
  sasl_ok? : Bool = true,
) -> Unit {
  for ;; {
    let size_bytes = tcp.read_exactly(4)
    let size = @buf.Decoder::new(size_bytes).read_i32()
    let rest = tcp.read_exactly(size)
    let key = (rest[0].to_int() << 8) | rest[1].to_int()
    let corr = (rest[4].to_int() << 24) |
      (rest[5].to_int() << 16) |
      (rest[6].to_int() << 8) |
      rest[7].to_int()
    let body = match key {
      18 => fake_api_versions_response(api_throttle_ms)
      17 => fake_sasl_handshake_response()
      36 => fake_sasl_authenticate_response(sasl_ok)
      3 => fake_metadata_response(topic, broker_port)
      0 => fake_produce_response(topic, 42L)
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
    tcp.write(frame.to_bytes())
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
) -> Unit {
  for ;; {
    let (tcp, _) = server.accept()
    accepted.push(tcp)
    root.spawn_bg(no_wait=true, allow_failure=true) <| () => {
      fake_serve_conn(
        tcp,
        topic,
        server.addr.port(),
        api_throttle_ms~,
        sasl_ok~,
      )
    }
  }
}

///|
async test "producer completes a full send against a fake broker" {
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
        request_timeout_ms=5000,
      ),
    )
    defer producer.close()
    let offset = producer.send(value=b"hello")
    assert_eq(offset, 42L)
    // Shut the broker down so its accept loop ends and the task group can
    // join it at exit.
    server.close()
  }
}

///|
async test "producer reconnects through the bootstrap set after a crash" {
  @async.with_task_group() <| root => {
    let server1 = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    let accepted1 : Array[@socket.Tcp] = []
    root.spawn_bg(no_wait=true, allow_failure=true) <| () => {
      fake_broker(server1, root, "events", accepted1)
    }
    let server2 = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    let accepted2 : Array[@socket.Tcp] = []
    root.spawn_bg(no_wait=true, allow_failure=true) <| () => {
      fake_broker(server2, root, "events", accepted2)
    }
    let producer = @moonkafka.Producer::connect_with_config(
      @moonkafka.ProducerConfig::new(
        ["127.0.0.1:\{server1.addr.port()}", "127.0.0.1:\{server2.addr.port()}"],
        "events",
        acks=1,
        request_timeout_ms=5000,
      ),
    )
    defer producer.close()
    assert_eq(producer.send(value=b"one"), 42L)
    // Simulate a crash of broker 1: the listener and every accepted
    // socket die together.
    server1.close()
    for c in accepted1 {
      c.close()
    }
    // The next send fails over: recovery rotates to broker 2.
    assert_eq(producer.send(value=b"two"), 42L)
    server2.close()
    for c in accepted2 {
      c.close()
    }
  }
}

///|
async test "connect_bootstrap fails over to a live server" {
  @async.with_task_group() <| root => {
    // A closed port: bind one, learn the port, then free it, so the dial
    // is refused at connect time rather than after accept.
    let refuse = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    let refuse_port = refuse.addr.port()
    refuse.close()
    let live = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    let live_conns : Array[@socket.Tcp] = []
    root.spawn_bg(no_wait=true, allow_failure=true) <| () => {
      for ;; {
        let (tcp, _) = live.accept()
        live_conns.push(tcp)
      }
    }
    let servers = @moonkafka.BootstrapServers::new([
      { host: "127.0.0.1", port: refuse_port, },
      { host: "127.0.0.1", port: live.addr.port(), },
    ])
    let conn = @moonkafka.connect_bootstrap(
      servers,
      "test-client",
      timeout_ms=2000,
    )
    conn.close()
    assert_false(live_conns.is_empty())
    live.close()
  }
}

///|
async test "broker throttling delays subsequent requests" {
  @async.with_task_group() <| root => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    let accepted : Array[@socket.Tcp] = []
    root.spawn_bg(no_wait=true, allow_failure=true) <| () => {
      fake_broker(server, root, "events", accepted, api_throttle_ms=400)
    }
    let conn = @moonkafka.BrokerConnection::connect(
      "127.0.0.1",
      server.addr.port(),
    )
    defer conn.close()
    // the ApiVersions response carries throttle_time_ms=400, so the next
    // request must wait it out before hitting the wire
    conn.check_api_versions(timeout_ms=2000)
    let started = @async.now()
    ignore(conn.fetch_metadata("events", timeout_ms=2000))
    let elapsed = @async.now() - started
    assert_true(elapsed >= 350L)
    server.close()
  }
}

///|
async test "producer authenticates with SASL PLAIN before producing" {
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
        request_timeout_ms=5000,
        security_protocol=SaslPlaintext,
        sasl=Some({ mechanism: Plain, username: "admin", password: "secret", }),
      ),
    )
    defer producer.close()
    assert_eq(producer.send(value=b"hello"), 42L)
    server.close()
  }
}

///|
async test "failed SASL authentication raises SaslError" {
  @async.with_task_group() <| root => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    let accepted : Array[@socket.Tcp] = []
    root.spawn_bg(no_wait=true, allow_failure=true) <| () => {
      fake_broker(server, root, "events", accepted, sasl_ok=false)
    }
    let mut sasl_failed = false
    ignore(
      @moonkafka.BrokerConnection::connect(
        "127.0.0.1",
        server.addr.port(),
        sasl=Some({ mechanism: Plain, username: "admin", password: "wrong", }),
        timeout_ms=2000,
      ),
    ) catch {
      @moonkafka.SaslError(_) => sasl_failed = true
      e => fail("expected a SaslError, got \{e}")
    }
    assert_true(sasl_failed)
    server.close()
  }
}
