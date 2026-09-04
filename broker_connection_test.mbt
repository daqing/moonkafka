// Integration tests for the pipelined BrokerConnection against an
// in-process fake broker (a real socket, no Kafka needed).

///|
/// Read one full request frame; the correlation id sits at bytes 4..8 of
/// the remainder (size prefix, then api_key/api_version/correlation).
async fn fake_read_request(tcp : @socket.Tcp) -> Int {
  let size_bytes = tcp.read_exactly(4)
  let size = @buf.Decoder::new(size_bytes).read_i32()
  let rest = tcp.read_exactly(size)
  let corr = (rest[4].to_int() << 24) |
    (rest[5].to_int() << 16) |
    (rest[6].to_int() << 8) |
    rest[7].to_int()
  corr
}

///|
/// Build a response frame: size, correlation, empty v1 header tag buffer,
/// one payload byte.
fn fake_response(corr : Int, payload : Int) -> Bytes {
  let e = @buf.Encoder::new()
  e.write_i32(4 + 1 + 1)
  e.write_i32(corr)
  e.write_tag_buffer()
  e.write_i8(payload)
  e.to_bytes()
}

///|
async test "responses are demuxed when the broker answers out of order" {
  @async.with_task_group() <| root => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    let port = server.addr.port()
    root.spawn_bg(() => {
      // Serve one connection: read both pipelined requests, then answer
      // the SECOND one first — impossible with a serial connection.
      let (tcp, _) = server.accept()
      defer tcp.close()
      let c1 = fake_read_request(tcp)
      let c2 = fake_read_request(tcp)
      tcp.write(fake_response(c2, 22))
      tcp.write(fake_response(c1, 11))
    })
    let conn = @moonkafka.BrokerConnection::connect("127.0.0.1", port)
    defer conn.close()
    let out = @async.all([
      () => {
        let d = conn.request(@moonkafka.API_PRODUCE, 11, b"a", timeout_ms=5000)
        d.read_i8()
      },
      () => {
        let d = conn.request(@moonkafka.API_FETCH, 12, b"b", timeout_ms=5000)
        d.read_i8()
      },
    ])
    assert_eq(out[0], 11)
    assert_eq(out[1], 22)
  }
}

///|
async test "request times out and the connection is closed" {
  @async.with_task_group() <| root => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    let port = server.addr.port()
    root.spawn_bg(allow_failure=true) <| () => {
      // Accept, swallow one request, then stay silent far longer than the
      // client timeout; the group cancels this sleep on exit.
      let (tcp, _) = server.accept()
      ignore(fake_read_request(tcp))
      @async.sleep(10_000)
      tcp.close()
    }
    let conn = @moonkafka.BrokerConnection::connect("127.0.0.1", port)
    let mut timed_out = false
    ignore(conn.request(@moonkafka.API_PRODUCE, 11, b"x", timeout_ms=200)) catch {
      @moonkafka.RequestTimeout(_) => timed_out = true
      e => fail("expected a timeout, got \{e}")
    }
    assert_true(timed_out)
    // a timed-out request closed the connection; the next one fails fast
    ignore(conn.request(@moonkafka.API_PRODUCE, 11, b"y", timeout_ms=200)) catch {
      @moonkafka.ConnectionClosed(_) => ()
      e => fail("expected a closed connection, got \{e}")
    }
  }
}

///|
async test "broker closing mid-request raises ConnectionClosed" {
  @async.with_task_group() <| root => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    let port = server.addr.port()
    root.spawn_bg(allow_failure=true) <| () => {
      let (tcp, _) = server.accept()
      tcp.close()
    }
    let conn = @moonkafka.BrokerConnection::connect("127.0.0.1", port)
    defer conn.close()
    let mut closed = false
    ignore(conn.request(@moonkafka.API_PRODUCE, 11, b"x", timeout_ms=5000)) catch {
      @moonkafka.ConnectionClosed(_) => closed = true
      e => fail("expected a closed connection, got \{e}")
    }
    assert_true(closed)
  }
}
