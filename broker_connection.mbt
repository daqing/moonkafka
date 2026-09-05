// Pipelined connection to a single Kafka broker.
//
// Several requests may be in flight at once:
// request tasks serialize frame WRITES on one lock and take turns reading
// whole response FRAMES on another, pooling frames addressed to other
// correlations until their owner comes to claim them. The cooperative event
// loop makes this check-then-wait pattern safe (a wakeup cannot be lost
// between a pool check and suspending on the socket or a lock).
//
// Any transport failure — broker EOF, corrupt frame, or a request exceeding
// its timeout — leaves the byte stream misaligned, so the connection is
// closed and every later request fails with TransportError.

///|
pub struct BrokerConnection {
  tcp : @socket.Tcp
  client_id : String
  write_lock : @async.Mutex
  read_lock : @async.Mutex
  /// Completed response frames (correlation id onward) waiting for their
  /// requester; guarded by read_lock.
  responses : Map[Int, Bytes]
  /// Last issued correlation id; guarded by write_lock.
  mut correlation_id : Int
  mut closed : Bool
  in_flight : @async.Semaphore
  /// Wall-clock deadline set by the broker's last throttle hint; requests
  /// started before it wait out the remainder.
  mut throttled_until_ms : Int64
}

///|
pub async fn BrokerConnection::connect(
  host : String,
  port : Int,
  client_id? : String = "moonkafka",
  max_in_flight? : Int = 5,
  sasl? : SaslConfig? = None,
  timeout_ms? : Int = 30000,
) -> BrokerConnection {
  let tcp = @socket.Tcp::connect_to_host(host, port~)
  let conn : BrokerConnection = {
    tcp,
    client_id,
    write_lock: @async.Mutex(),
    read_lock: @async.Mutex(),
    responses: Map([]),
    correlation_id: 0,
    closed: false,
    in_flight: @async.Semaphore(max_in_flight, initial_value=max_in_flight),
    throttled_until_ms: 0L,
  }
  // SASL runs before any other API on the connection.
  match sasl {
    Some(sasl) => conn.authenticate(sasl, timeout_ms~)
    None => ()
  }
  conn
}

///|
/// Record the broker's latest throttle hint, keeping the furthest deadline.
pub fn BrokerConnection::note_throttle(
  self : BrokerConnection,
  throttle_ms : Int,
) -> Unit {
  if throttle_ms > 0 {
    let until = @async.now() + throttle_ms.to_int64()
    if until > self.throttled_until_ms {
      self.throttled_until_ms = until
    }
  }
}

///|
/// Close the underlying socket. Idempotent; later requests fail with
/// ConnectionClosed, and in-flight ones fail as their reads break.
pub fn BrokerConnection::close(self : BrokerConnection) -> Unit {
  if !self.closed {
    self.closed = true
    self.tcp.close()
  }
}

///|
/// Send one request and wait for its response. At most `max_in_flight`
/// requests run concurrently; the rest queue on the semaphore. On timeout
/// or any transport failure the connection is closed.
pub async fn BrokerConnection::request(
  self : BrokerConnection,
  api_key : Int,
  api_version : Int,
  body : Bytes,
  timeout_ms? : Int = 30000,
) -> @buf.Decoder {
  self.request_raw(api_key, api_version, body, timeout_ms~, flexible=true)
}

///|
/// Like request, but for APIs whose chosen version is not flexible
/// (request header v1, response header v0): SaslHandshake v1 today.
pub async fn BrokerConnection::request_raw(
  self : BrokerConnection,
  api_key : Int,
  api_version : Int,
  body : Bytes,
  timeout_ms? : Int = 30000,
  flexible? : Bool = true,
) -> @buf.Decoder {
  if self.closed {
    raise TransportError::ConnectionClosed("connection is closed")
  }
  let remaining_throttle_ms = self.throttled_until_ms - @async.now()
  if remaining_throttle_ms > 0 {
    @async.sleep(remaining_throttle_ms.to_int())
  }
  let result = @async.with_timeout(timeout_ms, () => {
    self.exchange(api_key, api_version, body, flexible)
  }) catch {
    // The byte stream can no longer be trusted once a read is abandoned:
    // a cancelled read may have consumed part of a frame. Close so later
    // requests fail fast.
    @async.TimeoutError => {
      self.close()
      raise TransportError::RequestTimeout(
        "no response in \{timeout_ms} ms (api \{api_key} v\{api_version})",
      )
    }
    e => {
      self.close()
      raise e
    }
  }
  result
}

///|
async fn BrokerConnection::exchange(
  self : BrokerConnection,
  api_key : Int,
  api_version : Int,
  body : Bytes,
  flexible : Bool,
) -> @buf.Decoder {
  self.in_flight.acquire()
  defer self.in_flight.release()
  let corr = self.issue_request(api_key, api_version, body, flexible)
  self.await_response(corr, api_key, flexible)
}

///|
/// Issue one request. The write lock scope is this whole function so it is
/// released before the caller starts awaiting the response — holding it
/// across the wait would serialize pipelined requests again.
async fn BrokerConnection::issue_request(
  self : BrokerConnection,
  api_key : Int,
  api_version : Int,
  body : Bytes,
  flexible : Bool,
) -> Int {
  self.write_lock.acquire()
  defer self.write_lock.release()
  self.correlation_id += 1
  let corr = self.correlation_id
  self.tcp.write(
    encode_request(api_key, api_version, corr, self.client_id, body, flexible~),
  )
  corr
}

///|
/// Claim responses until our frame arrives, reading new frames while we
/// hold the read lock and pooling frames that belong to other requesters.
async fn BrokerConnection::await_response(
  self : BrokerConnection,
  corr : Int,
  api_key : Int,
  flexible : Bool,
) -> @buf.Decoder {
  self.read_lock.acquire()
  defer self.read_lock.release()
  let frame = self.demux(corr)
  // Frame layout after the size prefix: correlation id, then — for
  // flexible responses except ApiVersions, whose response header stays
  // v0 — the tag buffer, then the body.
  let d = @buf.Decoder::new(frame)
  ignore(d.read_i32())
  if flexible && api_key != API_API_VERSIONS {
    d.skip_tag_buffer()
  }
  d
}

///|
async fn BrokerConnection::demux(self : BrokerConnection, corr : Int) -> Bytes {
  for ;; {
    match self.responses.get(corr) {
      Some(frame) => {
        self.responses.remove(corr)
        return frame
      }
      None => ()
    }
    let frame = self.read_frame()
    let found = @buf.Decoder::new(frame).read_i32() catch {
      _ => raise TransportError::ConnectionClosed("corrupt response header")
    }
    if found == corr {
      return frame
    }
    self.responses[found] = frame
  }
}

///|
async fn BrokerConnection::read_frame(self : BrokerConnection) -> Bytes {
  // A timeout cancels the read mid-frame with a cancellation error; that
  // must pass through untranslated so request() can classify it, while
  // genuine socket failures become ConnectionClosed.
  let size_bytes = self.tcp.read_exactly(4) catch {
    e => {
      if @async.is_cancellation_error(e) {
        raise e
      }
      raise TransportError::ConnectionClosed("broker closed mid-frame: \{e}")
    }
  }
  let size = @buf.Decoder::new(size_bytes).read_i32() catch {
    _ => raise TransportError::ConnectionClosed("truncated response size")
  }
  if size < 4 {
    raise TransportError::ConnectionClosed("impossible response size \{size}")
  }
  self.tcp.read_exactly(size) catch {
    e => {
      if @async.is_cancellation_error(e) {
        raise e
      }
      raise TransportError::ConnectionClosed("broker closed mid-frame: \{e}")
    }
  }
}
