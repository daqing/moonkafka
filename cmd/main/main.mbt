///|
/// Simple consumer example:
///   moon run --target native cmd/main -- <topic> [host] [port]
/// Consumes all partitions of <topic> and prints records until interrupted.
///
/// Simple producer example:
///   moon run --target native cmd/main -- produce <topic> <value> [key] [host] [port]
/// Sends one message and prints the assigned partition offset.
async fn main {
  let args = @env.args()
  if args.get(1) is Some("produce") {
    produce(args)
  } else {
    consume(args)
  }
}

///|
async fn produce(args : Array[String]) -> Unit {
  guard args.get(2) is Some(topic) && args.get(3) is Some(value) else {
    println("usage: produce <topic> <value> [key] [host] [port]")
    abort("")
  }
  let key = args.get(4).map(fn(s) { @utf8.encode(s) })
  let host = args.get(5).unwrap_or("127.0.0.1")
  let port = match args.get(6) {
    Some(s) =>
      @string.parse_int(s) catch {
        _ => {
          println("invalid port: \{s}")
          abort("")
        }
      }
    None => 9092
  }
  let producer = @lib.Producer::connect(host~, port~, topic~) catch {
    e => {
      println("failed to connect: \{e}")
      abort("")
    }
  }
  defer producer.close()
  let offset = producer.send(key?, value=@utf8.encode(value)) catch {
    e => {
      println("produce failed: \{e}")
      abort("")
    }
  }
  println("produced to \"\{topic}\" at offset \{offset}")
}

///|
async fn consume(args : Array[String]) -> Unit {
  let topic = args.get(1).unwrap_or("events")
  let host = args.get(2).unwrap_or("127.0.0.1")
  let port = match args.get(3) {
    Some(s) =>
      @string.parse_int(s) catch {
        _ => {
          println("invalid port: \{s}")
          abort("")
        }
      }
    None => 9092
  }
  let consumer = @lib.Consumer::connect(host~, port~, topic~, start_from=Latest) catch {
    @lib.ProtocolError(msg) => {
      println("failed to connect: \{msg}")
      abort("")
    }
    @lib.Malformed(msg) => {
      println("protocol decode error: \{msg}")
      abort("")
    }
    @lib.UnexpectedEof => {
      println("protocol decode error: unexpected end of response")
      abort("")
    }
    e => {
      println("failed to connect: \{e}")
      abort("")
    }
  }
  defer consumer.close()
  println("consuming topic \"\{topic}\" from \{host}:\{port} (Ctrl-C to stop)")
  for ;; {
    let records = consumer.poll() catch {
      e => {
        println("poll error: \{e}; retrying")
        @async.sleep(1000)
        continue
      }
    }
    for record in records {
      let key = match record.key {
        Some(k) => @utf8.decode_lossy(k[:])
        None => "<null>"
      }
      let value = match record.value {
        Some(v) => @utf8.decode_lossy(v[:])
        None => "<tombstone>"
      }
      println("offset=\{record.offset} key=\{key} value=\{value}")
    }
    if records.is_empty() {
      @async.sleep(500)
    }
  }
}
