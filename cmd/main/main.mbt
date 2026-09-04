///|
/// Simple consumer example:
///   moon run --target native cmd/main -- <topic> [host] [port]
/// Consumes all partitions of <topic> and prints records until interrupted.
async fn main {
  let args = @env.args()
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
