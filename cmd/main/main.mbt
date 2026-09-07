///|
/// Simple consumer example:
///   moon run --target native cmd/main -- consume <topic> [host] [port]
/// Consumes all partitions of <topic> and prints records until interrupted.
///
/// Simple producer example:
///   moon run --target native cmd/main -- produce <topic> <value> [key] [host] [port]
/// Sends one message and prints the assigned partition offset.
async fn main {
  let args = @env.args()
  match args.get(1) {
    Some("-h") | Some("--help") => print_help()
    Some("produce") => produce(args)
    Some("consume") => consume(args)
    Some("bench-produce") => bench_produce(args)
    _ => {
      println("unknown command: \{args.get(1).unwrap_or("<none>")}\n")
      print_help()
      abort("")
    }
  }
}

///|
fn print_help() -> Unit {
  println(
    (
      $|moonkafka - a simple Kafka 4.x client
      $|
      $|Usage:
      $|  main consume [topic] [host] [port]                consume a topic
      $|  main produce <topic> <value> [key] [host] [port]  produce one message
      $|  main -h | --help                                  show this help
      $|
      $|Consume mode:
      $|  Consumes all partitions of <topic> from the latest offset and prints
      $|  records until interrupted (Ctrl-C).
      $|    topic   topic name (default "events")
      $|    host    bootstrap broker host (default 127.0.0.1)
      $|    port    bootstrap broker port (default 9092)
      $|
      $|Produce mode:
      $|  Sends one message to <topic> and prints the assigned offset.
      $|    value   message value (required)
      $|    key     optional message key; same key always lands on the same
      $|            partition, no key spreads messages round-robin
      $|    host    bootstrap broker host (default 127.0.0.1)
      $|    port    bootstrap broker port (default 9092)
      $|
      $|Examples:
      $|  moon run cmd/main -- consume events
      $|  moon run cmd/main -- produce events "hello"
      $|  moon run cmd/main -- produce events "hello" my-key broker.example.com 9093
    ),
  )
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
  // The producer's sender drain loop lives in a task group scoped to the
  // produce call.
  @async.with_task_group(fn(_group) {
    let producer = @lib.Producer::connect(group=_group, host~, port~, topic~) catch {
      @lib.BrokerError(code, msg) => {
        println("broker refused: \{@lib.error_name(code)}: \{msg}")
        abort("")
      }
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
  })
}

///|

///|
/// Benchmark producer: writes `count` records to `topic` in one process,
/// so throughput measures the in-process encode + send path rather than
/// process-launch overhead. Wall time is captured by the calling script.
///   main bench-produce <topic> <count> [key] [host] [port]
async fn bench_produce(args : Array[String]) -> Unit {
  let topic = args.get(2).unwrap_or("events")
  let count = match args.get(3) {
    Some(s) =>
      @string.parse_int(s) catch {
        _ => {
          println("invalid record count: \{s}")
          abort("")
        }
      }
    None => 1000
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
  @async.with_task_group(fn(_group) {
    let config = @lib.ProducerConfig::new(
      ["\{host}:\{port}"],
      topic,
      acks=-1,
      batch_size=1048576,
      linger_ms=0,
      buffer_memory=33554432,
      delivery_timeout_ms=60000,
    )
    let producer = @lib.Producer::connect_with_config(group=_group, config) catch {
      e => {
        println("failed to connect: \{e}")
        abort("")
      }
    }
    defer producer.close()
    let mut last_offset = 0L
    let mut sent = 0
    for i in 0..<count {
      last_offset = producer.send(key?, value=@utf8.encode("bench-\{i}")) catch {
        e => {
          println("produce failed at \{i}: \{e}")
          abort("")
        }
      }
      sent += 1
    }
    println("bench-produce sent=\{sent} last_offset=\{last_offset}")
  })
}

///|
async fn consume(args : Array[String]) -> Unit {
  let topic = args.get(2).unwrap_or("events")
  let host = args.get(3).unwrap_or("127.0.0.1")
  let port = match args.get(4) {
    Some(s) =>
      @string.parse_int(s) catch {
        _ => {
          println("invalid port: \{s}")
          abort("")
        }
      }
    None => 9092
  }
  // The consumer's autocommit loop lives in a task group scoped to the
  // consume call.
  @async.with_task_group(fn(group) {
    let consumer = @lib.Consumer::connect(
      group~,
      host~,
      port~,
      topic~,
      start_from=Latest,
    ) catch {
      @lib.ProtocolError(msg) => {
        println("failed to connect: \{msg}")
        abort("")
      }
      @lib.BrokerError(code, msg) => {
        println("broker error: \{@lib.error_name(code)}: \{msg}")
        abort("")
      }
      @buf.Malformed(msg) => {
        println("protocol decode error: \{msg}")
        abort("")
      }
      @buf.UnexpectedEof => {
        println("protocol decode error: unexpected end of response")
        abort("")
      }
      e => {
        println("failed to connect: \{e}")
        abort("")
      }
    }
    defer consumer.close()
    println(
      "consuming topic \"\{topic}\" from \{host}:\{port} (Ctrl-C to stop)",
    )
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
  })
}
