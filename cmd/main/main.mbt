///|
/// Simple consumer example:
///   moon run --target native cmd/main -- consume <topic> [host] [port]
/// Consumes all partitions of <topic> and prints records until interrupted.
///
/// Consumer-group example:
///   moon run --target native cmd/main -- consume-group <topic> <group-id> [host] [port]
/// Joins a KIP-848 consumer group and consumes with automatic offset commits.
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
    Some("consume-group") => consume_group(args)
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
      $|  main consume [topic] [host] [port] [start-from]      consume a topic
      $|  main consume-group <topic> <group-id> [host] [port]  consume in a group
      $|  main produce <topic> <value> [key] [host] [port]     produce one message
      $|  main -h | --help                                     show this help
      $|
      $|Consume mode:
      $|  Consumes all partitions of <topic> and prints records until interrupted
      $|  (Ctrl-C). By default it starts at the latest offset (tail); pass
      $|  "earliest" to replay the whole topic.
      $|    topic       topic name (default "events")
      $|    host        bootstrap broker host (default 127.0.0.1)
      $|    port        bootstrap broker port (default 9092)
      $|    start-from  "earliest" or "latest" (default "latest")
      $|
      $|Consumer-group mode:
      $|  Joins a KIP-848 consumer group and consumes <topic> from the earliest
      $|  offset, committing offsets automatically.
      $|    topic     topic name (required)
      $|    group-id  consumer group id (required)
      $|    host      bootstrap broker host (default 127.0.0.1)
      $|    port      bootstrap broker port (default 9092)
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
      $|  moon run cmd/main -- consume-group events payments
      $|  moon run cmd/main -- produce events "hello"
      $|  moon run cmd/main -- produce events "hello" my-key broker.example.com 9093
    ),
  )
}

///|
fn parse_port(args : Array[String], index : Int) -> Int {
  match args.get(index) {
    Some(value) =>
      @string.parse_int(value) catch {
        _ => {
          println("invalid port: \{value}")
          abort("")
        }
      }
    None => 9092
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
  let port = parse_port(args, 6)
  // The producer's sender drain loop lives in a task group scoped to the
  // produce call.
  @async.with_task_group(async fn(_group) {
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
  let port = parse_port(args, 6)
  @async.with_task_group(async fn(_group) {
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
  let port = parse_port(args, 4)
  let start_from = match args.get(5) {
    Some("earliest") => @lib.StartFrom::Earliest
    Some("latest") | None => @lib.StartFrom::Latest
    Some(other) => {
      println("invalid start-from: \{other} (expected earliest|latest)")
      abort("")
    }
  }
  // The consumer's autocommit loop lives in a task group scoped to the
  // consume call.
  @async.with_task_group(async fn(group) {
    let consumer = @lib.Consumer::connect(
      group~,
      host~,
      port~,
      topic~,
      start_from~,
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
    poll_forever(consumer)
  })
}

///|
/// Consume as a KIP-848 consumer-group member: the group coordinator assigns
/// partitions and offsets are committed on the auto-commit interval.
async fn consume_group(args : Array[String]) -> Unit {
  guard args.get(2) is Some(topic) && args.get(3) is Some(group_id) else {
    println("usage: consume-group <topic> <group-id> [host] [port]")
    abort("")
  }
  let host = args.get(4).unwrap_or("127.0.0.1")
  let port = parse_port(args, 5)
  // The heartbeat and auto-commit loops live in a task group scoped to the
  // consume-group call.
  @async.with_task_group(async fn(group) {
    let config = @lib.ConsumerConfig::new(
      ["\{host}:\{port}"],
      topic,
      start_from=@lib.StartFrom::Earliest,
      group_id=Some(group_id),
      enable_auto_commit=true,
      group_protocol=@lib.ConsumerProtocol,
    ) catch {
      e => {
        println("invalid consumer configuration: \{e}")
        abort("")
      }
    }
    let consumer = @lib.Consumer::connect_with_config(group~, config) catch {
      @lib.BrokerError(code, msg) => {
        println("broker error: \{@lib.error_name(code)}: \{msg}")
        abort("")
      }
      e => {
        println("failed to join the group: \{e}")
        abort("")
      }
    }
    defer consumer.close()
    consumer.subscribe([topic], listener=None) catch {
      e => {
        println("group subscription failed: \{e}")
        abort("")
      }
    }
    println(
      "consuming topic \"\{topic}\" as group \"\{group_id}\" from \{host}:\{port} (Ctrl-C to stop)",
    )
    poll_forever(consumer)
  })
}

///|
async fn poll_forever(consumer : @lib.Consumer) -> Unit {
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
