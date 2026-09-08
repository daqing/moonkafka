// Runnable moonkafka examples.
//
// Produce one message:
//   moon run --target native docs/demo -- produce events hello
//
// Consume without a group (manual assignment of the topic's partitions):
//   moon run --target native docs/demo -- consume events
//
// Consume as a KIP-848 consumer group member:
//   moon run --target native docs/demo -- consume-group events payments

///|
async fn main {
  let args = @env.args()
  match args.get(1) {
    Some("produce") => produce(args)
    Some("consume") => consume(args)
    Some("consume-group") => consume_group(args)
    Some("-h") | Some("--help") | None => print_help()
    Some(command) => {
      println("unknown command: \{command}\n")
      print_help()
    }
  }
}

///|
fn print_help() -> Unit {
  println(
    (
      $|moonkafka demo
      $|
      $|Usage:
      $|  moon run --target native docs/demo -- produce <topic> <value> [host] [port]
      $|  moon run --target native docs/demo -- consume <topic> [host] [port]
      $|  moon run --target native docs/demo -- consume-group <topic> <group-id> [host] [port]
      $|
      $|The broker defaults to 127.0.0.1:9092. The topic must already exist.
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
    println("usage: produce <topic> <value> [host] [port]")
    return
  }
  let host = args.get(4).unwrap_or("127.0.0.1")
  let port = parse_port(args, 5)

  @async.with_task_group(fn(group) {
    let producer = @lib.Producer::connect(group~, host~, port~, topic~, acks=-1) catch {
      e => {
        println("producer connection failed: \{e}")
        abort("")
      }
    }
    defer producer.close()

    let offset = producer.send(value=@utf8.encode(value)) catch {
      e => {
        println("produce failed: \{e}")
        abort("")
      }
    }
    println("published to \"\{topic}\" at offset \{offset}")
  })
}

///|
async fn consume(args : Array[String]) -> Unit {
  guard args.get(2) is Some(topic) else {
    println("usage: consume <topic> [host] [port]")
    return
  }
  let host = args.get(3).unwrap_or("127.0.0.1")
  let port = parse_port(args, 4)

  @async.with_task_group(fn(group) {
    let consumer = @lib.Consumer::connect(
      group~,
      host~,
      port~,
      topic~,
      start_from=@lib.StartFrom::Earliest,
    ) catch {
      e => {
        println("consumer connection failed: \{e}")
        abort("")
      }
    }
    defer consumer.close()
    println("consuming \"\{topic}\" without a group; press Ctrl-C to stop")
    poll_forever(consumer)
  })
}

///|
async fn consume_group(args : Array[String]) -> Unit {
  guard args.get(2) is Some(topic) && args.get(3) is Some(group_id) else {
    println("usage: consume-group <topic> <group-id> [host] [port]")
    return
  }
  let host = args.get(4).unwrap_or("127.0.0.1")
  let port = parse_port(args, 5)

  @async.with_task_group(fn(group) {
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
      e => {
        println("consumer group connection failed: \{e}")
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
      "consuming \"\{topic}\" as group \"\{group_id}\"; press Ctrl-C to stop",
    )
    poll_forever(consumer)
  })
}

///|
async fn poll_forever(consumer : @lib.Consumer) -> Unit {
  for ;; {
    let records = consumer.poll() catch {
      e => {
        println("poll failed: \{e}; retrying")
        @async.sleep(1000)
        continue
      }
    }
    for record in records {
      let value = match record.value {
        Some(bytes) => @utf8.decode_lossy(bytes[:])
        None => "<tombstone>"
      }
      println("offset=\{record.offset} value=\{value}")
    }
    if records.is_empty() {
      @async.sleep(250)
    }
  }
}
