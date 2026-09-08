# moonkafka demo

This directory contains a runnable native MoonBit example showing the three
common client patterns:

- publish a message with `Producer`;
- consume a topic without a consumer group;
- consume as a KIP-848 consumer-group member with automatic offset commits.

The example assumes a Kafka 4.x KRaft broker is listening at
`127.0.0.1:9092` and that the topic already exists. Pass a different host and
port as the optional arguments.

## Run

From the repository root:

```sh
# Show usage
moon run --target native docs/demo -- --help

# Publish one message (acks = all)
moon run --target native docs/demo -- produce events "hello from MoonBit"

# Consume from the earliest offset without a group
moon run --target native docs/demo -- consume events

# Join a consumer group; run this command in two terminals to see Kafka split partitions
moon run --target native docs/demo -- consume-group events payments
```

For a remote broker:

```sh
moon run --target native docs/demo -- produce events hello kafka.example.com 9092
moon run --target native docs/demo -- consume-group events payments kafka.example.com 9092
```

Stop a consumer with `Ctrl-C`. The demo's `defer consumer.close()` leaves a
consumer group cleanly and performs the final automatic commit when enabled.

## Producer pattern

The essential API is:

```mbt nocheck
@async.with_task_group(fn(group) {
  let producer = @moonkafka.Producer::connect(
    group~,
    host="127.0.0.1",
    port=9092,
    topic="events",
    acks=-1,
  )
  defer producer.close()
  let offset = producer.send(value=@utf8.encode("hello"))
  println("published at offset \{offset}")
})
```

For production applications, use `ProducerConfig::new` and
`Producer::connect_with_config` to configure TLS, SASL, batching, retries,
partitioning, idempotence, or transactions.

## Consumer without a group

```mbt nocheck
@async.with_task_group(fn(group) {
  let consumer = @moonkafka.Consumer::connect(
    group~,
    host="127.0.0.1",
    port=9092,
    topic="events",
    start_from=@moonkafka.StartFrom::Earliest,
  )
  defer consumer.close()
  for ;; {
    for record in consumer.poll() {
      println("offset=\{record.offset}")
    }
  }
})
```

This path reads partitions directly and keeps positions in memory. Use it for
simple tools or applications that manage their own offsets.

## Consumer group

```mbt nocheck
let config = @moonkafka.ConsumerConfig::new(
  ["127.0.0.1:9092"],
  "events",
  start_from=@moonkafka.StartFrom::Earliest,
  group_id=Some("payments"),
  enable_auto_commit=true,
  group_protocol=@moonkafka.ConsumerProtocol,
)
let consumer = @moonkafka.Consumer::connect_with_config(group~, config)
consumer.subscribe(["events"], listener=None)
```

A group consumer should call `poll()` continuously. Kafka assigns partitions
among members of the same group, and `enable_auto_commit=true` commits the
current positions periodically and once during close. To use the classic
protocol explicitly, set `group_protocol=@moonkafka.ClassicProtocol`.

## Security

The same config constructors accept `security_protocol`, `sasl`, and `tls`
options. See the public `ProducerConfig` and `ConsumerConfig` definitions for
the available fields and validation rules. The runnable demo intentionally
uses plaintext localhost defaults so it can be copied and tried immediately.
