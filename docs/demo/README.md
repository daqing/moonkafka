# moonkafka demo

This directory contains a runnable native MoonBit example showing the three
common client patterns:

- publish a message with `Producer`;
- consume a topic without a consumer group;
- consume as a KIP-848 consumer-group member with automatic offset commits.

The example assumes a Kafka 4.x KRaft broker is listening at
`127.0.0.1:9092` and that the topic already exists. Pass a different host and
port as the optional arguments.

## Prerequisites

You need:

- the MoonBit toolchain;
- a native build environment (the library uses native async sockets);
- a Kafka 4.x KRaft broker reachable from the demo process.

The repository includes a local Kafka 4.3 KRaft configuration. Start it and
create the demo topic with:

```sh
make docker-up

docker exec moonkafka-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic events \
  --partitions 3 --replication-factor 1
```

Check that the broker is reachable before running the demo:

```sh
docker exec moonkafka-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list
```

When finished, stop the local broker with `make docker-down`.

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

## How the demo works

All three commands run inside `@async.with_task_group`. A task group is
important because `Producer` and a configured `Consumer` start background
work: the producer's sender drains batches, while a group consumer sends
heartbeats and handles assignments. Leaving the task-group scope joins those
tasks instead of abandoning them.

### Producer flow

`produce` performs the following steps:

1. Parse the topic, message, host, and port from the command line.
2. Connect with `Producer::connect`, which negotiates broker API versions and
   resolves the topic metadata.
3. Append the UTF-8 message with `producer.send`.
4. Wait for the broker offset. The demo uses `acks=-1`, so Kafka acknowledges
   the write after all in-sync replicas accept it.
5. Call `producer.close()` through `defer` so pending work is drained and the
   connection is closed.

The message has no key, so the configured default partitioner selects a
partition for it. To use explicit settings (batching, retries, TLS, SASL, or
idempotence), replace `Producer::connect` with
`Producer::connect_with_config` and construct a `ProducerConfig`.

### Consumer without a group

`consume` uses the convenience `Consumer::connect` API. It fetches the topic's
partitions directly and starts at `StartFrom::Earliest`. Each call to
`poll()` returns currently available records; the loop prints their offsets and
values, then polls again. Positions are local to this process and are not
committed to Kafka, so restarting the command starts from the configured
position again.

This mode is useful for a small inspection tool or when the application owns
its offset storage. It is not the usual choice when several processes should
share work.

### Consumer-group flow

`consume-group` uses the explicit configuration path:

1. `ConsumerConfig::new` sets `group_id`, the starting offset policy, and
   `enable_auto_commit`.
2. `Consumer::connect_with_config` connects to the broker and starts the
   configured autocommit task.
3. `consumer.subscribe([topic], listener=None)` joins the group. The default
   `ConsumerProtocol` uses KIP-848 server-driven assignment.
4. Kafka assigns partitions to this member. The same command can run in
   multiple terminals with the same group id; each partition is assigned to
   only one active member.
5. The loop calls `poll()` continuously. The client heartbeats in the
   background, applies assignment changes, and commits positions periodically.
6. `consumer.close()` leaves the group and performs the final autocommit.

To select the compatibility protocol explicitly, use
`group_protocol=@moonkafka.ClassicProtocol`. To let the client prefer one
protocol and fall back when necessary, use `PreferConsumer` or
`PreferClassic`.

## Command-line reference

| Command | Required arguments | Optional arguments |
| --- | --- | --- |
| `produce` | `<topic> <value>` | `[host] [port]` |
| `consume` | `<topic>` | `[host] [port]` |
| `consume-group` | `<topic> <group-id>` | `[host] [port]` |

Defaults are `host=127.0.0.1` and `port=9092`. The topic is not created by the
demo; create it first with Kafka's `kafka-topics.sh` command.

## Troubleshooting

- **Connection refused:** start Kafka with `make docker-up`, verify port 9092,
  or pass the correct host and port.
- **Topic not found:** create the topic before starting the demo. The demo is
  intentionally a client example, not an admin-tool example.
- **No records printed:** `consume-group` may be waiting for a member
  assignment, or another member of the same group may own the partitions.
  Produce a new message or run `consume` to inspect the topic independently.
- **Group protocol unavailable:** Kafka must advertise KIP-848 for the default
  `ConsumerProtocol`. Select `ClassicProtocol` for a broker/group that uses
  the classic APIs, or use `PreferConsumer` to allow fallback.
- **The process is stopped:** use `Ctrl-C`; the demo's deferred close path is
  responsible for leaving the group and closing the broker connection.

## Security

The same config constructors accept `security_protocol`, `sasl`, and `tls`
options. See the public `ProducerConfig` and `ConsumerConfig` definitions for
the available fields and validation rules. The runnable demo intentionally
uses plaintext localhost defaults so it can be copied and tried immediately.
