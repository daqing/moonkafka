# moonkafka

An open-source [Apache Kafka](https://kafka.apache.org/) client driver written in [MoonBit](https://www.moonbitlang.com/).

**Target: Apache Kafka 4.x only.**

This driver deliberately supports only the latest Kafka generation — KRaft-based clusters (no ZooKeeper),
the modern Kafka protocol, and no legacy broker/version compatibility baggage.

> **Status:** early development.

The wire protocol implementation is in progress;
the API surface below reflects the intended design and may change before the first release.

## Why MoonBit + Kafka?

- MoonBit compiles to small, fast WebAssembly (and native) targets, making it a good fit for lightweight producers/consumers in edge, serverless, and embedded environments.
- Kafka 4.x is a clean protocol baseline: by dropping ZooKeeper-era and pre-4.x compatibility, the driver stays small and easy to reason about.

## Features

Working today:

- Kafka wire protocol codecs (compact types, zig-zag varints, tagged fields)
- Per-request API version negotiation against the broker's advertised ranges
- RecordBatch v2 (CRC32C-verified): record headers, control/transactional
  flags, read_committed filtering primitive, incremental batch builder, and
  whole-batch decompression for gzip, snappy, lz4, and zstd
- Data-plane APIs: Produce v12/v13 (topic-id addressing, per-record errors),
  Fetch v12-v16 with incremental fetch sessions (KIP-227), Metadata v12/v13,
  DescribeTopicPartitions v0 (paginated), ListOffsets v10/v11
- Simple producer: per-leader connections, partitioner strategies (Kafka-
  compatible murmur2 key-partitioning; sticky batching or round-robin for
  keyless messages; per-send manual partition override), record batching
  with linger/batch-size/buffer-memory accounting, metadata refresh on
  leadership changes, REBOOTSTRAP_REQUIRED recovery
- Simple consumer: incremental fetch sessions with eviction recovery,
  offset resolution by sentinel or timestamp
- Pipelined broker connections: request timeouts, in-flight cap, reconnect
  through bootstrap servers, broker throttling
- TLS (including verified certificates via a custom CA) and SASL
  (PLAIN, SCRAM-SHA-256/512, OAUTHBEARER)

Planned:

- Batched/async producer (accumulator, idempotence, transactions)
- Consumer groups with the new KIP-848 consumer rebalance protocol
- Admin client and share groups

## Requirements

- [MoonBit toolchain](https://www.moonbitlang.com/download/) (latest stable)
- An Apache Kafka **4.x** cluster (KRaft mode). Older brokers are not supported and will not be.

## Installation

```sh
moon add daqing/moonkafka
```

## Quick start

```moonbit nocheck
///|
async fn main {
  // Produce
  let producer = @moonkafka.Producer::connect(
    host="127.0.0.1",
    port=9092,
    topic="events",
  )
  defer producer.close()
  let offset = producer.send(
    key=@utf8.encode("k1"),
    value=@utf8.encode("hello"),
  )
  println("produced at offset \{offset}")

  // Consume
  let consumer = @moonkafka.Consumer::connect(
    host="127.0.0.1",
    port=9092,
    topic="events",
    start_from=@moonkafka.StartFrom::Earliest,
  )
  defer consumer.close()
  for ;; {
    for record in consumer.poll() {
      println("offset=\{record.offset} value=\{record.value}")
    }
  }
}
```

A runnable example lives in `cmd/main` — it consumes all partitions of a
topic and prints records until interrupted:

```sh
moon run cmd/main -- consume events [host] [port]
```

It can also produce a single message:

```sh
moon run cmd/main -- produce events "hello" [key] [host] [port]
```

Note: the socket layer is native-backend only (`moonbitlang/async`), so the
module targets `native`.

## Development

```sh
moon build          # build the library
moon test           # run tests (blackbox + whitebox)
moon fmt            # format code
moon info           # regenerate package interfaces (.mbti)
```

## License

[MIT](LICENSE)
