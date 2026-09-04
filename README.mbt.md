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
- RecordBatch v2 decoding with CRC32C verification
- ApiVersions v3, Metadata v12, ListOffsets v7, Fetch v12
- Simple consumer: per-leader connections, all partitions, in-memory offsets,
  earliest/latest start, metadata refresh on leadership changes

Planned:

- Producer
- Consumer groups with the new KIP-848 consumer rebalance protocol
- Compressed batches (gzip/snappy/lz4/zstd)
- TLS and SASL authentication

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
moon run cmd/main -- events [host] [port]
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
