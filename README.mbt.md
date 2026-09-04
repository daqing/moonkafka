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

## Features (planned)

- Kafka wire protocol encoding/decoding, generated against the Kafka 4.x protocol definitions
- Producer: send records to topics with partitioning support
- Consumer: fetch records, consumer groups with the new KIP-848 consumer rebalance protocol
- Metadata & admin operations against KRaft clusters
- TLS and SASL authentication

## Requirements

- [MoonBit toolchain](https://www.moonbitlang.com/download/) (latest stable)
- An Apache Kafka **4.x** cluster (KRaft mode). Older brokers are not supported and will not be.

## Installation

```sh
moon add daqing/moonkafka
```

## Quick start (API sketch)

```moonbit
// Produce a record
let producer = @moonkafka.Producer::connect("localhost:9092")
producer.send(topic="events", key="user-1", value="hello kafka")

// Consume records
let consumer = @moonkafka.Consumer::connect("localhost:9092", group="my-app")
for record in consumer.poll("events") {
  println(record.value)
}
```

## Development

```sh
moon build          # build the library
moon test           # run tests (blackbox + whitebox)
moon fmt            # format code
moon info           # regenerate package interfaces (.mbti)
```

## License

[MIT](LICENSE)
