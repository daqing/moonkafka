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
  DescribeTopicPartitions v0 (paginated), ListOffsets v10/v11,
  FindCoordinator v4 (batched)
- Cluster layer: shared connection pool keyed by node id, metadata caching
  with expiry/error-triggered refresh, topic-id map, coordinator lookups
- Simple producer: per-leader connections, partitioner strategies (Kafka-
  compatible murmur2 key-partitioning; sticky batching or round-robin for
  keyless messages; per-send manual partition override), record batching
  with linger/batch-size/buffer-memory accounting, a background sender
  task (pipelined Produce requests per leader, acks 0/1/-1, retries with
  backoff bounded by delivery timeout), metadata refresh on leadership
  changes, REBOOTSTRAP_REQUIRED recovery
- Producer API: `send`, per-record `SendHandle`s (`await`/`cancel`/
  `on_complete`), batched `send_all`, and on-demand metrics (queue depth,
  in-flight, sent/failed counters, broker throttle time)
- Idempotent producer (acks=all default): InitProducerId handshake,
  per-partition sequence stamping with rewind on failure, epoch bump on
  `UNKNOWN_PRODUCER_ID`
- Transactions: `transactional_id` config with coordinator init,
  `begin_transaction`/`commit_transaction`/`abort_transaction`,
  AddPartitionsToTxn before produce, transactional offset commits
  (AddOffsetsToTxn + TxnOffsetCommit), EndTxn with epoch adoption,
  coordinator retry/refind, fencing detection, and abort-on-error commit
  policy
- Simple consumer: concurrent per-leader fetches with incremental
  sessions and eviction recovery, offset resolution by sentinel or
  timestamp, the seek family (explicit/timestamp/beginning/end),
  committed-offset tracking with sync/async commit and autocommit
  (interval + commit-on-close), max_poll_records /
  max_partition_fetch_bytes caps, per-partition pause/resume,
  auto-offset-reset policies, leader-epoch truncation detection, and
  read_committed filtering of aborted transactions
- KIP-848 consumer groups (primary path): ConsumerGroupHeartbeat
  membership with client-generated member ids, server-driven assignment
  applied atomically around rebalance listener hooks, static membership,
  regex subscription, graceful leave, and fencing recovery
- Consumer surface: subscribe/assign split, position/committed/
  assignment/group_metadata introspection, max_poll_interval_ms
  enforcement, and utf8 record helpers
- Classic consumer groups (compat path): JoinGroup/SyncGroup/Heartbeat
  with range, round-robin, sticky, and cooperative-sticky assignors
  (two-round incremental rebalancing), static membership, graceful
  leave, and rebalance listener hooks
- group_protocol selection: KIP-848 (default), classic, or fallback
  ordering probed against the broker's advertised APIs
- Admin client: topics (create/delete/partitions/records/offset-delete),
  configs, ACLs, quotas, SCRAM credentials, log dirs, leaders,
  reassignments, cluster/controller introspection, transactions, and
  groups listing/describe/delete — with a shared retriable-result retry
  policy and paginated DescribeTopicPartitions walking
- Share groups (KIP-932): `ShareConsumer` over ShareGroupHeartbeat v1
  membership, ShareFetch v2 acquisition with delivery-count caps, and
  ShareAcknowledge v2 (accept/release/reject/renew); Describe/Alter/
  DeleteShareGroupOffsets admin ops
- Telemetry (KIP-714): GetTelemetrySubscriptions/PushTelemetry client
  driving a pluggable metrics provider (e.g. the driver's own counters)
- Pipelined broker connections: request timeouts, in-flight cap, reconnect
  through bootstrap servers, broker throttling
- TLS (including verified certificates via a custom CA) and SASL
  (PLAIN, SCRAM-SHA-256/512, OAUTHBEARER)

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
