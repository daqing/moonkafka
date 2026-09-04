# moonkafka — Full-Feature Kafka 4.3.x Driver Plan

This is the working plan to grow moonkafka from the current simple
producer/consumer into a fully featured Apache Kafka client driver targeting
**Kafka 4.3.x** (first 4.3.0 release: 2026-05-22), KRaft-only, no ZooKeeper-era
or pre-4.0 compatibility.

The user works through this plan step by step: each phase below is a sequence
of independently committable work items with acceptance criteria. Phases 0–4
are the critical path; 5–7 are breadth and polish.

Sources of truth for implementation work:

- Protocol guide: <https://kafka.apache.org/43/protocol.html>
- Per-API message schemas (field-by-field, per version):
  `clients/src/main/resources/common/message/*.json` in the Kafka repo
  (e.g. <https://github.com/apache/kafka/tree/4.3/clients/src/main/resources/common/message>)
- Implementation notes: <https://kafka.apache.org/43/implementation.html>
- 4.3 release notes / announcement for client-visible KIPs (KIP-1274 classic
  protocol deprecation phase 1, KIP-1251 assignment epochs, KIP-1258 OAuth
  client assertions, share-group tuning).

---

## 1. Scope

### In scope

- Complete wire-protocol coverage for everything a **client driver** needs on
  a 4.3.x cluster (data plane, group coordination, transactions, admin, ACLs,
  share groups), at flexible (modern) message versions only.
- Robust transport: request pipelining, timeouts, retries, reconnection,
  TLS, SASL (PLAIN, SCRAM-SHA-256/512, OAUTHBEARER), re-authentication,
  rebootstrap (`REBOOTSTRAP_REQUIRED`, error 129).
- Full record handling: RecordBatch v2 with compression (gzip, snappy, lz4,
  zstd), record headers, transactional/control batches, read-committed
  filtering.
- Producer: batching, idempotence, configurable partitioners (murmur2
  Kafka-compatible), transactions.
- Consumer: topic subscriptions (incl. regex), consumer groups via **both**
  the KIP-848 new protocol (primary; classic protocol deprecation started in
  4.3) and the classic protocol (compat), offset management (commit/seek/
  autocommit), cooperative + eager rebalancing, pause/resume, seek-by-timestamp.
- Admin client covering topic/cluster/config/group/ACL/quota/transaction
  management.
- Share groups (KIP-932) — consumer-style `ShareConsumer`.
- Native backend only (module already `preferred_target = "native"`;
  `moonbitlang/async` sockets are native-only).

### Out of scope

- Kafka < 4.0 brokers, ZooKeeper, message formats v0/v1 (magic < 2).
- Broker-internal APIs: `WriteTxnMarkers`, share-group state APIs
  (`InitializeShareGroupState`, `ReadShareGroupState`, `WriteShareGroupState`,
  `DeleteShareGroupState`, `ReadShareGroupStateSummary`), `AddRaftVoter`,
  `RemoveRaftVoter`, `FetchSnapshot`, `Vote`/`BeginQuorumEpoch`/
  `EndQuorumEpoch`, `ControlledShutdown`, `LeaderAndIsr`/`StopReplica`/
  `UpdateMetadata`, `ControllerRegistration`, `BrokerRegistration`,
  `BrokerHeartbeat`, `AllocateProducerIds`, `Envelope`, `AlterPartition`.
- KIP-1071 Streams group protocol (`StreamsGroupHeartbeat` 88,
  `StreamsGroupDescribe` 89) — that is the Kafka Streams application protocol,
  not a general client need. Revisit if a Streams-style runtime is ever wanted.
- Two-phase commit producer (InitProducerId v6 is marked unstable).
- Java AdminClient-parity for every exotic operation on day one; the admin
  phase grows coverage incrementally.

---

## 2. Current state (inventory)

Single root package `daqing/moonkafka` (native-only, depends on
`moonbitlang/async@0.21.2`):

| Area | File | State |
|---|---|---|
| Buffer codecs | `buf.mbt` | `Encoder`/`Decoder`: BE ints, unsigned LEB128 + zig-zag varints, compact strings, tag-buffer read/write (write is empty-only, skip only) |
| CRC32C | `crc32c.mbt` | table-driven, correct |
| Records | `record.mbt` | RecordBatch v2 decode (uncompressed only; compressed + control batches silently skipped; headers parsed but discarded) and encode (uncompressed, no headers) |
| Framing | `protocol.mbt` | header v2 framing; ApiVersions v3, Produce v11, Metadata v12, ListOffsets v7, Fetch v12 (topic names, no fetch session); hand-written per-API codecs; partial error-code names |
| Connection | `client.mbt` | strict sync request/response (one in flight), no TLS/SASL, no timeout/retry |
| Producer | `producer.mbt` | one record per request; **key hash is crc32c, not Kafka's murmur2** (placement incompatible with other clients — must fix); retries once on leadership change |
| Consumer | `consumer.mbt` | assigns all partitions of one topic, in-memory offsets, no groups/commits, no fetch sessions |
| CLI example | `cmd/main` | produce one / consume loop |

Notable gaps that block "full features": no version negotiation beyond the
ApiVersions range check, no tagged-field *writing* (preserving unknown tags),
no UUID type, no murmur2, no compression, no group/txn/admin APIs, no error
taxonomy (all errors are `ProtocolError(String)`).

---

## 3. Design decisions

These are the load-bearing choices; revisit per-phase only with cause.

**D1 — Version strategy.** Per API, implement a small explicit set of versions:
the newest that every 4.0+ broker accepts plus any older version a feature
needs (e.g. Produce v12 topic-name vs v13 topic-id). `ApiVersions` (v3/v4)
negotiation then picks the highest mutually supported version from the
implemented set; requests below the implemented floor fail fast with a clear
message. Never implement a version purely for pre-4.0 brokers.

**D2 — Hand-written codecs, block style.** Keep codecs hand-written in
MoonBit block style (one `///|` block per request/response pair), one file per
API key or small group (e.g. `protocol/group.mbt`), checked against the
`*.json` schemas in the Kafka repo. A JSON-schema → MoonBit code generator is
explicitly *not* adopted (the ~45 client APIs are tractable by hand, and
generated code fights `moon fmt`/block conventions). If the volume proves
painful in Phase 2, revisit with a generator kept out-of-tree.

**D3 — Tagged fields: read, skip, and write.** Encoders get a real
`write_tag_buffer(tags)` (tagged-field map) and decoders keep unknown top-level
tags where the API demands round-tripping (rare); everywhere else skipping
(`skip_tag_buffer`, already present) is correct and stays.

**D4 — Compression.** Decoder support is mandatory for all four codecs
(consumers must read whatever producers wrote); producer-side support follows
config. Implementation: gzip via `moonbitlang/async/gzip` (already a transitive
dep; buffer-to-buffer wrapper); snappy (Kafka's xerial-framed variant) and lz4
(Kafka's LZ4 frame variant) as pure-MoonBit implementations — both block
algorithms are small; zstd via C FFI on native (`moonbit-c-binding` skill).
Compressed data is en/decompressed whole-batch (never per-record).

**D5 — Group protocols.** KIP-848 (`ConsumerGroupHeartbeat` v1) is the
first-class, default protocol: it is broker-side assignment, server-driven, and
the classic path is on a deprecation track (KIP-1274, phase 1 in 4.3). The
classic protocol (JoinGroup/SyncGroup/Heartbeat) is still implemented for
compat with clusters/groups pinned to `classic`, with eager + cooperative
assignment strategies. Config selects; auto prefers `consumer` protocol.

**D6 — Concurrency model.** Build on `moonbitlang/async` tasks: one sender
task per producer (accumulator drain loop), one heartbeat task per group
member, background autocommit task, metadata refresher task. Shared state via
`@async.Mutex`/condvars where needed; user-facing calls stay async and
cancellation-aware (respect `moonbitlang/async` cancellation semantics).

**D7 — Error taxonomy.** Broker-reported failures are
`BrokerError(code, context)` (a suberror) with the full 4.3 error-code table
(codes -1–133) backing `error_name`/`error_retriable` lookups (the table is
generated mechanically from the protocol docs + `Errors.java`). Client-side
violations stay `ProtocolError(String)`; wire decoding stays `DecodeError`.
Public APIs surface typed errors; retry policy lives in the client, not the
caller.

**D8 — Config.** One `Config` struct per client surface (producer/consumer/
admin) with librdkafka-style snake_case fields (`bootstrap_servers`,
`client_id`, `acks`, `enable_idempotence`, `group_protocol`, …), validated at
connect. No stringly-typed generic config map on the public API.

---

## 4. Target package layout

Flat single-package code has served well so far; split as it grows (each dir
gets a `moon.pkg`):

```
src/
  buf/          Encoder/Decoder (move buf.mbt)
  internal/     crc32c, murmur2, uuid, timing/backoff, logging hooks
  compression/  gzip/snappy/lz4/zstd en+decoders behind one Codec iface
  protocol/     primitives (Uuid, error codes, header framing) +
                one file per API group: produce.mbt, fetch.mbt, metadata.mbt,
                offsets.mbt, group_classic.mbt, group_new.mbt, txn.mbt,
                admin.mbt, sasl.mbt, share.mbt, telemetry.mbt
  net/          BrokerConnection (pipelining, TLS/SASL, reauth), connection
                pool, request queue
  cluster/      metadata cache, topic-id↔name map, coordinator lookup,
                bootstrap/rebootstrap logic
  producer/     accumulator, sender, partitioner, idempotence, txn manager
  consumer/     subscription state, fetcher w/ sessions, offset manager,
                group member (848 + classic), assignment strategies
  admin/        Admin client
  share/        ShareConsumer
  config/       shared + per-surface config
moonkafka.mbt   root re-exports (public API surface)
```

Public API stays in the root package (`@moonkafka.Producer`, `…Consumer`,
`…Admin`, `…ShareConsumer`); internals are subpackages. Every phase ends with
`moon info && moon fmt` and a `.mbti` diff review (per AGENTS.md).

---

## 5. Protocol surface (Kafka 4.3)

Versions: **4.3 max** = ceiling from the 4.3 protocol guide; **floor** = the
version this driver implements at minimum (every 4.0+ broker accepts it).
Phases: P2 = protocol completeness, P3 = producer, P4 = consumer, P5 = admin,
P6 = share/telemetry.

> **API key numbers are intentionally not listed here.** Several keys shifted
> across 3.x/4.x releases and the protocol page contains stale duplicate
> listings. All `API_*` constants live in one file, `protocol/keys.mbt`, and
> each constant is pinned from the 4.3 protocol guide / the corresponding
> `message/*.json` schema **when that codec is implemented** — never from
> memory or from this document. The API *names* below are stable.

| API | 4.3 max | Floor we implement | Phase | Notes |
|---|---|---|---|---|
| Produce | 13 | v12 (topic names) + v13 (topic-id) | P2/P3 | upgrade from current v11 |
| Fetch | 18 | v12 (names) + v13–v16 (topic-ids, incremental sessions, node endpoints); v17/v18 opportunistic | P2/P4 | v13+ needs topic ids; sessions mandatory at v7+ |
| ListOffsets | 11 | v10/v11 (adds timeout_ms) | P2 | upgrade from current v7 |
| Metadata | 13 | v12 + v13 (top-level error_code) | P2 | upgrade from current v12 |
| DescribeTopicPartitions | 0 | v0 | P2/P5 | paginated, cursor-based; used by admin + regex subscription |
| OffsetCommit | 10 | v9 (names) + v10 (topic-id) | P4 | |
| OffsetFetch | 10 | v8 (multi-group) + v9 (member fields) + v10 | P4 | v9 carries member_id/member_epoch for 848 |
| FindCoordinator | 6 | v4 (batched `coordinator_keys`) / v5 | P4 | |
| DescribeQuorum | 2 | v2 | P5 | admin: metadata quorum health |
| UpdateFeatures | 2 | v2 | P5 | admin |
| ApiVersions | 4 | v3 (v4 opportunistic) | done | handshake becomes negotiation in P2 |
| CreateTopics | 7 | v7 | P5 | |
| DeleteTopics | 6 | v6 (names + topic-ids) | P5 | |
| DeleteRecords | 2 | v2 | P5 | |
| InitProducerId | 6 | v5 | P3 | v6 = 2PC, unstable — skip |
| OffsetForLeaderEpoch | 4 | v4 | P4 | truncation detection |
| AddPartitionsToTxn | 6 | v3 (client shape) | P3 | KIP-890 reshaped v4+ for broker-broker verification; spike first: check what the Java 4.x producer negotiates |
| AddOffsetsToTxn | 4 | v4 | P3 | |
| EndTxn | 5 | v5 | P3 | |
| TxnOffsetCommit | 5 | v5 | P3 | |
| DescribeAcls / CreateAcls / DeleteAcls | 3 | v3 | P5 | |
| DescribeConfigs | 4 | v4 | P5 | |
| AlterConfigs | 2 | v2 | P5 | |
| DescribeLogDirs | 5 | v5 (adds `is_cordoned`) | P5 | 4.3 broker cordoning adjacent (KIP-1066) |
| SaslHandshake | 1 | v1 | P1 | |
| SaslAuthenticate | 2 | v2 | P1 | |
| CreatePartitions | 3 | v3 | P5 | |
| Delegation tokens (create/renew/expire/describe) | 4 | defer | — | only useful behind SASL; revisit on demand |
| DeleteGroups | 2 | v2 | P4/P5 | |
| ElectLeaders | 2 | v2 | P5 | |
| IncrementalAlterConfigs | 1 | v1 | P5 | |
| AlterPartitionReassignments | 1 | v1 (per guide) | P5 | verify ceiling when implementing |
| ListPartitionReassignments | 0 | v0 | P5 | |
| OffsetDelete | 0 | v0 | P5 | |
| DescribeUserScramCredentials / AlterUserScramCredentials | 0 | v0 | P5 | |
| DescribeCluster | 2 | v2 | P5 | |
| DescribeProducers | 0 | v0 | P5 | active producers per partition |
| UnregisterBroker | 0 | v0 | P5 | |
| DescribeTransactions | 0 | v0 | P5 | |
| ListTransactions | 2 | v2 | P5 | |
| ConsumerGroupHeartbeat | 1 | v1 (adds `subscribed_topic_regex`) | P4 | KIP-848 — primary group protocol |
| ConsumerGroupDescribe | 1 | v1 | P4 | |
| ListConfigResources | 0 | v0 | P5 | |
| ShareGroupHeartbeat | 1 | v1 | P6 | KIP-932 |
| GetTelemetrySubscriptions / PushTelemetry | 0 | v0 | P6 | optional (KIP-714) |
| ShareGroupDescribe | 1 | v1 | P6 | |
| ShareFetch | 2 | v2 | P6 | |
| ShareAcknowledge | 2 | v2 | P6 | |
| DescribeShareGroupOffsets | 1 | v1 | P6 | |
| AlterShareGroupOffsets / DeleteShareGroupOffsets | 0 | v0 | P6 | |
| Out of scope (broker/controller-internal) | — | — | — | ControlledShutdown, LeaderAndIsr, StopReplica, UpdateMetadata, Vote/BeginQuorumEpoch/EndQuorumEpoch, AlterPartition, BrokerRegistration/BrokerHeartbeat, AllocateProducerIds, Envelope, FetchSnapshot, AddRaftVoter/RemoveRaftVoter, share-state (init/read/write/delete/summary), WriteTxnMarkers, AlterReplicaLogDirs, StreamsGroupHeartbeat/Describe |

Classic group management (still required for compat): JoinGroup, SyncGroup,
Heartbeat, LeaveGroup, DescribeGroups, ListGroups — implement at the 4.3 max
versions (9/5/4/5/6/5 respectively per the guide) (P4).

Error codes: implement the complete table (codes 0 through 133 as of 4.3) in
`protocol/errors.mbt`, each carrying `name`, `retriable`, and
`is_fatal`/`requires_metadata_refresh` style flags from the guide's error
table. Beyond the handful the current `error_name` knows, this must cover the
group/transaction/share-era errors — `STALE_MEMBER_EPOCH`,
`FENCED_MEMBER_EPOCH`, `UNRELEASED_INSTANCE_ID`, `UNSUPPORTED_ASSIGNOR`,
`GROUP_MAX_SIZE_REACHED`, `FENCED_INSTANCE_ID`, `FETCH_SESSION_ID_NOT_FOUND`,
`INVALID_FETCH_SESSION_EPOCH`, `INVALID_FETCH_EPOCH`, `INELIGIBLE_REPLICA`,
`NEW_LEADER_ELECTED`, `OFFSET_MOVED_TO_TIERED_STORAGE`,
`REBOOTSTRAP_REQUIRED`, `GROUP_ID_NOT_FOUND`, the `DUPLICATE_*` family, and
the share-group errors through `SHARE_SESSION_LIMIT_REACHED`. Exact numbers
and flags come from the guide at implementation time.

---

## 6. Roadmap

### Phase 0 — Foundations & housekeeping

Small, safe refactors that later phases stand on. Keep the existing simple
producer/consumer working at every step.

- [ ] **Error taxonomy.** Add `protocol/errors.mbt`: full error-code table
      (0–133) as a `KafkaError` suberror with `code`, `name`, `retriable`,
      `message`; replace ad-hoc `error_name` uses. Keep `DecodeError` as is.
- [ ] **Primitives.** `Uuid` (16-byte, ordered/hex formatting); murmur2
      (`internal/murmur2.mbt`) port of `Utils.murmur2` with the known test
      vectors (empty, `"21".getBytes`, `"foobar"`, `a2931906`… vectors from
      the Java test suite) — fix `Producer::pick_partition` to use it so key
      placement matches other clients.
- [ ] **Tagged-field encode.** Extend `Encoder` with
      `write_tag_buffer(tags : Array[(Int, Bytes)])` (sorted by tag, varint
      sizes); keep `write_tag_buffer()` (empty) as the common case.
- [ ] **Config structs.** `config/` package: `CommonConfig` (bootstrap
      servers list, client_id, request_timeout_ms, connections_max_idle_ms,
      retries/backoff knobs, security_protocol: plaintext|ssl|sasl_plaintext|
      sasl_ssl, sasl_mechanism + credentials), `ProducerConfig`, `ConsumerConfig`
      shells; thread through `Producer::connect`/`Consumer::connect` while
      keeping current named args as sugar.
- [ ] **Time/backoff helpers.** jittered exponential backoff, deadline type
      built on `@async.now()`; unit tests.
- [ ] Split `buf.mbt`/`crc32c.mbt` into subpackages (`src/buf`, `src/internal`)
      and re-export; verify `moon info` diffs are re-exports only.

**Acceptance:** `moon test` green; murmur2 vectors pass; producing with keys
lands on the same partitions as the Java/librdkafka clients on the same topic.

### Phase 1 — Transport hardening

- [ ] **BrokerConnection v2** (`net/`): keep-alive connection to one broker
      with (a) request **pipelining** — in-flight map `correlation_id →
      pending task`, responses matched by correlation id; (b) per-request
      timeout; (c) graceful close + half-close handling; (d) max in-flight
      cap. Do not break the current `Connection` API for the simple clients
      until they migrate.
- [ ] **Reconnect & bootstrap set.** Connect to any of `bootstrap_servers`
      (round-robin); automatic reconnect with jittered backoff; surface
      `REBOOTSTRAP_REQUIRED` up to the cluster layer which re-bootstraps.
- [ ] **Throttling.** Parse `throttle_time_ms` everywhere (currently
      skipped); delay subsequent requests to that broker accordingly.
- [ ] **TLS.** `security_protocol=ssl` via `moonbitlang/async/tls`
      (`Tls::client` over the TCP stream, SNI + trust store config);
      connection type becomes an enum `Plain(Tcp) | Tls(@tls.Tls)`.
- [ ] **SASL.** Handshake/Authenticate flow (SaslHandshake v1 +
      SaslAuthenticate v2) before any other API when configured:
      - PLAIN (trivial)
      - SCRAM-SHA-256/512: pure-MoonBit client (HMAC-SHA256/512 + PBKDF2 +
        SHA-256/512 — check `moonbitlang/core`/`x` crypto coverage; fall back
        to C FFI if a building block is missing)
      - OAUTHBEARER: token callback hook (user-supplied async fn), token
        refresh, `private_key_jwt`-style client-assertion helper (KIP-1258
        era) deferred unless requested
      - re-authentication: honor `session_lifetime_ms` from
        SaslAuthenticate; schedule transparent re-auth.
- [ ] **Cluster layer** (`cluster/`): `ClusterClient` owning a pool of
      BrokerConnections keyed by node id, bootstrap logic, metadata fetch +
      refresh scheduling (expiry + error-triggered), topic-id map,
      coordinator lookup helper (FindCoordinator v4 batched).

**Acceptance:** against a local 4.3 broker in Docker: pipelined parallel
requests; kill/restart broker mid-test → clients recover; TLS + SCRAM
(PLText/SSL listeners in the compose file) produce/consume succeed; OAuth
callback path unit-tested against a mock broker.

### Phase 2 — Protocol completeness (data plane)

- [ ] **Version negotiation matrix.** `protocol/versions.mbt`: per API key
      the implemented floor/ceiling; `ApiVersions` handshake stores the
      broker's ranges; helper `pick(api, want) -> Int` centralizing the
      choose-highest-common logic; the current hard failure in
      `decode_api_versions_response` becomes negotiation.
- [ ] **Metadata**: implement v12 **and** v13; add
      `DescribeTopicPartitions` v0 (paginated, cursor-based) used by admin and
      for regex subscription expansion.
- [ ] **Produce**: v12 + v13 (topic-id), with `record_errors` surfaced;
      keep acks validation; response is now per-batch with
      `base_offset/log_append_time` proper.
- [ ] **Fetch**: v12 (names) + v13–v16 (topic-ids; v15 drops replica_id,
      v16 `node_endpoints`); **incremental fetch sessions** — session
      acquisition (epoch 0 → id), incremental updates (added/forgotten
      partitions), session eviction handling (`FETCH_SESSION_ID_NOT_FOUND`
      → re-establish full request); `last_fetched_epoch`/leader-epoch
      validation; truncated-batch handling with `max_bytes` and
      `MaxBytesExceeded`-style backoff (current code drops the tail batch —
      keep, but make it explicit and offset-safe).
- [ ] **ListOffsets** v10/v11 with `timeout_ms`; timestamp queries
      (`OffsetForTimestamp`) exposed.
- [ ] **Record layer** (`record.mbt`): decode **and** encode record headers;
      expose headers on `Record`; `is_control`/`is_transactional` flags;
      aborted-transaction filter primitive for read_committed
      (aborted-tx list from Fetch + producerId/firstOffset bookkeeping);
      batch builder API for the accumulator (append records, size accounting,
      finalize + crc).
- [ ] **Compression** (`compression/`): interface
      `Codec { compress(Bytes) -> Bytes; decompress(Bytes) -> Bytes raise }` +
      attr-bit mapping; implementations gzip (async/gzip wrapper), snappy
      (xerial framing), lz4 (LZ4 frame format with magic number, block
      checksums optional-off as Kafka writes them), zstd (C FFI). Cross-check
      every codec against batches produced by the Java client (golden files
      committed under `test/golden/`).
- [ ] **Decoder robustness.** Property/fuzz-ish tests: random and truncated
      inputs must raise, never panic/index-OOB, on every codec.

**Acceptance:** codec round-trip tests for all implemented APIs at both
implemented versions (encode → decode equality via `debug_inspect`
snapshots); golden decode tests from Java-produced Fetch responses with each
compression codec; `moon coverage analyze` shows codec branches covered.

### Phase 3 — Full producer

- [ ] **Partitioner**: murmur2 (done in P0) as default for keyed records;
      uniform round-robin; **sticky partitioning** for unkeyed batches
      (switch partition per batch-full, like the Java client);
      `partitioner` config; manual `partition` override per send.
- [ ] **Batching accumulator** (`producer/accumulator.mbt`): per-partition
      batch deque; `linger_ms`, `batch_size`, `buffer_memory` accounting with
      blocking/`BufferExhausted` error; append API the public `send` uses.
- [ ] **Sender task**: drains ready partitions (linger expired or batch
      full), groups by leader, sends pipelined Produce requests with
      `max_in_flight` (default 5), `acks` 0/1/-1 (acks=0 now becomes
      possible since the sender doesn't wait), retries on retriable errors
      with backoff within `delivery_timeout_ms`; metadata-refresh-on-leadership
      moved here; results resolve per-record via completed future/`Async`.
- [ ] **Public API**: `send` returns a cancelable future-like handle
      (`SendHandle` with `await() -> Int64 raise`), plus a batched
      `send_all`; callbacks optional via async closure.
- [ ] **Idempotence** (`enable_idempotence`, default on when acks=all):
      InitProducerId v5; per-partition queues enforce sequence order;
      `DUPLICATE_SEQUENCE_NUMBER`/`OUT_OF_ORDER_SEQUENCE_NUMBER`/
      `UNKNOWN_PRODUCER_ID` handling incl. epoch bump on
      `UNKNOWN_PRODUCER_ID` with the Java client's reset rules; with
      idempotence on, `max_in_flight ≤ 5` preserves ordering.
- [ ] **Transactions**: `TransactionalProducer` (transactional_id config):
      init-with-coordinator, `begin_transaction`, partitions-added tracking,
      AddPartitionsToTxn, `send_offsets_to_transaction` (AddOffsetsToTxn +
      TxnOffsetCommit), `commit`/`abort` (EndTxn), coordinator failover
      (`COORDINATOR_LOAD_IN_PROGRESS` retry, epoch fencing), `abort`-on-error
      policy; `transaction_timeout_ms`.
- [ ] **Producer metrics/counters**: queue depth, in-flight, records sent/
      failed, throttle time (simple counters struct, read on demand).

**Acceptance:** integration tests vs Docker 4.3: throughput sanity (≥10k
rec/s small records, uncompressed and gzip); ordering preserved under retries
with idempotence; kill -9 broker during in-flight sends → no loss/dupes with
acks=all+idempotence; transaction produce-commit visible to read_committed
consumer, abort invisible; duplicate detection verified by replaying with
same PID/sequence (mock broker test).

### Phase 4 — Full consumer

- [ ] **Offset management** (`consumer/offsets.mbt`): committed-offset
      cache; `commit` (sync + async) at OffsetCommit v9/v10 with
      `COORDINATOR_LOAD_IN_PROGRESS`/`REBALANCE_IN_PROGRESS` retry semantics;
      `committed()` fetch (OffsetFetch v8/v9/v10); seek/seek_to_beginning/
      seek_to_end/seek_by_timestamp (ListOffsets); autocommit background
      task (interval config, commit-on-close, commit-on-revoke).
- [ ] **Fetcher**: per-leader pipelined Fetch with sessions (P2); position
      validation (`OffsetOutOfRange` → auto-reset policy earliest/latest/
      none); leader-epoch truncation detection via OffsetForLeaderEpoch v4
      on `UNKNOWN_LEADER_EPOCH`/`FENCED_LEADER_EPOCH`; pause/resume per
      partition; `max_poll_records`/`max_partition_fetch_bytes` honored;
      decompression + read_committed filtering wired in.
- [ ] **KIP-848 member** (`consumer/group_new.mbt`) — the primary path:
      client-generated member id (uuid); ConsumerGroupHeartbeat v1 loop
      (`heartbeat_interval_ms` from server); server-driven assignment applied
      atomically; handle `FENCED_MEMBER_EPOCH`, `UNKNOWN_MEMBER_ID`,
      `STALE_MEMBER_EPOCH`, `GROUP_MAX_SIZE_REACHED`,
      `UNSUPPORTED_ASSIGNOR`; `group_instance_id` static membership;
      regex subscription (`subscribed_topic_regex`, v1) driving metadata
      expansion; graceful leave (heartbeat with member-epoch -1 semantics per
      KIP-848); `RebalanceListener` hooks (on_assign/on_revoke incl.
      incremental assign/revoke sets).
- [ ] **Classic member** (`consumer/group_classic.mbt`) — compat path:
      FindCoordinator v4 → JoinGroup v9 → SyncGroup v5 → Heartbeat v4 loop;
      protocol negotiation (subscription → assignment strategy set);
      `ConsumerProtocolSubscription`/`Assignment` binary encoding (the
      embedded protocol structures, hand-coded);
      assignment strategies: RangeAssignor + RoundRobinAssignor first, then
      **StickyAssignor** and **CooperativeStickyAssignor** (incremental
      cooperative rebalancing: revoke-set sync protocol, `IN_PROGRESS`
      two-round handling); static membership (`group_instance_id`);
      LeaveGroup on close; rebalance listener integration; `require_stable`
      offsets (`UNSTABLE_OFFSET_COMMIT` retry).
- [ ] **Consumer surface**: `subscribe(topics | regex)` vs `assign(...)`
      split; `poll` with `max_poll_interval_ms` enforcement; position()/
      committed(); assignment(); group_metadata(); deserializer hooks
      (default identity `Bytes?`, optional typed helpers for utf8 etc.).
- [ ] **`group_protocol` config**: `consumer` (KIP-848, default) | `classic`
      | fallback ordering; document interop caveats.

**Acceptance:** integration suite vs Docker 4.3: N consumers in one group
over M partitions converge; rebalance on member join/leave/crash with no
duplicate processing beyond at-least-once expectations; commit/restore
offsets across restart; regex subscription picks up newly created topics;
static member restart does not trigger rebalance; cooperative sticky
incremental rebalance path exercised (assert via broker logs or
DescribeGroups); KIP-848 fencing test (old epoch member gets fenced and
rejoins); read_committed skips aborted txns (producer txn test from P3).

### Phase 5 — Admin client

- [ ] `Admin` struct over the cluster layer (any-broker or
      controller-routed as per API); per-item result arrays mirroring the
      protocol's per-topic/per-partition error codes; retriable-error retry
      policy knob.
- [ ] Topic ops: CreateTopics v7 (incl. configs + replication assignment),
      DeleteTopics v6 (by name/id), CreatePartitions v3,
      DescribeTopicPartitions, DeleteRecords v2, OffsetDelete v0.
- [ ] Cluster/config: DescribeCluster v2 (brokers, controller id, cluster id,
      `endpoint_type`), DescribeConfigs v4 + IncrementalAlterConfigs v1 +
      AlterConfigs v2, ListConfigResources v0, DescribeLogDirs v5
      (`is_cordoned`), ElectLeaders v2, reassignments
      (Alter/ListPartitionReassignments), UnregisterBroker v0,
      DescribeQuorum v2, UpdateFeatures v2.
- [ ] Groups/consumers: DescribeGroups v6 (incl. 848 members),
      ListGroups v5 (state/type filters), DeleteGroups v2;
      consumers-of / DescribeProducers v0, DescribeTransactions v0,
      ListTransactions v2.
- [ ] Security: Describe/Create/DeleteAcls v3; quotas
      Describe/AlterClientQuotas; SCRAM credentials
      Describe/AlterUserScramCredentials.
- [ ] Topic-id-first admin ops where the API supports it (fewer metadata
      round-trips).

**Acceptance:** admin integration tests: create/inspect/alter/delete a topic
end-to-end; ACL grant → unauthorized client fails with the right error;
quota + SCRAM credential round-trips against a configured cluster.

### Phase 6 — Share groups (KIP-932) & telemetry (optional)

- [ ] `ShareConsumer`: ShareGroupHeartbeat v1 membership, ShareFetch v2
      (acquisition/release semantics, `available/acknowledged` delivery
      states), ShareAcknowledge v2 (accept/release/reject per record);
      queue-size + delivery-count config; Describe/Alter/DeleteShareGroupOffsets
      via admin; share-group error codes (`SHARE_SESSION_LIMIT_REACHED` …).
- [ ] Telemetry (KIP-714): GetTelemetrySubscriptions/PushTelemetry client
      with pluggable metrics provider; low priority — only if there's a
      consumer for the telemetry (e.g. expose driver's own metrics via it).

**Acceptance:** share-consumer integration test on a 4.3 cluster with share
groups enabled; multiple share consumers never receive the same record
concurrently; acks release records for redelivery after timeout.

### Phase 7 — Hardening, performance, release

- [ ] **Test harness**: `docker-compose.kafka.yml` (KRaft single + multi
      broker, SASL/TLS listeners, 4.3.x image); `make integration` runner;
      mock-broker testkit package (in-process fake broker for
      fault injection: throttling, partial reads, error codes, version
      ceilings) used by codec/client unit tests.
- [ ] **CI**: GitHub Actions — build + `moon test`, fmt check, `moon info`
      clean-diff check, integration job with Docker; golden-file regeneration
      script documented.
- [ ] **Performance pass**: zero-copy-ish decoding (slice `Bytes` views
      instead of copying where the API allows), Encoder preallocation,
      Fetch/Produce buffer sizing; benchmark script (produce/consume
      rec/s) committed under `bench/`.
- [ ] **Docs**: README feature matrix updated per milestone; protocol
      notes in `docs/` (version negotiation table, group protocol guide,
      transaction cookbook); CHANGELOG; API reference from `.mbti`.
- [ ] **Semver + release**: 0.2.0 (transport+data plane), 0.3.0 (consumer
      groups), 0.4.0 (admin), 1.0.0 checklist (API freeze, `.mbti` audit).

---

## 7. Testing strategy (cross-phase)

1. **Unit (pure)**: codec round-trips with `debug_inspect` snapshot tests;
   known-vector tests (murmur2, CRC32C, varints, SCRAM); decoder robustness
   (truncation at every byte offset of a fixture response must raise, not
   panic).
2. **Golden files**: byte-exact request/response fixtures produced by the
   Java 4.x client captured once into `test/golden/`; decoders must parse
   them; encoders must byte-match where deterministic (minus
   client_id/correlation id, which are normalized).
3. **Mock broker**: an in-MoonBit fake broker (async `TcpServer`) speaking
   the real protocol — version ceilings, error injection, throttling,
   session churn. Fast fault-injection home for the group/txn state machines.
4. **Integration**: Dockerized `apache/kafka:4.3.x` (KRaft); suite tagged and
   run explicitly (`moon test --filter integration` style or separate make
   target); covers every "Acceptance" line above.
5. Per AGENTS.md: `assert_eq`/`assert_true(pattern is …)` over snapshots
   for stable values; `moon coverage analyze` reviewed at phase end;
   `moon info && moon fmt` and `.mbti` diff review every commit.

## 8. Risks & open questions

- **AddPartitionsToTxn v4/v5 shape**: KIP-890 reshaped this API;
  confirm which version a *producer* (vs broker) negotiates on 4.3 before
  implementing (small spike, Phase 3).
- **zstd via C FFI**: adds a native build dependency (libzstd); decide
  between vendoring, dlopen-style binding, or pure-MoonBit decoder (largest
  effort, only worth it if wasm-gc support ever lands for the socket layer).
- **moonbitlang/async maturity**: long-lived background tasks + cancellation
  semantics for heartbeat/sender loops need care (leaks on close paths);
  mock-broker tests should cover close-under-load.
- **Classic vs KIP-848 feature parity**: regex subscription for classic
  groups is client-side (metadata scan) and works differently from 848's
  server-side regex; document differences rather than force-unify.
- **SCRAM pure-MoonBit crypto**: verify HMAC-SHA256/512 + PBKDF2
  availability in core/x early (Phase 1 spike); C FFI fallback otherwise.
- **Kafka 4.3.x point releases** may bump max API versions; the negotiation
  matrix (D1) makes that a non-event, but golden files should be regenerated
  per minor release.
