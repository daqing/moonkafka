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
  buf/          Encoder/Decoder/DecodeError (moved)
  internal/     crc32c, murmur2 (+ timing/logging hooks later)
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
| FindCoordinator | 6 | v4 (batched `coordinator_keys`) | P1 (done), used in P4 | |
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
| DescribeAcls / CreateAcls / DeleteAcls | 3 | v2-v3 (one wire shape; v3 adds the USER resource type) | P5 (done) | |
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
| DescribeClientQuotas / AlterClientQuotas | 1 | v0-v1 (v1 enables flexible versions) | P5 (done) | row was missing from the original table; added when implemented |
| DescribeUserScramCredentials / AlterUserScramCredentials | 0 | v0 (flexible from the start) | P5 (done) | |
| DescribeCluster | 2 | v2 | P5 | |
| DescribeProducers | 0 | v0 | P5 | active producers per partition |
| UnregisterBroker | 0 | v0 | P5 | |
| DescribeTransactions | 0 | v0 | P5 | |
| ListTransactions | 2 | v2 | P5 | |
| ConsumerGroupHeartbeat | 1 | v1 (adds `subscribed_topic_regex`) | P4 | KIP-848 — primary group protocol |
| ConsumerGroupDescribe | 1 | v1 | P5 (done) | table said P4 but it was never implemented there; it is the only call that describes a KIP-848 group, so it landed with DescribeGroups |
| ListConfigResources | 1 | v0 + v1 (type filter, KIP-1142) | P5 | table said max 0; the 4.3 schema has v0-v1 |
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
versions (9/5/4/5/6/5 respectively per the guide) (P4). All are done;
DescribeGroups v6 and ListGroups v5 landed with the P5 admin group ops, since
they are admin calls rather than part of the coordination path.

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

- [x] **Error taxonomy.** DONE (commit 8ca9b85): `errors.mbt` with the full
      published 4.3 table (codes -1–133) generated mechanically from the
      protocol page + `Errors.java`; `BrokerError(code, context)` suberror
      with `name()`/`is_retriable()`; broker-error raise sites converted;
      `DecodeError` unchanged.
- [x] **Primitives.** DONE (commits 47aebad, 105e3bf): murmur2 port of
      `Utils.murmur2` verified against all six `UtilsTest` vectors, producer
      now partitions by `(murmur2(key) & 0x7fffffff) % n`; `Uuid` with
      Java-canonical 22-char base64url formatting, parse roundtrip, and
      validation. Random uuid generation deferred — needs a CSPRNG decision
      (see risks).
- [x] **Tagged-field encode.** DONE (commit 01827aa):
      `Encoder::write_tagged_fields(Array[(Int, Bytes)])` writes ascending,
      plain-uvarint tag buffers; `write_tag_buffer()` stays for the empty
      case. Roundtrip + skip tests in place.
- [x] **Config structs.** DONE (commit 94b749a, root package for now):
      `CommonConfig` (bootstrap server list with host:port/[ipv6]:port
      parsing, client_id, request_timeout_ms, idle/retry knobs,
      `SecurityProtocol` + `SaslConfig` shells), `ProducerConfig`,
      `ConsumerConfig`, all validated at construction; producer and consumer
      gained `connect_with_config`, old named-arg connect kept as sugar;
      `Connection` sends the configured client_id.
- [x] **Time/backoff helpers.** DONE (commit bfa6a86): pure `backoff_ms`
      growth function, jittered `Backoff` schedule (±20% wall-clock jitter),
      `Deadline` type on `@async.now()`; unit tests.
- [x] **Package split.** DONE (commit c898663): `buf/` (Encoder/Decoder/
      DecodeError) and `internal/` (crc32c, murmur2) are now subpackages;
      codec/hash tests moved with their code. Deviation from the original
      wording: instead of wrapper re-exports, the root surface was
      deliberately shrunk — Encoder/Decoder/crc32c/murmur2 are no longer
      public API, and `DecodeError` lives in `@buf` (cmd/main and user code
      match `@buf.Malformed` etc.). `pub type X = @pkg.Y` aliases remain
      available for future public cross-package types.

**Acceptance:** `moon test` green; murmur2 vectors pass; producing with keys
lands on the same partitions as the Java/librdkafka clients on the same topic.

### Phase 1 — Transport hardening

- [x] **BrokerConnection v2** — DONE at root scope (commits aee0edc,
      197b36b; the `net/` package move folds into the later protocol/
      split): pipelining via write-lock + frame-level read-lock + pooled
      out-of-order responses; per-request `with_timeout` closing the
      connection on timeout; EOF/corrupt-frame failures raise
      `TransportError` and close; max in-flight via semaphore. Producer and
      consumer migrated onto it; the old serial `Connection` is deleted.
      Design notes: cancellation errors must pass through untranslated
      inside frame reads; the write lock's scope must end before awaiting
      responses (defer-in-helper); `with_task_group` JOINS spawned tasks at
      exit — infinite accept loops need `spawn_bg(no_wait=true)`.
- [x] **Reconnect & bootstrap set.** DONE (commit 6d1ff1a):
      `BootstrapServers` rotates round-robin; `connect_bootstrap` fails
      over at dial time; producer `send` recovers from `TransportError` and
      leadership errors by re-dialing, refreshing metadata, and retrying
      with backoff (bounded by `retries`, default now 10 until
      delivery.timeout arrives in Phase 3); consumer `poll` recovers and
      keeps read positions across metadata refreshes. `REBOOTSTRAP_REQUIRED`
      handling landed with the Phase 2 version-negotiation rework
      (commit e8afa08): produce errors 5/6/129 all route to recovery,
      which re-dials the bootstrap set.
- [x] **Throttling.** DONE (commit 738dad9): every response decoder returns
      the parsed `throttle_time_ms`; the connection records the furthest
      deadline and delays subsequent requests. Tested via the fake broker
      serving a 400ms hint.
- [x] **TLS.** DONE (commit after 738dad9): `BrokerStream` enum
      (`Plain(Tcp) | Secure(Tls)`), `TlsClientOptions` (server name for
      SNI/verification, optional custom CA PEM file), wired through config
      and both clients for Ssl / SaslSsl; the fake broker serves TLS from a
      committed self-signed test certificate, and integration tests produce
      over verified TLS and over SASL_SSL.
- [x] **SASL.** DONE (commit 7f0d5a4 and follow-ups): SaslHandshake v1
      (non-flexible framing) + SaslAuthenticate v2, mechanisms PLAIN,
      SCRAM-SHA-256, SCRAM-SHA-512 (PBKDF2 built on moonbitlang/x crypto,
      RFC 7914/4231 + RFC 7677 vectors), and OAUTHBEARER via a
      user-supplied token provider. Wired through config onto every
      connection (bootstrap, leaders, brokers from metadata); failures
      raise `SaslError` and close the connection. Re-authentication on
      session_lifetime_ms is tracked for Phase 2.
      - PLAIN (trivial)
      - SCRAM-SHA-256/512: pure-MoonBit client (HMAC-SHA256/512 + PBKDF2 +
        SHA-256/512 — check `moonbitlang/core`/`x` crypto coverage; fall back
        to C FFI if a building block is missing)
      - OAUTHBEARER: token callback hook (user-supplied async fn), token
        refresh, `private_key_jwt`-style client-assertion helper (KIP-1258
        era) deferred unless requested
      - re-authentication: honor `session_lifetime_ms` from
        SaslAuthenticate; schedule transparent re-auth.
- [x] **Cluster layer**: DONE (commit pending, root scope `cluster.mbt`;
      the plan's `cluster/` package move takes it once the sender task and
      fetcher depend on it): `ClusterClient` owns the bootstrap set, a
      BrokerConnection pool keyed by node id (dialed on demand with SASL +
      ApiVersions negotiation), metadata caching with refresh scheduling
      (expiry via `metadata_max_age_ms` + `refresh_if_stale`, error trigger
      via `invalidate_metadata`/explicit refresh), the topic-id map
      (KIP-516), and a batched `FindCoordinator` v4 helper (KIP-699,
      `coordinator.mbt`, group/txn/share types). Producer and consumer
      migrated onto it — their duplicated `meta_conn`/`brokers`/
      `leader_conns`/recover flows deleted; the consumer additionally
      gained TLS/SASL leader connections it never had (its pool dials now
      go through the same SASL/TLS wiring as everyone else).

**Acceptance:** against a local 4.3 broker in Docker: pipelined parallel
requests; kill/restart broker mid-test → clients recover; TLS + SCRAM
(PLText/SSL listeners in the compose file) produce/consume succeed; OAuth
callback path unit-tested against a mock broker.

### Phase 2 — Protocol completeness (data plane)

- [x] **Version negotiation matrix.** DONE (commit e8afa08): `versions.mbt`
      (root scope; folds into the later `protocol/` split) holds the API
      key constants and the driver's per-API floor/ceiling matrix;
      `decode_api_versions_response` now parses the broker's ranges into
      `BrokerVersions`, stored on every connection (the handshake runs in
      `connect_bootstrap` and on leader dials, like the Java client);
      `BrokerVersions::pick` / `BrokerConnection::api_version` centralize
      the choose-highest-common logic and raise clear no-overlap errors
      (the old hard failure). Requests in `client.mbt` negotiate their
      version per call. This also delivered the deferred
      `REBOOTSTRAP_REQUIRED` (129) handling: the producer's recovery path
      re-bootstraps on it.
- [x] **Metadata**: DONE (commit 2164447): codecs moved to `metadata.mbt`
      (root scope; folds into the `protocol/` split) and now dispatch on
      the negotiated version — v12 and v13 (v13 appends a top-level
      error code). `TopicMetadata` exposes `topic_id` (needed by Fetch
      v13+) and `is_internal`; requests take a topic list where None
      means all topics (regex-expansion ready). `DescribeTopicPartitions`
      v0 added with cursor pagination (nullable struct = signed-byte
      marker per the Java generator; ELR arrays skipped), exposed as
      `BrokerConnection::describe_topic_partitions` and exercised
      end-to-end against the fake broker across two pages. Bonus:
      `@buf` decoder bounds checks hardened (subtraction-based) so
      malformed huge lengths raise instead of panicking — the start of
      the decoder-robustness bullet below.
- [x] **Produce**: DONE (commit 63ce59b): codecs moved to `produce.mbt`,
      dispatching on the negotiated version — v12 (topic names) and v13
      (topic-id, KIP-516), with the zero-id fail-fast when metadata has
      not supplied an id. `ProducePartitionResult` now carries
      `base_offset`, `log_append_time`, `log_start_offset`, and surfaced
      `record_errors`/`error_message` (KIP-467); the producer's error
      paths include the broker's message and offending batch indices.
      Acks validation stays in `ProducerConfig` (1 or -1). The fake
      broker advertises and serves v13, so end-to-end produce runs over
      topic-id addressing. Note for Phase 3 transactions: per the 4.3
      schema comment, a producer without txn v2 must cap at v11 inside
      transactions — the txn manager needs its own version choice.
- [x] **Fetch**: DONE (commit 84447fd): codecs in `fetch.mbt` cover the
      whole implemented range — v12 (names), v13+ (topic ids), v15
      (replica_id dropped from the mandatory fields, KIP-903), v16
      (tagged NodeEndpoints skipped); `last_fetched_epoch` is encoded
      (-1 until the P4 epoch tracking lands). **Incremental fetch
      sessions** (KIP-227) via `FetchSession`: full request (epoch 0) →
      adopt the granted session id → incremental requests carrying only
      changed positions and forgotten partitions; eviction
      (`FETCH_SESSION_ID_NOT_FOUND` / `INVALID_FETCH_SESSION_EPOCH`),
      broker-closed sessions (id 0 reply), and epoch wrap all restart
      from a full request. The consumer polls through one session per
      leader, re-establishes once after eviction, and drops sessions on
      metadata refresh. Truncated-batch handling is explicit:
      `decode_record_batches_ex` reports a max_bytes-split trailing
      batch, results carry `records_complete`, and the read position
      stays offset-safe. Leader-epoch validation
      (`OffsetForLeaderEpoch`, diverging-epoch tagged fields) stays
      queued for P4 as planned.
- [x] **ListOffsets** v10/v11 DONE (commit 9f0f0f7): codecs moved to
      `offsets.mbt`; v10 appends `timeout_ms` (KIP-1075), v11 adds the
      earliest pending upload sentinel (KIP-1023); all sentinels are
      public constants and a timestamp query is this API with a
      wall-clock timestamp (`OffsetForTimestamp`). The fake broker
      advertises v11.
- [x] **Record layer** DONE (commit 60d07c5): `Record` carries its
      `headers` array through encode and decode; `decode_record_batches_detailed`
      exposes per-batch `producer_id`/`producer_epoch`/`base_sequence` and the
      `is_control`/`is_transactional` flags (flat views still hide control
      batches from consumers); `collect_committed` is the read_committed
      primitive (drop control batches + records of aborted transactions by
      producerId/firstOffset); `RecordBatchBuilder` does incremental append,
      size accounting, and CRC finalizing for the Phase 3 accumulator.
- [x] **Compression** (`compression/`) DONE (commit 6b59a10): one
      `Codec` interface (`compress`/`decompress`, raising
      `@buf.DecodeError`) mapped to the RecordBatch attribute bits
      0-4. Snappy (raw block codec + xerial chunk framing) and LZ4
      (block codec with greedy matcher, Kafka's frame framing with
      XXH32 descriptor checksum) are pure MoonBit. Gzip wraps the
      system zlib through native FFI — deviation from D4: the
      async/gzip package is `@io`-async while batch decoding is
      synchronous, so the buffer-to-buffer wrapper was not usable;
      native-only module made FFI the pragmatic choice. Zstd decodes
      via the vendored zstd single-file decompressor
      (`compression/zstddeclib.c`); zstd *compression* is deferred to
      the Phase 3 producer codec config (the vendored tree is
      decode-only). Batch decode unwraps the whole-batch records
      region per the attribute bits, so consumers read whatever
      producers wrote. Golden fixtures under `test/golden/` (with
      `generate.py`) come from independent implementations — cramjam's
      Rust snappy/lz4/zstd and Python gzip — as raw streams and as
      whole batches with foreign-compressed record regions spliced in
      (the Java-client golden capture itself remains deferred to the
      Docker integration phase).
- [x] **Decoder robustness** DONE (commit 6b59a10,
      `robustness_test.mbt`): deterministic-xorshift fuzz over the four
      compressed streams (truncation at every 17th offset plus random
      blobs), the record-batch decoder (every truncation offset of a
      valid batch plus 200 random blobs), and five wire decoders
      (ApiVersions/Metadata/Produce/ListOffsets/DescribeTopicPartitions)
      — everything must raise a decode error or succeed, never panic.
      Backed by the subtraction-based bounds checks hardened earlier in
      this phase.

**Acceptance:** codec round-trip tests for all implemented APIs at both
implemented versions (encode → decode equality via `debug_inspect`
snapshots); golden decode tests from Java-produced Fetch responses with each
compression codec; `moon coverage analyze` shows codec branches covered.

### Phase 3 — Full producer

- [x] **Partitioner**: DONE (commit f31cdc2): `Partitioner` config strategy
      — `Murmur2` (default: keyed by murmur2, unkeyed sticky), `Murmur2RoundRobin`,
      `RoundRobin`; `StickyPartitioner` reuses one partition per batch and
      steps to the next at batch boundaries (KIP-794, sequential step
      instead of the Java cache's random roll); routing lives in a
      package-private `PartitionRouter` so the rules test without a
      connection. `send` takes a manual `partition` override validated
      against the partition count. Boundary switching is driven per send
      until the accumulator batches records.
- [x] **Batching accumulator**: DONE (this commit, root scope
      `accumulator.mbt`; folds into the `producer/` split with the sender
      work): per-partition queues of `ProducerBatch` over the Phase 2
      `RecordBatchBuilder` (new `estimated_append_size` dry-run for the
      has-room check); `batch_size` rotation, `linger_ms` readiness, and
      `buffer_memory` accounting raising the new `BufferExhausted`
      suberror. One flusher claims each batch (`close_and_claim`), puts it
      on the wire, and publishes the terminal result through a
      per-batch semaphore; coalesced senders derive `base_offset + index`.
      `send` now appends instead of encoding one record per request, takes
      `headers`, and drains queued predecessors first so per-partition
      order survives coalescing (the inline stand-in for the sender
      task's front-first drain); recoverable failures requeue the batch at
      the queue head and retry within the existing backoff/attempts bound.
      Sticky boundaries moved from per-pick to batch-close. E2E: two
      concurrent sends coalesce into one Produce request and take offsets
      base+0/base+1.
- [x] **Sender task**: DONE (this commit, root scope `sender.mbt`; joins
      the `producer/` split with the accumulator): a per-producer drain
      loop spawned into the caller's task group (`connect`/`connect_with_config`
      now take `group~` — structured concurrency leaves no ambient group
      to spawn into; `close()` stops the loop, which force-closes open
      batches, drains everything, resolves every pending send, and tears
      the cluster down before the group joins it). Each round takes the
      accumulator's ready batches (linger expired, full, or closing), one
      per partition to keep per-partition order, groups them by leader,
      and puts one Produce request per leader on the wire — pipelined up
      to `max_in_flight` (default 5) by the connection's own semaphore.
      `acks=0` writes the frame without registering a response
      (`BrokerConnection::send_only`) and resolves senders with -1, Java
      parity. Retriable produce-response codes (`error_retriable`) and
      transport failures requeue at the queue front, run recovery, and
      pace the next round with backoff, all inside the new
      `delivery_timeout_ms` (default 120s, validated >= linger +
      request_timeout; batches expire with a delivery error when it
      passes). Per-record results resolve through per-waiter semaphores
      registered under the accumulator lock (no lost wakeups, any number
      of coalesced senders). Known limitation, noted for the D6 polish:
      the sender ticks every 5ms instead of waking on first append, and
      terminal dial errors (SASL) retry until delivery timeout instead of
      failing fast.
- [x] **Public API**: DONE (commit 140051e): `send` is the await-everything
      path over the new `send_handle`, which appends once and returns a
      `SendHandle` (`await() -> Int64 raise` fast-pathing cancellation,
      per-waiter completion semaphores so any number of coalesced senders
      resolve; `cancel()` stops the wait without retracting the records).
      `send_all` appends a batch of `SendRecord`s and returns one handle
      per record; `on_complete` runs an async callback through the
      producer's task group with `Sent(offset)`/`Failed(reason)`.
- [x] **Idempotence** (`enable_idempotence`, default on when acks=all):
      DONE (commit 1948abf): InitProducerId v5 pinned from the 4.3 schema
      (fresh identity at connect for plain idempotent producers; epoch
      bump reuses it with pid/epoch set); batches carry pid/epoch and a
      per-partition sequence base stamped at append (every record consumes
      one sequence, rewind on terminal failure); `UNKNOWN_PRODUCER_ID`
      triggers the Java client's reset rules — bump the epoch, restart all
      sequences from zero, re-stamp in-flight batches, retry. Config
      validation: idempotence requires acks=-1 and `max_in_flight <= 5` to
      preserve ordering.
- [x] **Transactions**: DONE (commit 286ff08): `transactional_id` config
      (idempotence required, so acks must be -1; `transaction_timeout_ms`
      registered at connect) turns the Producer transactional — init runs
      FindCoordinator(Transaction) + InitProducerId at the coordinator with
      retries while it loads. `begin_transaction`/`commit_transaction`/
      `abort_transaction` bracket sends; the sender registers each round's
      new partitions with AddPartitionsToTxn v3 (client shape, KIP-890)
      before producing, tracked in a per-transaction added-set.
      `send_offsets_to_transaction` registers the group via AddOffsetsToTxn
      v4 then lands offsets with TxnOffsetCommit v4 at the group
      coordinator (v5 is wire-identical and the schema caps non-2PC
      transactional clients at v4). EndTxn v5 commits/aborts and adopts the
      coordinator-bumped identity (KIP-890 part 2: epoch up every
      transaction, sequences continue). Coordinator hiccups
      (COORDINATOR_LOAD_IN_PROGRESS/NOT_COORDINATOR/concurrent) retry with
      backoff and re-resolve; fencing (INVALID_PRODUCER_EPOCH/
      PRODUCER_FENCED) raises. Abort-on-error: a terminal send failure
      inside the transaction poisons commit (it raises and requires
      abort). Produce requests carry the transactional id. E2E against the
      fake broker: commit/abort flows, identity continuation across
      transactions, fenced produce poisoning the commit, NOT_COORDINATOR
      retry, and transactional offset commits.
- [x] **Producer metrics/counters**: DONE (commit e8f207d): `metrics()`
      returns `ProducerMetrics` — queued records/bytes from the
      accumulator snapshot, in-flight batches, sent/failed record and
      batch counters, and the sum of connection throttle hints.

**Acceptance:** integration tests vs Docker 4.3: throughput sanity (≥10k
rec/s small records, uncompressed and gzip); ordering preserved under retries
with idempotence; kill -9 broker during in-flight sends → no loss/dupes with
acks=all+idempotence; transaction produce-commit visible to read_committed
consumer, abort invisible; duplicate detection verified by replaying with
same PID/sequence (mock broker test).

### Phase 4 — Full consumer

- [x] **Offset management**: DONE (commit 3604bf9, root scope
      `offset_commit.mbt` + `consumer_offsets.mbt`; folds into the
      `consumer/` split): OffsetCommit v8-v10 and OffsetFetch v8-v10
      codecs pinned from the 4.3 schemas (v9 carries the KIP-848 member
      identity, v10 addresses topics by id; error codes travel as values
      for the manager to classify). The consumer gained a group-aware
      config (`group_id`, `enable_auto_commit`,
      `auto_commit_interval_ms`; auto-commit without a group is inert),
      a committed-offset cache, `commit(offsets?)` — positions or
      explicit — retrying COORDINATOR_LOAD_IN_PROGRESS /
      COORDINATOR_NOT_AVAILABLE / NOT_COORDINATOR /
      REBALANCE_IN_PROGRESS with backoff (the coordinator node caches
      until NOT_COORDINATOR), `commit_async(on_complete)` spawning into
      the consumer's task group, and `committed(partitions?)` refreshing
      the cache from the coordinator. Seeks: `seek` (validated against
      the assignment), `seek_to_beginning`/`seek_to_end`/`
      seek_by_timestamp` over ListOffsets. Autocommit is a background
      loop in the consumer's task group (connect now takes `group~`,
      structured concurrency like the producer): commits every interval
      at SENDER_TICK granularity and once more on close, then tears the
      cluster down before the group joins it. Commit-on-revoke arrives
      with the rebalance listeners (group membership below). E2E:
      commit/read-back round-trip through the fake coordinator store,
      load-retry, async callback, session-aware seek sequencing,
      autocommit interval + close.
- [x] **Fetcher**: DONE (commit 5a0f849): `poll` runs one concurrent
      long-poll fetch per leader through the incremental sessions, capped
      by `max_poll_records` (the position rewinds to the first
      unconsumed record so a capped poll re-fetches the rest) and
      `max_partition_fetch_bytes` (per-partition cap in every request).
      `pause`/`resume`/`paused` skip partitions while keeping positions.
      OFFSET_OUT_OF_RANGE follows the `auto_offset_reset` policy
      (ResetEarliest/ResetLatest/ResetNone raising); UNKNOWN/FENCED_
      LEADER_EPOCH answers trigger the KIP-320 check — OffsetForLeaderEpoch
      v4 (pinned from the 4.3 schema, key 23) asks the leader where the
      known epoch ends and rewinds the position on truncation, else the
      metadata refresh heals. `enable_read_committed` fetches with
      READ_COMMITTED isolation and filters the response's
      aborted-transaction list via `collect_committed`; fetch responses
      now carry the detailed decoded batches, and positions advance past
      whole batch ends (last_offset) so control/aborted ranges never
      re-fetch. The record batch builder gained `set_transactional`.
      Decompression and per-partition caps were already wired from Phase
      2. E2E: pause/resume wire behavior, cap+rewind sequencing,
      read_committed filtering against an aborted batch, epoch-error
      rewind.
- [x] **KIP-848 member**: DONE (commit d00bb89, root scope
      `consumer_group_new.mbt` + `consumer_group_heartbeat.mbt`):
      ConsumerGroupHeartbeat v0/v1 pinned from the 4.3 schemas (v1
      carries SubscribedTopicRegex). The member generates its own id
      (clock-seeded uuid-hex, kept for the consumer's lifetime), joins
      with epoch 0, and re-issues the heartbeat every server-guided
      interval (slept in tick-sized steps so close() lands promptly).
      Server-driven assignments apply atomically: revoke-set diff,
      assignment swap, assign-set — firing RebalanceListener on_revoke /
      on_assign around the swap. UNKNOWN_MEMBER_ID, STALE_MEMBER_EPOCH,
      and FENCED_MEMBER_EPOCH restart the join from epoch 0;
      GROUP_MAX_SIZE_REACHED and coordinator hiccups (load/unavailable/
      moved) retry with backoff (NOT_COORDINATOR re-resolves);
      UNSUPPORTED_ASSIGNOR, UNRELEASED_INSTANCE_ID, and
      INVALID_REGULAR_EXPRESSION are fatal and surface via
      `membership_error()`. Static membership sends the configured
      `group_instance_id` on every heartbeat. Regex subscription rides
      v1 heartbeats verbatim and expands client-side against the
      metadata catalog (refreshed periodically) through a
      glob-style matcher (`*`, `?`/`.` — a documented subset of Java
      regex; full patterns land with the polish pass). `unsubscribe`
      leaves gracefully with the epoch -1 heartbeat, firing the revoke
      on the way out; poll fetches only assigned partitions. E2E:
      join/reconcile/leave with listener hooks and epoch progression,
      fenced-epoch restart, static instance id, regex expansion.
- [x] **Classic member**: DONE (commit 3652d1c, root scope
      `consumer_group_classic.mbt` + `classic_group.mbt` +
      `consumer_protocol.mbt` + `assignment.mbt`): JoinGroup v9 →
      SyncGroup v5 → Heartbeat v4 loop with LeaveGroup v5 on exit, all
      pinned from the 4.3 schemas. Protocol negotiation: the member
      offers its configured assignment strategies as named protocol
      entries; the coordinator settles one and the leader computes the
      assignment over every member's ConsumerProtocolSubscription (the
      embedded int16-framed binary protocol, hand-coded) using fresh
      metadata partition counts, then hands out
      ConsumerProtocolAssignment bytes. Assignors: RangeAssignor
      (contiguous per-topic chunks), RoundRobinAssignor (one sorted
      circle), StickyAssignor (keep prior assignments, fill gaps
      least-loaded-first, then balance to a minimal spread — same
      balance/stickiness properties as Java with deterministic
      id-ordered tie-breaking instead of its random order), and
      CooperativeStickyAssignor running the KIP-429 two-round protocol:
      owned partitions ride the subscription userData, the leader's
      first round sends keep-sets (revocations), the revoking member
      rejoins, and the second round delivers the additions. Static
      membership (`group_instance_id`) rides every request; REBALANCE_
      IN_PROGRESS heartbeats rejoin, UNKNOWN_MEMBER_ID /
      ILLEGAL_GENERATION restart the join, FENCED_INSTANCE_ID is fatal;
      LeaveGroup on close/unsubscribe; rebalance listeners fire around
      each assignment change. E2E against a stateful mini-coordinator
      in the fake broker: single-member join/sync/leave with generation
      and protocol assertions, two-member range split with rebalance on
      join, and the cooperative two-round revoke-then-assign converge
      (order-agnostic, convergence-based assertions).
- [x] **Consumer surface**: DONE (commit 598d46d, root scope
      `consumer_surface.mbt`): `subscribe(topics)` / `subscribe_regex`
      / `subscribe_classic` versus `assign(partitions)` / `unassign()`
      are mutually exclusive paths (assign raises while a membership is
      active; positions start from committed offsets when a group is
      configured, else the auto-offset-reset sentinel). `poll` enforces
      `max_poll_interval_ms`: a gap past the window gracefully leaves
      the group (LeaveGroup / epoch -1 heartbeat) and raises instead of
      ghosting. `position(partition)`, `committed(partitions?)` (P4.1),
      `assignment()`, and `group_metadata()` (group id, member id,
      generation) report state. Deserializers: records carry their
      `Bytes?` natively (identity) with `key_utf8`/`value_utf8` typed
      helpers; generic parameterized decoders are deferred to the
      API-stability pass, where making the consumer generic can be
      judged against real usage.
- [x] **`group_protocol` config**: DONE (commit 89075f7):
      `GroupProtocol` — `ConsumerProtocol` (KIP-848, the default)
      fails fast when the broker does not advertise
      ConsumerGroupHeartbeat; `ClassicProtocol` always rides the
      classic APIs; `PreferConsumer` / `PreferClassic` probe the
      negotiated ApiVersions ranges (`ClusterClient::supports_api`) and
      degrade to the other path. `subscribe()` dispatches on the
      resolved protocol; explicit `subscribe_classic` remains the
      escape hatch, and regex subscription stays KIP-848-only (the
      classic protocol has no regex field).
      **Interop caveats:** (1) the cooperative-sticky owned-partition
      report uses our own assignment encoding in the subscription
      userData slot — Java serializes a private format there, so
      mixed-vendor cooperative groups will not converge on shared
      partition movements (single-vendor groups are correct; eager
      range/roundrobin interop is unaffected); (2) regex expansion
      implements a glob subset (`*`, `?`/`.`), not full Java regex —
      patterns outside the subset should use explicit topic lists; (3)
      the classic path reports `UNSTABLE_OFFSET_COMMIT` through commit
      retries rather than surfacing `require_stable` semantics
      explicitly.

**Acceptance:** integration suite vs Docker 4.3: N consumers in one group
over M partitions converge; rebalance on member join/leave/crash with no
duplicate processing beyond at-least-once expectations; commit/restore
offsets across restart; regex subscription picks up newly created topics;
static member restart does not trigger rebalance; cooperative sticky
incremental rebalance path exercised (assert via broker logs or
DescribeGroups); KIP-848 fencing test (old epoch member gets fenced and
rejoins); read_committed skips aborted txns (producer txn test from P3).

### Phase 5 — Admin client

- [x] `Admin` struct over the cluster layer: DONE (commit 8803fb9,
      root scope `admin.mbt`; folds into the `admin/` split with the
      ops): `AdminConfig` carries the admin-specific retry knob
      (`admin_retries`); ops run through `with_retries` — a result the
      op classifies retriable re-issues with backoff, transport
      failures raise. Routing helpers for any-broker (control
      connection, the default — brokers forward to the controller) and
      controller-routed ops (`controller_conn`, backed by the
      controller id metadata now keeps and exposes). Per-item results
      mirror the protocol: DescribeTopicPartitions per-topic error
      codes travel as values through `TopicMetadata` (the old
      raise-on-topic-error behavior became the admin value shape), with
      pages merged per topic behind the cursor walk.
- [x] Topic ops: DONE (commit 80fc6e8, root scope `admin_topics.mbt`):
      CreateTopics v7 (config overrides + explicit replica assignments,
      topic id echoed), DeleteTopics v6 (by name, by topic id, or
      mixed), CreatePartitions v3 (counts + new-partition placement),
      DeleteRecords v2 (low watermarks back), OffsetDelete v0 per
      group/topic/partition — all per-item error codes as values under
      the retry policy. OffsetDelete v0 is the one non-flexible API in
      the set and uses header-v0 framing both directions. E2E against
      the fake broker: full create → grow → truncate → offset-delete →
      delete-by-name → delete-by-id lifecycle.
- [x] Cluster/config: DONE (this commit, root scope `admin_cluster.mbt`):
      DescribeCluster v0-v2 (brokers with rack + fenced flags, controller
      id, cluster id, `endpoint_type`, authorized-operations bitfield),
      DescribeConfigs v4 (sources, synonyms, types, docs),
      IncrementalAlterConfigs v1 (set/delete/append/subtract) +
      AlterConfigs v2 (legacy full-replace), ListConfigResources v0-v1
      (v1 adds the resource-type filter, KIP-1142), DescribeLogDirs
      v2-v5 (`is_cordoned` at v5, KIP-1066; volume total/usable bytes at
      v4+), ElectLeaders v2 (preferred/unclean, nullable topic list),
      AlterPartitionReassignments v0-v1 (v1 toggles replication-factor
      changes) + ListPartitionReassignments v0, UnregisterBroker v0,
      DescribeQuorum v2 (voters/observers with log-end offsets and
      fetch/caught-up timestamps, KIP-853 node/listener map; the one
      admin API without a throttle field), and UpdateFeatures v2
      (upgrade/safe/unsafe-downgrade types, validate-only; v2 dropped
      per-feature results). Keys and enum byte values (config resource
      types, config ops, election types, endpoint types, upgrade types)
      pinned from the 4.3 schemas and `ConfigResource.java`/
      `AlterConfigOp.java`/`ElectionType.java`. UnregisterBroker and
      UpdateFeatures route through the controller connection; the rest
      take the any-broker control connection. Per-item error codes
      travel as values under the retry policy. E2E against the fake
      broker plus codec tests covering the version-dependent shapes the
      fake does not serve (DescribeCluster v0, DescribeLogDirs v2-v5,
      reassignments v0, ListConfigResources v0).
- [x] Groups — describing them: DONE (this commit, root scope
      `admin_groups.mbt`): DescribeGroups v6 and ConsumerGroupDescribe v1.
      This bullet's original wording, "DescribeGroups v6 (incl. 848
      members)", is not what the protocol offers: DescribeGroupsResponse
      v6 adds no KIP-848 fields (v6 adds only the per-group ErrorMessage,
      KIP-1043), and the coordinator's `GroupMetadataManager.classicGroup`
      raises GroupIdNotFoundException for any non-classic group, which v6
      turns into error code GROUP_ID_NOT_FOUND (69) with group state
      "Dead" and no members. So DescribeGroups describes classic groups
      only — `AdminGroupDescription::is_not_found` marks the 848 case —
      and ConsumerGroupDescribe (key 69, tabled for P4 in §5 but never
      implemented there) is what returns new-protocol members: member,
      group, and assignment epochs, topic-name and regex subscriptions,
      current versus target assignment, rack and instance ids, and the v1
      MemberType byte (KIP-1099: -1 unknown, 0 classic, +1 consumer, a
      group mid-migration holding both). Classic member metadata and
      assignment stay opaque bytes with `subscribed_topics` /
      `assigned_partitions` helpers over the Phase 4 ConsumerProtocol
      codecs, which report nothing rather than raising on a foreign
      protocol (Kafka Connect, say). Both pin to a single version — 4.0
      brokers already advertise DescribeGroups 0-6 and ConsumerGroupDescribe
      0-1 — both take the any-broker control connection, and per-group
      error codes travel as values under the retry policy. E2E against the
      fake broker (one DescribeGroups call covering a classic group and an
      848 group, then ConsumerGroupDescribe on that same 848 group) plus
      codec tests for the byte-identical request bodies, the
      empty-assignment tag buffers, the unknown member type, foreign
      member metadata, and both not-found paths.
- [x] Groups — ListGroups v5 (state/type filters): codecs, `Admin::list_groups`
      fanning out to every broker in the cached snapshot and merging (each
      broker answers only from the state it holds itself), per-call error code
      under the retry policy; fake-broker E2E and codec tests.
- [x] Groups — DeleteGroups v2: codecs, `Admin::delete_groups` with
      per-group error codes as values under the retry policy; fake-broker
      E2E and codec tests.
- [x] DescribeProducers v0: codecs, `Admin::describe_producers` over the
      any-broker control connection (the broker forwards to partition
      leaders), per-partition error codes as values under the retry policy;
      fake-broker E2E and codec tests.
- [x] DescribeTransactions v0: codecs, `Admin::describe_transactions` over
      the any-broker control connection (the broker forwards to the
      transaction coordinator), per-id error codes as values under the
      retry policy; fake-broker E2E and codec tests.
- [x] ListTransactions v2: codecs (v1/v2 request split at the
      transactional-id pattern field), `Admin::list_transactions` fanning
      out to every broker and merging, per-call error code under the retry
      policy; fake-broker E2E and codec tests.
- [x] Security — ACLs: DONE (this commit, root scope `admin_acls.mbt`):
      DescribeAcls v2-v3, CreateAcls v2-v3, DeleteAcls v2-v3 (v3 only
      adds the USER resource type — v2/v3 share one wire shape, so the
      matrix negotiates either). Enum byte values pinned from
      `ResourceType.java`, `PatternType.java`, `AclOperation.java`,
      `AclPermissionType.java` and surfaced as `ACL_RESOURCE_*` /
      `ACL_PATTERN_*` / `ACL_OPERATION_*` / `ACL_PERMISSION_*`
      constants. `AclFilter` (None fields match anything) drives
      describe/delete; `AclCreation`/`AclBinding` carry creations and
      results; DeleteAcls reports per-filter and per-matching-ACL error
      codes as values under the retry policy. E2E against the fake
      broker (create → describe → delete round trip with wire captures)
      plus codec tests for the shared v2/v3 shape and per-item error
      paths.
- [x] Security — quotas and SCRAM: DONE (this commit, root scope
      `admin_quotas.mbt`): DescribeClientQuotas v0-v1, AlterClientQuotas
      v0-v1, DescribeUserScramCredentials v0, AlterUserScramCredentials
      v0. The quota pair straddles the flexible boundary (v1 enables
      flexible versions), so its codecs shape string/array lengths and
      tag buffers by version and the connection frames v0 with the
      legacy header; the SCRAM pair is flexible from v0. `@buf` gained
      FLOAT64 (`write_f64`/`read_f64`) for quota values. Entity type
      strings pinned from `ClientQuotaEntity.java`, match types from
      the DescribeClientQuotas schema ({0 = exact name, 1 = default
      name, 2 = any specified name}), mechanism bytes and the
      4096-16384 iteration bounds from `ScramMechanism.java`.
      `ScramCredentialUpsertion::from_password` derives the salt and
      the RFC 5802 SaltedPassword over the Phase 1 SCRAM primitives, so
      callers hand over a plaintext password. Per-item error codes
      travel as values under the retry policy; all four take the
      any-broker control connection — `KafkaApis` serves both describes
      locally and forwards AlterClientQuotas and
      AlterUserScramCredentials to the controller. E2E against the fake
      broker (describe → alter quotas, describe → alter SCRAM with wire
      captures) plus codec tests for the v0/v1 quota request shapes,
      per-item error paths, and the null "all users" array.
- [x] Topic-id-first admin ops: DONE (commit 80fc6e8): DeleteTopics v6
      addresses deletions by topic id alone (null name + id, KIP-516),
      and DescribeTopicPartitions returns ids for follow-up ops; the
      remaining admin APIs in range are name-addressed by their v4.3
      schemas, so there is nothing further to prefer ids on.

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
- ~~**zstd via C FFI**~~ RESOLVED (Phase 2): the zstd single-file
  DECOMPRESSOR is vendored (`compression/zstddeclib.c`) — no external
  libzstd dependency. Gzip links the universally present system zlib
  (`-lz` from the root and cmd/main moon.pkg). Zstd compression is
  deferred to Phase 3, when the producer codec config lands.
- **moonbitlang/async maturity**: long-lived background tasks + cancellation
  semantics for heartbeat/sender loops need care (leaks on close paths);
  mock-broker tests should cover close-under-load.
- **Classic vs KIP-848 feature parity**: regex subscription for classic
  groups is client-side (metadata scan) and works differently from 848's
  server-side regex; document differences rather than force-unify.
- ~~**SCRAM pure-MoonBit crypto**~~ SPIKED (Phase 1): `moonbitlang/x@0.5.1`
  provides `sha256`/`sha512` and `hmac` over a `CryptoHasher` trait, so
  SCRAM-SHA-256/512 needs only a thin PBKDF2-HMAC loop (~15 lines) plus
  RFC 5802/7677 test vectors; no C FFI required. Client nonces need
  uniqueness, not CSPRNG (hash of timestamp + counter + client id).
  CSPRNG for KIP-848 member ids remains open (x/uuid is format-only).
- **Client-side CSPRNG for member ids**: KIP-848 membership needs
  client-generated random member ids; `moonbitlang/async`'s `rand_bytes`
  lives in the tls package and is marked internal-use. Pick a randomness
  source (moonbitlang/x, FFI getrandom, or an async API change) before
  Phase 4.
- **Kafka 4.3.x point releases** may bump max API versions; the negotiation
  matrix (D1) makes that a non-event, but golden files should be regenerated
  per minor release.
