# Testing & release harness

This is the Phase-7 hardening guide: how unit, mock-broker, golden, and
integration tests run, and how releases are cut. Everything is driven through
the `Makefile` (`make help` for all targets) and GitHub Actions
(`.github/workflows/ci.yml`).

## Layers

1. **Unit (pure)**: codec round-trips with `debug_inspect` snapshot tests;
   known-vector tests (murmur2, CRC32C, varints, SCRAM); decoder robustness
   (truncation at every byte offset of a fixture must raise, not panic).
   Runs in <1s: `moon test -p buf -p compression -p internal`.

2. **Golden files**: `test/golden/*.bin` are byte-exact compressed RecordBatch
   fixtures produced by *independent* implementations (cramjam's Rust
   snappy/lz4/zstd and Python's gzip), captured into the repo. Decoders must
   parse them. Regenerate with `make generate-golden`
   (wrapper for `test/golden/generate.py`); CI re-runs it and fails on a diff.

3. **Mock broker (in-process testkit)**: `fake_broker_test.mbt` /
   `fake_broker_codec_test.mbt` implement a real-protocol fake broker on an
   in-process `TcpServer` (optionally wrapped in TLS) with fault injection:
   API version ceilings (`hide_apis`), throttling, session churn, and
   brokered SASL (PLAIN/SCRAM with a real scram server). This is the fast,
   hermetic home for the group/txn state machines and reconnect-under-load —
   no Kafka installation required. Runs as part of `moon test`.

4. **Integration (real Kafka 4.3)**: `test/kafka-cluster.sh` starts a KRaft
   cluster (single by default, 3 combined broker/controller nodes for the
   multi profile) and waits until the brokers accept connections.
   `make integration` brings it up and runs `test/integration.sh`, which drives
   `cmd/main` to produce a record and consume it back over the full
   socket/negotiate/encode/fetch path.

   The container engine is auto-detected — docker if its daemon answers, else
   podman — and can be forced with `ENGINE=docker|podman`. When the engine has
   a compose provider (`docker compose`, `docker-compose`, `podman compose`,
   `podman-compose`) the cluster is driven from `docker-compose.kafka.yml`,
   which also provides the 3-node profile. Without one, podman is driven
   directly with `podman run` (single broker; `MULTI=1` then needs a compose
   provider).

   ```
   make docker-up            # single broker, waits until ready
   make docker-up MULTI=1    # 3-node cluster (needs a compose provider)
   make integration          # docker-up + real-client smoke test
   make docker-down          # tear down (add KEEP_VOLUMES=1 to keep data)
   ```

## CI (GitHub Actions)

`.github/workflows/ci.yml` runs three jobs on push/PR:

| job         | what it runs                                   |
|-------------|------------------------------------------------|
| `build-test`| install MoonBit, `moon update`, build, `moon test`, `moon fmt --check`, and a `.mbti` clean-diff check (catches forgotten `moon info`) |
| `integration`| spins up `apache/kafka:4.3.0` as a service, runs the mock-broker suite and `test/integration.sh` |
| `golden`    | python3 + cramjam, regenerates `test/golden/*.bin` and fails if they differ |

## Performance (`make bench`)

`bench/bench.sh` measures produce/consume records/sec against a live broker
using the in-process `bench-produce` mode of `cmd/main`. See `bench/README.md`.

## Releases

See `CHANGELOG.md`. Before tagging:

- `make info` and commit any `.mbti` changes (public API audit).
- `make fmt`, `make test`, `make golden-commit` (regenerated fixtures).
- `make integration` once to prove the real-cluster path.
- Bump `version` in `moon.mod` per semver.

## Coverage

`moon test --enable-coverage && moon coverage analyze > uncovered.log`
(`make coverage`) — review at the end of each phase.