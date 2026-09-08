# moonkafka benchmarks

Throughput micro-benchmark for the native client. The shell driver measures
producer/consumer records-per-second against a live broker using the
`cmd/main` CLI.

You need a running Kafka 4.x cluster (`make docker-up`) and the MoonBit
toolchain.

## Running

```sh
KAFKA_BOOTSTRAP=localhost:9092 ./bench/bench.sh
# or, after `make docker-up`:
make bench
```

`bench.sh` accounts for process-launch overhead: production runs in a single
process (`moon run --target native cmd/main -- bench-produce …`), which
measures in-process encode + send throughput rather than N process starts.
The consume side drains records from the continuous consumer under a timeout.

## Tuning

| env var         | default | meaning                                        |
|-----------------|---------|------------------------------------------------|
| `KAFKA_BOOTSTRAP`| `127.0.0.1:9092` | bootstrap `host:port`            |
| `BENCH_N`       | `10000` | records to produce (and to consume)             |
| `MOON`          | `moon`  | moon binary                                     |

## Interpreting

- `bench-produce` uses `batch_size=1048576, linger_ms=0` for maximal
  batching; the default producer config trades latency for smaller batches.
- Values are wall-clock round-trips including connection setup. On a fresh
  cluster warm up once (run twice) so the metadata/Session handshake is not
  counted on the first iteration.
- Preference: build with `--release` for representative numbers; the
  default (debug) build is noticeably slower.