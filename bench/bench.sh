#!/usr/bin/env bash
# End-to-end throughput benchmark: produce and then consume a topic through
# moonkafka's cmd/main client against a live Kafka 4.x broker.
#
# Producing runs in a single process (`bench-produce`) so the number counts
# real in-process encode/send throughput, not process-launch overhead. The
# consume side drains records from the continuous consumer until timeout.
#
# Usage:
#   KAFKA_BOOTSTRAP=127.0.0.1:9092 ./bench/bench.sh            # defaults
#   BENCH_N=50000 BENCH_SIZE=1024 ./bench/bench.sh             # tune
#
# Outputs produce-rec/s and consume-rec/s.
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP:-127.0.0.1:9092}"
HOST="${BOOTSTRAP%:*}"
PORT="${BOOTSTRAP##*:}"
N="${BENCH_N:-10000}"
TOPIC="moonkafka-bench-$(date +%s)"
MOON="${MOON:-moon}"

echo "==> bench topic=${TOPIC} records=${N} host=${BOOTSTRAP}"

echo "    producing ${N} records in-process..."
start=$(date +%s%N)
"${MOON}" run --target native cmd/main -- bench-produce "${TOPIC}" "${N}" "" "${HOST}" "${PORT}"
end=$(date +%s%N)
produce_s=$(echo "scale=3;(${end} - ${start})/1000000000" | bc)
produce_rate=$(echo "scale=1;${N}/${produce_s}" | bc)
echo "    produce: ${N} records in ${produce_s}s -> ${produce_rate} rec/s"

echo "    consuming ${N} records (drain until timeout)..."
start=$(date +%s%N)
timeout 300 "${MOON}" run --target native cmd/main -- consume "${TOPIC}" "${HOST}" "${PORT}" >/dev/null 2>&1 || true
end=$(date +%s%N)
consume_s=$(echo "scale=3;(${end} - ${start})/1000000000" | bc)
consume_rate=$(echo "scale=1;${N}/${consume_s}" | bc)
echo "    consume: ${N} records in ${consume_s}s -> ${consume_rate} rec/s"

echo "==> bench done (produce ${produce_rate} rec/s, consume ${consume_rate} rec/s)"