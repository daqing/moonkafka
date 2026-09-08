#!/usr/bin/env bash
# Real-cluster smoke test: drives moonkafka's cmd/main producer and consumer
# against a live Kafka 4.x cluster (KRaft) reachable at $KAFKA_BOOTSTRAP.
#
# This exercises the full network path a real deployment uses:
# connect over the socket/TLS layer, ApiVersions + Metadata negotiation,
# Produce record-batch encoding, Fetch with incremental sessions, and
# classic consumer-group offset commits.
#
# Usage:
#   KAFKA_BOOTSTRAP=localhost:9092 ./test/integration.sh
#   make integration          # same thing, but brings the docker cluster up first
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP:-localhost:9092}"
HOST="${BOOTSTRAP%:*}"
PORT="${BOOTSTRAP##*:}"
TOPIC="moonkafka-it-$(date +%s)"
VALUE="integration-smoke-$(date +%s)"
MOON="${MOON:-moon}"

echo "==> integration smoke against ${HOST}:${PORT} on topic ${TOPIC}"

# Produce one record (empty key so host/port can be passed positionally).
echo "==> producing..."
if ! OUT="$("${MOON}" run --target native cmd/main -- produce "${TOPIC}" "${VALUE}" "" "${HOST}" "${PORT}" 2>&1)"; then
  echo "    ${OUT}"
  echo "produce command failed"
  exit 1
fi
echo "    ${OUT}"
grep -q 'produced to' <<<"${OUT}" || { echo "produce step failed"; exit 1; }

# Consume the same record back from the earliest offset and confirm the
# payload round-trips intact.
echo "==> consuming..."
OUT="$(timeout 30 "${MOON}" run --target native cmd/main -- consume "${TOPIC}" "${HOST}" "${PORT}" earliest 2>&1 || true)"
echo "    ${OUT}" | head -5
grep -qF "value=${VALUE}" <<<"${OUT}" || { echo "consume step did not observe the produced value"; exit 1; }

echo "==> integration smoke PASSED"