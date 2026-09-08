#!/usr/bin/env bash
# Local KRaft Kafka test cluster for moonkafka, on docker or podman.
#
#   test/kafka-cluster.sh up [--multi]           start the cluster, wait until ready
#   test/kafka-cluster.sh down [--keep-volumes]  tear it down
#   test/kafka-cluster.sh ps                     show status
#
# Engine: $CONTAINER_ENGINE if set, else docker (when its daemon answers), else
# podman. If the chosen engine has a compose provider (`docker compose`,
# `docker-compose`, `podman compose`, `podman-compose`) the cluster is driven
# from docker-compose.kafka.yml, which also provides the 3-node profile.
# Otherwise podman is driven directly with `podman run` — single broker only,
# because the multi-node topology needs a compose provider.
#
# The Makefile wraps this: `make docker-up`, `make docker-up MULTI=1`,
# `make docker-down`, `make docker-ps`, `make integration`.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="$ROOT/docker-compose.kafka.yml"
PROJECT=moonkafka
SINGLE_CONTAINER=moonkafka-kafka
READY_TIMEOUT="${KAFKA_READY_TIMEOUT:-120}"

die() { echo "error: $*" >&2; exit 1; }

# Take the image tag from the compose file so the compose and direct paths
# cannot drift apart.
IMAGE="${KAFKA_IMAGE:-$(sed -n 's/^[[:space:]]*image:[[:space:]]*//p' "$COMPOSE_FILE" | head -n1)}"
[[ -n "$IMAGE" ]] || die "could not read the image tag from $COMPOSE_FILE"

detect_engine() {
  local engine
  if [[ -n "${CONTAINER_ENGINE:-}" ]]; then
    engine="$CONTAINER_ENGINE"
    [[ "$engine" == docker || "$engine" == podman ]] \
      || die "unsupported CONTAINER_ENGINE '$engine' (expected docker or podman)"
    "$engine" info >/dev/null 2>&1 \
      || die "CONTAINER_ENGINE=$engine, but '$engine info' failed (is it installed and running?)"
  elif docker info >/dev/null 2>&1; then
    engine=docker
  elif podman info >/dev/null 2>&1; then
    engine=podman
  else
    die "no usable container engine: 'docker info' and 'podman info' both failed (set CONTAINER_ENGINE to force one)"
  fi
  printf '%s' "$engine"
}

# Print the compose command for the selected engine, or nothing when that
# engine has no compose provider installed.
compose_cmd() {
  case "$ENGINE" in
    docker)
      if docker compose version >/dev/null 2>&1; then
        printf 'docker compose'
      elif command -v docker-compose >/dev/null 2>&1; then
        printf 'docker-compose'
      fi
      ;;
    podman)
      if podman compose version >/dev/null 2>&1; then
        printf 'podman compose'
      elif command -v podman-compose >/dev/null 2>&1; then
        printf 'podman-compose'
      fi
      ;;
  esac
}

image_present() {
  if [[ "$ENGINE" == podman ]]; then
    podman image exists "$IMAGE"
  else
    docker image inspect "$IMAGE" >/dev/null 2>&1
  fi
}

# kafka_ready <container> <in-container bootstrap port>
kafka_ready() {
  local container="$1" port="$2" cli=/opt/kafka/bin/kafka-topics.sh
  # apache/kafka keeps the CLI in /opt/kafka/bin; apache/kafka-native has it on PATH.
  "$ENGINE" exec "$container" test -x "$cli" >/dev/null 2>&1 || cli=kafka-topics.sh
  "$ENGINE" exec "$container" "$cli" --bootstrap-server "localhost:$port" --list >/dev/null 2>&1
}

# wait_ready <container:port> ...
wait_ready() {
  local deadline=$((SECONDS + READY_TIMEOUT)) pair ok
  echo "waiting for Kafka to accept connections (up to ${READY_TIMEOUT}s)..."
  while (( SECONDS < deadline )); do
    ok=1
    for pair in "$@"; do
      kafka_ready "${pair%%:*}" "${pair##*:}" || ok=0
    done
    (( ok )) && { echo "cluster ready"; return 0; }
    sleep 2
  done
  die "cluster did not become ready within ${READY_TIMEOUT}s"
}

podman_run_single() {
  # Like `compose up -d`: an already-running broker is left alone.
  if [[ "$("$ENGINE" inspect --format '{{.State.Running}}' "$SINGLE_CONTAINER" 2>/dev/null || true)" == true ]]; then
    echo "$SINGLE_CONTAINER is already running"
    wait_ready "$SINGLE_CONTAINER:9092"
    return 0
  fi
  image_present || { echo "pulling $IMAGE..."; "$ENGINE" pull "$IMAGE"; }
  "$ENGINE" rm -f "$SINGLE_CONTAINER" >/dev/null 2>&1 || true
  echo "starting Kafka via '$ENGINE run'..."
  # Mirrors the single-broker `kafka:` service in docker-compose.kafka.yml.
  "$ENGINE" run -d --name "$SINGLE_CONTAINER" -p 9092:9092 \
    -e KAFKA_NODE_ID=1 \
    -e KAFKA_PROCESS_ROLES=broker,controller \
    -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
    -e KAFKA_LISTENERS=CONTROLLER://:9093,PLAINTEXT://:9092 \
    -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
    -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
    -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
    -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
    -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
    -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
    -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
    -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
    "$IMAGE" >/dev/null
  wait_ready "$SINGLE_CONTAINER:9092"
}

cmd_up() {
  local multi=0
  [[ "${1:-}" == "--multi" ]] && multi=1
  local compose
  compose="$(compose_cmd)"

  if [[ -n "$compose" ]]; then
    local args=(-f "$COMPOSE_FILE" --project-name "$PROJECT")
    (( multi )) && args+=(--profile multi)
    echo "starting Kafka via '$compose'..."
    # shellcheck disable=SC2086  # $compose may be two words ("docker compose")
    $compose "${args[@]}" up -d

    local services=(kafka) ports=(9092)
    if (( multi )); then
      services=(kafka1 kafka2 kafka3)
      ports=(29092 29093 29094)
    fi
    local pairs=() i id
    for i in "${!services[@]}"; do
      # shellcheck disable=SC2086
      id="$($compose "${args[@]}" ps -q "${services[$i]}")"
      [[ -n "$id" ]] || die "service '${services[$i]}' did not start"
      pairs+=("$id:${ports[$i]}")
    done
    wait_ready "${pairs[@]}"
  else
    [[ "$ENGINE" == podman ]] || die "no compose provider for $ENGINE; install 'docker compose'"
    (( multi )) && die "MULTI=1 needs a compose provider (install podman-compose, or use docker); the direct podman path starts the single broker only"
    podman_run_single
  fi
}

cmd_down() {
  local keep_volumes=0
  [[ "${1:-}" == "--keep-volumes" ]] && keep_volumes=1
  local compose
  compose="$(compose_cmd)"

  if [[ -n "$compose" ]]; then
    local args=(-f "$COMPOSE_FILE" --project-name "$PROJECT" down)
    (( keep_volumes )) || args+=(-v)
    # shellcheck disable=SC2086
    $compose "${args[@]}"
  else
    "$ENGINE" rm -f "$SINGLE_CONTAINER" >/dev/null 2>&1 || true
    echo "removed $SINGLE_CONTAINER"
  fi
}

cmd_ps() {
  local compose
  compose="$(compose_cmd)"
  if [[ -n "$compose" ]]; then
    # shellcheck disable=SC2086
    $compose -f "$COMPOSE_FILE" --project-name "$PROJECT" ps
  else
    "$ENGINE" ps -a --filter "name=$SINGLE_CONTAINER"
  fi
}

ENGINE="$(detect_engine)"
case "${1:-}" in
  up)   shift; cmd_up "${1:-}" ;;
  down) shift; cmd_down "${1:-}" ;;
  ps)   cmd_ps ;;
  *)    die "usage: $(basename "$0") {up [--multi] | down [--keep-volumes] | ps}" ;;
esac
