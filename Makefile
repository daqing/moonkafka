# moonkafka development / release toolbelt. See docs/5-testing.md for the
# full harness guide. Run `make help` to list targets.

.PHONY: help build test test-unit test-integration fmt fmt-check info info-check \
        generate-golden docker-up docker-down docker-ps bench coverage clean

DOCKER_COMPOSE ?= docker compose -f docker-compose.kafka.yml --project-name moonkafka
MOON          ?= moon

## help             : print available targets
help:
	@echo "moonkafka targets:"
	@echo "  make build           build the library"
	@echo "  make test            run the full unit suite (incl. mock-broker testkit)"
	@echo "  make fmt             format code (moon fmt)"
	@echo "  make info            regenerate package interfaces (.mbti)"
	@echo "  make fmt-check       fail if code is not formatted"
	@echo "  make info-check      fail on unexpected .mbti diffs"
	@echo "  make generate-golden regenerate golden compression fixtures"
	@echo "  make docker-up [MULTI=1] bring up a KRaft Kafka cluster (Docker)"
	@echo "  make docker-down     tear the cluster down"
	@echo "  make integration     docker cluster + real-client smoker via cmd/main"
	@echo "  make bench           run the throughput benchmark (needs a broker)"
	@echo "  make coverage        write per-line coverage to uncovered.log"

## build            : type-check the whole library (no link; the module root
##                    is a library, so `moon build` would try to link a main)
build:
	$(MOON) check

## test             : run unit + mock-broker (fake broker) tests
test:
	$(MOON) test

## test-unit        : pure codec/client tests only (quick, no broker)
test-unit:
	$(MOON) test -p buf -p compression

## fmt              : format all source
fmt:
	$(MOON) fmt

## fmt-check        : fail if formatting is not clean
fmt-check:
	$(MOON) fmt --check 2>&1 || { echo "run 'moon fmt'"; exit 1; }

## info             : regenerate .mbti interfaces
info:
	$(MOON) info

## info-check       : fail on unexpected .mbti changes
info-check:
	$(MOON) info
	@git diff --exit-code -- '*.pkg.generated.mbti' \
	  || { echo ".mbti changed unexpectedly; run 'moon info' and review the diff"; exit 1; }

## generate-golden  : recompute test/golden/*.bin + golden_test_data_test.mbt
##                    (deterministic; fmt twice: moon fmt is two-pass on the
##                    emitted doc-comment block)
generate-golden:
	python3 test/golden/generate.py
	$(MOON) fmt
	$(MOON) fmt

## docker-up        : start KRaft Kafka (single by default, MULTI=1 for 3 nodes)
docker-up:
ifneq ($(MULTI),)
	$(DOCKER_COMPOSE) --profile multi up -d
else
	$(DOCKER_COMPOSE) up -d
endif
	@echo "waiting for brokers..."; \
	timeout 120 sh -c 'until docker compose -f docker-compose.kafka.yml ps --status running | grep -q Healthy; do sleep 2; done' && echo "cluster healthy"

## docker-down      : stop and remove the cluster (use KEEP_VOLUMES=1 to retain data)
docker-down:
	$(DOCKER_COMPOSE) down $(if $(KEEP_VOLUMES),, -v)

## docker-ps        : show cluster status
docker-ps:
	$(DOCKER_COMPOSE) ps

## integration      : real-client smoke test against the docker cluster
integration: docker-up
	@KAFKA_BOOTSTRAP=localhost:9092 ./test/integration.sh

## bench            : produce/consume throughput benchmark (needs a running broker)
bench:
	@test -n "$$KAFKA_BOOTSTRAP" || KAFKA_BOOTSTRAP=localhost:9092; \
	./bench/bench.sh "$$KAFKA_BOOTSTRAP"

## coverage         : emit coverage analysis to uncovered.log
coverage:
	$(MOON) test --enable-coverage 2>&1 | tail -5
	$(MOON) coverage analyze > uncovered.log 2>&1 || true
	@echo "written uncovered.log ($(shell wc -l < uncovered.log) lines)"