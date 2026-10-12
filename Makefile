SHELL := /bin/bash
.SHELLFLAGS := -eu -o pipefail -c
PYTHON ?= python3
.DEFAULT_GOAL := bootstrap
VENV ?= .venv
BIN := $(VENV)/bin
ARCHITECTURE_NODE_IMAGE := node:24.20.0-alpine@sha256:e67514e5d0f6c46656005e1b693b2ec9d52e80b641307de684d4a015ba7a4eaf
ARCHITECTURE_NPM := docker run --rm --user "$$(id -u):$$(id -g)" -e npm_config_cache=/tmp/npm-cache -v "$(CURDIR)/architecture-site:/app" -w /app $(ARCHITECTURE_NODE_IMAGE) npm
SIMULATION_PROFILE ?= smoke
SIMULATION_SEED ?= $(shell date -u +%s)
SIMULATION_ARGS ?=
STRESS_PROFILE ?= steady
STRESS_SEED ?= $(shell date -u +%s)
STRESS_ARGS ?=
SHOWCASE_CONFIG ?= config/showcase.json
SHOWCASE_ARGS ?=
ANALYTICS_ORDERS ?= 1000
ANALYTICS_CUSTOMERS ?= 100
ANALYTICS_SEED ?= 42
ANALYTICS_PROJECT ?= marketplace-lakehouse-showcase
ANALYTICS_COMPOSE := docker compose -p $(ANALYTICS_PROJECT) -f docker-compose.yml -f docker-compose.showcase.yml
export ANALYTICS_PROJECT

.PHONY: analytics-demo analytics-refresh analytics-up analytics-load analytics-stop analytics-retail-publish
analytics-stop:
	$(ANALYTICS_COMPOSE) --profile '*' stop

analytics-up:
	$(ANALYTICS_COMPOSE) up -d --build --wait postgres minio iceberg-rest kafka spark-master spark-worker
	$(ANALYTICS_COMPOSE) run --rm kafka-init
	DOCKER_GID="$$(stat -c '%g' /var/run/docker.sock)" $(ANALYTICS_COMPOSE) up -d --build --wait trino service-controller control-api architecture-site

ANALYTICS_SKIP ?= 0
analytics-load:
	$(ANALYTICS_COMPOSE) run --rm --no-deps spark-master python3 -m marketplace_data.purchase_source --generate $(ANALYTICS_ORDERS) --customers $(ANALYTICS_CUSTOMERS) --seed $(ANALYTICS_SEED) --skip $(ANALYTICS_SKIP) --as-of 2026-10-12T12:00:00+00:00

analytics-demo: analytics-up analytics-load
	$(MAKE) analytics-refresh ANALYTICS_ARGS="--as-of 2026-10-12 --public-demo"

# Without --public-demo, new publications require an authenticated dashboard user.
ANALYTICS_ARGS ?=
analytics-refresh:
	$(ANALYTICS_COMPOSE) run --rm --no-deps spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/marketplace/apps/analytics/ingest.py --available-now
	$(ANALYTICS_COMPOSE) run --rm --no-deps spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/marketplace/apps/analytics/publish.py $(ANALYTICS_ARGS)

.PHONY: analytics-verify
analytics-verify:
	$(ANALYTICS_COMPOSE) run --rm --no-deps -v "$(CURDIR)/scripts:/verification:ro" spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /verification/verify-analytics.py

analytics-reference-check:
	$(ANALYTICS_COMPOSE) run --rm --no-deps -v "$(CURDIR)/scripts:/verification:ro" spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /verification/verify-analytics.py --reference-only

.PHONY: analytics-import-retail analytics-real analytics-retention-plan analytics-retention-apply analytics-benchmark
analytics-import-retail:
	$(BIN)/python -m marketplace_data.retail_import --download

# Separate volumes, ports and historical observation date: never mix with generated data.
analytics-real:
	ANALYTICS_WEB_PORT=24273 ANALYTICS_TRINO_PORT=24286 $(MAKE) analytics-up ANALYTICS_PROJECT=marketplace-lakehouse-retail
	$(MAKE) analytics-retail-publish ANALYTICS_PROJECT=marketplace-lakehouse-retail

analytics-retail-publish:
	$(ANALYTICS_COMPOSE) run --rm --no-deps -v "$(CURDIR)/data/retail:/import:ro" spark-master python3 -m marketplace_data.purchase_source --input /import/purchases.jsonl --rate 2000
	$(MAKE) analytics-refresh ANALYTICS_ARGS="--as-of 2011-12-09 --public-dataset"

analytics-retention-plan:
	$(ANALYTICS_COMPOSE) run --rm --no-deps -v "$(CURDIR)/scripts:/verification:ro" spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /verification/analytics-retention.py

analytics-retention-apply:
	$(ANALYTICS_COMPOSE) run --rm --no-deps -v "$(CURDIR)/scripts:/verification:ro" spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /verification/analytics-retention.py --apply

analytics-compaction-plan:
	$(ANALYTICS_COMPOSE) run --rm --no-deps -v "$(CURDIR)/scripts:/verification:ro" spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /verification/analytics-retention.py --compact

analytics-compact:
	$(ANALYTICS_COMPOSE) run --rm --no-deps -v "$(CURDIR)/scripts:/verification:ro" spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /verification/analytics-retention.py --compact --apply

analytics-benchmark:
	$(BIN)/python scripts/benchmark-analytics.py $(BENCHMARK_ARGS)

.PHONY: showcase showcase-advance showcase-verify showcase-stop
showcase:
	$(PYTHON) scripts/showcase.py run --config "$(SHOWCASE_CONFIG)" $(SHOWCASE_ARGS)
showcase-advance:
	$(PYTHON) scripts/showcase.py advance $(SHOWCASE_ARGS)
showcase-verify:
	$(PYTHON) scripts/showcase.py verify
showcase-stop:
	$(PYTHON) scripts/showcase.py stop

.PHONY: bootstrap install format lint type python-test test security check compose-validate up down demo simulate stress clean architecture-install architecture-test architecture-check architecture-audit architecture-up dashboard-up dashboard-down

bootstrap:
	$(PYTHON) -m pip install --user --upgrade virtualenv==21.7.7
	$(PYTHON) -m virtualenv $(VENV)
	$(BIN)/python -m pip install --upgrade pip
	$(BIN)/python -m pip install -e '.[spark,dev]'

install:
	$(BIN)/python -m pip install -e '.[spark,dev]'

format:
	$(BIN)/ruff format .
	$(BIN)/ruff check --fix .

lint:
	$(BIN)/ruff format --check .
	$(BIN)/ruff check .

type:
	$(BIN)/mypy src

python-test:
	$(BIN)/pytest --cov --cov-report=term-missing --cov-report=xml

architecture-install:
	$(ARCHITECTURE_NPM) ci --ignore-scripts --no-audit --no-fund

architecture-test: architecture-install
	$(ARCHITECTURE_NPM) run test

test: python-test architecture-test

architecture-check: architecture-install
	$(ARCHITECTURE_NPM) run check

architecture-audit:
	$(ARCHITECTURE_NPM) audit --audit-level=high

security: architecture-audit
	$(BIN)/bandit -q -r src apps orchestration
	$(BIN)/pip-audit

compose-validate:
	docker compose config --quiet
	docker compose -f docker-compose.yml -f docker-compose.showcase.yml config --quiet

check: lint type python-test compose-validate architecture-check security

up:
	docker compose up -d --build --wait postgres minio iceberg-rest kafka schema-registry spark-master spark-worker
	docker compose run --rm kafka-init

down:
	docker compose down

demo:
	./scripts/demo.sh

simulate: up
	mkdir -p benchmark-results
	docker compose run --rm spark-master python3 -m marketplace_data.simulator \
		--bootstrap-servers kafka:9092 --profile "$(SIMULATION_PROFILE)" \
		--seed "$(SIMULATION_SEED)" --report - $(SIMULATION_ARGS) \
		| tee "benchmark-results/simulation-$(SIMULATION_PROFILE)-$(SIMULATION_SEED).json"

stress:
	test -x "$(BIN)/python" || { echo "Run 'make bootstrap' first." >&2; exit 2; }
	STRESS_PROFILE="$(STRESS_PROFILE)" STRESS_SEED="$(STRESS_SEED)" \
		STRESS_SIMULATOR_ARGS="$(STRESS_ARGS)" ./scripts/stress-test.sh

architecture-up:
	docker compose --profile architecture up -d --build --wait architecture-site

dashboard-up:
	$(MAKE) up
	docker compose --profile query --profile observability create trino prometheus grafana
	DOCKER_GID="$$(stat -c '%g' /var/run/docker.sock)" docker compose --profile architecture --profile dashboard up -d --build --wait control-api architecture-site

dashboard-down:
	docker compose --profile architecture --profile dashboard --profile query --profile observability stop architecture-site control-api service-controller trino prometheus grafana pipeline-metrics

.PHONY: integration-correctness monitoring-test
monitoring-test:
	docker compose exec -T -w /etc/prometheus prometheus promtool test rules alerts.test.yml

integration-correctness:
	docker compose run --rm -v "$(CURDIR)/scripts:/verification:ro" spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /verification/verify-correctness.py

clean:
	rm -rf .pytest_cache .mypy_cache .ruff_cache htmlcov coverage.xml build dist
