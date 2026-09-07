SHELL := /bin/bash
.SHELLFLAGS := -eu -o pipefail -c
PYTHON ?= python3
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

check: lint type python-test compose-validate architecture-check

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
