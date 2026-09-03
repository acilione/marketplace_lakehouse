SHELL := /bin/bash
PYTHON ?= python3
VENV ?= .venv
BIN := $(VENV)/bin
ARCHITECTURE_NODE_IMAGE := node:24.20.0-alpine@sha256:e67514e5d0f6c46656005e1b693b2ec9d52e80b641307de684d4a015ba7a4eaf
ARCHITECTURE_NPM := docker run --rm --user "$$(id -u):$$(id -g)" -e npm_config_cache=/tmp/npm-cache -v "$(CURDIR)/architecture-site:/app" -w /app $(ARCHITECTURE_NODE_IMAGE) npm

.PHONY: bootstrap install format lint type python-test test security check compose-validate up down demo clean architecture-install architecture-test architecture-check architecture-up

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
	$(ARCHITECTURE_NPM) ci --ignore-scripts

architecture-test: architecture-install
	$(ARCHITECTURE_NPM) run test

test: python-test architecture-test

architecture-check: architecture-install
	$(ARCHITECTURE_NPM) run check

security:
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

architecture-up:
	docker compose --profile architecture up -d --build --wait architecture-site

clean:
	rm -rf .pytest_cache .mypy_cache .ruff_cache htmlcov coverage.xml build dist
