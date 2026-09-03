#!/usr/bin/env bash
set -euo pipefail

readonly CONFIG=/opt/marketplace/config/local.yaml
readonly DEMO_RUN_ID="${MLH_DEMO_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)-$$}"
readonly DEMO_CHECKPOINT_ROOT="s3a://checkpoints/demo-runs/${DEMO_RUN_ID}"
readonly SUBMIT=(
  docker compose run --rm
  -e "MLH_PROCESSING__CHECKPOINT_ROOT=${DEMO_CHECKPOINT_ROOT}"
  spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077
)

printf 'Demo run: %s\nReplay checkpoint: %s\n' "$DEMO_RUN_ID" "$DEMO_CHECKPOINT_ROOT"

docker compose up -d --build --wait postgres minio iceberg-rest kafka schema-registry spark-master spark-worker
docker compose run --rm kafka-init

"${SUBMIT[@]}" /opt/marketplace/apps/bootstrap_catalog/main.py --config "$CONFIG"

docker compose run --rm spark-master python3 -m marketplace_data.generator \
  --bootstrap-servers kafka:9092 --count 2000 --duplicate-rate 0.01 \
  --malformed-rate 0.005 --late-rate 0.02 --events-per-second 1000

"${SUBMIT[@]}" /opt/marketplace/apps/bronze_ingest/main.py --config "$CONFIG" --available-now
"${SUBMIT[@]}" /opt/marketplace/apps/silver_conform/orders.py --config "$CONFIG"
"${SUBMIT[@]}" /opt/marketplace/apps/gold_marts/marketplace_kpis.py \
  --config "$CONFIG" --candidate-branch demo_candidate --publish

"${SUBMIT[@]}" /opt/marketplace/apps/demo_results/main.py --config "$CONFIG"
