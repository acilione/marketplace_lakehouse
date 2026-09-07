#!/usr/bin/env bash
set -euo pipefail

readonly PROFILE="${STRESS_PROFILE:-steady}"
readonly SEED="${STRESS_SEED:-$(date -u +%s)}"
readonly EXTRA_ARGS_RAW="${STRESS_SIMULATOR_ARGS:-}"
readonly RUN_ID="stress_${PROFILE}_${SEED}_$(date -u +%Y%m%dT%H%M%SZ)"
readonly CHECKPOINT_ROOT="s3a://checkpoints/stress-runs/${RUN_ID}"
readonly RESULT_ROOT="benchmark-results/${RUN_ID}"
readonly CONFIG=/opt/marketplace/config/local.yaml
readonly PRODUCER_REPORT="${RESULT_ROOT}/producer.json"
readonly BRONZE_REPORT="${RESULT_ROOT}/bronze-by-topic.jsonl"
readonly INGESTION_REPORT="${RESULT_ROOT}/ingestion.jsonl"
readonly DATASET_REPORT="${RESULT_ROOT}/datasets.jsonl"
readonly FINAL_REPORT="${RESULT_ROOT}/report.json"
readonly INGEST_CONTAINER="${RUN_ID}_ingest"
readonly SUBMIT=(
  docker compose run --rm
  spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077
)

case "$PROFILE" in
  smoke | steady | peak | chaos) ;;
  *) printf 'Unknown stress profile: %s\n' "$PROFILE" >&2; exit 2 ;;
esac
if [[ ! "$SEED" =~ ^[0-9]+$ ]]; then
  printf 'STRESS_SEED must be a non-negative integer.\n' >&2
  exit 2
fi

simulator_args=()
if [[ -n "$EXTRA_ARGS_RAW" ]]; then
  read -r -a simulator_args <<<"$EXTRA_ARGS_RAW"
fi

mkdir -p "$RESULT_ROOT"

run_phase() {
  local result_variable="$1"
  shift
  local started=$SECONDS
  "$@"
  printf -v "$result_variable" '%s' "$((SECONDS - started))"
}

printf 'Stress run: %s\nProfile: %s\nSeed: %s\nEvidence: %s\n' \
  "$RUN_ID" "$PROFILE" "$SEED" "$RESULT_ROOT"

run_phase core_seconds make dashboard-up
run_phase observability_seconds \
  docker compose --profile query --profile observability up -d --wait trino prometheus grafana
run_phase bootstrap_seconds \
  "${SUBMIT[@]}" /opt/marketplace/apps/bootstrap_catalog/main.py --config "$CONFIG"

docker compose run --rm spark-master python3 -m marketplace_data.kafka_offsets kafka:9092 >"${RESULT_ROOT}/starting-offsets.json"
starting_offsets=$(<"${RESULT_ROOT}/starting-offsets.json")
cleanup() {
  if [[ -n "${ingest_pid:-}" ]] && kill -0 "$ingest_pid" 2>/dev/null; then
    docker stop "$INGEST_CONTAINER" >/dev/null 2>&1 || true
    wait "$ingest_pid" || true
  fi
}
trap cleanup EXIT
bronze_started=$SECONDS
docker compose run --rm --name "$INGEST_CONTAINER" \
  -e "MLH_KAFKA__STARTING_OFFSETS=${starting_offsets}" \
  -e "MLH_PROCESSING__CHECKPOINT_ROOT=${CHECKPOINT_ROOT}" \
  -e MLH_PROCESSING__TRIGGER_INTERVAL="2 seconds" \
  spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 \
  /opt/marketplace/apps/bronze_ingest/main.py --config "$CONFIG" --run-id "$RUN_ID" \
  --completion-file /tmp/producer.done >"${RESULT_ROOT}/bronze.log" 2>&1 &
ingest_pid=$!
ready_deadline=$((SECONDS + 180))
until docker exec "$INGEST_CONTAINER" test -f /tmp/producer.ready 2>/dev/null; do
  if ! kill -0 "$ingest_pid" 2>/dev/null; then
    wait "$ingest_pid"
    printf 'Ingestion exited before readiness. Inspect %s/bronze.log\n' "$RESULT_ROOT" >&2
    exit 1
  fi
  if (( SECONDS > ready_deadline )); then
    printf 'Ingestion readiness timed out. Inspect %s/bronze.log\n' "$RESULT_ROOT" >&2
    exit 1
  fi
  sleep 1
done

producer_started=$SECONDS
docker compose run --rm spark-master python3 -m marketplace_data.simulator \
  --bootstrap-servers kafka:9092 \
  "${simulator_args[@]}" \
  --profile "$PROFILE" --seed "$SEED" --simulation-id "$RUN_ID" --report - | tee "$PRODUCER_REPORT"
producer_seconds=$((SECONDS - producer_started))
docker exec "$INGEST_CONTAINER" touch /tmp/producer.done
wait "$ingest_pid"
ingest_pid=
bronze_seconds=$((SECONDS - bronze_started))

run_phase orders_seconds \
  "${SUBMIT[@]}" /opt/marketplace/apps/silver_conform/orders.py \
  --config "$CONFIG" --run-id "${RUN_ID}_orders"
run_phase domains_seconds \
  "${SUBMIT[@]}" /opt/marketplace/apps/silver_conform/domains.py \
  --config "$CONFIG" --run-id "${RUN_ID}_domains"
run_phase customers_seconds \
  "${SUBMIT[@]}" /opt/marketplace/apps/silver_conform/customers.py \
  --config "$CONFIG" --run-id "${RUN_ID}_customers"
run_phase gold_seconds \
  "${SUBMIT[@]}" /opt/marketplace/apps/gold_marts/marketplace_kpis.py \
  --config "$CONFIG" \
  --run-id "${RUN_ID}_gold" \
  --candidate-branch "stress_${PROFILE}_${SEED}" \
  --publish

docker compose exec -T trino trino --catalog lakehouse --output-format JSON --execute "
  SELECT source_topic, count(*) AS events
  FROM bronze.marketplace_events
  WHERE ingestion_run_id = '${RUN_ID}' AND element_at(payload, 'simulation_id') = '${RUN_ID}'
  GROUP BY source_topic
  ORDER BY source_topic
" >"$BRONZE_REPORT"

docker compose exec -T trino trino --catalog lakehouse --output-format JSON --execute "
  SELECT
    (SELECT count(*) FROM bronze.marketplace_events WHERE ingestion_run_id = '${RUN_ID}' AND element_at(payload, 'simulation_id') = '${RUN_ID}')
      AS accepted_events,
    (SELECT count(*) FROM quarantine.marketplace_events WHERE ingestion_run_id = '${RUN_ID}' AND raw_value LIKE '%${RUN_ID}%')
      AS quarantined_events,
    (SELECT CAST(approx_percentile(CAST(date_diff('millisecond', source_timestamp, ingested_at)
      AS DOUBLE), 0.50) AS BIGINT) FROM bronze.marketplace_events
      WHERE ingestion_run_id = '${RUN_ID}' AND element_at(payload, 'simulation_id') = '${RUN_ID}') AS latency_ms_p50,
    (SELECT CAST(approx_percentile(CAST(date_diff('millisecond', source_timestamp, ingested_at)
      AS DOUBLE), 0.95) AS BIGINT) FROM bronze.marketplace_events
      WHERE ingestion_run_id = '${RUN_ID}' AND element_at(payload, 'simulation_id') = '${RUN_ID}') AS latency_ms_p95
" >"$INGESTION_REPORT"

docker compose exec -T trino trino --catalog lakehouse --output-format JSON --execute "
  WITH run_events AS (
    SELECT * FROM bronze.marketplace_events
    WHERE element_at(payload, 'simulation_id') = '${RUN_ID}' AND cardinality(quality_flags) = 0
  ), order_keys AS (SELECT DISTINCT partition_key AS id FROM run_events WHERE event_type LIKE 'order.%'),
  payment_keys AS (SELECT DISTINCT partition_key AS id FROM run_events WHERE event_type LIKE 'payment.%'),
  shipment_keys AS (SELECT DISTINCT partition_key AS id FROM run_events WHERE event_type LIKE 'shipment.%'),
  customer_keys AS (SELECT DISTINCT element_at(payload, 'customer_id') AS id FROM run_events WHERE event_type = 'customer.changed'),
  inventory_keys AS (SELECT DISTINCT element_at(payload, 'sku') AS sku, element_at(payload, 'location_id') AS location_id, CAST(occurred_at AS DATE) AS date FROM run_events WHERE event_type LIKE 'inventory.%'),
  expected_gold AS (
    SELECT CAST(created_at AS DATE) AS metric_date, market, 'UNCLASSIFIED' AS seller_tier,
      count(*) AS order_count, sum(gross_amount) AS gmv,
      sum(CASE WHEN status NOT IN ('CANCELLED', 'REFUNDED') THEN gross_amount ELSE DECIMAL '0' END) AS net_revenue
    FROM silver.orders GROUP BY CAST(created_at AS DATE), market
  )
  SELECT 'silver.orders' AS dataset, count(*) AS row_count, (SELECT count(*) FROM order_keys) AS expected_rows FROM silver.orders JOIN order_keys ON order_id=id
  UNION ALL SELECT 'silver.payments', count(*), (SELECT count(*) FROM payment_keys) FROM silver.payments JOIN payment_keys ON payment_id=id
  UNION ALL SELECT 'silver.shipments', count(*), (SELECT count(*) FROM shipment_keys) FROM silver.shipments JOIN shipment_keys ON shipment_id=id
  UNION ALL SELECT 'silver.inventory_daily', count(*), (SELECT count(*) FROM inventory_keys) FROM silver.inventory_daily JOIN inventory_keys USING (sku, location_id, date)
  UNION ALL SELECT 'silver.customers_scd2', count(*), (SELECT count(*) FROM customer_keys) FROM silver.customers_scd2 JOIN customer_keys ON customer_id=id WHERE is_current
  UNION ALL SELECT 'gold.daily_marketplace_kpis', CASE WHEN count(*) = (SELECT count(*) FROM gold.daily_marketplace_kpis) THEN count(*) ELSE -1 END, (SELECT count(*) FROM expected_gold)
    FROM gold.daily_marketplace_kpis g JOIN expected_gold e
    ON g.metric_date IS NOT DISTINCT FROM e.metric_date AND g.market IS NOT DISTINCT FROM e.market
    AND g.seller_tier = e.seller_tier AND g.order_count = e.order_count AND g.gmv = e.gmv AND g.net_revenue = e.net_revenue
  ORDER BY dataset
" >"$DATASET_REPORT"

.venv/bin/python -m marketplace_data.stress_report \
  --run-id "$RUN_ID" \
  --profile "$PROFILE" \
  --producer-report "$PRODUCER_REPORT" \
  --bronze-report "$BRONZE_REPORT" \
  --ingestion-report "$INGESTION_REPORT" \
  --dataset-report "$DATASET_REPORT" \
  --phase "core_start=${core_seconds}" \
  --phase "observability_start=${observability_seconds}" \
  --phase "catalog_bootstrap=${bootstrap_seconds}" \
  --producer-seconds "$producer_seconds" \
  --phase "bronze=${bronze_seconds}" \
  --phase "silver_orders=${orders_seconds}" \
  --phase "silver_domains=${domains_seconds}" \
  --phase "silver_customers=${customers_seconds}" \
  --phase "gold=${gold_seconds}" \
  --output "$FINAL_REPORT"

printf '\nStress test passed. Evidence: %s\n' "$FINAL_REPORT"
