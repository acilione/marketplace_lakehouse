#!/usr/bin/env bash
# Exercise source-built MinIO and mc using a disposable, isolated Compose project.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

readonly TEST_PROJECT="marketplace-object-store-test-$(date -u +%s)-$$"
export MLH_MINIO_API_PORT=0 MLH_MINIO_CONSOLE_PORT=0
readonly COMPOSE=(docker compose --project-name "$TEST_PROJECT")

cleanup() {
  "${COMPOSE[@]}" down --volumes --remove-orphans
}
trap cleanup EXIT

# Wait only for the long-lived server; the initialization job is expected to exit.
"${COMPOSE[@]}" up -d --wait minio
"${COMPOSE[@]}" run --rm minio-init
"${COMPOSE[@]}" run --rm --no-deps --entrypoint /bin/sh minio-init -ec '
  mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null
  mc stat local/warehouse
  mc stat local/checkpoints
  mc version info local/warehouse
  printf "%s\n" lakehouse-ci-probe | mc pipe local/warehouse/ci-probe.txt >/dev/null
  test "$(mc cat local/warehouse/ci-probe.txt)" = lakehouse-ci-probe
  mc rm local/warehouse/ci-probe.txt
'
printf '%s\n' 'Object-store initialization, versioning, and S3 write/read checks passed.'
