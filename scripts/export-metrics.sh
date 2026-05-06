#!/usr/bin/env bash
set -euo pipefail
export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL="*"

cd "$(dirname "${BASH_SOURCE[0]}")/.."

if [ $# -ne 2 ]; then
  echo "Usage: ./scripts/export-metrics.sh <case_id> <mode>"
  exit 1
fi

CASE_ID="$1"
MODE="$2"
OUT="/workspace/metrics/${CASE_ID}_${MODE}_history_applications.json"

docker compose up -d --no-recreate spark-history-server spark-client >/dev/null
docker compose exec -T spark-client bash -lc "mkdir -p /workspace/metrics && curl -fsS 'http://spark-history-server:18080/api/v1/applications?limit=200' > '$OUT'"

echo "Exported Spark History Server application list to metrics/${CASE_ID}_${MODE}_history_applications.json"
echo "This minimal exporter captures the REST index. Use the app id inside the JSON for deeper manual REST calls."
