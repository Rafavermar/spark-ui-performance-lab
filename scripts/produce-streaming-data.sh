#!/usr/bin/env bash
set -euo pipefail
export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL="*"

cd "$(dirname "${BASH_SOURCE[0]}")/.."

if ! docker compose --profile streaming exec -T redpanda rpk cluster info -X brokers=redpanda:9092 >/dev/null 2>&1; then
  echo "Redpanda is not running. Start it with ./scripts/up-streaming.sh."
  exit 1
fi

ts="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

for i in $(seq 1 800); do
  key="k$((i % 20))"
  printf '{"id":%d,"key":"%s","event_time":"%s","value":%d}\n' "$i" "$key" "$ts" "$((i % 100))"
done | docker compose --profile streaming exec -T redpanda rpk topic produce spark-ui-lab-input -X brokers=redpanda:9092 >/dev/null

for i in $(seq 1 800); do
  key="state$((i % 80))"
  printf '{"id":%d,"key":"%s","event_time":"%s","value":%d}\n' "$i" "$key" "$ts" "$((i % 50))"
done | docker compose --profile streaming exec -T redpanda rpk topic produce spark-ui-lab-stateful-input -X brokers=redpanda:9092 >/dev/null

echo "Produced deterministic sample records to Redpanda topics."
