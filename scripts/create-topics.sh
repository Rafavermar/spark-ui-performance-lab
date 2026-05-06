#!/usr/bin/env bash
set -euo pipefail
export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL="*"

cd "$(dirname "${BASH_SOURCE[0]}")/.."

if ! docker compose --profile streaming exec -T redpanda rpk cluster info -X brokers=redpanda:9092 >/dev/null 2>&1; then
  echo "Redpanda is not running. Start it with ./scripts/up-streaming.sh."
  exit 1
fi

for topic in spark-ui-lab-input spark-ui-lab-output spark-ui-lab-stateful-input; do
  docker compose --profile streaming exec -T redpanda rpk topic create "$topic" \
    --partitions 4 \
    --replicas 1 \
    -X brokers=redpanda:9092 >/dev/null 2>&1 || true
  echo "Topic ready: $topic"
done
