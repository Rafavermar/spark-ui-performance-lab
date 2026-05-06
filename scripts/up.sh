#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

if [ -f .env ]; then
  set -a
  # shellcheck disable=SC1091
  . ./.env
  set +a
fi

docker compose up -d --build \
  spark-master \
  spark-worker-1 \
  spark-worker-2 \
  spark-history-server \
  spark-client

cat <<'MSG'
Default Spark lab is starting.
Spark Master UI:      http://localhost:8080
Spark History Server: http://localhost:18080
Live Spark App UI:    http://localhost:4040 while a case is running
MSG
