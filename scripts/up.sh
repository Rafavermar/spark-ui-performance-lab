#!/usr/bin/env bash
set -euo pipefail
export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL="*"

cd "$(dirname "${BASH_SOURCE[0]}")/.."

if [ -f .env ]; then
  set -a
  # shellcheck disable=SC1091
  . ./.env
  set +a
fi

BUILD_ARGS=(--build)
if [ "${SPARK_USE_PREBUILT_IMAGE:-false}" = "true" ]; then
  docker compose pull spark-master spark-worker-1 spark-worker-2 spark-history-server spark-client
  BUILD_ARGS=(--no-build)
fi

docker compose up -d "${BUILD_ARGS[@]}" \
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
