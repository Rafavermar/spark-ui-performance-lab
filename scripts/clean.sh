#!/usr/bin/env bash
set -euo pipefail
export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL="*"

cd "$(dirname "${BASH_SOURCE[0]}")/.."

rm -rf data/generated
find metrics -mindepth 1 ! -name .gitkeep -delete
find tmp -mindepth 1 ! -name .gitkeep -delete

if docker compose ps --status running spark-client >/dev/null 2>&1; then
  docker compose exec -T spark-client bash -lc 'rm -rf /opt/spark-checkpoints/* /opt/spark-warehouse/*'
fi

echo "Cleaned generated data, metrics, tmp files, checkpoints and warehouse data."
