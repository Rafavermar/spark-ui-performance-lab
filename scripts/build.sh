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

docker compose up -d --no-recreate spark-client
docker compose exec -T spark-client sbt \
  -Dspark.version="${SPARK_VERSION:-4.1.1}" \
  -Dscala.version="${SCALA_VERSION:-2.13.17}" \
  clean assembly
