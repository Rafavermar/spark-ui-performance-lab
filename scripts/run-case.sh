#!/usr/bin/env bash
set -euo pipefail
export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL="*"

cd "$(dirname "${BASH_SOURCE[0]}")/.."

if [ $# -ne 2 ]; then
  echo "Usage: ./scripts/run-case.sh <case_id> <mode>"
  echo "Example: ./scripts/run-case.sh 01_too_many_actions baseline"
  exit 1
fi

CASE_ID="$1"
MODE="$2"
APP_JAR="target/scala-2.13/spark-ui-performance-lab-assembly-0.1.0.jar"

case "$MODE" in
  baseline|optimized|advanced)
    ;;
  optmized|optimised)
    echo "Unsupported mode: $MODE"
    echo "Did you mean: optimized?"
    echo "Usage: ./scripts/run-case.sh <case_id> baseline|optimized"
    echo "Case 17 also accepts: advanced"
    exit 1
    ;;
  *)
    echo "Unsupported mode: $MODE"
    echo "Usage: ./scripts/run-case.sh <case_id> baseline|optimized"
    echo "Case 17 also accepts: advanced"
    exit 1
    ;;
esac

if [ "$MODE" = "advanced" ] && [ "$CASE_ID" != "17_real_time_mode" ]; then
  echo "Mode 'advanced' is only valid for 17_real_time_mode."
  echo "Use baseline or optimized for $CASE_ID."
  exit 1
fi

if [ ! -f "$APP_JAR" ]; then
  echo "Build artifact not found: $APP_JAR"
  echo "Run ./scripts/build.sh first."
  exit 1
fi

COMPOSE_ARGS=()
if [[ "$CASE_ID" =~ ^1[5-7]_ ]]; then
  COMPOSE_ARGS=(--profile streaming)
  if ! docker compose --profile streaming exec -T redpanda rpk cluster info -X brokers=redpanda:9092 >/dev/null 2>&1; then
    echo "Redpanda is not running. Start it with ./scripts/up-streaming.sh and create topics with ./scripts/create-topics.sh."
    exit 1
  fi
fi

EXEC_ARGS=()
if [ ! -t 0 ]; then
  EXEC_ARGS=(-T)
fi

ENV_ARGS=()
if [ "${LAB_AUTO_EXIT:-}" = "true" ] || [ ! -t 0 ]; then
  ENV_ARGS=(-e LAB_AUTO_EXIT=true)
fi

EXTRA_CONF=()
if [ "$CASE_ID" = "14_config_validation" ] && [ "$MODE" = "optimized" ]; then
  EXTRA_CONF+=(
    --conf spark.sql.shuffle.partitions=6
    --conf spark.sql.adaptive.enabled=true
    --conf spark.ui.retainedJobs=300
  )
fi

docker compose "${COMPOSE_ARGS[@]}" up -d --no-recreate spark-client >/dev/null
docker compose "${COMPOSE_ARGS[@]}" exec "${EXEC_ARGS[@]}" "${ENV_ARGS[@]}" spark-client spark-submit \
  --class lab.Main \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.driver.host=spark-client \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.ui.port=4040 \
  --conf spark.sql.streaming.metricsEnabled=true \
  "${EXTRA_CONF[@]}" \
  /workspace/"$APP_JAR" "$CASE_ID" "$MODE"
