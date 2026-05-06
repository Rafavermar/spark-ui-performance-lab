#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

APP_JAR="target/scala-2.13/spark-ui-performance-lab-assembly-0.1.0.jar"
if [ ! -f "$APP_JAR" ]; then
  echo "Build artifact not found: $APP_JAR"
  echo "Run ./scripts/build.sh first."
  exit 1
fi

docker compose up -d --no-recreate spark-client
docker compose exec -T spark-client spark-submit \
  --class lab.Main \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.driver.host=spark-client \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.ui.port=4040 \
  /workspace/"$APP_JAR" generate_data once
