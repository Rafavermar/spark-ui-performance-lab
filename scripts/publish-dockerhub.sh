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

SPARK_VERSION="${SPARK_VERSION:-4.1.1}"
LOCAL_IMAGE="${LOCAL_SPARK_IMAGE:-spark-ui-performance-lab-spark:${SPARK_VERSION}}"
DOCKERHUB_IMAGE="${DOCKERHUB_SPARK_IMAGE:-jrvm/spark-ui-performance-lab-spark:${SPARK_VERSION}}"
DOCKERHUB_LATEST="${DOCKERHUB_IMAGE%:*}:latest"

echo "Building local image: $LOCAL_IMAGE"
SPARK_IMAGE="$LOCAL_IMAGE" docker compose build spark-client

echo "Tagging Docker Hub images:"
echo "  $DOCKERHUB_IMAGE"
echo "  $DOCKERHUB_LATEST"
docker tag "$LOCAL_IMAGE" "$DOCKERHUB_IMAGE"
docker tag "$LOCAL_IMAGE" "$DOCKERHUB_LATEST"

echo "Pushing Docker Hub images. Run 'docker login' first if needed."
docker push "$DOCKERHUB_IMAGE"
docker push "$DOCKERHUB_LATEST"
