#!/usr/bin/env bash
set -euo pipefail
export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL="*"

cd "$(dirname "${BASH_SOURCE[0]}")/.."

ACTION="${1:-}"
TOPIC="${2:-}"
NUM="${3:-5}"
BROKERS="${REDPANDA_BROKERS:-redpanda:9092}"

usage() {
  cat <<'MSG'
Usage:
  ./scripts/inspect-streaming.sh topics
  ./scripts/inspect-streaming.sh describe <topic>
  ./scripts/inspect-streaming.sh consume <topic> [num]
  ./scripts/inspect-streaming.sh groups

Examples:
  ./scripts/inspect-streaming.sh topics
  ./scripts/inspect-streaming.sh describe spark-ui-lab-input
  ./scripts/inspect-streaming.sh consume spark-ui-lab-input 5
  ./scripts/inspect-streaming.sh consume spark-ui-lab-output 5
MSG
}

if [ -z "$ACTION" ]; then
  usage
  exit 1
fi

if ! docker compose --profile streaming exec -T redpanda rpk cluster info -X brokers="$BROKERS" >/dev/null 2>&1; then
  echo "Redpanda is not running. Start it with ./scripts/up-streaming.sh."
  exit 1
fi

case "$ACTION" in
  topics)
    docker compose --profile streaming exec -T redpanda rpk topic list -X brokers="$BROKERS"
    ;;
  describe)
    if [ -z "$TOPIC" ]; then
      echo "Missing topic."
      usage
      exit 1
    fi
    docker compose --profile streaming exec -T redpanda rpk topic describe "$TOPIC" -X brokers="$BROKERS"
    ;;
  consume)
    if [ -z "$TOPIC" ]; then
      echo "Missing topic."
      usage
      exit 1
    fi
    docker compose --profile streaming exec -T redpanda rpk topic consume "$TOPIC" \
      --offset start \
      --num "$NUM" \
      --format '%p:%o key=%k value=%v\n' \
      -X brokers="$BROKERS"
    ;;
  groups)
    docker compose --profile streaming exec -T redpanda rpk group list -X brokers="$BROKERS"
    ;;
  *)
    echo "Unsupported action: $ACTION"
    usage
    exit 1
    ;;
esac
