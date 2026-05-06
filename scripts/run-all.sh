#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

CASES=(
  01_too_many_actions
  02_recomputation
  03_shuffle_explosion
  04_broadcast_join
  05_data_skew
  06_small_files
  07_too_few_partitions
  08_too_many_partitions
  09_spill
  10_cache_misuse
  11_udf_cost
  12_aqe_comparison
  13_task_failure_retry
  14_config_validation
)

for case_id in "${CASES[@]}"; do
  echo "Running $case_id baseline"
  LAB_AUTO_EXIT=true ./scripts/run-case.sh "$case_id" baseline
  echo "Running $case_id optimized"
  LAB_AUTO_EXIT=true ./scripts/run-case.sh "$case_id" optimized
done
