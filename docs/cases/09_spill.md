# 09 Spill

## Problem

Wide rows and low partition count can cause memory pressure and spill.

## Why It Matters

Spill often indicates that Spark is moving data out of memory during expensive shuffle or sort work.

## Baseline Command

```bash
./scripts/run-case.sh 09_spill baseline
```

## What To Inspect In Spark UI

Stages and Executors.

## UI Drilldown

Use Stage detail metrics as the main evidence: memory spill, disk spill, peak execution memory, GC time and long task duration. Executors helps confirm memory or GC pressure. SQL is optional unless you want to connect pressure to sort/aggregate operators.

## Evidence Interpretation

Spill bytes are strong evidence when they appear, but they can vary with Docker memory and host resources. If spill is low or absent, use wide rows, low partition count, peak memory, GC and longer tasks as supporting evidence.

## Expected Baseline Symptoms

Look for memory spill, disk spill, long task times or executor memory pressure. Spill visibility depends on machine resources.

## Diagnosis Explanation

The baseline keeps wide payloads and uses few shuffle partitions.

## Code-Level Cause

`SpillCase.runBaseline` sets `spark.sql.shuffle.partitions=4`, creates wide SHA-256 string payloads, repartitions by a high-cardinality key and sorts within partitions. This increases per-task memory pressure.

## Optimized Command

```bash
./scripts/run-case.sh 09_spill optimized
```

## Expected Optimized Symptoms

Lower memory pressure because the query narrows rows and uses more reasonable partitioning.

## Explanation Of The Fix

Avoid unnecessary wide rows and distribute work across more partitions.

## Code-Level Fix

`SpillCase.runOptimized` raises shuffle partitions to `24`, replaces the wide string payload with a numeric metric and reduces aggregation cardinality.

## How To Verify Improvement

Compare spill metrics, task duration and executor memory evidence.

## Cleanup Notes

No special cleanup required.

## Optional AI-Assisted Diagnosis

Use `docs/ai/03-spark-ui-evidence-review-prompt.md` and include spill metrics if present.
