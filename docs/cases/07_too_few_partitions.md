# 07 Too Few Partitions

## Problem

Insufficient parallelism leaves executors underused.

## Why It Matters

Spark cannot use available cores if there are too few tasks.

## Baseline Command

```bash
./scripts/run-case.sh 07_too_few_partitions baseline
```

## What To Inspect In Spark UI

Executors and Stages.

## UI Drilldown

Use Stages for task count and Executors for task activity per executor. The key question is whether there is enough parallel work to use the cluster. DAG and SQL plan details are secondary.

## Expected Baseline Symptoms

Very few tasks appear, and executor utilization is low.

## Diagnosis Explanation

The workload is forced into too few partitions.

## Code-Level Cause

`TooFewPartitionsCase.runBaseline` creates the range with one partition: `spark.range(..., 1)`. The following computation has too few tasks to keep both workers busy.

## Optimized Command

```bash
./scripts/run-case.sh 07_too_few_partitions optimized
```

## Expected Optimized Symptoms

More tasks are available for the workers.

## Explanation Of The Fix

Repartition to a reasonable local demo value.

## Code-Level Fix

`TooFewPartitionsCase.runOptimized` starts with `16` partitions and calls `repartition(16)` before running the same grouping logic.

## How To Verify Improvement

Compare task counts and executor task activity.

## Cleanup Notes

No special cleanup required.

## Optional AI-Assisted Diagnosis

Use `docs/ai/03-spark-ui-evidence-review-prompt.md` with Executors and Stages evidence.
