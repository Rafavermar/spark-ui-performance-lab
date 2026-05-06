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

## Expected Baseline Symptoms

Very few tasks appear, and executor utilization is low.

## Diagnosis Explanation

The workload is forced into too few partitions.

## Optimized Command

```bash
./scripts/run-case.sh 07_too_few_partitions optimized
```

## Expected Optimized Symptoms

More tasks are available for the workers.

## Explanation Of The Fix

Repartition to a reasonable local demo value.

## How To Verify Improvement

Compare task counts and executor task activity.

## Cleanup Notes

No special cleanup required.

## Optional AI-Assisted Diagnosis

Use `docs/ai/03-spark-ui-evidence-review-prompt.md` with Executors and Stages evidence.
