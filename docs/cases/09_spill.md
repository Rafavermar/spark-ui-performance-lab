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

## Expected Baseline Symptoms

Look for memory spill, disk spill, long task times or executor memory pressure. Spill visibility depends on machine resources.

## Diagnosis Explanation

The baseline keeps wide payloads and uses few shuffle partitions.

## Optimized Command

```bash
./scripts/run-case.sh 09_spill optimized
```

## Expected Optimized Symptoms

Lower memory pressure because the query narrows rows and uses more reasonable partitioning.

## Explanation Of The Fix

Avoid unnecessary wide rows and distribute work across more partitions.

## How To Verify Improvement

Compare spill metrics, task duration and executor memory evidence.

## Cleanup Notes

No special cleanup required.

## Optional AI-Assisted Diagnosis

Use `docs/ai/03-spark-ui-evidence-review-prompt.md` and include spill metrics if present.
