# 05 Data Skew

## Problem

One hot key dominates a join and creates uneven task duration.

## Why It Matters

Skew makes one or a few tasks become stragglers while other tasks finish quickly.

## Baseline Command

```bash
./scripts/run-case.sh 05_data_skew baseline
```

## What To Inspect In Spark UI

Stages task table and SQL.

## Expected Baseline Symptoms

Task duration distribution is uneven, with one or a few tasks much slower.

## Diagnosis Explanation

The hot key sends too much data to the same reduce-side task.

## Optimized Command

```bash
./scripts/run-case.sh 05_data_skew optimized
```

## Expected Optimized Symptoms

Task duration should be less uneven because the hot key is salted across multiple tasks.

## Explanation Of The Fix

Salt the skewed key and expand the small side for that key so the join remains correct.

## How To Verify Improvement

Compare task duration distribution and SQL plan shape.

## Cleanup Notes

No special cleanup required.

## Optional AI-Assisted Diagnosis

Use `docs/ai/03-spark-ui-evidence-review-prompt.md` with task duration observations.
