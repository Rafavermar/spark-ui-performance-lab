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

## UI Drilldown

Open the slowest Stage detail page. Compare task duration percentiles and max duration; this is where skew becomes visible. Use SQL to connect the symptom to the join and aggregation shape.

## Evidence Interpretation

The signal is uneven task distribution: most tasks finish much earlier than the slowest task. Exact durations are machine-dependent; compare the spread between median/75th percentile and max task duration.

## Common Misread

Do not expect every task to take exactly the same time after optimization. The goal is a less extreme spread, not perfectly identical task durations.

## Expected Baseline Symptoms

Task duration distribution is uneven, with one or a few tasks much slower.

## Diagnosis Explanation

The hot key sends too much data to the same reduce-side task.

## Code-Level Cause

`DataSkewCase.runBaseline` joins skewed data on `join_key` with AQE skew join handling disabled. The generated data contains a dominant hot key, so work is unevenly distributed.

## Optimized Command

```bash
./scripts/run-case.sh 05_data_skew optimized
```

## Expected Optimized Symptoms

Task duration should be less uneven because the hot key is salted across multiple tasks.

## Explanation Of The Fix

Salt the skewed key and expand the small side for that key so the join remains correct.

## Code-Level Fix

`DataSkewCase.runOptimized` adds a `salt` column for the hot key, expands the matching right-side row with `explode(sequence(0, 15))`, joins on `join_key, salt` and enables AQE skew handling.

## How To Verify Improvement

Compare task duration distribution and SQL plan shape.

## Cleanup Notes

No special cleanup required.

## Optional AI-Assisted Diagnosis

Use [Spark UI evidence review prompt](../ai/03-spark-ui-evidence-review-prompt.md) with task duration observations.
