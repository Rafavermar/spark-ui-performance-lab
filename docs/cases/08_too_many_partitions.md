# 08 Too Many Partitions

## Problem

Too many partitions create scheduling overhead for a small dataset.

## Why It Matters

Many tiny tasks can make overhead dominate useful work.

## Baseline Command

```bash
./scripts/run-case.sh 08_too_many_partitions baseline
```

## What To Inspect In Spark UI

Jobs and Stages.

## UI Drilldown

Use Stages for task count and Stage detail for many tiny task durations. If visible, scheduler delay helps explain overhead. Timeline is useful when the UI looks noisy because of many short tasks.

## Evidence Interpretation

The signal is excessive task count for a small dataset. Scheduler delay or many tiny task durations support the diagnosis, but the exact milliseconds vary by machine.

## Expected Baseline Symptoms

Hundreds of tiny tasks appear.

## Diagnosis Explanation

The partition count is too high for the data size.

## Code-Level Cause

`TooManyPartitionsCase.runBaseline` forces only `30000` rows into `400` partitions. The UI shows the cost of scheduling many tiny tasks.

## Optimized Command

```bash
./scripts/run-case.sh 08_too_many_partitions optimized
```

## Expected Optimized Symptoms

Task count is lower and each task has more useful work.

## Explanation Of The Fix

Coalesce or repartition to a sensible number.

## Code-Level Fix

`TooManyPartitionsCase.runOptimized` starts with fewer partitions and applies `coalesce(12)`, keeping enough parallelism without hundreds of tiny tasks.

## How To Verify Improvement

Compare task counts and stage duration patterns.

## Cleanup Notes

No special cleanup required.

## Optional AI-Assisted Diagnosis

Use [baseline vs optimized comparison prompt](../ai/02-baseline-vs-optimized-comparison-prompt.md) with stage task counts.
