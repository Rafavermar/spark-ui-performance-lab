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

## Expected Baseline Symptoms

Hundreds of tiny tasks appear.

## Diagnosis Explanation

The partition count is too high for the data size.

## Optimized Command

```bash
./scripts/run-case.sh 08_too_many_partitions optimized
```

## Expected Optimized Symptoms

Task count is lower and each task has more useful work.

## Explanation Of The Fix

Coalesce or repartition to a sensible number.

## How To Verify Improvement

Compare task counts and stage duration patterns.

## Cleanup Notes

No special cleanup required.

## Optional AI-Assisted Diagnosis

Use `docs/ai/02-baseline-vs-optimized-comparison-prompt.md` with stage task counts.
