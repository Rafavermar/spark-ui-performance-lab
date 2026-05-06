# 03 Shuffle Explosion

## Problem

A wide `groupBy` and `orderBy` plan shuffles more data than needed.

## Why It Matters

Shuffle is often the dominant cost in Spark SQL workloads.

## Baseline Command

```bash
./scripts/run-case.sh 03_shuffle_explosion baseline
```

## What To Inspect In Spark UI

SQL and Stages.

## Expected Baseline Symptoms

The SQL plan shows Exchange operators. Stages show shuffle read/write for wide rows that include payload columns.

## Diagnosis Explanation

The query groups before filtering or projecting down to the required columns.

## Optimized Command

```bash
./scripts/run-case.sh 03_shuffle_explosion optimized
```

## Expected Optimized Symptoms

The optimized plan still shuffles for aggregation, but it shuffles fewer columns and fewer rows.

## Explanation Of The Fix

Filter early, select only required columns and use a smaller local shuffle partition count.

## How To Verify Improvement

Compare SQL plans and stage shuffle metrics.

## Cleanup Notes

No special cleanup required.

## Optional AI-Assisted Diagnosis

Use `docs/ai/02-baseline-vs-optimized-comparison-prompt.md` with SQL plan and shuffle metrics.
