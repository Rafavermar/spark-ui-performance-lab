# 04 Broadcast Join

## Problem

A small dimension table is joined without broadcast, causing shuffle on both sides.

## Why It Matters

Broadcasting a genuinely small table can remove expensive sort-merge shuffle work.

## Baseline Command

```bash
./scripts/run-case.sh 04_broadcast_join baseline
```

## What To Inspect In Spark UI

SQL and Stages.

## Expected Baseline Symptoms

The physical plan shows SortMergeJoin and Exchange operators.

## Diagnosis Explanation

Broadcast is disabled, so Spark must repartition and sort both sides.

## Code-Level Cause

`BroadcastJoinCase.runBaseline` sets `spark.sql.autoBroadcastJoinThreshold=-1` and joins fact and dimension data without a broadcast hint. Spark therefore plans a shuffle join.

## Optimized Command

```bash
./scripts/run-case.sh 04_broadcast_join optimized
```

## Expected Optimized Symptoms

The physical plan should show broadcast exchange and BroadcastHashJoin evidence.

## Explanation Of The Fix

Enable broadcast for the small side and use an explicit broadcast hint.

## Code-Level Fix

`BroadcastJoinCase.runOptimized` sets `spark.sql.autoBroadcastJoinThreshold=20m` and joins with `broadcast(SyntheticData.dim(spark))`.

## How To Verify Improvement

Compare the join operator and shuffle stages.

## Cleanup Notes

No special cleanup required.

## Optional AI-Assisted Diagnosis

Use `docs/ai/01-diagnosis-prompt-template.md` with SQL physical plan evidence.
