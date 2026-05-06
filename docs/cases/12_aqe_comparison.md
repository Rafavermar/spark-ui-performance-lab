# 12 AQE Comparison

## Problem

Adaptive Query Execution is disabled for a shuffle-heavy query.

## Why It Matters

AQE can adapt plans at runtime, especially around shuffle partitioning.

## Baseline Command

```bash
./scripts/run-case.sh 12_aqe_comparison baseline
```

## What To Inspect In Spark UI

SQL and Stages.

## UI Drilldown

Use SQL Plan Visualization and Plan Details as the primary evidence. Look for `AdaptiveSparkPlan`, initial/final plan sections and `AQEShuffleRead`. Use Stages to confirm shuffle behavior changed.

## Evidence Interpretation

The signal is adaptive plan evidence, not a fixed speedup. Compare plan structure and shuffle adaptation markers; stage counts and durations are supporting evidence.

## Expected Baseline Symptoms

The plan is non-adaptive and uses the configured shuffle partitions directly.

## Diagnosis Explanation

AQE is disabled, so Spark cannot coalesce or adapt shuffle behavior at runtime.

## Code-Level Cause

`AqeComparisonCase.runBaseline` sets `spark.sql.adaptive.enabled=false`, disables broadcast and uses `spark.sql.shuffle.partitions=64` for the comparison query.

## Optimized Command

```bash
./scripts/run-case.sh 12_aqe_comparison optimized
```

## Expected Optimized Symptoms

The SQL plan shows adaptive planning evidence.

## Explanation Of The Fix

Enable AQE and compare physical plan and stage behavior.

## Code-Level Fix

`AqeComparisonCase.runOptimized` keeps the same query shape but sets `spark.sql.adaptive.enabled=true`, so the SQL tab can show adaptive planning evidence.

## How To Verify Improvement

Compare the SQL plan and shuffle stage details.

## Cleanup Notes

No special cleanup required.

## Optional AI-Assisted Diagnosis

Use [baseline vs optimized comparison prompt](../ai/02-baseline-vs-optimized-comparison-prompt.md) with SQL and stage evidence.
