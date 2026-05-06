# 01 Too Many Actions

## Problem

Multiple unnecessary actions trigger multiple Spark jobs over the same lineage.

## Why It Matters

Each action can rescan or recompute upstream work, increasing runtime and making the Jobs tab noisy.

## Baseline Command

```bash
./scripts/run-case.sh 01_too_many_actions baseline
```

## What To Inspect In Spark UI

Jobs and Stages.

## UI Drilldown

Use the Jobs table as the primary evidence. The Event Timeline is optional and helps show that several actions created separate jobs. Open one DAG only if you want to see similar lineage repeated; detailed stage metrics are introduced later.

## Expected Baseline Symptoms

Several jobs appear for `count`, filtered `count`, grouped `count` and aggregate actions.

Some stages may be marked as skipped because Spark can reuse already completed stage output inside the same application. The key symptom is still visible: one logical case execution created multiple Spark jobs.

## Diagnosis Explanation

The issue is not a single slow transformation; it is repeated actions over similar lineage.

For this introductory case, focus first on the Jobs tab and job count. The DAG is optional; it can help show repeated lineage, but deeper DAG analysis is introduced in later shuffle and join cases.

## Code-Level Cause

`TooManyActions.runBaseline` executes several actions over the same DataFrame: `count`, filtered `count`, grouped `count` and `collect`. Each action can create a separate Spark job.

## Optimized Command

```bash
./scripts/run-case.sh 01_too_many_actions optimized
```

## Expected Optimized Symptoms

Fewer jobs because the summary is computed in one aggregate action.

## Explanation Of The Fix

Consolidate actions and compute required metrics together where possible.

In `src/main/scala/lab/cases/BatchCasesPart1.scala`, `runBaseline` executes several actions over the same DataFrame. `runOptimized` computes the same learning-relevant metrics in one aggregate action, so Spark has fewer jobs to schedule.

## Code-Level Fix

`TooManyActions.runOptimized` replaces several independent actions with one `agg(...)` that computes total rows, high-score rows and amount sum together.

## How To Verify Improvement

Compare the number of jobs and stages in baseline vs optimized executions.

## Cleanup Notes

No special cleanup required.

## Optional AI-Assisted Diagnosis

Use `docs/ai/02-baseline-vs-optimized-comparison-prompt.md` with Jobs tab evidence.
