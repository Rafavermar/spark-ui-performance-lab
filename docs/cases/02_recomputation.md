# 02 Recomputation

## Problem

The same expensive transformed DataFrame is reused without persistence.

## Why It Matters

Spark recomputes lineage for every action unless an intermediate result is persisted and materialized.

## Baseline Command

```bash
./scripts/run-case.sh 02_recomputation baseline
```

## What To Inspect In Spark UI

Jobs, Stages and Storage.

## UI Drilldown

Use Jobs and Stages to see repeated execution, then use Storage to prove that no persisted intermediate exists. Open one Job detail page and follow `Associated SQL Query` once; this shows that DataFrame actions appear in the SQL/DataFrame tab. Open one Stage detail page only to learn the layout. For this case, do not focus on GC, spill or locality unless they are extreme. Executors is optional in optimized mode: it may show non-zero storage memory, but the exact value is machine-dependent.

## Evidence Interpretation

Repeated stage pattern means similar stage shapes and task counts appear across several actions over the same lineage. Do not require identical shuffle read/write bytes; those numbers are supporting evidence and can vary. Storage is the decisive evidence: empty in baseline, populated during optimized inspection.

## Expected Baseline Symptoms

Repeated stages appear for the same transformation lineage. Storage does not show useful persisted data.

Some jobs may show skipped stages. That is normal stage reuse inside the same Spark application. It is not the same as DataFrame persistence; the Storage tab is the evidence for cache/persist.

## Diagnosis Explanation

The DataFrame is reused by several actions, but Spark has no stored intermediate result to reuse.

## Code-Level Cause

`Recomputation.runBaseline` builds `expensiveFrame(spark)` and then runs `count`, `distinct().count()` and grouped `count` over it without persistence. The hash and score transformations can be recomputed for each action.

## Optimized Command

```bash
./scripts/run-case.sh 02_recomputation optimized
```

## Expected Optimized Symptoms

The Storage tab shows a persisted DataFrame during inspection, and later actions reuse it.

Do not expect fewer jobs as the main improvement. The optimized version deliberately materializes the persisted DataFrame with an action, so job count can remain similar or even be slightly higher.

## Explanation Of The Fix

Persist only the reused intermediate DataFrame, materialize it with an action and unpersist it after inspection.

## Code-Level Fix

`Recomputation.runOptimized` applies `persist(StorageLevel.MEMORY_AND_DISK)`, materializes with `df.count()`, reuses the DataFrame for later actions and unpersists after inspection.

## How To Verify Improvement

Compare repeated stage patterns and check the Storage tab in the optimized run.

Expected values are patterns, not exact numbers: optimized should show cached partitions and a non-empty Storage tab while the app is paused, but storage memory, task time, GC time and shuffle bytes can differ by machine.

## Cleanup Notes

The application unpersists after the inspection pause.

## Optional AI-Assisted Diagnosis

Use [diagnosis prompt template](../ai/01-diagnosis-prompt-template.md) with Jobs, Stages and Storage observations.
