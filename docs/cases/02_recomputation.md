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

## Expected Baseline Symptoms

Repeated stages appear for the same transformation lineage. Storage does not show useful persisted data.

## Diagnosis Explanation

The DataFrame is reused by several actions, but Spark has no stored intermediate result to reuse.

## Optimized Command

```bash
./scripts/run-case.sh 02_recomputation optimized
```

## Expected Optimized Symptoms

The Storage tab shows a persisted DataFrame during inspection, and later actions reuse it.

## Explanation Of The Fix

Persist only the reused intermediate DataFrame, materialize it with an action and unpersist it after inspection.

## How To Verify Improvement

Compare repeated stage patterns and check the Storage tab in the optimized run.

## Cleanup Notes

The application unpersists after the inspection pause.

## Optional AI-Assisted Diagnosis

Use `docs/ai/01-diagnosis-prompt-template.md` with Jobs, Stages and Storage observations.
