# 06 Small Files

## Problem

Reading many small files creates many tiny tasks and scheduling overhead.

## Why It Matters

Scheduler overhead can dominate when work per task is too small.

## Baseline Command

```bash
./scripts/run-case.sh 06_small_files baseline
```

## What To Inspect In Spark UI

Jobs and Stages.

## Expected Baseline Symptoms

Many short tasks appear while reading the small JSON files.

## Diagnosis Explanation

The input layout creates too many small scan tasks for the amount of data.

## Code-Level Cause

`SmallFilesCase.runBaseline` reads `data/generated/small_files` directly as JSON. The synthetic generator intentionally creates many tiny files, so the scan produces many small tasks.

## Optimized Command

```bash
./scripts/run-case.sh 06_small_files optimized
```

## Expected Optimized Symptoms

Downstream processing reads fewer compacted Parquet partitions.

## Explanation Of The Fix

Compact small files before downstream processing.

## Code-Level Fix

`SmallFilesCase.runOptimized` reads the JSON files once, writes a compacted Parquet dataset with `coalesce(8)` under `tmp/`, then reads that compacted path for the downstream aggregation.

## How To Verify Improvement

Compare input partition count and task count.

## Cleanup Notes

Temporary compacted files are written under `tmp/` and removed by `./scripts/clean.sh`.

## Optional AI-Assisted Diagnosis

Use `docs/ai/01-diagnosis-prompt-template.md` with Jobs and Stages observations.
