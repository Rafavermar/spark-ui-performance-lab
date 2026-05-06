# 10 Cache Misuse

## Problem

Caching data that is not reused consumes memory without benefit.

## Why It Matters

Storage memory can evict useful data or pressure executors.

## Baseline Command

```bash
./scripts/run-case.sh 10_cache_misuse baseline
```

## What To Inspect In Spark UI

Storage and Executors.

## UI Drilldown

Use Storage as the primary evidence. Executors can show storage memory usage. Jobs, Stages and SQL are secondary because the diagnosis is about whether cached data is actually reused.

## Evidence Interpretation

The signal is cached data without meaningful reuse. Storage tab presence is decisive; executor storage memory is supporting evidence and exact memory values are machine-dependent.

## Expected Baseline Symptoms

The Storage tab shows cached data, but the cached DataFrame is not reused enough to justify it.

## Diagnosis Explanation

Caching is a cost unless downstream actions reuse the cached result.

## Code-Level Cause

`CacheMisuseCase.runBaseline` creates a wide payload, persists it with `StorageLevel.MEMORY_AND_DISK`, materializes it and then uses it only once downstream.

## Optimized Command

```bash
./scripts/run-case.sh 10_cache_misuse optimized
```

## Expected Optimized Symptoms

Storage remains empty or much quieter.

## Explanation Of The Fix

Cache only reused intermediate results and unpersist explicitly.

## Code-Level Fix

`CacheMisuseCase.runOptimized` removes `persist` entirely and runs the aggregation directly from the source DataFrame.

## How To Verify Improvement

Compare Storage tab entries and executor storage memory.

## Cleanup Notes

The baseline unpersists after the inspection pause.

## Optional AI-Assisted Diagnosis

Use [diagnosis prompt template](../ai/01-diagnosis-prompt-template.md) with Storage evidence.
