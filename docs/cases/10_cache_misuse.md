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

## Expected Baseline Symptoms

The Storage tab shows cached data, but the cached DataFrame is not reused enough to justify it.

## Diagnosis Explanation

Caching is a cost unless downstream actions reuse the cached result.

## Optimized Command

```bash
./scripts/run-case.sh 10_cache_misuse optimized
```

## Expected Optimized Symptoms

Storage remains empty or much quieter.

## Explanation Of The Fix

Cache only reused intermediate results and unpersist explicitly.

## How To Verify Improvement

Compare Storage tab entries and executor storage memory.

## Cleanup Notes

The baseline unpersists after the inspection pause.

## Optional AI-Assisted Diagnosis

Use `docs/ai/01-diagnosis-prompt-template.md` with Storage evidence.
