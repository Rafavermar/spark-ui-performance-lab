# 13 Task Failure Retry

## Problem

A controlled transient failure causes a Spark task retry.

## Why It Matters

Spark may recover from task failures, but retries still appear in UI and affect runtime.

## Baseline Command

```bash
./scripts/run-case.sh 13_task_failure_retry baseline
```

## What To Inspect In Spark UI

Jobs, Stages and Executors.

## UI Drilldown

Use Jobs and Stages detail pages to find the failed task attempt and retry. Event Timeline is useful because retry timing is visible. Executors helps confirm failed task counts and provides log links.

## Evidence Interpretation

The signal is failure followed by successful retry. Exact timing is irrelevant; look for failed task attempt evidence in Jobs/Stages and a final successful application.

## Expected Baseline Symptoms

One task attempt fails and a retry succeeds.

## Diagnosis Explanation

The baseline intentionally fails one partition once so learners can identify retry evidence.

## Code-Level Cause

`TaskFailureRetryCase.runBaseline` uses `mapPartitionsWithIndex` and intentionally throws once for partition `3` on attempt `0`. A marker file prevents repeated failure, so Spark retries and completes.

## Optimized Command

```bash
./scripts/run-case.sh 13_task_failure_retry optimized
```

## Expected Optimized Symptoms

The job completes without the controlled retry.

## Explanation Of The Fix

Validate and filter problematic records before processing.

## Code-Level Fix

`TaskFailureRetryCase.runOptimized` introduces one bad raw value, filters values with `rlike("^[0-9]+$")`, casts only valid rows and then aggregates.

## How To Verify Improvement

Compare failed task count and stage attempt evidence.

## Cleanup Notes

The failure marker is stored under Spark checkpoints and can be reset with `./scripts/clean.sh`.

## Optional AI-Assisted Diagnosis

Use `docs/ai/03-spark-ui-evidence-review-prompt.md` with failed task evidence.
