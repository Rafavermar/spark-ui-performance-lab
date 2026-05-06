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

## Expected Baseline Symptoms

One task attempt fails and a retry succeeds.

## Diagnosis Explanation

The baseline intentionally fails one partition once so learners can identify retry evidence.

## Optimized Command

```bash
./scripts/run-case.sh 13_task_failure_retry optimized
```

## Expected Optimized Symptoms

The job completes without the controlled retry.

## Explanation Of The Fix

Validate and filter problematic records before processing.

## How To Verify Improvement

Compare failed task count and stage attempt evidence.

## Cleanup Notes

The failure marker is stored under Spark checkpoints and can be reset with `./scripts/clean.sh`.

## Optional AI-Assisted Diagnosis

Use `docs/ai/03-spark-ui-evidence-review-prompt.md` with failed task evidence.
