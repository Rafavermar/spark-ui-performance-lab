# 15 Structured Streaming Backlog

## Problem

Input rate exceeds processing rate in a micro-batch streaming query.

## Why It Matters

Backlog symptoms show up as growing batch duration or processed rows/sec lagging input rows/sec.

## Baseline Command

```bash
./scripts/up-streaming.sh
./scripts/create-topics.sh
./scripts/produce-streaming-data.sh
./scripts/run-case.sh 15_structured_streaming_backlog baseline
```

## What To Inspect In Spark UI

Structured Streaming.

## UI Drilldown

Use Structured Streaming query progress: batch duration, input rows/sec and processed rows/sec. Jobs and Stages are secondary unless a specific micro-batch needs debugging.

## Evidence Interpretation

The signal is backlog tendency: processing takes longer than the trigger cadence or processed rows/sec lags input rows/sec. Exact rates depend on host resources and Kafka/Redpanda timing.

## Expected Baseline Symptoms

Batch duration is intentionally slower than the trigger interval because the query adds processing delay.

## Diagnosis Explanation

The query is doing more work per trigger than it can comfortably finish.

## Code-Level Cause

`StructuredStreamingBacklogCase.runBaseline` reads Kafka with `maxOffsetsPerTrigger=500`, uses a `2 seconds` processing-time trigger and sleeps for `3500` ms inside `foreachBatch`.

## Optimized Command

```bash
./scripts/run-case.sh 15_structured_streaming_backlog optimized
```

## Expected Optimized Symptoms

Batch duration should be steadier because offset volume is reduced and artificial delay is removed.

## Explanation Of The Fix

Tune input rate and processing logic so each trigger can complete predictably.

## Code-Level Fix

`StructuredStreamingBacklogCase.runOptimized` lowers `maxOffsetsPerTrigger` to `150` and removes the artificial `Thread.sleep`.

## How To Verify Improvement

Compare batch duration, input rows/sec and processed rows/sec.

## Cleanup Notes

Use `./scripts/reset-streaming.sh` to reset topics and checkpoints.

## Optional AI-Assisted Diagnosis

Use [baseline vs optimized comparison prompt](../ai/02-baseline-vs-optimized-comparison-prompt.md) with streaming progress metrics.
