# 17 Real-Time Mode

## Problem

Compare standard micro-batch streaming with Spark 4.1 real-time mode for a stateless query.

## Why It Matters

Real-time mode is an advanced execution option and should be evaluated through actual query progress evidence, not fixed latency promises.

## Baseline Command

```bash
./scripts/up-streaming.sh
./scripts/create-topics.sh
./scripts/produce-streaming-data.sh
./scripts/run-case.sh 17_real_time_mode baseline
```

## What To Inspect In Spark UI

Structured Streaming.

## UI Drilldown

Use Structured Streaming query progress and trigger evidence. Compare micro-batch baseline with advanced real-time mode. Do not use this case to claim fixed latency numbers.

## Evidence Interpretation

The signal is execution mode and query progress evidence. This is a feature comparison, not a benchmark; do not interpret exact latency or rows/sec as universal.

## Expected Baseline Symptoms

The query uses regular micro-batch processing with Kafka source and sink.

## Diagnosis Explanation

The baseline establishes standard micro-batch behavior for comparison.

## Code-Level Baseline

`RealTimeModeCase.runBaseline` runs a stateless Kafka-to-Kafka query with standard micro-batch execution using `Trigger.ProcessingTime("5 seconds")`.

## Optimized Command

```bash
./scripts/run-case.sh 17_real_time_mode advanced
```

`optimized` is accepted as an alias for `advanced`.

## Expected Optimized Symptoms

The query uses Spark 4.1 real-time trigger where supported.

## Explanation Of The Fix

Use real-time mode only for a stateless streaming query and compare Structured Streaming progress evidence.

## Code-Level Advanced Mode

`RealTimeModeCase.runOptimized` sets `spark.sql.streaming.realTimeMode.minBatchDuration=5s` and uses `Trigger.RealTime("5 seconds")` for the same stateless Kafka-to-Kafka query. `optimized` is accepted as an alias for `advanced`.

## How To Verify Improvement

Compare trigger mode, query progress and processing metrics. Do not claim fixed latency.

## Cleanup Notes

Use `./scripts/reset-streaming.sh` before rerunning from scratch.

## Optional AI-Assisted Diagnosis

Use [baseline vs optimized comparison prompt](../ai/02-baseline-vs-optimized-comparison-prompt.md) with Structured Streaming metrics.
