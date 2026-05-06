# 17 Real-Time Mode

## Problem

Compare standard micro-batch streaming with Spark 4.1 real-time mode for a stateless query.

## Why It Matters

Real-time mode is an advanced execution option and should be evaluated through actual query progress evidence, not fixed latency promises.

For the complete Redpanda topic flow, start/stop commands and code path, see [Streaming and real-time mode](../09-streaming-real-time-mode.md).

## Baseline Command

```bash
./scripts/up-streaming.sh
./scripts/create-topics.sh
./scripts/produce-streaming-data.sh
./scripts/run-case.sh 17_real_time_mode baseline
```

## What To Inspect In Spark UI

- Structured Streaming.
- Jobs.
- Stages.
- Executors.
- Environment.

## UI Drilldown

Use Structured Streaming query progress and trigger evidence. Compare micro-batch baseline with advanced real-time mode. Do not use this case to claim fixed latency numbers.

Jobs and Stages are useful supporting evidence because real-time mode creates recurring active work. Executors can show active tasks distributed across workers. Environment can confirm the real-time mode configuration.

## Evidence Interpretation

The signal is execution mode and query progress evidence. This is a feature comparison, not a benchmark; do not interpret exact latency or rows/sec as universal.

Some Spark UI columns can be blank or low-signal for this stateless Kafka-to-Kafka query. For example, shuffle columns are not central because the case does not perform a shuffle.

## Common Misread

Do not present real-time mode as a guaranteed latency number. This case demonstrates where to inspect the execution mode and query progress evidence.

## Expected Baseline Symptoms

The query uses regular micro-batch processing with Kafka source and sink.

## Diagnosis Explanation

The baseline establishes standard micro-batch behavior for comparison.

## Code-Level Baseline

`RealTimeModeCase.runBaseline` runs a stateless Kafka-to-Kafka query with standard micro-batch execution using `Trigger.ProcessingTime("5 seconds")`.

Code location: [StreamingCases.scala](../../src/main/scala/lab/cases/StreamingCases.scala).

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

You can also inspect topic data:

```bash
./scripts/inspect-streaming.sh consume spark-ui-lab-input 5
./scripts/inspect-streaming.sh consume spark-ui-lab-output 5
```

Use Redpanda to confirm records exist in topics. Use Spark UI to confirm Spark processing.

## Cleanup Notes

Use `./scripts/reset-streaming.sh` before rerunning from scratch.

After stopping the query, Spark may print task-cancellation messages while the active streaming batch shuts down. If the script exits normally, this is expected shutdown noise.

## Optional AI-Assisted Diagnosis

Use [baseline vs optimized comparison prompt](../ai/02-baseline-vs-optimized-comparison-prompt.md) with Structured Streaming metrics.
