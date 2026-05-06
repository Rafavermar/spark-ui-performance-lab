# 16 Stateful Streaming

## Problem

State grows over time without a bounded watermark strategy.

## Why It Matters

Unbounded streaming state increases memory pressure and recovery cost.

## Baseline Command

```bash
./scripts/up-streaming.sh
./scripts/create-topics.sh
./scripts/produce-streaming-data.sh
./scripts/run-case.sh 16_stateful_streaming baseline
```

## What To Inspect In Spark UI

Structured Streaming state operator metrics.

## UI Drilldown

Use state operator metrics as the primary evidence: state rows, memory used by state and batch progress. SQL plan details are secondary for this case.

## Expected Baseline Symptoms

State rows accumulate for a wide window with no watermark.

## Diagnosis Explanation

The query groups by window and key without a bounded state cleanup strategy.

## Code-Level Cause

`StatefulStreamingCase.runBaseline` groups by a `10 minutes` window and `key` without a watermark, then writes in `complete` mode. This makes state growth visible in the Structured Streaming tab.

## Optimized Command

```bash
./scripts/run-case.sh 16_stateful_streaming optimized
```

## Expected Optimized Symptoms

The query uses watermarking and shorter windows, making state behavior easier to bound.

## Explanation Of The Fix

Add watermarking and design stateful aggregations with bounded windows.

## Code-Level Fix

`StatefulStreamingCase.runOptimized` adds `withWatermark("event_ts", "1 minute")`, changes the window to `1 minute` and uses `append` output mode.

## How To Verify Improvement

Compare state operator metrics and query progress.

## Cleanup Notes

Use `./scripts/reset-streaming.sh` before rerunning from scratch.

## Optional AI-Assisted Diagnosis

Use `docs/ai/03-spark-ui-evidence-review-prompt.md` with state operator metrics.
