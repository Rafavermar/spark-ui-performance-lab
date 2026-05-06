# 14 Config Validation

## Problem

Users may think a Spark configuration is active without verifying it.

## Why It Matters

Performance diagnosis depends on knowing which Spark properties actually applied.

## Baseline Command

```bash
./scripts/run-case.sh 14_config_validation baseline
```

## What To Inspect In Spark UI

Environment.

## Expected Baseline Symptoms

The printed configuration should match Spark Properties in the Environment tab.

## Diagnosis Explanation

The Environment tab is the source of truth for Spark properties passed to the application.

## Optimized Command

```bash
./scripts/run-case.sh 14_config_validation optimized
```

## Expected Optimized Symptoms

The optimized script passes explicit config values through `spark-submit`; verify them in Environment.

## Explanation Of The Fix

Pass config through `spark-submit` or `spark-defaults.conf` and confirm it in Spark UI.

## How To Verify Improvement

Compare printed config and Environment tab values.

## Cleanup Notes

No special cleanup required.

## Optional AI-Assisted Diagnosis

Use `docs/ai/01-diagnosis-prompt-template.md` with Environment evidence.
