# 11 UDF Cost

## Problem

A UDF is used where built-in Spark SQL functions are enough.

## Why It Matters

UDFs can make plans less optimizer-friendly and harder to reason about in SQL UI.

## Baseline Command

```bash
./scripts/run-case.sh 11_udf_cost baseline
```

## What To Inspect In Spark UI

SQL.

## Expected Baseline Symptoms

The physical plan includes UDF-related expressions.

## Diagnosis Explanation

The logic is simple, but Catalyst sees it as a UDF expression.

## Code-Level Cause

`UdfCostCase.runBaseline` uses a Scala UDF to label each row as `even` or `odd`. This simple logic is visible in the plan as UDF-related execution instead of native SQL expressions.

## Optimized Command

```bash
./scripts/run-case.sh 11_udf_cost optimized
```

## Expected Optimized Symptoms

The SQL plan uses built-in conditional expressions.

## Explanation Of The Fix

Replace the UDF with built-in functions such as `when` and `otherwise`.

## Code-Level Fix

`UdfCostCase.runOptimized` replaces the UDF with `when($"id" % 2 === 0, lit("even")).otherwise(lit("odd"))`.

## How To Verify Improvement

Compare the SQL physical plan.

## Cleanup Notes

No special cleanup required.

## Optional AI-Assisted Diagnosis

Use `docs/ai/01-diagnosis-prompt-template.md` with the SQL plan.
