# 03 Shuffle Explosion

## Problem

A wide `groupBy` and `orderBy` plan shuffles more data than needed.

## Why It Matters

Shuffle is often the dominant cost in Spark SQL workloads.

## Baseline Command

```bash
./scripts/run-case.sh 03_shuffle_explosion baseline
```

## What To Inspect In Spark UI

SQL and Stages.

## UI Drilldown

Open the SQL query, Plan Visualization and Plan Details. Search for `Exchange`, then compare shuffle read/write in Stages. DAG inspection is useful here because shuffle boundaries are the lesson.

## Evidence Interpretation

The decisive signal is physical plan shape: wide grouping before projection/filtering and visible `Exchange` operators. Shuffle read/write values should move in the right direction after optimization, but exact bytes depend on execution details.

## Expected Baseline Symptoms

The SQL plan shows Exchange operators. Stages show shuffle read/write for wide rows that include payload columns.

## Diagnosis Explanation

The query groups before filtering or projecting down to the required columns.

## Code-Level Cause

`ShuffleExplosion.runBaseline` groups by `country_id`, `category_id`, `payload_a` and `payload_b`, then orders by the aggregate. It also sets `spark.sql.shuffle.partitions=48`, making the wide shuffle easy to spot in the SQL and Stages tabs.

## Optimized Command

```bash
./scripts/run-case.sh 03_shuffle_explosion optimized
```

## Expected Optimized Symptoms

The optimized plan still shuffles for aggregation, but it shuffles fewer columns and fewer rows.

## Explanation Of The Fix

Filter early, select only required columns and use a smaller local shuffle partition count.

## Code-Level Fix

`ShuffleExplosion.runOptimized` filters `is_active` records for selected countries, projects only `country_id`, `category_id` and `amount`, groups by fewer keys and lowers shuffle partitions to `12`.

## How To Verify Improvement

Compare SQL plans and stage shuffle metrics.

## Cleanup Notes

No special cleanup required.

## Optional AI-Assisted Diagnosis

Use `docs/ai/02-baseline-vs-optimized-comparison-prompt.md` with SQL plan and shuffle metrics.
