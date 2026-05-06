# Runbook

This runbook is the step-by-step path for running the lab from a clean checkout. It assumes no prior Spark UI experience.

The goal is not to memorize Spark internals first. The goal is to run one controlled problem, open the Spark UI, observe concrete evidence, run the optimized version and compare the same evidence again.

## 1. What You Need

- Docker Desktop running.
- A terminal opened at the repository root.
- No local Scala installation.
- No local SBT installation.
- No external datasets.

Confirm you are in the repository root:

```bash
pwd
ls
```

You should see files such as `docker-compose.yml`, `build.sbt`, `scripts/` and `docs/`.

## 2. First-Time Setup

Create a local `.env` file:

```bash
cp .env.example .env
```

Start the default Spark lab:

```bash
./scripts/up.sh
```

This starts:

- `spark-master`
- `spark-worker-1`
- `spark-worker-2`
- `spark-history-server`
- `spark-client`

It does not start Redpanda.

Open these pages:

- Spark Master UI: <http://localhost:8080>
- Spark History Server: <http://localhost:18080>

The live Spark Application UI at <http://localhost:4040> appears only while a case is running.

## 3. Build The Scala Application

Build inside the `spark-client` container:

```bash
./scripts/build.sh
```

Expected result:

- SBT runs inside Docker.
- The assembly jar is created at `target/scala-2.13/spark-ui-performance-lab-assembly-0.1.0.jar`.

If this fails, do not continue to cases. Fix build or Docker first.

## 4. Generate Synthetic Data

Run:

```bash
./scripts/generate-data.sh
```

Expected result:

- Synthetic datasets are written under `data/generated/`.
- No internet datasets are downloaded.

The generated datasets are ignored by git and can be recreated.

## 5. How To Run One Case

Every case uses:

```bash
./scripts/run-case.sh <case_id> <mode>
```

Example:

```bash
./scripts/run-case.sh 03_shuffle_explosion baseline
```

Modes:

- `baseline`: creates the problem.
- `optimized`: applies the fix.
- `advanced`: only used by `17_real_time_mode`.

When a case runs:

1. `scripts/run-case.sh` calls `spark-submit` inside `spark-client`.
2. `lab.Main` dispatches to the selected Scala case.
3. The Spark application name includes the case id and mode.
4. The application prints the UI tabs to inspect.
5. The application pauses so you can open <http://localhost:4040>.
6. Press Enter in the terminal when you finish inspecting the live UI.
7. After the app exits, open <http://localhost:18080> and inspect the completed app in History Server.

To see which Scala file is executed for each case, use [Code Execution Map](05-code-execution-map.md).

To understand Spark configuration before running cases, read [Spark Configuration Guide](08-spark-configuration.md). It explains what comes from `.env`, `docker-compose.yml`, `conf/spark-defaults.conf`, `spark-submit` and case-specific Scala overrides.

## 6. Standard Baseline-To-Optimized Loop

Use this exact loop for every batch case:

```text
1. Read the case section in this runbook.
2. Open the source file listed in docs/05-code-execution-map.md.
3. Run the baseline command.
4. Open http://localhost:4040 while the app is paused.
5. Inspect only the tabs listed for the case.
6. Press Enter in the terminal.
7. Open http://localhost:18080.
8. Find the completed app named "spark-ui-lab | <case_id> | baseline".
9. Run the optimized command.
10. Inspect the same UI tabs again.
11. Open History Server and compare baseline vs optimized.
```

Do not compare random tabs. Compare the tabs listed for the case.

## 7. Batch Cases

Batch cases `01` to `14` do not require Redpanda.

### 01_too_many_actions

Problem: repeated actions create repeated Spark jobs.

Baseline:

```bash
./scripts/run-case.sh 01_too_many_actions baseline
```

Inspect:

- Jobs tab.
- Stages tab.

Expected baseline evidence:

- Several jobs appear for separate actions.
- The terminal prints several independent action results.
- In the Jobs tab, `Completed Jobs` should be greater than one for this single case execution.
- In the Stages tab, several completed stages appear because each action creates its own Spark job path.
- Some stages may appear as skipped because Spark can reuse already completed shuffle/map stage output inside the same application. That is still useful evidence: the repeated actions are visible as repeated jobs.

Diagnosis:

- Multiple actions are executed over similar lineage.
- Each action can trigger its own Spark job.
- For this first case, the important evidence is the number of jobs and repeated action pattern. You do not need to deeply inspect the DAG yet.
- Optional: open a job or stage DAG to see that Spark is repeatedly walking similar lineage. Detailed DAG reading becomes more important in shuffle, join and skew cases.

Code-level cause:

- In `src/main/scala/lab/cases/BatchCasesPart1.scala`, `TooManyActions.runBaseline` executes several actions over the same DataFrame:
  - `df.count()`
  - `df.where(...).count()`
  - `df.groupBy(...).count().count()`
  - `df.agg(...).collect()`
- Each action is allowed to create a Spark job.

Optimized:

```bash
./scripts/run-case.sh 01_too_many_actions optimized
```

Verify:

- Jobs tab should be simpler.
- The optimized run computes summary metrics together.

Code-level fix:

- `TooManyActions.runOptimized` replaces several independent actions with one aggregate:
  - total rows
  - high-score rows
  - amount sum
- This is why the optimized UI shows fewer jobs and stages.

### 02_recomputation

Problem: an expensive transformed DataFrame is reused without persistence.

Baseline:

```bash
./scripts/run-case.sh 02_recomputation baseline
```

Inspect:

- Jobs tab.
- Stages tab.
- Storage tab.

Expected baseline evidence:

- Similar stages repeat across multiple actions.
- Storage tab does not show a useful persisted intermediate.

Diagnosis:

- Spark recomputes lineage because nothing is persisted.

Code-level cause:

- In `src/main/scala/lab/cases/BatchCasesPart1.scala`, `Recomputation.runBaseline` builds `expensiveFrame(spark)` once and then runs three actions over it:
  - `df.count()`
  - `df.select("bucket").distinct().count()`
  - `df.groupBy("bucket").agg(avg("score")).count()`
- Because the DataFrame is not persisted, Spark is free to recompute the expensive hash/score lineage for each action.

Optimized:

```bash
./scripts/run-case.sh 02_recomputation optimized
```

Verify:

- Storage tab shows the persisted DataFrame while the app is paused.
- Later actions reuse the persisted result.

Code-level fix:

- `Recomputation.runOptimized` applies `persist(StorageLevel.MEMORY_AND_DISK)`.
- It materializes the cache with `df.count()`, reuses the persisted DataFrame for later actions and unpersists it after inspection.

### 03_shuffle_explosion

Problem: a wide `groupBy` and `orderBy` shuffle too much data.

Baseline:

```bash
./scripts/run-case.sh 03_shuffle_explosion baseline
```

Inspect:

- SQL tab.
- Stages tab.

Expected baseline evidence:

- SQL physical plan contains `Exchange`.
- The grouping keys include payload columns.
- Stages show shuffle read/write.

Diagnosis:

- The query shuffles wide rows and too many grouping keys.

Code-level cause:

- In `ShuffleExplosion.runBaseline`, the query groups by `country_id`, `category_id`, `payload_a` and `payload_b`.
- It keeps wide payload columns until the shuffle and uses `spark.sql.shuffle.partitions=48`.
- The wide `groupBy` plus `orderBy(desc("amount_sum"))` forces expensive `Exchange` operators in the SQL plan.

Optimized:

```bash
./scripts/run-case.sh 03_shuffle_explosion optimized
```

Verify:

- SQL scan reads fewer columns.
- The plan filters earlier.
- Shuffle partition count is lower.

Code-level fix:

- `ShuffleExplosion.runOptimized` filters early with `is_active` and `country_id < 12`.
- It selects only `country_id`, `category_id` and `amount` before aggregation.
- It groups by fewer keys and reduces shuffle partitions to `12` for this laptop-scale lab.

### 04_broadcast_join

Problem: Spark uses a shuffle join even though one side is small.

Baseline:

```bash
./scripts/run-case.sh 04_broadcast_join baseline
```

Inspect:

- SQL tab.
- Stages tab.

Expected baseline evidence:

- Physical plan shows `SortMergeJoin`.
- Exchanges appear on both sides of the join.

Code-level cause:

- In `BroadcastJoinCase.runBaseline`, `spark.sql.autoBroadcastJoinThreshold` is set to `-1`.
- Spark joins `SyntheticData.fact(spark)` and `SyntheticData.dim(spark)` without a broadcast hint, so both sides are shuffled and sorted.

Optimized:

```bash
./scripts/run-case.sh 04_broadcast_join optimized
```

Verify:

- Physical plan shows broadcast join evidence, such as `BroadcastHashJoin` or broadcast exchange.

Code-level fix:

- `BroadcastJoinCase.runOptimized` sets `spark.sql.autoBroadcastJoinThreshold=20m`.
- It also wraps the small dimension side with `broadcast(SyntheticData.dim(spark))`.

### 05_data_skew

Problem: one hot key dominates the join.

Baseline:

```bash
./scripts/run-case.sh 05_data_skew baseline
```

Inspect:

- Stages tab.
- SQL tab.
- Task table inside the slow stage.

Expected baseline evidence:

- Task durations are uneven.
- A small number of tasks run much longer than the rest.

Code-level cause:

- In `DataSkewCase.runBaseline`, `SyntheticData.skew(spark)` contains a dominant hot key.
- AQE skew join handling is disabled with `spark.sql.adaptive.skewJoin.enabled=false`.
- The join runs on `join_key`, so the hot key can concentrate work into a small number of tasks.

Optimized:

```bash
./scripts/run-case.sh 05_data_skew optimized
```

Verify:

- Task duration distribution is less uneven.
- The optimized source code salts the hot key.

Code-level fix:

- `DataSkewCase.runOptimized` enables AQE skew handling.
- It adds a `salt` column to the hot-key records on the left side.
- It expands the matching hot-key row on the right side with `explode(sequence(0, 15))`.
- The join changes from `join_key` to `join_key, salt`, spreading the hot-key work across more tasks.

### 06_small_files

Problem: many small files create many small scan tasks.

Baseline:

```bash
./scripts/run-case.sh 06_small_files baseline
```

Inspect:

- Jobs tab.
- Stages tab.

Expected baseline evidence:

- Many short tasks.
- Input partition count is higher than needed for the data size.

Code-level cause:

- In `SmallFilesCase.runBaseline`, Spark reads `data/generated/small_files` directly as JSON.
- The generated dataset intentionally contains many tiny files, so Spark creates many small scan tasks.

Optimized:

```bash
./scripts/run-case.sh 06_small_files optimized
```

Verify:

- The optimized run compacts to Parquet under `tmp/`.
- Downstream input partition count is lower.

Code-level fix:

- `SmallFilesCase.runOptimized` reads the small JSON files once.
- It writes a compacted Parquet dataset with `raw.coalesce(8).write.parquet(...)`.
- It then reads the compacted Parquet path for downstream processing.

### 07_too_few_partitions

Problem: too few partitions underuse available workers.

Baseline:

```bash
./scripts/run-case.sh 07_too_few_partitions baseline
```

Inspect:

- Executors tab.
- Stages tab.

Expected baseline evidence:

- Very few tasks.
- Executors are not used evenly.

Code-level cause:

- In `TooFewPartitionsCase.runBaseline`, `spark.range(..., 1)` creates a single input partition.
- The following transformations cannot use the available workers effectively because there is too little parallel work.

Optimized:

```bash
./scripts/run-case.sh 07_too_few_partitions optimized
```

Verify:

- More tasks are available.
- Executors show more activity.

Code-level fix:

- `TooFewPartitionsCase.runOptimized` starts the range with `16` partitions and calls `repartition(16)`.
- The same computation now has enough tasks to use both workers.

### 08_too_many_partitions

Problem: too many partitions create scheduler overhead.

Baseline:

```bash
./scripts/run-case.sh 08_too_many_partitions baseline
```

Inspect:

- Jobs tab.
- Stages tab.

Expected baseline evidence:

- Hundreds of tiny tasks.
- Task scheduling overhead dominates useful work.

Code-level cause:

- In `TooManyPartitionsCase.runBaseline`, a small `30000` row range is forced into `400` partitions.
- Spark spends visible time scheduling many tiny tasks with little useful work per task.

Optimized:

```bash
./scripts/run-case.sh 08_too_many_partitions optimized
```

Verify:

- Fewer tasks.
- More reasonable partition count for the data size.

Code-level fix:

- `TooManyPartitionsCase.runOptimized` starts with fewer partitions and applies `coalesce(12)`.
- This keeps enough parallelism for the lab cluster while reducing scheduler overhead.

### 09_spill

Problem: wide rows and low partition count can create spill.

Baseline:

```bash
./scripts/run-case.sh 09_spill baseline
```

Inspect:

- Stages tab.
- Executors tab.

Expected baseline evidence:

- Look for memory spill, disk spill, long tasks or memory pressure.
- Spill visibility depends on machine resources.

Code-level cause:

- In `SpillCase.runBaseline`, `spark.sql.shuffle.partitions` is reduced to `4`.
- The DataFrame creates wide string payloads, repartitions by a high-cardinality key and sorts within partitions.
- This intentionally increases per-task memory pressure.

Optimized:

```bash
./scripts/run-case.sh 09_spill optimized
```

Verify:

- Rows are narrower.
- Partitioning is more reasonable.
- Spill or memory pressure should be lower if it appeared in baseline.

Code-level fix:

- `SpillCase.runOptimized` increases shuffle partitions to `24`.
- It replaces the wide string payload with a narrow numeric metric and lowers aggregation cardinality.

### 10_cache_misuse

Problem: caching data without enough reuse wastes memory.

Baseline:

```bash
./scripts/run-case.sh 10_cache_misuse baseline
```

Inspect:

- Storage tab while the app is paused.
- Executors tab.

Expected baseline evidence:

- Storage tab shows cached data.
- The cached data is not reused enough to justify the memory.

Code-level cause:

- In `CacheMisuseCase.runBaseline`, a wide DataFrame is persisted with `StorageLevel.MEMORY_AND_DISK`.
- The cached DataFrame is materialized and then used only once downstream, so the Storage tab shows memory usage without a real reuse benefit.

Optimized:

```bash
./scripts/run-case.sh 10_cache_misuse optimized
```

Verify:

- Storage tab is empty or much quieter.

Code-level fix:

- `CacheMisuseCase.runOptimized` removes the unnecessary `persist`.
- The aggregation runs directly from the source DataFrame.

### 11_udf_cost

Problem: a UDF is used where built-in Spark SQL functions are enough.

Baseline:

```bash
./scripts/run-case.sh 11_udf_cost baseline
```

Inspect:

- SQL tab.

Expected baseline evidence:

- The plan includes UDF-related expressions.

Code-level cause:

- In `UdfCostCase.runBaseline`, simple even/odd labeling is implemented with a Scala UDF.
- The logic is easy, but the UDF makes the SQL plan less optimizer-friendly than native Spark SQL expressions.

Optimized:

```bash
./scripts/run-case.sh 11_udf_cost optimized
```

Verify:

- The SQL plan uses built-in conditional expressions.

Code-level fix:

- `UdfCostCase.runOptimized` replaces the UDF with `when(...).otherwise(...)`.
- Catalyst can represent the logic directly in the SQL physical plan.

### 12_aqe_comparison

Problem: Adaptive Query Execution is disabled.

Baseline:

```bash
./scripts/run-case.sh 12_aqe_comparison baseline
```

Inspect:

- SQL tab.
- Stages tab.

Expected baseline evidence:

- Non-adaptive physical plan.

Code-level cause:

- In `AqeComparisonCase.runBaseline`, `spark.sql.adaptive.enabled=false`.
- Broadcast is also disabled and shuffle partitions are set to `64`, making the query shape easier to compare without adaptive changes.

Optimized:

```bash
./scripts/run-case.sh 12_aqe_comparison optimized
```

Verify:

- SQL plan shows adaptive planning evidence.
- Stage behavior may show coalesced or adapted shuffle behavior.

Code-level fix:

- `AqeComparisonCase.runOptimized` runs the same query with `spark.sql.adaptive.enabled=true`.
- The intended comparison is the same business logic with adaptive planning enabled.

### 13_task_failure_retry

Problem: a controlled transient failure creates a retry.

Baseline:

```bash
./scripts/run-case.sh 13_task_failure_retry baseline
```

Inspect:

- Jobs tab.
- Stages tab.
- Executors tab.

Expected baseline evidence:

- One failed task attempt.
- A later retry succeeds.

Code-level cause:

- In `TaskFailureRetryCase.runBaseline`, partition `3` throws a controlled exception on attempt `0`.
- A marker file under `/opt/spark-checkpoints` prevents repeated failure, so Spark retries once and the job can still finish.

Optimized:

```bash
./scripts/run-case.sh 13_task_failure_retry optimized
```

Verify:

- No controlled retry.
- Input is validated before processing.

Code-level fix:

- `TaskFailureRetryCase.runOptimized` creates one bad record but validates `raw_value` with a numeric regex before casting.
- Bad input is filtered before the aggregation, so no task retry is expected.

### 14_config_validation

Problem: users assume Spark config is active without checking.

Baseline:

```bash
./scripts/run-case.sh 14_config_validation baseline
```

Inspect:

- Environment tab.

Expected baseline evidence:

- Spark Properties show the actual values.
- Terminal output prints the same key configuration values.

Code-level cause:

- In `ConfigValidationCase.runBaseline`, the case prints selected values from `SparkConf` and `spark.conf`.
- This is not a performance fix by itself; it proves that the Environment tab is the source of truth for active Spark properties.

Optimized:

```bash
./scripts/run-case.sh 14_config_validation optimized
```

Verify:

- The script passes explicit `spark-submit` config.
- Environment tab confirms the applied values.

Code-level fix:

- The Scala code is intentionally the same for baseline and optimized.
- The difference comes from `scripts/run-case.sh`, which passes explicit `--conf` values for this optimized mode.
- The verification happens in the Spark UI Environment tab.

## 8. Streaming Setup

Streaming cases require Redpanda. Start the streaming profile:

```bash
./scripts/up-streaming.sh
```

Create topics:

```bash
./scripts/create-topics.sh
```

Produce deterministic input:

```bash
./scripts/produce-streaming-data.sh
```

Streaming topics:

- `spark-ui-lab-input`
- `spark-ui-lab-output`
- `spark-ui-lab-stateful-input`

If a streaming case fails because of missing topics or old checkpoints, run:

```bash
./scripts/reset-streaming.sh
./scripts/produce-streaming-data.sh
```

## 9. Streaming Cases

### 15_structured_streaming_backlog

Problem: processing is intentionally slower than input.

Baseline:

```bash
./scripts/run-case.sh 15_structured_streaming_backlog baseline
```

Inspect:

- Structured Streaming tab.

Expected baseline evidence:

- Batch duration can exceed trigger interval.
- Processed rows/sec can lag input rows/sec.

Code-level cause:

- In `StructuredStreamingBacklogCase.runBaseline`, Kafka input uses `maxOffsetsPerTrigger=500`.
- The `foreachBatch` block intentionally sleeps for `3500` ms while the trigger interval is `2 seconds`.

Optimized:

```bash
./scripts/run-case.sh 15_structured_streaming_backlog optimized
```

Verify:

- Lower `maxOffsetsPerTrigger`.
- No artificial processing delay.
- Batch duration should be steadier.

Code-level fix:

- `StructuredStreamingBacklogCase.runOptimized` lowers `maxOffsetsPerTrigger` to `150`.
- It removes the artificial `Thread.sleep`, so each trigger has a better chance to complete predictably.

### 16_stateful_streaming

Problem: stateful aggregation grows state without a bounded strategy.

Baseline:

```bash
./scripts/run-case.sh 16_stateful_streaming baseline
```

Inspect:

- Structured Streaming tab.
- State operator metrics.

Expected baseline evidence:

- State rows accumulate for the aggregation.

Code-level cause:

- In `StatefulStreamingCase.runBaseline`, the stream groups by a `10 minutes` window and `key`.
- There is no watermark, and the query uses `complete` output mode, so state can grow without a bounded event-time cleanup strategy.

Optimized:

```bash
./scripts/run-case.sh 16_stateful_streaming optimized
```

Verify:

- Watermark is applied.
- Window size is smaller.
- State behavior is easier to bound.

Code-level fix:

- `StatefulStreamingCase.runOptimized` adds `withWatermark("event_ts", "1 minute")`.
- It changes the window to `1 minute` and uses `append` output mode.
- This demonstrates bounded state design instead of unbounded complete-mode accumulation.

### 17_real_time_mode

Problem: compare micro-batch with Spark 4.1 real-time mode for a stateless query.

Baseline:

```bash
./scripts/run-case.sh 17_real_time_mode baseline
```

Inspect:

- Structured Streaming tab.

Code-level baseline:

- In `RealTimeModeCase.runBaseline`, the stateless Kafka-to-Kafka query uses standard micro-batch execution with `Trigger.ProcessingTime("5 seconds")`.

Advanced:

```bash
./scripts/run-case.sh 17_real_time_mode advanced
```

`optimized` is also accepted as an alias for `advanced`.

Verify:

- The advanced run uses Spark 4.1 real-time trigger where supported.
- Do not claim fixed latency. Compare query progress evidence only.

Code-level advanced mode:

- `RealTimeModeCase.runOptimized` sets `spark.sql.streaming.realTimeMode.minBatchDuration=5s`.
- It uses `Trigger.RealTime("5 seconds")` for the same stateless Kafka-to-Kafka pattern.
- `optimized` is accepted as an alias for `advanced`, but the documentation uses `advanced` because this is a Spark 4.1 capability demonstration, not a universal performance fix.

## 10. Metrics Export

After running a case:

```bash
./scripts/export-metrics.sh 03_shuffle_explosion optimized
```

This writes a minimal History Server REST export to `metrics/`.

The exporter intentionally captures the application index only. Use the app id in the JSON for deeper manual API calls.

## 11. Cleanup

Stop containers:

```bash
./scripts/down.sh
```

Clean generated artifacts:

```bash
./scripts/clean.sh
```

`clean.sh` removes:

- `data/generated/`
- files under `metrics/`
- files under `tmp/`
- Spark checkpoints
- Spark warehouse data

It does not delete source files.

## 12. When Something Is Confusing

Use this order:

1. Re-read the case section.
2. Open [Code Execution Map](05-code-execution-map.md).
3. Open the Scala source file for the case.
4. Run baseline again.
5. Inspect only the listed UI tabs.
6. Run optimized again.
7. Compare the same tabs.
8. Check [Troubleshooting](04-troubleshooting.md).
