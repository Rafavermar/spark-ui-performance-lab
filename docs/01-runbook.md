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

Diagnosis:

- Multiple actions are executed over similar lineage.
- Each action can trigger its own Spark job.

Optimized:

```bash
./scripts/run-case.sh 01_too_many_actions optimized
```

Verify:

- Jobs tab should be simpler.
- The optimized run computes summary metrics together.

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

Optimized:

```bash
./scripts/run-case.sh 02_recomputation optimized
```

Verify:

- Storage tab shows the persisted DataFrame while the app is paused.
- Later actions reuse the persisted result.

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

Optimized:

```bash
./scripts/run-case.sh 03_shuffle_explosion optimized
```

Verify:

- SQL scan reads fewer columns.
- The plan filters earlier.
- Shuffle partition count is lower.

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

Optimized:

```bash
./scripts/run-case.sh 04_broadcast_join optimized
```

Verify:

- Physical plan shows broadcast join evidence, such as `BroadcastHashJoin` or broadcast exchange.

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

Optimized:

```bash
./scripts/run-case.sh 05_data_skew optimized
```

Verify:

- Task duration distribution is less uneven.
- The optimized source code salts the hot key.

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

Optimized:

```bash
./scripts/run-case.sh 06_small_files optimized
```

Verify:

- The optimized run compacts to Parquet under `tmp/`.
- Downstream input partition count is lower.

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

Optimized:

```bash
./scripts/run-case.sh 07_too_few_partitions optimized
```

Verify:

- More tasks are available.
- Executors show more activity.

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

Optimized:

```bash
./scripts/run-case.sh 08_too_many_partitions optimized
```

Verify:

- Fewer tasks.
- More reasonable partition count for the data size.

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

Optimized:

```bash
./scripts/run-case.sh 09_spill optimized
```

Verify:

- Rows are narrower.
- Partitioning is more reasonable.
- Spill or memory pressure should be lower if it appeared in baseline.

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

Optimized:

```bash
./scripts/run-case.sh 10_cache_misuse optimized
```

Verify:

- Storage tab is empty or much quieter.

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

Optimized:

```bash
./scripts/run-case.sh 11_udf_cost optimized
```

Verify:

- The SQL plan uses built-in conditional expressions.

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

Optimized:

```bash
./scripts/run-case.sh 12_aqe_comparison optimized
```

Verify:

- SQL plan shows adaptive planning evidence.
- Stage behavior may show coalesced or adapted shuffle behavior.

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

Optimized:

```bash
./scripts/run-case.sh 13_task_failure_retry optimized
```

Verify:

- No controlled retry.
- Input is validated before processing.

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

Optimized:

```bash
./scripts/run-case.sh 14_config_validation optimized
```

Verify:

- The script passes explicit `spark-submit` config.
- Environment tab confirms the applied values.

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

Optimized:

```bash
./scripts/run-case.sh 15_structured_streaming_backlog optimized
```

Verify:

- Lower `maxOffsetsPerTrigger`.
- No artificial processing delay.
- Batch duration should be steadier.

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

Optimized:

```bash
./scripts/run-case.sh 16_stateful_streaming optimized
```

Verify:

- Watermark is applied.
- Window size is smaller.
- State behavior is easier to bound.

### 17_real_time_mode

Problem: compare micro-batch with Spark 4.1 real-time mode for a stateless query.

Baseline:

```bash
./scripts/run-case.sh 17_real_time_mode baseline
```

Inspect:

- Structured Streaming tab.

Advanced:

```bash
./scripts/run-case.sh 17_real_time_mode advanced
```

`optimized` is also accepted as an alias for `advanced`.

Verify:

- The advanced run uses Spark 4.1 real-time trigger where supported.
- Do not claim fixed latency. Compare query progress evidence only.

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
