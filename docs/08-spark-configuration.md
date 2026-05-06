# Spark Configuration Guide

This page explains exactly where Spark configuration lives in the lab, which settings are global, which settings are changed per case and how to verify the final values in Spark UI.

The short version:

```text
.env / .env.example
  -> Docker Compose variables and pinned versions

docker-compose.yml
  -> services, ports, worker cores, worker memory, mounted volumes

conf/spark-defaults.conf
  -> default Spark properties loaded by every Spark application

scripts/run-case.sh and scripts/generate-data.sh
  -> spark-submit options for each execution

src/main/scala/lab/cases/*.scala
  -> case-specific overrides used to create or fix a symptom

Spark UI Environment tab
  -> source of truth for what the application actually received
```

## Why Configuration Matters

Spark performance diagnosis depends on separating three things:

1. Cluster shape: how many workers, cores and memory the lab starts with.
2. Global Spark defaults: event logs, History Server, shuffle partitions, AQE and UI retention.
3. Case behavior: code and configuration deliberately changed to create a baseline symptom or an optimized comparison.

If these are mixed together, the lab feels like magic. This document makes the layers explicit.

## 1. Version And Cluster Variables

File: `.env.example`

These values are copied to `.env` by the learner:

```bash
cp .env.example .env
```

Important variables:

| Variable | Default | Purpose |
|---|---:|---|
| `SPARK_VERSION` | `4.1.1` | Spark version downloaded into the Docker image. |
| `SCALA_VERSION` | `2.13.17` | Scala version used by SBT. |
| `SBT_VERSION` | `1.10.7` | SBT version installed in the container. |
| `JAVA_BASE_IMAGE` | `eclipse-temurin:17.0.13_11-jdk-jammy` | Java base image for Spark. |
| `REDPANDA_IMAGE` | `docker.redpanda.com/redpandadata/redpanda:v26.1.5` | Optional streaming broker image. |
| `SPARK_WORKER_CORES` | `2` | Cores per Spark worker container. |
| `SPARK_WORKER_MEMORY` | `2g` | Memory per Spark worker container. |
| `SPARK_DRIVER_MEMORY` | `1g` | Driver memory for demo jobs. |
| `SPARK_EXECUTOR_MEMORY` | `1g` | Executor memory for demo jobs. |

These values make the lab laptop-friendly. They are not benchmark recommendations.

## 2. Docker Compose Cluster Configuration

File: `docker-compose.yml`

The default profile starts:

- `spark-master`
- `spark-worker-1`
- `spark-worker-2`
- `spark-history-server`
- `spark-client`

The streaming profile also starts:

- `redpanda`

Worker configuration:

```yaml
--cores ${SPARK_WORKER_CORES:-2}
--memory ${SPARK_WORKER_MEMORY:-2g}
```

With defaults, the cluster has:

- 2 workers.
- 2 cores per worker.
- 4 total cores.
- 2 GiB memory per worker.
- 4 GiB total worker memory.

This is why Spark Master UI shows:

```text
Workers: 2 Alive
Cores in use: 4 Total
Memory in use: 4.0 GiB Total
```

Mounted volumes:

| Volume | Purpose |
|---|---|
| `spark-events` | Spark event logs for History Server. |
| `spark-warehouse` | Spark warehouse path if needed. |
| `spark-checkpoints` | Structured Streaming checkpoints and controlled failure markers. |
| `redpanda-data` | Redpanda data when streaming profile is active. |
| project directory | Mounted as `/workspace` in `spark-client` and workers. |

## 3. Global Spark Defaults

File: `conf/spark-defaults.conf`

These settings are mounted into `/opt/spark/conf/spark-defaults.conf` in the Spark containers. Every Spark application gets them unless a script or case overrides them.

Important defaults:

| Property | Default | Why it exists |
|---|---:|---|
| `spark.master` | `spark://spark-master:7077` | Submit to the standalone cluster. |
| `spark.eventLog.enabled` | `true` | Persist completed app evidence. |
| `spark.eventLog.dir` | `file:/opt/spark-events` | Shared event log volume. |
| `spark.history.fs.logDirectory` | `file:/opt/spark-events` | History Server reads the same location. |
| `spark.sql.adaptive.enabled` | `true` | AQE enabled by default unless a case disables it. |
| `spark.sql.shuffle.partitions` | `16` | Small but visible local shuffle default. |
| `spark.serializer` | `KryoSerializer` | Reasonable Spark serializer for the lab. |
| `spark.sql.streaming.metricsEnabled` | `true` | Show streaming metrics. |
| `spark.ui.retainedJobs` | `200` | Keep enough UI history for inspection. |
| `spark.ui.retainedStages` | `200` | Keep enough stage history. |
| `spark.sql.ui.retainedExecutions` | `200` | Keep enough SQL executions. |
| `spark.driver.host` | `spark-client` | Make client deploy mode work inside Docker networking. |
| `spark.driver.bindAddress` | `0.0.0.0` | Bind driver inside the container. |
| `spark.ui.port` | `4040` | Expose live Spark application UI. |
| `spark.executor.memory` | `1g` | Laptop-friendly executor memory. |
| `spark.driver.memory` | `1g` | Laptop-friendly driver memory. |

## 4. spark-submit Configuration

Files:

- `scripts/generate-data.sh`
- `scripts/run-case.sh`

The scripts are the source of truth for executing applications. They pass a small set of explicit `spark-submit` settings:

```bash
--master spark://spark-master:7077
--deploy-mode client
--conf spark.driver.host=spark-client
--conf spark.driver.bindAddress=0.0.0.0
--conf spark.ui.port=4040
```

These are infrastructure settings. They make the Docker network and live UI work consistently.

`scripts/run-case.sh` also adds this for streaming visibility:

```bash
--conf spark.sql.streaming.metricsEnabled=true
```

Case `14_config_validation` optimized mode intentionally passes extra config through `spark-submit`:

```bash
--conf spark.sql.shuffle.partitions=6
--conf spark.sql.adaptive.enabled=true
--conf spark.ui.retainedJobs=300
```

That case exists to teach learners to verify Spark configuration in the Environment tab.

## 5. Case-Specific Configuration Overrides

Most fixes are code or data-layout fixes, not global Spark tuning.

Some cases deliberately change Spark SQL configuration to make UI evidence visible or to compare behavior.

| Case | Baseline config changes | Optimized config changes | Why |
|---|---|---|---|
| `03_shuffle_explosion` | `spark.sql.shuffle.partitions=48` | `spark.sql.shuffle.partitions=12` | Make shuffle partition impact visible while also changing query shape. |
| `04_broadcast_join` | `spark.sql.autoBroadcastJoinThreshold=-1` | `spark.sql.autoBroadcastJoinThreshold=20m` plus broadcast hint | Show SortMergeJoin vs broadcast join. |
| `05_data_skew` | `spark.sql.adaptive.skewJoin.enabled=false`, `spark.sql.shuffle.partitions=24` | `spark.sql.adaptive.skewJoin.enabled=true`, `spark.sql.shuffle.partitions=24` | Compare skew handling while salting the hot key. |
| `09_spill` | `spark.sql.shuffle.partitions=4` | `spark.sql.shuffle.partitions=24` | Create and then reduce memory pressure. |
| `12_aqe_comparison` | `spark.sql.adaptive.enabled=false`, broadcast disabled, shuffle partitions `64` | `spark.sql.adaptive.enabled=true`, broadcast disabled, shuffle partitions `64` | Isolate AQE behavior. |
| `14_config_validation` | Prints active config | Passes explicit `spark-submit` config | Teach Environment tab validation. |
| `17_real_time_mode` | Standard micro-batch trigger | `spark.sql.streaming.realTimeMode.minBatchDuration=5s` and real-time trigger | Compare streaming execution mode where supported. |

Cases not listed above mainly change application logic, partitioning APIs, persistence, file layout, joins, UDF usage or streaming query design.

## 6. Fix Type By Case

| Case | Main fix type |
|---|---|
| `01_too_many_actions` | Code: reduce actions and compute summary together. |
| `02_recomputation` | Code: persist only reused intermediate data and unpersist later. |
| `03_shuffle_explosion` | Code plus config: filter/project early and reduce shuffle partitions. |
| `04_broadcast_join` | Code plus config: broadcast the small side. |
| `05_data_skew` | Code plus config: salt hot key and enable skew handling. |
| `06_small_files` | Data layout: compact small files. |
| `07_too_few_partitions` | Code: repartition to increase parallelism. |
| `08_too_many_partitions` | Code: coalesce/repartition to reduce tiny tasks. |
| `09_spill` | Code plus config: narrower rows and better partitioning. |
| `10_cache_misuse` | Code: remove unnecessary cache. |
| `11_udf_cost` | Code: replace UDF with built-in functions. |
| `12_aqe_comparison` | Config: compare AQE off vs on. |
| `13_task_failure_retry` | Code: validate/filter problematic input. |
| `14_config_validation` | Config: verify actual Spark properties. |
| `15_structured_streaming_backlog` | Streaming query design: tune input rate and remove artificial delay. |
| `16_stateful_streaming` | Streaming query design: watermark and bounded windows. |
| `17_real_time_mode` | Streaming config/API: compare trigger modes. |

## 7. How To Verify Configuration In Spark UI

For any completed or running application:

1. Open live UI at <http://localhost:4040> while the app is running, or History Server at <http://localhost:18080> after it finishes.
2. Open the application.
3. Go to the Environment tab.
4. Find Spark Properties.
5. Search for the property you care about.

Useful properties to check:

- `spark.master`
- `spark.app.name`
- `spark.eventLog.enabled`
- `spark.eventLog.dir`
- `spark.sql.adaptive.enabled`
- `spark.sql.shuffle.partitions`
- `spark.sql.autoBroadcastJoinThreshold`
- `spark.serializer`
- `spark.driver.host`
- `spark.executor.memory`
- `spark.driver.memory`

The Environment tab is the source of truth. If a value is not there, do not assume it was applied.

## 8. What Learners Should And Should Not Change

For the first pass through the lab:

- Do not change `.env`.
- Do not change `conf/spark-defaults.conf`.
- Do not change `docker-compose.yml`.
- Run the baseline and optimized commands exactly as documented.

After understanding the UI evidence:

- Change one setting at a time.
- Rerun the same case.
- Compare the same Spark UI tab.
- Record what changed and what did not.

This keeps the lab reproducible and prevents accidental tuning from hiding the intended symptom.
