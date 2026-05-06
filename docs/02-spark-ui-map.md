# Spark UI Map

Use this page as the UI vocabulary for the lab. Each case tells you which tabs and drilldowns matter. Do not inspect every metric in every case; Spark UI is useful when you read it with a question in mind.

Spark UI has built-in help in many tables. Hover over column titles such as task time, scheduler delay, shuffle read, spill, storage memory or locality to see a short explanation from Spark itself. Use those hover cards as first-line help while this guide explains how each metric fits the lab cases.

## Jobs

Shows Spark jobs created by actions. Use it to diagnose repeated actions, failed jobs, retry patterns and jobs that finish too quickly to inspect elsewhere.

Cases: `01`, `02`, `06`, `08`, `13`.

What to read:

- `Completed Jobs`: how many Spark jobs the application created.
- `Job Group`: confirms you are looking at the selected case and mode.
- `Associated SQL Query`: links a DataFrame action to the SQL/DataFrame tab.
- `Completed Stages` and `Skipped Stages`: shows whether this job executed new stages or reused already completed stage output.
- `Event Timeline`: useful for seeing bursts of many small jobs or retries over time.

Common interpretation:

- One Spark application can create many jobs. A job is usually triggered by an action such as `count`, `collect`, `write` or `foreachBatch`.
- Skipped stages are not failed stages. They usually mean Spark found already completed stage output it could reuse inside the same application.
- `Scheduling Mode: FIFO` means jobs are scheduled in submission order. This is the default in this lab. It can be changed with Spark scheduling pools, but this lab does not tune it because the cases focus on query and data behavior.

## Stages

Shows stage DAGs, task counts, task duration distribution, shuffle read/write, spill and failed task attempts. Use it for skew, partitioning, shuffle explosion, spill and retries.

Cases: `01`, `02`, `03`, `05`, `06`, `07`, `08`, `09`, `12`, `13`.

What to read:

- `Tasks: Succeeded/Total`: tells you the amount of parallel work.
- `Duration`: stage wall-clock duration.
- `Shuffle Read` and `Shuffle Write`: evidence of data movement between stages.
- Stage detail page: task-level percentiles, locality, spill, GC, scheduler delay and failed attempts.
- `DAG Visualization`: shows stage dependencies. Use it when the problem is about repeated lineage, shuffle boundaries, joins or retries.

Stage detail metrics:

- `Scheduler Delay`: time waiting before task execution. High values can point to scheduling pressure or too many tiny tasks.
- `Task Deserialization Time`: time to deserialize task code and metadata on the executor.
- `Shuffle Read Fetch Wait Time`: time waiting for remote shuffle blocks.
- `Shuffle Remote Reads`: evidence that shuffle data came from other executors.
- `Result Serialization Time` and `Getting Result Time`: driver/result transfer overhead.
- `Peak Execution Memory`: memory used by execution operators such as sort, aggregate or join.
- `Memory Spill` and `Disk Spill`: data moved out of memory during execution. These are central in case `09`.
- `Locality Level`: where the task ran relative to its data. `NODE_LOCAL` means the task ran on the same worker node as the data; in this Docker lab it is usually not the main bottleneck.

## Storage

Shows cached or persisted DataFrames/RDDs. Use it to verify whether persistence helps or whether memory is wasted.

Cases: `02`, `10`.

## Environment

Shows actual Spark properties, JVM information and classpath evidence. Use it to verify what config Spark actually received.

Cases: `14`.

## Executors

Shows executor memory, task totals, failed tasks, shuffle, storage and GC-related evidence. Use it to detect underuse, spill pressure and task retry impact.

Cases: `07`, `09`, `10`, `13`.

What to read:

- `Cores`: available parallelism per executor.
- `Active Tasks`: currently running tasks while the app is live.
- `Complete Tasks` and `Total Tasks`: whether work was distributed across executors.
- `Failed Tasks`: retry/failure evidence.
- `Task Time (GC Time)`: total executor task time and the portion spent in garbage collection.
- `Storage Memory`: cached/persisted data footprint. Use this with the Storage tab.
- `Shuffle Read` and `Shuffle Write`: executor-level data movement.
- `Logs`: useful when a task fails and you need stdout/stderr.

How to interpret values:

- Exact task time, GC time, storage memory and shuffle bytes are not portable benchmark numbers.
- Distribution is more important than the exact value. For example, case `07` should show underuse with too few tasks; case `10` should show storage memory when unnecessary cache is used; case `13` should show failed task evidence.
- A small non-zero `Storage Memory` value can appear even when the primary Storage tab has no meaningful cached dataset. Use the Storage tab as the source of truth for persisted DataFrames.

## SQL

Shows SQL/DataFrame query plans and execution metrics. Use it to inspect Exchange, SortMergeJoin, BroadcastHashJoin, AdaptiveSparkPlan, UDF expressions and physical plan shape.

Cases: `03`, `04`, `05`, `11`, `12`.

DataFrame API still appears here. You do not need to write SQL for Spark to create SQL/DataFrame executions.

What to read:

- Query list: one or more SQL/DataFrame executions created by actions.
- `Associated Jobs`: which jobs belong to the query.
- `Plan Visualization`: graphical operator tree with runtime metrics.
- `Plan Details`: physical plan text. Use it to search for operators.

Useful operators:

- `Range`: synthetic source used by many lab cases.
- `Filter`: rows removed before later work.
- `Project`: selected or computed columns.
- `HashAggregate`: aggregation operator.
- `Exchange`: shuffle boundary. This is a key signal in shuffle and join cases.
- `AQEShuffleRead`: Adaptive Query Execution reading an adapted shuffle.
- `AdaptiveSparkPlan`: AQE is active for the query.
- `SortMergeJoin`: shuffle join; important in case `04`.
- `BroadcastHashJoin` and `BroadcastExchange`: broadcast join evidence; important in case `04`.
- UDF-related expressions: important in case `11`.

When to use SQL:

- Optional in `01` and `02`: open one query to connect DataFrame actions to SQL plans, but do not over-analyze every operator.
- Required in `03`, `04`, `05`, `11` and `12`: the physical plan is part of the diagnosis.
- Helpful in `09`: use it if you want to connect spill or memory pressure to sort/aggregate operators.

## Structured Streaming

Shows streaming query progress, input rows/sec, processed rows/sec, batch duration, state operator metrics and query status.

Cases: `15`, `16`, `17`.

## History Server

Reads persisted Spark event logs after applications exit. Use it whenever the live UI was missed or when comparing baseline and optimized runs after both complete.

All cases write event logs to the shared `spark-events` Docker volume.

## REST API Metrics

`./scripts/export-metrics.sh <case_id> <mode>` exports the History Server applications REST index to `metrics/`. This is intentionally minimal; use the exported app id for deeper manual REST calls if needed.

## Reproducibility Of Metrics

This lab is designed to reproduce the same diagnosis flow, not the same exact numbers.

Stable across machines:

- Case and mode names.
- Required UI tabs.
- Presence or absence of important operators.
- Relative patterns such as fewer jobs, fewer tasks, visible cache, broadcast join, failed retry, state metrics or AQE evidence.

Variable across machines:

- Duration.
- Task time.
- GC time.
- Scheduler delay.
- Shuffle byte counts.
- Spill byte counts.
- Peak memory.
- Streaming rows/sec.

Use exact numbers only as local evidence for your own run. In public documentation or screenshots, describe patterns such as "more jobs than optimized", "Storage tab contains the persisted DataFrame" or "baseline has many more tiny tasks" instead of claiming universal timings.

## Drilldown Rules

Use this rule of thumb while running the lab:

| Evidence needed | Open this |
|---|---|
| Too many actions or retries | Jobs tab, Event Timeline and one Job detail page |
| Repeated lineage or skipped stages | Jobs, Stages and optional DAG Visualization |
| Shuffle, join or AQE behavior | SQL query, Plan Visualization, Plan Details and Stages |
| Skew or partition count | Stages, stage detail task table and percentile metrics |
| Cache or persist behavior | Storage tab and Executors storage memory |
| Memory pressure or spill | Stage detail metrics and Executors |
| Configuration truth | Environment tab |
| Streaming rate or state | Structured Streaming query progress and state operator metrics |

## Excluded Tabs

The Streaming/DStreams tab is intentionally excluded because DStreams are legacy and the lab uses Structured Streaming only.

The JDBC/ODBC Server tab is not part of the initial lab. It can be added later only if a Spark Thrift Server extension is introduced.
