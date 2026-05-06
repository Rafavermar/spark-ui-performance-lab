# Spark UI Map

## Jobs

Shows Spark jobs created by actions. Use it to diagnose repeated actions, failed jobs, retry patterns and jobs that finish too quickly to inspect elsewhere.

Cases: `01`, `02`, `06`, `08`, `13`.

## Stages

Shows stage DAGs, task counts, task duration distribution, shuffle read/write, spill and failed task attempts. Use it for skew, partitioning, shuffle explosion, spill and retries.

Cases: `01`, `02`, `03`, `05`, `06`, `07`, `08`, `09`, `12`, `13`.

## Storage

Shows cached or persisted DataFrames/RDDs. Use it to verify whether persistence helps or whether memory is wasted.

Cases: `02`, `10`.

## Environment

Shows actual Spark properties, JVM information and classpath evidence. Use it to verify what config Spark actually received.

Cases: `14`.

## Executors

Shows executor memory, task totals, failed tasks, shuffle, storage and GC-related evidence. Use it to detect underuse, spill pressure and task retry impact.

Cases: `07`, `09`, `10`, `13`.

## SQL

Shows SQL/DataFrame query plans and execution metrics. Use it to inspect Exchange, SortMergeJoin, BroadcastHashJoin, AdaptiveSparkPlan, UDF expressions and physical plan shape.

Cases: `03`, `04`, `05`, `11`, `12`.

## Structured Streaming

Shows streaming query progress, input rows/sec, processed rows/sec, batch duration, state operator metrics and query status.

Cases: `15`, `16`, `17`.

## History Server

Reads persisted Spark event logs after applications exit. Use it whenever the live UI was missed or when comparing baseline and optimized runs after both complete.

All cases write event logs to the shared `spark-events` Docker volume.

## REST API Metrics

`./scripts/export-metrics.sh <case_id> <mode>` exports the History Server applications REST index to `metrics/`. This is intentionally minimal; use the exported app id for deeper manual REST calls if needed.

## Excluded Tabs

The Streaming/DStreams tab is intentionally excluded because DStreams are legacy and the lab uses Structured Streaming only.

The JDBC/ODBC Server tab is not part of the initial lab. It can be added later only if a Spark Thrift Server extension is introduced.
