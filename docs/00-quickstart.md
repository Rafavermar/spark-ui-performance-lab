# Quickstart

## Script Path

```bash
cp .env.example .env
./scripts/up.sh
./scripts/build.sh
./scripts/generate-data.sh
./scripts/run-case.sh 01_too_many_actions baseline
./scripts/run-case.sh 01_too_many_actions optimized
```

Open:

- Spark Master UI: <http://localhost:8080>
- Live Spark Application UI: <http://localhost:4040> while the case is paused
- Spark History Server: <http://localhost:18080> after the application exits

## Makefile Path

```bash
make up
make build
make generate-data
make run CASE=01_too_many_actions MODE=baseline
make run CASE=01_too_many_actions MODE=optimized
```

The Makefile is convenience only. Scripts are the source of truth.

## What To Expect

For `01_too_many_actions baseline`, the Jobs tab should show several jobs from repeated actions over similar lineage. For `optimized`, the number of actions and jobs should be lower because the summary is computed in one pass.

If the live UI is missed, open the same execution in Spark History Server.

Before reading or changing a case, open [Code Execution Map](05-code-execution-map.md) to see the exact Scala object and source file that runs. For the complete visual flow, use [Lab Flow Tree](06-lab-flow-tree.md).

If Spark configuration feels unclear, read [Spark Configuration Guide](08-spark-configuration.md) before running cases. It explains what is set globally, what is passed by scripts and what each case overrides.

## Streaming Minimal Path

```bash
./scripts/up-streaming.sh
./scripts/create-topics.sh
./scripts/produce-streaming-data.sh
./scripts/run-case.sh 15_structured_streaming_backlog baseline
```

Open the Structured Streaming tab while the query is running.
