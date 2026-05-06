# AGENTS.md

## Purpose

This repository is a reproducible Apache Spark UI troubleshooting lab. It teaches learners to diagnose Spark performance problems through Spark Web UI evidence, not through synthetic benchmark claims.

## Coding Conventions

- Keep Scala code minimal and readable.
- Prefer Spark SQL/DataFrame APIs and built-in functions.
- Avoid unnecessary abstractions and framework noise.
- Use deterministic synthetic data.
- Keep data sizes laptop-friendly but large enough to make UI symptoms visible.
- Every Spark application name must include the case id and mode.
- Every case must set a clear Spark job group through the dispatcher.
- Every case must pause before exit unless `LAB_AUTO_EXIT=true`.

## Build

```bash
./scripts/up.sh
./scripts/build.sh
```

SBT runs inside `spark-client`; no local SBT or Scala installation is required.

## Run

```bash
./scripts/generate-data.sh
./scripts/run-case.sh 01_too_many_actions baseline
./scripts/run-case.sh 01_too_many_actions optimized
```

Makefile shortcuts call scripts only:

```bash
make run CASE=05_data_skew MODE=baseline
```

## Streaming

```bash
./scripts/up-streaming.sh
./scripts/create-topics.sh
./scripts/produce-streaming-data.sh
./scripts/run-case.sh 15_structured_streaming_backlog baseline
```

Redpanda must remain optional and profile-gated under `streaming`.

## Do Not

- Do not add notebooks as the main interface.
- Do not add DStreams.
- Do not add cloud or Databricks-specific dependencies.
- Do not require local Scala or SBT.
- Do not add AI runtime code, API keys, embeddings, RAG, vector databases or agents.
- Do not make Redpanda mandatory for batch cases.
- Do not promise fixed latency numbers.

## AI Scope

AI is documentation-only. Prompt templates help learners reason over evidence they manually collect from Spark UI. Do not add LLM calls or automatic diagnosis.

## Definition Of Done

- Default Docker Compose services start without Redpanda.
- Scala project builds inside `spark-client`.
- Data generation succeeds.
- Batch cases run without Redpanda.
- Streaming cases require the `streaming` profile.
- Event logs are visible in Spark History Server.
- Docs match actual scripts and commands.
