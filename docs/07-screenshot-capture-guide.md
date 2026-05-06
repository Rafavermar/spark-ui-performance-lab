# Screenshot Capture Guide

This guide helps you capture evidence for articles, talks or internal documentation. It does not include screenshots in the repository.

Use it together with the [Runbook](01-runbook.md), [Spark UI Map](02-spark-ui-map.md) and [Case Catalog](03-case-catalog.md).

## Capture Rules

- Capture evidence, not decoration.
- Always capture the case id and mode in the Spark application name when visible.
- Capture baseline and optimized from the same UI tab whenever possible.
- Do not claim fixed timing or latency numbers. Use UI evidence such as job count, plan shape, task count, shuffle, storage or streaming progress.
- Prefer History Server screenshots for stable completed applications. Use live UI screenshots only when the case needs active streaming or cached Storage visibility.

## Minimum Article Capture Set

### Cluster And Setup

1. Open <http://localhost:8080>.
2. Capture Spark Master UI showing two workers.
3. Open <http://localhost:18080>.
4. Capture History Server after at least one baseline and optimized run.

### Case 01: Too Many Actions

Baseline:

```bash
./scripts/run-case.sh 01_too_many_actions baseline
```

Capture:

- Live UI or History Server.
- Jobs tab.
- The list of jobs showing repeated actions.

Optimized:

```bash
./scripts/run-case.sh 01_too_many_actions optimized
```

Capture:

- Jobs tab again.
- Fewer jobs or simpler job pattern.

### Case 03: Shuffle Explosion

Baseline:

```bash
./scripts/run-case.sh 03_shuffle_explosion baseline
```

Capture:

- SQL tab.
- Physical plan with `Exchange`.
- Stages tab with shuffle read/write metrics.

Optimized:

```bash
./scripts/run-case.sh 03_shuffle_explosion optimized
```

Capture:

- SQL tab showing fewer grouped columns and fewer shuffle partitions.
- Stages tab for comparison.

### Case 04: Broadcast Join

Baseline:

```bash
./scripts/run-case.sh 04_broadcast_join baseline
```

Capture:

- SQL tab showing `SortMergeJoin` and `Exchange`.

Optimized:

```bash
./scripts/run-case.sh 04_broadcast_join optimized
```

Capture:

- SQL tab showing broadcast join evidence.

### Case 05: Data Skew

Baseline:

```bash
./scripts/run-case.sh 05_data_skew baseline
```

Capture:

- Stages tab.
- Task table sorted by duration.
- Evidence of uneven task durations.

Optimized:

```bash
./scripts/run-case.sh 05_data_skew optimized
```

Capture:

- Same stage view.
- More balanced task duration distribution.

### Case 10: Cache Misuse

Baseline:

```bash
./scripts/run-case.sh 10_cache_misuse baseline
```

Capture:

- Storage tab while the app is paused.
- Cached DataFrame memory/storage usage.

Optimized:

```bash
./scripts/run-case.sh 10_cache_misuse optimized
```

Capture:

- Storage tab with no unnecessary cached dataset.

### Case 15: Structured Streaming Backlog

Setup:

```bash
./scripts/up-streaming.sh
./scripts/create-topics.sh
./scripts/produce-streaming-data.sh
```

Baseline:

```bash
./scripts/run-case.sh 15_structured_streaming_backlog baseline
```

Capture:

- Structured Streaming tab while query is active.
- Batch duration.
- Input rows/sec.
- Processed rows/sec.

Optimized:

```bash
./scripts/run-case.sh 15_structured_streaming_backlog optimized
```

Capture:

- Same Structured Streaming metrics for comparison.

## Recommended Article Sequence

1. Spark Master UI: cluster is real and reproducible.
2. Case 01 Jobs tab: repeated actions.
3. Case 03 SQL tab: Exchange and shuffle.
4. Case 04 SQL tab: join strategy before and after.
5. Case 10 Storage tab: cache is visible and measurable.
6. Case 15 Structured Streaming tab: streaming evidence is different from batch evidence.
7. Closing screenshot: History Server with completed baseline and optimized applications.
