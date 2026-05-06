# Article Capture Plan

Private helper for article or video production. Do not link this file from `README.md`.

## Suggested Narrative

1. Explain the lab goal: learn Spark performance troubleshooting through Spark UI evidence.
2. Show one baseline problem.
3. Diagnose using Spark UI tabs.
4. Apply the fix.
5. Compare baseline vs optimized UI evidence.
6. Explain why AI is documentation-only and evidence-driven.

## Suggested Screenshots

- Spark Master UI showing the standalone cluster.
- Case `01` baseline Jobs tab with multiple jobs.
- Case `01` optimized Jobs tab with fewer jobs.
- Case `03` SQL plan showing Exchange.
- Case `04` baseline SortMergeJoin and optimized BroadcastHashJoin.
- Case `05` Stages task duration distribution for skew.
- Case `10` Storage tab showing cache misuse.
- Case `14` Environment tab showing actual Spark properties.
- Case `15` Structured Streaming progress with backlog symptoms.

## Article Sections

- Why Spark UI evidence matters.
- Reproducible local architecture.
- Baseline run.
- UI diagnosis.
- Optimized run.
- Before/after comparison.
- Optional LLM-assisted reasoning over manually collected evidence.

## Before/After Ideas

- Number of Spark jobs.
- Shuffle read/write volume.
- Task duration distribution.
- Join operator type.
- Storage memory usage.
- State operator rows.

## Author Notes

- Avoid claiming exact timing improvements.
- Mention that screenshots are intentionally excluded from public docs.
- Keep Redpanda framed as optional infrastructure for streaming cases only.
