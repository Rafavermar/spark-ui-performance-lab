# Baseline Vs Optimized Comparison Prompt

```text
Compare this baseline and optimized Spark execution.
Use only the evidence provided.

Case:
<case_id>

Baseline evidence:
...

Optimized evidence:
...

Focus on:
- Jobs
- Stages
- Shuffle read/write
- Task duration distribution
- SQL physical plan
- Spill
- Executor usage
- Storage usage
- Streaming metrics if applicable

Explain:
1. What changed.
2. Why it improved.
3. Which Spark UI evidence proves the improvement.
4. What remaining bottlenecks may exist.
```
