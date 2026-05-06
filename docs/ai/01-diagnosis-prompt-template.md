# Diagnosis Prompt Template

```text
You are helping me diagnose an Apache Spark performance problem.
Do not guess.
Use only the evidence I provide.

Case:
<case_id>

Mode:
baseline | optimized

Spark UI evidence:
- Jobs:
- Stages:
- SQL plan:
- Executors:
- Storage:
- Environment:
- Structured Streaming:

Task:
1. Identify the most likely issue.
2. Explain which evidence supports the diagnosis.
3. Identify the most relevant Spark UI tab.
4. Suggest the fix.
5. Explain what should improve after optimization.
```
