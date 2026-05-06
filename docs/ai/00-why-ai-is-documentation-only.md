# Why AI Is Documentation-Only

This project uses AI only as optional learning support for LLM-assisted reasoning over Spark UI evidence.

The lab does not call any LLM API. It does not require API keys. It does not use RAG, embeddings, vector search or agents.

The reason is focus: adding AI runtime components would shift the proof of concept away from Spark UI diagnosis and toward application architecture. The lab intentionally keeps runtime behavior deterministic, reproducible and vendor-neutral.

Learners should inspect Spark UI first, collect concrete evidence and then optionally use the prompt templates to structure reasoning. AI should not replace understanding Spark internals.

When using an LLM, provide specific observations from Jobs, Stages, SQL, Storage, Executors, Environment or Structured Streaming. Do not ask the model to guess from a case name alone.
