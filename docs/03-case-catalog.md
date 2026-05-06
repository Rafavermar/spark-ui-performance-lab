# Case Catalog

| Case | Problem | UI evidence | Fix | Tabs |
|---|---|---|---|---|
| `01_too_many_actions` | Repeated actions | Multiple jobs for similar lineage | Consolidate actions | Jobs, Stages |
| `02_recomputation` | Reused lineage recomputed | Repeated stages, empty Storage | Persist reused intermediate | Jobs, Stages, Storage |
| `03_shuffle_explosion` | Wide shuffle | Exchange, high shuffle read/write | Filter/project early, tune partitions | SQL, Stages |
| `04_broadcast_join` | Missing broadcast | SortMergeJoin and Exchanges | Broadcast small side | SQL, Stages |
| `05_data_skew` | Hot key | Straggler tasks | Salt hot key, enable skew handling | Stages, SQL |
| `06_small_files` | Many tiny files | Many short input tasks | Compact files | Jobs, Stages |
| `07_too_few_partitions` | Low parallelism | Few tasks, underused executors | Repartition reasonably | Executors, Stages |
| `08_too_many_partitions` | Scheduler overhead | Many tiny tasks | Coalesce/repartition sensibly | Jobs, Stages |
| `09_spill` | Memory pressure | Spill metrics, long tasks | Narrow rows, better partitioning | Stages, Executors |
| `10_cache_misuse` | Wasteful cache | Storage memory without reuse | Cache only reused data | Storage, Executors |
| `11_udf_cost` | UDF blocks optimizer-friendly logic | UDF in SQL plan | Use built-in functions | SQL |
| `12_aqe_comparison` | AQE disabled | Non-adaptive plan | Enable AQE | SQL, Stages |
| `13_task_failure_retry` | Transient failure | Failed task attempt and retry | Validate/filter bad input | Jobs, Stages, Executors |
| `14_config_validation` | Misunderstood config | Environment tab values | Pass config explicitly | Environment |
| `15_structured_streaming_backlog` | Processing slower than input | Batch duration, input vs processed rates | Tune trigger/rate/work | Structured Streaming |
| `16_stateful_streaming` | Unbounded state | State operator metrics | Watermark and bounded windows | Structured Streaming |
| `17_real_time_mode` | Micro-batch vs real-time | Query trigger/progress evidence | Use Spark 4.1 real-time mode where supported | Structured Streaming |
