# Case Catalog

| Case | Problem | UI evidence | Fix | Tabs | Drilldown |
|---|---|---|---|---|---|
| `01_too_many_actions` | Repeated actions | Multiple jobs for similar lineage | Consolidate actions | Jobs, Stages | Jobs table; Timeline optional; DAG optional |
| `02_recomputation` | Reused lineage recomputed | Repeated stages, empty Storage | Persist reused intermediate | Jobs, Stages, Storage | One Associated SQL Query; Storage required; Stage metrics light |
| `03_shuffle_explosion` | Wide shuffle | Exchange, high shuffle read/write | Filter/project early, tune partitions | SQL, Stages | Plan Visualization, Plan Details, Exchange, shuffle metrics |
| `04_broadcast_join` | Missing broadcast | SortMergeJoin and Exchanges | Broadcast small side | SQL, Stages | Plan Details required; compare join operator |
| `05_data_skew` | Hot key | Straggler tasks | Salt hot key, enable skew handling | Stages, SQL | Stage task table and duration percentiles required |
| `06_small_files` | Many tiny files | Many short input tasks | Compact files | Jobs, Stages | Stage detail task count; Timeline optional |
| `07_too_few_partitions` | Low parallelism | Few tasks, underused executors | Repartition reasonably | Executors, Stages | Executors task activity and stage task count |
| `08_too_many_partitions` | Scheduler overhead | Many tiny tasks | Coalesce/repartition sensibly | Jobs, Stages | Stage detail task count and scheduler delay if visible |
| `09_spill` | Memory pressure | Spill metrics, long tasks | Narrow rows, better partitioning | Stages, Executors | Stage detail spill, peak memory, GC and Executors |
| `10_cache_misuse` | Wasteful cache | Storage memory without reuse | Cache only reused data | Storage, Executors | Storage required; Executors storage memory helpful |
| `11_udf_cost` | UDF blocks optimizer-friendly logic | UDF in SQL plan | Use built-in functions | SQL | Plan Details required; search for UDF expression |
| `12_aqe_comparison` | AQE disabled | Non-adaptive plan | Enable AQE | SQL, Stages | Initial/final plan, AdaptiveSparkPlan, AQEShuffleRead |
| `13_task_failure_retry` | Transient failure | Failed task attempt and retry | Validate/filter bad input | Jobs, Stages, Executors | Timeline, failed attempts and executor logs |
| `14_config_validation` | Misunderstood config | Environment tab values | Pass config explicitly | Environment | Spark Properties only |
| `15_structured_streaming_backlog` | Processing slower than input | Batch duration, input vs processed rates | Tune trigger/rate/work | Structured Streaming | Query progress rates and batch duration |
| `16_stateful_streaming` | Unbounded state | State operator metrics | Watermark and bounded windows | Structured Streaming | State operator rows and memory |
| `17_real_time_mode` | Micro-batch vs real-time | Query trigger/progress evidence | Use Spark 4.1 real-time mode where supported | Structured Streaming | Trigger/query progress only; no fixed latency claims |
