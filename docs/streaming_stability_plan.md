# Streaming stability plan

This document records reliability work intentionally deferred from the first
ClickHouse sink implementation. The current implementation writes each Spark
Structured Streaming micro-batch to ClickHouse through JDBC without custom retry
logic.

## Current behavior

| Area | Current behavior |
|---|---|
| Collector cadence | Runs one collection cycle, then waits `COLLECTION_INTERVAL_SECONDS` before the next cycle. |
| Kafka source | Spark reads `host_metrics` and `process_metrics` with independent checkpoints. |
| Spark sink | `foreachBatch` appends each micro-batch to ClickHouse through JDBC. |
| Insert failure | The Spark query fails, so the failed micro-batch is not checkpointed as completed. |
| Duplicate handling | No explicit deduplication is implemented yet. |

## Future stability items

| Situation | Risk | Future direction |
|---|---|---|
| ClickHouse query or connection failure | The streaming query stops and ingestion pauses. | Add bounded retry with backoff, alerting, and a clear restart runbook. |
| Partial batch insert followed by failure | Reprocessing the same Kafka offsets can duplicate rows. | Add deterministic row identity or batch tracking before enabling retries. |
| Invalid JSON or schema mismatch | One malformed record can fail a batch after parsing or insertion. | Split invalid rows into a dead-letter topic/table and track error counts. |
| Null or unparsable timestamps | ClickHouse insert can fail or analytical time ordering becomes unreliable. | Validate required fields and reject or quarantine bad records before the sink. |
| Numeric range mismatch | ClickHouse unsigned integer columns can reject negative or overflowing values. | Add range checks for PID and byte counters before insertion. |
| Kafka backlog growth | Large catch-up batches can overload ClickHouse. | Tune `maxOffsetsPerTrigger`, trigger interval, and JDBC batch size. |
| Checkpoint deletion or relocation | Spark may replay already inserted Kafka data. | Treat checkpoint paths as persistent state and document recovery steps. |
| ClickHouse table schema drift | Spark columns may no longer match the target table. | Add schema compatibility checks in CI or startup validation. |
| Sink observability gap | Failures may only be visible in Spark logs. | Emit per-batch row counts, failure metrics, and ingestion lag metrics. |
| Graceful shutdown during batch write | In-flight writes may be interrupted. | Document shutdown behavior and add operational checks before stopping jobs. |
