# Spark partition sink test boundary

## Failure

After moving ClickHouse writes into Spark `foreachPartition`, the existing local Spark test failed
to reach its monkeypatched HTTP stub. A live smoke run also found that a slotted config dataclass
captured from the `spark-submit` `__main__` module did not restore its fields in the Python worker.

## Cause

The old test assumed that ClickHouse HTTP I/O ran in the Spark driver. That assumption no longer
matches the distributed sink and would not verify partition-level insert behavior. The live failure
showed that `__main__` module references are also an unsafe boundary for a captured custom config
object.

## Improvement

The Spark parsing test now validates one small parsed row without invoking the network sink. Sink
unit tests use a Spark-shaped partition callback with several rows and count HTTP requests, proving
that rows in one partition are sent in one insert. A separate bounded-flush test verifies that a
partition is split when its configured row limit is reached. The worker receives a plain string
tuple and reconstructs its config locally. The live Spark-to-ClickHouse smoke run then verified the
distributed worker path. Production still validates each untrusted row and writes only from Spark
partition tasks.

## End-to-end smoke finding

The smoke harness accidentally left two independent Spark queries consuming the same topic with
separate checkpoints. Both correctly read the topic records, so the overlapping runs duplicated
rows in ClickHouse. Both temporary jobs were stopped. A later single-query run processed 572
collector records, advanced Kafka by 572 offsets, and inserted 572 rows through three ClickHouse
requests (37, 500, and 35 rows). The earlier smoke rows remain because the repository policy forbids
manual truncation; normal TTL will expire them asynchronously.
