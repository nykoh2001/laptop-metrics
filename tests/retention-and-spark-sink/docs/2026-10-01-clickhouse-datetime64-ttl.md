# ClickHouse DateTime64 TTL failure

## Failure

Applying `clickhouse/init/001_create_security_events.sql` to the Docker Compose ClickHouse
25.3 service failed with `BAD_TTL_EXPRESSION`. The table used a `DateTime64(6, 'UTC')`
`collected_at` column and defined the retention expression as:

```sql
TTL collected_at + INTERVAL 7 DAY DELETE
```

## Cause

In the tested ClickHouse version, the interval expression retained the `DateTime64` result type,
while a MergeTree TTL expression must produce `DateTime` or `Date`.

## Improvement

The TTL expression now converts the retention timestamp explicitly before adding the interval:

```sql
TTL toDateTime(collected_at) + INTERVAL 7 DAY DELETE
```

The event column keeps its microsecond precision for storage and ordering. Only the deletion
deadline is reduced to second precision. The security control regression test checks the
compatible expression, and the schema is also applied to a real ClickHouse container during the
end-to-end smoke test.
