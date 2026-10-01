CREATE TABLE IF NOT EXISTS security_events
(
    schema_version LowCardinality(String),
    event_id UUID,
    event_time DateTime64(6, 'UTC'),
    collected_at DateTime64(6, 'UTC'),
    host_id String,
    source LowCardinality(String),
    event_type LowCardinality(String),
    action LowCardinality(String),
    payload String,
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(collected_at)
ORDER BY (host_id, event_time, event_id)
TTL collected_at + INTERVAL 7 DAY DELETE
COMMENT 'Phase-1 common endpoint security telemetry events';
