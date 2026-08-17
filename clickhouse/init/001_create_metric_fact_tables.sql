CREATE TABLE IF NOT EXISTS fact_host_metrics
(
    timestamp DateTime64(6, 'UTC') COMMENT 'UTC time at which the host metric was collected',
    hostname String COMMENT 'Name of the monitored host',
    cpu_usage_percent Float64,
    memory_usage_percent Float64,
    total_memory_bytes Int64,
    available_memory_bytes Int64,
    swap_usage_percent Float64,
    total_swap_bytes Int64,
    used_swap_bytes Int64,
    disk_usage_percent Float64,
    disk_read_bytes Int64 COMMENT 'Cumulative bytes read since system boot',
    disk_write_bytes Int64 COMMENT 'Cumulative bytes written since system boot',
    network_bytes_sent Int64 COMMENT 'Cumulative bytes sent since system boot',
    network_bytes_received Int64 COMMENT 'Cumulative bytes received since system boot',
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY (hostname, timestamp)
COMMENT 'One host metric snapshot per host and collection timestamp';

CREATE TABLE IF NOT EXISTS fact_process_metrics
(
    timestamp DateTime64(6, 'UTC') COMMENT 'UTC time at which the process metric was collected',
    pid Int32,
    process_name String,
    cpu_usage_percent Float64,
    memory_usage_percent Float64,
    rss_memory_bytes Int64,
    process_status String,
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY (process_name, pid, timestamp)
COMMENT 'One process metric snapshot per process and collection timestamp';
