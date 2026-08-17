ALTER TABLE fact_host_metrics
    MODIFY COLUMN hostname String COMMENT 'Name of the monitored host',
    MODIFY COLUMN cpu_usage_percent Float64,
    MODIFY COLUMN memory_usage_percent Float64,
    MODIFY COLUMN total_memory_bytes Int64,
    MODIFY COLUMN available_memory_bytes Int64,
    MODIFY COLUMN swap_usage_percent Float64,
    MODIFY COLUMN total_swap_bytes Int64,
    MODIFY COLUMN used_swap_bytes Int64,
    MODIFY COLUMN disk_usage_percent Float64,
    MODIFY COLUMN disk_read_bytes Int64 COMMENT 'Cumulative bytes read since system boot',
    MODIFY COLUMN disk_write_bytes Int64 COMMENT 'Cumulative bytes written since system boot',
    MODIFY COLUMN network_bytes_sent Int64 COMMENT 'Cumulative bytes sent since system boot',
    MODIFY COLUMN network_bytes_received Int64 COMMENT 'Cumulative bytes received since system boot';

ALTER TABLE fact_process_metrics
    MODIFY COLUMN pid Int32,
    MODIFY COLUMN process_name String,
    MODIFY COLUMN cpu_usage_percent Float64,
    MODIFY COLUMN memory_usage_percent Float64,
    MODIFY COLUMN rss_memory_bytes Int64,
    MODIFY COLUMN process_status String;
