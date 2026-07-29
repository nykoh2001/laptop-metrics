"""Tests for collection cycle orchestration."""

import threading
from collections.abc import Iterator

from collector.config import CollectorConfig
from collector.main import CollectorService
from collector.models import HostMetric, ProcessMetric


class StubCollector:
    """Return deterministic metrics for service tests."""

    def collect_host_metric(self, timestamp: str | None = None) -> HostMetric:
        """Return one host metric."""
        return HostMetric(
            timestamp=timestamp or "timestamp",
            hostname="host",
            cpu_usage_percent=1.0,
            memory_usage_percent=2.0,
            total_memory_bytes=3,
            available_memory_bytes=4,
            swap_usage_percent=5.0,
            total_swap_bytes=6,
            used_swap_bytes=7,
            disk_usage_percent=8.0,
            disk_read_bytes=9,
            disk_write_bytes=10,
            network_bytes_sent=11,
            network_bytes_received=12,
        )

    def collect_process_metrics(self, timestamp: str | None = None) -> Iterator[ProcessMetric]:
        """Yield one process metric."""
        yield ProcessMetric(
            timestamp=timestamp or "timestamp",
            pid=1,
            process_name="process",
            cpu_usage_percent=1.0,
            memory_usage_percent=2.0,
            rss_memory_bytes=3,
            process_status="running",
        )


class RecordingPublisher:
    """Record calls without connecting to Kafka."""

    def __init__(self) -> None:
        """Initialize recorded state."""
        self.records: list[tuple[str, HostMetric | ProcessMetric]] = []
        self.flushed = False
        self.closed = False

    def publish(self, topic: str, metric: HostMetric | ProcessMetric) -> None:
        """Record one published metric."""
        self.records.append((topic, metric))

    def flush(self, timeout_seconds: float = 10.0) -> None:
        """Record a flush call."""
        self.flushed = True

    def close(self, timeout_seconds: float = 10.0) -> None:
        """Record a close call."""
        self.closed = True


def test_run_cycle_publishes_both_metric_types() -> None:
    """One cycle publishes host and process metrics to separate topics."""
    config = CollectorConfig(
        kafka_bootstrap_servers=("localhost:9092",),
        host_metrics_topic="host_metrics",
        process_metrics_topic="process_metrics",
        collection_interval_seconds=5.0,
        kafka_client_id="test",
        log_level="INFO",
    )
    publisher = RecordingPublisher()
    service = CollectorService(
        config,
        StubCollector(),
        publisher,
        threading.Event(),
    )

    service.run_cycle()

    assert [topic for topic, _metric in publisher.records] == [
        "host_metrics",
        "process_metrics",
    ]
