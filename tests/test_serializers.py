"""Tests for metric serialization."""

import json

from collector.models import HostMetric
from collector.serializers import serialize_metric


def test_serialize_metric_returns_valid_json_bytes() -> None:
    """Metric serialization preserves all fields as UTF-8 JSON."""
    metric = HostMetric(
        timestamp="2026-01-01T00:00:00.000000Z",
        hostname="test-host",
        cpu_usage_percent=10.0,
        memory_usage_percent=20.0,
        total_memory_bytes=100,
        available_memory_bytes=80,
        swap_usage_percent=0.0,
        total_swap_bytes=0,
        used_swap_bytes=0,
        disk_usage_percent=30.0,
        disk_read_bytes=1,
        disk_write_bytes=2,
        network_bytes_sent=3,
        network_bytes_received=4,
    )

    payload = json.loads(serialize_metric(metric))

    assert payload["hostname"] == "test-host"
    assert payload["network_bytes_received"] == 4
