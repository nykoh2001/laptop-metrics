"""Tests for system metric collection edge cases."""

from types import SimpleNamespace

import pytest

import collector.collector as collector_module
from collector.collector import SystemMetricCollector


def test_process_without_memory_info_is_skipped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An unreadable process does not stop collection of later processes."""
    unreadable = SimpleNamespace(
        pid=1,
        info={
            "pid": 1,
            "name": "unreadable",
            "cpu_percent": 0.0,
            "memory_percent": 0.0,
            "memory_info": None,
            "status": "running",
        },
    )
    readable = SimpleNamespace(
        pid=2,
        info={
            "pid": 2,
            "name": "readable",
            "cpu_percent": 1.0,
            "memory_percent": 2.0,
            "memory_info": SimpleNamespace(rss=3),
            "status": "sleeping",
        },
    )
    monkeypatch.setattr(
        collector_module.psutil,
        "process_iter",
        lambda attrs: iter((unreadable, readable)),
    )
    metric_collector = SystemMetricCollector()

    metrics = list(metric_collector.collect_process_metrics("timestamp"))

    assert len(metrics) == 1
    assert metrics[0].pid == 2
