"""Metric value objects."""

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class HostMetric:
    """A snapshot of macOS host resource usage."""

    timestamp: str
    hostname: str
    cpu_usage_percent: float
    memory_usage_percent: float
    total_memory_bytes: int
    available_memory_bytes: int
    swap_usage_percent: float
    total_swap_bytes: int
    used_swap_bytes: int
    disk_usage_percent: float
    disk_read_bytes: int
    disk_write_bytes: int
    network_bytes_sent: int
    network_bytes_received: int


@dataclass(frozen=True, slots=True)
class ProcessMetric:
    """A snapshot of one process running on the macOS host."""

    timestamp: str
    pid: int
    process_name: str
    cpu_usage_percent: float
    memory_usage_percent: float
    rss_memory_bytes: int
    process_status: str
