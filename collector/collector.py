"""Host and process metric collection using psutil."""

import logging
import socket
from collections.abc import Iterator

import psutil

from collector.models import HostMetric, ProcessMetric
from collector.utils import utc_timestamp

LOGGER = logging.getLogger(__name__)


class SystemMetricCollector:
    """Collect host and per-process metric snapshots from macOS."""

    def __init__(self, disk_path: str = "/") -> None:
        """Initialize the collector.

        Args:
            disk_path: Filesystem path whose disk usage should be measured.
        """
        self._disk_path = disk_path
        psutil.cpu_percent(interval=None)

    def collect_host_metric(self, timestamp: str | None = None) -> HostMetric:
        """Collect one host metric snapshot.

        Args:
            timestamp: Optional shared cycle timestamp.

        Returns:
            Current host resource metrics.

        Raises:
            OSError: If host metrics cannot be read.
        """
        memory = psutil.virtual_memory()
        swap = psutil.swap_memory()
        disk_usage = psutil.disk_usage(self._disk_path)
        disk_io = psutil.disk_io_counters()
        network_io = psutil.net_io_counters()
        if disk_io is None:
            raise OSError("Disk I/O counters are unavailable")
        if network_io is None:
            raise OSError("Network I/O counters are unavailable")

        return HostMetric(
            timestamp=timestamp or utc_timestamp(),
            hostname=socket.gethostname(),
            cpu_usage_percent=psutil.cpu_percent(interval=None),
            memory_usage_percent=memory.percent,
            total_memory_bytes=memory.total,
            available_memory_bytes=memory.available,
            swap_usage_percent=swap.percent,
            total_swap_bytes=swap.total,
            used_swap_bytes=swap.used,
            disk_usage_percent=disk_usage.percent,
            disk_read_bytes=disk_io.read_bytes,
            disk_write_bytes=disk_io.write_bytes,
            network_bytes_sent=network_io.bytes_sent,
            network_bytes_received=network_io.bytes_recv,
        )

    def collect_process_metrics(self, timestamp: str | None = None) -> Iterator[ProcessMetric]:
        """Yield metrics for processes that remain readable during collection.

        Args:
            timestamp: Optional shared cycle timestamp.

        Yields:
            One metric for each readable process.
        """
        collected_at = timestamp or utc_timestamp()
        attributes = ["pid", "name", "cpu_percent", "memory_percent", "memory_info", "status"]
        for process in psutil.process_iter(attrs=attributes):
            try:
                info = process.info
                memory_info = info["memory_info"]
                if memory_info is None:
                    LOGGER.debug(
                        "Process memory information is unavailable", extra={"pid": process.pid}
                    )
                    continue
                yield ProcessMetric(
                    timestamp=collected_at,
                    pid=int(info["pid"]),
                    process_name=str(info["name"]),
                    cpu_usage_percent=float(info["cpu_percent"] or 0.0),
                    memory_usage_percent=float(info["memory_percent"] or 0.0),
                    rss_memory_bytes=int(memory_info.rss),
                    process_status=str(info["status"]),
                )
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess, KeyError):
                LOGGER.debug("Process disappeared or became unreadable", extra={"pid": process.pid})
