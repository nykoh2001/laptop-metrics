"""Collector process lifecycle and continuous collection loop."""

import logging
import signal
import threading
from collections.abc import Iterator
from pathlib import Path
from types import FrameType
from typing import Protocol

from collector.collector import SystemMetricCollector
from collector.config import CollectorConfig
from collector.kafka_producer import KafkaMetricProducer
from collector.models import HostMetric, ProcessMetric
from collector.utils import load_env_file, utc_timestamp

LOGGER = logging.getLogger(__name__)


class MetricPublisher(Protocol):
    """Publishing behavior required by the collector service."""

    def publish(self, topic: str, metric: HostMetric | ProcessMetric) -> None:
        """Publish one metric record."""

    def flush(self, timeout_seconds: float = 10.0) -> None:
        """Flush pending records."""

    def close(self, timeout_seconds: float = 10.0) -> None:
        """Close the publisher."""


class MetricSource(Protocol):
    """Metric collection behavior required by the service."""

    def collect_host_metric(self, timestamp: str | None = None) -> HostMetric:
        """Collect one host metric snapshot."""

    def collect_process_metrics(self, timestamp: str | None = None) -> Iterator[ProcessMetric]:
        """Yield the available process metric snapshots."""


class CollectorService:
    """Run collection cycles until a stop event is set."""

    def __init__(
        self,
        config: CollectorConfig,
        metric_collector: MetricSource,
        publisher: MetricPublisher,
        stop_event: threading.Event,
    ) -> None:
        """Initialize the collector service from composed dependencies.

        Args:
            config: Runtime configuration.
            metric_collector: Host metric collection implementation.
            publisher: Metric publishing implementation.
            stop_event: Event used to request graceful shutdown.
        """
        self._config = config
        self._metric_collector = metric_collector
        self._publisher = publisher
        self._stop_event = stop_event

    def run(self) -> None:
        """Collect and publish metrics continuously, then close cleanly."""
        LOGGER.info(
            "System metric collector started",
            extra={"interval_seconds": self._config.collection_interval_seconds},
        )
        try:
            while not self._stop_event.is_set():
                self.run_cycle()
                self._stop_event.wait(self._config.collection_interval_seconds)
        finally:
            LOGGER.info("Flushing pending Kafka messages")
            self._publisher.flush()
            self._publisher.close()
            LOGGER.info("System metric collector stopped gracefully")

    def run_cycle(self) -> None:
        """Collect and publish one complete metric cycle."""
        LOGGER.debug("Metric collection cycle started")
        timestamp = utc_timestamp()
        host_count = self._publish_host_metric(timestamp)
        process_count = self._publish_process_metrics(timestamp)
        LOGGER.debug(
            "Metric collection cycle completed",
            extra={"host_metrics": host_count, "process_metrics": process_count},
        )

    def _publish_host_metric(self, timestamp: str) -> int:
        try:
            metric = self._metric_collector.collect_host_metric(timestamp)
            self._publisher.publish(self._config.host_metrics_topic, metric)
            return 1
        except Exception:
            LOGGER.exception("Failed to collect host metrics")
            return 0

    def _publish_process_metrics(self, timestamp: str) -> int:
        count = 0
        try:
            for metric in self._metric_collector.collect_process_metrics(timestamp):
                self._publisher.publish(self._config.process_metrics_topic, metric)
                count += 1
        except Exception:
            LOGGER.exception("Unexpected process metric collection failure")
        return count


def configure_logging(log_level: str) -> None:
    """Configure process-wide standard logging.

    Args:
        log_level: Valid logging level name.
    """
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def install_signal_handlers(stop_event: threading.Event) -> None:
    """Install SIGINT and SIGTERM handlers that request shutdown.

    Args:
        stop_event: Event to set when a shutdown signal arrives.
    """

    def request_shutdown(signum: int, _frame: FrameType | None) -> None:
        LOGGER.info("Shutdown requested", extra={"signal": signal.Signals(signum).name})
        stop_event.set()

    signal.signal(signal.SIGINT, request_shutdown)
    signal.signal(signal.SIGTERM, request_shutdown)


def main() -> None:
    """Load configuration, compose dependencies, and run the collector."""
    load_env_file(Path(".env"))
    try:
        config = CollectorConfig.from_env()
    except ValueError as error:
        raise SystemExit(f"Invalid collector configuration: {error}") from error

    configure_logging(config.log_level)
    stop_event = threading.Event()
    install_signal_handlers(stop_event)
    collector = SystemMetricCollector()
    producer = KafkaMetricProducer(config.kafka_bootstrap_servers, config.kafka_client_id)
    service = CollectorService(config, collector, producer, stop_event)
    try:
        service.run()
    except Exception:
        LOGGER.exception("Unexpected collector failure")
        raise


if __name__ == "__main__":
    main()
