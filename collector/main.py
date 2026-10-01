"""Collector process lifecycle and continuous collection loop."""

import logging
import signal
import threading
from collections.abc import Iterator
from pathlib import Path
from types import FrameType
from typing import Protocol

from collector.collector import (
    DockerLifecycleCollector,
    EventCollector,
    NetworkConnectionCollector,
    ProcessStateCollector,
    SecurityTelemetryCollector,
)
from collector.config import CollectorConfig
from collector.kafka_producer import KafkaEventProducer
from collector.models import SecurityEvent
from collector.utils import load_env_file, stable_host_id, utc_timestamp

LOGGER = logging.getLogger(__name__)
_COLLECTOR_ENV_NAMES = frozenset(
    {
        "KAFKA_BOOTSTRAP_SERVERS",
        "SECURITY_EVENTS_TOPIC",
        "COLLECTION_INTERVAL_SECONDS",
        "KAFKA_CLIENT_ID",
        "HOST_ID_SALT",
        "DOCKER_EVENTS_ENABLED",
        "LOG_LEVEL",
    }
)


class EventPublisher(Protocol):
    """Publishing behavior required by the collector service."""

    def publish(self, topic: str, event: SecurityEvent) -> None:
        """Publish one event record."""

    def flush(self, timeout_seconds: float = 10.0) -> None:
        """Flush pending records."""

    def close(self, timeout_seconds: float = 10.0) -> None:
        """Close the publisher."""


class TelemetrySource(Protocol):
    """Telemetry collection behavior required by the service."""

    def collect_events(self, collected_at: str | None = None) -> Iterator[SecurityEvent]:
        """Yield events available in one collection cycle."""

    def close(self) -> None:
        """Release source resources."""


class CollectorService:
    """Run telemetry collection cycles until a stop event is set."""

    def __init__(
        self,
        config: CollectorConfig,
        telemetry_source: TelemetrySource,
        publisher: EventPublisher,
        stop_event: threading.Event,
    ) -> None:
        """Initialize the collector service from composed dependencies.

        Args:
            config: Runtime configuration.
            telemetry_source: Aggregate security telemetry source.
            publisher: Event publishing implementation.
            stop_event: Event used to request graceful shutdown.
        """
        self._config = config
        self._telemetry_source = telemetry_source
        self._publisher = publisher
        self._stop_event = stop_event

    def run(self) -> None:
        """Collect and publish events continuously, then close cleanly."""
        LOGGER.info(
            "Security telemetry collector started",
            extra={"interval_seconds": self._config.collection_interval_seconds},
        )
        try:
            while not self._stop_event.is_set():
                self.run_cycle()
                self._stop_event.wait(self._config.collection_interval_seconds)
        finally:
            self._telemetry_source.close()
            LOGGER.info("Flushing pending Kafka messages")
            self._publisher.flush()
            self._publisher.close()
            LOGGER.info("Security telemetry collector stopped gracefully")

    def run_cycle(self) -> int:
        """Collect and publish one complete telemetry cycle.

        Returns:
            Number of events passed to the publisher.
        """
        collected_at = utc_timestamp()
        count = 0
        try:
            for event in self._telemetry_source.collect_events(collected_at):
                self._publisher.publish(self._config.security_events_topic, event)
                count += 1
        except Exception:
            LOGGER.error("Unexpected telemetry collection failure")
        LOGGER.debug("Telemetry collection cycle completed", extra={"event_count": count})
        return count


def configure_logging(log_level: str) -> None:
    """Configure process-wide standard logging.

    Args:
        log_level: Valid logging level name.
    """
    logging.basicConfig(level=log_level, format="%(asctime)s %(levelname)s %(name)s %(message)s")


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


def build_telemetry_source(config: CollectorConfig) -> SecurityTelemetryCollector:
    """Compose the collectors enabled by configuration.

    Args:
        config: Validated runtime configuration.

    Returns:
        Aggregate collector containing mandatory and optional sources.
    """
    host_id = stable_host_id(config.host_id_salt)
    collectors: list[EventCollector] = [
        ProcessStateCollector(host_id, config.host_id_salt),
        NetworkConnectionCollector(host_id),
    ]
    if config.docker_events_enabled:
        collectors.append(DockerLifecycleCollector(host_id))
    return SecurityTelemetryCollector(collectors)


def main() -> None:
    """Load configuration, compose dependencies, and run the collector."""
    load_env_file(Path(".env"), _COLLECTOR_ENV_NAMES)
    try:
        config = CollectorConfig.from_env()
    except ValueError as error:
        raise SystemExit(f"Invalid collector configuration: {error}") from error

    configure_logging(config.log_level)
    stop_event = threading.Event()
    install_signal_handlers(stop_event)
    source = build_telemetry_source(config)
    producer = KafkaEventProducer(config.kafka_bootstrap_servers, config.kafka_client_id)
    service = CollectorService(config, source, producer, stop_event)
    try:
        service.run()
    except Exception:
        LOGGER.error("Unexpected collector failure")
        raise


if __name__ == "__main__":
    main()
