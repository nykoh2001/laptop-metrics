"""Tests for collection cycle orchestration."""

import threading
from collections.abc import Iterator

from collector.config import CollectorConfig
from collector.events import create_security_event
from collector.main import CollectorService
from collector.models import SecurityEvent


class StubSource:
    """Return deterministic security events for service tests."""

    def __init__(self) -> None:
        """Initialize source state."""
        self.closed = False

    def collect_events(self, collected_at: str | None = None) -> Iterator[SecurityEvent]:
        """Yield one process event."""
        cycle_time = collected_at or "2026-10-01T00:00:01.000000Z"
        yield create_security_event(
            host_id="host-0123456789abcdef01234567",
            source="psutil",
            event_type="process.state",
            action="observed",
            event_time="2026-10-01T00:00:00.000000Z",
            collected_at=cycle_time,
            payload={"pid": 1},
        )

    def close(self) -> None:
        """Record shutdown."""
        self.closed = True


class RecordingPublisher:
    """Record calls without connecting to Kafka."""

    def __init__(self) -> None:
        """Initialize recorded state."""
        self.records: list[tuple[str, SecurityEvent]] = []

    def publish(self, topic: str, event: SecurityEvent) -> None:
        """Record one published event."""
        self.records.append((topic, event))

    def flush(self, timeout_seconds: float = 10.0) -> None:
        """Flush no records."""

    def close(self, timeout_seconds: float = 10.0) -> None:
        """Close no resources."""


def test_run_cycle_publishes_all_events_to_single_topic() -> None:
    """Phase 1 uses exactly one Kafka topic for every event type."""
    config = CollectorConfig(
        kafka_bootstrap_servers=("localhost:9092",),
        security_events_topic="security_events",
        collection_interval_seconds=5.0,
        kafka_client_id="test",
        host_id_salt="test-salt",
        docker_events_enabled=False,
        log_level="INFO",
    )
    publisher = RecordingPublisher()
    service = CollectorService(config, StubSource(), publisher, threading.Event())

    count = service.run_cycle()

    assert count == 1
    assert [topic for topic, _event in publisher.records] == ["security_events"]
