"""Unit tests for phase-1 telemetry sources."""

from types import SimpleNamespace

import pytest

import collector.collector as collector_module
from collector.collector import (
    DockerLifecycleCollector,
    NetworkConnectionCollector,
    ProcessStateCollector,
    SecurityTelemetryCollector,
    build_docker_lifecycle_event,
)

COLLECTED_AT = "2026-10-01T00:00:05.000000Z"
HOST_ID = "host-0123456789abcdef01234567"


def test_process_collector_emits_required_fields_without_sensitive_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Process snapshots contain required state but no command or environment."""
    process = SimpleNamespace(
        pid=42,
        info={
            "pid": 42,
            "ppid": 1,
            "name": "worker",
            "exe": None,
            "username": "tester",
            "create_time": 1_767_225_600.0,
        },
    )
    monkeypatch.setattr(
        collector_module.psutil,
        "process_iter",
        lambda attrs, ad_value: iter((process,)),
    )

    events = list(ProcessStateCollector(HOST_ID, "private-salt").collect_events(COLLECTED_AT))

    assert len(events) == 1
    assert events[0].event_type == "process.state"
    assert events[0].event_time == "2026-01-01T00:00:00.000000Z"
    assert events[0].payload == {
        "pid": 42,
        "ppid": 1,
        "process_name": "worker",
        "executable_path": None,
        "user_id": "user-91a056e49004f2254275fb0b",
        "start_time": "2026-01-01T00:00:00.000000Z",
    }
    assert "cmdline" not in events[0].payload
    assert "environ" not in events[0].payload
    assert "tester" not in str(events[0].payload)


def test_network_collector_emits_listener_and_current_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Network snapshots distinguish listeners and include endpoint fields."""
    listener = SimpleNamespace(
        laddr=("127.0.0.1", 8080),
        raddr=(),
        status=collector_module.psutil.CONN_LISTEN,
        pid=42,
    )
    connection = SimpleNamespace(
        laddr=("10.0.0.2", 50123),
        raddr=("10.0.0.3", 443),
        status="ESTABLISHED",
        pid=None,
    )
    monkeypatch.setattr(
        collector_module.psutil,
        "net_connections",
        lambda kind: [listener, connection],
    )

    events = list(NetworkConnectionCollector(HOST_ID).collect_events(COLLECTED_AT))

    assert [event.action for event in events] == ["listening", "observed"]
    assert events[0].payload["local_port"] == 8080
    assert events[0].payload["remote_address"] is None
    assert events[1].payload["remote_address"] == "10.0.0.3"
    assert events[1].payload["remote_port"] == 443


def test_network_collector_falls_back_to_readable_processes(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Denied system-wide access yields partial user-level network visibility."""

    class DeniedProcess:
        """Represent one protected process."""

        pid = 1

        def net_connections(self, kind: str) -> list[object]:
            """Reject access to process connections."""
            raise collector_module.psutil.AccessDenied(pid=self.pid)

    readable_connection = SimpleNamespace(
        laddr=("127.0.0.1", 9000),
        raddr=(),
        status=collector_module.psutil.CONN_LISTEN,
    )
    readable_process = SimpleNamespace(
        pid=2,
        net_connections=lambda kind: [readable_connection],
    )

    def deny_global_connections(kind: str) -> list[object]:
        raise collector_module.psutil.AccessDenied()

    monkeypatch.setattr(collector_module.psutil, "net_connections", deny_global_connections)
    monkeypatch.setattr(
        collector_module.psutil,
        "process_iter",
        lambda attrs, ad_value: iter((DeniedProcess(), readable_process)),
    )
    caplog.set_level("INFO")

    events = list(NetworkConnectionCollector(HOST_ID).collect_events(COLLECTED_AT))

    assert len(events) == 1
    assert events[0].action == "listening"
    assert events[0].payload["pid"] == 2
    records = [record for record in caplog.records if hasattr(record, "access_denied_count")]
    assert len(records) == 1
    assert records[0].access_denied_count == 1


def test_docker_event_is_sanitized_and_uses_native_event_time() -> None:
    """Docker attributes are excluded while lifecycle identity is retained."""
    event = build_docker_lifecycle_event(
        {
            "action": "start",
            "container_id": "abc123",
            "time_nano": 1_767_225_600_000_000_000,
        },
        HOST_ID,
        COLLECTED_AT,
    )

    assert event is not None
    assert event.event_type == "docker.container.lifecycle"
    assert event.action == "start"
    assert event.event_time == "2026-01-01T00:00:00.000000Z"
    assert event.payload == {"container_id": "abc123"}


def test_docker_cli_format_collects_only_allowlisted_fields() -> None:
    """The Docker CLI never emits labels or unrestricted actor attributes."""
    event_format = collector_module._DOCKER_EVENT_FORMAT

    assert ".Action" in event_format
    assert ".Actor.ID" in event_format
    assert ".TimeNano" in event_format
    assert ".Attributes" not in event_format
    assert ".Name" not in event_format


def test_docker_unavailable_disables_only_optional_collector() -> None:
    """An unavailable Docker daemon produces no events and no exception."""
    docker = DockerLifecycleCollector(
        HOST_ID,
        executable="/missing/docker",
        availability_check=lambda: False,
    )

    assert docker.enabled is False
    assert list(docker.collect_events(COLLECTED_AT)) == []
    docker.close()


def test_docker_permission_denied_disables_only_optional_collector() -> None:
    """Denied Docker access is treated like an unavailable optional source."""

    def deny_access() -> bool:
        raise PermissionError("docker access denied")

    docker = DockerLifecycleCollector(
        HOST_ID,
        executable="/usr/local/bin/docker",
        availability_check=deny_access,
    )

    assert docker.enabled is False
    assert list(docker.collect_events(COLLECTED_AT)) == []


def test_docker_subprocess_environment_excludes_pipeline_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Optional Docker CLI processes do not inherit unrelated credentials."""
    monkeypatch.setenv("PATH", "/usr/bin")
    monkeypatch.setenv("CLICKHOUSE_PASSWORD", "credential-placeholder")

    environment = collector_module._docker_subprocess_environment()

    assert environment["PATH"] == "/usr/bin"
    assert "CLICKHOUSE_PASSWORD" not in environment


def test_process_access_denied_does_not_stop_later_processes(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Per-process access denial is counted while readable processes continue."""

    class DeniedProcess:
        """Represent a process whose aggregate info becomes inaccessible."""

        pid = 1

        @property
        def info(self) -> dict[str, object]:
            """Raise the same error psutil exposes for protected processes."""
            raise collector_module.psutil.AccessDenied(pid=self.pid)

    readable = SimpleNamespace(
        pid=2,
        info={
            "pid": 2,
            "ppid": 1,
            "name": "readable",
            "exe": "/usr/bin/readable",
            "username": "tester",
            "create_time": None,
        },
    )
    requested_attributes: list[str] = []

    def process_iter(attrs: list[str], ad_value: object):
        requested_attributes.extend(attrs)
        return iter((DeniedProcess(), readable))

    monkeypatch.setattr(collector_module.psutil, "process_iter", process_iter)
    caplog.set_level("INFO")

    events = list(ProcessStateCollector(HOST_ID, "private-salt").collect_events(COLLECTED_AT))

    assert len(events) == 1
    assert events[0].payload["pid"] == 2
    assert requested_attributes == ["pid", "ppid", "name", "exe", "username", "create_time"]
    records = [record for record in caplog.records if hasattr(record, "access_denied_count")]
    assert len(records) == 1
    assert records[0].access_denied_count == 1


def test_aggregate_collector_isolates_source_failure() -> None:
    """One source exception does not suppress events from later sources."""

    class BrokenCollector:
        """Fail every collection call."""

        def collect_events(self, collected_at: str):  # type: ignore[no-untyped-def]
            """Raise a source failure."""
            raise OSError("unavailable")

        def close(self) -> None:
            """Release no resources."""

    class EmptyCollector:
        """Complete successfully without events."""

        def collect_events(self, collected_at: str):  # type: ignore[no-untyped-def]
            """Yield no events."""
            yield from ()

        def close(self) -> None:
            """Release no resources."""

    aggregate = SecurityTelemetryCollector([BrokenCollector(), EmptyCollector()])

    assert list(aggregate.collect_events(COLLECTED_AT)) == []


def test_aggregate_collector_does_not_log_source_exception_contents(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Source failures are reported without copying sensitive exception text."""

    class SensitiveFailureCollector:
        """Raise an exception containing a marker that must not be logged."""

        def collect_events(self, collected_at: str):  # type: ignore[no-untyped-def]
            """Raise a simulated sensitive failure."""
            raise OSError("credential-placeholder")

        def close(self) -> None:
            """Release no resources."""

    caplog.set_level("ERROR")
    aggregate = SecurityTelemetryCollector([SensitiveFailureCollector()])

    assert list(aggregate.collect_events(COLLECTED_AT)) == []
    assert "Telemetry source collection failed" in caplog.text
    assert "credential-placeholder" not in caplog.text
