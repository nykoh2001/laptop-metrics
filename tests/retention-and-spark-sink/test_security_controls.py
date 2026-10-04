"""Regression tests for phase-1 least-privilege and data-minimization controls."""

import json
import os
import re
from pathlib import Path

import pytest

import collector.utils as utils_module
from collector.events import create_security_event
from collector.serializers import serialize_event
from collector.utils import (
    load_env_file,
    normalize_executable_path,
    pseudonymous_user_id,
    stable_host_id,
)


def test_serialized_process_event_contains_only_allowlisted_fields() -> None:
    """Serialized process telemetry excludes command, environment, and secret fields."""
    event = create_security_event(
        host_id="host-0123456789abcdef01234567",
        source="psutil",
        event_type="process.state",
        action="observed",
        event_time="2026-10-01T00:00:00.000000Z",
        collected_at="2026-10-01T00:00:01.000000Z",
        payload={
            "pid": 10,
            "ppid": 1,
            "process_name": "worker",
            "executable_path": "$HOME/bin/worker",
            "user_id": "user-0123456789abcdef01234567",
            "start_time": "2026-10-01T00:00:00.000000Z",
        },
    )

    serialized = serialize_event(event)
    decoded = json.loads(serialized)

    assert set(decoded["payload"]) == {
        "pid",
        "ppid",
        "process_name",
        "executable_path",
        "user_id",
        "start_time",
    }
    forbidden_terms = {
        "cmdline",
        "command_line",
        "arguments",
        "environment",
        "environ",
        "credential",
        "token",
        "secret",
        "file_content",
        "browser",
    }
    assert forbidden_terms.isdisjoint(decoded["payload"])


def test_serializer_rejects_non_allowlisted_sensitive_field() -> None:
    """A future collector cannot silently serialize an added command line."""
    event = create_security_event(
        host_id="host-0123456789abcdef01234567",
        source="psutil",
        event_type="process.state",
        action="observed",
        event_time="2026-10-01T00:00:00.000000Z",
        collected_at="2026-10-01T00:00:01.000000Z",
        payload={
            "pid": 10,
            "ppid": 1,
            "process_name": "worker",
            "executable_path": "/usr/bin/worker",
            "user_id": None,
            "start_time": None,
            "command_line": "worker --token secret",
        },
    )

    with pytest.raises(ValueError, match="Payload violates allowlist"):
        serialize_event(event)


def test_host_and_user_identifiers_do_not_expose_raw_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Stable identifiers are salted digests rather than source identifiers."""
    monkeypatch.setattr(utils_module.uuid, "getnode", lambda: 1_234_567)

    host_id = stable_host_id("private-salt")
    user_id = pseudonymous_user_id("alice", "private-salt")

    assert "1234567" not in host_id
    assert user_id is not None
    assert "alice" not in user_id
    assert host_id == stable_host_id("private-salt")
    assert user_id == pseudonymous_user_id("alice", "private-salt")


def test_home_directory_is_normalized_without_changing_system_path() -> None:
    """A recognized user-home prefix is removed before transmission."""
    assert (
        normalize_executable_path("/Users/alice/bin/tool", "alice", home_directory="/Users/current")
        == "$HOME/bin/tool"
    )
    assert (
        normalize_executable_path("/usr/bin/tool", "alice", home_directory="/Users/current")
        == "/usr/bin/tool"
    )
    assert (
        normalize_executable_path(
            "/Users/unavailable-user/bin/tool",
            None,
            home_directory="/Users/current",
        )
        == "$HOME/bin/tool"
    )


def test_compose_has_no_privilege_escalation_or_sensitive_host_mounts() -> None:
    """Container services cannot be used to expand host telemetry privileges."""
    project_root = Path(__file__).resolve().parents[2]
    compose = (project_root / "docker-compose.yml").read_text(encoding="utf-8")
    execution_files = [
        *sorted((project_root / "scripts").glob("*.sh")),
        *sorted((project_root / "launchd").glob("*.plist.example")),
    ]
    execution_config = (
        compose + "\n" + "\n".join(path.read_text(encoding="utf-8") for path in execution_files)
    )
    normalized = execution_config.lower().replace(" ", "")

    forbidden_fragments = (
        "privileged:true",
        "cap_sys_ptrace",
        "cap_sys_admin",
        "pid:host",
        "network_mode:host",
        "/var/run/docker.sock",
        "/run/docker.sock",
    )
    assert all(fragment not in normalized for fragment in forbidden_fragments)
    assert "\n  collector:" not in compose

    bind_sources = re.findall(r"^\s*-\s+([^\s:]+):/[^\s]+", compose, flags=re.MULTILINE)
    assert bind_sources
    assert all(source.startswith("./") or source.endswith("_data") for source in bind_sources)

    published_ports = re.findall(r'^\s*-\s+"([^"\n]+:[0-9]+)"$', compose, flags=re.MULTILINE)
    assert published_ports
    assert all(port.startswith("127.0.0.1:") for port in published_ports)


def test_clickhouse_schema_has_bounded_retention() -> None:
    """Security telemetry is not retained indefinitely by the phase-1 table."""
    project_root = Path(__file__).resolve().parents[2]
    ddl = (project_root / "clickhouse" / "init" / "001_create_security_events.sql").read_text(
        encoding="utf-8"
    )

    assert "CREATE TABLE IF NOT EXISTS metrics.security_events" in ddl
    assert "TTL toDateTime(collected_at) + INTERVAL 7 DAY DELETE" in ddl
    assert "event_time" not in ddl.split("TTL", maxsplit=1)[1].split("COMMENT", maxsplit=1)[0]
    compose = (project_root / "docker-compose.yml").read_text(encoding="utf-8")
    clickhouse_service = compose.split("  clickhouse:", maxsplit=1)[1].split(
        "  grafana:", maxsplit=1
    )[0]
    assert "/var/lib/clickhouse" in clickhouse_service
    assert "clickhouse_data:" not in compose
    assert "KAFKA_LOG_RETENTION_MS: 86400000" in compose
    assert "KAFKA_LOG_ROLL_MS: 3600000" in compose
    assert "KAFKA_LOG_RETENTION_CHECK_INTERVAL_MS: 300000" in compose


def test_collector_interval_and_kafka_retention_defaults() -> None:
    """The documented local defaults match the requested retention horizon."""
    project_root = Path(__file__).resolve().parents[2]
    example_env = (project_root / ".env.example").read_text(encoding="utf-8")
    compose = (project_root / "docker-compose.yml").read_text(encoding="utf-8")

    assert "COLLECTION_INTERVAL_SECONDS=30" in example_env
    assert "KAFKA_LOG_RETENTION_MS: 86400000" in compose


def test_kafka_image_transient_volumes_do_not_create_anonymous_volumes() -> None:
    """Kafka image-declared scratch paths use tmpfs; broker logs use the named volume."""
    project_root = Path(__file__).resolve().parents[2]
    compose = (project_root / "docker-compose.yml").read_text(encoding="utf-8")
    kafka_service = compose.split("  kafka:", maxsplit=1)[1].split("  spark:", maxsplit=1)[0]

    assert "- /mnt/shared/config" in kafka_service
    assert "- /etc/kafka/secrets" in kafka_service
    assert "kafka_data:/var/lib/kafka/data" in kafka_service


def test_collector_env_loader_does_not_import_downstream_credentials(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The native collector reads only configuration it actually needs."""
    environment_file = tmp_path / ".env"
    environment_file.write_text(
        "HOST_ID_SALT=test-placeholder\nCLICKHOUSE_PASSWORD=credential-placeholder\n",
        encoding="utf-8",
    )
    monkeypatch.delenv("HOST_ID_SALT", raising=False)
    monkeypatch.delenv("CLICKHOUSE_PASSWORD", raising=False)

    load_env_file(environment_file, frozenset({"HOST_ID_SALT"}))

    assert "HOST_ID_SALT" in os.environ
    assert "CLICKHOUSE_PASSWORD" not in os.environ
