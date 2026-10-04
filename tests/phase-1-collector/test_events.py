"""Tests for the common security event contract."""

import json
from dataclasses import fields
from pathlib import Path

import pytest

from collector.events import SCHEMA_VERSION, create_security_event
from collector.models import SecurityEvent
from collector.serializers import serialize_event


def test_event_id_is_deterministic_and_excludes_collection_time() -> None:
    """Repeated observation of one source event produces the same UUID."""
    first = create_security_event(
        host_id="host-0123456789abcdef01234567",
        source="psutil",
        event_type="process.state",
        action="observed",
        event_time="2026-10-01T00:00:00.000000Z",
        collected_at="2026-10-01T00:00:01.000000Z",
        payload={"pid": 1, "process_name": "init"},
    )
    second = create_security_event(
        host_id=first.host_id,
        source=first.source,
        event_type=first.event_type,
        action=first.action,
        event_time=first.event_time,
        collected_at="2026-10-01T00:00:02.000000Z",
        payload={"process_name": "init", "pid": 1},
    )

    assert first.event_id == second.event_id
    assert first.collected_at != second.collected_at


def test_serialize_event_preserves_common_envelope_and_null() -> None:
    """Serialization emits compact UTF-8 JSON without dropping null values."""
    event = create_security_event(
        host_id="host-0123456789abcdef01234567",
        source="psutil",
        event_type="process.state",
        action="observed",
        event_time="2026-10-01T00:00:00.000000Z",
        collected_at="2026-10-01T00:00:01.000000Z",
        payload={
            "pid": 1,
            "ppid": None,
            "process_name": "init",
            "executable_path": None,
            "user_id": None,
            "start_time": None,
        },
    )

    serialized = serialize_event(event)
    decoded = json.loads(serialized)

    assert isinstance(serialized, bytes)
    assert decoded["schema_version"] == SCHEMA_VERSION
    assert decoded["payload"]["pid"] == 1
    assert decoded["payload"]["executable_path"] is None


def test_model_fields_match_json_schema_required_fields() -> None:
    """The Python model and checked-in JSON contract expose one envelope."""
    project_root = Path(__file__).resolve().parents[2]
    schema = json.loads(
        (project_root / "schemas" / "security_event.schema.json").read_text(encoding="utf-8")
    )

    assert schema["properties"]["schema_version"]["const"] == SCHEMA_VERSION
    assert set(schema["required"]) == {field.name for field in fields(SecurityEvent)}
    assert all(
        definition["additionalProperties"] is False for definition in schema["$defs"].values()
    )


def test_checked_in_example_uses_deterministic_event_id() -> None:
    """The documented sample follows the same event identifier rule as code."""
    project_root = Path(__file__).resolve().parents[2]
    sample = json.loads(
        (project_root / "schemas" / "security_event.example.json").read_text(encoding="utf-8")
    )
    rebuilt = create_security_event(
        host_id=sample["host_id"],
        source=sample["source"],
        event_type=sample["event_type"],
        action=sample["action"],
        event_time=sample["event_time"],
        collected_at=sample["collected_at"],
        payload=sample["payload"],
    )

    assert rebuilt.event_id == sample["event_id"]
    assert json.loads(serialize_event(rebuilt))["payload"] == sample["payload"]


def test_serializer_rejects_oversized_field_without_echoing_value() -> None:
    """Unexpectedly large telemetry is rejected before Kafka publication."""
    oversized_value = "x" * 5_000
    event = create_security_event(
        host_id="host-0123456789abcdef01234567",
        source="psutil",
        event_type="process.state",
        action="observed",
        event_time="2026-10-01T00:00:00.000000Z",
        collected_at="2026-10-01T00:00:01.000000Z",
        payload={
            "pid": 1,
            "ppid": 0,
            "process_name": oversized_value,
            "executable_path": None,
            "user_id": None,
            "start_time": None,
        },
    )

    with pytest.raises(ValueError, match="oversized") as error:
        serialize_event(event)

    assert oversized_value not in str(error.value)


def test_serializer_rejects_control_characters() -> None:
    """Control characters cannot enter Kafka through an allowlisted string."""
    event = create_security_event(
        host_id="host-0123456789abcdef01234567",
        source="psutil",
        event_type="process.state",
        action="observed",
        event_time="2026-10-01T00:00:00.000000Z",
        collected_at="2026-10-01T00:00:01.000000Z",
        payload={
            "pid": 1,
            "ppid": 0,
            "process_name": "unsafe\nname",
            "executable_path": None,
            "user_id": None,
            "start_time": None,
        },
    )

    with pytest.raises(ValueError, match="control character"):
        serialize_event(event)
