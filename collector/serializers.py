"""JSON serialization for common security events."""

import json
from dataclasses import asdict

from collector.models import SecurityEvent

_ALLOWED_PAYLOAD_FIELDS = {
    "process.state": frozenset(
        {"pid", "ppid", "process_name", "executable_path", "user_id", "start_time"}
    ),
    "network.connection": frozenset(
        {
            "local_address",
            "local_port",
            "remote_address",
            "remote_port",
            "connection_status",
            "pid",
        }
    ),
    "docker.container.lifecycle": frozenset({"container_id"}),
}
_MAX_STRING_LENGTH = 4_096
_MAX_EVENT_BYTES = 32_768


def serialize_event(event: SecurityEvent) -> bytes:
    """Serialize a security event as compact UTF-8 JSON.

    Args:
        event: Common security event to serialize.

    Returns:
        UTF-8 encoded JSON object.

    Raises:
        ValueError: If an event type or payload field is outside the phase-1
            collection allowlist.
    """
    allowed_fields = _ALLOWED_PAYLOAD_FIELDS.get(event.event_type)
    if allowed_fields is None:
        raise ValueError(f"Unsupported event type: {event.event_type}")
    payload_fields = set(event.payload)
    if payload_fields != allowed_fields:
        unexpected = sorted(payload_fields - allowed_fields)
        missing = sorted(allowed_fields - payload_fields)
        raise ValueError(f"Payload violates allowlist; unexpected={unexpected}, missing={missing}")
    string_values = [
        event.schema_version,
        event.event_id,
        event.event_time,
        event.collected_at,
        event.host_id,
        event.source,
        event.event_type,
        event.action,
        *(value for value in event.payload.values() if isinstance(value, str)),
    ]
    if any(len(value) > _MAX_STRING_LENGTH for value in string_values):
        raise ValueError("Event contains an oversized string field")
    if any(
        ord(character) < 32 or ord(character) == 127
        for value in string_values
        for character in value
    ):
        raise ValueError("Event contains a control character")
    serialized = json.dumps(asdict(event), separators=(",", ":"), ensure_ascii=False).encode(
        "utf-8"
    )
    if len(serialized) > _MAX_EVENT_BYTES:
        raise ValueError("Serialized event exceeds the size limit")
    return serialized
