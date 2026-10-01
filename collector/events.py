"""Construction and identification of common security events."""

import json
from collections.abc import Mapping
from uuid import UUID, uuid5

from collector.models import JsonScalar, SecurityEvent

SCHEMA_VERSION = "1.0.0"
_EVENT_NAMESPACE = UUID("d6c269a6-5f54-4bed-b004-a06f5dfbf7ec")


def create_security_event(
    *,
    host_id: str,
    source: str,
    event_type: str,
    action: str,
    event_time: str,
    collected_at: str,
    payload: Mapping[str, JsonScalar],
) -> SecurityEvent:
    """Create a common event with a deterministic content identifier.

    The identifier is UUIDv5 over the schema version, host, source, type,
    action, event time, and canonical payload. ``collected_at`` is deliberately
    excluded so collecting the same source event again yields the same ID.

    Args:
        host_id: Stable pseudonymous source-host identifier.
        source: Collector that produced the event.
        event_type: Namespaced event category.
        action: Observed action.
        event_time: UTC source event time.
        collected_at: UTC collector observation time.
        payload: Event-specific JSON scalar fields.

    Returns:
        Immutable common security event.
    """
    event_payload = dict(payload)
    identity = json.dumps(
        {
            "schema_version": SCHEMA_VERSION,
            "host_id": host_id,
            "source": source,
            "event_type": event_type,
            "action": action,
            "event_time": event_time,
            "payload": event_payload,
        },
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return SecurityEvent(
        schema_version=SCHEMA_VERSION,
        event_id=str(uuid5(_EVENT_NAMESPACE, identity)),
        event_time=event_time,
        collected_at=collected_at,
        host_id=host_id,
        source=source,
        event_type=event_type,
        action=action,
        payload=event_payload,
    )
