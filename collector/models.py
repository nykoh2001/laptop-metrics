"""Versioned security telemetry event model."""

from dataclasses import dataclass

JsonScalar = str | int | float | bool | None


@dataclass(frozen=True, slots=True)
class SecurityEvent:
    """One event in the common security telemetry envelope.

    Args:
        schema_version: Version of the common event contract.
        event_id: Deterministic identifier for the event contents.
        event_time: UTC time at which the observed event occurred.
        collected_at: UTC time at which the collector observed the event.
        host_id: Stable pseudonymous identifier for the source host.
        source: Collector that produced the event.
        event_type: Namespaced category of the event.
        action: Observed lifecycle or state action.
        payload: Event-specific fields containing JSON scalar values.
    """

    schema_version: str
    event_id: str
    event_time: str
    collected_at: str
    host_id: str
    source: str
    event_type: str
    action: str
    payload: dict[str, JsonScalar]
