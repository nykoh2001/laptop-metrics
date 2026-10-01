"""Consistency checks for duplicated trust-boundary allowlists."""

import json
from pathlib import Path

import collector.serializers as collector_contract
import spark.jobs.security_events_to_clickhouse as sink_contract


def test_payload_allowlists_match_json_schema_and_sink() -> None:
    """Collector, documented schema, and sink reject the same extra fields."""
    project_root = Path(__file__).resolve().parents[1]
    schema = json.loads(
        (project_root / "schemas" / "security_event.schema.json").read_text(encoding="utf-8")
    )
    schema_fields = {
        "process.state": frozenset(schema["$defs"]["processPayload"]["properties"]),
        "network.connection": frozenset(schema["$defs"]["networkPayload"]["properties"]),
        "docker.container.lifecycle": frozenset(schema["$defs"]["dockerPayload"]["properties"]),
    }

    assert schema_fields == collector_contract._ALLOWED_PAYLOAD_FIELDS
    assert schema_fields == sink_contract._PAYLOAD_FIELDS
