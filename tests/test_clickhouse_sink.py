"""Tests for the minimal ClickHouse HTTP sink."""

import json
from urllib.request import Request

import pytest

import spark.jobs.security_events_to_clickhouse as sink_module
from spark.jobs.security_events_to_clickhouse import (
    ClickHouseConfig,
    clickhouse_insert_url,
    insert_batch,
    validate_clickhouse_json_row,
)


def valid_clickhouse_row() -> str:
    """Return one safe Spark-style JSON row for sink tests."""
    return json.dumps(
        {
            "schema_version": "1.0.0",
            "event_id": "00000000-0000-5000-8000-000000000001",
            "event_time": "2026-10-01T00:00:00.000Z",
            "collected_at": "2026-10-01T00:00:01.000Z",
            "host_id": "host-0123456789abcdef01234567",
            "source": "psutil",
            "event_type": "process.state",
            "action": "observed",
            "payload": json.dumps(
                {
                    "pid": 1,
                    "ppid": 0,
                    "process_name": "launchd",
                    "executable_path": "/sbin/launchd",
                    "user_id": "user-0123456789abcdef01234567",
                    "start_time": "2026-10-01T00:00:00.000Z",
                }
            ),
        }
    )


def test_clickhouse_insert_url_encodes_database_and_query() -> None:
    """The sink targets only the phase-1 security event table."""
    config = ClickHouseConfig(
        url="http://clickhouse:8123/",
        database="metrics-local",
        user="metrics",
        password="secret",
    )

    url = clickhouse_insert_url(config)

    assert url.startswith("http://clickhouse:8123?")
    assert "database=metrics-local" in url
    assert "INSERT+INTO+security_events+FORMAT+JSONEachRow" in url
    assert "secret" not in url


def test_insert_batch_posts_json_each_row_without_password_in_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A micro-batch is sent as newline-delimited JSON with header credentials."""

    class FakeJsonRows:
        """Provide the iterator exposed by a Spark JSON DataFrame."""

        def toLocalIterator(self):  # type: ignore[no-untyped-def]
            """Return one serialized row."""
            return iter((valid_clickhouse_row(),))

    class FakeEvents:
        """Provide only the DataFrame operation used by the sink."""

        def toJSON(self) -> FakeJsonRows:
            """Return fake JSON rows."""
            return FakeJsonRows()

    class FakeResponse:
        """Act as a successful urlopen context manager."""

        def __enter__(self) -> "FakeResponse":
            """Enter the response context."""
            return self

        def __exit__(self, *args: object) -> None:
            """Exit the response context."""

        def read(self) -> bytes:
            """Return an empty ClickHouse response body."""
            return b""

    captured: list[Request] = []

    def fake_urlopen(request: Request, timeout: int) -> FakeResponse:
        captured.append(request)
        assert timeout == 10
        return FakeResponse()

    monkeypatch.setattr(sink_module, "urlopen", fake_urlopen)
    config = ClickHouseConfig(
        url="http://clickhouse:8123/",
        database="metrics",
        user="metrics",
        password="private-password",
    )

    insert_batch(FakeEvents(), 1, config)  # type: ignore[arg-type]

    assert len(captured) == 1
    assert "private-password" not in captured[0].full_url
    assert captured[0].data is not None
    inserted = json.loads(captured[0].data)
    assert inserted["event_id"] == "00000000-0000-5000-8000-000000000001"
    assert json.loads(inserted["payload"])["process_name"] == "launchd"
    assert captured[0].get_header("X-clickhouse-key") == "private-password"


@pytest.mark.parametrize(
    "unsafe_row",
    [
        "{malformed",
        "x" * 40_000,
        json.dumps(
            {
                **json.loads(valid_clickhouse_row()),
                "payload": json.dumps(
                    {
                        **json.loads(json.loads(valid_clickhouse_row())["payload"]),
                        "command_line": "tool --token placeholder",
                    }
                ),
            }
        ),
    ],
)
def test_sink_rejects_malformed_oversized_and_non_allowlisted_rows(unsafe_row: str) -> None:
    """Untrusted Kafka data cannot bypass the storage allowlist."""
    assert validate_clickhouse_json_row(unsafe_row) is None
