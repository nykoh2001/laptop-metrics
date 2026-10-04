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
    write_to_clickhouse,
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
    assert "date_time_input_format=best_effort" in url
    assert "secret" not in url


def test_insert_batch_posts_json_each_row_without_password_in_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A micro-batch is sent as newline-delimited JSON with header credentials."""

    class FakeJsonRows:
        """Provide the iterator exposed by a Spark JSON DataFrame."""

        def foreachPartition(self, function):  # type: ignore[no-untyped-def]
            """Run one task with several serialized rows."""
            function(iter((valid_clickhouse_row(),) * 3))

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
    inserted = [json.loads(row) for row in captured[0].data.decode().splitlines()]
    assert len(inserted) == 3
    assert inserted[0]["event_id"] == "00000000-0000-5000-8000-000000000001"
    assert json.loads(inserted[0]["payload"])["process_name"] == "launchd"
    assert captured[0].get_header("X-clickhouse-key") == "private-password"


def test_partition_insert_splits_large_batches_by_bounded_row_count(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Large Spark partitions are flushed in bounded multi-row inserts."""

    class FakeJsonRows:
        """Provide rows through a Spark-style partition callback."""

        def foreachPartition(self, function):  # type: ignore[no-untyped-def]
            """Run one task with two serialized rows."""
            function(iter((valid_clickhouse_row(),) * 2))

    class FakeEvents:
        """Provide only the DataFrame operation used by the sink."""

        def toJSON(self) -> FakeJsonRows:
            """Return fake JSON rows."""
            return FakeJsonRows()

    class FakeResponse:
        """Act as a successful ClickHouse response."""

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
        return FakeResponse()

    monkeypatch.setattr(sink_module, "urlopen", fake_urlopen)
    monkeypatch.setattr(sink_module, "_MAX_INSERT_ROWS", 1)
    insert_batch(
        FakeEvents(),
        2,
        ClickHouseConfig(
            url="http://clickhouse:8123/",
            database="metrics",
            user="metrics",
            password="credential-placeholder",
        ),
    )  # type: ignore[arg-type]

    assert len(captured) == 2
    assert all(len(request.data.decode().splitlines()) == 1 for request in captured)


def test_streaming_sink_uses_30_second_trigger() -> None:
    """The Spark polling cadence matches the 30-second collector cadence."""

    class FakeQueryBuilder:
        """Capture the streaming options without starting Spark."""

        trigger_interval: str | None = None

        def outputMode(self, mode: str) -> "FakeQueryBuilder":
            assert mode == "append"
            return self

        def option(self, key: str, value: str) -> "FakeQueryBuilder":
            assert key == "checkpointLocation"
            assert value == "/checkpoint"
            return self

        def trigger(self, processingTime: str) -> "FakeQueryBuilder":
            self.trigger_interval = processingTime
            return self

        def foreachBatch(self, callback):  # type: ignore[no-untyped-def]
            assert callable(callback)
            return self

        def start(self) -> "FakeQueryBuilder":
            return self

    class FakeEvents:
        """Provide the Spark DataFrame streaming writer interface."""

        writeStream = FakeQueryBuilder()

    query = write_to_clickhouse(
        FakeEvents(),  # type: ignore[arg-type]
        "/checkpoint",
        ClickHouseConfig(
            url="http://clickhouse:8123/",
            database="metrics",
            user="metrics",
            password="credential-placeholder",
        ),
    )

    assert isinstance(query, FakeQueryBuilder)
    assert query.trigger_interval == "30 seconds"


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
