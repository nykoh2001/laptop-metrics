"""Integration tests for common-event parsing with a local Spark session."""

import json
from collections.abc import Iterator
from urllib.request import Request

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format

import spark.jobs.security_events_to_clickhouse as sink_module
from collector.events import create_security_event
from collector.serializers import serialize_event
from spark.jobs.security_events_to_clickhouse import (
    ClickHouseConfig,
    insert_batch,
    parse_security_event_records,
)


@pytest.fixture(scope="module")
def spark_session() -> Iterator[SparkSession]:
    """Provide one small local Spark session for parsing tests."""
    session = (
        SparkSession.builder.master("local[1]")
        .appName("security-event-parsing-tests")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    try:
        yield session
    finally:
        session.stop()


def test_security_event_is_parsed_for_clickhouse(spark_session: SparkSession) -> None:
    """Kafka JSON becomes typed envelope columns while payload types survive."""
    event = create_security_event(
        host_id="host-0123456789abcdef01234567",
        source="psutil",
        event_type="network.connection",
        action="observed",
        event_time="2026-10-01T00:00:00.000000Z",
        collected_at="2026-10-01T00:00:01.000000Z",
        payload={
            "local_address": "127.0.0.1",
            "local_port": 8080,
            "remote_address": None,
            "remote_port": None,
            "connection_status": "LISTEN",
            "pid": 10,
        },
    )
    kafka_records = spark_session.createDataFrame([(serialize_event(event),)], "value binary")

    row = (
        parse_security_event_records(kafka_records)
        .withColumn(
            "event_time_text",
            date_format("event_time", "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'"),
        )
        .first()
    )

    assert row is not None
    assert row.event_id == event.event_id
    assert row.event_time_text == "2026-10-01T00:00:00.000000Z"
    assert row.event_type == "network.connection"
    payload = json.loads(row.payload)
    assert payload["local_port"] == 8080
    assert payload["remote_address"] is None


def test_parsed_event_passes_storage_guard_and_http_sink(
    spark_session: SparkSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A collector event survives parse, validation, and sink serialization."""
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
            "process_name": "launchd",
            "executable_path": "/sbin/launchd",
            "user_id": "user-0123456789abcdef01234567",
            "start_time": "2026-10-01T00:00:00.000000Z",
        },
    )
    kafka_records = spark_session.createDataFrame([(serialize_event(event),)], "value binary")
    parsed = parse_security_event_records(kafka_records)

    class FakeResponse:
        """Act as a successful ClickHouse response."""

        def __enter__(self) -> "FakeResponse":
            """Enter the context."""
            return self

        def __exit__(self, *args: object) -> None:
            """Exit the context."""

        def read(self) -> bytes:
            """Return an empty response body."""
            return b""

    captured: list[Request] = []

    def fake_urlopen(request: Request, timeout: int) -> FakeResponse:
        captured.append(request)
        assert timeout == 10
        return FakeResponse()

    monkeypatch.setattr(sink_module, "urlopen", fake_urlopen)
    insert_batch(
        parsed,
        0,
        ClickHouseConfig(
            url="http://clickhouse:8123/",
            database="metrics",
            user="metrics",
            password="credential-placeholder",
        ),
    )

    assert len(captured) == 1
    assert captured[0].data is not None
    inserted = json.loads(captured[0].data)
    assert inserted["event_id"] == event.event_id
    assert json.loads(inserted["payload"])["process_name"] == "launchd"
