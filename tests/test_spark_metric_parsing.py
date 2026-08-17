"""Integration tests for metric parsing with a local Spark session."""

from collections.abc import Iterator

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format

from collector.models import HostMetric, ProcessMetric
from collector.serializers import serialize_metric
from spark.jobs.host_metrics_sink import parse_host_metric_records
from spark.jobs.process_metrics_sink import parse_process_metric_records


@pytest.fixture(scope="module")
def spark_session() -> Iterator[SparkSession]:
    """Provide one small local Spark session for parsing tests."""
    session = (
        SparkSession.builder.master("local[1]")
        .appName("metric-parsing-tests")
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


def test_host_metric_payload_is_parsed_into_typed_columns(
    spark_session: SparkSession,
) -> None:
    """Collector host JSON becomes the typed row consumed by Spark."""
    metric = HostMetric(
        timestamp="2026-01-01T00:00:00.000000Z",
        hostname="test-host",
        cpu_usage_percent=10.5,
        memory_usage_percent=20.5,
        total_memory_bytes=100,
        available_memory_bytes=80,
        swap_usage_percent=0.0,
        total_swap_bytes=0,
        used_swap_bytes=0,
        disk_usage_percent=30.5,
        disk_read_bytes=1,
        disk_write_bytes=2,
        network_bytes_sent=3,
        network_bytes_received=4,
    )
    kafka_records = spark_session.createDataFrame(
        [(serialize_metric(metric),)],
        "value binary",
    )

    parsed = (
        parse_host_metric_records(kafka_records)
        .withColumn(
            "timestamp_text",
            date_format("timestamp", "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'"),
        )
        .first()
    )

    assert parsed is not None
    assert parsed.timestamp_text == "2026-01-01T00:00:00.000000Z"
    assert parsed.hostname == "test-host"
    assert parsed.cpu_usage_percent == 10.5
    assert parsed.total_memory_bytes == 100
    assert parsed.network_bytes_received == 4


def test_process_metric_payload_is_parsed_into_typed_columns(
    spark_session: SparkSession,
) -> None:
    """Collector process JSON becomes the typed row consumed by Spark."""
    metric = ProcessMetric(
        timestamp="2026-01-01T00:00:00.000000Z",
        pid=123,
        process_name="test-process",
        cpu_usage_percent=1.5,
        memory_usage_percent=2.5,
        rss_memory_bytes=4096,
        process_status="running",
    )
    kafka_records = spark_session.createDataFrame(
        [(serialize_metric(metric),)],
        "value binary",
    )

    parsed = (
        parse_process_metric_records(kafka_records)
        .withColumn(
            "timestamp_text",
            date_format("timestamp", "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'"),
        )
        .first()
    )

    assert parsed is not None
    assert parsed.timestamp_text == "2026-01-01T00:00:00.000000Z"
    assert parsed.pid == 123
    assert parsed.process_name == "test-process"
    assert parsed.rss_memory_bytes == 4096
    assert parsed.process_status == "running"
