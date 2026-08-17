"""Tests for shared ClickHouse Spark sink configuration."""

import pytest
from pyspark.sql import Row

from spark.jobs.clickhouse_sink import (
    _build_insert_query,
    _row_to_tab_separated,
    clickhouse_http_config_from_env,
)


def test_clickhouse_http_config_uses_defaults_and_required_password(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """ClickHouse sink configuration is read from environment variables."""
    monkeypatch.setenv("CLICKHOUSE_PASSWORD", "secret")
    monkeypatch.delenv("CLICKHOUSE_DB", raising=False)
    monkeypatch.delenv("CLICKHOUSE_HTTP_URL", raising=False)
    monkeypatch.delenv("CLICKHOUSE_USER", raising=False)
    monkeypatch.delenv("CLICKHOUSE_HTTP_BATCH_SIZE", raising=False)
    monkeypatch.delenv("CLICKHOUSE_HTTP_TIMEOUT_SECONDS", raising=False)

    config = clickhouse_http_config_from_env()

    assert config.url == "http://clickhouse:8123"
    assert config.database == "metrics"
    assert config.user == "metrics"
    assert config.password == "secret"
    assert config.batch_size == 1000
    assert config.timeout_seconds == 10


def test_clickhouse_http_config_rejects_missing_password(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """ClickHouse password is required before starting the stream."""
    monkeypatch.delenv("CLICKHOUSE_PASSWORD", raising=False)

    with pytest.raises(ValueError, match="CLICKHOUSE_PASSWORD"):
        clickhouse_http_config_from_env()


def test_clickhouse_http_config_rejects_invalid_batch_size(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """ClickHouse HTTP batch size must be a positive integer."""
    monkeypatch.setenv("CLICKHOUSE_PASSWORD", "secret")
    monkeypatch.setenv("CLICKHOUSE_HTTP_BATCH_SIZE", "0")

    with pytest.raises(ValueError, match="greater than zero"):
        clickhouse_http_config_from_env()


def test_build_insert_query_uses_tab_separated_format() -> None:
    """ClickHouse insert query targets explicit columns with a compact row format."""
    query = _build_insert_query("fact_host_metrics", ["timestamp", "hostname"])

    assert query == "INSERT INTO `fact_host_metrics` (`timestamp`, `hostname`) FORMAT TabSeparated"


def test_row_to_tab_separated_escapes_string_values() -> None:
    """TabSeparated rows escape delimiters and special characters."""
    row = Row(name="a\tb\nc\\d", value=1, missing=None)

    assert _row_to_tab_separated(row) == r"a\tb\nc\\d	1	\N"
