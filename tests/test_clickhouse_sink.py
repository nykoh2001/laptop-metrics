"""Tests for shared ClickHouse Spark sink configuration."""

import pytest

from spark.jobs.clickhouse_sink import (
    clickhouse_jdbc_config_from_env,
)


def test_clickhouse_jdbc_config_uses_defaults_and_required_password(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """ClickHouse sink configuration is read from environment variables."""
    monkeypatch.setenv("CLICKHOUSE_PASSWORD", "secret")
    monkeypatch.delenv("CLICKHOUSE_DB", raising=False)
    monkeypatch.delenv("CLICKHOUSE_JDBC_URL", raising=False)
    monkeypatch.delenv("CLICKHOUSE_USER", raising=False)
    monkeypatch.delenv("CLICKHOUSE_JDBC_DRIVER", raising=False)
    monkeypatch.delenv("CLICKHOUSE_JDBC_BATCH_SIZE", raising=False)

    config = clickhouse_jdbc_config_from_env()

    assert config.url == "jdbc:clickhouse://clickhouse:8123/metrics"
    assert config.user == "metrics"
    assert config.password == "secret"
    assert config.driver == "com.clickhouse.jdbc.ClickHouseDriver"
    assert config.batch_size == 1000


def test_clickhouse_jdbc_config_rejects_missing_password(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """ClickHouse password is required before starting the stream."""
    monkeypatch.delenv("CLICKHOUSE_PASSWORD", raising=False)

    with pytest.raises(ValueError, match="CLICKHOUSE_PASSWORD"):
        clickhouse_jdbc_config_from_env()


def test_clickhouse_jdbc_config_rejects_invalid_batch_size(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """ClickHouse JDBC batch size must be a positive integer."""
    monkeypatch.setenv("CLICKHOUSE_PASSWORD", "secret")
    monkeypatch.setenv("CLICKHOUSE_JDBC_BATCH_SIZE", "0")

    with pytest.raises(ValueError, match="greater than zero"):
        clickhouse_jdbc_config_from_env()
