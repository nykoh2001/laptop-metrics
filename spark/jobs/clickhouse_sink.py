"""Shared ClickHouse JDBC sink helpers for Spark streaming jobs."""

import logging
import os
from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery

LOGGER = logging.getLogger(__file__)


@dataclass(frozen=True)
class ClickHouseJdbcConfig:
    """ClickHouse JDBC connection settings.

    Attributes:
        url: JDBC URL for the ClickHouse HTTP endpoint and database.
        user: ClickHouse user name.
        password: ClickHouse password.
        driver: Fully qualified JDBC driver class name.
        batch_size: Number of rows Spark should send per JDBC batch.
    """

    url: str
    user: str
    password: str
    driver: str
    batch_size: int


def clickhouse_jdbc_config_from_env() -> ClickHouseJdbcConfig:
    """Build ClickHouse JDBC settings from environment variables.

    Returns:
        ClickHouse JDBC settings used by Spark's JDBC writer.

    Raises:
        ValueError: If ``CLICKHOUSE_PASSWORD`` is empty or the batch size is invalid.
    """
    password = os.getenv("CLICKHOUSE_PASSWORD", "")
    if not password:
        raise ValueError("CLICKHOUSE_PASSWORD must be set before writing metrics to ClickHouse")

    batch_size = _positive_int_from_env("CLICKHOUSE_JDBC_BATCH_SIZE", "1000")
    database = os.getenv("CLICKHOUSE_DB", "metrics")

    return ClickHouseJdbcConfig(
        url=os.getenv("CLICKHOUSE_JDBC_URL", f"jdbc:clickhouse://clickhouse:8123/{database}"),
        user=os.getenv("CLICKHOUSE_USER", "metrics"),
        password=password,
        driver=os.getenv("CLICKHOUSE_JDBC_DRIVER", "com.clickhouse.jdbc.ClickHouseDriver"),
        batch_size=batch_size,
    )


def write_stream_to_clickhouse(
    metrics: DataFrame,
    checkpoint_location: str,
    table: str,
    config: ClickHouseJdbcConfig,
) -> StreamingQuery:
    """Start a ClickHouse JDBC sink for a metric stream.

    Args:
        metrics: Structured metric streaming DataFrame.
        checkpoint_location: Directory in which Spark persists stream progress.
        table: Target ClickHouse table name.
        config: ClickHouse JDBC connection settings.

    Returns:
        Running streaming query.
    """

    def write_batch(batch_metrics: DataFrame, batch_id: int) -> None:
        LOGGER.info("Writing micro-batch %s to ClickHouse table %s", batch_id, table)
        (
            batch_metrics.write.format("jdbc")
            .mode("append")
            .option("url", config.url)
            .option("dbtable", table)
            .option("user", config.user)
            .option("password", config.password)
            .option("driver", config.driver)
            .option("batchsize", str(config.batch_size))
            .option("isolationLevel", "NONE")
            .save()
        )

    return (
        metrics.writeStream.foreachBatch(write_batch)
        .outputMode("append")
        .option("checkpointLocation", checkpoint_location)
        .start()
    )


def _positive_int_from_env(name: str, default: str) -> int:
    raw_value = os.getenv(name, default)
    try:
        value = int(raw_value)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if value <= 0:
        raise ValueError(f"{name} must be greater than zero")
    return value
