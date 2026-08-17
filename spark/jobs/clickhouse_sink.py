"""Shared ClickHouse HTTP sink helpers for Spark streaming jobs."""

import base64
import logging
import os
import re
from dataclasses import dataclass
from datetime import date, datetime
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from pyspark.sql import DataFrame, Row
from pyspark.sql.streaming import StreamingQuery

LOGGER = logging.getLogger(__file__)
_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True)
class ClickHouseHttpConfig:
    """ClickHouse HTTP connection settings.

    Attributes:
        url: ClickHouse HTTP endpoint URL.
        database: ClickHouse database name.
        user: ClickHouse user name.
        password: ClickHouse password.
        batch_size: Number of rows to send per HTTP insert request.
        timeout_seconds: HTTP request timeout in seconds.
    """

    url: str
    database: str
    user: str
    password: str
    batch_size: int
    timeout_seconds: float


def clickhouse_http_config_from_env() -> ClickHouseHttpConfig:
    """Build ClickHouse HTTP settings from environment variables.

    Returns:
        ClickHouse HTTP settings used by the streaming sink.

    Raises:
        ValueError: If required values are empty or numeric settings are invalid.
    """
    password = os.getenv("CLICKHOUSE_PASSWORD", "")
    if not password:
        raise ValueError("CLICKHOUSE_PASSWORD must be set before writing metrics to ClickHouse")

    batch_size = _positive_int_from_env("CLICKHOUSE_HTTP_BATCH_SIZE", "1000")
    timeout_seconds = _positive_float_from_env("CLICKHOUSE_HTTP_TIMEOUT_SECONDS", "10")

    return ClickHouseHttpConfig(
        url=os.getenv("CLICKHOUSE_HTTP_URL", "http://clickhouse:8123").rstrip("/"),
        database=os.getenv("CLICKHOUSE_DB", "metrics"),
        user=os.getenv("CLICKHOUSE_USER", "metrics"),
        password=password,
        batch_size=batch_size,
        timeout_seconds=timeout_seconds,
    )


def write_stream_to_clickhouse(
    metrics: DataFrame,
    checkpoint_location: str,
    table: str,
    config: ClickHouseHttpConfig,
) -> StreamingQuery:
    """Start a ClickHouse HTTP sink for a metric stream.

    Args:
        metrics: Structured metric streaming DataFrame.
        checkpoint_location: Directory in which Spark persists stream progress.
        table: Target ClickHouse table name.
        config: ClickHouse HTTP connection settings.

    Returns:
        Running streaming query.

    Raises:
        ValueError: If the table or column identifiers are invalid.
    """
    insert_query = _build_insert_query(table, metrics.columns)

    def write_batch(batch_metrics: DataFrame, batch_id: int) -> None:
        row_count = 0
        payload_lines: list[str] = []

        for row in batch_metrics.toLocalIterator():
            payload_lines.append(_row_to_tab_separated(row))
            row_count += 1
            if len(payload_lines) >= config.batch_size:
                _insert_tab_separated(config, insert_query, payload_lines)
                payload_lines = []

        if payload_lines:
            _insert_tab_separated(config, insert_query, payload_lines)

        LOGGER.info(
            "Wrote micro-batch %s to ClickHouse table %s",
            batch_id,
            table,
            extra={"rows": row_count},
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


def _positive_float_from_env(name: str, default: str) -> float:
    raw_value = os.getenv(name, default)
    try:
        value = float(raw_value)
    except ValueError as exc:
        raise ValueError(f"{name} must be a number") from exc
    if value <= 0:
        raise ValueError(f"{name} must be greater than zero")
    return value


def _build_insert_query(table: str, columns: list[str]) -> str:
    formatted_table = _format_table_identifier(table)
    formatted_columns = ", ".join(_format_identifier(column) for column in columns)
    return f"INSERT INTO {formatted_table} ({formatted_columns}) FORMAT TabSeparated"


def _format_table_identifier(table: str) -> str:
    parts = table.split(".")
    if not parts or any(not part for part in parts):
        raise ValueError("ClickHouse table name must not be empty")
    return ".".join(_format_identifier(part) for part in parts)


def _format_identifier(identifier: str) -> str:
    if _IDENTIFIER_PATTERN.fullmatch(identifier) is None:
        raise ValueError(f"Invalid ClickHouse identifier: {identifier}")
    return f"`{identifier}`"


def _row_to_tab_separated(row: Row) -> str:
    values = [_serialize_clickhouse_value(value) for value in row.asDict(recursive=False).values()]
    return "\t".join(values)


def _serialize_clickhouse_value(value: object) -> str:
    if value is None:
        return r"\N"
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S.%f")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, int | float):
        return str(value)
    return _escape_tab_separated_value(str(value))


def _escape_tab_separated_value(value: str) -> str:
    return (
        value.replace("\\", "\\\\")
        .replace("\t", "\\t")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\b", "\\b")
        .replace("\f", "\\f")
        .replace("\0", "\\0")
    )


def _insert_tab_separated(
    config: ClickHouseHttpConfig,
    insert_query: str,
    payload_lines: list[str],
) -> None:
    query_string = urlencode({"database": config.database, "query": insert_query})
    request = Request(
        url=f"{config.url}/?{query_string}",
        data=("\n".join(payload_lines) + "\n").encode("utf-8"),
        method="POST",
        headers={
            "Authorization": _basic_auth_header(config.user, config.password),
            "Content-Type": "application/x-ndjson",
        },
    )

    try:
        with urlopen(request, timeout=config.timeout_seconds) as response:
            response.read()
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"ClickHouse insert failed with HTTP {exc.code}: {body}") from exc
    except URLError as exc:
        raise RuntimeError(f"ClickHouse insert request failed: {exc.reason}") from exc


def _basic_auth_header(user: str, password: str) -> str:
    token = base64.b64encode(f"{user}:{password}".encode()).decode("ascii")
    return f"Basic {token}"
