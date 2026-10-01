"""Stream common security events from Kafka into ClickHouse."""

import json
import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from uuid import UUID

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json, get_json_object, to_timestamp
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import StringType, StructField, StructType

LOGGER = logging.getLogger(__name__)
_UTC = timezone.utc  # noqa: UP017 - The pinned Spark image runs Python 3.10.

_COMMON_FIELDS = frozenset(
    {
        "schema_version",
        "event_id",
        "event_time",
        "collected_at",
        "host_id",
        "source",
        "event_type",
        "action",
        "payload",
    }
)
_PAYLOAD_FIELDS = {
    "process.state": frozenset(
        {"pid", "ppid", "process_name", "executable_path", "user_id", "start_time"}
    ),
    "network.connection": frozenset(
        {
            "local_address",
            "local_port",
            "remote_address",
            "remote_port",
            "connection_status",
            "pid",
        }
    ),
    "docker.container.lifecycle": frozenset({"container_id"}),
}
_MAX_ROW_BYTES = 32_768
_MAX_STRING_LENGTH = 4_096
_HOST_ID_PATTERN = re.compile(r"^host-[0-9a-f]{24}$")
_USER_ID_PATTERN = re.compile(r"^user-[0-9a-f]{24}$")


@dataclass(frozen=True, slots=True)
class ClickHouseConfig:
    """Connection values needed by the minimal HTTP sink."""

    url: str
    database: str
    user: str
    password: str


def security_event_envelope_schema() -> StructType:
    """Build the Spark schema for common envelope fields.

    ``payload`` is extracted separately as raw JSON so event-specific scalar
    types are preserved without premature normalization.

    Returns:
        Spark schema for all common fields except ``payload``.
    """
    return StructType(
        [
            StructField("schema_version", StringType(), nullable=False),
            StructField("event_id", StringType(), nullable=False),
            StructField("event_time", StringType(), nullable=False),
            StructField("collected_at", StringType(), nullable=False),
            StructField("host_id", StringType(), nullable=False),
            StructField("source", StringType(), nullable=False),
            StructField("event_type", StringType(), nullable=False),
            StructField("action", StringType(), nullable=False),
        ]
    )


def parse_security_event_records(kafka_records: DataFrame) -> DataFrame:
    """Parse Kafka values into ClickHouse-compatible common event columns.

    Args:
        kafka_records: DataFrame containing a binary or string ``value`` column.

    Returns:
        DataFrame with typed timestamps and raw JSON payload strings.
    """
    raw_records = kafka_records.select(col("value").cast("string").alias("raw_json"))
    parsed_records = raw_records.select(
        from_json(col("raw_json"), security_event_envelope_schema()).alias("event"),
        get_json_object(col("raw_json"), "$.payload").alias("payload"),
    )
    return (
        parsed_records.select("event.*", "payload")
        .withColumn("event_time", to_timestamp(col("event_time")))
        .withColumn("collected_at", to_timestamp(col("collected_at")))
    )


def read_security_events(spark: SparkSession, bootstrap_servers: str, topic: str) -> DataFrame:
    """Create a structured stream of common security events from Kafka.

    Args:
        spark: Active Spark session.
        bootstrap_servers: Comma-separated Kafka broker addresses.
        topic: Kafka topic containing common security event JSON.

    Returns:
        Parsed streaming DataFrame.
    """
    kafka_records = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .load()
    )
    return parse_security_event_records(kafka_records)


def clickhouse_insert_url(config: ClickHouseConfig) -> str:
    """Build the ClickHouse HTTP insert URL.

    Args:
        config: ClickHouse connection configuration.

    Returns:
        URL containing the database and insert query.
    """
    parameters = urlencode(
        {
            "database": config.database,
            "query": "INSERT INTO security_events FORMAT JSONEachRow",
            "date_time_input_format": "best_effort",
        }
    )
    return f"{config.url.rstrip('/')}?{parameters}"


def insert_batch(events: DataFrame, _batch_id: int, config: ClickHouseConfig) -> None:
    """Insert one Spark micro-batch through ClickHouse's HTTP interface.

    This intentionally simple driver-side sink is sufficient for the local
    phase-1 volume and avoids introducing another production dependency.

    Args:
        events: Parsed micro-batch DataFrame.
        _batch_id: Spark micro-batch identifier.
        config: ClickHouse connection configuration.

    Raises:
        OSError: If ClickHouse cannot be reached or rejects the insert.
    """
    raw_rows = list(events.toJSON().toLocalIterator())
    rows = [validated for row in raw_rows if (validated := validate_clickhouse_json_row(row))]
    rejected_event_count = len(raw_rows) - len(rows)
    if rejected_event_count:
        LOGGER.warning(
            "Rejected unsafe security events before ClickHouse insert",
            extra={"rejected_event_count": rejected_event_count},
        )
    if not rows:
        return
    request = Request(
        clickhouse_insert_url(config),
        data=("\n".join(rows) + "\n").encode("utf-8"),
        headers={
            "Content-Type": "application/x-ndjson",
            "X-ClickHouse-User": config.user,
            "X-ClickHouse-Key": config.password,
        },
        method="POST",
    )
    with urlopen(request, timeout=10) as response:
        response.read()


def validate_clickhouse_json_row(json_row: str) -> str | None:
    """Validate and sanitize one sink row without logging its contents.

    This is a minimal trust-boundary safety check. It does not implement the
    phase-3 normalization, DLQ, or general schema-evolution behavior.

    Args:
        json_row: Spark JSON representation of one parsed Kafka record.

    Returns:
        Canonical JSONEachRow string, or null when the record is unsafe.
    """
    if len(json_row.encode("utf-8")) > _MAX_ROW_BYTES or "\ufffd" in json_row:
        return None
    try:
        record = json.loads(json_row)
    except (json.JSONDecodeError, RecursionError, UnicodeError):
        return None
    if not isinstance(record, dict) or set(record) != _COMMON_FIELDS:
        return None
    if record.get("schema_version") != "1.0.0":
        return None
    common_strings = (
        record.get("event_id"),
        record.get("host_id"),
        record.get("source"),
        record.get("event_type"),
        record.get("action"),
    )
    if not all(_valid_text(value, _MAX_STRING_LENGTH) for value in common_strings):
        return None
    if not _valid_uuid(record["event_id"]):
        return None
    if not _HOST_ID_PATTERN.fullmatch(record["host_id"]):
        return None
    if _parse_timestamp(record.get("event_time")) is None:
        return None
    if _parse_timestamp(record.get("collected_at")) is None:
        return None
    payload_json = record.get("payload")
    if not isinstance(payload_json, str) or len(payload_json.encode("utf-8")) > _MAX_ROW_BYTES:
        return None
    try:
        payload = json.loads(payload_json)
    except (json.JSONDecodeError, RecursionError, UnicodeError):
        return None
    event_type = record["event_type"]
    allowed_fields = _PAYLOAD_FIELDS.get(event_type)
    if not isinstance(payload, dict) or allowed_fields is None or set(payload) != allowed_fields:
        return None
    if not _valid_payload(event_type, record["source"], record["action"], payload):
        return None
    record["payload"] = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    sanitized = json.dumps(record, ensure_ascii=False, separators=(",", ":"))
    return sanitized if len(sanitized.encode("utf-8")) <= _MAX_ROW_BYTES else None


def _valid_payload(event_type: str, source: str, action: str, payload: dict[str, object]) -> bool:
    if event_type == "process.state":
        return (
            source == "psutil"
            and action == "observed"
            and _valid_nullable_id(payload["pid"])
            and _valid_nullable_id(payload["ppid"])
            and _valid_nullable_text(payload["process_name"], 256)
            and _valid_executable_path(payload["executable_path"])
            and _valid_user_id(payload["user_id"])
            and _valid_nullable_timestamp(payload["start_time"])
        )
    if event_type == "network.connection":
        return (
            source == "psutil"
            and action in {"observed", "listening"}
            and _valid_nullable_text(payload["local_address"], 255)
            and _valid_nullable_port(payload["local_port"])
            and _valid_nullable_text(payload["remote_address"], 255)
            and _valid_nullable_port(payload["remote_port"])
            and _valid_text(payload["connection_status"], 64)
            and _valid_nullable_id(payload["pid"])
        )
    if event_type == "docker.container.lifecycle":
        return (
            source == "docker"
            and _valid_text(action, 64)
            and _valid_text(payload["container_id"], 128)
        )
    return False


def _valid_text(value: object, maximum_length: int) -> bool:
    return (
        isinstance(value, str)
        and 0 < len(value) <= maximum_length
        and all(ord(character) >= 32 and ord(character) != 127 for character in value)
    )


def _valid_nullable_text(value: object, maximum_length: int) -> bool:
    return value is None or _valid_text(value, maximum_length)


def _valid_nullable_id(value: object) -> bool:
    return value is None or (type(value) is int and 0 <= value <= 2**32 - 1)


def _valid_nullable_port(value: object) -> bool:
    return value is None or (type(value) is int and 0 <= value <= 65_535)


def _valid_uuid(value: object) -> bool:
    if not isinstance(value, str):
        return False
    try:
        UUID(value)
    except ValueError:
        return False
    return True


def _parse_timestamp(value: object) -> datetime | None:
    if not isinstance(value, str) or len(value) > 40:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    normalized = parsed.astimezone(_UTC)
    if normalized < datetime(2000, 1, 1, tzinfo=_UTC):
        return None
    if normalized > datetime.now(_UTC) + timedelta(days=1):
        return None
    return normalized


def _valid_nullable_timestamp(value: object) -> bool:
    return value is None or _parse_timestamp(value) is not None


def _valid_executable_path(value: object) -> bool:
    if value is None:
        return True
    if not _valid_text(value, _MAX_STRING_LENGTH):
        return False
    return not value.startswith(("/Users/", "/home/"))


def _valid_user_id(value: object) -> bool:
    return value is None or (isinstance(value, str) and bool(_USER_ID_PATTERN.fullmatch(value)))


def write_to_clickhouse(
    events: DataFrame,
    checkpoint_location: str,
    config: ClickHouseConfig,
) -> StreamingQuery:
    """Start the minimal ClickHouse streaming sink.

    Args:
        events: Parsed security event stream.
        checkpoint_location: Directory for Spark stream progress.
        config: ClickHouse connection configuration.

    Returns:
        Running streaming query.
    """
    return (
        events.writeStream.outputMode("append")
        .option("checkpointLocation", checkpoint_location)
        .foreachBatch(lambda batch, batch_id: insert_batch(batch, batch_id, config))
        .start()
    )


def main() -> None:
    """Run the Kafka-to-ClickHouse security event streaming job."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")
    topic = os.getenv("SECURITY_EVENTS_TOPIC", "security_events")
    checkpoint_location = os.getenv(
        "SECURITY_EVENTS_CHECKPOINT_LOCATION",
        "/opt/laptop-metrics/spark/checkpoints/security_events_clickhouse",
    )
    clickhouse = ClickHouseConfig(
        url=os.getenv("CLICKHOUSE_URL", "http://clickhouse:8123/"),
        database=os.getenv("CLICKHOUSE_DB", "metrics"),
        user=os.getenv("CLICKHOUSE_USER", "metrics"),
        password=os.environ["CLICKHOUSE_PASSWORD"],
    )
    spark = (
        SparkSession.builder.appName("security-events-to-clickhouse")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    query = write_to_clickhouse(
        read_security_events(spark, bootstrap_servers, topic),
        checkpoint_location,
        clickhouse,
    )
    try:
        query.awaitTermination()
    finally:
        if query.isActive:
            query.stop()
        spark.stop()
        LOGGER.info("Security event stream stopped")


if __name__ == "__main__":
    main()
