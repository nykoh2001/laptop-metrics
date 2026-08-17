"""Read host metrics from Kafka and write structured rows to ClickHouse."""

import logging
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

try:
    from spark.jobs.clickhouse_sink import (
        clickhouse_http_config_from_env,
        write_stream_to_clickhouse,
    )
except ModuleNotFoundError:
    from clickhouse_sink import clickhouse_http_config_from_env, write_stream_to_clickhouse

LOGGER = logging.getLogger(__file__)


def host_metric_schema() -> StructType:
    """Build the schema for the collector's host metric JSON.

    Returns:
        Spark schema matching the documented host metric input contract.
    """
    return StructType(
        [
            StructField("timestamp", StringType(), nullable=False),
            StructField("hostname", StringType(), nullable=False),
            StructField("cpu_usage_percent", DoubleType(), nullable=False),
            StructField("memory_usage_percent", DoubleType(), nullable=False),
            StructField("total_memory_bytes", LongType(), nullable=False),
            StructField("available_memory_bytes", LongType(), nullable=False),
            StructField("swap_usage_percent", DoubleType(), nullable=False),
            StructField("total_swap_bytes", LongType(), nullable=False),
            StructField("used_swap_bytes", LongType(), nullable=False),
            StructField("disk_usage_percent", DoubleType(), nullable=False),
            StructField("disk_read_bytes", LongType(), nullable=False),
            StructField("disk_write_bytes", LongType(), nullable=False),
            StructField("network_bytes_sent", LongType(), nullable=False),
            StructField("network_bytes_received", LongType(), nullable=False),
        ]
    )


def read_host_metrics(
    spark: SparkSession,
    bootstrap_servers: str,
    topic: str,
) -> DataFrame:
    """Create a structured stream of host metrics from Kafka.

    Args:
        spark: Active Spark session.
        bootstrap_servers: Comma-separated Kafka broker addresses.
        topic: Kafka topic containing host metric JSON records.

    Returns:
        Streaming DataFrame with typed host metric columns.
    """
    kafka_records = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .load()
    )

    return parse_host_metric_records(kafka_records)


def parse_host_metric_records(kafka_records: DataFrame) -> DataFrame:
    """Parse Kafka record values into typed host metric columns.

    Args:
        kafka_records: DataFrame containing a binary or string ``value`` column.

    Returns:
        DataFrame containing typed host metric columns and a UTC timestamp.
    """
    parsed_records = kafka_records.select(
        from_json(col("value").cast("string"), host_metric_schema()).alias("metric")
    )

    return parsed_records.select("metric.*").withColumn(
        "timestamp",
        to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSX"),
    )


def write_to_clickhouse(metrics: DataFrame, checkpoint_location: str) -> StreamingQuery:
    """Start a ClickHouse sink for a host metric stream.

    Args:
        metrics: Structured host metric streaming DataFrame.
        checkpoint_location: Directory in which Spark persists stream progress.

    Returns:
        Running streaming query.

    Raises:
        ValueError: If required ClickHouse settings are invalid.
    """
    return write_stream_to_clickhouse(
        metrics=metrics,
        checkpoint_location=checkpoint_location,
        table=os.getenv("HOST_METRICS_CLICKHOUSE_TABLE", "fact_host_metrics"),
        config=clickhouse_http_config_from_env(),
    )


def main() -> None:
    """Run the Kafka-to-ClickHouse host metric streaming job."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")
    topic = os.getenv("HOST_METRICS_TOPIC", "host_metrics")
    checkpoint_location = os.getenv(
        "HOST_METRICS_CHECKPOINT_LOCATION",
        "/opt/laptop-metrics/spark/checkpoints/host_metrics_clickhouse",
    )

    spark = (
        SparkSession.builder.appName("host-metrics-clickhouse")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))

    LOGGER.info(
        "Starting host metric stream from topic %s using %s",
        topic,
        bootstrap_servers,
    )
    query = write_to_clickhouse(
        read_host_metrics(spark, bootstrap_servers, topic),
        checkpoint_location,
    )
    try:
        query.awaitTermination()
    finally:
        if query.isActive:
            query.stop()
        spark.stop()
        LOGGER.info("Host metric stream stopped")


if __name__ == "__main__":
    main()
