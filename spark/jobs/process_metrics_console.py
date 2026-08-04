"""Learning scaffold for streaming process metrics from Kafka to the console."""

import logging
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, LongType

LOGGER = logging.getLogger(__file__)


def process_metric_schema() -> StructType:
    """Build the schema for the collector's process metric JSON.

    Returns:
        Spark schema matching ``schemas/process_metric.json``.
    """
    return StructType(
        StructField("timestamp", StringType, nullable=False),
        StructField("pid", IntegerType, nullable=False),
        StructField("process_name", StringType, nullable=False),
        StructField("cpu_usage_percent", DoubleType, nullable=False),
        StructField("memory_usage_percent", DoubleType, nullable=False),
        StructField("rss_memory_bytes", LongType, nullable=False),
        StructField("process_status", StringType, nullable=False),
    )


def read_process_metrics(
    spark: SparkSession,
    bootstrap_servers: str,
    topic: str,
) -> DataFrame:
    """Create a structured stream of process metrics from Kafka.

    Args:
        spark: Active Spark session.
        bootstrap_servers: Comma-separated Kafka broker addresses.
        topic: Kafka topic containing process metric JSON records.

    Returns:
        Streaming DataFrame with typed process metric columns.
    """
    # EXERCISE 2:
    # Use spark.readStream with the Kafka source. Configure the broker,
    # subscribed topic, and initial offset position, then call load().
    raise NotImplementedError("Complete EXERCISE 2: read process records from Kafka")

    # EXERCISE 3 (place below the Kafka source after removing the raise above):
    # Kafka's value column is binary. Convert it to a string, parse it with
    # process_metric_schema(), expand the parsed struct, and convert timestamp
    # from the contract format to Spark TimestampType before returning it.


def write_to_console(metrics: DataFrame, checkpoint_location: str) -> StreamingQuery:
    """Start a console sink for a process metric stream.

    Args:
        metrics: Structured process metric streaming DataFrame.
        checkpoint_location: Directory in which Spark persists stream progress.

    Returns:
        Running streaming query.
    """
    # EXERCISE 4:
    # Start an append-mode console streaming query. Show complete values without
    # truncation and store progress at checkpoint_location.
    raise NotImplementedError("Complete EXERCISE 4: write process metrics to the console")


def main() -> None:
    """Configure and run the Kafka-to-console process metric streaming job."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")
    topic = os.getenv("PROCESS_METRICS_TOPIC", "process_metrics")
    checkpoint_location = os.getenv(
        "PROCESS_METRICS_CHECKPOINT_LOCATION",
        "/opt/laptop-metrics/spark/checkpoints/process_metrics_console",
    )

    spark = (
        SparkSession.builder.appName("process-metrics-console")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))

    LOGGER.info(
        "Starting process metric stream from topic %s using %s",
        topic,
        bootstrap_servers,
    )
    try:
        metrics = read_process_metrics(spark, bootstrap_servers, topic)
        query = write_to_console(metrics, checkpoint_location)
        query.awaitTermination()
    finally:
        for active_query in spark.streams.active:
            active_query.stop()
        spark.stop()
        LOGGER.info("Process metric stream stopped")


if __name__ == "__main__":
    main()
