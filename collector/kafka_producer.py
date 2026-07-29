"""Kafka publishing adapter for collected metrics."""

import logging
from collections.abc import Callable

from kafka import KafkaProducer

from collector.models import HostMetric, ProcessMetric
from collector.serializers import serialize_metric

LOGGER = logging.getLogger(__name__)


class KafkaMetricProducer:
    """Publish individual JSON metric records to Kafka."""

    def __init__(self, bootstrap_servers: tuple[str, ...], client_id: str) -> None:
        """Create a Kafka producer configured for the local Kafka 4 broker.

        Args:
            bootstrap_servers: Kafka bootstrap server addresses.
            client_id: Client identifier reported to Kafka.
        """
        self._producer = KafkaProducer(
            bootstrap_servers=list(bootstrap_servers),
            client_id=client_id,
            api_version=(4, 0),
            batch_size=0,
            linger_ms=0,
            max_block_ms=5_000,
        )
        LOGGER.info("Kafka producer configured", extra={"bootstrap_servers": bootstrap_servers})

    def publish(self, topic: str, metric: HostMetric | ProcessMetric) -> None:
        """Publish one metric as one Kafka record.

        Args:
            topic: Destination Kafka topic.
            metric: Metric record to serialize and publish.
        """
        try:
            future = self._producer.send(topic, value=serialize_metric(metric))
            future.add_errback(self._publish_error_callback(topic))
        except Exception:
            LOGGER.exception("Failed to enqueue Kafka metric", extra={"topic": topic})

    def flush(self, timeout_seconds: float = 10.0) -> None:
        """Wait for pending records to complete.

        Args:
            timeout_seconds: Maximum time to wait.
        """
        try:
            self._producer.flush(timeout=timeout_seconds)
        except Exception:
            LOGGER.exception("Failed to flush Kafka producer")

    def close(self, timeout_seconds: float = 10.0) -> None:
        """Close the underlying producer.

        Args:
            timeout_seconds: Maximum time to wait.
        """
        try:
            self._producer.close(timeout=timeout_seconds)
        except Exception:
            LOGGER.exception("Failed to close Kafka producer")

    @staticmethod
    def _publish_error_callback(topic: str) -> Callable[[BaseException], None]:
        def log_error(error: BaseException) -> None:
            LOGGER.error(
                "Kafka publish failed",
                extra={"topic": topic},
                exc_info=(type(error), error, error.__traceback__),
            )

        return log_error
