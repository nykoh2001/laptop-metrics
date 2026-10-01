"""Kafka publishing adapter for common security events."""

import logging
from collections.abc import Callable

from kafka import KafkaProducer

from collector.models import SecurityEvent
from collector.serializers import serialize_event

LOGGER = logging.getLogger(__name__)


class KafkaEventProducer:
    """Publish individual security events as keyed JSON Kafka records."""

    def __init__(self, bootstrap_servers: tuple[str, ...], client_id: str) -> None:
        """Create a producer configured for the local Kafka broker.

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

    def publish(self, topic: str, event: SecurityEvent) -> None:
        """Publish one event using its deterministic identifier as the key.

        Args:
            topic: Destination Kafka topic.
            event: Security event to serialize and publish.
        """
        try:
            future = self._producer.send(
                topic,
                key=event.event_id.encode("utf-8"),
                value=serialize_event(event),
            )
            future.add_errback(self._publish_error_callback(topic))
        except Exception:
            LOGGER.exception("Failed to enqueue security event", extra={"topic": topic})

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
