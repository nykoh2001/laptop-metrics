"""Integration test for publishing common events through a real Kafka broker."""

import json
import os
from dataclasses import asdict
from uuid import uuid4

import pytest
from kafka import KafkaAdminClient, KafkaConsumer

from collector.events import create_security_event
from collector.kafka_producer import KafkaEventProducer

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("RUN_KAFKA_INTEGRATION_TESTS") != "1",
        reason="set RUN_KAFKA_INTEGRATION_TESTS=1 to use a real Kafka broker",
    ),
]


def test_event_producer_publishes_keyed_payload_to_kafka() -> None:
    """Kafka preserves the event identifier key and serialized envelope."""
    bootstrap_servers = tuple(
        server.strip()
        for server in os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(",")
        if server.strip()
    )
    topic = f"integration_security_events_{uuid4().hex}"
    event = create_security_event(
        host_id="host-0123456789abcdef01234567",
        source="psutil",
        event_type="process.state",
        action="observed",
        event_time="2026-10-01T00:00:00.000000Z",
        collected_at="2026-10-01T00:00:01.000000Z",
        payload={
            "pid": 1,
            "ppid": 0,
            "process_name": "launchd",
            "executable_path": "/sbin/launchd",
            "user_id": "user-0123456789abcdef01234567",
            "start_time": "2026-10-01T00:00:00.000000Z",
        },
    )
    producer = KafkaEventProducer(bootstrap_servers, "integration-test-producer")

    try:
        producer.publish(topic, event)
        producer.flush()
    finally:
        producer.close()

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=list(bootstrap_servers),
        api_version=(4, 0),
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=10_000,
    )
    try:
        message = next(iter(consumer), None)
    finally:
        consumer.close()
        admin = KafkaAdminClient(
            bootstrap_servers=list(bootstrap_servers),
            api_version=(4, 0),
            client_id="integration-test-admin",
        )
        try:
            admin.delete_topics([topic], timeout_ms=5_000)
        finally:
            admin.close()

    assert message is not None, "Kafka did not return the published event within 10 seconds"
    assert message.key.decode("utf-8") == event.event_id
    assert json.loads(message.value) == asdict(event)
