"""Integration tests for publishing metrics through a real Kafka broker."""

import json
import os
from dataclasses import asdict
from uuid import uuid4

import pytest
from kafka import KafkaAdminClient, KafkaConsumer

from collector.kafka_producer import KafkaMetricProducer
from collector.models import HostMetric

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("RUN_KAFKA_INTEGRATION_TESTS") != "1",
        reason="set RUN_KAFKA_INTEGRATION_TESTS=1 to use a real Kafka broker",
    ),
]


def test_metric_producer_publishes_serialized_payload_to_kafka() -> None:
    """A metric published by the adapter can be consumed unchanged."""
    bootstrap_servers = tuple(
        server.strip()
        for server in os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(",")
        if server.strip()
    )
    topic = f"integration_host_metrics_{uuid4().hex}"
    metric = HostMetric(
        timestamp="2026-01-01T00:00:00.000000Z",
        hostname="integration-host",
        cpu_usage_percent=10.0,
        memory_usage_percent=20.0,
        total_memory_bytes=100,
        available_memory_bytes=80,
        swap_usage_percent=0.0,
        total_swap_bytes=0,
        used_swap_bytes=0,
        disk_usage_percent=30.0,
        disk_read_bytes=1,
        disk_write_bytes=2,
        network_bytes_sent=3,
        network_bytes_received=4,
    )
    producer = KafkaMetricProducer(bootstrap_servers, "integration-test-producer")

    try:
        producer.publish(topic, metric)
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

    assert message is not None, "Kafka did not return the published metric within 10 seconds"
    assert json.loads(message.value) == asdict(metric)
