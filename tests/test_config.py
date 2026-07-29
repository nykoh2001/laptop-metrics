"""Tests for collector configuration."""

import pytest

from collector.config import CollectorConfig


def test_config_uses_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    """Default environment values form a valid local configuration."""
    for name in (
        "KAFKA_BOOTSTRAP_SERVERS",
        "HOST_METRICS_TOPIC",
        "PROCESS_METRICS_TOPIC",
        "COLLECTION_INTERVAL_SECONDS",
        "KAFKA_CLIENT_ID",
        "LOG_LEVEL",
    ):
        monkeypatch.delenv(name, raising=False)

    config = CollectorConfig.from_env()

    assert config.kafka_bootstrap_servers == ("localhost:9092",)
    assert config.host_metrics_topic == "host_metrics"
    assert config.process_metrics_topic == "process_metrics"
    assert config.collection_interval_seconds == 5.0


@pytest.mark.parametrize("value", ["0", "-1", "invalid"])
def test_config_rejects_invalid_interval(monkeypatch: pytest.MonkeyPatch, value: str) -> None:
    """Invalid collection intervals produce an actionable error."""
    monkeypatch.setenv("COLLECTION_INTERVAL_SECONDS", value)

    with pytest.raises(ValueError, match="COLLECTION_INTERVAL_SECONDS"):
        CollectorConfig.from_env()
