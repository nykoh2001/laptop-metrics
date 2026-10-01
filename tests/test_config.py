"""Tests for collector configuration and host identity."""

import pytest

from collector.config import CollectorConfig
from collector.utils import stable_host_id


def test_config_uses_phase_one_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    """Default environment values form a valid local configuration."""
    for name in (
        "KAFKA_BOOTSTRAP_SERVERS",
        "SECURITY_EVENTS_TOPIC",
        "COLLECTION_INTERVAL_SECONDS",
        "KAFKA_CLIENT_ID",
        "DOCKER_EVENTS_ENABLED",
        "LOG_LEVEL",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("HOST_ID_SALT", "test-only-placeholder")

    config = CollectorConfig.from_env()

    assert config.kafka_bootstrap_servers == ("localhost:9092",)
    assert config.security_events_topic == "security_events"
    assert config.collection_interval_seconds == 5.0
    assert config.docker_events_enabled is True


def test_config_requires_private_host_id_salt(monkeypatch: pytest.MonkeyPatch) -> None:
    """The collector never falls back to a repository-known identity salt."""
    monkeypatch.delenv("HOST_ID_SALT", raising=False)

    with pytest.raises(ValueError, match="HOST_ID_SALT"):
        CollectorConfig.from_env()


@pytest.mark.parametrize("value", ["short", "replace-with-a-private-stable-local-value"])
def test_config_rejects_unsafe_host_id_salt(monkeypatch: pytest.MonkeyPatch, value: str) -> None:
    """Known placeholders and short salts cannot identify a host."""
    monkeypatch.setenv("HOST_ID_SALT", value)

    with pytest.raises(ValueError, match="HOST_ID_SALT"):
        CollectorConfig.from_env()


@pytest.mark.parametrize("value", ["0", "-1", "invalid"])
def test_config_rejects_invalid_interval(monkeypatch: pytest.MonkeyPatch, value: str) -> None:
    """Invalid collection intervals produce an actionable error."""
    monkeypatch.setenv("HOST_ID_SALT", "test-only-placeholder")
    monkeypatch.setenv("COLLECTION_INTERVAL_SECONDS", value)

    with pytest.raises(ValueError, match="COLLECTION_INTERVAL_SECONDS"):
        CollectorConfig.from_env()


def test_host_id_is_stable_pseudonymous_and_salt_scoped() -> None:
    """Host identity remains stable without exposing the device name."""
    first = stable_host_id("one-test-salt-1234")

    assert first == stable_host_id("one-test-salt-1234")
    assert first != stable_host_id("two-test-salt-1234")
    assert first.startswith("host-")
    assert len(first) == 29
