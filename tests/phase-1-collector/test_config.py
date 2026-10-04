"""Tests for collector configuration and host identity."""

from pathlib import Path

import pytest

from collector.config import CollectorConfig
from collector.main import _COLLECTOR_ENV_NAMES
from collector.utils import load_env_file, stable_host_id


def _set_valid_config_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Set sample values for every required collector environment variable."""
    for name, value in {
        "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
        "SECURITY_EVENTS_TOPIC": "security_events",
        "COLLECTION_INTERVAL_SECONDS": "30",
        "KAFKA_CLIENT_ID": "security-telemetry-test",
        "HOST_ID_SALT": "test-only-placeholder",
        "DOCKER_EVENTS_ENABLED": "true",
        "LOG_LEVEL": "INFO",
    }.items():
        monkeypatch.setenv(name, value)


def test_config_uses_collector_environment_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    """Collector settings come from explicit environment values."""
    _set_valid_config_env(monkeypatch)

    config = CollectorConfig.from_env()

    assert config.kafka_bootstrap_servers == ("localhost:9092",)
    assert config.security_events_topic == "security_events"
    assert config.collection_interval_seconds == 30.0
    assert config.docker_events_enabled is True


def test_config_loads_all_values_from_dotenv(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An example-shaped env file supplies every required collector setting."""
    for name in _COLLECTOR_ENV_NAMES:
        monkeypatch.delenv(name, raising=False)
    environment_file = tmp_path / ".env"
    environment_file.write_text(
        "\n".join(
            (
                "KAFKA_BOOTSTRAP_SERVERS=localhost:9092",
                "SECURITY_EVENTS_TOPIC=security_events",
                "COLLECTION_INTERVAL_SECONDS=30",
                "KAFKA_CLIENT_ID=security-telemetry-test",
                "HOST_ID_SALT=test-only-placeholder",
                "DOCKER_EVENTS_ENABLED=true",
                "LOG_LEVEL=INFO",
            )
        ),
        encoding="utf-8",
    )

    load_env_file(environment_file, _COLLECTOR_ENV_NAMES)
    config = CollectorConfig.from_env()

    assert config.collection_interval_seconds == 30.0
    assert config.security_events_topic == "security_events"
    assert config.kafka_client_id == "security-telemetry-test"


def test_env_example_contains_every_collector_setting() -> None:
    """The sample env file documents every setting required by the collector."""
    project_root = Path(__file__).resolve().parents[2]
    example_path = project_root / ".env.example"
    example_names = {
        line.split("=", 1)[0]
        for line in example_path.read_text(encoding="utf-8").splitlines()
        if "=" in line and not line.lstrip().startswith("#")
    }

    assert example_names >= _COLLECTOR_ENV_NAMES


def test_config_rejects_missing_collector_environment_value(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Collector configuration cannot silently fall back to source defaults."""
    _set_valid_config_env(monkeypatch)
    monkeypatch.delenv("SECURITY_EVENTS_TOPIC")

    with pytest.raises(ValueError, match="SECURITY_EVENTS_TOPIC"):
        CollectorConfig.from_env()


def test_config_requires_private_host_id_salt(monkeypatch: pytest.MonkeyPatch) -> None:
    """The collector never falls back to a repository-known identity salt."""
    _set_valid_config_env(monkeypatch)
    monkeypatch.delenv("HOST_ID_SALT", raising=False)

    with pytest.raises(ValueError, match="HOST_ID_SALT"):
        CollectorConfig.from_env()


@pytest.mark.parametrize("value", ["short", "replace-with-a-private-stable-local-value"])
def test_config_rejects_unsafe_host_id_salt(monkeypatch: pytest.MonkeyPatch, value: str) -> None:
    """Known placeholders and short salts cannot identify a host."""
    _set_valid_config_env(monkeypatch)
    monkeypatch.setenv("HOST_ID_SALT", value)

    with pytest.raises(ValueError, match="HOST_ID_SALT"):
        CollectorConfig.from_env()


@pytest.mark.parametrize("value", ["0", "-1", "invalid"])
def test_config_rejects_invalid_interval(monkeypatch: pytest.MonkeyPatch, value: str) -> None:
    """Invalid collection intervals produce an actionable error."""
    _set_valid_config_env(monkeypatch)
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
