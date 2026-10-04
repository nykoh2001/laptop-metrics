"""Environment-based security telemetry collector configuration."""

import os
from dataclasses import dataclass

_VALID_LOG_LEVELS = frozenset({"CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"})


@dataclass(frozen=True, slots=True)
class CollectorConfig:
    """Runtime configuration for the security telemetry collector."""

    kafka_bootstrap_servers: tuple[str, ...]
    security_events_topic: str
    collection_interval_seconds: float
    kafka_client_id: str
    host_id_salt: str
    docker_events_enabled: bool
    log_level: str

    @classmethod
    def from_env(cls) -> "CollectorConfig":
        """Build and validate configuration from environment variables.

        Returns:
            Validated collector configuration.

        Raises:
            ValueError: If a required environment value is missing or invalid.
        """
        servers = tuple(
            server.strip()
            for server in _required_env("KAFKA_BOOTSTRAP_SERVERS").split(",")
            if server.strip()
        )
        if not servers:
            raise ValueError("KAFKA_BOOTSTRAP_SERVERS must contain at least one server")

        log_level = _required_env("LOG_LEVEL").upper()
        if log_level not in _VALID_LOG_LEVELS:
            raise ValueError(f"LOG_LEVEL must be one of {sorted(_VALID_LOG_LEVELS)}")

        host_id_salt = _required_env("HOST_ID_SALT")
        if host_id_salt == "replace-with-a-private-stable-local-value":
            raise ValueError("HOST_ID_SALT must be replaced with a private stable local value")
        if len(host_id_salt) < 16:
            raise ValueError("HOST_ID_SALT must contain at least 16 characters")

        return cls(
            kafka_bootstrap_servers=servers,
            security_events_topic=_required_env("SECURITY_EVENTS_TOPIC"),
            collection_interval_seconds=_positive_float("COLLECTION_INTERVAL_SECONDS"),
            kafka_client_id=_required_env("KAFKA_CLIENT_ID"),
            host_id_salt=host_id_salt,
            docker_events_enabled=_boolean_env("DOCKER_EVENTS_ENABLED"),
            log_level=log_level,
        )


def _required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ValueError(f"{name} must be set in the environment or .env file")
    return value


def _positive_float(name: str) -> float:
    raw_value = _required_env(name)
    try:
        value = float(raw_value)
    except ValueError as error:
        raise ValueError(f"{name} must be a number") from error
    if value <= 0:
        raise ValueError(f"{name} must be greater than zero")
    return value


def _boolean_env(name: str) -> bool:
    value = _required_env(name).lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be a boolean")
