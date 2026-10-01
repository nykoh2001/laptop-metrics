"""Security telemetry collectors and their composition."""

import json
import logging
import os
import queue
import shutil
import subprocess
import threading
from collections.abc import Callable, Iterator, Sequence
from typing import Protocol

import psutil

from collector.events import create_security_event
from collector.models import JsonScalar, SecurityEvent
from collector.utils import (
    normalize_executable_path,
    pseudonymous_user_id,
    timestamp_from_epoch,
    utc_timestamp,
)

LOGGER = logging.getLogger(__name__)
_DOCKER_EVENT_FORMAT = (
    '{"action":{{json .Action}},"container_id":{{json .Actor.ID}},"time_nano":{{json .TimeNano}}}'
)
_MAX_DOCKER_EVENT_LINE_LENGTH = 4_096
_MAX_QUEUED_DOCKER_EVENTS = 10_000


class EventCollector(Protocol):
    """Behavior implemented by one telemetry source."""

    def collect_events(self, collected_at: str) -> Iterator[SecurityEvent]:
        """Yield currently available events."""

    def close(self) -> None:
        """Release resources held by the collector."""


class ConnectionSnapshot(Protocol):
    """Network connection fields consumed by the allowlist mapper."""

    @property
    def laddr(self) -> object:
        """Return the local endpoint value."""

    @property
    def raddr(self) -> object:
        """Return the remote endpoint value."""

    @property
    def status(self) -> str:
        """Return the operating-system connection status."""


class ProcessStateCollector:
    """Collect privacy-conscious process state snapshots with psutil."""

    def __init__(self, host_id: str, identity_salt: str) -> None:
        """Initialize the process collector.

        Args:
            host_id: Stable pseudonymous host identifier.
            identity_salt: Private stable salt for user pseudonymization.
        """
        self._host_id = host_id
        self._identity_salt = identity_salt

    def collect_events(self, collected_at: str) -> Iterator[SecurityEvent]:
        """Yield one event for each readable process.

        Command-line arguments and environment variables are intentionally not
        requested from psutil.

        Args:
            collected_at: Shared UTC collection-cycle time.

        Yields:
            Process state events. Inaccessible optional fields are null.
        """
        attributes = ["pid", "ppid", "name", "exe", "username", "create_time"]
        access_denied_count = 0
        disappeared_count = 0
        for process in psutil.process_iter(attrs=attributes, ad_value=None):
            try:
                info = process.info
                start_time = _optional_epoch_timestamp(info.get("create_time"))
                raw_user = _optional_string(info.get("username"))
                payload: dict[str, JsonScalar] = {
                    "pid": _optional_int(info.get("pid")),
                    "ppid": _optional_int(info.get("ppid")),
                    "process_name": _optional_string(info.get("name")),
                    "executable_path": normalize_executable_path(
                        _optional_string(info.get("exe")), raw_user
                    ),
                    "user_id": pseudonymous_user_id(raw_user, self._identity_salt),
                    "start_time": start_time,
                }
                yield create_security_event(
                    host_id=self._host_id,
                    source="psutil",
                    event_type="process.state",
                    action="observed",
                    event_time=start_time or collected_at,
                    collected_at=collected_at,
                    payload=payload,
                )
            except psutil.AccessDenied:
                access_denied_count += 1
            except (psutil.NoSuchProcess, psutil.ZombieProcess, KeyError):
                disappeared_count += 1
        if access_denied_count:
            LOGGER.info(
                "Some process records were inaccessible",
                extra={"access_denied_count": access_denied_count},
            )
        if disappeared_count:
            LOGGER.debug(
                "Some processes disappeared during collection",
                extra={"disappeared_count": disappeared_count},
            )

    def close(self) -> None:
        """Release resources; the process collector owns none."""


class NetworkConnectionCollector:
    """Collect listening sockets and current internet connections with psutil."""

    def __init__(self, host_id: str) -> None:
        """Initialize the network collector.

        Args:
            host_id: Stable pseudonymous host identifier.
        """
        self._host_id = host_id

    def collect_events(self, collected_at: str) -> Iterator[SecurityEvent]:
        """Yield current listening-port and network-connection events.

        Args:
            collected_at: Shared UTC collection-cycle time.

        Yields:
            Network connection state events.

        A denied system-wide query falls back to connections owned by readable
        processes. This intentionally provides partial visibility instead of
        requesting broader privileges.
        """
        try:
            connections = psutil.net_connections(kind="inet")
        except (psutil.AccessDenied, PermissionError):
            yield from self._collect_readable_process_connections(collected_at)
            return
        for connection in connections:
            yield self._build_event(connection, connection.pid, collected_at)

    def close(self) -> None:
        """Release resources; the network collector owns none."""

    def _collect_readable_process_connections(self, collected_at: str) -> Iterator[SecurityEvent]:
        access_denied_count = 0
        disappeared_count = 0
        for process in psutil.process_iter(attrs=["pid"], ad_value=None):
            try:
                connections = process.net_connections(kind="inet")
                for connection in connections:
                    yield self._build_event(connection, process.pid, collected_at)
            except psutil.AccessDenied:
                access_denied_count += 1
            except (psutil.NoSuchProcess, psutil.ZombieProcess):
                disappeared_count += 1
        if access_denied_count:
            LOGGER.info(
                "Some process network records were inaccessible",
                extra={"access_denied_count": access_denied_count},
            )
        if disappeared_count:
            LOGGER.debug(
                "Some processes disappeared during network collection",
                extra={"disappeared_count": disappeared_count},
            )

    def _build_event(
        self, connection: ConnectionSnapshot, pid: int | None, collected_at: str
    ) -> SecurityEvent:
        local_address, local_port = _split_address(connection.laddr)
        remote_address, remote_port = _split_address(connection.raddr)
        status = str(connection.status)
        action = "listening" if status == psutil.CONN_LISTEN else "observed"
        payload: dict[str, JsonScalar] = {
            "local_address": local_address,
            "local_port": local_port,
            "remote_address": remote_address,
            "remote_port": remote_port,
            "connection_status": status,
            "pid": pid,
        }
        return create_security_event(
            host_id=self._host_id,
            source="psutil",
            event_type="network.connection",
            action=action,
            event_time=collected_at,
            collected_at=collected_at,
            payload=payload,
        )


class DockerLifecycleCollector:
    """Optionally stream Docker container lifecycle events through the CLI."""

    def __init__(
        self,
        host_id: str,
        *,
        executable: str | None = None,
        availability_check: Callable[[], bool] | None = None,
        process_factory: Callable[..., subprocess.Popen[str]] = subprocess.Popen,
    ) -> None:
        """Start the Docker event stream only when the daemon is usable.

        Args:
            host_id: Stable pseudonymous host identifier.
            executable: Explicit Docker executable, primarily for tests.
            availability_check: Optional daemon availability probe.
            process_factory: Factory used to start the event subprocess.
        """
        self._host_id = host_id
        self._events: queue.Queue[dict[str, object]] = queue.Queue(
            maxsize=_MAX_QUEUED_DOCKER_EVENTS
        )
        self._dropped_event_count = 0
        self._process: subprocess.Popen[str] | None = None
        self._reader: threading.Thread | None = None
        docker = executable or shutil.which("docker")
        try:
            available = (
                availability_check()
                if availability_check is not None
                else _docker_available(docker)
            )
        except (OSError, subprocess.SubprocessError):
            available = False
        if docker is None or not available:
            LOGGER.info("Docker lifecycle collection disabled; daemon is unavailable")
            return
        try:
            self._process = process_factory(
                [docker, "events", "--filter", "type=container", "--format", _DOCKER_EVENT_FORMAT],
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                text=True,
                bufsize=1,
                env=_docker_subprocess_environment(),
            )
        except (OSError, subprocess.SubprocessError):
            LOGGER.warning("Docker lifecycle collection could not be started")
            return
        self._reader = threading.Thread(
            target=self._read_events,
            name="docker-event-reader",
            daemon=True,
        )
        self._reader.start()
        LOGGER.info("Docker lifecycle collection started")

    @property
    def enabled(self) -> bool:
        """Return whether a Docker event stream was started.

        Returns:
            True when Docker events are being consumed.
        """
        return self._process is not None

    def collect_events(self, collected_at: str) -> Iterator[SecurityEvent]:
        """Drain lifecycle events received since the previous cycle.

        Args:
            collected_at: Shared UTC collection-cycle time.

        Yields:
            Sanitized Docker container lifecycle events.
        """
        while True:
            try:
                raw_event = self._events.get_nowait()
            except queue.Empty:
                break
            event = self._convert_event(raw_event, collected_at)
            if event is not None:
                yield event
        if self._dropped_event_count:
            LOGGER.warning(
                "Docker lifecycle events were dropped because the queue was full",
                extra={"dropped_event_count": self._dropped_event_count},
            )
            self._dropped_event_count = 0

    def close(self) -> None:
        """Stop the optional Docker event subprocess."""
        if self._process is None:
            return
        if self._process.poll() is None:
            self._process.terminate()
            try:
                self._process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                self._process.kill()
                self._process.wait(timeout=2)
        if self._reader is not None:
            self._reader.join(timeout=2)
        self._process = None

    def _read_events(self) -> None:
        process = self._process
        if process is None or process.stdout is None:
            return
        stdout = process.stdout
        for line in stdout:
            if len(line) > _MAX_DOCKER_EVENT_LINE_LENGTH:
                LOGGER.warning("Ignoring oversized Docker lifecycle event")
                continue
            try:
                parsed = json.loads(line)
            except json.JSONDecodeError:
                LOGGER.warning("Ignoring malformed Docker lifecycle event")
                continue
            if isinstance(parsed, dict):
                try:
                    self._events.put_nowait(parsed)
                except queue.Full:
                    self._dropped_event_count += 1

    def _convert_event(
        self, raw_event: dict[str, object], collected_at: str
    ) -> SecurityEvent | None:
        return build_docker_lifecycle_event(raw_event, self._host_id, collected_at)


def build_docker_lifecycle_event(
    raw_event: dict[str, object], host_id: str, collected_at: str
) -> SecurityEvent | None:
    """Convert a Docker CLI event without retaining sensitive attributes.

    Args:
        raw_event: JSON object emitted by ``docker events``.
        host_id: Stable pseudonymous host identifier.
        collected_at: UTC time at which the event was drained.

    Returns:
        Common lifecycle event, or null for an incomplete input event.
    """
    action = _optional_string(raw_event.get("action"))
    container_id = _optional_string(raw_event.get("container_id"))
    if action is None or container_id is None:
        LOGGER.warning("Ignoring incomplete Docker lifecycle event")
        return None
    event_time = _docker_event_time(raw_event) or collected_at
    return create_security_event(
        host_id=host_id,
        source="docker",
        event_type="docker.container.lifecycle",
        action=action,
        event_time=event_time,
        collected_at=collected_at,
        payload={"container_id": container_id},
    )


class SecurityTelemetryCollector:
    """Compose independent telemetry sources without coupling their failures."""

    def __init__(self, collectors: Sequence[EventCollector]) -> None:
        """Initialize the aggregate collector.

        Args:
            collectors: Ordered telemetry source implementations.
        """
        self._collectors = tuple(collectors)

    def collect_events(self, collected_at: str | None = None) -> Iterator[SecurityEvent]:
        """Yield events from each source while isolating source failures.

        Args:
            collected_at: Optional shared collection-cycle UTC timestamp.

        Yields:
            Events from all sources that complete successfully.
        """
        cycle_time = collected_at or utc_timestamp()
        for collector in self._collectors:
            try:
                yield from collector.collect_events(cycle_time)
            except Exception:
                LOGGER.error(
                    "Telemetry source collection failed",
                    extra={"collector": type(collector).__name__},
                )

    def close(self) -> None:
        """Close all telemetry sources, logging isolated shutdown failures."""
        for collector in self._collectors:
            try:
                collector.close()
            except Exception:
                LOGGER.error(
                    "Telemetry source shutdown failed",
                    extra={"collector": type(collector).__name__},
                )


def _docker_available(executable: str | None) -> bool:
    if executable is None:
        return False
    try:
        result = subprocess.run(
            [executable, "info", "--format", "{{.ServerVersion}}"],
            check=False,
            capture_output=True,
            text=True,
            timeout=3,
            env=_docker_subprocess_environment(),
        )
    except (OSError, subprocess.SubprocessError):
        return False
    return result.returncode == 0


def _docker_subprocess_environment() -> dict[str, str]:
    allowed_names = ("PATH", "HOME", "TMPDIR", "DOCKER_HOST", "DOCKER_CONTEXT")
    return {name: os.environ[name] for name in allowed_names if name in os.environ}


def _split_address(address: object) -> tuple[str | None, int | None]:
    if not address:
        return None, None
    if hasattr(address, "ip") and hasattr(address, "port"):
        return str(address.ip), int(address.port)
    if isinstance(address, tuple) and len(address) >= 2:
        return str(address[0]), int(address[1])
    return None, None


def _optional_epoch_timestamp(value: object) -> str | None:
    if isinstance(value, int | float):
        return timestamp_from_epoch(float(value))
    return None


def _optional_int(value: object) -> int | None:
    return int(value) if isinstance(value, int) else None


def _optional_string(value: object) -> str | None:
    return value if isinstance(value, str) and value else None


def _docker_event_time(raw_event: dict[str, object]) -> str | None:
    time_nano = raw_event.get("time_nano")
    if isinstance(time_nano, int | float):
        return timestamp_from_epoch(float(time_nano) / 1_000_000_000)
    epoch_seconds = raw_event.get("time")
    if isinstance(epoch_seconds, int | float):
        return timestamp_from_epoch(float(epoch_seconds))
    return None
