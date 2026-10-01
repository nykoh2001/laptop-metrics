"""Small utilities shared by collector modules."""

import hashlib
import hmac
import os
import re
import uuid
from datetime import UTC, datetime
from pathlib import Path


def utc_timestamp() -> str:
    """Return the current time as a UTC ISO 8601 string.

    Returns:
        Timestamp ending in ``Z`` with microsecond precision.
    """
    return datetime.now(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def timestamp_from_epoch(epoch_seconds: float) -> str:
    """Convert Unix epoch seconds to a UTC ISO 8601 timestamp.

    Args:
        epoch_seconds: Seconds since the Unix epoch.

    Returns:
        UTC timestamp ending in ``Z`` with microsecond precision.

    Raises:
        ValueError: If the timestamp is outside the platform-supported range.
    """
    return (
        datetime.fromtimestamp(epoch_seconds, UTC)
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z")
    )


def stable_host_id(salt: str) -> str:
    """Derive a stable pseudonymous host identifier.

    The raw node identifier is used only as local input and is never emitted.
    The caller-provided salt prevents the output from being a direct
    representation of that identifier.

    Args:
        salt: Non-empty namespace salt used for pseudonymization.

    Returns:
        Stable identifier prefixed with ``host-``.

    Raises:
        ValueError: If the salt is empty.
    """
    if not salt:
        raise ValueError("host ID salt must not be empty")
    raw_identity = str(uuid.getnode())
    digest = hmac.new(salt.encode(), raw_identity.encode(), hashlib.sha256).hexdigest()
    return f"host-{digest[:24]}"


def pseudonymous_user_id(raw_user: str | None, salt: str) -> str | None:
    """Convert an operating-system user value to a stable pseudonym.

    Args:
        raw_user: User name or identifier obtained from the operating system.
        salt: Private stable local salt.

    Returns:
        A stable prefixed digest, or null when the user is unavailable.

    Raises:
        ValueError: If the salt is empty.
    """
    if raw_user is None:
        return None
    if not salt:
        raise ValueError("user ID salt must not be empty")
    digest = hmac.new(salt.encode(), f"user:{raw_user}".encode(), hashlib.sha256).hexdigest()
    return f"user-{digest[:24]}"


def normalize_executable_path(
    executable_path: str | None,
    raw_user: str | None,
    home_directory: str | None = None,
) -> str | None:
    """Replace an identifiable user-home prefix in an executable path.

    Args:
        executable_path: Path reported by the operating system.
        raw_user: Local user name used only to recognize common home paths.
        home_directory: Explicit current home, primarily for deterministic tests.

    Returns:
        Path with a recognized home prefix replaced by ``$HOME``, or null.
    """
    if executable_path is None:
        return None
    current_home = home_directory or str(Path.home())
    candidates = {current_home.rstrip("/")}
    if raw_user:
        candidates.update({f"/Users/{raw_user}", f"/home/{raw_user}"})
    for candidate in sorted(candidates, key=len, reverse=True):
        if candidate and (
            executable_path == candidate or executable_path.startswith(f"{candidate}/")
        ):
            return executable_path.replace(candidate, "$HOME", 1)
    generic_home = re.match(r"^/(?:Users|home)/[^/]+(?=/|$)", executable_path)
    if generic_home is not None:
        return executable_path.replace(generic_home.group(0), "$HOME", 1)
    return executable_path


def load_env_file(path: Path, allowed_names: frozenset[str] | None = None) -> None:
    """Load simple KEY=VALUE entries without overriding the environment.

    Blank lines and lines beginning with ``#`` are ignored. Values may be
    surrounded by matching single or double quotes. This deliberately supports
    the simple format used by this project and is not a shell parser.

    Args:
        path: Environment file to load. A missing file is ignored.
        allowed_names: Optional names to load. Other entries remain unread by
            the process.

    Raises:
        ValueError: If a non-comment line is malformed.
    """
    if not path.exists():
        return

    for line_number, raw_line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            raise ValueError(f"Invalid environment entry at {path}:{line_number}")
        name, value = line.split("=", maxsplit=1)
        name = name.strip()
        value = value.strip()
        if not name:
            raise ValueError(f"Empty environment name at {path}:{line_number}")
        if allowed_names is not None and name not in allowed_names:
            continue
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
            value = value[1:-1]
        os.environ.setdefault(name, value)
