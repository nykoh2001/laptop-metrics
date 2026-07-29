"""Small utilities shared by collector modules."""

import os
from datetime import UTC, datetime
from pathlib import Path


def utc_timestamp() -> str:
    """Return the current time as a UTC ISO 8601 string.

    Returns:
        Timestamp ending in ``Z`` with microsecond precision.
    """
    return datetime.now(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def load_env_file(path: Path) -> None:
    """Load simple KEY=VALUE entries without overriding the environment.

    Blank lines and lines beginning with ``#`` are ignored. Values may be
    surrounded by matching single or double quotes. This deliberately supports
    the simple format used by this project and is not a shell parser.

    Args:
        path: Environment file to load. A missing file is ignored.

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
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
            value = value[1:-1]
        os.environ.setdefault(name, value)
