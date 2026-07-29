"""JSON serialization for metric records."""

import json
from dataclasses import asdict

from collector.models import HostMetric, ProcessMetric

Metric = HostMetric | ProcessMetric


def serialize_metric(metric: Metric) -> bytes:
    """Serialize a metric as compact UTF-8 JSON.

    Args:
        metric: Host or process metric to serialize.

    Returns:
        A UTF-8 encoded JSON object.
    """
    return json.dumps(asdict(metric), separators=(",", ":"), ensure_ascii=False).encode("utf-8")
