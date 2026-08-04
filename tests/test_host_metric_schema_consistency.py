"""Test if host metric schema matches as expected between Kafka contract and Spark consumers."""

import pytest
import json

from dataclasses import fields
from pathlib import Path

from spark.jobs.host_metrics_console import host_metric_schema
from collector.models import HostMetric

def test_host_metric_schema_consistency():
    """All host metric representations expose the same field names."""
  
    spark_field_names = {field.name for field in host_metric_schema()}
    model_field_names = {field.name for field in fields(HostMetric)}
    
    sample_path = Path("schemas/host_metric.json")
    sample = json.loads(sample_path.read_text(encoding="utf-8"))
    sample_field_names = set(sample)
    
    assert spark_field_names == model_field_names
    assert spark_field_names == sample_field_names