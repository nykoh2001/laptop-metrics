"""Test consistency between host metric schema representations."""

import json
from dataclasses import fields
from pathlib import Path
from typing import get_type_hints

from pyspark.sql.types import DoubleType, LongType, StringType

from collector.models import HostMetric
from spark.jobs.host_metrics_sink import host_metric_schema


def test_host_metric_schema_consistency() -> None:
    """All host metric representations expose the same field names."""
    spark_field_names = {field.name for field in host_metric_schema()}
    model_field_names = {field.name for field in fields(HostMetric)}

    project_root = Path(__file__).resolve().parents[1]
    sample_path = project_root / "schemas" / "host_metric.json"
    sample = json.loads(sample_path.read_text(encoding="utf-8"))
    sample_field_names = set(sample)

    assert spark_field_names == model_field_names
    assert spark_field_names == sample_field_names


def test_host_metric_field_types_are_compatible() -> None:
    """Host model, JSON sample, and Spark schema use compatible field types."""
    project_root = Path(__file__).resolve().parents[1]
    sample_path = project_root / "schemas" / "host_metric.json"
    sample = json.loads(sample_path.read_text(encoding="utf-8"))

    model_types = get_type_hints(HostMetric)
    spark_types = {field.name: field.dataType for field in host_metric_schema()}
    expected_spark_types = {
        str: StringType(),
        int: LongType(),
        float: DoubleType(),
    }
    compatible_json_types = {
        str: (str,),
        int: (int,),
        float: (int, float),
    }

    for field_name, model_type in model_types.items():
        assert model_type in expected_spark_types, (
            f"Unsupported Python type for {field_name}: {model_type}"
        )
        assert spark_types[field_name] == expected_spark_types[model_type], (
            f"Spark type for {field_name} is incompatible with {model_type}"
        )
        assert type(sample[field_name]) in compatible_json_types[model_type], (
            f"JSON sample type for {field_name} is incompatible with {model_type}"
        )
