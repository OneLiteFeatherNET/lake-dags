import pytest

from lake.config import TableConfig, load_config, parse_config


def _raw(**overrides):
    raw = {
        "catalog": "corvus",
        "snapshot_max_age_days": 7,
        "spark": {
            "spark_version": "4.1.4",
            "image": "apache/spark:4.1.4-scala2.13-java17-python3-ubuntu",
            "iceberg_runtime": "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.12.0",
            "iceberg_aws_bundle": "org.apache.iceberg:iceberg-aws-bundle:1.12.0",
        },
        "tables": [{"name": "capture.raw", "retention_days": 21, "day_column": "date"}],
    }
    raw.update(overrides)
    return raw


def test_real_maintenance_yaml_loads_with_expected_tables():
    config = load_config()

    assert config.catalog == "corvus"
    assert config.snapshot_max_age_days == 7
    assert config.spark.spark_version == "4.1.4"
    assert config.tables == [
        TableConfig(name="capture.raw", retention_days=21, day_column="date"),
        TableConfig(name="capture.events", retention_days=None, day_column="date"),
    ]


def test_rejects_table_name_without_namespace():
    raw = _raw(tables=[{"name": "raw", "retention_days": 21}])

    with pytest.raises(ValueError, match="namespace.table"):
        parse_config(raw)


def test_rejects_table_name_with_two_dots():
    raw = _raw(tables=[{"name": "a.b.c", "retention_days": 21}])

    with pytest.raises(ValueError, match="namespace.table"):
        parse_config(raw)


def test_rejects_negative_retention():
    raw = _raw(tables=[{"name": "capture.raw", "retention_days": -1}])

    with pytest.raises(ValueError, match="retention_days"):
        parse_config(raw)


def test_rejects_boolean_retention():
    raw = _raw(tables=[{"name": "capture.raw", "retention_days": True}])

    with pytest.raises(ValueError, match="retention_days"):
        parse_config(raw)


def test_rejects_duplicate_tables():
    raw = _raw(
        tables=[
            {"name": "capture.raw", "retention_days": 21},
            {"name": "capture.raw", "retention_days": None},
        ]
    )

    with pytest.raises(ValueError, match="duplicate table 'capture.raw'"):
        parse_config(raw)


def test_rejects_unknown_table_key():
    raw = _raw(tables=[{"name": "capture.raw", "retention_days": 21, "retention": 5}])

    with pytest.raises(ValueError, match="unknown key.*retention"):
        parse_config(raw)


def test_rejects_unknown_top_level_key():
    raw = _raw(schedule="daily")

    with pytest.raises(ValueError, match="unknown key.*schedule"):
        parse_config(raw)


def test_rejects_non_positive_snapshot_age():
    raw = _raw(snapshot_max_age_days=0)

    with pytest.raises(ValueError, match="snapshot_max_age_days"):
        parse_config(raw)


def test_rejects_identifier_with_sql_metacharacters():
    raw = _raw(tables=[{"name": "capture.raw; DROP", "retention_days": 21}])

    with pytest.raises(ValueError, match="namespace.table|identifier"):
        parse_config(raw)
