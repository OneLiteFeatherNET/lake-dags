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
            "driver_memory": "3g",
            "executor_memory": "3g",
        },
        "tables": [{"name": "capture.raw", "retention_days": 21, "day_column": "date"}],
    }
    raw.update(overrides)
    return raw


def _with_spark(**values):
    raw = _raw()
    raw["spark"].update(values)
    return raw


def test_real_maintenance_yaml_loads_with_expected_tables():
    config = load_config()

    assert config.catalog == "corvus"
    assert config.snapshot_max_age_days == 7
    assert config.spark.spark_version == "4.1.4"
    assert config.tables == [
        TableConfig(
            name="capture.raw",
            retention_days=None,
            day_column="date",
            snapshot_max_age_days=1,
        ),
        TableConfig(
            name="capture.events",
            retention_days=None,
            day_column="date",
            snapshot_max_age_days=1,
        ),
        TableConfig(name="capture.sessions", retention_days=None, day_column="date"),
        TableConfig(name="capture.segments", retention_days=None, day_column="date"),
        TableConfig(name="capture.session_ends", retention_days=None, day_column="date"),
        TableConfig(name="refined.packets", retention_days=365, day_column="date"),
        TableConfig(name="refined.client_info", retention_days=365, day_column="date"),
        TableConfig(name="refined.plugin_messages", retention_days=365, day_column="date"),
        TableConfig(name="refined.sessions", retention_days=365, day_column="date"),
        TableConfig(name="features.session_stats", retention_days=365, day_column="date"),
        TableConfig(name="features.windows", retention_days=365, day_column="date"),
        TableConfig(name="quality.check_results", retention_days=400, day_column="date"),
        TableConfig(name="quality.quarantine_sessions", retention_days=30, day_column="date"),
    ]


def test_real_maintenance_yaml_sets_spark_memory_to_3g():
    spark = load_config().spark

    assert spark.driver_memory == "3g"
    assert spark.executor_memory == "3g"


@pytest.mark.parametrize("value", ["3g", "512m", "10g", "1024m"])
def test_accepts_spark_memory_size_strings(value):
    config = parse_config(_with_spark(driver_memory=value, executor_memory=value))

    assert config.spark.driver_memory == value
    assert config.spark.executor_memory == value


@pytest.mark.parametrize("key", ["driver_memory", "executor_memory"])
def test_spark_memory_keys_are_required(key):
    raw = _raw()
    del raw["spark"][key]

    with pytest.raises(ValueError, match=f"config.spark.{key} is required"):
        parse_config(raw)


@pytest.mark.parametrize("value", ["", "3G", "3gb", "1.5g", "0g", "03g", "512", "g", " 3g", 3])
def test_rejects_malformed_spark_memory(value):
    with pytest.raises(ValueError, match="config.spark.driver_memory"):
        parse_config(_with_spark(driver_memory=value))


def test_rejects_malformed_executor_memory_with_its_path():
    with pytest.raises(ValueError, match="config.spark.executor_memory"):
        parse_config(_with_spark(executor_memory="4 g"))


def test_snapshot_max_age_is_none_when_table_does_not_set_it():
    config = parse_config(_raw())

    assert config.tables[0].snapshot_max_age_days is None


def test_accepts_per_table_snapshot_max_age():
    raw = _raw(tables=[{"name": "capture.raw", "retention_days": None, "snapshot_max_age_days": 1}])

    config = parse_config(raw)

    assert config.tables[0].snapshot_max_age_days == 1


@pytest.mark.parametrize("value", [0, -3, True, "1", 1.5])
def test_rejects_malformed_per_table_snapshot_max_age(value):
    raw = _raw(tables=[{"name": "capture.raw", "snapshot_max_age_days": value}])

    with pytest.raises(ValueError, match=r"config.tables\[0\].snapshot_max_age_days"):
        parse_config(raw)


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
