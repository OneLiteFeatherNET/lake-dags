"""Load and validate config/maintenance.yaml. Pure: no Airflow, no Kubernetes."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

CONFIG_PATH = Path(__file__).resolve().parents[1] / "config" / "maintenance.yaml"

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_TOP_KEYS = {"catalog", "snapshot_max_age_days", "spark", "tables"}
_SPARK_KEYS = {"spark_version", "image", "iceberg_runtime", "iceberg_aws_bundle"}
_TABLE_KEYS = {"name", "retention_days", "day_column"}


@dataclass(frozen=True)
class SparkSettings:
    spark_version: str
    image: str
    iceberg_runtime: str
    iceberg_aws_bundle: str


@dataclass(frozen=True)
class TableConfig:
    name: str
    retention_days: int | None
    day_column: str = "date"


@dataclass(frozen=True)
class MaintenanceConfig:
    catalog: str
    spark: SparkSettings
    tables: list[TableConfig]
    snapshot_max_age_days: int = 7


def load_config(path: Path = CONFIG_PATH) -> MaintenanceConfig:
    with path.open(encoding="utf-8") as handle:
        raw = yaml.safe_load(handle)
    return parse_config(raw)


def parse_config(raw: Any) -> MaintenanceConfig:
    top = _mapping(raw, "config")
    _check_keys(top, _TOP_KEYS, "config")
    catalog = _identifier(_required(top, "catalog", "config"), "config.catalog")
    snapshot_max_age_days = _positive_int(
        top.get("snapshot_max_age_days", 7), "config.snapshot_max_age_days"
    )
    spark = _parse_spark(_required(top, "spark", "config"))
    tables = _parse_tables(_required(top, "tables", "config"))
    return MaintenanceConfig(
        catalog=catalog,
        spark=spark,
        tables=tables,
        snapshot_max_age_days=snapshot_max_age_days,
    )


def _parse_spark(raw: Any) -> SparkSettings:
    section = _mapping(raw, "config.spark")
    _check_keys(section, _SPARK_KEYS, "config.spark")
    values = {
        key: _non_empty_str(_required(section, key, "config.spark"), f"config.spark.{key}")
        for key in _SPARK_KEYS
    }
    return SparkSettings(**values)


def _parse_tables(raw: Any) -> list[TableConfig]:
    if not isinstance(raw, list):
        raise ValueError("config.tables must be a list")
    seen: set[str] = set()
    tables: list[TableConfig] = []
    for index, entry in enumerate(raw):
        where = f"config.tables[{index}]"
        section = _mapping(entry, where)
        _check_keys(section, _TABLE_KEYS, where)
        name = _table_name(_required(section, "name", where), f"{where}.name")
        if name in seen:
            raise ValueError(f"config.tables: duplicate table '{name}'")
        seen.add(name)
        retention = section.get("retention_days")
        if retention is not None:
            retention = _positive_int(retention, f"{where}.retention_days")
        day_column = _identifier(section.get("day_column", "date"), f"{where}.day_column")
        tables.append(TableConfig(name=name, retention_days=retention, day_column=day_column))
    return tables


def _table_name(value: Any, where: str) -> str:
    text = _non_empty_str(value, where)
    parts = text.split(".")
    if len(parts) != 2:
        raise ValueError(f"{where}: table name must be 'namespace.table', got '{text}'")
    _identifier(parts[0], where)
    _identifier(parts[1], where)
    return text


def _identifier(value: Any, where: str) -> str:
    text = _non_empty_str(value, where)
    if not _IDENTIFIER.match(text):
        raise ValueError(
            f"{where}: '{text}' must be a plain identifier (letters, digits, underscore)"
        )
    return text


def _positive_int(value: Any, where: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{where} must be an integer >= 1, got {value!r}")
    return value


def _non_empty_str(value: Any, where: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{where} must be a non-empty string")
    return value


def _required(section: dict[str, Any], key: str, where: str) -> Any:
    if key not in section:
        raise ValueError(f"{where}.{key} is required")
    return section[key]


def _mapping(value: Any, where: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{where} must be a mapping")
    return value


def _check_keys(section: dict[str, Any], allowed: set[str], where: str) -> None:
    unknown = sorted(str(key) for key in section if key not in allowed)
    if unknown:
        raise ValueError(f"{where}: unknown key(s): {', '.join(unknown)}")
