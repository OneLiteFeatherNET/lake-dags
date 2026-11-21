"""Load and validate config/layers.yaml. Pure: no Airflow, no Spark, no I/O beyond the YAML read."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from lake.config import (
    _check_keys,
    _mapping,
    _positive_int,
    _required,
    _table_name,
)

LAYERS_CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "layers.yaml"

WINDOW_MIN_PACKETS = 256
WINDOW_MAX_PACKETS = 512

_CHECK_ID = re.compile(r"^[a-z][a-z_]*\.[a-z][a-z_]*$")
_TOP_KEYS = {"window", "split", "checks", "kept_tables"}
_WINDOW_KEYS = {"size_packets"}
_SPLIT_KEYS = {"train_ratio", "seed"}
_CHECK_KEYS = {"id", "blocking", "threshold"}
_KEPT_KEYS = {"name", "retention_days"}


@dataclass(frozen=True)
class WindowConfig:
    size_packets: int


@dataclass(frozen=True)
class SplitConfig:
    train_ratio: float
    seed: int


@dataclass(frozen=True)
class CheckConfig:
    id: str
    blocking: bool
    threshold: float | None


@dataclass(frozen=True)
class KeptTable:
    name: str
    retention_days: int | None


@dataclass(frozen=True)
class LayersConfig:
    window: WindowConfig
    split: SplitConfig
    checks: tuple[CheckConfig, ...]
    kept_tables: tuple[KeptTable, ...]


def load_layers_config(path: Path = LAYERS_CONFIG_PATH) -> LayersConfig:
    with path.open(encoding="utf-8") as handle:
        raw = yaml.safe_load(handle)
    return parse_layers_config(raw)


def parse_layers_config(raw: Any) -> LayersConfig:
    top = _mapping(raw, "config")
    _check_keys(top, _TOP_KEYS, "config")
    return LayersConfig(
        window=_parse_window(_required(top, "window", "config")),
        split=_parse_split(_required(top, "split", "config")),
        checks=_parse_checks(_required(top, "checks", "config")),
        kept_tables=_parse_kept_tables(_required(top, "kept_tables", "config")),
    )


def _parse_window(raw: Any) -> WindowConfig:
    section = _mapping(raw, "config.window")
    _check_keys(section, _WINDOW_KEYS, "config.window")
    size = _required(section, "size_packets", "config.window")
    if isinstance(size, bool) or not isinstance(size, int):
        raise ValueError(f"config.window.size_packets must be an integer, got {size!r}")
    if not WINDOW_MIN_PACKETS <= size <= WINDOW_MAX_PACKETS:
        raise ValueError(
            f"config.window.size_packets must be between {WINDOW_MIN_PACKETS} and "
            f"{WINDOW_MAX_PACKETS}, got {size}"
        )
    return WindowConfig(size_packets=size)


def _parse_split(raw: Any) -> SplitConfig:
    section = _mapping(raw, "config.split")
    _check_keys(section, _SPLIT_KEYS, "config.split")
    ratio = _required(section, "train_ratio", "config.split")
    if isinstance(ratio, bool) or not isinstance(ratio, float) or not 0 < ratio < 1:
        raise ValueError(
            f"config.split.train_ratio must be a number between 0 and 1, got {ratio!r}"
        )
    seed = _non_negative_int(_required(section, "seed", "config.split"), "config.split.seed")
    return SplitConfig(train_ratio=ratio, seed=seed)


def _parse_checks(raw: Any) -> tuple[CheckConfig, ...]:
    if not isinstance(raw, list):
        raise ValueError("config.checks must be a list")
    seen: set[str] = set()
    checks: list[CheckConfig] = []
    for index, entry in enumerate(raw):
        where = f"config.checks[{index}]"
        section = _mapping(entry, where)
        _check_keys(section, _CHECK_KEYS, where)
        check_id = _check_id(_required(section, "id", where), f"{where}.id")
        if check_id in seen:
            raise ValueError(f"config.checks: duplicate check '{check_id}'")
        seen.add(check_id)
        blocking = _required(section, "blocking", where)
        if not isinstance(blocking, bool):
            raise ValueError(f"{where}.blocking must be true or false, got {blocking!r}")
        threshold = section.get("threshold")
        if threshold is not None:
            threshold = _non_negative_number(threshold, f"{where}.threshold")
        checks.append(CheckConfig(id=check_id, blocking=blocking, threshold=threshold))
    return tuple(checks)


def _parse_kept_tables(raw: Any) -> tuple[KeptTable, ...]:
    if not isinstance(raw, list):
        raise ValueError("config.kept_tables must be a list")
    seen: set[str] = set()
    tables: list[KeptTable] = []
    for index, entry in enumerate(raw):
        where = f"config.kept_tables[{index}]"
        section = _mapping(entry, where)
        _check_keys(section, _KEPT_KEYS, where)
        name = _table_name(_required(section, "name", where), f"{where}.name")
        if name in seen:
            raise ValueError(f"config.kept_tables: duplicate table '{name}'")
        seen.add(name)
        retention = section.get("retention_days")
        if retention is not None:
            retention = _positive_int(retention, f"{where}.retention_days")
        tables.append(KeptTable(name=name, retention_days=retention))
    return tuple(tables)


def _check_id(value: Any, where: str) -> str:
    if not isinstance(value, str) or not _CHECK_ID.match(value):
        raise ValueError(f"{where} must be a dotted identifier like 'dup.rate', got {value!r}")
    return value


def _non_negative_int(value: Any, where: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{where} must be an integer >= 0, got {value!r}")
    return value


def _non_negative_number(value: Any, where: str) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float) or value < 0:
        raise ValueError(f"{where} must be a number >= 0, got {value!r}")
    return float(value)
