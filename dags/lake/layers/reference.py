"""Reference CSV validation and MERGE statements for packet names and backends (decision 6).

Pure: the files are read by the caller and passed in as text. Every file is validated in full
before any statement is built, so an invalid file produces no statement at all. A file replaces
the rows of the protocol versions (packet names) or backends (backend configuration) it names.
"""

from __future__ import annotations

import csv
import io
import re
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date

from lake.layers.literals import identifier, int_literal, string_literal

PACKET_NAME_COLUMNS = ("protocol_version", "state", "direction", "packet_id", "packet_name")
BACKEND_COLUMNS = ("backend", "period_start", "period_end", "anti_xray_mode")
STATES = ("handshaking", "status", "login", "configuration", "play")
DIRECTIONS = ("serverbound", "clientbound")
ANTI_XRAY_MODES = ("none", "simple", "advanced")

_PACKET_NAME = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,95}$")
_BACKEND = re.compile(r"^[a-z][a-z0-9_-]{0,63}$")
_DIGITS = re.compile(r"^[0-9]+$")
_ISO_DATE = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")
_MAX_INT32 = 0x7FFFFFFF


@dataclass(frozen=True)
class PacketName:
    protocol_version: int
    state: str
    direction: str
    packet_id: int
    packet_name: str


@dataclass(frozen=True)
class BackendPeriod:
    backend: str
    period_start: date
    period_end: date | None
    anti_xray_mode: str


@dataclass(frozen=True)
class PacketNameLoad:
    rows: tuple[PacketName, ...]
    protocol_versions: tuple[int, ...]
    statements: tuple[str, ...]


@dataclass(frozen=True)
class BackendConfigLoad:
    rows: tuple[BackendPeriod, ...]
    backends: tuple[str, ...]
    statements: tuple[str, ...]


def parse_packet_names(text: str) -> list[PacketName]:
    """Validate ref.packet_names rows. Any invalid row rejects the whole file."""
    reader = _reader(text, PACKET_NAME_COLUMNS)
    rows: list[PacketName] = []
    seen: set[tuple[int, str, str, int]] = set()
    for line, values in reader:
        version = _int(values[0], "protocol_version", line, minimum=1)
        state = _choice(values[1], STATES, "state", line)
        direction = _choice(values[2], DIRECTIONS, "direction", line)
        packet_id = _int(values[3], "packet_id", line, minimum=0)
        name = values[4]
        if not _PACKET_NAME.match(name):
            raise ValueError(f"line {line}: packet_name '{name}' must be a plain identifier")
        key = (version, state, direction, packet_id)
        if key in seen:
            raise ValueError(f"line {line}: duplicate key {key}")
        seen.add(key)
        rows.append(PacketName(version, state, direction, packet_id, name))
    if not rows:
        raise ValueError("packet name file has no rows")
    return rows


def parse_backend_config(text: str) -> list[BackendPeriod]:
    """Validate backend periods. Periods are half-open; an empty end means open-ended."""
    reader = _reader(text, BACKEND_COLUMNS)
    periods: list[BackendPeriod] = []
    for line, values in reader:
        backend = values[0]
        if not _BACKEND.match(backend):
            raise ValueError(f"line {line}: backend '{backend}' must be a lower case name")
        start = _date(values[1], "period_start", line)
        end = _date(values[2], "period_end", line) if values[2] else None
        if end is not None and end <= start:
            raise ValueError(f"line {line}: period_end must be after period_start")
        mode = _choice(values[3], ANTI_XRAY_MODES, "anti_xray_mode", line)
        periods.append(BackendPeriod(backend, start, end, mode))
    if not periods:
        raise ValueError("backend config file has no rows")
    _check_no_overlap(periods)
    return periods


def load_packet_names(text: str, source_view: str) -> PacketNameLoad:
    rows = parse_packet_names(text)
    versions = tuple(sorted({row.protocol_version for row in rows}))
    return PacketNameLoad(
        rows=tuple(rows),
        protocol_versions=versions,
        statements=(packet_names_statement(source_view, versions),),
    )


def load_backend_config(text: str, source_view: str) -> BackendConfigLoad:
    rows = parse_backend_config(text)
    backends = tuple(sorted({row.backend for row in rows}))
    return BackendConfigLoad(
        rows=tuple(rows),
        backends=backends,
        statements=(backend_config_statement(source_view, backends),),
    )


def packet_names_statement(source_view: str, protocol_versions: Iterable[int]) -> str:
    """Replace the rows of the given protocol versions from the validated source view."""
    versions = sorted(protocol_versions)
    if not versions:
        raise ValueError("at least one protocol version is required")
    version_list = ", ".join(int_literal(version) for version in versions)
    return (
        "MERGE INTO ref.packet_names AS target "
        f"USING {identifier(source_view)} AS source "
        "ON target.protocol_version = source.protocol_version "
        "AND target.state = source.state "
        "AND target.direction = source.direction "
        "AND target.packet_id = source.packet_id "
        "WHEN MATCHED AND target.packet_name <> source.packet_name THEN UPDATE SET "
        "target.packet_name = source.packet_name "
        "WHEN NOT MATCHED THEN INSERT * "
        f"WHEN NOT MATCHED BY SOURCE AND target.protocol_version IN ({version_list}) "
        "THEN DELETE"
    )


def backend_config_statement(source_view: str, backends: Iterable[str]) -> str:
    """Replace the periods of the given backends from the validated source view."""
    names = sorted(backends)
    if not names:
        raise ValueError("at least one backend is required")
    backend_list = ", ".join(string_literal(name) for name in names)
    return (
        "MERGE INTO ref.backend_config AS target "
        f"USING {identifier(source_view)} AS source "
        "ON target.backend = source.backend AND target.period_start = source.period_start "
        "WHEN MATCHED THEN UPDATE SET "
        "target.period_end = source.period_end, "
        "target.anti_xray_mode = source.anti_xray_mode "
        "WHEN NOT MATCHED THEN INSERT * "
        f"WHEN NOT MATCHED BY SOURCE AND target.backend IN ({backend_list}) THEN DELETE"
    )


def _reader(text: str, columns: tuple[str, ...]) -> list[tuple[int, list[str]]]:
    rows = csv.reader(io.StringIO(text))
    try:
        header = next(rows)
    except StopIteration:
        raise ValueError("reference file is empty") from None
    if tuple(header) != columns:
        raise ValueError(f"reference header must be {','.join(columns)}")
    result: list[tuple[int, list[str]]] = []
    for line, row in enumerate(rows, start=2):
        if not row:
            continue
        if len(row) != len(columns):
            raise ValueError(f"line {line}: expected {len(columns)} columns, got {len(row)}")
        result.append((line, row))
    return result


def _choice(value: str, allowed: tuple[str, ...], column: str, line: int) -> str:
    if value not in allowed:
        raise ValueError(f"line {line}: {column} must be one of {', '.join(allowed)}")
    return value


def _int(value: str, column: str, line: int, minimum: int) -> int:
    if not _DIGITS.match(value) or int(value) < minimum or int(value) > _MAX_INT32:
        raise ValueError(f"line {line}: {column} must be an integer from {minimum} to 2147483647")
    return int(value)


def _date(value: str, column: str, line: int) -> date:
    if not _ISO_DATE.match(value):
        raise ValueError(f"line {line}: {column} must be a date like 2026-01-31")
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise ValueError(f"line {line}: {column} must be a date like 2026-01-31") from None


def _check_no_overlap(periods: list[BackendPeriod]) -> None:
    by_backend: dict[str, list[BackendPeriod]] = {}
    for period in periods:
        by_backend.setdefault(period.backend, []).append(period)
    for backend, group in by_backend.items():
        ordered = sorted(group, key=lambda period: period.period_start)
        for earlier, later in zip(ordered, ordered[1:], strict=False):
            if earlier.period_end is None or later.period_start < earlier.period_end:
                raise ValueError(f"periods of backend '{backend}' overlap")
