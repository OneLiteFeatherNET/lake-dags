"""Label rules, admin CSV batches and the MERGE upsert (design decisions 9 and 10).

Pure: no Spark, no Airflow, no I/O beyond the strings passed in, no clock.
"""

from __future__ import annotations

import csv
import io
import re
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol, runtime_checkable

UNKNOWN = "unknown"
LABELS = ("trusted", "untrusted", "cheater")
LABEL_REASONS = ("known", "behaviour", "client_not_vanilla", "blackhole_ban")
SOURCES = ("admin", "blackhole")
CONTROLLED_SOURCE = "controlled_recording"

LABEL_COLUMNS = (
    "label_id",
    "player_key",
    "session_id",
    "start_ts",
    "end_ts",
    "label",
    "hack_types",
    "label_reason",
    "source",
    "editor",
    "edited_at",
    "record_version",
)
WINDOW_COLUMNS = (
    "label_id",
    "session_id",
    "player_key",
    "module",
    "active",
    "event_ts",
    "source",
    "editor",
    "record_version",
)
_TABLE_COLUMNS = {
    "labels.sessions": LABEL_COLUMNS,
    "labels.windows": WINDOW_COLUMNS,
}

_LABEL_ID = re.compile(r"^[A-Za-z0-9_.:-]{1,80}$")
_PLAYER_KEY = re.compile(r"^[0-9a-fA-F]{64}$")
_SESSION_ID = re.compile(r"^[0-9a-fA-F]{32}$")
_HACK_TYPE = re.compile(r"^[a-z][a-z0-9_]{0,31}$")
_EDITOR = re.compile(r"^[A-Za-z0-9_.@-]{1,64}$")
_IDENTIFIER = re.compile(r"^[a-z_][a-z0-9_]*$")
_TIMESTAMP = "%Y-%m-%dT%H:%M:%SZ"


@dataclass(frozen=True)
class SessionLabel:
    """One row of labels.sessions (design decision 9)."""

    label_id: str
    player_key: str
    session_id: str | None
    start_ts: datetime
    end_ts: datetime
    label: str
    hack_types: tuple[str, ...]
    label_reason: str
    source: str
    editor: str
    edited_at: datetime
    record_version: int


@dataclass(frozen=True)
class WindowLabel:
    """Label of one feature window. label is None when no record overlaps the window."""

    label: str | None
    hack_types: tuple[str, ...]
    conflict: bool


@dataclass(frozen=True)
class ControlledEvent:
    """An on or off event of a module from a controlled recording."""

    session_id: str
    player_key: str
    module: str
    active: bool
    event_ts: datetime


@dataclass(frozen=True)
class ModuleWindow:
    """A module-active interval: from an on event to the next off event."""

    session_id: str
    player_key: str
    module: str
    start_ts: datetime
    end_ts: datetime


@runtime_checkable
class LabelSource(Protocol):
    """Adapter interface for a label source such as Blackhole. No concrete source ships in v1."""

    def fetch_since(self, since: datetime) -> Sequence[SessionLabel]: ...


def parse_label_batch(text: str) -> list[SessionLabel]:
    """Validate a reviewed CSV batch for labels.sessions. Any invalid row rejects the batch."""
    reader = csv.reader(io.StringIO(text))
    try:
        header = next(reader)
    except StopIteration:
        raise ValueError("label batch is empty") from None
    if tuple(header) != LABEL_COLUMNS:
        raise ValueError(f"label batch header must be {','.join(LABEL_COLUMNS)}")
    records: list[SessionLabel] = []
    seen: set[str] = set()
    for line, row in enumerate(reader, start=2):
        if not row:
            continue
        if len(row) != len(LABEL_COLUMNS):
            raise ValueError(f"line {line}: expected {len(LABEL_COLUMNS)} columns, got {len(row)}")
        record = _parse_row(dict(zip(LABEL_COLUMNS, row, strict=True)), line)
        if record.label_id in seen:
            raise ValueError(f"line {line}: duplicate label_id '{record.label_id}'")
        seen.add(record.label_id)
        records.append(record)
    return records


def _parse_row(values: dict[str, str], line: int) -> SessionLabel:
    def fail(message: str) -> ValueError:
        return ValueError(f"line {line}: {message}")

    label_id = values["label_id"]
    if not _LABEL_ID.match(label_id):
        raise fail(f"label_id '{label_id}' is not a plain identifier")
    player_key = values["player_key"]
    if not _PLAYER_KEY.match(player_key):
        raise fail("player_key must be 64 hex characters")
    session_text = values["session_id"]
    if session_text and not _SESSION_ID.match(session_text):
        raise fail("session_id must be empty or 32 hex characters")
    start_ts = _timestamp(values["start_ts"], "start_ts", fail)
    end_ts = _timestamp(values["end_ts"], "end_ts", fail)
    if end_ts <= start_ts:
        raise fail("end_ts must be after start_ts")
    label = values["label"]
    if label not in LABELS:
        raise fail(f"label must be one of {', '.join(LABELS)}")
    hack_types = _hack_types(values["hack_types"], label, fail)
    reason = values["label_reason"]
    if reason not in LABEL_REASONS:
        raise fail(f"label_reason must be one of {', '.join(LABEL_REASONS)}")
    source = values["source"]
    if source not in SOURCES:
        raise fail(f"source must be one of {', '.join(SOURCES)}")
    editor = values["editor"]
    if not _EDITOR.match(editor):
        raise fail("editor must be 1 to 64 characters of letters, digits and . @ _ -")
    edited_at = _timestamp(values["edited_at"], "edited_at", fail)
    version_text = values["record_version"]
    if not re.fullmatch(r"[0-9]+", version_text) or int(version_text) < 1:
        raise fail("record_version must be an integer >= 1")
    return SessionLabel(
        label_id=label_id,
        player_key=player_key.lower(),
        session_id=session_text.lower() or None,
        start_ts=start_ts,
        end_ts=end_ts,
        label=label,
        hack_types=hack_types,
        label_reason=reason,
        source=source,
        editor=editor,
        edited_at=edited_at,
        record_version=int(version_text),
    )


def _timestamp(text: str, column: str, fail) -> datetime:
    try:
        return datetime.strptime(text, _TIMESTAMP).replace(tzinfo=UTC)
    except ValueError:
        raise fail(
            f"{column} must be an ISO 8601 UTC timestamp like 2026-11-20T10:00:00Z"
        ) from None


def _hack_types(text: str, label: str, fail) -> tuple[str, ...]:
    if label != "cheater":
        if text:
            raise fail("hack_types must be empty unless label is cheater")
        return ()
    if not text:
        raise fail("hack_types must name at least one hack type for a cheater label")
    names = tuple(text.split(";"))
    for name in names:
        if not _HACK_TYPE.match(name):
            raise fail(f"hack type '{name}' must be lower case letters, digits and underscores")
    if len(set(names)) != len(names):
        raise fail("hack_types must not repeat a hack type")
    return names


def latest_per_label_id(records: Iterable[SessionLabel]) -> list[SessionLabel]:
    """Keep the newest record_version per label_id, as the MERGE does."""
    newest: dict[str, SessionLabel] = {}
    for record in records:
        current = newest.get(record.label_id)
        if current is None or record.record_version > current.record_version:
            newest[record.label_id] = record
        elif record.record_version == current.record_version and record != current:
            raise ValueError(
                f"label_id '{record.label_id}' has two different records at version "
                f"{record.record_version}"
            )
    return list(newest.values())


def overlapping(
    records: Iterable[SessionLabel], start: datetime, end: datetime
) -> list[SessionLabel]:
    return [record for record in records if record.start_ts < end and start < record.end_ts]


def resolve_label(records: Iterable[SessionLabel]) -> WindowLabel:
    """Label rules (design decision 9) for the records that cover one window.

    untrusted is unknown and never a negative or a positive. trusted is a negative. cheater is a
    positive with its hack types. A cheater mixed with trusted or untrusted is a conflict: the
    result is unknown and conflict is set, never resolved silently.
    """
    records = list(records)
    kinds = {record.label for record in records}
    if not kinds:
        return WindowLabel(label=None, hack_types=(), conflict=False)
    hack_types = tuple(
        sorted(
            {hack for record in records if record.label == "cheater" for hack in record.hack_types}
        )
    )
    if "cheater" in kinds and kinds & {"trusted", "untrusted"}:
        return WindowLabel(label=UNKNOWN, hack_types=(), conflict=True)
    if "untrusted" in kinds:
        return WindowLabel(label=UNKNOWN, hack_types=(), conflict=False)
    if "cheater" in kinds:
        return WindowLabel(label="cheater", hack_types=hack_types, conflict=False)
    return WindowLabel(label="trusted", hack_types=(), conflict=False)


def resolve_window(records: Iterable[SessionLabel], start: datetime, end: datetime) -> WindowLabel:
    """Label of a window: only the records that overlap its time range count."""
    return resolve_label(overlapping(records, start, end))


def module_windows(events: Sequence[ControlledEvent]) -> list[ModuleWindow]:
    """Pair each on event with the next off event of the same module and session.

    An off without an on, a second on before the off, or an on that is never closed is rejected.
    """
    groups: dict[tuple[str, str], list[ControlledEvent]] = {}
    for event in events:
        groups.setdefault((event.session_id, event.module), []).append(event)
    windows: list[ModuleWindow] = []
    for (session_id, module), group in groups.items():
        ordered = sorted(group, key=lambda event: event.event_ts)
        open_on: ControlledEvent | None = None
        for event in ordered:
            if event.active:
                if open_on is not None:
                    raise ValueError(f"module '{module}' in session has two on events in a row")
                open_on = event
                continue
            if open_on is None:
                raise ValueError(f"module '{module}' in session has an off event without on")
            windows.append(
                ModuleWindow(
                    session_id=session_id,
                    player_key=open_on.player_key,
                    module=module,
                    start_ts=open_on.event_ts,
                    end_ts=event.event_ts,
                )
            )
            open_on = None
        if open_on is not None:
            raise ValueError(f"module '{module}' in session has an on event that is never closed")
    return windows


def label_merge_statement(table: str, source: str) -> str:
    """MERGE upsert by label_id. Only a newer record_version replaces a row. Never deletes."""
    columns = _TABLE_COLUMNS.get(table)
    if columns is None:
        raise ValueError(f"table '{table}' is not a label table (labels.sessions, labels.windows)")
    if not _IDENTIFIER.match(source):
        raise ValueError(f"source '{source}' must be a plain identifier")
    updates = ", ".join(f"target.{c} = source.{c}" for c in columns if c != "label_id")
    insert_columns = ", ".join(columns)
    insert_values = ", ".join(f"source.{c}" for c in columns)
    return (
        f"MERGE INTO {table} AS target "
        f"USING {source} AS source "
        "ON target.label_id = source.label_id "
        "WHEN MATCHED AND source.record_version > target.record_version "
        f"THEN UPDATE SET {updates} "
        f"WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values})"
    )
