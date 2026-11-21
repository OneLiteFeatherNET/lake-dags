"""Data quality check catalogue and verdict rules (design decision 4). Pure: no Spark, no I/O."""

from __future__ import annotations

from collections.abc import Collection, Iterable
from dataclasses import dataclass
from enum import StrEnum


class Scope(StrEnum):
    TABLE = "table"
    SESSION = "session"


class Outcome(StrEnum):
    """What a check measured: the rule held, was broken, or had nothing to measure."""

    PASSED = "passed"
    FAILED = "failed"
    NOT_AVAILABLE = "not_available"


class Verdict(StrEnum):
    PASS = "pass"
    WARN = "warn"
    FAIL = "fail"
    OVERRIDDEN = "overridden"
    NOT_AVAILABLE = "not_available"


class SessionVerdict(StrEnum):
    ACCEPTED = "accepted"
    FLAGGED = "flagged"
    QUARANTINED = "quarantined"


@dataclass(frozen=True)
class CheckSpec:
    id: str
    scope: Scope
    rule: str


CATALOGUE: tuple[CheckSpec, ...] = (
    CheckSpec("schema.capture_raw", Scope.TABLE, "columns and types match the schema version"),
    CheckSpec("schema.capture_events", Scope.TABLE, "event columns match the schema version"),
    CheckSpec("events.coverage", Scope.TABLE, "keys present in only one of raw and events"),
    CheckSpec("key.nulls", Scope.TABLE, "null rate of the key columns is zero"),
    CheckSpec("rows.reconcile", Scope.TABLE, "distinct keys equal refined rows plus quarantined"),
    CheckSpec("rows.baseline", Scope.TABLE, "rows at least half the median of the 7 days before"),
    CheckSpec("dup.rate", Scope.TABLE, "exact duplicate rate"),
    CheckSpec("dup.conflict", Scope.SESSION, "same key with different content hash"),
    CheckSpec("seq.gaps", Scope.SESSION, "missing seq ratio per session"),
    CheckSpec("time.monotonic", Scope.SESSION, "share of timestamp decreases in seq order"),
    CheckSpec("time.skew", Scope.TABLE, "p99 of server minus client time"),
    CheckSpec("drops.reported", Scope.SESSION, "sum of the drop counters of the session"),
    CheckSpec("session.complete", Scope.SESSION, "login and disconnect event present"),
    CheckSpec("payload.present", Scope.TABLE, "payload null rate for classes that need it"),
    CheckSpec("versions.distribution", Scope.TABLE, "protocol versions missing from the names"),
)

_BY_ID = {spec.id: spec for spec in CATALOGUE}


def check_verdict(check_id: str, blocking: bool, outcome: Outcome, forced: bool = False) -> Verdict:
    """Verdict of one table-scope check. A forced publish overrides a blocking failure only."""
    spec = _BY_ID.get(check_id)
    if spec is None:
        raise ValueError(f"unknown check '{check_id}'")
    if spec.scope is Scope.SESSION:
        raise ValueError(f"check '{check_id}' is session scope: it is recorded in refined.sessions")
    if outcome is Outcome.NOT_AVAILABLE:
        return Verdict.NOT_AVAILABLE
    if outcome is Outcome.PASSED:
        return Verdict.PASS
    if not blocking:
        return Verdict.WARN
    return Verdict.OVERRIDDEN if forced else Verdict.FAIL


def day_publishable(verdicts: Iterable[Verdict]) -> bool:
    """A day is published only when no check of it has failed."""
    return all(verdict is not Verdict.FAIL for verdict in verdicts)


def session_verdict(quarantine_failed: bool, flags: Collection[str]) -> SessionVerdict:
    """Quarantine outranks flags; a session with neither is accepted."""
    if quarantine_failed:
        return SessionVerdict.QUARANTINED
    if flags:
        return SessionVerdict.FLAGGED
    return SessionVerdict.ACCEPTED


def below_baseline(rows: int, median_rows: float, ratio: float) -> bool:
    """True when the day has fewer rows than ratio times the median of the days before."""
    if not 0 <= ratio <= 1:
        raise ValueError(f"ratio must be between 0 and 1, got {ratio}")
    return rows < ratio * median_rows
