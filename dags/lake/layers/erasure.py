"""GDPR erasure builders for the downstream layers (design decisions 13 and 14). Pure.

Corvus erases the five capture tables itself. The lake erases every downstream table for one
pseudonym, but only after two guards pass:

1. capture guard: the player has no rows left in any of the five capture tables;
2. tag guard: no tag of features.windows or refined.packets still holds the key.

A failed guard produces a status update and no delete. This module only builds statements. The
Spark job runs the guard queries, passes their results to plan_erasure and executes the plan.
Only the guard queries read capture. No statement writes to capture.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime

from lake.layers.literals import (
    identifier,
    phrase_literal,
    player_key_literal,
    string_literal,
    table_name,
    timestamp_literal,
)

CAPTURE_TABLES = (
    "capture.raw",
    "capture.events",
    "capture.sessions",
    "capture.segments",
    "capture.session_ends",
)
DOWNSTREAM_TABLES = (
    "refined.packets",
    "refined.client_info",
    "refined.plugin_messages",
    "refined.sessions",
    "features.session_stats",
    "features.windows",
    "labels.sessions",
    "labels.windows",
    "quality.quarantine_sessions",
)
TAGGED_TABLES = ("features.windows", "refined.packets")
CATALOG = "corvus"
STATUS_PENDING = "pending"
STATUS_DONE = "done"
STATUS_FAILED_CAPTURE = "failed_capture_not_empty"
STATUS_FAILED_TAG = "failed_tagged_snapshot"
_AUDIT_PREFIX = "audit_"


@dataclass(frozen=True)
class GuardResult:
    """Guard outcome: row count per capture table, and the tags that still hold the key."""

    capture_rows: Mapping[str, int]
    tagged: Sequence[str]


@dataclass(frozen=True)
class ErasurePlan:
    status: str
    failure_reason: str | None
    statements: tuple[str, ...]


def pending_requests_query() -> str:
    return (
        "SELECT request_id, player_key FROM quality.erasure_requests "
        f"WHERE status = {string_literal(STATUS_PENDING)}"
    )


def capture_guard_query(player_key: str) -> str:
    """One row with the player's row count in each capture table. Read only."""
    key = player_key_literal(player_key)
    return (
        "SELECT "
        f"(SELECT count(*) FROM capture.raw WHERE player_key = {key}) AS raw_rows, "
        f"(SELECT count(*) FROM capture.events WHERE player_key = {key}) AS events_rows, "
        f"(SELECT count(*) FROM capture.sessions WHERE player_key = {key}) AS sessions_rows, "
        "(SELECT count(*) FROM capture.segments WHERE session_id IN "
        f"(SELECT session_id FROM capture.sessions WHERE player_key = {key})) AS segments_rows, "
        f"(SELECT count(*) FROM capture.session_ends WHERE player_key = {key}) AS session_ends_rows"
    )


def tag_guard_query(table: str, tag: str, player_key: str) -> str:
    """Does the tag still hold rows of the key? Read only."""
    if table not in TAGGED_TABLES:
        raise ValueError(f"tags are guarded only on {', '.join(TAGGED_TABLES)}, not {table}")
    return (
        f"SELECT 1 FROM {table} VERSION AS OF {string_literal(identifier(tag))} "
        f"WHERE player_key = {player_key_literal(player_key)} LIMIT 1"
    )


def plan_erasure(
    *,
    request_id: str,
    player_key: str,
    run_id: str,
    now: datetime,
    guard: GuardResult,
    audit_branches: Mapping[str, Sequence[str]],
    catalog: str = CATALOG,
) -> ErasurePlan:
    """Statements for one pending request, in the order they must run.

    A failed guard returns only the status update. Otherwise the plan drops the audit branches,
    deletes the key from every downstream table, expires snapshots and removes orphan files with
    the run time as older_than, and then writes status done.
    """
    key = player_key_literal(player_key)
    run_time = timestamp_literal(now)
    request = string_literal(request_id)
    run = string_literal(run_id)
    catalog = identifier(catalog)

    missing = [table for table in CAPTURE_TABLES if table not in guard.capture_rows]
    if missing:
        raise ValueError(f"capture guard has no count for {', '.join(missing)}")
    non_empty = [t for t in CAPTURE_TABLES if guard.capture_rows[t] != 0]
    if non_empty:
        return _failed(
            request,
            run,
            run_time,
            STATUS_FAILED_CAPTURE,
            f"capture still holds rows of the key in {', '.join(non_empty)}",
        )
    if guard.tagged:
        return _failed(
            request,
            run,
            run_time,
            STATUS_FAILED_TAG,
            f"tag {', '.join(guard.tagged)} still holds rows of the key",
        )

    statements: list[str] = []
    for table in sorted(audit_branches):
        table_name(table)
        for branch in audit_branches[table]:
            if not branch.startswith(_AUDIT_PREFIX):
                raise ValueError(f"only audit branches may be dropped, got '{branch}'")
            statements.append(f"ALTER TABLE {table} DROP BRANCH IF EXISTS {identifier(branch)}")
    for table in DOWNSTREAM_TABLES:
        statements.append(f"DELETE FROM {table} WHERE player_key = {key}")
    for table in DOWNSTREAM_TABLES:
        statements.append(
            f"CALL {catalog}.system.expire_snapshots(table => {string_literal(table)}, "
            f"older_than => {run_time}, retain_last => 1)"
        )
        statements.append(
            f"CALL {catalog}.system.remove_orphan_files(table => {string_literal(table)}, "
            f"older_than => {run_time}, prefix_listing => true)"
        )
    statements.append(_status_merge(request, run, run_time, STATUS_DONE, None))
    return ErasurePlan(status=STATUS_DONE, failure_reason=None, statements=tuple(statements))


def _failed(request: str, run: str, run_time: str, status: str, reason: str) -> ErasurePlan:
    return ErasurePlan(
        status=status,
        failure_reason=reason,
        statements=(_status_merge(request, run, run_time, status, reason),),
    )


def _status_merge(request: str, run: str, run_time: str, status: str, reason: str | None) -> str:
    reason_sql = "CAST(NULL AS STRING)" if reason is None else phrase_literal(reason)
    return (
        "MERGE INTO quality.erasure_requests AS target "
        f"USING (SELECT {request} AS request_id, {string_literal(status)} AS status, "
        f"{reason_sql} AS failure_reason, {run} AS lake_run_id, "
        f"{run_time} AS completed_at) AS source "
        "ON target.request_id = source.request_id "
        f"WHEN MATCHED AND target.status = {string_literal(STATUS_PENDING)} THEN UPDATE SET "
        "target.status = source.status, "
        "target.failure_reason = source.failure_reason, "
        "target.lake_run_id = source.lake_run_id, "
        "target.completed_at = source.completed_at"
    )
