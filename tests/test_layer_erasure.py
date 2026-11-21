import re
from datetime import UTC, datetime

import pytest

from lake.layers.erasure import (
    CAPTURE_TABLES,
    DOWNSTREAM_TABLES,
    STATUS_DONE,
    STATUS_FAILED_CAPTURE,
    STATUS_FAILED_TAG,
    GuardResult,
    capture_guard_query,
    pending_requests_query,
    plan_erasure,
    tag_guard_query,
)

KEY = "ab" * 32
KEY_LITERAL = f"X'{KEY}'"
RUN = datetime(2026, 11, 21, 4, 0, tzinfo=UTC)
RUN_TS = "TIMESTAMP '2026-11-21 04:00:00'"
REQUEST = "req-0001"
RUN_ID = "run_20261121_0400"
CLEAN = GuardResult(capture_rows={table: 0 for table in CAPTURE_TABLES}, tagged=())
BRANCHES = {"refined.packets": ["audit_20261120"], "refined.sessions": ["audit_20261120"]}


def _plan(guard=CLEAN, branches=BRANCHES):
    return plan_erasure(
        request_id=REQUEST,
        player_key=KEY,
        run_id=RUN_ID,
        now=RUN,
        guard=guard,
        audit_branches=branches,
    )


def _deletes(statements):
    return [s for s in statements if s.startswith("DELETE")]


def test_downstream_list_is_the_nine_tables_of_decision_14():
    assert set(DOWNSTREAM_TABLES) == {
        "refined.packets",
        "refined.client_info",
        "refined.plugin_messages",
        "refined.sessions",
        "features.session_stats",
        "features.windows",
        "labels.sessions",
        "labels.windows",
        "quality.quarantine_sessions",
    }


def test_clean_guard_deletes_every_downstream_table_once():
    deletes = _deletes(_plan().statements)

    assert sorted(re.match(r"DELETE FROM (\S+)", s).group(1) for s in deletes) == sorted(
        DOWNSTREAM_TABLES
    )


def test_every_delete_is_filtered_by_player_key_literal():
    for statement in _deletes(_plan().statements):
        assert statement.endswith(f"WHERE player_key = {KEY_LITERAL}"), statement


def test_no_statement_deletes_from_or_writes_to_capture():
    statements = _plan().statements

    for statement in statements:
        assert "capture." not in statement, statement


def test_capture_guard_reads_all_five_tables_through_sessions_for_segments():
    query = capture_guard_query(KEY)

    assert query.startswith("SELECT")
    for table in CAPTURE_TABLES:
        assert f"FROM {table}" in query
    assert "FROM capture.segments WHERE session_id IN" in query
    assert f"FROM capture.sessions WHERE player_key = {KEY_LITERAL}" in query


def test_capture_guard_is_read_only():
    query = capture_guard_query(KEY)

    for keyword in ("DELETE", "INSERT", "MERGE", "UPDATE", "ALTER", "DROP", "CALL"):
        assert keyword not in query.upper().split()


def test_tag_guard_reads_a_tag_and_is_read_only():
    query = tag_guard_query("features.windows", "dataset_1", KEY)

    assert query == (
        "SELECT 1 FROM features.windows VERSION AS OF 'dataset_1' "
        f"WHERE player_key = {KEY_LITERAL} LIMIT 1"
    )


def test_tag_guard_only_covers_the_tagged_tables():
    with pytest.raises(ValueError, match="features.windows"):
        tag_guard_query("labels.sessions", "dataset_1", KEY)


def test_failed_capture_guard_deletes_nothing():
    guard = GuardResult(
        capture_rows={**{t: 0 for t in CAPTURE_TABLES}, "capture.raw": 12}, tagged=()
    )

    plan = _plan(guard=guard)

    assert plan.status == STATUS_FAILED_CAPTURE
    assert _deletes(plan.statements) == []
    assert len(plan.statements) == 1
    assert "capture.raw" in plan.failure_reason


def test_tagged_snapshot_deletes_nothing_and_names_the_tag():
    guard = GuardResult(capture_rows=CLEAN.capture_rows, tagged=("dataset_1",))

    plan = _plan(guard=guard)

    assert plan.status == STATUS_FAILED_TAG
    assert _deletes(plan.statements) == []
    assert "dataset_1" in plan.failure_reason
    assert "failed_tagged_snapshot" in plan.statements[0]


def test_failure_status_is_written_with_run_id_and_time():
    plan = _plan(
        guard=GuardResult(capture_rows={**CLEAN.capture_rows, "capture.raw": 1}, tagged=())
    )

    assert f"'{RUN_ID}' AS lake_run_id" in plan.statements[0]
    assert RUN_TS in plan.statements[0]


def test_done_plan_drops_branches_before_deletes_and_expires_last():
    statements = _plan().statements

    first_drop = next(i for i, s in enumerate(statements) if "DROP BRANCH" in s)
    first_delete = next(i for i, s in enumerate(statements) if s.startswith("DELETE"))
    first_expire = next(i for i, s in enumerate(statements) if "expire_snapshots" in s)
    assert first_drop < first_delete < first_expire


def test_branch_drop_names_only_audit_branches():
    statements = _plan().statements

    assert "ALTER TABLE refined.packets DROP BRANCH IF EXISTS audit_20261120" in statements


def test_refusing_to_drop_main_or_a_non_audit_branch():
    with pytest.raises(ValueError, match="audit branches"):
        _plan(branches={"refined.packets": ["main"]})


def test_expiry_uses_the_run_time_as_older_than_on_every_downstream_table():
    statements = _plan().statements

    for table in DOWNSTREAM_TABLES:
        expire = f"expire_snapshots(table => '{table}', older_than => {RUN_TS}, retain_last => 1)"
        orphans = (
            f"remove_orphan_files(table => '{table}', older_than => {RUN_TS}, "
            "prefix_listing => true)"
        )
        assert any(expire in s for s in statements), table
        assert any(orphans in s for s in statements), table


def test_done_status_is_written_only_for_pending_requests():
    statements = _plan().statements

    status = statements[-1]
    assert f"'{STATUS_DONE}' AS status" in status
    assert "MERGE INTO quality.erasure_requests" in status
    assert "WHEN MATCHED AND target.status = 'pending'" in status


def test_pending_query_reads_only_pending_requests():
    query = pending_requests_query()

    assert query == (
        "SELECT request_id, player_key FROM quality.erasure_requests WHERE status = 'pending'"
    )


def test_player_key_is_validated_before_any_statement():
    with pytest.raises(ValueError, match="player_key"):
        plan_erasure(
            request_id=REQUEST,
            player_key="x' OR '1'='1",
            run_id=RUN_ID,
            now=RUN,
            guard=CLEAN,
            audit_branches={},
        )


def test_naive_run_time_is_rejected():
    with pytest.raises(ValueError, match="timezone-aware"):
        plan_erasure(
            request_id=REQUEST,
            player_key=KEY,
            run_id=RUN_ID,
            now=datetime(2026, 11, 21, 4, 0),
            guard=CLEAN,
            audit_branches={},
        )


def test_plan_is_deterministic_for_the_same_inputs():
    assert _plan().statements == _plan().statements
