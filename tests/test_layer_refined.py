import re
from datetime import UTC, date, datetime, timedelta

import pytest

from lake.layers.checks import Outcome, Verdict
from lake.layers.config import load_layers_config
from lake.layers.refined import (
    PACKET_COLUMNS,
    WAP_TABLES,
    CheckResult,
    SessionThresholds,
    build_refined_plan,
    check_results_insert,
    drop_branch,
    reconcile_outcome,
    session_thresholds,
    stale_branches_query,
)

CATALOG = "corvus"
DAY = date(2026, 11, 18)
NOW = datetime(2026, 11, 19, 4, 0, tzinfo=UTC)
RUN = "run_20261118"
BRANCH = "audit_run_20261118"
THRESHOLDS = SessionThresholds(gap_ratio=0.01, monotonic_ratio=0.001)
CAPTURE_SOURCES = {"raw", "events", "sessions", "segments", "session_ends"}
WRITE_VERBS = ("INSERT", "MERGE", "DELETE", "UPDATE", "ALTER", "DROP", "TRUNCATE", "CALL")


def _plan(day=DAY, run_id=RUN):
    return build_refined_plan(CATALOG, day, run_id, THRESHOLDS)


def _all_statements(plan):
    return [
        *plan.setup,
        *plan.write,
        plan.reconcile,
        *plan.publish,
        *plan.drop_branch,
        *plan.record_quarantine,
    ]


def _view_text(plan, name):
    matches = [s for s in plan.setup if f"VIEW {name} " in s]
    assert len(matches) == 1, f"expected one view {name}"
    return matches[0]


def _capture_refs(statement):
    return set(re.findall(r"capture\.(\w+)", statement))


# Structure of the build: which statements write what


def test_every_write_targets_a_refined_table_or_a_quality_table():
    plan = _plan()
    targets = []
    for statement in [*plan.write, *plan.record_quarantine]:
        if statement.startswith("INSERT"):
            targets.append(re.search(r"INSERT OVERWRITE (\S+)", statement).group(1))
    assert targets, "the build writes at least one table"
    for target in targets:
        assert target.startswith((f"{CATALOG}.refined.", f"{CATALOG}.quality.")), target


def test_wap_tables_are_the_four_refined_tables():
    assert WAP_TABLES == (
        "refined.packets",
        "refined.sessions",
        "refined.client_info",
        "refined.plugin_messages",
    )


def test_no_statement_writes_to_capture():
    for statement in _all_statements(_plan()):
        if statement.startswith(WRITE_VERBS):
            assert "capture." not in statement, statement


def test_statements_that_read_capture_are_views_or_selects():
    for statement in _all_statements(_plan()):
        if _capture_refs(statement):
            assert statement.startswith(("CREATE OR REPLACE TEMPORARY VIEW", "SELECT", "WITH")), (
                statement
            )


def test_capture_sources_are_only_the_five_corvus_tables():
    refs = set()
    for statement in _all_statements(_plan()):
        refs |= _capture_refs(statement)
    assert refs == CAPTURE_SOURCES


def test_packet_table_has_no_payload_column():
    assert "payload" not in PACKET_COLUMNS
    assert "payload_hash" in PACKET_COLUMNS


def test_write_statements_never_select_the_payload_itself():
    for statement in _plan().write:
        if statement.startswith("INSERT"):
            assert not re.search(r"\bpayload\b", statement), statement


def test_dedup_window_partitions_by_session_and_seq_only():
    dedup = _view_text(_plan(), "layers_dedup")

    assert "PARTITION BY session_id, seq" in dedup
    assert "direction" not in dedup.split("PARTITION BY")[1].split(")")[0]


def test_dedup_orders_copies_by_content_hash():
    dedup = _view_text(_plan(), "layers_dedup")

    assert "row_number() OVER (PARTITION BY session_id, seq ORDER BY content_hash)" in dedup
    assert "xxhash64(" in _view_text(_plan(), "layers_raw")


def test_conflicting_copies_are_detected_by_min_and_max_hash():
    dedup = _view_text(_plan(), "layers_dedup")

    assert "min(content_hash) OVER (PARTITION BY session_id, seq) AS min_hash" in dedup
    assert "max(content_hash) OVER (PARTITION BY session_id, seq) AS max_hash" in dedup


def test_raw_read_is_limited_to_the_day_partition():
    raw = _view_text(_plan(), "layers_raw")

    assert f"WHERE date = DATE '{DAY.isoformat()}'" in raw


def test_packet_name_join_uses_the_version_key():
    named = _view_text(_plan(), "layers_named")

    assert "ref.packet_names" in named
    for column in ("protocol_version", "state", "direction"):
        assert f"n.{column} = " in named
    assert "wire_packet_id" in named


def test_time_delta_is_taken_within_one_direction_from_offset_nanos():
    distinct = _view_text(_plan(), "layers_distinct")

    assert "PARTITION BY session_id, direction ORDER BY seq" in distinct
    assert "offset_nanos" in distinct


def test_session_dedup_is_one_row_per_session_for_capture_sessions():
    sessions = _view_text(_plan(), "layers_sessions")

    assert "row_number() OVER (PARTITION BY session_id" in sessions


def test_segment_dedup_is_one_row_per_session_and_segment():
    segments = _view_text(_plan(), "layers_segments")

    assert "PARTITION BY session_id, segment" in segments


def test_session_ends_are_summed_after_removing_exact_redeliveries():
    ends = _view_text(_plan(), "layers_session_ends")

    assert "SELECT DISTINCT" in ends
    assert "dropped_queue_full" in ends and "dropped_transport" in ends


def test_session_verdict_uses_configured_thresholds_and_verdict_words():
    verdicts = _view_text(_plan(), "layers_session_verdicts")

    assert "0.01" in verdicts
    assert "0.001" in verdicts
    for word in ("dup.conflict", "seq.gaps", "time.monotonic"):
        assert word in verdicts
    for word in ("quarantined", "flagged", "accepted", "no_config_phase", "no_disconnect"):
        assert word in verdicts


def test_missing_disconnect_is_flagged_and_not_available_drops_are_marked():
    verdicts = _view_text(_plan(), "layers_session_verdicts")

    assert "not_available" in verdicts


def test_quarantined_sessions_are_left_out_of_packets():
    write = next(s for s in _plan().write if "refined.packets" in s)

    assert "<> 'quarantined'" in write


def test_client_info_uses_the_decoder_functions_of_the_job():
    client = _view_text(_plan(), "layers_client_facts")

    assert "layers_decode_brand(" in client
    assert "layers_decode_channels(" in client


def test_plugin_messages_come_from_custom_payloads_only():
    custom = _view_text(_plan(), "layers_custom")

    assert "packet_name = 'minecraft:custom_payload'" in custom


def test_reconcile_compares_raw_keys_with_the_branch_and_quarantine():
    reconcile = _plan().reconcile

    assert "DISTINCT session_id, seq" in reconcile
    assert f"{CATALOG}.refined.packets.branch_{BRANCH}" in reconcile
    assert "quarantined" in reconcile


# Write-Audit-Publish sequence (design decision 3)


def test_branch_is_created_before_the_writes_and_dropped_last():
    plan = _plan()

    setup = " ".join(plan.setup)
    assert f"ALTER TABLE {CATALOG}.refined.packets CREATE BRANCH IF NOT EXISTS {BRANCH}" in setup
    assert f"spark.wap.branch = '{BRANCH}'" in " ".join(plan.write)
    assert plan.write[-1].startswith("RESET spark.wap.branch")
    assert len(plan.drop_branch) == len(WAP_TABLES)


def test_stale_branch_from_an_earlier_run_is_dropped_before_create():
    setup = _plan().setup
    drop_index = next(i for i, s in enumerate(setup) if "DROP BRANCH IF EXISTS" in s)
    create_index = next(i for i, s in enumerate(setup) if "CREATE BRANCH" in s)

    assert drop_index < create_index


def test_publish_fast_forwards_main_to_the_audit_branch_per_table():
    publish = _plan().publish

    assert len(publish) == len(WAP_TABLES)
    for table, statement in zip(WAP_TABLES, publish, strict=True):
        assert statement == (
            f"CALL {CATALOG}.system.fast_forward(table => '{table}', "
            f"branch => 'main', to => '{BRANCH}')"
        )


def test_drop_branch_runs_for_every_wap_table():
    for table, statement in zip(WAP_TABLES, _plan().drop_branch, strict=True):
        assert statement == f"ALTER TABLE {CATALOG}.{table} DROP BRANCH IF EXISTS {BRANCH}"


def test_quarantine_is_recorded_after_publish_only():
    plan = _plan()

    assert plan.record_quarantine
    assert plan.record_quarantine[0].startswith(
        "INSERT OVERWRITE corvus.quality.quarantine_sessions"
    )


def test_dynamic_partition_overwrite_is_set_before_the_writes():
    setup = _plan().setup

    assert "SET spark.sql.sources.partitionOverwriteMode = 'dynamic'" in setup


def test_plan_is_deterministic_for_a_given_day():
    assert _plan() == _plan()


def test_plan_differs_between_days_only_by_the_day_literal():
    first = "\n".join(_all_statements(_plan(DAY)))
    second = "\n".join(_all_statements(_plan(DAY + timedelta(days=1))))

    assert DAY.isoformat() in first
    assert (DAY + timedelta(days=1)).isoformat() in second
    assert DAY.isoformat() not in second


# Identifiers and inputs


def test_catalog_must_be_a_plain_identifier():
    with pytest.raises(ValueError, match="catalog"):
        build_refined_plan("corvus; DROP TABLE x", DAY, RUN, THRESHOLDS)


def test_run_id_must_be_a_plain_identifier():
    with pytest.raises(ValueError, match="run_id"):
        build_refined_plan(CATALOG, DAY, "manual__2026-11-18", THRESHOLDS)


def test_day_must_be_a_date_not_a_datetime():
    with pytest.raises(TypeError, match="date"):
        build_refined_plan(CATALOG, datetime(2026, 11, 18, 12, 0), RUN, THRESHOLDS)


def test_session_thresholds_come_from_the_layers_config():
    config = load_layers_config()

    assert session_thresholds(config) == SessionThresholds(gap_ratio=0.01, monotonic_ratio=0.001)


# Stale branch cleanup


def test_stale_query_lists_audit_branches_older_than_one_day():
    query = stale_branches_query(CATALOG, "refined.packets", NOW)

    assert "left(r.name, 6) = 'audit_'" in query
    assert "r.type = 'BRANCH'" in query
    assert "TIMESTAMP '2026-11-18 04:00:00'" in query
    assert f"{CATALOG}.refined.packets.refs" in query


def test_stale_query_requires_an_aware_datetime():
    with pytest.raises(ValueError, match="timezone-aware"):
        stale_branches_query(CATALOG, "refined.packets", datetime(2026, 11, 19, 4, 0))


def test_stale_query_rejects_tables_outside_the_wap_set():
    with pytest.raises(ValueError, match="refined"):
        stale_branches_query(CATALOG, "capture.raw", NOW)


def test_drop_branch_statement_is_idempotent():
    assert drop_branch(CATALOG, "refined.sessions", BRANCH) == (
        f"ALTER TABLE {CATALOG}.refined.sessions DROP BRANCH IF EXISTS {BRANCH}"
    )


# Reconcile and check results


@pytest.mark.parametrize(
    ("raw_keys", "packet_rows", "quarantined_rows", "outcome"),
    [
        (100, 90, 10, Outcome.PASSED),
        (100, 90, 0, Outcome.FAILED),
        (100, 100, 10, Outcome.FAILED),
    ],
)
def test_reconcile_passes_only_when_raw_keys_equal_packets_plus_quarantine(
    raw_keys, packet_rows, quarantined_rows, outcome
):
    assert reconcile_outcome(raw_keys, packet_rows, quarantined_rows) is outcome


def test_check_results_insert_writes_verdicts_without_keys():
    statement = check_results_insert(
        CATALOG,
        DAY,
        RUN,
        [CheckResult("rows.reconcile", Verdict.PASS, 0.0)],
    )

    assert statement.startswith(f"INSERT INTO {CATALOG}.quality.check_results")
    assert "session_id" not in statement
    assert "player_key" not in statement
    assert "'rows.reconcile'" in statement and "'pass'" in statement


def test_check_results_insert_rejects_unknown_check_id():
    with pytest.raises(ValueError, match="unknown check"):
        check_results_insert(CATALOG, DAY, RUN, [CheckResult("made.up", Verdict.PASS, None)])


def test_check_results_insert_rejects_session_scope_checks():
    with pytest.raises(ValueError, match="session"):
        check_results_insert(CATALOG, DAY, RUN, [CheckResult("dup.conflict", Verdict.WARN, None)])


def test_check_results_insert_rejects_empty_list():
    with pytest.raises(ValueError, match="at least one"):
        check_results_insert(CATALOG, DAY, RUN, [])
