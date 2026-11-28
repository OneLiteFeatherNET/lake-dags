from datetime import UTC, datetime, timedelta, timezone

import pytest

from lake.config import TableConfig
from lake.sql import build_statements, cutoff_day

NOW = datetime(2026, 11, 18, 3, 0, tzinfo=UTC)


def test_cutoff_day_subtracts_retention_from_utc_date():
    assert cutoff_day(NOW, 21).isoformat() == "2026-10-28"


def test_cutoff_day_uses_utc_date_for_non_utc_input():
    berlin = timezone(timedelta(hours=1))
    late_in_berlin = datetime(2026, 11, 18, 23, 30, tzinfo=berlin)  # 22:30 UTC, still Nov 18

    assert cutoff_day(late_in_berlin, 1).isoformat() == "2026-11-17"


def test_cutoff_day_rejects_naive_datetime():
    with pytest.raises(ValueError, match="timezone-aware"):
        cutoff_day(datetime(2026, 11, 18, 3, 0), 21)


def test_build_statements_for_capture_raw_in_order_with_exact_text():
    table = TableConfig(name="capture.raw", retention_days=21, day_column="date")

    statements = build_statements("corvus", table, NOW, snapshot_max_age_days=7)

    assert statements == [
        "DELETE FROM corvus.capture.raw WHERE date < DATE '2026-10-28'",
        "CALL corvus.system.rewrite_data_files('capture.raw')",
        "CALL corvus.system.expire_snapshots(table => 'capture.raw', "
        "older_than => TIMESTAMP '2026-11-11 03:00:00', retain_last => 1, stream_results => true)",
        "CALL corvus.system.remove_orphan_files(table => 'capture.raw', "
        "older_than => TIMESTAMP '2026-11-15 03:00:00', prefix_listing => true)",
    ]


def test_table_override_replaces_global_snapshot_age():
    table = TableConfig(name="capture.raw", retention_days=None, snapshot_max_age_days=1)
    now = datetime(2026, 12, 1, 3, 0, tzinfo=UTC)

    statements = build_statements("corvus", table, now, snapshot_max_age_days=7)

    assert statements[1] == (
        "CALL corvus.system.expire_snapshots(table => 'capture.raw', "
        "older_than => TIMESTAMP '2026-11-30 03:00:00', retain_last => 1, stream_results => true)"
    )


def test_table_without_override_uses_global_snapshot_age():
    table = TableConfig(name="capture.sessions", retention_days=None)

    statements = build_statements("corvus", table, NOW, snapshot_max_age_days=7)

    assert "TIMESTAMP '2026-11-11 03:00:00'" in statements[1]


def test_expire_snapshots_streams_its_results():
    table = TableConfig(name="capture.events", retention_days=None)

    statements = build_statements("corvus", table, NOW, snapshot_max_age_days=7)

    assert "stream_results => true" in statements[1]


def test_delete_step_is_absent_when_retention_is_none():
    table = TableConfig(name="capture.events", retention_days=None)

    statements = build_statements("corvus", table, NOW, snapshot_max_age_days=7)

    assert len(statements) == 3
    assert all(not statement.startswith("DELETE") for statement in statements)
    assert statements[0] == "CALL corvus.system.rewrite_data_files('capture.events')"


def test_orphan_cleanup_lists_files_through_table_file_io():
    # Hadoop FileSystem has no s3 scheme in the image; prefix_listing uses the table's FileIO.
    table = TableConfig(name="capture.events", retention_days=None)

    statements = build_statements("corvus", table, NOW, snapshot_max_age_days=7)

    assert "remove_orphan_files" in statements[-1]
    assert "prefix_listing => true" in statements[-1]


def test_orphan_cutoff_is_three_days_before_now():
    table = TableConfig(name="capture.events", retention_days=None)

    statements = build_statements("corvus", table, NOW, snapshot_max_age_days=7)

    assert "older_than => TIMESTAMP '2026-11-15 03:00:00'" in statements[-1]


def test_snapshot_cutoff_follows_configured_age():
    table = TableConfig(name="capture.events", retention_days=None)

    statements = build_statements("corvus", table, NOW, snapshot_max_age_days=2)

    assert "TIMESTAMP '2026-11-16 03:00:00'" in statements[1]


def test_timestamps_are_formatted_in_utc_for_non_utc_now():
    table = TableConfig(name="capture.events", retention_days=None)
    berlin = timezone(timedelta(hours=1))
    now = datetime(2026, 11, 18, 4, 0, tzinfo=berlin)  # 03:00 UTC

    statements = build_statements("corvus", table, now, snapshot_max_age_days=7)

    assert "TIMESTAMP '2026-11-11 03:00:00'" in statements[1]
