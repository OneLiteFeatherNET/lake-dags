from datetime import UTC, date, datetime

import pytest

from lake.layers.features import (
    FEATURE_VERSION,
    TOKENIZER_VERSION,
    build_run_statement,
    direction_token,
    packet_token,
    protocol_token,
    session_stats_statement,
    time_delta_token,
    token_window_statement,
    window_bounds,
)

DAY = date(2026, 11, 20)
SNAPSHOT = 4815162342
SPLIT = "CASE WHEN pmod(hash(player_key), 10) < 8 THEN 'train' ELSE 'test' END"


# --- token rule v1, one fixture per token type --------------------------------------


@pytest.mark.parametrize(
    ("direction", "token"),
    [(1, "d:s"), (2, "d:c"), (0, "d:u")],
)
def test_direction_token_maps_serverbound_clientbound_and_unspecified(direction, token):
    assert direction_token(direction) == token


def test_direction_token_rejects_unknown_direction():
    with pytest.raises(ValueError, match="direction"):
        direction_token(7)


@pytest.mark.parametrize(("packet_type", "token"), [(0, "p:0"), (1, "p:1"), (4096, "p:4096")])
def test_packet_token_uses_the_version_stable_packet_type(packet_type, token):
    assert packet_token(packet_type) == token


def test_packet_token_rejects_negative_packet_type():
    with pytest.raises(ValueError, match="packet_type"):
        packet_token(-1)


@pytest.mark.parametrize(
    ("delta_nanos", "token"),
    [
        (None, "t:none"),
        (0, "t:0"),
        (999_999, "t:0"),  # just under 1 ms
        (1_000_000, "t:1"),  # 1 ms falls into the next bucket
        (4_999_999, "t:1"),
        (5_000_000, "t:2"),
        (19_999_999, "t:2"),
        (20_000_000, "t:3"),
        (49_999_999, "t:3"),
        (50_000_000, "t:4"),
        (199_999_999, "t:4"),
        (200_000_000, "t:5"),
        (999_999_999, "t:5"),
        (1_000_000_000, "t:6"),
        (60_000_000_000, "t:6"),
    ],
)
def test_time_delta_token_buckets_each_boundary(delta_nanos, token):
    assert time_delta_token(delta_nanos) == token


def test_time_delta_token_marks_negative_delta():
    assert time_delta_token(-5) == "t:neg"


@pytest.mark.parametrize(("version", "token"), [(767, "v:767"), (None, "v:none")])
def test_protocol_token_uses_version_or_none(version, token):
    assert protocol_token(version) == token


# --- window bounds ----------------------------------------------------------------------


def test_window_bounds_keep_only_complete_windows():
    assert window_bounds(600, 256) == [(0, 256), (256, 512)]


def test_window_bounds_are_empty_for_a_short_session():
    assert window_bounds(255, 256) == []


def test_window_bounds_accept_the_size_limits():
    assert window_bounds(512, 512) == [(0, 512)]


@pytest.mark.parametrize("size", [255, 513])
def test_window_bounds_reject_size_outside_the_configured_range(size):
    with pytest.raises(ValueError, match="256"):
        window_bounds(1000, size)


# --- SQL builders -----------------------------------------------------------------------


def test_versions_are_fixed_strings_for_v1():
    assert TOKENIZER_VERSION == "t1"
    assert FEATURE_VERSION == "v1"


def test_session_stats_overwrites_only_the_day_partition():
    statement = session_stats_statement(DAY)

    assert statement.startswith("INSERT OVERWRITE features.session_stats")
    assert "FROM refined.packets" in statement
    assert "WHERE date = DATE '2026-11-20'" in statement


def test_session_stats_orders_deltas_within_one_direction():
    statement = session_stats_statement(DAY)

    assert "PARTITION BY session_id, direction ORDER BY seq" in statement
    assert "percentile_approx" in statement


def test_session_stats_reads_no_capture_table():
    assert "capture." not in session_stats_statement(DAY)


def test_token_window_statement_writes_features_windows_only():
    statement = token_window_statement(DAY, 256, SPLIT, SNAPSHOT)

    assert statement.startswith("INSERT OVERWRITE features.windows")
    assert "capture." not in statement
    assert "FROM refined.packets" in statement


def test_token_window_statement_carries_versions_and_snapshot():
    statement = token_window_statement(DAY, 256, SPLIT, SNAPSHOT)

    assert f"'{TOKENIZER_VERSION}' AS tokenizer_version" in statement
    assert f"'{FEATURE_VERSION}' AS feature_version" in statement
    assert f"{SNAPSHOT} AS source_snapshot_id" in statement


def test_token_window_statement_keeps_windows_of_exact_size():
    statement = token_window_statement(DAY, 256, SPLIT, SNAPSHOT)

    assert "HAVING count(*) = 256" in statement


def test_token_array_is_ordered_by_position_not_by_collection_order():
    statement = token_window_statement(DAY, 256, SPLIT, SNAPSHOT)

    assert "sort_array(collect_list" in statement


def test_token_statement_uses_the_same_bucket_thresholds_as_python():
    statement = token_window_statement(DAY, 256, SPLIT, SNAPSHOT)

    for threshold in ("1000000", "5000000", "20000000", "50000000", "200000000", "1000000000"):
        assert threshold in statement


def test_token_window_statement_places_split_expression_on_the_window():
    statement = token_window_statement(DAY, 256, SPLIT, SNAPSHOT)

    assert SPLIT in statement


@pytest.mark.parametrize("size", [255, 513])
def test_token_window_statement_rejects_size_outside_range(size):
    with pytest.raises(ValueError, match="256"):
        token_window_statement(DAY, size, SPLIT, SNAPSHOT)


def test_token_window_statement_rejects_split_with_a_statement_separator():
    with pytest.raises(ValueError, match="split"):
        token_window_statement(DAY, 256, "1; DROP TABLE refined.packets", SNAPSHOT)


def test_build_run_statement_upserts_one_row_per_key():
    statement = build_run_statement(
        feature_version=FEATURE_VERSION,
        tokenizer_version=TOKENIZER_VERSION,
        day=DAY,
        refined_snapshot_id=SNAPSHOT,
        labels_snapshot_id=77,
        code_version="0.1.0",
        built_at=datetime(2026, 11, 21, 3, 0, tzinfo=UTC),
    )

    assert statement.startswith("MERGE INTO features.build_runs AS target")
    assert "target.feature_version = source.feature_version" in statement
    assert "target.tokenizer_version = source.tokenizer_version" in statement
    assert "target.date = source.date" in statement
    assert "WHEN NOT MATCHED THEN INSERT" in statement
    assert "DELETE" not in statement.upper()


def test_build_run_statement_rejects_unsafe_code_version():
    with pytest.raises(ValueError, match="safe SQL text literal"):
        build_run_statement(
            feature_version=FEATURE_VERSION,
            tokenizer_version=TOKENIZER_VERSION,
            day=DAY,
            refined_snapshot_id=SNAPSHOT,
            labels_snapshot_id=None,
            code_version="0.1'; --",
            built_at=datetime(2026, 11, 21, 3, 0, tzinfo=UTC),
        )
