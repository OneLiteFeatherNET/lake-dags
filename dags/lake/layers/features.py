"""Gold builders: session statistics, token windows and the build-run record (design decision 12).

Pure: builds Spark SQL strings. The token rule v1 is written once here as Python functions and
mirrored in SQL from the same bucket bounds, so the two cannot drift apart.

Reads refined.packets, which holds one row per (session_id, seq) with the columns session_id,
seq, offset_nanos, direction, packet_type, protocol_version, player_key and date. Writes only
features.* tables. Partitions are overwritten dynamically (partitionOverwriteMode dynamic).
"""

from __future__ import annotations

from bisect import bisect_right
from datetime import date, datetime

from lake.layers.config import WINDOW_MAX_PACKETS, WINDOW_MIN_PACKETS
from lake.layers.literals import (
    date_literal,
    int_literal,
    string_literal,
    timestamp_literal,
)

TOKENIZER_VERSION = "t1"
FEATURE_VERSION = "v1"
# Upper bounds of the time-delta buckets in nanoseconds; bucket i is [bound[i-1], bound[i]).
DELTA_BOUNDS_NANOS = (1_000_000, 5_000_000, 20_000_000, 50_000_000, 200_000_000, 1_000_000_000)
_DIRECTION_TOKENS = {1: "d:s", 2: "d:c", 0: "d:u"}


def direction_token(direction: int) -> str:
    """Serverbound s, clientbound c, unspecified u (Corvus direction codes 1, 2, 0)."""
    if direction not in _DIRECTION_TOKENS:
        raise ValueError(f"direction must be 0, 1 or 2, got {direction!r}")
    return _DIRECTION_TOKENS[direction]


def packet_token(packet_type: int) -> str:
    """The version-stable packet type. 0 is an unmapped packet and keeps its own token."""
    if isinstance(packet_type, bool) or not isinstance(packet_type, int) or packet_type < 0:
        raise ValueError(f"packet_type must be an integer >= 0, got {packet_type!r}")
    return f"p:{packet_type}"


def time_delta_token(delta_nanos: int | None) -> str:
    """Bucket of the offset difference to the previous packet of the same direction."""
    if delta_nanos is None:
        return "t:none"
    if delta_nanos < 0:
        return "t:neg"
    return f"t:{bisect_right(DELTA_BOUNDS_NANOS, delta_nanos)}"


def protocol_token(protocol_version: int | None) -> str:
    return "v:none" if protocol_version is None else f"v:{protocol_version}"


def window_bounds(packet_count: int, size: int) -> list[tuple[int, int]]:
    """Half-open [start, end) bounds of complete windows; a partial trailing window is dropped."""
    _check_size(size)
    return [(start, start + size) for start in range(0, packet_count - size + 1, size)]


def _check_size(size: int) -> None:
    if isinstance(size, bool) or not isinstance(size, int):
        raise ValueError(f"window size must be an integer, got {size!r}")
    if not WINDOW_MIN_PACKETS <= size <= WINDOW_MAX_PACKETS:
        raise ValueError(
            f"window size must be between {WINDOW_MIN_PACKETS} and {WINDOW_MAX_PACKETS}, got {size}"
        )


def _time_delta_sql() -> str:
    branches = ["WHEN delta_nanos IS NULL THEN 't:none'", "WHEN delta_nanos < 0 THEN 't:neg'"]
    for index, bound in enumerate(DELTA_BOUNDS_NANOS):
        branches.append(f"WHEN delta_nanos < {bound} THEN 't:{index}'")
    return "CASE " + " ".join(branches) + f" ELSE 't:{len(DELTA_BOUNDS_NANOS)}' END"


def session_stats_statement(day: date) -> str:
    """features.session_stats for one day: counts, rate and delta percentiles per direction."""
    return (
        "INSERT OVERWRITE features.session_stats "
        "WITH deltas AS ("
        "SELECT session_id, date, player_key, direction, offset_nanos, "
        "offset_nanos - lag(offset_nanos) OVER "
        "(PARTITION BY session_id, direction ORDER BY seq) AS delta_nanos "
        f"FROM refined.packets WHERE date = {date_literal(day)}"
        ") "
        "SELECT session_id, date, any_value(player_key) AS player_key, direction, "
        "count(*) AS packet_count, "
        "max(offset_nanos) - min(offset_nanos) AS duration_nanos, "
        "CASE WHEN max(offset_nanos) > min(offset_nanos) THEN "
        "count(*) * 1000000000.0 / (max(offset_nanos) - min(offset_nanos)) END "
        "AS packets_per_second, "
        "percentile_approx(delta_nanos, 0.5) AS delta_p50_nanos, "
        "percentile_approx(delta_nanos, 0.9) AS delta_p90_nanos, "
        "percentile_approx(delta_nanos, 0.99) AS delta_p99_nanos "
        "FROM deltas GROUP BY session_id, date, direction"
    )


def token_window_statement(day: date, size: int, split_sql: str, source_snapshot_id: int) -> str:
    """features.windows for one day: fixed windows of complete size, one token per packet.

    A token joins the four v1 token types with '|': direction, packet type, time-delta bucket and
    protocol version. split_sql is a SQL expression over player_key that gives 'train' or 'test'.
    """
    _check_size(size)
    if ";" in split_sql or "--" in split_sql:
        raise ValueError("split expression must be a SQL expression over player_key")
    snapshot = int_literal(source_snapshot_id)
    return (
        "INSERT OVERWRITE features.windows "
        "WITH ranked AS ("
        "SELECT session_id, date, player_key, seq, offset_nanos, direction, packet_type, "
        "protocol_version, "
        "row_number() OVER (PARTITION BY session_id ORDER BY seq) - 1 AS position, "
        "offset_nanos - lag(offset_nanos) OVER "
        "(PARTITION BY session_id, direction ORDER BY seq) AS delta_nanos "
        f"FROM refined.packets WHERE date = {date_literal(day)}"
        "), tokens AS ("
        "SELECT session_id, date, player_key, seq, offset_nanos, position, "
        "CAST(floor(position / "
        f"{size}) AS INT) AS window_index, "
        "concat_ws('|', "
        "CASE direction WHEN 1 THEN 'd:s' WHEN 2 THEN 'd:c' ELSE 'd:u' END, "
        "concat('p:', CAST(packet_type AS STRING)), "
        f"{_time_delta_sql()}, "
        "CASE WHEN protocol_version IS NULL THEN 'v:none' "
        "ELSE concat('v:', CAST(protocol_version AS STRING)) END) AS token "
        "FROM ranked"
        ") "
        "SELECT session_id, window_index, "
        "any_value(date) AS date, any_value(player_key) AS player_key, "
        f"{split_sql} AS split, "
        "min(seq) AS first_seq, max(seq) AS last_seq, "
        "min(offset_nanos) AS start_offset_nanos, max(offset_nanos) AS end_offset_nanos, "
        "transform(sort_array(collect_list(named_struct('pos', position, 'tok', token))), "
        "s -> s.tok) AS tokens, "
        f"{string_literal(TOKENIZER_VERSION)} AS tokenizer_version, "
        f"{string_literal(FEATURE_VERSION)} AS feature_version, "
        f"{snapshot} AS source_snapshot_id "
        "FROM tokens GROUP BY session_id, window_index "
        f"HAVING count(*) = {size}"
    )


def build_run_statement(
    *,
    feature_version: str,
    tokenizer_version: str,
    day: date,
    refined_snapshot_id: int,
    labels_snapshot_id: int | None,
    code_version: str,
    built_at: datetime,
) -> str:
    """One features.build_runs row per (feature_version, tokenizer_version, date), as a MERGE."""
    labels = "NULL" if labels_snapshot_id is None else int_literal(labels_snapshot_id)
    values = (
        f"{string_literal(feature_version)} AS feature_version, "
        f"{string_literal(tokenizer_version)} AS tokenizer_version, "
        f"{date_literal(day)} AS date, "
        f"{int_literal(refined_snapshot_id)} AS refined_snapshot_id, "
        f"{labels} AS labels_snapshot_id, "
        f"{string_literal(code_version)} AS code_version, "
        f"{timestamp_literal(built_at)} AS built_at"
    )
    return (
        "MERGE INTO features.build_runs AS target "
        f"USING (SELECT {values}) AS source "
        "ON target.feature_version = source.feature_version "
        "AND target.tokenizer_version = source.tokenizer_version "
        "AND target.date = source.date "
        "WHEN MATCHED THEN UPDATE SET "
        "target.refined_snapshot_id = source.refined_snapshot_id, "
        "target.labels_snapshot_id = source.labels_snapshot_id, "
        "target.code_version = source.code_version, "
        "target.built_at = source.built_at "
        "WHEN NOT MATCHED THEN INSERT *"
    )
