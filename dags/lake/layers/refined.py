"""Silver layer statements: dedup, packet names, sessions and the Write-Audit-Publish sequence.

Design decisions 3 to 6 of add-lake-layers. Pure: the functions return Spark SQL strings and
small values; nothing is executed here. The Spark job runs the statements in the order of
RefinedPlan and reads the check values it needs from the audit branch.

Every statement that reads capture.* is a temporary view or a select. No statement writes to
the capture namespace. Writes go to the refined namespace (on the audit branch) and to the
quality namespace (on main, after the decision).
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta

from lake.config import _identifier
from lake.layers.checks import CATALOGUE, Outcome, Scope, Verdict
from lake.layers.config import LayersConfig

WAP_TABLES = (
    "refined.packets",
    "refined.sessions",
    "refined.client_info",
    "refined.plugin_messages",
)
BRANCH_PREFIX = "audit_"
STALE_AFTER = timedelta(days=1)
CUSTOM_PAYLOAD_PACKET = "minecraft:custom_payload"
_RUN_ID = re.compile(r"^[A-Za-z0-9_]{1,64}$")
_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"

PACKET_COLUMNS = (
    "session_id",
    "seq",
    "date",
    "segment",
    "direction",
    "state",
    "packet_type",
    "packet_name",
    "wire_packet_id",
    "protocol_version",
    "player_key",
    "backend_server",
    "time_delta_ms",
    "payload_size",
    "payload_hash",
    "has_event",
)
SESSION_COLUMNS = (
    "session_id",
    "date",
    "player_key",
    "started_at",
    "client_protocol_version",
    "observed_protocol_version",
    "proxy_id",
    "plugin_version",
    "packet_count",
    "segment_count",
    "backend_servers",
    "seq_gap_ratio",
    "monotonic_ratio",
    "drops_reported",
    "dropped_total",
    "has_disconnect",
    "verdict",
    "flags",
    "quarantine_check",
)
CLIENT_INFO_COLUMNS = (
    "session_id",
    "date",
    "brand",
    "channels",
    "channel_count",
    "client_protocol_version",
)
PLUGIN_MESSAGE_COLUMNS = (
    "session_id",
    "seq",
    "date",
    "direction",
    "channel",
    "payload_size",
    "payload_hash",
)


@dataclass(frozen=True)
class SessionThresholds:
    gap_ratio: float
    monotonic_ratio: float


@dataclass(frozen=True)
class CheckResult:
    check_id: str
    verdict: Verdict
    metric_value: float | None


@dataclass(frozen=True)
class RefinedPlan:
    """Ordered statements of one day. Run setup, write, reconcile, then publish or drop."""

    branch: str
    setup: tuple[str, ...]
    write: tuple[str, ...]
    reconcile: str
    publish: tuple[str, ...]
    drop_branch: tuple[str, ...]
    record_quarantine: tuple[str, ...]


def session_thresholds(config: LayersConfig) -> SessionThresholds:
    """Gap and monotonic ratios from layers.yaml; both checks need a threshold there."""
    values: dict[str, float] = {}
    for check in config.checks:
        if check.id in {"seq.gaps", "time.monotonic"}:
            if check.threshold is None:
                raise ValueError(f"config.checks: {check.id} needs a threshold")
            values[check.id] = check.threshold
    return SessionThresholds(gap_ratio=values["seq.gaps"], monotonic_ratio=values["time.monotonic"])


def build_refined_plan(
    catalog: str, day: date, run_id: str, thresholds: SessionThresholds
) -> RefinedPlan:
    """Statements for one day, one run, on the audit branch named after the run."""
    catalog = _identifier(catalog, "catalog")
    day_literal = _day(day)
    _run_id(run_id)
    branch = f"{BRANCH_PREFIX}{run_id}"
    setup = [
        "SET spark.sql.session.timeZone = 'UTC'",
        "SET spark.sql.sources.partitionOverwriteMode = 'dynamic'",
    ]
    for table in WAP_TABLES:
        setup.append(f"ALTER TABLE {catalog}.{table} DROP BRANCH IF EXISTS {branch}")
    for table in WAP_TABLES:
        setup.append(f"ALTER TABLE {catalog}.{table} CREATE BRANCH IF NOT EXISTS {branch}")
    setup.extend(_views(catalog, day_literal, thresholds))

    write = [f"SET spark.wap.branch = '{branch}'"]
    write.append(_insert(catalog, "refined.packets", PACKET_COLUMNS, _packets_select(day_literal)))
    write.append(
        _insert(catalog, "refined.sessions", SESSION_COLUMNS, _sessions_select(day_literal))
    )
    write.append(
        _insert(catalog, "refined.client_info", CLIENT_INFO_COLUMNS, _client_select(day_literal))
    )
    write.append(
        _insert(
            catalog,
            "refined.plugin_messages",
            PLUGIN_MESSAGE_COLUMNS,
            _plugin_select(day_literal),
        )
    )
    write.append("RESET spark.wap.branch")

    publish = tuple(
        f"CALL {catalog}.system.fast_forward("
        f"table => '{table}', branch => 'main', to => '{branch}')"
        for table in WAP_TABLES
    )
    drops = tuple(drop_branch(catalog, table, branch) for table in WAP_TABLES)
    quarantine = (
        _insert(
            catalog,
            "quality.quarantine_sessions",
            ("session_id", "player_key", "date", "check_id", "row_count"),
            "SELECT v.session_id, v.player_key, "
            f"{day_literal} AS date, v.quarantine_check AS check_id, v.packet_count AS row_count "
            "FROM layers_session_verdicts v WHERE v.verdict = 'quarantined'",
        ),
    )
    return RefinedPlan(
        branch=branch,
        setup=tuple(setup),
        write=tuple(write),
        reconcile=_reconcile(catalog, day_literal, branch),
        publish=publish,
        drop_branch=drops,
        record_quarantine=quarantine,
    )


def drop_branch(catalog: str, table: str, branch: str) -> str:
    """Drop an audit branch. Idempotent, so it can run in a finally step."""
    catalog = _identifier(catalog, "catalog")
    _wap_table(table)
    _branch(branch)
    return f"ALTER TABLE {catalog}.{table} DROP BRANCH IF EXISTS {branch}"


def stale_branches_query(catalog: str, table: str, now: datetime) -> str:
    """Audit branches of one table whose head is older than one day. Run before a new build."""
    catalog = _identifier(catalog, "catalog")
    _wap_table(table)
    cutoff = _utc(now) - STALE_AFTER
    return (
        "SELECT r.name AS branch "
        f"FROM {catalog}.{table}.refs r "
        f"JOIN {catalog}.{table}.snapshots s ON s.snapshot_id = r.snapshot_id "
        "WHERE r.type = 'BRANCH' AND left(r.name, 6) = 'audit_' "
        f"AND s.committed_at < TIMESTAMP '{cutoff.strftime(_TIMESTAMP_FORMAT)}'"
    )


def reconcile_outcome(raw_keys: int, packet_rows: int, quarantined_rows: int) -> Outcome:
    """rows.reconcile: each distinct raw key is a refined row or in a quarantined session."""
    if raw_keys == packet_rows + quarantined_rows:
        return Outcome.PASSED
    return Outcome.FAILED


def check_results_insert(
    catalog: str, day: date, run_id: str, results: Sequence[CheckResult]
) -> str:
    """Append table-scope check verdicts to quality.check_results. No session id, no player key."""
    catalog = _identifier(catalog, "catalog")
    day_literal = _day(day)
    _run_id(run_id)
    if not results:
        raise ValueError("check_results_insert needs at least one result")
    rows = []
    for result in results:
        spec = next((item for item in CATALOGUE if item.id == result.check_id), None)
        if spec is None:
            raise ValueError(f"unknown check '{result.check_id}'")
        if spec.scope is Scope.SESSION:
            raise ValueError(
                f"check '{result.check_id}' is session scope: it is recorded in refined.sessions"
            )
        if not isinstance(result.verdict, Verdict):
            raise ValueError(f"verdict must be a Verdict, got {result.verdict!r}")
        metric = "NULL" if result.metric_value is None else repr(float(result.metric_value))
        rows.append(
            f"('{run_id}', {day_literal}, '{result.check_id}', 'table', "
            f"'{result.verdict.value}', {metric})"
        )
    return (
        f"INSERT INTO {catalog}.quality.check_results "
        "(run_id, date, check_id, scope, verdict, metric_value) VALUES " + ", ".join(rows)
    )


# Helpers


def _day(day: date) -> str:
    if type(day) is not date:
        raise TypeError(f"day must be a datetime.date, got {type(day).__name__}")
    return f"DATE '{day.isoformat()}'"


def _run_id(run_id: str) -> None:
    if not isinstance(run_id, str) or not _RUN_ID.match(run_id):
        raise ValueError(f"run_id must be 1 to 64 letters, digits or underscores, got {run_id!r}")


def _branch(branch: str) -> None:
    if not branch.startswith(BRANCH_PREFIX) or not _RUN_ID.match(branch[len(BRANCH_PREFIX) :]):
        raise ValueError(f"branch must be an audit branch name, got {branch!r}")


def _wap_table(table: str) -> None:
    if table not in WAP_TABLES:
        raise ValueError(f"table must be one of {', '.join(WAP_TABLES)}, got {table!r}")


def _utc(now: datetime) -> datetime:
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("now must be a timezone-aware datetime")
    return now.astimezone(UTC)


def _insert(catalog: str, table: str, columns: Sequence[str], select: str) -> str:
    return f"INSERT OVERWRITE {catalog}.{table} ({', '.join(columns)}) {select}"


def _reconcile(catalog: str, day: str, branch: str) -> str:
    return (
        "SELECT "
        "(SELECT count(*) FROM (SELECT DISTINCT session_id, seq FROM layers_raw)) AS raw_keys, "
        f"(SELECT count(*) FROM {catalog}.refined.packets.branch_{branch} "
        f"WHERE date = {day}) AS packet_rows, "
        "(SELECT coalesce(sum(packet_count), 0) FROM layers_session_verdicts "
        "WHERE verdict = 'quarantined') AS quarantined_rows"
    )


def _view(name: str, select: str) -> str:
    return f"CREATE OR REPLACE TEMPORARY VIEW {name} AS\n{select}"


def _views(catalog: str, day: str, thresholds: SessionThresholds) -> list[str]:
    gap = repr(float(thresholds.gap_ratio))
    mono = repr(float(thresholds.monotonic_ratio))
    return [
        _view(
            "layers_raw",
            "SELECT session_id, seq, segment, offset_nanos, direction, state, packet_type, "
            "wire_packet_id, protocol_version, player_key, payload_original_size, payload, "
            "xxhash64(payload) AS payload_hash, "
            "xxhash64(segment, offset_nanos, direction, state, packet_type, wire_packet_id, "
            "protocol_version, player_key, payload_original_size, payload) AS content_hash "
            f"FROM {catalog}.capture.raw WHERE date = {day}",
        ),
        _view(
            "layers_dedup",
            "SELECT *, "
            "row_number() OVER (PARTITION BY session_id, seq ORDER BY content_hash) AS copy_rank, "
            "min(content_hash) OVER (PARTITION BY session_id, seq) AS min_hash, "
            "max(content_hash) OVER (PARTITION BY session_id, seq) AS max_hash "
            "FROM layers_raw",
        ),
        _view(
            "layers_distinct",
            "SELECT session_id, seq, segment, offset_nanos, direction, state, packet_type, "
            "wire_packet_id, protocol_version, player_key, payload_original_size, payload_hash, "
            "payload, min_hash, max_hash, "
            "(offset_nanos - lag(offset_nanos) OVER (PARTITION BY session_id, direction "
            "ORDER BY seq)) / 1000000.0 AS time_delta_ms, "
            "lag(offset_nanos) OVER (PARTITION BY session_id ORDER BY seq) AS prev_offset_nanos "
            "FROM layers_dedup WHERE copy_rank = 1",
        ),
        _view(
            "layers_segments",
            "SELECT session_id, segment, backend_server, client_brand FROM ("
            "SELECT session_id, segment, backend_server, client_brand, "
            "row_number() OVER (PARTITION BY session_id, segment ORDER BY started_at, "
            "backend_server) AS copy_rank "
            f"FROM {catalog}.capture.segments "
            "WHERE session_id IN (SELECT session_id FROM layers_distinct)) WHERE copy_rank = 1",
        ),
        _view(
            "layers_segment_facts",
            "SELECT session_id, count(*) AS segment_count, "
            "array_sort(collect_set(backend_server)) AS backend_servers "
            "FROM layers_segments GROUP BY session_id",
        ),
        _view(
            "layers_sessions",
            "SELECT session_id, player_key, client_protocol_version, observed_protocol_version, "
            "proxy_id, plugin_version, started_at FROM ("
            "SELECT session_id, player_key, client_protocol_version, observed_protocol_version, "
            "proxy_id, plugin_version, started_at, "
            "row_number() OVER (PARTITION BY session_id ORDER BY started_at, plugin_version, "
            "proxy_id) AS copy_rank "
            f"FROM {catalog}.capture.sessions "
            "WHERE session_id IN (SELECT session_id FROM layers_distinct)) WHERE copy_rank = 1",
        ),
        _view(
            "layers_session_ends",
            "SELECT DISTINCT session_id, player_key, ended_at, dropped_queue_full, "
            "dropped_privacy, listener_errors, dropped_transport "
            f"FROM {catalog}.capture.session_ends "
            "WHERE session_id IN (SELECT session_id FROM layers_distinct)",
        ),
        _view(
            "layers_end_facts",
            "SELECT session_id, sum(dropped_queue_full + dropped_privacy + dropped_transport) "
            "AS dropped_total FROM layers_session_ends GROUP BY session_id",
        ),
        _view(
            "layers_events",
            f"SELECT DISTINCT session_id, seq FROM {catalog}.capture.events WHERE date = {day}",
        ),
        _view(
            "layers_named",
            "SELECT d.session_id, d.seq, d.segment, d.direction, d.state, d.packet_type, "
            "d.wire_packet_id, d.protocol_version, d.player_key, d.time_delta_ms, "
            "d.payload_original_size, d.payload_hash, d.payload, n.packet_name, "
            "s.backend_server, e.session_id IS NOT NULL AS has_event "
            "FROM layers_distinct d "
            f"LEFT JOIN {catalog}.ref.packet_names n ON n.protocol_version = d.protocol_version "
            "AND n.state = d.state AND n.direction = d.direction "
            "AND n.packet_id = d.wire_packet_id "
            "LEFT JOIN layers_segments s ON s.session_id = d.session_id AND s.segment = d.segment "
            "LEFT JOIN layers_events e ON e.session_id = d.session_id AND e.seq = d.seq",
        ),
        _view(
            "layers_custom",
            "SELECT session_id, seq, direction, payload_original_size, payload_hash, payload, "
            "layers_custom_channel(payload) AS channel "
            f"FROM layers_named WHERE packet_name = '{CUSTOM_PAYLOAD_PACKET}'",
        ),
        _view(
            "layers_client_facts",
            "SELECT session_id, "
            "max(CASE WHEN channel = 'minecraft:brand' THEN layers_decode_brand(payload) END) "
            "AS brand, "
            "max(CASE WHEN channel = 'minecraft:register' "
            "THEN layers_decode_channels(payload) END) AS channels "
            "FROM layers_custom WHERE channel IN ('minecraft:brand', 'minecraft:register') "
            "GROUP BY session_id",
        ),
        _view(
            "layers_session_checks",
            "WITH conflicts AS ("
            "SELECT session_id, bool_or(min_hash <> max_hash) AS conflict "
            "FROM layers_dedup GROUP BY session_id), "
            "kept AS ("
            "SELECT session_id, count(*) AS packet_count, "
            "min(seq) AS min_seq, max(seq) AS max_seq, "
            "sum(CASE WHEN prev_offset_nanos IS NOT NULL AND offset_nanos < prev_offset_nanos "
            "THEN 1 ELSE 0 END) AS decreases "
            "FROM layers_distinct GROUP BY session_id) "
            "SELECT k.session_id, k.packet_count, "
            "(k.max_seq - k.min_seq + 1 - k.packet_count) / (k.max_seq - k.min_seq + 1) "
            "AS seq_gap_ratio, "
            "k.decreases / k.packet_count AS monotonic_ratio, c.conflict "
            "FROM kept k JOIN conflicts c ON c.session_id = k.session_id",
        ),
        _view(
            "layers_session_verdicts",
            "WITH facts AS ("
            "SELECT c.session_id, s.player_key, c.packet_count, c.seq_gap_ratio, "
            "c.monotonic_ratio, c.conflict, g.segment_count, g.backend_servers, "
            "s.client_protocol_version, s.observed_protocol_version, s.proxy_id, "
            "s.plugin_version, s.started_at, e.dropped_total, "
            "e.session_id IS NOT NULL AS has_disconnect, "
            "CASE WHEN e.session_id IS NULL THEN 'not_available' ELSE 'reported' END "
            "AS drops_reported, "
            "cf.brand, cf.channels, "
            "CASE WHEN c.conflict THEN 'dup.conflict' "
            f"WHEN c.seq_gap_ratio > {gap} THEN 'seq.gaps' "
            f"WHEN c.monotonic_ratio > {mono} THEN 'time.monotonic' END AS quarantine_check, "
            "array_compact(array("
            "CASE WHEN cf.session_id IS NULL THEN 'no_config_phase' END, "
            "CASE WHEN e.session_id IS NULL THEN 'no_disconnect' END)) AS flags "
            "FROM layers_session_checks c "
            "LEFT JOIN layers_sessions s ON s.session_id = c.session_id "
            "LEFT JOIN layers_segment_facts g ON g.session_id = c.session_id "
            "LEFT JOIN layers_end_facts e ON e.session_id = c.session_id "
            "LEFT JOIN layers_client_facts cf ON cf.session_id = c.session_id) "
            "SELECT facts.*, "
            "CASE WHEN quarantine_check IS NOT NULL THEN 'quarantined' "
            "WHEN size(flags) > 0 THEN 'flagged' ELSE 'accepted' END AS verdict "
            "FROM facts",
        ),
    ]


def _packets_select(day: str) -> str:
    return (
        "SELECT n.session_id, n.seq, "
        f"{day} AS date, n.segment, n.direction, n.state, n.packet_type, n.packet_name, "
        "n.wire_packet_id, n.protocol_version, n.player_key, n.backend_server, "
        "n.time_delta_ms, n.payload_original_size AS payload_size, n.payload_hash, n.has_event "
        "FROM layers_named n JOIN layers_session_verdicts v ON v.session_id = n.session_id "
        "WHERE v.verdict <> 'quarantined'"
    )


def _sessions_select(day: str) -> str:
    return (
        "SELECT v.session_id, "
        f"{day} AS date, v.player_key, v.started_at, v.client_protocol_version, "
        "v.observed_protocol_version, v.proxy_id, v.plugin_version, v.packet_count, "
        "v.segment_count, v.backend_servers, v.seq_gap_ratio, v.monotonic_ratio, "
        "v.drops_reported, v.dropped_total, v.has_disconnect, v.verdict, v.flags, "
        "v.quarantine_check FROM layers_session_verdicts v"
    )


def _client_select(day: str) -> str:
    return (
        "SELECT v.session_id, "
        f"{day} AS date, v.brand, v.channels, "
        "CASE WHEN v.channels IS NULL THEN NULL ELSE size(v.channels) END AS channel_count, "
        "v.client_protocol_version FROM layers_session_verdicts v"
    )


def _plugin_select(day: str) -> str:
    return (
        "SELECT c.session_id, c.seq, "
        f"{day} AS date, c.direction, c.channel, c.payload_original_size AS payload_size, "
        "c.payload_hash FROM layers_custom c"
    )
