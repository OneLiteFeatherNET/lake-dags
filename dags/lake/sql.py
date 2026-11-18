"""Build the Iceberg maintenance SQL for one table. Pure: no Airflow, no I/O."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

from lake.config import TableConfig

ORPHAN_AGE_DAYS = 3
_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"


def _to_utc(now: datetime) -> datetime:
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("now must be a timezone-aware datetime")
    return now.astimezone(UTC)


def cutoff_day(now: datetime, retention_days: int) -> date:
    """First day that is kept: today (UTC) minus retention_days."""
    if retention_days < 1:
        raise ValueError("retention_days must be >= 1")
    return _to_utc(now).date() - timedelta(days=retention_days)


def build_statements(
    catalog: str, table: TableConfig, now: datetime, snapshot_max_age_days: int
) -> list[str]:
    utc_now = _to_utc(now)
    name = table.name
    statements: list[str] = []
    if table.retention_days is not None:
        cutoff = cutoff_day(utc_now, table.retention_days)
        statements.append(
            f"DELETE FROM {catalog}.{name} WHERE {table.day_column} < DATE '{cutoff.isoformat()}'"
        )
    statements.append(f"CALL {catalog}.system.rewrite_data_files('{name}')")
    expire_before = utc_now - timedelta(days=snapshot_max_age_days)
    statements.append(
        f"CALL {catalog}.system.expire_snapshots(table => '{name}', "
        f"older_than => TIMESTAMP '{expire_before.strftime(_TIMESTAMP_FORMAT)}', retain_last => 1)"
    )
    orphan_before = utc_now - timedelta(days=ORPHAN_AGE_DAYS)
    statements.append(
        f"CALL {catalog}.system.remove_orphan_files(table => '{name}', "
        f"older_than => TIMESTAMP '{orphan_before.strftime(_TIMESTAMP_FORMAT)}')"
    )
    return statements
