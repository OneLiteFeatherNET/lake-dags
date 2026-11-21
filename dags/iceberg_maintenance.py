"""Nightly Iceberg maintenance: one SparkApplication per table listed in config/maintenance.yaml."""

from __future__ import annotations

import time
from datetime import UTC, datetime

from airflow.sdk import dag, task

from lake.assets import iceberg_asset
from lake.config import TableConfig, load_config
from lake.kube import KubeSparkClient
from lake.manifest import read_job_source
from lake.runner import run_table_maintenance

CONFIG = load_config()
POLL_SECONDS = 30
TIMEOUT_SECONDS = 4 * 60 * 60


def _now() -> datetime:
    return datetime.now(UTC)


def _maintenance_task(table: TableConfig) -> None:
    # Inlet, not outlet: maintenance must never trigger asset-scheduled DAGs such as lake_features.
    @task(
        task_id=f"maintain_{table.name.replace('.', '_')}",
        inlets=[iceberg_asset(CONFIG.catalog, table.name)],
    )
    def maintain() -> None:
        run_table_maintenance(
            KubeSparkClient(),
            CONFIG,
            table,
            job_source=read_job_source(),
            now_fn=_now,
            sleep_fn=time.sleep,
            timeout_seconds=TIMEOUT_SECONDS,
            poll_seconds=POLL_SECONDS,
        )

    maintain()


@dag(
    dag_id="iceberg_maintenance",
    schedule="0 3 * * *",
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    # Cluster memory fits one driver+executor pair at a time; parallel tables stayed Pending.
    max_active_tasks=1,
    default_args={"retries": 2},
)
def iceberg_maintenance() -> None:
    for table in CONFIG.tables:
        _maintenance_task(table)


iceberg_maintenance()
