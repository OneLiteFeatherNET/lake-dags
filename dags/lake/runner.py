"""Create, wait for and clean up one SparkApplication per table, via an injected client."""

from __future__ import annotations

import json
import logging
from collections.abc import Callable
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Protocol

from lake.config import MaintenanceConfig, TableConfig
from lake.manifest import NAMESPACE, build_configmap, build_spark_application, job_name
from lake.sql import build_statements

log = logging.getLogger(__name__)

_FAILURE_STATES = {"Failed", "SchedulingFailure", "TerminatedWithoutReleaseResources", "Suspended"}


class Outcome(Enum):
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class SparkClient(Protocol):
    def create_configmap(self, namespace: str, body: dict[str, Any]) -> None: ...

    def delete_configmap(self, namespace: str, name: str) -> None: ...

    def create_spark_application(self, namespace: str, body: dict[str, Any]) -> None: ...

    def get_spark_application(self, namespace: str, name: str) -> dict[str, Any]: ...

    def delete_spark_application(self, namespace: str, name: str) -> None: ...


def classify(app: dict[str, Any]) -> Outcome:
    """Map a SparkApplication object to running, succeeded or failed."""
    status = app.get("status") or {}
    summary = (status.get("currentState") or {}).get("currentStateSummary")
    if summary == "Succeeded":
        return Outcome.SUCCEEDED
    if summary in _FAILURE_STATES:
        return Outcome.FAILED
    if summary == "ResourceReleased":
        # Succeeded is terminal: seeing it in the history means the run finished before release.
        return Outcome.SUCCEEDED if _succeeded_in_history(status) else Outcome.FAILED
    return Outcome.RUNNING


def _succeeded_in_history(status: dict[str, Any]) -> bool:
    histories = [status.get("stateTransitionHistory") or {}]
    attempt = status.get("currentAttemptSummary") or {}
    histories.append(attempt.get("stateTransitionHistory") or {})
    return any(
        isinstance(entry, dict) and entry.get("currentStateSummary") == "Succeeded"
        for history in histories
        for entry in history.values()
    )


def _summary(app: dict[str, Any]) -> str:
    state = (app.get("status") or {}).get("currentState") or {}
    text = state.get("currentStateSummary", "unknown")
    message = state.get("message")
    return f"{text}: {message}" if message else str(text)


def _delete_quietly(action: Callable[[str, str], None], kind: str, name: str) -> None:
    try:
        action(NAMESPACE, name)
    except Exception:
        log.exception("failed to delete %s %s/%s", kind, NAMESPACE, name)


def run_table_maintenance(
    client: SparkClient,
    config: MaintenanceConfig,
    table: TableConfig,
    *,
    job_source: str,
    now_fn: Callable[[], datetime],
    sleep_fn: Callable[[float], None],
    timeout_seconds: float,
    poll_seconds: float,
) -> None:
    now = now_fn()
    name = job_name(table.name, now.strftime("%Y%m%d%H%M%S"))
    labels = {"app.kubernetes.io/part-of": "lake-maintenance", "app.kubernetes.io/instance": name}
    statements = build_statements(config.catalog, table, now, config.snapshot_max_age_days)
    configmap = build_configmap(name, NAMESPACE, job_source, labels)
    app = build_spark_application(
        name=name,
        namespace=NAMESPACE,
        catalog=config.catalog,
        settings=config.spark,
        config_map_name=name,
        table_fqn=table.name,
        statements_json=json.dumps(statements),
        labels=labels,
    )
    client.create_configmap(NAMESPACE, configmap)
    try:
        client.create_spark_application(NAMESPACE, app)
        deadline = now + timedelta(seconds=timeout_seconds)
        while True:
            current = client.get_spark_application(NAMESPACE, name)
            outcome = classify(current)
            if outcome is Outcome.SUCCEEDED:
                log.info("maintenance for %s succeeded", table.name)
                return
            if outcome is Outcome.FAILED:
                raise RuntimeError(f"maintenance for {table.name} failed ({_summary(current)})")
            if now_fn() >= deadline:
                raise TimeoutError(
                    f"maintenance for {table.name} did not finish within {timeout_seconds}s"
                )
            sleep_fn(poll_seconds)
    finally:
        _delete_quietly(client.delete_spark_application, "SparkApplication", name)
        _delete_quietly(client.delete_configmap, "ConfigMap", name)
