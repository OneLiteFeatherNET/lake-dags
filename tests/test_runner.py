import logging
from datetime import UTC, datetime, timedelta

import pytest

from lake.config import load_config
from lake.runner import run_table_maintenance

START = datetime(2026, 11, 18, 3, 0, tzinfo=UTC)
JOB_SOURCE = "print('job')\n"


class FakeClock:
    """Time only moves when the runner sleeps, so tests never wait."""

    def __init__(self):
        self.now = START
        self.sleeps: list[float] = []

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += timedelta(seconds=seconds)


class FakeClient:
    def __init__(self, states=(), fail_deletes=False):
        self.states = list(states)
        self.fail_deletes = fail_deletes
        self.calls: list[tuple] = []

    def create_configmap(self, namespace, body):
        self.calls.append(("create_configmap", namespace, body["metadata"]["name"]))

    def delete_configmap(self, namespace, name):
        self.calls.append(("delete_configmap", namespace, name))
        if self.fail_deletes:
            raise RuntimeError("configmap delete boom")

    def create_spark_application(self, namespace, body):
        self.calls.append(("create_spark_application", namespace, body["metadata"]["name"]))

    def get_spark_application(self, namespace, name):
        self.calls.append(("get_spark_application", namespace, name))
        if len(self.states) > 1:
            return self.states.pop(0)
        return self.states[0]

    def delete_spark_application(self, namespace, name):
        self.calls.append(("delete_spark_application", namespace, name))
        if self.fail_deletes:
            raise RuntimeError("app delete boom")


def _state(summary, history=None):
    status = {"currentState": {"currentStateSummary": summary}}
    if history is not None:
        status["stateTransitionHistory"] = history
    return {"status": status}


def _run(client, clock, timeout_seconds=3600, table_name="capture.raw"):
    config = load_config()
    table = next(t for t in config.tables if t.name == table_name)
    run_table_maintenance(
        client,
        config,
        table,
        job_source=JOB_SOURCE,
        now_fn=clock,
        sleep_fn=clock.sleep,
        timeout_seconds=timeout_seconds,
        poll_seconds=30,
    )


def test_success_creates_polls_until_succeeded_then_deletes_both():
    client = FakeClient(states=[_state("RunningHealthy"), _state("Succeeded")])
    clock = FakeClock()

    _run(client, clock)

    name = "lake-maint-capture-raw-20261118030000"
    assert client.calls == [
        ("create_configmap", "data", name),
        ("create_spark_application", "data", name),
        ("get_spark_application", "data", name),
        ("get_spark_application", "data", name),
        ("delete_spark_application", "data", name),
        ("delete_configmap", "data", name),
    ]
    assert clock.sleeps == [30]


def test_failed_state_raises_runtime_error_and_still_deletes_both():
    client = FakeClient(states=[_state("Failed")])

    with pytest.raises(RuntimeError, match="Failed"):
        _run(client, FakeClock())

    kinds = [call[0] for call in client.calls]
    assert kinds[-2:] == ["delete_spark_application", "delete_configmap"]


def test_delete_errors_are_logged_and_do_not_mask_the_failure(caplog):
    client = FakeClient(states=[_state("Failed")], fail_deletes=True)

    with caplog.at_level(logging.ERROR, logger="lake.runner"):
        with pytest.raises(RuntimeError, match="failed"):
            _run(client, FakeClock())

    messages = [record.getMessage() for record in caplog.records]
    assert any("SparkApplication" in message for message in messages)
    assert any("ConfigMap" in message for message in messages)


def test_timeout_raises_timeout_error_with_fake_clock_and_deletes_both():
    client = FakeClient(states=[_state("RunningHealthy")])
    clock = FakeClock()

    with pytest.raises(TimeoutError, match="capture.raw"):
        _run(client, clock, timeout_seconds=60)

    assert clock.sleeps == [30, 30]
    kinds = [call[0] for call in client.calls]
    assert kinds[-2:] == ["delete_spark_application", "delete_configmap"]


def test_resource_released_after_succeeded_is_success():
    history = {"2026-11-18T03:00:10Z": {"currentStateSummary": "Succeeded"}}
    client = FakeClient(states=[_state("ResourceReleased", history=history)])

    _run(client, FakeClock())

    assert ("delete_configmap", "data", "lake-maint-capture-raw-20261118030000") in client.calls


def test_resource_released_without_succeeded_is_failure():
    client = FakeClient(states=[_state("ResourceReleased")])

    with pytest.raises(RuntimeError, match="ResourceReleased"):
        _run(client, FakeClock())


def test_run_for_table_without_retention_skips_the_delete_statement():
    client = FakeClient(states=[_state("Succeeded")])
    config = load_config()
    events = next(t for t in config.tables if t.name == "capture.events")
    body_seen = {}

    def create_app(namespace, body):
        body_seen.update(body)

    client.create_spark_application = create_app
    run_table_maintenance(
        client,
        config,
        events,
        job_source=JOB_SOURCE,
        now_fn=FakeClock(),
        sleep_fn=lambda seconds: None,
        timeout_seconds=60,
        poll_seconds=30,
    )

    statements = body_seen["spec"]["driverArgs"][2]
    assert "DELETE" not in statements
    assert body_seen["spec"]["driverArgs"][1] == "capture.events"
