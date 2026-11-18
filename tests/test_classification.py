import pytest

from lake.runner import Outcome, classify


def _app(summary, history=None, attempt_history=None):
    status = {"currentState": {"currentStateSummary": summary}}
    if history is not None:
        status["stateTransitionHistory"] = history
    if attempt_history is not None:
        status["currentAttemptSummary"] = {"stateTransitionHistory": attempt_history}
    return {"status": status}


def test_succeeded_is_success():
    assert classify(_app("Succeeded")) is Outcome.SUCCEEDED


@pytest.mark.parametrize(
    "summary",
    ["Failed", "SchedulingFailure", "TerminatedWithoutReleaseResources", "Suspended"],
)
def test_failure_states_are_failures(summary):
    assert classify(_app(summary)) is Outcome.FAILED


@pytest.mark.parametrize("summary", ["RunningHealthy", "DriverRequested", "DriverStarted"])
def test_in_progress_states_keep_polling(summary):
    assert classify(_app(summary)) is Outcome.RUNNING


def test_missing_status_keeps_polling():
    assert classify({}) is Outcome.RUNNING


def test_resource_released_after_succeeded_in_history_is_success():
    app = _app("ResourceReleased", history={"1": {"currentStateSummary": "Succeeded"}})

    assert classify(app) is Outcome.SUCCEEDED


def test_resource_released_after_succeeded_in_attempt_history_is_success():
    app = _app("ResourceReleased", attempt_history={"1": {"currentStateSummary": "Succeeded"}})

    assert classify(app) is Outcome.SUCCEEDED


def test_resource_released_without_succeeded_is_failure():
    app = _app("ResourceReleased", history={"1": {"currentStateSummary": "Failed"}})

    assert classify(app) is Outcome.FAILED


def test_resource_released_with_empty_history_is_failure():
    assert classify(_app("ResourceReleased")) is Outcome.FAILED
