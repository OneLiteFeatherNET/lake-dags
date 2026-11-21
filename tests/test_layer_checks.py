import pytest

from lake.layers.checks import (
    CATALOGUE,
    Outcome,
    Scope,
    SessionVerdict,
    Verdict,
    below_baseline,
    check_verdict,
    day_publishable,
    session_verdict,
)
from lake.layers.config import load_layers_config


def _ids(scope: Scope) -> set[str]:
    return {spec.id for spec in CATALOGUE if spec.scope is scope}


def test_passing_table_check_is_pass():
    assert check_verdict("dup.rate", blocking=False, outcome=Outcome.PASSED) is Verdict.PASS


def test_passing_blocking_check_is_pass():
    assert check_verdict("rows.reconcile", blocking=True, outcome=Outcome.PASSED) is Verdict.PASS


def test_failing_blocking_table_check_is_fail():
    assert check_verdict("schema.capture_raw", blocking=True, outcome=Outcome.FAILED) is (
        Verdict.FAIL
    )


def test_failing_non_blocking_table_check_is_warn():
    assert check_verdict("dup.rate", blocking=False, outcome=Outcome.FAILED) is Verdict.WARN


def test_forced_publish_turns_blocking_failure_into_overridden():
    verdict = check_verdict("rows.baseline", blocking=True, outcome=Outcome.FAILED, forced=True)

    assert verdict is Verdict.OVERRIDDEN


def test_forced_publish_keeps_non_blocking_failure_as_warn():
    verdict = check_verdict("dup.rate", blocking=False, outcome=Outcome.FAILED, forced=True)

    assert verdict is Verdict.WARN


def test_forced_publish_does_not_change_a_pass():
    verdict = check_verdict("rows.baseline", blocking=True, outcome=Outcome.PASSED, forced=True)

    assert verdict is Verdict.PASS


@pytest.mark.parametrize("blocking", [True, False])
def test_not_available_is_recorded_not_failed(blocking):
    verdict = check_verdict("time.skew", blocking=blocking, outcome=Outcome.NOT_AVAILABLE)

    assert verdict is Verdict.NOT_AVAILABLE


def test_session_scope_check_is_not_a_check_result():
    with pytest.raises(ValueError, match="refined.sessions"):
        check_verdict("seq.gaps", blocking=False, outcome=Outcome.FAILED)


def test_unknown_check_id_is_rejected():
    with pytest.raises(ValueError, match="unknown check"):
        check_verdict("made.up", blocking=True, outcome=Outcome.PASSED)


def test_day_is_publishable_without_any_fail():
    verdicts = [Verdict.PASS, Verdict.WARN, Verdict.OVERRIDDEN, Verdict.NOT_AVAILABLE]

    assert day_publishable(verdicts) is True


def test_day_is_not_publishable_with_one_fail():
    assert day_publishable([Verdict.PASS, Verdict.FAIL]) is False


def test_day_without_checks_is_publishable():
    assert day_publishable([]) is True


def test_session_without_failures_is_accepted():
    assert session_verdict(quarantine_failed=False, flags=[]) is SessionVerdict.ACCEPTED


def test_session_with_flag_is_flagged():
    verdict = session_verdict(quarantine_failed=False, flags=["no_config_phase"])

    assert verdict is SessionVerdict.FLAGGED


def test_quarantine_outranks_flags():
    verdict = session_verdict(quarantine_failed=True, flags=["session.complete"])

    assert verdict is SessionVerdict.QUARANTINED


def test_baseline_fails_below_half_of_median():
    assert below_baseline(rows=499, median_rows=1000, ratio=0.5) is True


def test_baseline_passes_at_exactly_half_of_median():
    assert below_baseline(rows=500, median_rows=1000, ratio=0.5) is False


def test_baseline_never_fails_without_history():
    assert below_baseline(rows=0, median_rows=0, ratio=0.5) is False


def test_baseline_rejects_ratio_outside_unit_interval():
    with pytest.raises(ValueError, match="ratio"):
        below_baseline(rows=1, median_rows=1, ratio=1.5)


def test_catalogue_has_one_entry_per_check_id():
    ids = [spec.id for spec in CATALOGUE]

    assert len(ids) == len(set(ids))


def test_catalogue_matches_checks_in_layers_yaml():
    configured = {check.id for check in load_layers_config().checks}

    assert configured == {spec.id for spec in CATALOGUE}


def test_session_checks_never_block_the_day():
    blocking_session = {
        check.id
        for check in load_layers_config().checks
        if check.blocking and check.id in _ids(Scope.SESSION)
    }

    assert blocking_session == set()


def test_session_scope_is_exactly_the_quarantine_and_flag_checks():
    assert _ids(Scope.SESSION) == {
        "dup.conflict",
        "seq.gaps",
        "time.monotonic",
        "drops.reported",
        "session.complete",
    }
