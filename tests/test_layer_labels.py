from datetime import UTC, datetime

import pytest

from lake.layers.labels import (
    LABEL_COLUMNS,
    ControlledEvent,
    LabelSource,
    ModuleWindow,
    SessionLabel,
    label_merge_statement,
    latest_per_label_id,
    module_windows,
    parse_label_batch,
    resolve_label,
    resolve_window,
)

PLAYER = "a" * 64
SESSION = "0" * 32
HEADER = ",".join(LABEL_COLUMNS)


def _row(**overrides):
    row = {
        "label_id": "L1",
        "player_key": PLAYER,
        "session_id": SESSION,
        "start_ts": "2026-11-20T10:00:00Z",
        "end_ts": "2026-11-20T11:00:00Z",
        "label": "trusted",
        "hack_types": "",
        "label_reason": "known",
        "source": "admin",
        "editor": "reviewer-1",
        "edited_at": "2026-11-21T08:00:00Z",
        "record_version": "1",
    }
    row.update(overrides)
    return ",".join(row[column] for column in LABEL_COLUMNS)


def _batch(*rows):
    return "\n".join([HEADER, *rows]) + "\n"


def _record(label="trusted", hack_types=(), start=10, end=11, version=1, label_id="L1"):
    return SessionLabel(
        label_id=label_id,
        player_key=PLAYER,
        session_id=SESSION,
        start_ts=datetime(2026, 11, 20, start, tzinfo=UTC),
        end_ts=datetime(2026, 11, 20, end, tzinfo=UTC),
        label=label,
        hack_types=tuple(hack_types),
        label_reason="behaviour" if label == "cheater" else "known",
        source="admin",
        editor="reviewer-1",
        edited_at=datetime(2026, 11, 21, 8, tzinfo=UTC),
        record_version=version,
    )


# --- CSV batch validation -------------------------------------------------------------


def test_valid_batch_parses_every_contract_column():
    records = parse_label_batch(_batch(_row()))

    assert records == [_record()]


def test_batch_header_must_match_contract_exactly():
    with pytest.raises(ValueError, match="header"):
        parse_label_batch("label_id,label\nL1,trusted\n")


def test_cheater_row_carries_hack_types_separated_by_semicolon():
    row = _row(label="cheater", hack_types="killaura;velocity", label_reason="behaviour")

    records = parse_label_batch(_batch(row))

    assert records[0].hack_types == ("killaura", "velocity")


@pytest.mark.parametrize("label", ["untrusted", "trusted"])
def test_hack_types_on_a_non_cheater_row_are_rejected(label):
    with pytest.raises(ValueError, match="hack_types"):
        parse_label_batch(_batch(_row(label=label, hack_types="killaura")))


def test_cheater_row_without_hack_types_is_rejected():
    with pytest.raises(ValueError, match="hack_types"):
        parse_label_batch(_batch(_row(label="cheater", label_reason="behaviour")))


def test_unknown_label_value_is_rejected():
    with pytest.raises(ValueError, match="label"):
        parse_label_batch(_batch(_row(label="suspicious")))


def test_unknown_label_reason_is_rejected():
    with pytest.raises(ValueError, match="label_reason"):
        parse_label_batch(_batch(_row(label_reason="vibes")))


def test_unknown_source_is_rejected():
    with pytest.raises(ValueError, match="source"):
        parse_label_batch(_batch(_row(source="chat")))


def test_end_before_start_is_rejected():
    with pytest.raises(ValueError, match="end_ts"):
        parse_label_batch(
            _batch(_row(start_ts="2026-11-20T11:00:00Z", end_ts="2026-11-20T10:00:00Z"))
        )


def test_timestamp_without_utc_suffix_is_rejected():
    with pytest.raises(ValueError, match="start_ts"):
        parse_label_batch(_batch(_row(start_ts="2026-11-20 10:00:00")))


def test_record_version_must_be_positive_integer():
    with pytest.raises(ValueError, match="record_version"):
        parse_label_batch(_batch(_row(record_version="0")))


def test_malformed_player_key_is_rejected():
    with pytest.raises(ValueError, match="player_key"):
        parse_label_batch(_batch(_row(player_key="not-hex")))


def test_empty_session_id_becomes_none():
    records = parse_label_batch(_batch(_row(session_id="")))

    assert records[0].session_id is None


def test_duplicate_label_id_in_one_batch_is_rejected():
    with pytest.raises(ValueError, match="duplicate label_id"):
        parse_label_batch(_batch(_row(), _row()))


def test_invalid_row_rejects_the_whole_batch():
    with pytest.raises(ValueError, match="line 3"):
        parse_label_batch(_batch(_row(label_id="L1"), _row(label_id="L2", label="bogus")))


# --- label rules ----------------------------------------------------------------------


def test_untrusted_is_unknown_and_not_a_negative():
    result = resolve_label([_record(label="untrusted")])

    assert result.label == "unknown"
    assert result.conflict is False


def test_trusted_is_a_negative():
    result = resolve_label([_record(label="trusted")])

    assert result.label == "trusted"
    assert result.hack_types == ()


def test_cheater_carries_its_hack_types_sorted():
    result = resolve_label([_record(label="cheater", hack_types=["velocity", "killaura"])])

    assert result.label == "cheater"
    assert result.hack_types == ("killaura", "velocity")


def test_trusted_and_cheater_overlap_is_unknown_with_conflict():
    result = resolve_label(
        [_record(label="trusted"), _record(label="cheater", hack_types=["fly"], label_id="L2")]
    )

    assert result.label == "unknown"
    assert result.conflict is True


def test_untrusted_and_cheater_overlap_is_a_conflict_too():
    result = resolve_label(
        [_record(label="untrusted"), _record(label="cheater", hack_types=["fly"], label_id="L2")]
    )

    assert result.label == "unknown"
    assert result.conflict is True


def test_no_overlapping_label_leaves_the_window_unlabelled():
    result = resolve_label([])

    assert result.label is None
    assert result.conflict is False


def test_window_resolution_only_considers_overlapping_records():
    early_cheater = _record(label="cheater", hack_types=["fly"], start=8, end=9, label_id="L2")

    result = resolve_window(
        [_record(label="trusted"), early_cheater],
        datetime(2026, 11, 20, 10, tzinfo=UTC),
        datetime(2026, 11, 20, 10, 30, tzinfo=UTC),
    )

    assert result.label == "trusted"
    assert result.conflict is False


def test_latest_record_version_wins_per_label_id():
    records = latest_per_label_id(
        [_record(version=1), _record(label="cheater", hack_types=["fly"], version=2)]
    )

    assert [(r.label, r.record_version) for r in records] == [("cheater", 2)]


def test_same_version_again_is_kept_once():
    records = latest_per_label_id([_record(version=2), _record(version=2)])

    assert records == [_record(version=2)]


# --- controlled recordings ------------------------------------------------------------


def _event(module, active, minute, second=0):
    return ControlledEvent(
        session_id=SESSION,
        player_key=PLAYER,
        module=module,
        active=active,
        event_ts=datetime(2026, 11, 20, 10, minute, second, tzinfo=UTC),
    )


def test_on_and_off_event_give_one_module_window():
    windows = module_windows([_event("killaura", True, 0), _event("killaura", False, 0, 30)])

    assert windows == [
        ModuleWindow(
            session_id=SESSION,
            player_key=PLAYER,
            module="killaura",
            start_ts=datetime(2026, 11, 20, 10, 0, tzinfo=UTC),
            end_ts=datetime(2026, 11, 20, 10, 0, 30, tzinfo=UTC),
        )
    ]


def test_each_module_pairs_with_its_own_off_event():
    windows = module_windows(
        [
            _event("killaura", True, 0),
            _event("fly", True, 1),
            _event("fly", False, 2),
            _event("killaura", False, 3),
        ]
    )

    spans = {(w.module, w.start_ts.minute, w.end_ts.minute) for w in windows}
    assert spans == {("killaura", 0, 3), ("fly", 1, 2)}


def test_unclosed_on_event_is_rejected():
    with pytest.raises(ValueError, match="killaura"):
        module_windows([_event("killaura", True, 0)])


def test_off_event_without_on_is_rejected():
    with pytest.raises(ValueError, match="killaura"):
        module_windows([_event("killaura", False, 0)])


def test_second_on_before_off_is_rejected():
    with pytest.raises(ValueError, match="killaura"):
        module_windows([_event("killaura", True, 0), _event("killaura", True, 1)])


# --- Blackhole adapter interface and upsert -------------------------------------------


class _FakeSource:
    def fetch_since(self, since):
        return []


def test_fake_source_satisfies_the_label_source_protocol():
    assert isinstance(_FakeSource(), LabelSource)


def test_merge_upserts_by_label_id_and_keeps_newest_version():
    statement = label_merge_statement("labels.sessions", "staged_labels")

    assert statement.startswith("MERGE INTO labels.sessions AS target")
    assert "USING staged_labels AS source" in statement
    assert "ON target.label_id = source.label_id" in statement
    assert (
        "WHEN MATCHED AND source.record_version > target.record_version THEN UPDATE SET"
        in statement
    )
    assert "WHEN NOT MATCHED THEN INSERT" in statement


def test_merge_never_deletes_rows():
    statement = label_merge_statement("labels.sessions", "staged_labels")

    assert "DELETE" not in statement.upper()


def test_merge_targets_only_label_tables():
    with pytest.raises(ValueError, match="labels"):
        label_merge_statement("capture.raw", "staged_labels")


def test_merge_rejects_source_name_that_is_not_an_identifier():
    with pytest.raises(ValueError, match="source"):
        label_merge_statement("labels.sessions", "staged; DROP TABLE x")
