from datetime import date

import pytest

from lake.layers.reference import (
    BackendPeriod,
    PacketName,
    backend_config_statement,
    load_backend_config,
    load_packet_names,
    packet_names_statement,
    parse_backend_config,
    parse_packet_names,
)

PACKET_HEADER = "protocol_version,state,direction,packet_id,packet_name"
BACKEND_HEADER = "backend,period_start,period_end,anti_xray_mode"


def _packets(*rows):
    return "\n".join([PACKET_HEADER, *rows]) + "\n"


def _backends(*rows):
    return "\n".join([BACKEND_HEADER, *rows]) + "\n"


# --- packet names -------------------------------------------------------------------


def test_valid_packet_names_parse_to_typed_rows():
    rows = parse_packet_names(_packets("767,play,clientbound,35,PLAY_EXAMPLE_PACKET"))

    assert rows == [
        PacketName(
            protocol_version=767,
            state="play",
            direction="clientbound",
            packet_id=35,
            packet_name="PLAY_EXAMPLE_PACKET",
        )
    ]


def test_header_must_match_exactly():
    with pytest.raises(ValueError, match="header"):
        parse_packet_names("protocol_version,state\n767,play\n")


def test_unknown_state_is_rejected_before_any_write():
    with pytest.raises(ValueError, match="state"):
        parse_packet_names(_packets("767,playing,clientbound,35,PLAY_EXAMPLE_PACKET"))


def test_unknown_direction_is_rejected():
    with pytest.raises(ValueError, match="direction"):
        parse_packet_names(_packets("767,play,both,35,PLAY_EXAMPLE_PACKET"))


def test_duplicate_key_is_rejected_with_its_line():
    text = _packets(
        "767,play,clientbound,35,PLAY_A",
        "767,play,clientbound,35,PLAY_B",
    )

    with pytest.raises(ValueError, match="line 3.*duplicate"):
        parse_packet_names(text)


def test_same_packet_id_in_two_versions_is_not_a_duplicate():
    rows = parse_packet_names(
        _packets(
            "767,play,clientbound,35,PLAY_A_767",
            "769,play,clientbound,35,PLAY_A_769",
        )
    )

    assert {row.packet_name for row in rows} == {"PLAY_A_767", "PLAY_A_769"}


@pytest.mark.parametrize("name", ["", "has space", "x'; DROP", "1_STARTS_WITH_DIGIT"])
def test_unsafe_packet_name_is_rejected(name):
    with pytest.raises(ValueError, match="packet_name"):
        parse_packet_names(_packets(f"767,play,clientbound,35,{name}"))


def test_negative_packet_id_is_rejected():
    with pytest.raises(ValueError, match="packet_id"):
        parse_packet_names(_packets("767,play,clientbound,-1,PLAY_A"))


def test_empty_packet_file_is_rejected():
    with pytest.raises(ValueError, match="no rows"):
        parse_packet_names(PACKET_HEADER + "\n")


def test_load_packet_names_returns_the_merge_only_after_validation():
    load = load_packet_names(_packets("767,play,clientbound,35,PLAY_A"), "staged_packet_names")

    assert load.protocol_versions == (767,)
    assert len(load.statements) == 1
    assert load.statements[0] == packet_names_statement("staged_packet_names", [767])


def test_invalid_packet_file_produces_no_statement():
    with pytest.raises(ValueError):
        load_packet_names(_packets("767,playing,clientbound,35,PLAY_A"), "staged_packet_names")


def test_packet_names_merge_replaces_only_affected_protocol_versions():
    statement = packet_names_statement("staged_packet_names", [767, 769])

    assert statement.startswith("MERGE INTO ref.packet_names AS target")
    assert (
        "ON target.protocol_version = source.protocol_version "
        "AND target.state = source.state "
        "AND target.direction = source.direction "
        "AND target.packet_id = source.packet_id"
    ) in statement
    assert "WHEN NOT MATCHED BY SOURCE AND target.protocol_version IN (767, 769) THEN DELETE" in (
        statement
    )
    assert "capture." not in statement


def test_packet_names_merge_rejects_an_empty_version_list():
    with pytest.raises(ValueError, match="protocol"):
        packet_names_statement("staged_packet_names", [])


# --- backend configuration -----------------------------------------------------------


def test_valid_backend_periods_parse_with_open_end_as_none():
    periods = parse_backend_config(
        _backends(
            "lobby,2026-01-01,2026-06-01,none",
            "lobby,2026-06-01,,simple",
        )
    )

    assert periods == [
        BackendPeriod("lobby", date(2026, 1, 1), date(2026, 6, 1), "none"),
        BackendPeriod("lobby", date(2026, 6, 1), None, "simple"),
    ]


def test_adjacent_periods_do_not_overlap():
    periods = parse_backend_config(
        _backends("lobby,2026-01-01,2026-06-01,none", "lobby,2026-06-01,2026-07-01,simple")
    )

    assert len(periods) == 2


def test_overlapping_periods_of_one_backend_are_rejected():
    with pytest.raises(ValueError, match="overlap"):
        parse_backend_config(
            _backends("lobby,2026-01-01,2026-06-01,none", "lobby,2026-05-01,2026-07-01,simple")
        )


def test_open_ended_period_overlaps_a_later_period():
    with pytest.raises(ValueError, match="overlap"):
        parse_backend_config(
            _backends("lobby,2026-01-01,,none", "lobby,2026-06-01,2026-07-01,simple")
        )


def test_overlap_is_checked_per_backend_only():
    periods = parse_backend_config(
        _backends("lobby,2026-01-01,,none", "survival,2026-01-01,,simple")
    )

    assert len(periods) == 2


def test_period_end_before_start_is_rejected():
    with pytest.raises(ValueError, match="period_end"):
        parse_backend_config(_backends("lobby,2026-06-01,2026-01-01,none"))


def test_unknown_anti_xray_mode_is_rejected():
    with pytest.raises(ValueError, match="anti_xray_mode"):
        parse_backend_config(_backends("lobby,2026-01-01,,maximum"))


def test_invalid_date_is_rejected():
    with pytest.raises(ValueError, match="period_start"):
        parse_backend_config(_backends("lobby,2026-13-01,,none"))


def test_unsafe_backend_name_is_rejected():
    with pytest.raises(ValueError, match="backend"):
        parse_backend_config(_backends("lobby'; DROP,2026-01-01,,none"))


def test_backend_merge_replaces_rows_of_affected_backends_only():
    statement = backend_config_statement("staged_backend_config", ["lobby"])

    assert statement.startswith("MERGE INTO ref.backend_config AS target")
    assert "ON target.backend = source.backend AND target.period_start = source.period_start" in (
        statement
    )
    assert "WHEN NOT MATCHED BY SOURCE AND target.backend IN ('lobby') THEN DELETE" in statement


def test_load_backend_config_validates_before_building():
    with pytest.raises(ValueError):
        load_backend_config(_backends("lobby,2026-06-01,2026-01-01,none"), "staged_backend_config")


def test_load_backend_config_lists_backends_of_the_file():
    load = load_backend_config(
        _backends("lobby,2026-01-01,,none", "survival,2026-01-01,,simple"), "staged_backend_config"
    )

    assert load.backends == ("lobby", "survival")
