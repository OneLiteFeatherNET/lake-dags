import pytest

from lake.config import load_config
from lake.layers.config import load_layers_config

# Retention per table from design decision 15 (add-lake-layers). None means kept without expiry.
DECISION_15_RETENTION = {
    "capture.raw": None,
    "capture.events": None,
    "capture.sessions": None,
    "capture.segments": None,
    "capture.session_ends": None,
    "refined.packets": 365,
    "refined.client_info": 365,
    "refined.plugin_messages": 365,
    "refined.sessions": 365,
    "features.session_stats": 365,
    "features.windows": 365,
    "labels.sessions": None,
    "labels.windows": None,
    "quality.check_results": 400,
    "quality.quarantine_sessions": 30,
    "quality.erasure_requests": None,
}


def _maintenance_by_name():
    return {table.name: table for table in load_config().tables}


def _kept_by_name():
    return {table.name: table for table in load_layers_config().kept_tables}


def test_capture_raw_is_kept_permanently_in_maintenance():
    assert _maintenance_by_name()["capture.raw"].retention_days is None


@pytest.mark.parametrize("name", sorted(DECISION_15_RETENTION))
def test_decision_15_table_has_expected_retention(name):
    expected = DECISION_15_RETENTION[name]
    maintained = _maintenance_by_name()
    kept = _kept_by_name()

    if name in maintained:
        assert maintained[name].retention_days == expected
        assert maintained[name].day_column == "date"
    else:
        assert name in kept, f"{name} is neither in maintenance.yaml nor layers.yaml"
        assert kept[name].retention_days == expected


def test_layers_kept_tables_are_not_also_in_maintenance():
    overlap = set(_kept_by_name()) & set(_maintenance_by_name())

    assert overlap == set()
