import pytest

from lake.assets import iceberg_asset_uri


def test_uri_uses_iceberg_scheme_with_catalog_and_table():
    assert iceberg_asset_uri("corvus", "refined.packets") == "iceberg://corvus/refined.packets"


def test_uri_keeps_table_name_with_underscores():
    assert iceberg_asset_uri("corvus", "features.session_stats") == (
        "iceberg://corvus/features.session_stats"
    )


def test_uri_rejects_table_without_namespace():
    with pytest.raises(ValueError, match="namespace.table"):
        iceberg_asset_uri("corvus", "packets")


def test_uri_rejects_table_with_extra_dots():
    with pytest.raises(ValueError, match="namespace.table"):
        iceberg_asset_uri("corvus", "refined.packets.v2")


def test_uri_rejects_catalog_that_is_not_a_plain_identifier():
    with pytest.raises(ValueError, match="plain identifier"):
        iceberg_asset_uri("cor/vus", "refined.packets")
