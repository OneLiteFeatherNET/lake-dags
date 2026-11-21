"""Airflow Assets for the Iceberg tables. Only the Asset factory imports Airflow."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from airflow.sdk import Asset

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def iceberg_asset_uri(catalog: str, table: str) -> str:
    """Return the asset URI for a table named 'namespace.table' in the given catalog."""
    if not _IDENTIFIER.match(catalog):
        raise ValueError(
            f"catalog '{catalog}' must be a plain identifier (letters, digits, underscore)"
        )
    parts = table.split(".")
    if len(parts) != 2:
        raise ValueError(f"table name must be 'namespace.table', got '{table}'")
    for part in parts:
        if not _IDENTIFIER.match(part):
            raise ValueError(f"table name part '{part}' must be a plain identifier")
    return f"iceberg://{catalog}/{table}"


def iceberg_asset(catalog: str, table: str) -> Asset:
    from airflow.sdk import Asset

    return Asset(uri=iceberg_asset_uri(catalog, table))
