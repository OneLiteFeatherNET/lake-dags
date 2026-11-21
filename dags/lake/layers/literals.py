"""SQL literal and identifier helpers for the layer statements. Pure.

Untrusted values never reach a statement as raw text: strings must match a safe pattern, player
keys are fixed 32-byte values written as X'..' literals, and every other value is typed.
"""

from __future__ import annotations

import re
from datetime import UTC, date, datetime

_SAFE_TEXT = re.compile(r"^[A-Za-z0-9_.:-]{1,128}$")
_SAFE_PHRASE = re.compile(r"^[A-Za-z0-9_.:,;() -]{1,256}$")
_IDENTIFIER = re.compile(r"^[a-z_][a-z0-9_]{0,127}$")
_HEX32 = re.compile(r"^[0-9a-fA-F]{64}$")
_TABLE = re.compile(r"^(refined|features|labels|ref|quality)\.[a-z_][a-z0-9_]*$")
TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"


def string_literal(value: str) -> str:
    if not _SAFE_TEXT.match(value):
        raise ValueError(f"value '{value}' is not a safe SQL text literal")
    return f"'{value}'"


def phrase_literal(value: str) -> str:
    """Human-readable text such as a failure reason: letters, digits and simple punctuation."""
    if not _SAFE_PHRASE.match(value):
        raise ValueError(f"value '{value}' is not a safe SQL text literal")
    return f"'{value}'"


def identifier(value: str) -> str:
    if not _IDENTIFIER.match(value):
        raise ValueError(f"'{value}' is not a plain SQL identifier")
    return value


def int_literal(value: int) -> str:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"value must be an integer >= 0, got {value!r}")
    return str(value)


def player_key_literal(player_key: str) -> str:
    """A player key as X'<64 hex>' (32 bytes). Anything else is rejected."""
    if not _HEX32.match(player_key):
        raise ValueError("player_key must be 64 hex characters (32 bytes)")
    return f"X'{player_key.lower()}'"


def date_literal(value: date) -> str:
    return f"DATE '{value.isoformat()}'"


def timestamp_literal(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")
    return f"TIMESTAMP '{value.astimezone(UTC).strftime(TIMESTAMP_FORMAT)}'"


def table_name(value: str) -> str:
    """A layer table, namespace.name, in one of the namespaces this change writes."""
    if not _TABLE.match(value):
        raise ValueError(f"'{value}' is not a table of the layer namespaces")
    return value
