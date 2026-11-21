"""Decode configuration-phase custom payloads: brand and channel list.

Standalone on purpose: the Spark job ships this file next to itself and must not import the
lake package. Pure: bytes in, values out.
"""

from __future__ import annotations

_MAX_VARINT_BYTES = 5
_MAX_INT32 = 0x7FFFFFFF
_NUL = b"\x00"


def read_varint(data: bytes, offset: int = 0) -> tuple[int, int]:
    """Read a Minecraft VarInt at offset. Returns the value and the offset after it."""
    value = 0
    for index in range(_MAX_VARINT_BYTES):
        position = offset + index
        if position >= len(data):
            raise ValueError(f"VarInt truncated at byte offset {position}")
        byte = data[position]
        value |= (byte & 0x7F) << (7 * index)
        if not byte & 0x80:
            if value > _MAX_INT32:
                raise ValueError(f"VarInt exceeds the 32-bit range: {value}")
            return value, position + 1
    raise ValueError(f"VarInt too long: more than {_MAX_VARINT_BYTES} bytes")


def decode_brand(payload: bytes) -> str:
    """Brand payload: a VarInt byte length, then the UTF-8 brand, and nothing after it."""
    length, start = read_varint(payload)
    end = start + length
    if end > len(payload):
        raise ValueError(f"brand truncated: {length} bytes announced, {len(payload) - start} left")
    if end != len(payload):
        raise ValueError("brand payload has trailing bytes")
    return _utf8(payload[start:end], "brand")


def decode_channels(payload: bytes) -> list[str]:
    """Channel list payload: channel names separated by NUL. An empty payload is an empty list."""
    if not payload:
        return []
    names = payload.split(_NUL)
    if any(not name for name in names):
        raise ValueError("channel list has an empty channel name")
    return [_utf8(name, "channel") for name in names]


def _utf8(raw: bytes, what: str) -> str:
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError as error:
        raise ValueError(f"{what} is not valid UTF-8") from error
