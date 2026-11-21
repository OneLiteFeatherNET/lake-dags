"""Train and test split by player (design decision 11). Pure: no Spark, no I/O.

The split is a function of the player key and the seed, so it does not depend on the data,
the input order or the run. A player never lands in both splits, because the value is computed
from the key alone.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable

from lake.layers.config import SplitConfig

PLAYER_KEY_BYTES = 32  # capture.raw.player_key is fixed(32), the HMAC of the player UUID
_TRAIN = "train"
_TEST = "test"
_SPACE = 2**64


def split_of(player_key: bytes, config: SplitConfig) -> str:
    """Return "train" or "test" for one player key."""
    if not isinstance(player_key, bytes) or len(player_key) != PLAYER_KEY_BYTES:
        raise ValueError(f"player_key must be {PLAYER_KEY_BYTES} bytes")
    if not 0 < config.train_ratio < 1:
        raise ValueError(f"train_ratio must be between 0 and 1, got {config.train_ratio}")
    digest = hashlib.sha256(config.seed.to_bytes(8, "big") + player_key).digest()
    position = int.from_bytes(digest[:8], "big") / _SPACE
    return _TRAIN if position < config.train_ratio else _TEST


def split_players(keys: Iterable[bytes], config: SplitConfig) -> dict[bytes, str]:
    """Map every distinct player key to its split. Input order and repeats do not matter."""
    return {key: split_of(key, config) for key in set(keys)}
