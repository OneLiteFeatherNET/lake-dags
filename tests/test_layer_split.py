import hashlib

import pytest

from lake.layers.config import SplitConfig
from lake.layers.split import split_of, split_players

SEED = 20261121
CONFIG = SplitConfig(train_ratio=0.8, seed=SEED)


def _key(index: int) -> bytes:
    return hashlib.sha256(f"synthetic-player-{index}".encode()).digest()


def test_same_key_and_seed_give_same_split_on_every_call():
    key = _key(1)

    results = {split_of(key, CONFIG) for _ in range(5)}

    assert len(results) == 1


def test_split_is_either_train_or_test():
    assert split_of(_key(2), CONFIG) in {"train", "test"}


def test_split_does_not_depend_on_input_order():
    keys = [_key(index) for index in range(200)]

    forward = split_players(keys, CONFIG)
    backward = split_players(list(reversed(keys)), CONFIG)

    assert forward == backward


def test_repeated_key_gets_one_split_value():
    key = _key(3)

    assignment = split_players([key, key, key], CONFIG)

    assert list(assignment) == [key]


def test_player_is_in_exactly_one_split():
    keys = [_key(index) for index in range(500)]

    assignment = split_players(keys, CONFIG)

    assert set(assignment) == set(keys)
    assert set(assignment.values()) <= {"train", "test"}


def test_ratio_is_roughly_respected_on_synthetic_players():
    keys = [_key(index) for index in range(10_000)]

    assignment = split_players(keys, CONFIG)
    train_share = sum(1 for value in assignment.values() if value == "train") / len(keys)

    assert 0.78 <= train_share <= 0.82, f"train share was {train_share:.3f}"


def test_changing_the_seed_changes_some_assignments():
    keys = [_key(index) for index in range(1_000)]
    other = SplitConfig(train_ratio=0.8, seed=SEED + 1)

    assert split_players(keys, CONFIG) != split_players(keys, other)


def test_extreme_ratio_puts_players_in_one_split_only():
    keys = [_key(index) for index in range(100)]

    assignment = split_players(keys, SplitConfig(train_ratio=0.999, seed=SEED))

    assert set(assignment.values()) == {"train"}


@pytest.mark.parametrize("bad_key", [b"", b"short", _key(1) + b"x", "not-bytes"])
def test_key_must_be_the_32_byte_player_key(bad_key):
    with pytest.raises(ValueError, match="player_key"):
        split_of(bad_key, CONFIG)


@pytest.mark.parametrize("ratio", [0.0, 1.0, -0.1, 1.5])
def test_ratio_must_be_strictly_between_zero_and_one(ratio):
    with pytest.raises(ValueError, match="train_ratio"):
        split_of(_key(1), SplitConfig(train_ratio=ratio, seed=SEED))
