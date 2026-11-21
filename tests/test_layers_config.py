import pytest

from lake.layers.config import (
    CheckConfig,
    KeptTable,
    load_layers_config,
    parse_layers_config,
)


def _raw(**overrides):
    raw = {
        "window": {"size_packets": 256},
        "split": {"train_ratio": 0.8, "seed": 20261121},
        "checks": [{"id": "dup.rate", "blocking": False, "threshold": 0.05}],
        "kept_tables": [{"name": "labels.sessions", "retention_days": None}],
    }
    raw.update(overrides)
    return raw


def _with_split(**values):
    split = {"train_ratio": 0.8, "seed": 1}
    split.update(values)
    return _raw(split=split)


def _with_window(size):
    return _raw(window={"size_packets": size})


def test_real_layers_yaml_loads_with_decided_values():
    config = load_layers_config()

    assert config.window.size_packets == 256
    assert config.split.train_ratio == 0.8
    assert config.split.seed == 20261121
    assert CheckConfig(id="schema.capture_raw", blocking=True, threshold=None) in config.checks
    assert CheckConfig(id="rows.baseline", blocking=True, threshold=0.5) in config.checks
    assert CheckConfig(id="dup.rate", blocking=False, threshold=0.05) in config.checks
    assert KeptTable(name="labels.sessions", retention_days=None) in config.kept_tables


def test_accepts_minimal_valid_config():
    config = parse_layers_config(_raw())

    assert config.checks == (CheckConfig(id="dup.rate", blocking=False, threshold=0.05),)
    assert config.kept_tables == (KeptTable(name="labels.sessions", retention_days=None),)


def test_rejects_unknown_top_level_key():
    with pytest.raises(ValueError, match="unknown key.*schedule"):
        parse_layers_config(_raw(schedule="daily"))


def test_rejects_missing_window_section():
    raw = _raw()
    del raw["window"]

    with pytest.raises(ValueError, match="config.window is required"):
        parse_layers_config(raw)


@pytest.mark.parametrize("size", [256, 384, 512])
def test_accepts_window_size_inside_range(size):
    assert parse_layers_config(_with_window(size)).window.size_packets == size


@pytest.mark.parametrize("size", [255, 0, 513, 4096])
def test_rejects_window_size_outside_range(size):
    with pytest.raises(ValueError, match="size_packets.*256.*512"):
        parse_layers_config(_with_window(size))


def test_rejects_boolean_window_size():
    with pytest.raises(ValueError, match="size_packets"):
        parse_layers_config(_with_window(True))


def test_rejects_unknown_window_key():
    with pytest.raises(ValueError, match="unknown key.*stride"):
        parse_layers_config(_raw(window={"size_packets": 256, "stride": 8}))


@pytest.mark.parametrize("ratio", [0, 0.0, 1, 1.0, 1.5, -0.2])
def test_rejects_train_ratio_outside_open_unit_interval(ratio):
    with pytest.raises(ValueError, match="train_ratio"):
        parse_layers_config(_with_split(train_ratio=ratio))


@pytest.mark.parametrize("ratio", ["0.8", True, None])
def test_rejects_train_ratio_that_is_not_a_number(ratio):
    with pytest.raises(ValueError, match="train_ratio"):
        parse_layers_config(_with_split(train_ratio=ratio))


def test_rejects_negative_seed():
    with pytest.raises(ValueError, match="seed"):
        parse_layers_config(_with_split(seed=-1))


def test_accepts_zero_seed():
    assert parse_layers_config(_with_split(seed=0)).split.seed == 0


def test_rejects_boolean_seed():
    with pytest.raises(ValueError, match="seed"):
        parse_layers_config(_with_split(seed=False))


def test_rejects_unknown_check_key():
    raw = _raw(checks=[{"id": "dup.rate", "blocking": False, "severity": "high"}])

    with pytest.raises(ValueError, match="unknown key.*severity"):
        parse_layers_config(raw)


@pytest.mark.parametrize("check_id", ["DupRate", "dup", "dup.rate; DROP", "dup..rate", ""])
def test_rejects_check_id_that_is_not_a_dotted_identifier(check_id):
    raw = _raw(checks=[{"id": check_id, "blocking": False}])

    with pytest.raises(ValueError, match="identifier"):
        parse_layers_config(raw)


def test_rejects_duplicate_check_id():
    raw = _raw(
        checks=[
            {"id": "dup.rate", "blocking": False, "threshold": 0.05},
            {"id": "dup.rate", "blocking": True, "threshold": 0.01},
        ]
    )

    with pytest.raises(ValueError, match="duplicate check 'dup.rate'"):
        parse_layers_config(raw)


@pytest.mark.parametrize("flag", ["yes", 1, None])
def test_rejects_blocking_flag_that_is_not_boolean(flag):
    raw = _raw(checks=[{"id": "dup.rate", "blocking": flag}])

    with pytest.raises(ValueError, match="blocking"):
        parse_layers_config(raw)


def test_rejects_missing_blocking_flag():
    raw = _raw(checks=[{"id": "dup.rate", "threshold": 0.05}])

    with pytest.raises(ValueError, match="blocking is required"):
        parse_layers_config(raw)


def test_accepts_null_threshold_for_check_without_limit():
    raw = _raw(checks=[{"id": "key.nulls", "blocking": True, "threshold": None}])

    assert parse_layers_config(raw).checks == (
        CheckConfig(id="key.nulls", blocking=True, threshold=None),
    )


@pytest.mark.parametrize("threshold", [-0.1, "0.05", True])
def test_rejects_threshold_that_is_not_a_non_negative_number(threshold):
    raw = _raw(checks=[{"id": "dup.rate", "blocking": False, "threshold": threshold}])

    with pytest.raises(ValueError, match="threshold"):
        parse_layers_config(raw)


def test_rejects_kept_table_without_namespace():
    raw = _raw(kept_tables=[{"name": "sessions", "retention_days": None}])

    with pytest.raises(ValueError, match="namespace.table"):
        parse_layers_config(raw)


@pytest.mark.parametrize("retention", [0, -5, True])
def test_rejects_kept_table_retention_that_is_not_a_positive_integer(retention):
    raw = _raw(kept_tables=[{"name": "labels.sessions", "retention_days": retention}])

    with pytest.raises(ValueError, match="retention_days"):
        parse_layers_config(raw)


def test_rejects_duplicate_kept_table():
    raw = _raw(
        kept_tables=[
            {"name": "labels.sessions", "retention_days": None},
            {"name": "labels.sessions", "retention_days": None},
        ]
    )

    with pytest.raises(ValueError, match="duplicate table 'labels.sessions'"):
        parse_layers_config(raw)
