from jevymarket.evaluation import checkpoint_for


def test_checkpoint_for_5m():
    assert checkpoint_for("5m", 238, tolerance_seconds=20) == 240
    assert checkpoint_for("5m", 181, tolerance_seconds=20) == 180
    assert checkpoint_for("5m", 149, tolerance_seconds=20) is None


def test_checkpoint_for_other_timeframes():
    assert checkpoint_for("15m", 606, tolerance_seconds=20) == 600
    assert checkpoint_for("1h", 1790, tolerance_seconds=20) == 1800
    assert checkpoint_for("1h", 1400, tolerance_seconds=20) is None
