import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from jevymarket.price_value_forward import (
    ARMS,
    CRYPTO_TAKER_FEE_RATE,
    MIN_RESOLVED_TRADES,
    REVISION,
    V8Store,
    arm_metrics,
    build_report,
    checkpoint,
    experiment_parameters,
    jev_quality,
    probability_direction,
    quant_direction,
    resolve_paths,
    side_probability,
    taker_fee_usd,
    value_decision,
)
from jevymarket.signal import Book, JevView


def settings(**overrides):
    data = dict(
        max_spread=.06,
        min_trade_price=.10,
        max_trade_price=.90,
        min_edge=.08,
        max_usd_per_trade=5.0,
        min_answerable=.70,
        min_clarity=2,
        jev_model="jev-test",
        jev_timeout_seconds=30.0,
        jev_max_retries=0,
    )
    data.update(overrides)
    return SimpleNamespace(**data)


def book(
    *,
    yes_bid=.69,
    yes_ask=.70,
    no_bid=.29,
    no_ask=.30,
    yes_depth=100.0,
    no_depth=100.0,
    tick=.01,
    minimum=5.0,
):
    return Book(
        yes_token_id="1",
        no_token_id="2",
        yes_bid=yes_bid,
        yes_ask=yes_ask,
        no_bid=no_bid,
        no_ask=no_ask,
        tick_size=tick,
        min_order_size=minimum,
        yes_label="UP",
        no_label="DOWN",
        yes_bid_depth_5c_usd=100,
        yes_ask_depth_5c_usd=yes_depth,
        no_bid_depth_5c_usd=100,
        no_ask_depth_5c_usd=no_depth,
    )


def view(p=.8, answerable=.9, clarity=3):
    return JevView(
        p_yes=p,
        answerable=answerable,
        clarity=clarity,
        clarity_mean=None,
        clarity_confidence=None,
        model="jev-test",
        cost=0,
        raw={"raw_secret": "not exported"},
    )


@pytest.mark.parametrize(
    ("left", "expected"),
    [(120, 120), (111, 120), (110, None), (90, 90), (81, 90), (80, None),
     (60, 60), (51, 60), (50, None), (45, 45), (36, 45), (35, None)],
)
def test_checkpoint_windows(left, expected):
    assert checkpoint(left) == expected


def test_direction_thresholds_are_symmetric():
    assert quant_direction(.92) == "UP"
    assert quant_direction(.08) == "DOWN"
    assert quant_direction(.919) is None
    assert probability_direction(.51) == "UP"
    assert probability_direction(.49) == "DOWN"
    assert probability_direction(.5) is None
    assert side_probability(.04, "DOWN") == pytest.approx(.96)


def test_crypto_taker_fee_formula_and_rounding():
    assert CRYPTO_TAKER_FEE_RATE == .07
    assert taker_fee_usd(100, .5) == 1.75
    assert taker_fee_usd(10, .9) == .063


def test_value_decision_uses_real_predicted_side_ask_for_up_and_down():
    s = settings()
    up, reason = value_decision(.95, "UP", book(), s)
    assert reason == "value_ready"
    assert up is not None
    assert up.ask == .70
    assert up.probability == .95
    assert up.edge == pytest.approx(.25)
    assert up.notional_usd <= 5
    assert up.taker_fee_usd_est > 0

    down, reason = value_decision(.05, "DOWN", book(), s)
    assert reason == "value_ready"
    assert down is not None
    assert down.ask == .30
    assert down.probability == pytest.approx(.95)
    assert down.edge == pytest.approx(.65)


@pytest.mark.parametrize(
    ("b", "s", "p", "direction", "reason"),
    [
        (book(yes_bid=.80, yes_ask=.91), settings(), .99, "UP", "spread_too_wide"),
        (book(yes_bid=.89, yes_ask=.91), settings(), .99, "UP", "ask_outside_price_band"),
        (book(yes_bid=.84, yes_ask=.85), settings(), .92, "UP", "edge_below_threshold"),
        (book(yes_depth=1), settings(), .99, "UP", "insufficient_5c_ask_depth"),
        (book(minimum=10), settings(), .99, "UP", "minimum_size_exceeds_budget"),
    ],
)
def test_value_filters_fail_closed(b, s, p, direction, reason):
    result, observed = value_decision(p, direction, b, s)
    assert result is None
    assert observed == reason


def test_jev_quality_gate_is_unchanged():
    s = settings()
    assert jev_quality(view(), s)
    assert not jev_quality(view(answerable=.69), s)
    assert not jev_quality(view(clarity=1), s)


def sample_trade(i: int, *, won=True, ask=.70, edge=.20, arm="A_quant_taker", size=7.14):
    direction = "UP"
    up_won = 1 if won else 0
    fee = taker_fee_usd(size, ask)
    return {
        "id": i,
        "version": REVISION,
        "arm": arm,
        "slug": f"m{i}",
        "checkpoint": 120,
        "observation_id": i,
        "signal_ts": 100 + i,
        "decision_ts": 101 + i,
        "direction": direction,
        "quant_p": .95,
        "jev_p": .85,
        "jev_answerable": .9,
        "jev_clarity": 3,
        "market_mid": .695,
        "bid": ask - .01,
        "ask": ask,
        "spread": .01,
        "depth_5c_usd": 100,
        "tick_size": .01,
        "min_order_size": 5,
        "edge_probability": ask + edge,
        "edge": edge,
        "size": size,
        "notional_usd": size * ask,
        "taker_fee_rate": .07,
        "taker_fee_usd_est": fee,
        "up_won": up_won,
    }


def test_profit_gate_needs_50_positive_net_and_stress():
    rows = [sample_trade(i, won=i < 47, ask=.70) for i in range(50)]
    metrics = arm_metrics(rows)
    assert metrics["settled"] == MIN_RESOLVED_TRADES
    assert metrics["wins"] == 47
    assert metrics["win_rate"] == pytest.approx(.94)
    assert metrics["net_pnl_estimated"] > 0
    assert metrics["stress_plus_1tick"]["net_pnl_estimated"] > 0
    assert metrics["gate"]["passed"]

    short = arm_metrics(rows[:49])
    assert not short["gate"]["passed"]
    assert not short["gate"]["checks"]["minimum_50_settled_trades"]


def test_top3_concentration_guard_can_fail_even_with_positive_total():
    rows = [sample_trade(i, won=False, ask=.90, size=.10) for i in range(47)]
    rows += [sample_trade(100 + i, won=True, ask=.10, size=7.14) for i in range(3)]
    metrics = arm_metrics(rows)
    assert metrics["net_pnl_estimated"] > 0
    assert metrics["net_pnl_minus_top3_positive_contributions"] < 0
    assert not metrics["gate"]["passed"]


def test_store_one_trade_per_arm_market_and_report_excludes_raw_payload(tmp_path):
    db = tmp_path / "v8.db"
    store = V8Store(db)
    s = settings()
    manifest = experiment_parameters(s, 10.0, .07)
    store.ensure_experiment(REVISION, manifest)
    try:
        obs, cp = store.record(
            version=REVISION, session="s", slug="btc-updown-5m-1800000000",
            condition_id="c", ts=100, seconds_left=120, checkpoint=120,
            quant_p=.95, market_p=.695, yes_ask=.70, no_ask=.30,
            status="prediction_ready", reason="test",
            payload={"secret_payload": "do not export"},
        )
        assert cp == 120
        decision, reason = value_decision(.95, "UP", book(), s)
        assert reason == "value_ready" and decision is not None
        assert store.record_value_trade(
            version=REVISION, arm="A_quant_taker", slug="btc-updown-5m-1800000000",
            checkpoint_value=120, observation_id=obs, signal_ts=100, decision_ts=101,
            quant_p=.95, jev=None, market_mid=.695, decision=decision, fee_rate=.07,
        )
        assert not store.record_value_trade(
            version=REVISION, arm="A_quant_taker", slug="btc-updown-5m-1800000000",
            checkpoint_value=90, observation_id=obs, signal_ts=110, decision_ts=111,
            quant_p=.96, jev=None, market_mid=.70, decision=decision, fee_rate=.07,
        )
        store.put_market_result(
            slug="btc-updown-5m-1800000000", condition_id="c", timeframe="5m",
            winner="UP", up_won=True, up_final_price=1, down_final_price=0, source="test",
        )
        report = build_report(store, REVISION)
        assert report["arms"]["A_quant_taker"]["trades"] == 1
        assert report["arms"]["A_quant_taker"]["wins"] == 1
        encoded = json.dumps(report)
        assert "secret_payload" not in encoded
        assert "raw_secret" not in encoded
        assert set(report["arms"]) == set(ARMS)
    finally:
        store.close()


def test_resume_paths_are_explicit(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    db = tmp_path / "runs" / "old.db"
    db.parent.mkdir()
    db.write_bytes(b"existing")
    resolved, out, resumed = resolve_paths(None, db, None, "STAMP")
    assert resolved == db
    assert out == Path("runs") / "v8_price_value_forward_resume_STAMP.json.gz"
    assert resumed is True
    with pytest.raises(FileExistsError):
        resolve_paths(db, None, None, "STAMP")
