import json

import pytest

from jevymarket.checkpoint_jev_forward import (
    ARMS,
    CHECKPOINTS,
    QUANT_CONFIDENCE,
    arm_direction,
    arm_metrics,
    build_report,
    checkpoint,
    first_signals,
    probability_direction,
    quant_direction,
    wilson_95,
)
from jevymarket.fast_store import FastStore
from jevymarket.signal import JevView


def params():
    return {
        "interval_seconds": 10.0,
        "min_answerable": .70,
        "min_clarity": 2,
    }


@pytest.mark.parametrize(
    ("seconds_left", "expected"),
    [(120, 120), (111, 120), (110, None), (90, 90), (81, 90), (80, None),
     (60, 60), (51, 60), (50, None), (45, 45), (36, 45), (35, None)],
)
def test_checkpoint_windows(seconds_left, expected):
    assert checkpoint(seconds_left) == expected


def test_direction_rules_are_explicit():
    assert QUANT_CONFIDENCE == .92
    assert quant_direction(.92) == "UP"
    assert quant_direction(.08) == "DOWN"
    assert quant_direction(.919) is None
    assert probability_direction(.6) == "UP"
    assert probability_direction(.4) == "DOWN"
    assert probability_direction(.5) is None


def row(slug, ts, cp, quant=.95, jev=.8, market=.7, answerable=.9, clarity=3, status="ok", up=1):
    return {
        "id": int(ts * 10),
        "slug": slug,
        "ts": ts,
        "checkpoint": cp,
        "seconds_left": cp,
        "quant_p": quant,
        "jev_p": jev,
        "market_p": market,
        "jev_status": status,
        "jev_answerable": answerable,
        "jev_clarity": clarity,
        "jev_requested_ts": ts + .1,
        "jev_received_ts": ts + 1.1,
        "up_won": up,
    }


def test_arms_are_nested_filters_not_different_directions():
    r = row("m", 1, 120)
    assert arm_direction(r, "A_quant", params()) == "UP"
    assert arm_direction(r, "B_quant_jev", params()) == "UP"
    assert arm_direction(r, "C_quant_jev_market", params()) == "UP"

    jev_disagrees = r | {"jev_p": .2}
    assert arm_direction(jev_disagrees, "A_quant", params()) == "UP"
    assert arm_direction(jev_disagrees, "B_quant_jev", params()) is None

    market_disagrees = r | {"market_p": .3}
    assert arm_direction(market_disagrees, "B_quant_jev", params()) == "UP"
    assert arm_direction(market_disagrees, "C_quant_jev_market", params()) is None


def test_first_signal_is_one_independent_market_only():
    rows = [
        row("m1", 1, 120, up=1),
        row("m1", 2, 90, up=1),
        row("m2", 3, 120, quant=.70, up=0),
        row("m2", 4, 90, quant=.05, jev=.1, market=.2, up=0),
    ]
    a = first_signals(rows, "A_quant", params())
    assert [(x["slug"], x["checkpoint"], x["signal_direction"]) for x in a] == [
        ("m1", 120, "UP"),
        ("m2", 90, "DOWN"),
    ]


def test_arm_metrics_gate_needs_50_and_strictly_over_65_percent():
    signals = [row(f"m{i}", i + 1, 120, up=1 if i < 33 else 0) | {"signal_direction": "UP"} for i in range(50)]
    result = arm_metrics(signals, 50)
    assert result["resolved_signal_markets"] == 50
    assert result["wins"] == 33
    assert result["win_rate"] == pytest.approx(.66)
    assert result["gate"]["passed"]

    exactly_65 = [row(f"x{i}", i + 1, 120, up=1 if i < 39 else 0) | {"signal_direction": "UP"} for i in range(60)]
    result = arm_metrics(exactly_65, 60)
    assert result["win_rate"] == pytest.approx(.65)
    assert not result["gate"]["passed"]


def test_wilson_interval_is_bounded():
    lo, hi = wilson_95(33, 50)
    assert 0 <= lo < .66 < hi <= 1
    assert wilson_95(0, 0) == (None, None)


def test_build_report_uses_same_forward_rows_and_exports_no_raw_jev(tmp_path):
    db = tmp_path / "forward.db"
    store = FastStore(db)
    version = "test-forward"
    manifest = params() | {
        "protocol_revision": "test",
        "checkpoints": list(CHECKPOINTS),
        "checkpoint_lateness_seconds": 10,
        "quant_confidence": .92,
        "jev_mode": "high_confidence_checkpoint_confirmation",
        "jev_model": "jev-test",
        "jev_timeout_seconds": 30,
        "jev_max_retries": 0,
        "short_term_min_history_seconds": 180,
        "short_term_max_sample_age_seconds": 5,
        "max_spread": .06,
        "paper_only": True,
        "orders_enabled": False,
    }
    store.ensure_experiment(version, manifest)

    try:
        for i in range(2):
            slug = f"btc-updown-5m-{1800000000 + i * 300}"
            observation_id, cp = store.record(
                version=version,
                session="s",
                slug=slug,
                condition_id="c",
                ts=100 + i,
                seconds_left=120,
                checkpoint=120,
                quant_p=.96 if i == 0 else .04,
                market_p=.7 if i == 0 else .3,
                yes_ask=.7,
                no_ask=.3,
                status="prediction_ready",
                reason="test",
                payload={"secret_should_not_export": "x"},
            )
            assert cp == 120
            store.complete_jev(
                observation_id,
                JevView(
                    p_yes=.8 if i == 0 else .2,
                    answerable=.9,
                    clarity=3,
                    clarity_mean=None,
                    clarity_confidence=None,
                    model="jev-test",
                    cost=1,
                    raw={"private_raw_payload": "do not export"},
                ),
                received_ts=102 + i,
            )
            store.put_market_result(
                slug=slug,
                condition_id="c",
                timeframe="5m",
                winner="UP" if i == 0 else "DOWN",
                up_won=i == 0,
                up_final_price=1.0 if i == 0 else 0.0,
                down_final_price=0.0 if i == 0 else 1.0,
                source="test",
            )

        report = build_report(store, version)
        assert report["orders_created"] == 0
        assert report["arms"]["A_quant"]["wins"] == 2
        assert report["arms"]["B_quant_jev"]["wins"] == 2
        assert report["arms"]["C_quant_jev_market"]["wins"] == 2
        assert report["coverage"]["jev_success"] == 2
        encoded = json.dumps(report)
        assert "private_raw_payload" not in encoded
        assert "secret_should_not_export" not in encoded
        assert set(report["arms"]) == set(ARMS)
    finally:
        store.close()
