import json
from types import SimpleNamespace

import pytest

from jevymarket.price_value_early_forward import (
    EARLY_SLOTS,
    EARLY_START_SECONDS,
    EARLY_END_SECONDS,
    REVISION,
    EarlyStore,
    build_report,
    early_arm_metrics,
    early_slot,
    experiment_parameters,
    rejection_summary,
)
from jevymarket.price_value_forward import taker_fee_usd


def settings():
    return SimpleNamespace(
        min_edge=.08,
        min_trade_price=.10,
        max_trade_price=.90,
        max_spread=.06,
        max_usd_per_trade=5.0,
        min_answerable=.70,
        min_clarity=2,
    )


@pytest.mark.parametrize(
    ("seconds_left", "slot"),
    [
        (121, None),
        (120, 120),
        (111, 120),
        (110, 110),
        (101, 110),
        (100, 100),
        (91, 100),
        (90, None),
        (89, None),
        (60, None),
        (45, None),
    ],
)
def test_early_window_is_continuous_from_120_until_before_90(seconds_left, slot):
    assert early_slot(seconds_left) == slot


def test_protocol_keeps_v8_value_rules_and_changes_window_only():
    p = experiment_parameters(settings(), 10.0, .07)
    assert p["primary_window_seconds"] == {"start": 120, "end_exclusive": 90}
    assert p["slots"] == [120, 110, 100]
    assert p["quant_confidence"] == .92
    assert p["min_edge"] == .08
    assert p["max_trade_price"] == .90
    assert p["max_spread"] == .06
    assert p["max_usd_per_trade"] == 5.0
    assert p["fresh_validation_after_v8"] is True


def test_failed_read_style_observation_does_not_consume_later_slot(tmp_path):
    store = EarlyStore(tmp_path / "v81.db")
    store.ensure_experiment(REVISION, experiment_parameters(settings(), 10.0, .07))
    try:
        first, cp1 = store.record(
            version=REVISION,
            session="s",
            slug="btc-updown-5m-1800000000",
            condition_id=None,
            ts=1,
            seconds_left=118,
            checkpoint=None,
            quant_p=None,
            market_p=None,
            yes_ask=None,
            no_ask=None,
            status="unavailable",
            reason="read_timeout",
            payload={},
        )
        second, cp2 = store.record(
            version=REVISION,
            session="s",
            slug="btc-updown-5m-1800000000",
            condition_id="c",
            ts=2,
            seconds_left=115,
            checkpoint=120,
            quant_p=.95,
            market_p=.70,
            yes_ask=.71,
            no_ask=.29,
            status="prediction_ready",
            reason="ok",
            payload={},
        )
        assert first != second
        assert cp1 is None
        assert cp2 == 120
    finally:
        store.close()


def test_value_event_is_unique_per_arm_market_slot(tmp_path):
    store = EarlyStore(tmp_path / "events.db")
    try:
        kwargs = dict(
            version=REVISION,
            arm="A_quant_taker",
            slug="m1",
            slot=120,
            accepted=False,
            reason="edge_below_threshold",
            ts=1,
        )
        store.record_value_event(**kwargs)
        store.record_value_event(**(kwargs | {"reason": "different_reason", "ts": 2}))
        rows = store.value_events(REVISION)
        assert len(rows) == 1
        assert rows[0]["reason"] == "edge_below_threshold"
    finally:
        store.close()


def test_rejection_summary_surfaces_why_trades_are_sparse():
    events = [
        {"arm": "A_quant_taker", "accepted": 0, "reason": "ask_outside_price_band"},
        {"arm": "A_quant_taker", "accepted": 0, "reason": "ask_outside_price_band"},
        {"arm": "A_quant_taker", "accepted": 0, "reason": "edge_below_threshold"},
        {"arm": "A_quant_taker", "accepted": 1, "reason": "value_ready"},
    ]
    summary = rejection_summary(events)
    assert summary["A_quant_taker"]["evaluations"] == 4
    assert summary["A_quant_taker"]["accepted_events"] == 1
    assert summary["A_quant_taker"]["top_rejections"] == {
        "ask_outside_price_band": 2,
        "edge_below_threshold": 1,
    }


def sample_trade(i, *, slot, won=True):
    ask = .70
    size = 7.14
    return {
        "id": i,
        "version": REVISION,
        "arm": "A_quant_taker",
        "slug": f"m{i}",
        "checkpoint": slot,
        "observation_id": i,
        "signal_ts": i,
        "decision_ts": i + .1,
        "direction": "UP",
        "quant_p": .95,
        "jev_p": None,
        "jev_answerable": None,
        "jev_clarity": None,
        "market_mid": .695,
        "bid": .69,
        "ask": ask,
        "spread": .01,
        "depth_5c_usd": 100,
        "tick_size": .01,
        "min_order_size": 5,
        "edge_probability": .95,
        "edge": .25,
        "size": size,
        "notional_usd": size * ask,
        "taker_fee_rate": .07,
        "taker_fee_usd_est": taker_fee_usd(size, ask),
        "up_won": int(won),
    }


def test_early_metrics_report_only_new_entry_slots():
    rows = [
        sample_trade(i, slot=EARLY_SLOTS[i % len(EARLY_SLOTS)], won=i < 47)
        for i in range(50)
    ]
    metrics = early_arm_metrics(rows)
    assert "checkpoint_distribution" not in metrics
    assert set(metrics["entry_slot_distribution"]) == {"T-120", "T-110", "T-100"}
    assert sum(metrics["entry_slot_distribution"].values()) == 50
    assert metrics["gate"]["passed"]


def test_build_report_marks_v81_as_fresh_and_exports_no_payload(tmp_path):
    db = tmp_path / "report.db"
    store = EarlyStore(db)
    store.ensure_experiment(REVISION, experiment_parameters(settings(), 10.0, .07))
    try:
        store.record(
            version=REVISION,
            session="s",
            slug="btc-updown-5m-1800000000",
            condition_id="c",
            ts=1,
            seconds_left=115,
            checkpoint=120,
            quant_p=.95,
            market_p=.70,
            yes_ask=.71,
            no_ask=.29,
            status="prediction_ready",
            reason="test",
            payload={"do_not_export": "secret"},
        )
        store.record_value_event(
            version=REVISION,
            arm="A_quant_taker",
            slug="btc-updown-5m-1800000000",
            slot=120,
            accepted=False,
            reason="edge_below_threshold",
        )
        report = build_report(store, REVISION)
        assert report["primary_window"]["start_seconds"] == EARLY_START_SECONDS
        assert report["primary_window"]["end_seconds_exclusive"] == EARLY_END_SECONDS
        assert report["primary_window"]["slots"] == list(EARLY_SLOTS)
        assert report["protocol"]["fresh_validation_after_v8"] is True
        assert report["coverage"]["early_slot_observations"] == 1
        assert "do_not_export" not in json.dumps(report)
    finally:
        store.close()
