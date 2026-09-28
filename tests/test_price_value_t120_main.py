import gzip
import json
from types import SimpleNamespace

import pytest

from jevymarket.price_value_forward import ValueDecision
from jevymarket.price_value_t120_main import (
    PRIMARY_ARM,
    PRIMARY_SLOT,
    REPORT_FORMAT,
    REVISION,
    SHADOW_SLOT,
    V82Store,
    build_report,
    experiment_parameters,
    strategy_slot,
)


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


def decision(direction="UP", ask=.70, probability=.95):
    return ValueDecision(
        direction=direction,
        probability=probability,
        bid=ask - .01,
        ask=ask,
        spread=.01,
        depth_5c_usd=100,
        tick_size=.01,
        min_order_size=5,
        edge=probability - ask,
        size=7.14,
        notional_usd=4.998,
        taker_fee_usd_est=.10,
    )


@pytest.mark.parametrize(
    ("seconds_left", "slot"),
    [
        (121, None),
        (120, 120),
        (111, 120),
        (110, None),
        (105, None),
        (101, None),
        (100, 100),
        (91, 100),
        (90, None),
        (60, None),
    ],
)
def test_v82_only_samples_t120_and_t100(seconds_left, slot):
    assert strategy_slot(seconds_left) == slot


def test_manifest_makes_t120_b_primary_and_t100_c_shadow():
    p = experiment_parameters(settings(), 10.0, .07)
    assert p["primary_strategy"]["slot"] == 120
    assert p["primary_strategy"]["arm"] == "B_quant_jev_taker"
    assert p["shadow_research"]["retain_t100"] is True
    assert p["shadow_research"]["retain_c"] is True
    assert p["shadow_research"]["t110_retired"] is True
    assert p["shadow_research"]["affects_primary_gate"] is False
    assert p["min_edge"] == .08
    assert p["max_trade_price"] == .90
    assert p["max_usd_per_trade"] == 5.0


def test_same_market_keeps_t120_and_t100_independent_shadow_rows(tmp_path):
    store = V82Store(tmp_path / "v82.db")
    store.ensure_experiment(REVISION, experiment_parameters(settings(), 10.0, .07))
    try:
        for slot, obs in ((120, 1), (100, 2)):
            inserted = store.record_value_trade(
                version=REVISION,
                arm="B_quant_jev_taker",
                slug="same-market",
                checkpoint_value=slot,
                observation_id=obs,
                signal_ts=float(obs),
                decision_ts=float(obs) + .1,
                quant_p=.95,
                jev=None,
                market_mid=.695,
                decision=decision(),
                fee_rate=.07,
            )
            assert inserted

        rows = store.slot_trades(REVISION, "B_quant_jev_taker")
        assert [(r["slug"], r["slot"]) for r in rows] == [
            ("same-market", 120),
            ("same-market", 100),
        ]
        assert [r["checkpoint"] for r in rows] == [120, 100]

        # Legacy table deliberately still keeps only the first arm/market trade.
        assert len(store.value_trades(REVISION, "B_quant_jev_taker")) == 1
    finally:
        store.close()


def metric_row(i, *, arm, slot, won, ask=.70):
    return {
        "arm": arm,
        "slug": f"{arm}-{slot}-{i}",
        "slot": slot,
        "checkpoint": slot,
        "signal_ts": i,
        "decision_ts": i + .1,
        "direction": "UP",
        "quant_p": .95,
        "jev_p": .85 if arm != "A_quant_taker" else None,
        "jev_answerable": .9 if arm != "A_quant_taker" else None,
        "jev_clarity": 3 if arm != "A_quant_taker" else None,
        "market_mid": .695,
        "bid": ask - .01,
        "ask": ask,
        "spread": .01,
        "depth_5c_usd": 100,
        "tick_size": .01,
        "min_order_size": 5,
        "edge_probability": .95,
        "edge": .25,
        "size": 7.14,
        "notional_usd": 4.998,
        "taker_fee_rate": .07,
        "taker_fee_usd_est": .10,
        "up_won": int(won),
    }


class FakeStore:
    def __init__(self, trades):
        self._trades = trades

    def parameters(self, version):
        assert version == REVISION
        return {"protocol_revision": REVISION}

    def slot_trades(self, version, arm=None, slot=None):
        assert version == REVISION
        rows = list(self._trades.get(arm, []))
        if slot is not None:
            rows = [r for r in rows if r["slot"] == slot]
        return rows

    def value_events(self, version):
        assert version == REVISION
        return []

    def observations(self, version):
        assert version == REVISION
        return []


def test_shadow_success_cannot_make_primary_pass():
    trades = {
        "A_quant_taker": [],
        "B_quant_jev_taker": [
            metric_row(i, arm=PRIMARY_ARM, slot=SHADOW_SLOT, won=True)
            for i in range(60)
        ],
        "C_jev_value": [
            metric_row(i, arm="C_jev_value", slot=PRIMARY_SLOT, won=True)
            for i in range(60)
        ],
    }
    report = build_report(FakeStore(trades), REVISION)
    assert report["format"] == REPORT_FORMAT
    assert report["primary_strategy"]["metrics"]["settled"] == 0
    assert report["primary_strategy"]["passed"] is False
    assert report["price_value_gate_passed"] is False
    assert report["shadow_research"]["metrics"]["t100_quant_jev_b"]["settled"] == 60
    assert report["shadow_research"]["metrics"]["t120_jev_own_edge_c"]["settled"] == 60


def test_primary_pass_ignores_bad_t100_and_bad_c():
    primary = [
        metric_row(i, arm=PRIMARY_ARM, slot=PRIMARY_SLOT, won=i < 47)
        for i in range(50)
    ]
    bad_t100 = [
        metric_row(i, arm=PRIMARY_ARM, slot=SHADOW_SLOT, won=False)
        for i in range(50)
    ]
    bad_c = [
        metric_row(i, arm="C_jev_value", slot=PRIMARY_SLOT, won=False)
        for i in range(50)
    ]
    report = build_report(FakeStore({
        "A_quant_taker": [],
        "B_quant_jev_taker": primary + bad_t100,
        "C_jev_value": bad_c,
    }), REVISION)
    main = report["primary_strategy"]["metrics"]
    assert main["settled"] == 50
    assert main["wins"] == 47
    assert main["gate"]["passed"]
    assert report["price_value_gate_passed"] is True
    assert report["shadow_research"]["metrics"]["t100_quant_jev_b"]["wins"] == 0
    assert report["shadow_research"]["metrics"]["t120_jev_own_edge_c"]["wins"] == 0


def test_report_exports_slot_tag_for_research_rows():
    row = metric_row(1, arm="C_jev_value", slot=100, won=True)
    report = build_report(FakeStore({
        "A_quant_taker": [],
        "B_quant_jev_taker": [],
        "C_jev_value": [row],
    }), REVISION)
    exported = report["trades"]["C_jev_value"][0]
    assert exported["slot"] == 100
    assert "checkpoint" not in exported
    assert report["shadow_research"]["affects_primary_gate"] is False


def test_v82_report_can_be_serialized_without_internal_payload():
    report = build_report(FakeStore({
        "A_quant_taker": [],
        "B_quant_jev_taker": [],
        "C_jev_value": [],
    }), REVISION)
    encoded = json.dumps(report)
    assert "payload" not in encoded
    assert "raw_secret" not in encoded
