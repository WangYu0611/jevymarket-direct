import asyncio
import gzip
import json
from types import SimpleNamespace

import pytest

import jevymarket.price_value_live_t120 as live
from jevymarket.price_value_forward import ValueDecision
from jevymarket.signal import JevView


def paper_trade(i, *, won=True):
    ask = .70
    size = 7.14
    return {
        "arm": "B_quant_jev_taker",
        "slug": f"m{i}",
        "checkpoint": 120,
        "signal_ts": i,
        "decision_ts": i + .1,
        "direction": "UP",
        "quant_p": .95,
        "jev_p": .84,
        "jev_answerable": .9,
        "jev_clarity": 3,
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
        "taker_fee_usd_est": 0,
        "up_won": int(won),
    }


def make_report(tmp_path, *, t120_count=23):
    wins = 20 if t120_count == 23 else 45
    rows = [paper_trade(i, won=i < wins) for i in range(t120_count)]
    payload = {
        "format": "v8.1-early-window-value-report-r1",
        "arms": {
            "B_quant_jev_taker": {
                "trades": t120_count,
                "settled": t120_count,
                "wins": wins,
                "losses": t120_count - wins,
                "win_rate": wins / t120_count,
                "net_pnl_estimated": 1.0,
                "net_roi_estimated": .01,
                "net_pnl_minus_top3_positive_contributions": 1.0,
            }
        },
        "trades": {"B_quant_jev_taker": rows},
    }
    path = tmp_path / "paper.json.gz"
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        json.dump(payload, handle)
    return path



def make_v82_report(tmp_path, *, settled=23, passed=False):
    metrics = {
        "trades": settled,
        "settled": settled,
        "pending": 0,
        "wins": max(0, settled - 3),
        "losses": min(3, settled),
        "win_rate": (settled - min(3, settled)) / settled if settled else None,
        "stake_usd": settled * 5.0,
        "gross_pnl_before_fee": 10.0,
        "estimated_taker_fee_usd": 1.0,
        "net_pnl_estimated": 9.0,
        "net_roi_estimated": .08,
        "net_pnl_minus_top3_positive_contributions": 2.0,
        "ask_mean": .70,
        "ask_median": .70,
        "edge_mean": .20,
        "edge_median": .20,
        "checkpoint_distribution": {"T-120": settled},
        "stress_plus_1tick": {
            "mode": "plus_1tick",
            "settled": settled,
            "net_pnl_estimated": 7.0,
            "positive": max(0, settled - 3),
        },
        "stress_plus_1cent": {
            "mode": "plus_1cent",
            "settled": settled,
            "net_pnl_estimated": 6.0,
            "positive": max(0, settled - 3),
        },
        "gate": {"checks": {}, "passed": passed},
    }
    payload = {
        "format": "v8.2-t120-main-shadow-report-r1",
        "primary_strategy": {
            "slot": 120,
            "arm": "B_quant_jev_taker",
            "metrics": metrics,
            "passed": passed,
        },
    }
    path = tmp_path / "v82.json.gz"
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        json.dump(payload, handle)
    return path

def decision(notional=4.99, ask=.70):
    return ValueDecision(
        direction="UP",
        probability=.95,
        bid=.69,
        ask=ask,
        spread=.01,
        depth_5c_usd=100,
        tick_size=.01,
        min_order_size=5,
        edge=.25,
        size=7.12,
        notional_usd=notional,
        taker_fee_usd_est=.10,
    )


def view():
    return JevView(
        p_yes=.84,
        answerable=.9,
        clarity=3,
        clarity_mean=None,
        clarity_confidence=None,
        model="jev-test",
        cost=0,
        raw={},
    )


def test_current_v81_t120_is_canary_ready_but_not_session_ready(tmp_path):
    gate = live.evaluate_live_gate(make_report(tmp_path, t120_count=23))
    assert gate["canary_ready"]
    assert not gate["session_ready"]
    assert gate["paper_b_t120"]["settled"] == 23


def test_session_requires_full_t120_paper_gate(tmp_path):
    gate = live.evaluate_live_gate(make_report(tmp_path, t120_count=50))
    assert gate["canary_ready"]
    assert gate["session_ready"]



def test_v82_primary_metrics_are_used_directly_for_live_gate(tmp_path):
    gate = live.evaluate_live_gate(make_v82_report(tmp_path, settled=23, passed=False))
    assert gate["report_generation"] == "v8.2"
    assert gate["canary_ready"]
    assert not gate["session_ready"]
    assert gate["paper_b_t120"]["settled"] == 23

    gate2 = live.evaluate_live_gate(make_v82_report(tmp_path, settled=50, passed=True))
    assert gate2["canary_ready"]
    assert gate2["session_ready"]

def test_market_order_is_fak_bounded_by_final_ask_and_five_dollars():
    kwargs = live.market_order_kwargs("token", decision())
    assert kwargs == {
        "token_id": "token",
        "side": "BUY",
        "amount": "4.99",
        "max_spend": "5.0",
        "max_price": "0.7",
        "order_type": "FAK",
    }
    with pytest.raises(ValueError):
        live.market_order_kwargs("token", decision(notional=5.01))


def test_live_store_reserves_only_once_per_market(tmp_path):
    store = live.LiveStore(tmp_path / "live.db")
    try:
        kwargs = dict(
            slug="m1",
            condition_id="c",
            token_id="t",
            direction="UP",
            quant_p=.95,
            jev_p=.84,
            ask=.70,
            edge=.25,
            notional=4.99,
        )
        assert store.reserve_live_intent(**kwargs)
        assert not store.reserve_live_intent(**kwargs)
        assert store.live_attempt_count() == 1
        assert store.planned_notional_total() == pytest.approx(4.99)
    finally:
        store.close()


def test_unresolved_live_intent_is_detected(tmp_path):
    store = live.LiveStore(tmp_path / "live.db")
    try:
        store.reserve_live_intent(
            slug="m1", condition_id="c", token_id="t", direction="UP",
            quant_p=.95, jev_p=.84, ask=.70, edge=.25, notional=4.99,
        )
        assert store.unresolved_live_rows()[0]["state"] == "intent"
        store.update_live("m1", "filled")
        assert store.unresolved_live_rows() == []
    finally:
        store.close()


def test_only_t120_b_hook_can_reach_live_path():
    calls = []

    class Runner:
        live_halted = False
        store = SimpleNamespace(record_live_event=lambda **kwargs: None)

        async def _place_fak(self, **kwargs):
            calls.append(kwargs)

    runner = Runner()
    method = live.LiveT120Runner.after_b_value_decision
    common = dict(
        self=runner,
        observation_id=1,
        slug="m1",
        original_direction="UP",
        view=view(),
        fresh_quant=.95,
        cand=SimpleNamespace(condition_id="c"),
        fresh=object(),
        decision=decision(),
        reason="value_ready",
    )
    asyncio.run(method(slot=110, **common))
    asyncio.run(method(slot=100, **common))
    assert calls == []

    asyncio.run(method(slot=120, **common))
    assert len(calls) == 1
    assert calls[0]["slug"] == "m1"
    assert calls[0]["direction"] == "UP"



def test_pre_submit_position_check_uses_authenticated_wallet_and_condition_id(monkeypatch):
    calls = []

    async def geo():
        return {"blocked": False, "country": "HK", "region": ""}

    class Page:
        items = []

    class Pager:
        async def first_page(self):
            return Page()

    class Balance:
        balance = "10000000"

    class Client:
        wallet = "0xwallet"

        async def get_balance_allowance(self, *, asset_type):
            assert asset_type == "COLLATERAL"
            return Balance()

        def list_positions(self, **kwargs):
            calls.append(("positions", kwargs))
            assert "market" not in kwargs
            assert "user" not in kwargs
            assert kwargs["condition_id"] == "condition-123"
            assert kwargs["status"] == "OPEN"
            return Pager()

        def list_open_orders(self, **kwargs):
            calls.append(("orders", kwargs))
            assert kwargs["market"] == "condition-123"
            return Pager()

    monkeypatch.setattr(live, "geoblock_check", geo)
    runner = SimpleNamespace(secure_client=Client())

    ok, reason = asyncio.run(
        live.LiveT120Runner._pre_submit_checks(
            runner, "btc-updown-5m-test", "condition-123"
        )
    )

    assert ok is True
    assert reason == "ok"
    assert calls[0][0] == "positions"
    assert calls[1][0] == "orders"


def test_pre_submit_position_check_error_exposes_class_only(monkeypatch):
    async def geo():
        return {"blocked": False, "country": "HK", "region": ""}

    class Balance:
        balance = "10000000"

    class Client:
        wallet = "0xwallet"

        async def get_balance_allowance(self, *, asset_type):
            return Balance()

        def list_positions(self, **kwargs):
            raise TypeError("secret-ish error text must not be surfaced")

    monkeypatch.setattr(live, "geoblock_check", geo)
    runner = SimpleNamespace(secure_client=Client())

    ok, reason = asyncio.run(
        live.LiveT120Runner._pre_submit_checks(
            runner, "btc-updown-5m-test", "condition-123"
        )
    )

    assert ok is False
    assert reason == "position_check_failed:TypeError"

def test_geoblock_failure_prevents_secure_client_creation(tmp_path, monkeypatch):
    report = make_report(tmp_path)
    args = SimpleNamespace(
        paper_report=report,
        mode="one",
        check_only=False,
        db=tmp_path / "unused.db",
        seconds=300,
    )
    created = False

    async def blocked():
        return {"blocked": True, "country": "XX", "region": ""}

    class Secure:
        @classmethod
        async def create(cls, **kwargs):
            nonlocal created
            created = True
            raise AssertionError("must not create secure client when blocked")

    monkeypatch.setattr(live, "geoblock_check", blocked)
    monkeypatch.setattr(live, "AsyncSecureClient", Secure)
    result = asyncio.run(live.async_main(args))
    assert result["termination"] == "geoblocked"
    assert not created


def test_geoblock_unverified_fails_closed(tmp_path, monkeypatch):
    report = make_report(tmp_path)
    args = SimpleNamespace(
        paper_report=report,
        mode="one",
        check_only=False,
        db=tmp_path / "unused.db",
        seconds=300,
    )

    async def broken():
        raise RuntimeError("network")

    monkeypatch.setattr(live, "geoblock_check", broken)
    result = asyncio.run(live.async_main(args))
    assert result["termination"] == "geoblock_unverified"


def test_check_only_still_never_places_order(tmp_path, monkeypatch):
    report = make_report(tmp_path)
    args = SimpleNamespace(
        paper_report=report,
        mode="check",
        check_only=True,
        db=tmp_path / "unused.db",
        seconds=300,
    )

    async def geo():
        return {"blocked": False, "country": "ZZ", "region": ""}

    monkeypatch.setattr(live, "geoblock_check", geo)
    monkeypatch.setattr(live, "_valid_private_key", lambda value: True)
    monkeypatch.setattr(
        live,
        "load_settings",
        lambda: SimpleNamespace(
            polymarket_private_key="x",
            polymarket_wallet=None,
            polymarket_builder_api_key="",
            polymarket_builder_secret="",
            polymarket_builder_passphrase="",
        ),
    )

    class Secure:
        wallet = "wallet"

        @classmethod
        async def create(cls, **kwargs):
            return cls()

        async def close(self):
            return None

        async def place_market_order(self, **kwargs):
            raise AssertionError("check-only must never place")

    async def preflight(client):
        return {
            "wallet_type": "test",
            "balance_usd": 50.0,
            "open_orders_present": False,
            "trading_approved": True,
        }

    monkeypatch.setattr(live, "AsyncSecureClient", Secure)
    monkeypatch.setattr(live, "account_preflight", preflight)
    result = asyncio.run(live.async_main(args))
    assert result["termination"] == "check_only_complete"



def test_live_event_log_persists_pre_submit_reason(tmp_path):
    store = live.LiveStore(tmp_path / "live-events.db")
    try:
        store.record_live_event(
            slug="m1",
            stage="final_refresh",
            outcome="skip",
            reason="final_value_invalidated:edge_below_threshold",
        )
        rows = store.live_events()
        assert len(rows) == 1
        assert rows[0]["slug"] == "m1"
        assert rows[0]["slot"] == 120
        assert rows[0]["stage"] == "final_refresh"
        assert rows[0]["outcome"] == "skip"
        assert rows[0]["reason"] == "final_value_invalidated:edge_below_threshold"
    finally:
        store.close()

def test_live_constants_match_user_requested_caps():
    assert live.LIVE_SLOT == 120
    assert live.MAX_ORDER_USD == 5.0
    assert live.MAX_SESSION_ORDERS == 5
    assert live.MAX_SESSION_NOTIONAL_USD == 25.0
