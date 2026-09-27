import asyncio
import gzip
import json
from types import SimpleNamespace

import pytest

import jevymarket.price_value_live_t120 as live
from jevymarket.price_value_forward import ValueDecision
from jevymarket.signal import JevView


def make_report(tmp_path, *, gate_passed=False):
    payload = {
        "format": "v8.1-early-window-value-report-r1",
        "arms": {
            "B_quant_jev_taker": {
                "trades": 31,
                "settled": 31,
                "wins": 25,
                "losses": 6,
                "win_rate": 25 / 31,
                "net_pnl_estimated": 9.95,
                "net_roi_estimated": .064,
                "net_pnl_minus_top3_positive_contributions": -6.67,
                "stress_plus_1tick": {"net_pnl_estimated": 7.84},
                "gate": {"passed": gate_passed},
            }
        },
    }
    path = tmp_path / "paper.json.gz"
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


def test_current_v81_is_canary_ready_but_not_session_ready(tmp_path):
    gate = live.evaluate_live_gate(make_report(tmp_path, gate_passed=False))
    assert gate["canary_ready"]
    assert not gate["session_ready"]
    assert gate["paper_b"]["settled"] == 31


def test_session_requires_full_paper_gate(tmp_path):
    gate = live.evaluate_live_gate(make_report(tmp_path, gate_passed=True))
    assert gate["canary_ready"]
    assert gate["session_ready"]


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
        lambda: SimpleNamespace(polymarket_private_key="x", polymarket_wallet=None),
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


def test_live_constants_match_user_requested_caps():
    assert live.LIVE_SLOT == 120
    assert live.MAX_ORDER_USD == 5.0
    assert live.MAX_SESSION_ORDERS == 24
    assert live.MAX_SESSION_NOTIONAL_USD == 120.0
