import os
from pathlib import Path
from types import SimpleNamespace

import pytest

import jevymarket.price_value_live_t120 as v82live
import jevymarket.v83_live_runtime as runtime
from jevymarket.price_value_live_v83 import (
    patched_v83_runtime,
    resolve_paper_report,
)
from jevymarket.signal import Book


def settings():
    return SimpleNamespace(
        max_spread=0.06,
        min_trade_price=0.10,
        max_trade_price=0.90,
        min_edge=0.08,
        max_usd_per_trade=5.0,
    )


def book(*, yes_bid, yes_ask, no_bid, no_ask, minimum=5.0):
    return Book(
        yes_token_id="yes",
        no_token_id="no",
        yes_bid=yes_bid,
        yes_ask=yes_ask,
        no_bid=no_bid,
        no_ask=no_ask,
        tick_size=0.01,
        min_order_size=minimum,
        yes_label="UP",
        no_label="DOWN",
        yes_bid_depth_5c_usd=100.0,
        yes_ask_depth_5c_usd=100.0,
        no_bid_depth_5c_usd=100.0,
        no_ask_depth_5c_usd=100.0,
    )


def context(*, slot, jev_p, minimum=False):
    return {
        "slot": slot,
        "jev_p": jev_p,
        "preliminary_minimum": minimum,
        "preliminary_tier": "test",
        "final_tier": None,
        "final_reason": None,
    }


def call_final(*, p_up, direction, market, state):
    token = runtime._FINAL_CONTEXT.set(state)
    try:
        return runtime.final_value_decision(
            p_up,
            direction,
            market,
            settings(),
        )
    finally:
        runtime._FINAL_CONTEXT.reset(token)


def test_final_refresh_rejects_first_live_loss_pattern():
    state = context(slot=120, jev_p=0.40)
    decision, reason = call_final(
        p_up=0.065533837,
        direction="DOWN",
        market=book(
            yes_bid=0.44,
            yes_ask=0.45,
            no_bid=0.55,
            no_ask=0.56,
        ),
        state=state,
    )
    assert decision is None
    assert reason == "v83_final_gate:weak_jev_mid_market"
    assert state["final_tier"] is None


def test_final_refresh_resizes_extreme_tier_to_minimum_shares():
    state = context(slot=120, jev_p=0.62, minimum=True)
    decision, reason = call_final(
        p_up=0.97,
        direction="UP",
        market=book(
            yes_bid=0.81,
            yes_ask=0.82,
            no_bid=0.17,
            no_ask=0.18,
        ),
        state=state,
    )
    assert reason == "value_ready"
    assert decision is not None
    assert decision.size == 5.0
    assert decision.notional_usd == pytest.approx(4.10)
    assert state["final_tier"] == "t120_extreme_high"


def test_final_refresh_keeps_full_sizing_for_strong_tier():
    state = context(slot=120, jev_p=0.72)
    decision, reason = call_final(
        p_up=0.96,
        direction="UP",
        market=book(
            yes_bid=0.69,
            yes_ask=0.70,
            no_bid=0.29,
            no_ask=0.30,
        ),
        state=state,
    )
    assert reason == "value_ready"
    assert decision is not None
    assert decision.size > 5.0
    assert decision.notional_usd <= 5.0
    assert state["final_tier"] == "t120_strong"


def test_runtime_patch_is_restored_after_context():
    original_store = v82live.LiveStore
    original_runner = v82live.LiveT120Runner
    original_value = v82live.value_decision

    with patched_v83_runtime(allow_five_from_canary_gate=False):
        assert v82live.LiveStore is runtime.V83LiveStore
        assert v82live.LiveT120Runner is runtime.V83Runner
        assert v82live.value_decision is runtime.final_value_decision

    assert v82live.LiveStore is original_store
    assert v82live.LiveT120Runner is original_runner
    assert v82live.value_decision is original_value


def test_report_auto_resolution_uses_latest_supported_file(tmp_path, monkeypatch):
    runs = tmp_path / "runs"
    runs.mkdir()
    older = runs / "v81_early_value_forward_old.json.gz"
    newer = runs / "v82_t120_main_shadow_new.json.gz"
    ignored = runs / "v82_live_t120_not_a_gate.json.gz"
    for path in (older, newer, ignored):
        path.write_bytes(b"x")
    os.utime(older, (1, 1))
    os.utime(newer, (2, 2))
    os.utime(ignored, (3, 3))
    monkeypatch.chdir(tmp_path)

    assert resolve_paper_report(None) == Path("runs") / newer.name
    assert resolve_paper_report(Path(newer.name)) == Path("runs") / newer.name
