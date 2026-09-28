from jevymarket.price_value_forward import ValueDecision
from jevymarket.v83_candidate_filter import (
    classify_candidate,
    minimum_share_decision,
)


def decision(*, ask=0.56, edge=0.37, minimum=5.0, size=8.92):
    return ValueDecision(
        direction="DOWN",
        probability=ask + edge,
        bid=ask - 0.01,
        ask=ask,
        spread=0.01,
        depth_5c_usd=1000.0,
        tick_size=0.01,
        min_order_size=minimum,
        edge=edge,
        size=size,
        notional_usd=round(size * ask, 6),
        taker_fee_usd_est=0.1,
    )


def test_first_live_loss_pattern_is_rejected():
    tier = classify_candidate(
        slot=120,
        direction="DOWN",
        quant_p=0.065533837,
        jev_p=0.40,
        decision=decision(ask=0.56, edge=0.374466163),
    )
    assert not tier.accepted
    assert tier.reason == "weak_jev_mid_market"
    assert tier.jev_side == 0.60


def test_strong_jev_t120_is_full_tier():
    tier = classify_candidate(
        slot=120,
        direction="UP",
        quant_p=0.96,
        jev_p=0.72,
        decision=decision(ask=0.81, edge=0.15),
    )
    assert tier.accepted
    assert tier.name == "t120_strong"
    assert not tier.minimum_shares


def test_market_extreme_t120_keeps_controlled_volume():
    tier = classify_candidate(
        slot=120,
        direction="UP",
        quant_p=0.97,
        jev_p=0.62,
        decision=decision(ask=0.82, edge=0.15),
    )
    assert tier.accepted
    assert tier.name == "t120_extreme_high"
    assert tier.minimum_shares


def test_mid_market_weak_jev_is_not_rescued_by_large_quant_edge():
    tier = classify_candidate(
        slot=120,
        direction="UP",
        quant_p=0.99,
        jev_p=0.64,
        decision=decision(ask=0.64, edge=0.35),
    )
    assert not tier.accepted


def test_strict_t110_fallback_is_minimum_share_tier():
    tier = classify_candidate(
        slot=110,
        direction="DOWN",
        quant_p=0.04,
        jev_p=0.34,
        decision=decision(ask=0.77, edge=0.19),
    )
    assert tier.accepted
    assert tier.name == "t110_fallback"
    assert tier.minimum_shares


def test_t100_never_enters_candidate_tiers():
    tier = classify_candidate(
        slot=100,
        direction="UP",
        quant_p=0.99,
        jev_p=0.99,
        decision=decision(ask=0.80, edge=0.19),
    )
    assert not tier.accepted
    assert tier.reason == "slot_not_enabled"


def test_minimum_share_sizing_reduces_fallback_exposure():
    resized = minimum_share_decision(decision(ask=0.36, edge=0.59, size=13.88))
    assert resized.size == 5.0
    assert resized.notional_usd == 1.8
    assert resized.taker_fee_usd_est > 0
