from types import SimpleNamespace

import pytest

from jevymarket.maker_config import MakerConfig
from jevymarket.maker_window_ab import ARM_SPECS, _complete_window, aggregate, book_gate, make_arms


def test_config_allows_60_second_shadow_window_only():
    c = MakerConfig(entry_seconds=60, cancel_before_end_seconds=20)
    assert c.entry_seconds == 60 and c.cancel_before_end_seconds == 20
    with pytest.raises(ValueError):
        MakerConfig(entry_seconds=60.01, cancel_before_end_seconds=20)


def test_arms_change_timing_only():
    base = MakerConfig(entry_seconds=60, cancel_before_end_seconds=10)
    arms = make_arms(base)
    assert arms["A_late_30_to_10"].entry_seconds == 30
    assert arms["B_early_60_to_20"].entry_seconds == 60
    for name, arm in arms.items():
        spec = ARM_SPECS[name]
        assert arm.cancel_before_end_seconds == spec["cancel_before_end_seconds"]
        assert arm.min_confidence == base.min_confidence
        assert arm.max_book_age_seconds == base.max_book_age_seconds
        assert arm.max_order_usd == base.max_order_usd


@pytest.mark.parametrize(("statuses", "expected"), [
    ({"UP": {"status": "ready"}, "DOWN": {"status": "ready"}}, "two_sided_ready"),
    ({"UP": {"status": "valid_bid_only"}, "DOWN": {"status": "valid_ask_only"}}, "one_sided_or_empty"),
    ({"UP": {"status": "stale_source"}, "DOWN": {"status": "ready"}}, "stale"),
    ({"UP": {"status": "invalidated"}, "DOWN": {"status": "ready"}}, "unavailable"),
])
def test_book_gate_is_explicit(statuses, expected):
    assert book_gate({"book_details": statuses}) == expected


def row(slug, left, a_active, b_active, a_candidate=False, b_candidate=False, gate="two_sided_ready"):
    def arm(active, candidate):
        return {"active": active, "reason": "quote_ready" if candidate else "confidence_or_agreement_failed",
                "candidate": {"price": .9} if candidate else None}
    return {"slug": slug, "seconds_left": left, "book_gate": gate,
            "arms": {"A_late_30_to_10": arm(a_active, a_candidate),
                     "B_early_60_to_20": arm(b_active, b_candidate)}}


def test_aggregate_uses_rates_and_complete_market_pairing():
    samples = [
        row("m1", 59, False, True, b_candidate=True),
        row("m1", 30, True, True),
        row("m1", 21, True, True),
        row("m1", 11, True, False, a_candidate=True),
        row("m2", 59, False, True),
        row("m2", 21, True, True),
        row("m2", 11, True, False),
    ]
    result = aggregate(samples)
    assert result["arms"]["A_late_30_to_10"]["complete_markets"] == 1
    assert result["arms"]["B_early_60_to_20"]["complete_markets"] == 2
    assert result["paired_complete_markets"] == 1
    assert result["paired_candidate_matrix"] == {"both": 1}
    assert result["arms"]["A_late_30_to_10"]["candidate_sample_rate"] == pytest.approx(1/5)
    assert result["arms"]["B_early_60_to_20"]["candidate_sample_rate"] == pytest.approx(1/5)
    assert result["bands"]["B_60_to_40"]["candidate_rate"] == pytest.approx(1/2)
    assert result["overlap_decision_mismatches"] == 0


def test_complete_window_requires_both_edges():
    assert _complete_window([29.5, 20, 11], ARM_SPECS["A_late_30_to_10"])
    assert not _complete_window([20, 11], ARM_SPECS["A_late_30_to_10"])
