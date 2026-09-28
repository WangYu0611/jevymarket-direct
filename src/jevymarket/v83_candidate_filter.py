"""Pure V8.3 candidate filters used by paper and execution runners."""
from __future__ import annotations

import math
from dataclasses import dataclass, replace

from .price_value_forward import (
    CRYPTO_TAKER_FEE_RATE,
    ValueDecision,
    side_probability,
    taker_fee_usd,
)

T120_STRONG_JEV = 0.68
T120_EXTREME_MIN_JEV = 0.60
T120_EXTREME_MIN_QUANT = 0.94
T120_LOW_ASK_MAX = 0.50
T120_HIGH_ASK_MIN = 0.80
T120_LOW_MIN_EDGE = 0.20
T120_HIGH_MIN_EDGE = 0.10
T110_MIN_JEV = 0.64
T110_MIN_QUANT = 0.92
T110_MIN_EDGE = 0.10


@dataclass(frozen=True)
class CandidateTier:
    accepted: bool
    name: str
    reason: str
    quant_side: float
    jev_side: float
    minimum_shares: bool


def selected_probability(p_up: float, direction: str) -> float:
    if direction not in {"UP", "DOWN"}:
        raise ValueError("invalid_direction")
    if not math.isfinite(p_up) or not 0 <= p_up <= 1:
        raise ValueError("invalid_probability")
    return side_probability(p_up, direction)


def classify_candidate(
    *, slot: int, direction: str, quant_p: float, jev_p: float,
    decision: ValueDecision,
) -> CandidateTier:
    quant_side = selected_probability(quant_p, direction)
    jev_side = selected_probability(jev_p, direction)
    if slot == 120:
        if jev_side >= T120_STRONG_JEV - 1e-12:
            return CandidateTier(True, "t120_strong", "strong_jev", quant_side, jev_side, False)
        common = jev_side >= T120_EXTREME_MIN_JEV - 1e-12 and quant_side >= T120_EXTREME_MIN_QUANT - 1e-12
        if common and decision.ask <= T120_LOW_ASK_MAX + 1e-12 and decision.edge >= T120_LOW_MIN_EDGE - 1e-12:
            return CandidateTier(True, "t120_extreme_low", "extreme_low", quant_side, jev_side, True)
        if common and decision.ask >= T120_HIGH_ASK_MIN - 1e-12 and decision.edge >= T120_HIGH_MIN_EDGE - 1e-12:
            return CandidateTier(True, "t120_extreme_high", "extreme_high", quant_side, jev_side, True)
        return CandidateTier(False, "rejected", "weak_jev_mid_market", quant_side, jev_side, False)
    if slot == 110:
        if jev_side >= T110_MIN_JEV - 1e-12 and quant_side >= T110_MIN_QUANT - 1e-12 and decision.edge >= T110_MIN_EDGE - 1e-12:
            return CandidateTier(True, "t110_fallback", "strict_t110", quant_side, jev_side, True)
        return CandidateTier(False, "rejected", "t110_quality_gate_failed", quant_side, jev_side, False)
    return CandidateTier(False, "rejected", "slot_not_enabled", quant_side, jev_side, False)


def minimum_share_decision(
    decision: ValueDecision, *, hard_cap_usd: float = 5.0,
    fee_rate: float = CRYPTO_TAKER_FEE_RATE,
) -> ValueDecision:
    size = math.ceil((decision.min_order_size - 1e-12) * 100) / 100
    notional = round(size * decision.ask, 6)
    if size <= 0 or notional <= 0 or notional > hard_cap_usd + 1e-9:
        raise ValueError("minimum_shares_exceed_cap")
    return replace(
        decision,
        size=size,
        notional_usd=notional,
        taker_fee_usd_est=taker_fee_usd(size, decision.ask, fee_rate),
    )
