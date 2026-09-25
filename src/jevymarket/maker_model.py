"""Causal late-TWAP approximation, explicitly NOT a fitted/calibrated probability."""
from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass
from decimal import ROUND_FLOOR, Decimal

from .maker_config import MakerConfig


def finite(value) -> float:
    if value is None or isinstance(value, bool):
        raise ValueError("missing_numeric_value")
    result = float(value)
    if not math.isfinite(result):
        raise ValueError("nonfinite_numeric_value")
    return result


def source_time(value) -> float:
    result = finite(value)
    if result <= 0:
        raise ValueError("invalid_source_time")
    return result / 1000 if result > 10_000_000_000 else result


def floor_step(value: float, step: float) -> float:
    v, s = Decimal(str(value)), Decimal(str(step))
    if not v.is_finite() or not s.is_finite() or s <= 0:
        raise ValueError("invalid_tick")
    # Float-derived probabilities/prices can land a few ulps below an exact
    # tick (e.g. 0.9199999999999999 instead of 0.92). Add only one-billionth
    # of a step before flooring: enough to absorb representation noise, far
    # too small to promote a genuinely off-tick economic value.
    tolerance = s * Decimal("1e-9")
    return float(((v + tolerance) / s).to_integral_value(rounding=ROUND_FLOOR) * s)


def twap_window(description: str, resolution_source: str) -> int:
    """Unknown settlement wording is an error, never silently assume 60 seconds."""
    text = (description + " " + resolution_source).lower()
    if "chainlink" not in text:
        raise ValueError("unknown_resolution_source")
    matches = []
    for window in (30, 60):
        if any(s in text for s in (f"twap-{window}s", f"twap {window}s", f"{window}-second", f"{window} second")):
            matches.append(window)
    if len(matches) != 1:
        raise ValueError("ambiguous_or_missing_twap_window")
    return matches[0]


def integral(rows: list[tuple[float, float]], start: float, end: float, max_gap: float) -> float:
    """Previous-tick integral, no future samples, no interpolation over big gaps."""
    if end < start or not rows or rows[0][0] > start:
        raise ValueError("history_missing")
    if end == start:
        return 0.0
    previous = max((r for r in rows if r[0] <= start), key=lambda r: r[0])
    if start - previous[0] > max_gap:
        raise ValueError("history_gap")
    pos, area = start, 0.0
    for row in rows:
        if not start < row[0] <= end:
            continue
        if row[0] - previous[0] > max_gap:
            raise ValueError("history_gap")
        area += (row[0] - pos) * previous[1]
        pos, previous = row[0], row
    if end - previous[0] > max_gap:
        raise ValueError("history_gap")
    return area + (end - pos) * previous[1]


@dataclass(frozen=True)
class Estimate:
    p_up: float
    ts: float
    raw_ts: float
    twap_ts: float
    anchor: float
    raw_price: float
    final_mean: float
    final_sigma: float
    alignment_error_usd: float


class ReferenceCache:
    def __init__(self, config: MakerConfig):
        self.config = config
        self.samples = {k: deque(maxlen=2400) for k in ("raw", "twap30", "twap60")}
        self.anchors: dict[tuple[int, int], float] = {}
        self.revision = 0

    def add(self, name: str, ts: float, price: float, received: float) -> bool:
        ts, price, received = finite(ts), finite(price), finite(received)
        if name not in self.samples or price <= 0 or not 0 <= received - ts <= self.config.max_reference_age_seconds:
            return False
        rows = self.samples[name]
        if rows and ts <= rows[-1][0]:
            return False
        rows.append((ts, price))
        self.revision += 1
        # Exact source-clock boundary only. A startup midway through a market
        # waits until a subsequent captured boundary. No web-price fallback.
        if name.startswith("twap") and ts % 300 < 1e-6:
            self.anchors.setdefault((int(ts), int(name[4:])), price)
        self.anchors = {k: v for k, v in self.anchors.items() if k[0] >= ts - 7200}
        return True

    def estimate(self, start: int, window: int, now: float) -> Estimate:
        c = self.config
        raw, twaps = list(self.samples["raw"]), list(self.samples[f"twap{window}"])
        anchor = self.anchors.get((start, window))
        if anchor is None or not raw or not twaps:
            raise ValueError("missing_anchor_or_reference")
        t, spot = raw[-1]
        u, published = twaps[-1]
        if any(not 0 <= now - ts <= c.max_reference_age_seconds for ts in (t, u)):
            raise ValueError("stale_reference")
        h = start + 300 - t
        if h <= 0 or now >= start + 300:
            raise ValueError("window_ended")
        history = [r for r in raw if t - c.min_history_seconds - c.max_history_gap_seconds <= r[0] <= t]
        integral(history, t - c.min_history_seconds, t, c.max_history_gap_seconds)
        def variance_rate(seconds):
            rs = [r for r in history if r[0] >= t - seconds]
            if len(rs) < 20 or rs[-1][0] - rs[0][0] < seconds - c.max_history_gap_seconds:
                raise ValueError("insufficient_volatility_history")
            return sum((b[1] - a[1]) ** 2 for a, b in zip(rs, rs[1:], strict=False)) / (rs[-1][0] - rs[0][0])
        # Use the larger short/long variance plus an explicit stress multiplier.
        # This is an experiment assumption, not a number fitted on winning orders.
        sigma2 = max(variance_rate(60), variance_rate(c.min_history_seconds),
                     c.sigma_floor_usd_sqrt_second ** 2) * 1.5 ** 2
        if u > t:
            raise ValueError("raw_behind_twap")
        reconstructed = integral(raw, u - window, u, c.max_history_gap_seconds) / window
        mismatch = reconstructed - published
        if abs(mismatch) > max(10.0, 6 * math.sqrt(sigma2 * window)):
            raise ValueError("raw_twap_alignment_failure")
        if h < window:
            mean = (integral(raw, start + 300 - window, t, c.max_history_gap_seconds) + h * spot) / window
            variance = sigma2 * h ** 3 / (3 * window ** 2)
        else:
            mean = spot
            variance = sigma2 * (h - 2 * window / 3)
        # Unobserved time is already inside h (measured from latest raw source
        # time); add sampling/alignment noise rather than claiming certainty.
        noise = max(c.reference_noise_usd, 2 * abs(mismatch))
        sd = math.sqrt(variance + noise ** 2)
        p = 0.5 * (1 + math.erf((mean - anchor) / (sd * math.sqrt(2))))
        return Estimate(min(0.995, max(0.005, p)), now, t, u, anchor, spot, mean, sd, mismatch)
