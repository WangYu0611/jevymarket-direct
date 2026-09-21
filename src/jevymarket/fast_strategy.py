"""BTC 5m experiment helpers. No API calls or learned trading thresholds."""

from __future__ import annotations

import math
from dataclasses import asdict
from datetime import UTC, datetime

from .config import Settings, load_settings
from .market_data import (
    ShortTermSnapshot,
    chainlink_twap_window_seconds,
    compute_path_features,
    recover_chainlink_anchor_from_history,
)
from .markets import Candidate, market_window
from .signal import Book, Skip, Trade, kelly_fraction, round_to_tick
from .store import Store

VERSION = "v3-btc5m-10s-quant-shadow"
CHECKPOINTS = (240, 180, 120, 60, 30)


def fast_settings() -> Settings:
    # Explicit overrides take precedence over an old .env. Do not edit secrets,
    # delete history, or accidentally bring back 15m/Binance through old settings.
    return load_settings(allowed_assets="BTC", allowed_timeframes="5m", dry_run=True,
                         strategy_version=VERSION, research_enabled=False)


def experiment_parameters(s: Settings, interval: float, jev_enabled: bool) -> dict:
    names = (
        "min_edge", "min_answerable", "min_clarity", "min_trade_price", "max_trade_price",
        "max_spread", "max_usd_per_trade", "max_open_exposure_usd", "kelly_fraction",
        "short_term_min_liquidity_usd", "short_term_min_volume_usd",
        "short_term_min_history_seconds", "short_term_max_sample_age_seconds",
        "anchor_capture_grace_seconds", "min_market_price", "max_market_price", "max_days_to_resolution",
        "jev_model", "jev_timeout_seconds", "jev_max_retries", "description_max_chars",
    )
    data = {name: getattr(s, name) for name in names}
    data.update(interval_seconds=float(interval), timeframe="5m", primary="quant_without_jev_gate",
                jev_mode="checkpoint_shadow" if jev_enabled else "off",
                checkpoints=list(CHECKPOINTS), checkpoint_lateness_seconds=10,
                fees_included=False, slippage_included=False, paper_only=True)
    return data


def checkpoint(seconds_left: int | None) -> int | None:
    """First valid observation at/after the target, at most 10 seconds late."""
    if seconds_left is None:
        return None
    return next((cp for cp in CHECKPOINTS if cp - 10 < seconds_left <= cp), None)


def next_deadline(previous: float, finished: float, interval: float) -> tuple[float, int]:
    """Fixed-rate start times; skip missed slots instead of catch-up bursts."""
    if not math.isfinite(interval) or interval <= 0:
        raise ValueError("计算间隔必须为正数")
    target = previous + interval
    missed = max(0, math.floor((finished - target) / interval) + 1) if finished > target else 0
    return target + missed * interval, missed


def local_snapshot(c: Candidate, s: Settings, store: Store, *, now: datetime | None = None) -> ShortTermSnapshot:
    """Use continuously persisted RTDS samples; never open a per-tick WebSocket."""
    now = now or datetime.now(UTC)
    if now.tzinfo is None:
        now = now.replace(tzinfo=UTC)
    window = market_window(c.market, s)
    seconds_left = max(0, int((window[1] - now).total_seconds())) if window else None
    twap = chainlink_twap_window_seconds(c)
    anchor = store.get_price_anchor(c.slug, twap)
    if anchor is None:
        anchor = recover_chainlink_anchor_from_history(s, store, c, twap_window=twap)
    rows = store.get_price_samples(source="chainlink_twap", twap_window=twap,
                                   since_ts=now.timestamp() - s.short_term_max_sample_age_seconds,
                                   until_ts=now.timestamp())
    # SQLite stores whole seconds; explicitly check age after querying its inclusive bounds.
    current = None
    if rows and 0 <= now.timestamp() - rows[-1]["ts"] <= s.short_term_max_sample_age_seconds:
        value = float(rows[-1]["price"])
        if math.isfinite(value) and value > 0:
            current = value
    target = float(anchor["price"]) if anchor else None
    if target is not None and (not math.isfinite(target) or target <= 0):
        target = None
    features = compute_path_features(
        store.get_price_samples(source="chainlink_spot", since_ts=now.timestamp() - 330,
                                until_ts=now.timestamp()),
        history_source="Polymarket RTDS · Chainlink BTC/USD raw", captured_at=now,
        target_price=target, current_price=current, seconds_left=seconds_left,
        min_history_seconds=s.short_term_min_history_seconds,
        max_sample_age_seconds=s.short_term_max_sample_age_seconds,
    )
    return ShortTermSnapshot(target_price=target, current_price=current, seconds_left=seconds_left,
                             target_source=anchor["source"] if target is not None else None,
                             current_source=f"RTDS Chainlink BTC/USD TWAP {twap}s（持续监听缓存）" if current else None,
                             captured_at=now, twap_window_seconds=twap, path_features=features)


def quantitative_trade(p_up: float, book: Book, s: Settings) -> Trade | Skip:
    """Same Phi(Z), edge band and fractional Kelly as v2, with NO fake Jev view.

    Jev answerability/clarity is not used by this paper-only experiment. It is
    measured separately. No probability blending or fitted threshold is implied.
    """
    if not math.isfinite(p_up) or not 0 <= p_up <= 1:
        return Skip("量化概率无效")
    for bid, ask in ((book.yes_bid, book.yes_ask), (book.no_bid, book.no_ask)):
        if any(v is not None and (not math.isfinite(v) or not 0 <= v <= 1) for v in (bid, ask)):
            return Skip("盘口价格无效")
        if bid is not None and ask is not None and (ask < bid or ask - bid > s.max_spread):
            return Skip("盘口交叉或点差超限")
    options = [(book.yes_label.upper(), book.yes_token_id, p_up, book.yes_ask),
               (book.no_label.upper(), book.no_token_id, 1 - p_up, book.no_ask)]
    options = [o for o in options if o[3] is not None and 0 < o[3] < 1
               and s.min_trade_price <= o[3] <= s.max_trade_price]
    if not options:
        return Skip("没有交易价格区间内的卖价")
    outcome, token, p, ask = max(options, key=lambda o: o[2] - o[3])
    edge = p - ask
    if edge < s.min_edge:
        return Skip(f"最佳优势 {edge:+.3f} < {s.min_edge:.3f}")
    price = round_to_tick(ask, book.tick_size)
    if not 0 < price < 1 or price < ask - 1e-9:
        return Skip("价格不符合 tick，不能假定按低于卖价成交")
    stake = min(s.max_usd_per_trade, kelly_fraction(p, ask) * s.kelly_fraction * s.max_open_exposure_usd)
    if stake <= 0:
        return Skip("凯利仓位为零")
    size = max(book.min_order_size, math.floor(stake / price * 100) / 100)
    usd = round(price * size, 4)
    if not math.isfinite(usd) or usd <= 0 or usd > s.max_usd_per_trade * 1.5:
        return Skip("最小订单量导致金额超过原有单笔上限")
    return Trade(outcome=outcome, token_id=token, side="BUY", price=price, size=size,
                 usd=usd, p=p, edge=edge, rationale="量化 Φ(Z)；Jev 仅作异步对照，不门控本单")


def snapshot_payload(c: Candidate, snapshot: ShortTermSnapshot) -> dict:
    return {"snapshot": snapshot.to_state(), "book": asdict(c.book)}
