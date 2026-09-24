"""Post-only paper state machine. No signing, account credentials, or order API."""
from __future__ import annotations

import math
import uuid
from dataclasses import asdict, dataclass

from .maker_book import BookCache
from .maker_config import MakerConfig
from .maker_model import Estimate, floor_step


@dataclass(frozen=True)
class Market:
    slug: str
    condition: str
    start: int
    window: int
    up_token: str
    down_token: str

    @property
    def end(self):
        return self.start + 300


@dataclass(frozen=True)
class Quote:
    token: str
    outcome: str
    price: float
    size: float
    fair_p: float
    model_p: float
    market_p: float


def choose_quote(m: Market, cache: BookCache, est: Estimate, c: MakerConfig,
                 wall: float, mono: float, budget: float) -> tuple[Quote | None, str]:
    left = m.end - wall
    if not c.cancel_before_end_seconds < left <= c.entry_seconds:
        return None, "outside_entry_window"
    if not 0 <= wall - est.ts <= c.max_model_age_seconds:
        return None, "stale_model"
    if any(not 0 <= wall - ts <= c.max_reference_age_seconds for ts in (est.raw_ts, est.twap_ts)):
        return None, "stale_reference"
    up, down = cache.books[m.up_token], cache.books[m.down_token]
    if not all(b.fresh(wall, mono, c.max_book_age_seconds) for b in (up, down)):
        return None, "book_not_ready_or_stale"
    if any(b.ask - b.bid > c.max_spread for b in (up, down)):
        return None, "spread_too_wide"
    u, d = (up.bid + up.ask) / 2, (down.bid + down.ask) / 2
    if abs(u + d - 1) > 0.05:
        return None, "complement_prices_inconsistent"
    market_up = u / (u + d)
    is_up = market_up >= 0.5
    pm = market_up if is_up else 1 - market_up
    pq = est.p_up if is_up else 1 - est.p_up
    if not all(math.isfinite(p) and 0 <= p <= 1 for p in (pm, pq)):
        return None, "invalid_probability"
    if min(pm, pq) < c.min_confidence or abs(pm - pq) > c.max_model_market_gap:
        return None, "confidence_or_agreement_failed"
    # A conservative quoting heuristic, NOT a confidence interval or a trained
    # calibration. Positive expected rebate is deliberately not included.
    fair = min(pm, pq) - c.uncertainty_buffer
    book = up if is_up else down
    price = floor_step(min(book.bid, fair - c.min_edge, c.max_price, book.ask - book.tick), book.tick)
    if not c.min_price <= price <= c.max_price or price >= book.ask:
        return None, "no_postonly_price"
    cash = min(budget, c.max_order_usd, c.capital_usd * c.risk_fraction)
    size = floor_step(min(cash / price, c.max_market_profit_usd / (1 - price)), 0.01)
    if size < book.minimum or size <= 0:
        return None, "minimum_size_exceeds_budget"
    return Quote(book.token, "UP" if is_up else "DOWN", price, size, fair, pq, pm), "quote_ready"


@dataclass
class PaperOrder:
    id: str
    slug: str
    condition: str
    token: str
    outcome: str
    price: float
    size: float
    fair_p: float
    requested_ts: float
    requested_mono: float
    generation: int
    end: float
    side: str = "BUY"
    order_type: str = "GTC"
    post_only: bool = True
    state: str = "pending_submit"
    filled: float = 0.0
    queue_ahead: float = 0.0
    active_ts: float | None = None
    active_mono: float | None = None
    cancel_requested_ts: float | None = None
    cancel_mono: float | None = None
    closed_ts: float | None = None
    cancel_reason: str | None = None
    winner: str | None = None
    settled_ts: float | None = None
    pnl: float | None = None
    uncertain: bool = False

    @property
    def open(self):
        return self.state in {"pending_submit", "active", "cancel_pending"}

    @property
    def remaining(self):
        return max(0.0, self.size - self.filled)


class PaperEngine:
    def __init__(self, config: MakerConfig, emit=lambda kind, data: None):
        self.c, self.emit = config, emit
        self.orders: list[PaperOrder] = []
        self.seen_trades: set[str] = set()
        self.halted = False
        self.last_submit_mono = -1e30

    def changed(self, order: PaperOrder, event: str):
        self.emit("order", asdict(order))
        self.emit("execution", {"event": event, "order_id": order.id, "state": order.state,
                                "filled": order.filled, "queue_ahead": order.queue_ahead})

    def restore(self, rows: list[dict], wall: float):
        self.orders = [PaperOrder(**r) for r in rows]
        for o in self.orders:
            if o.open:
                # No public data exists for downtime. Retain known fills and
                # flag unknown remaining inventory, never manufacture fills.
                o.state, o.uncertain, o.closed_ts = "interrupted", True, wall
                self.changed(o, "restart_unobserved_interval")
        self.halted = any(o.uncertain and o.winner is None for o in self.orders)

    def exposure(self) -> float:
        return sum((o.filled if o.winner is None else 0) * o.price +
                   (o.remaining * o.price if o.open or (o.uncertain and o.winner is None) else 0)
                   for o in self.orders)

    @staticmethod
    def risk_pnl(o: PaperOrder) -> float:
        extra_loss = (o.remaining * o.price if o.uncertain and o.winner is not None
                      and o.winner != o.outcome else 0.0)
        return (o.pnl or 0.0) - extra_loss

    def budget(self, slug: str, wall: float) -> float:
        day = int((wall + 8 * 3600) // 86400)
        losses = sum(-min(0, self.risk_pnl(o)) for o in self.orders
                     if o.settled_ts is not None and int((o.settled_ts + 8 * 3600) // 86400) == day)
        market_used = sum((o.filled + (o.remaining if o.open or o.uncertain else 0)) * o.price
                          for o in self.orders if o.slug == slug)
        realized = sum(self.risk_pnl(o) for o in self.orders)
        return max(0, min(self.c.max_market_usd - market_used,
                          self.c.max_open_usd - self.exposure(),
                          self.c.capital_usd + realized - self.exposure(),
                          self.c.daily_loss_limit_usd - losses - self.exposure()))

    def active(self):
        return next((o for o in self.orders if o.open), None)

    def submit(self, market: Market, cache: BookCache, q: Quote, wall: float, mono: float) -> bool:
        if self.halted or self.active() or mono - self.last_submit_mono < self.c.min_requote_seconds:
            return False
        past = [o for o in self.orders if o.slug == market.slug]
        if any(o.filled > 0 or o.uncertain for o in past) or len(past) >= self.c.max_quotes_per_market:
            return False
        if (q.token not in cache.books or q.outcome not in {"UP", "DOWN"}
                or q.token != (market.up_token if q.outcome == "UP" else market.down_token)
                or not all(math.isfinite(v) for v in (q.price, q.size, q.fair_p))
                or not self.c.min_price <= q.price <= self.c.max_price or q.size <= 0
                or not 0 < q.fair_p < 1 or q.size != floor_step(q.size, 0.01)
                or q.fair_p - q.price < self.c.min_edge - 1e-9):
            return False
        b = cache.books[q.token]
        # The broker independently rechecks price, limits and post-only status.
        if (not b.fresh(wall, mono, self.c.max_book_age_seconds) or q.price >= b.ask
                or q.size < b.minimum or q.price != floor_step(q.price, b.tick)
                or q.price * q.size > min(self.budget(market.slug, wall), self.c.max_order_usd,
                                          self.c.capital_usd * self.c.risk_fraction) + 1e-9
                or q.size * (1 - q.price) > self.c.max_market_profit_usd + 1e-9
                or not self.c.cancel_before_end_seconds < market.end - wall <= self.c.entry_seconds):
            return False
        order = PaperOrder(uuid.uuid4().hex, market.slug, market.condition, q.token, q.outcome,
                           q.price, q.size, q.fair_p, wall, mono, cache.generation, market.end)
        self.orders.append(order)
        self.last_submit_mono = mono
        self.changed(order, "postonly_submit_intent")
        return True

    def cancel(self, reason: str, wall: float, mono: float):
        for o in self.orders:
            if o.open and o.state != "cancel_pending":
                o.state, o.cancel_reason = "cancel_pending", reason
                o.cancel_requested_ts, o.cancel_mono = wall, mono
                self.changed(o, "cancel_intent")

    def advance(self, cache: BookCache | None, wall: float, mono: float, safe: bool):
        for o in self.orders:
            if not o.open:
                continue
            # A pending submit is not magically aborted by a cancel intent.
            # It can arrive and fill while cancellation is in flight.
            if o.active_ts is None and mono - o.requested_mono >= self.c.paper_submit_latency_seconds:
                b = cache.books.get(o.token) if cache else None
                if (not b or cache.generation != o.generation
                        or not b.fresh(wall, mono, self.c.max_book_age_seconds)):
                    o.uncertain, self.halted = True, True
                    self.cancel("arrival_not_observable", wall, mono)
                elif o.price >= b.ask or o.price != floor_step(o.price, b.tick):
                    o.state, o.closed_ts = "rejected", wall
                    self.changed(o, "postonly_rejected_on_arrival")
                    continue
                else:
                    if o.state != "cancel_pending":
                        o.state = "active"
                    o.active_ts, o.active_mono = wall, mono
                    o.queue_ahead = b.ahead(o.price)
                    self.changed(o, "simulated_postonly_ack")
            if o.state == "cancel_pending":
                if mono - o.cancel_mono >= self.c.paper_cancel_latency_seconds:
                    o.state, o.closed_ts = "cancelled", wall
                    self.changed(o, "simulated_cancel_ack")
                continue
            if not safe or wall >= o.end - self.c.cancel_before_end_seconds:
                self.cancel("risk_or_end_cutoff", wall, mono)
            elif o.state == "active" and mono - o.active_mono >= self.c.quote_ttl_seconds:
                self.cancel("quote_ttl", wall, mono)

    def on_trade(self, trade: dict, wall: float):
        if trade["key"] in self.seen_trades:
            return
        self.seen_trades.add(trade["key"])
        if trade["side"] != "SELL":
            return
        remaining_volume = trade["size"]
        for o in self.orders:
            if (o.token != trade["token"] or o.active_ts is None or o.winner is not None
                    or not o.active_ts < trade["ts"] or o.price < trade["price"] or o.remaining <= 0):
                continue
            if o.closed_ts is not None and trade["ts"] >= o.closed_ts:
                continue
            if o.closed_ts is not None:
                # A late public print can precede our simulated cancel ack.
                # Reserve unknown remaining risk and halt rather than declaring
                # the cancelled order safely unfilled or double-counting capital.
                o.uncertain, self.halted = True, True
                self.changed(o, "late_trade_cancel_race_unverified")
                continue
            before = remaining_volume
            consumed = min(o.queue_ahead, remaining_volume)
            o.queue_ahead -= consumed
            remaining_volume -= consumed
            fill = min(o.remaining, remaining_volume)
            if fill > 0:
                o.filled = round(o.filled + fill, 8)
                remaining_volume -= fill
                if o.remaining < 1e-8:
                    o.state, o.closed_ts = "filled", wall
                self.changed(o, "estimated_partial_or_full_fill")
            elif before > 0:
                self.changed(o, "estimated_queue_consumption")
            if remaining_volume <= 0:
                break

    def settle(self, slug: str, winner: str, wall: float):
        if winner not in {"UP", "DOWN"}:
            raise ValueError("invalid_official_winner")
        for o in self.orders:
            if o.slug != slug or wall < o.end or o.open:
                continue
            if o.winner is not None:
                if o.winner != winner:
                    self.halted = True
                    raise ValueError("official_result_conflict")
                continue
            o.winner, o.settled_ts = winner, wall
            o.pnl = o.filled * ((1 if o.outcome == winner else 0) - o.price)
            self.changed(o, "official_settlement_of_estimated_fill")
        self.halted = any(o.uncertain and o.winner is None for o in self.orders)

    def report(self) -> dict:
        filled = [o for o in self.orders if o.filled > 0]
        settled = [o for o in filled if o.winner is not None]
        positive = sorted((o.pnl for o in settled if o.pnl > 0), reverse=True)
        pnl = sum(o.pnl for o in settled)
        by_market = {}
        for o in settled:
            by_market[o.slug] = by_market.get(o.slug, 0) + o.pnl
        best = max(by_market.values(), default=0)
        return {"paper_only": True, "quotes": len(self.orders), "filled_orders_estimated": len(filled),
                "settled_filled_markets": len(by_market), "pending_filled_orders": len(filled) - len(settled),
                "gross_pnl_estimated": pnl, "settled_stake_estimated": sum(o.filled * o.price for o in settled),
                "open_risk_reserved_usd": self.exposure(), "halted": self.halted or any(o.uncertain and o.winner is None for o in self.orders),
                "wins_estimated": sum(o.pnl > 0 for o in settled),
                "losses_estimated": sum(o.pnl < 0 for o in settled),
                "open_quotes": sum(o.open for o in self.orders),
                "uncertain_settled_pnl_lower": sum(self.risk_pnl(o) for o in self.orders if o.winner is not None),
                "uncertain_settled_pnl_upper": pnl + sum(o.remaining * (1 - o.price) for o in self.orders
                                                        if o.uncertain and o.winner == o.outcome),
                "uncertain_orders": sum(o.uncertain for o in self.orders),
                "top1_share_net_profit": best / pnl if pnl > 0 else None,
                "top1_share_positive_profit": max(positive, default=0) / sum(positive) if positive else None,
                "concentration_target": self.c.concentration_target,
                "concentration_evaluable": len(by_market) >= 50 and pnl > 0,
                "pnl_minus_top3_positive_contributions": pnl - sum(positive[:3]),
                "confirmed_rebates_usd": 0.0, "rebates_included": False,
                "maker_fee_assumption": 0.0, "live_cancel_ack_ms": None,
                "limitations": ["公开成交+保守队列是假设成交，不是实盘下界或成交证明",
                                "返佣未计入；Maker零手续费为当前规则假设",
                                "延迟参数为模拟假设，本地反应速度不等于交易所撤单回执",
                                "未知断线成交单标记uncertain，不能当作普通未成交单",
                                "2%利润集中度为事后指标，不是可事前保证的风控"]}
