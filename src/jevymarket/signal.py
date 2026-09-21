"""Turn a market state into a Jev view, and a Jev view + order book into a trade (or not).

`evaluate` is a pure function so the sizing/threshold logic is unit-testable without
touching Jev or Polymarket.
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from .config import Settings
from .jev import Decision, JevClient, noul, score
from .research import Brief, Researcher, ResearchError

if TYPE_CHECKING:
    from .market_data import ShortTermSnapshot
    from .markets import Candidate
    from .store import Store

log = logging.getLogger(__name__)

CLARITY_LEVELS = [
    "Resolution criteria are missing, contradictory, or depend on undefined terms.",
    "Criteria exist but leave major ambiguity about what counts or who decides.",
    "Criteria are mostly clear; a few edge cases are unspecified.",
    "Criteria are clear with a named resolution source; minor edge cases only.",
    "Criteria are precise, objective, and leave no room for interpretation.",
]

QUESTIONS = {
    "resolves_yes": noul(
        "Estimate the probability that the FIRST listed market outcome, provided as "
        "`primary_outcome` in state, will win. For these BTC short-term markets this is UP. "
        "Use `short_term_market_data` as the primary evidence. It contains the authoritative "
        "target/open price, current reference price, distance from target, seconds remaining, "
        "and `path_features`: recent 30s/60s/180s/300s returns, realized volatility, recent "
        "price range, up-tick ratio, trend slope, expected remaining volatility, and "
        "`distance_z` (signed log-distance from the target divided by estimated volatility "
        "over the remaining time). Treat positive distance_z as price above the target and "
        "negative distance_z as below it. Use momentum as context, not as certainty. Do not "
        "infer probability from prediction-market odds; those odds are intentionally absent."
    ),
    "answerable": noul(
        "The authoritative short-term data contain enough current information to form a "
        "well-informed probability estimate. Give high answerability only when target/open "
        "price, current reference price, seconds remaining, and `path_features.feature_ready` "
        "are present, with recent non-stale history and at least short-term volatility/momentum "
        "coverage. Missing or stale path history should materially reduce answerability."
    ),
    "clarity": score(
        "How clear and objective are the resolution criteria for this market?",
        CLARITY_LEVELS,
    ),
}


@dataclass(frozen=True)
class JevView:
    p_yes: float
    answerable: float
    clarity: int
    clarity_mean: float | None
    clarity_confidence: float | None
    model: str | None
    cost: float
    raw: dict


@dataclass(frozen=True)
class Book:
    """Best-of-book snapshot for one binary market."""

    yes_token_id: str
    no_token_id: str
    yes_bid: float | None
    yes_ask: float | None
    no_bid: float | None
    no_ask: float | None
    tick_size: float
    min_order_size: float
    yes_label: str = "YES"
    no_label: str = "NO"

    @property
    def midpoint(self) -> float | None:
        if self.yes_bid is not None and self.yes_ask is not None:
            return (self.yes_bid + self.yes_ask) / 2
        return self.yes_ask if self.yes_ask is not None else self.yes_bid

    @property
    def spread(self) -> float | None:
        if self.yes_bid is not None and self.yes_ask is not None:
            return self.yes_ask - self.yes_bid
        return None


@dataclass(frozen=True)
class Trade:
    outcome: str           # market outcome label, e.g. "UP" | "DOWN"
    token_id: str
    side: str              # always "BUY" in v1
    price: float           # limit price, rounded to tick
    size: float            # number of shares
    usd: float             # price * size
    p: float               # Jev's probability that this outcome wins
    edge: float            # p - price
    rationale: str


@dataclass(frozen=True)
class Skip:
    reason: str


async def ask_jev(jev: JevClient, state: dict) -> JevView:
    d: Decision = await jev.decide(state, QUESTIONS)
    a = d.answers
    p_yes = a["resolves_yes"].noul
    answerable = a["answerable"].noul
    clarity = a["clarity"].score
    if p_yes is None or answerable is None or clarity is None:
        raise RuntimeError(f"Unexpected Jev answer shape: {d.model_dump()}")
    return JevView(
        p_yes=p_yes,
        answerable=answerable,
        clarity=clarity,
        clarity_mean=a["clarity"].score_mean,
        clarity_confidence=a["clarity"].confidence,
        model=d.model,
        cost=d.usage.cost,
        raw=d.model_dump(),
    )


def round_to_tick(price: float, tick: float) -> float:
    if tick <= 0:
        return round(price, 4)
    steps = round(price / tick)
    return round(steps * tick, 6)


def kelly_fraction(p: float, price: float) -> float:
    """Kelly fraction for a binary contract bought at `price` paying 1 if it wins.

    b = (1 - price) / price is the net odds; f* = (p*b - (1-p)) / b = p - (1-p)*price/(1-price).
    """
    if price <= 0 or price >= 1:
        return 0.0
    f = p - (1 - p) * price / (1 - price)
    return max(0.0, f)


def evaluate(view: JevView, book: Book, s: Settings, bankroll_usd: float | None = None) -> Trade | Skip:
    if view.answerable < s.min_answerable:
        return Skip(f"信息充分度 {view.answerable:.2f} < 阈值 {s.min_answerable}")
    if view.clarity < s.min_clarity:
        return Skip(f"结算规则清晰度 {view.clarity} < 阈值 {s.min_clarity}")

    candidates: list[tuple[str, str, float, float]] = []  # outcome, token, p, ask
    if book.yes_ask is not None:
        candidates.append((book.yes_label.upper(), book.yes_token_id, view.p_yes, book.yes_ask))
    if book.no_ask is not None:
        candidates.append((book.no_label.upper(), book.no_token_id, 1 - view.p_yes, book.no_ask))
    if not candidates:
        return Skip("两个方向都没有可成交卖价")
    in_band = [c for c in candidates if s.min_trade_price <= c[3] <= s.max_trade_price]
    if not in_band:
        asks = ", ".join(f"{o} ask {a:.2f}" for o, _, _, a in candidates)
        return Skip(f"价格超出交易区间 [{s.min_trade_price}, {s.max_trade_price}]: {asks}")
    candidates = in_band

    outcome, token, p, ask = max(candidates, key=lambda c: c[2] - c[3])
    edge = p - ask
    if edge < s.min_edge:
        return Skip(f"最佳优势 {edge:+.3f}（{outcome} 模型概率={p:.2f} 卖价={ask:.2f}）< 阈值 {s.min_edge}")

    # Sizing: fractional Kelly on the bankroll, hard-capped per trade.
    bankroll = bankroll_usd if bankroll_usd is not None else s.max_open_exposure_usd
    f = kelly_fraction(p, ask) * s.kelly_fraction
    usd = min(s.max_usd_per_trade, f * bankroll)
    if usd <= 0:
        return Skip("凯利仓位计算结果为 0")

    price = round_to_tick(ask, book.tick_size)
    size = math.floor(usd / price * 100) / 100  # 2-dp shares
    if size < book.min_order_size:
        size = book.min_order_size
    usd = round(price * size, 4)
    if usd > s.max_usd_per_trade * 1.5:
        # min_order_size forced us well past the cap; refuse.
        return Skip(f"最小下单量 {book.min_order_size} × {price} = ${usd:.2f} 超过单笔上限")

    return Trade(
        outcome=outcome,
        token_id=token,
        side="BUY",
        price=price,
        size=size,
        usd=usd,
        p=p,
        edge=edge,
        rationale=(
            f"Jev P({outcome})={p:.2f}，卖价={ask:.2f}，优势={edge:+.2f}；"
            f"信息充分度={view.answerable:.2f}，规则清晰度={view.clarity}"
        ),
    )


async def market_data_and_ask(
    c: Candidate,
    s: Settings,
    jev: JevClient,
    snapshot: ShortTermSnapshot,
) -> tuple[dict, JevView]:
    """Build an odds-independent short-term state from authoritative price data."""
    from .markets import build_state

    state = build_state(c, s, brief=None)
    state.pop("market_implied_probability_primary", None)
    state["short_term_market_data"] = snapshot.to_state()
    view = await ask_jev(jev, state)
    return state, view


async def get_brief(c: Candidate, s: Settings, store: Store, researcher: Researcher | None,
                    fresh: bool = False) -> tuple[Brief | None, bool]:
    """Return (brief, from_cache). None if research is disabled, over budget, or failed."""
    if researcher is None:
        return None, False
    if not fresh:
        cached = store.get_brief(c.slug, s.research_ttl_hours * 3600)
        if cached is not None:
            b = Brief.from_json(cached, model=cached.get("model", ""), cost=0.0)
            b.sources = list(cached.get("sources") or [])
            return b, True
    if not researcher.budget_left:
        log.info("%s: research budget exhausted for this run", c.slug)
        return None, False
    m = c.market
    try:
        b = await researcher.brief(
            question=m.question,
            description=(m.description or "")[: s.description_max_chars * 2],
            resolution_source=m.resolution.source if m.resolution else None,
            start_date=m.state.start_date.date().isoformat() if m.state.start_date else None,
            end_date=m.state.end_date.date().isoformat() if m.state.end_date else None,
            today=datetime.now(UTC).date().isoformat(),
        )
    except ResearchError as e:
        if e.status in (401, 402):
            raise
        log.warning("%s: research failed: %s", c.slug, e)
        return None, False
    store.put_brief(c.slug, b.model, b.to_dict(), b.cost)
    return b, False


async def research_and_ask(c: Candidate, s: Settings, store: Store,
                           researcher: Researcher | None, jev: JevClient,
                           fresh: bool = False) -> tuple[dict, Brief | None, bool, JevView]:
    """The one path from candidate to Jev view: research (cached) -> state -> Jev."""
    from .markets import build_state

    brief, cached = await get_brief(c, s, store, researcher, fresh=fresh)
    state = build_state(c, s, brief)
    view = await ask_jev(jev, state)
    return state, brief, cached, view
