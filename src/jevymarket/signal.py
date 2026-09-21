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
        "Given the market question, its description and resolution rules, this market "
        "will resolve YES. If `market_start_date` is present, treat events before that date "
        "as background unless the resolution rules explicitly require a lookback. If an "
        "`evidence` brief is present, weigh its dated facts and latest development against "
        "`days_until_resolution`."
    ),
    "answerable": noul(
        "The state (including the `evidence` brief, if present) contains enough current and "
        "relevant information to form a well-informed probability estimate for this question. "
        "This is about information sufficiency, not certainty: an uncertain outcome can still "
        "be well-informed. Answer NO only if key facts needed to estimate it are missing, stale, "
        "or would require news that is not in the state."
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
    outcome: str           # "YES" | "NO"
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
        return Skip(f"answerable {view.answerable:.2f} < {s.min_answerable}")
    if view.clarity < s.min_clarity:
        return Skip(f"clarity {view.clarity} < {s.min_clarity}")

    candidates: list[tuple[str, str, float, float]] = []  # outcome, token, p, ask
    if book.yes_ask is not None:
        candidates.append(("YES", book.yes_token_id, view.p_yes, book.yes_ask))
    if book.no_ask is not None:
        candidates.append(("NO", book.no_token_id, 1 - view.p_yes, book.no_ask))
    if not candidates:
        return Skip("no asks on either side")
    in_band = [c for c in candidates if s.min_trade_price <= c[3] <= s.max_trade_price]
    if not in_band:
        asks = ", ".join(f"{o} ask {a:.2f}" for o, _, _, a in candidates)
        return Skip(f"outside trade band [{s.min_trade_price}, {s.max_trade_price}]: {asks}")
    candidates = in_band

    outcome, token, p, ask = max(candidates, key=lambda c: c[2] - c[3])
    edge = p - ask
    if edge < s.min_edge:
        return Skip(f"best edge {edge:+.3f} ({outcome} p={p:.2f} ask={ask:.2f}) < {s.min_edge}")

    # Sizing: fractional Kelly on the bankroll, hard-capped per trade.
    bankroll = bankroll_usd if bankroll_usd is not None else s.max_open_exposure_usd
    f = kelly_fraction(p, ask) * s.kelly_fraction
    usd = min(s.max_usd_per_trade, f * bankroll)
    if usd <= 0:
        return Skip("kelly sizing gave zero")

    price = round_to_tick(ask, book.tick_size)
    size = math.floor(usd / price * 100) / 100  # 2-dp shares
    if size < book.min_order_size:
        size = book.min_order_size
    usd = round(price * size, 4)
    if usd > s.max_usd_per_trade * 1.5:
        # min_order_size forced us well past the cap; refuse.
        return Skip(f"min order size {book.min_order_size} x {price} = ${usd:.2f} exceeds cap")

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
            f"Jev P({outcome})={p:.2f} vs ask {ask:.2f} -> edge {edge:+.2f}; "
            f"answerable={view.answerable:.2f} clarity={view.clarity}"
        ),
    )


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
