"""Discover candidate Polymarket markets and turn them into compact Jev states."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import UTC, datetime

from polymarket import AsyncPublicClient
from polymarket.models.clob.order_book import OrderBook
from polymarket.models.gamma.market import Market

from .config import Settings
from .research import Brief
from .signal import Book

log = logging.getLogger(__name__)

ASSET_ALIASES: dict[str, set[str]] = {
    "BTC": {"bitcoin", "btc"},
}

TIMEFRAME_LABELS = {
    "5m": "5分钟",
    "15m": "15分钟",
    "1h": "1小时",
}


def _allowed_asset_symbols(s: Settings) -> tuple[str, ...]:
    return tuple(
        symbol
        for raw in s.allowed_assets.split(",")
        if (symbol := raw.strip().upper())
    )


def _allowed_timeframes(s: Settings) -> tuple[str, ...]:
    return tuple(
        timeframe
        for raw in s.allowed_timeframes.split(",")
        if (timeframe := raw.strip().lower())
    )


def market_timeframe(m: Market, s: Settings) -> str | None:
    """识别支持的 BTC 短周期涨跌市场。"""
    slug = (m.slug or "").lower()
    desc = (m.description or "").lower()

    if slug.startswith("btc-updown-5m-"):
        timeframe = "5m"
    elif slug.startswith("btc-updown-15m-"):
        timeframe = "15m"
    elif slug.startswith("btc-updown-1h-"):
        timeframe = "1h"
    elif slug.startswith("bitcoin-up-or-down-") and (
        "1 hour candle" in desc or '"1h" candle' in desc or "relevant 1h candle" in desc
    ):
        timeframe = "1h"
    else:
        return None

    return timeframe if timeframe in _allowed_timeframes(s) else None


def market_asset_symbol(m: Market, s: Settings) -> str | None:
    """Return the allowed crypto asset referenced by a market, or None.

    Matching is token-based so short tickers such as ETH and SOL cannot match
    substrings inside unrelated words.
    """
    allowed = _allowed_asset_symbols(s)
    if not allowed:
        return None

    parts = [m.question or "", m.slug or "", m.category or ""]
    for tag in m.tags or ():
        parts.extend([tag.slug or "", tag.label or ""])
    tokens = set(re.findall(r"[a-z0-9]+", " ".join(parts).lower()))

    for symbol in allowed:
        aliases = ASSET_ALIASES.get(symbol, {symbol.lower()})
        if tokens & aliases:
            return symbol
    return None


@dataclass(frozen=True)
class Candidate:
    market: Market
    book: Book

    @property
    def slug(self) -> str:
        return self.market.slug

    @property
    def condition_id(self) -> str:
        return str(self.market.condition_id)

    @property
    def question(self) -> str:
        return self.market.question

    @property
    def days_to_resolution(self) -> int | None:
        return _days_until(self.market.state.end_date)


def _days_until(dt: datetime | None) -> int | None:
    if dt is None:
        return None
    now = datetime.now(UTC)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return max(0, (dt - now).days)


def _f(x) -> float | None:
    return None if x is None else float(x)


def _best(levels, pick) -> float | None:
    prices = [float(lvl.price) for lvl in levels or []]
    return pick(prices) if prices else None


def book_from_orderbooks(market: Market, books: list[OrderBook] | tuple[OrderBook, ...]) -> Book:
    """Match books to the market's YES/NO tokens by asset id (order is not guaranteed)."""
    yes_id = market.outcomes.yes.token_id
    no_id = market.outcomes.no.token_id
    by_id = {str(b.asset_id): b for b in books}
    yb, nb = by_id.get(str(yes_id)), by_id.get(str(no_id))
    ref = yb or nb
    return Book(
        yes_token_id=str(yes_id),
        no_token_id=str(no_id),
        yes_bid=_best(yb.bids, max) if yb else None,
        yes_ask=_best(yb.asks, min) if yb else None,
        no_bid=_best(nb.bids, max) if nb else None,
        no_ask=_best(nb.asks, min) if nb else None,
        tick_size=float(ref.tick_size) if ref and ref.tick_size else float(market.trading.minimum_tick_size or 0.01),
        min_order_size=float(ref.min_order_size) if ref and ref.min_order_size else float(market.trading.minimum_order_size or 5),
        yes_label=str(market.outcomes.yes.label or "UP"),
        no_label=str(market.outcomes.no.label or "DOWN"),
    )


def passes_static_filters(m: Market, s: Settings) -> str | None:
    """Return a skip reason, or None if the market is a candidate."""
    st = m.state
    if market_asset_symbol(m, s) != "BTC":
        return "不是 BTC 市场"
    timeframe = market_timeframe(m, s)
    if timeframe is None:
        return f"不是允许的 BTC 短周期市场 [{s.allowed_timeframes}]"
    if not (st.active and st.accepting_orders) or st.closed or st.archived:
        return "not tradable"
    if not st.enable_order_book:
        return "no CLOB order book"
    if not (m.outcomes and m.outcomes.yes and m.outcomes.no and m.outcomes.yes.token_id and m.outcomes.no.token_id):
        return "缺少两个方向的交易 token"
    labels = ((m.outcomes.yes.label or "").strip().lower(), (m.outcomes.no.label or "").strip().lower())
    if labels != ("up", "down"):
        return f"结果标签不是 Up/Down，而是 {m.outcomes.yes.label}/{m.outcomes.no.label}"
    liq = _f(m.metrics.liquidity_num) or 0.0
    vol = _f(m.metrics.volume_num) or 0.0
    if liq < s.short_term_min_liquidity_usd:
        return f"流动性 ${liq:,.0f} < ${s.short_term_min_liquidity_usd:,.0f}"
    if vol < s.short_term_min_volume_usd:
        return f"成交量 ${vol:,.0f} < ${s.short_term_min_volume_usd:,.0f}"
    days = _days_until(st.end_date)
    if days is None:
        return "no end date"
    if days > s.max_days_to_resolution:
        return f"resolves in {days}d > {s.max_days_to_resolution}d"
    spread = _f(m.prices.spread)
    if spread is not None and spread > s.max_spread:
        return f"spread {spread:.2f} > {s.max_spread}"
    px = _f(m.outcomes.yes.price)
    if px is not None and not (s.min_market_price <= px <= s.max_market_price):
        return f"yes price {px:.3f} outside [{s.min_market_price}, {s.max_market_price}]"
    return None


async def fetch_book(client: AsyncPublicClient, m: Market) -> Book:
    books = await client.get_order_books(
        token_ids=[str(m.outcomes.yes.token_id), str(m.outcomes.no.token_id)]
    )
    return book_from_orderbooks(m, books)


async def scan(client: AsyncPublicClient, s: Settings, limit: int = 20, pages: int = 5) -> list[Candidate]:
    """Walk the most-traded open markets and keep those passing filters + a live book."""
    out: list[Candidate] = []
    paginator = client.list_markets(
        closed=False,
        order="startDate",
        ascending=False,
        page_size=100,
    )
    seen_pages = 0
    async for page in paginator:
        seen_pages += 1
        for m in page.items:
            why = passes_static_filters(m, s)
            if why:
                log.debug("skip %s: %s", m.slug, why)
                continue
            try:
                book = await fetch_book(client, m)
            except Exception as e:  # noqa: BLE001
                log.warning("book fetch failed for %s: %s", m.slug, e)
                continue
            if book.spread is not None and book.spread > s.max_spread:
                log.debug("skip %s: live spread %.2f", m.slug, book.spread)
                continue
            out.append(Candidate(market=m, book=book))
            if len(out) >= limit:
                return out
        if seen_pages >= pages:
            break
    return out


async def load_candidate(client: AsyncPublicClient, s: Settings, ref: str) -> Candidate:
    """Load one market by slug or polymarket.com URL, bypassing liquidity filters."""
    if ref.startswith("http"):
        m = await client.get_market(url=ref)
    else:
        m = await client.get_market(slug=ref)
    if not (m.outcomes and m.outcomes.yes and m.outcomes.no):
        raise ValueError(f"{m.slug} 不是二元市场")
    if market_asset_symbol(m, s) != "BTC" or market_timeframe(m, s) is None:
        raise ValueError(f"{m.slug} 不是允许的 BTC 5分钟/15分钟/1小时涨跌市场")
    labels = ((m.outcomes.yes.label or "").strip().lower(), (m.outcomes.no.label or "").strip().lower())
    if labels != ("up", "down"):
        raise ValueError(f"{m.slug} 的结果不是 Up/Down")
    return Candidate(market=m, book=await fetch_book(client, m))


def build_state(c: Candidate, s: Settings, brief: Brief | None = None) -> dict:
    """Compact state for Jev. Numbers are pre-computed; nothing the question doesn't need.
    When a research brief is available it goes in as `evidence`."""
    m = c.market
    desc = (m.description or "").strip()
    if len(desc) > s.description_max_chars:
        desc = desc[: s.description_max_chars].rsplit(" ", 1)[0] + " …"
    state: dict = {
        "question": m.question,
        "description": desc,
        "today": datetime.now(UTC).date().isoformat(),
        "days_until_resolution": c.days_to_resolution,
        "primary_outcome": str(m.outcomes.yes.label or "Up"),
        "secondary_outcome": str(m.outcomes.no.label or "Down"),
        "timeframe": market_timeframe(m, s),
    }
    if m.state.start_date:
        state["market_start_date"] = m.state.start_date.date().isoformat()
    if m.resolution and m.resolution.source:
        state["resolution_source"] = m.resolution.source
    mid = c.book.midpoint
    if mid is not None:
        state["market_implied_probability_primary"] = round(mid, 2)
    if brief is not None:
        state["evidence"] = brief.to_state(s.research_max_chars)
    return state
