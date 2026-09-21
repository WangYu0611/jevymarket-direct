"""Discover candidate Polymarket markets and turn them into compact Jev states."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

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


_EPOCH_WINDOW_RE = re.compile(r"^btc-updown-(5m|15m|1h)-(\d+)$")
_HOURLY_SLUG_RE = re.compile(
    r"^bitcoin-up-or-down-([a-z]+)-(\d{1,2})-(\d{4})-(\d{1,2})(am|pm)-et$"
)
_WINDOW_MINUTES = {"5m": 5, "15m": 15, "1h": 60}


def market_window(m: Market, s: Settings) -> tuple[datetime, datetime] | None:
    """从 recurring market slug 解析真实交易时间窗。"""
    timeframe = market_timeframe(m, s)
    if timeframe is None:
        return None

    slug = (m.slug or "").lower()
    match = _EPOCH_WINDOW_RE.fullmatch(slug)
    if match:
        start = datetime.fromtimestamp(int(match.group(2)), tz=UTC)
        return start, start + timedelta(minutes=_WINDOW_MINUTES[timeframe])

    match = _HOURLY_SLUG_RE.fullmatch(slug)
    if timeframe == "1h" and match:
        month, day, year, hour, ampm = match.groups()
        try:
            naive = datetime.strptime(
                f"{month.title()} {day} {year} {hour}{ampm.upper()}",
                "%B %d %Y %I%p",
            )
        except ValueError:
            return None
        start = naive.replace(tzinfo=ZoneInfo("America/New_York")).astimezone(UTC)
        return start, start + timedelta(hours=1)

    return None


def market_is_current(m: Market, s: Settings, now: datetime | None = None) -> bool:
    """只接受此刻正在进行的短周期窗口，不接受未来预创建或刚结束市场。"""
    window = market_window(m, s)
    if window is None:
        return False
    current = now or datetime.now(UTC)
    if current.tzinfo is None:
        current = current.replace(tzinfo=UTC)
    else:
        current = current.astimezone(UTC)
    start, end = window
    return start <= current < end


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


def passes_static_filters(m: Market, s: Settings, now: datetime | None = None) -> str | None:
    """Return a skip reason, or None if the market is a candidate."""
    st = m.state
    if market_asset_symbol(m, s) != "BTC":
        return "不是 BTC 市场"
    timeframe = market_timeframe(m, s)
    if timeframe is None:
        return f"不是允许的 BTC 短周期市场 [{s.allowed_timeframes}]"
    if not market_is_current(m, s, now=now):
        return "不是当前正在进行的时间窗"
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


def current_market_slugs(s: Settings, now: datetime | None = None) -> list[str]:
    """根据当前 UTC 时间直接构造 BTC 5m / 15m / 1h 市场 slug。"""
    current = now or datetime.now(UTC)
    if current.tzinfo is None:
        current = current.replace(tzinfo=UTC)
    else:
        current = current.astimezone(UTC)

    slugs: list[str] = []
    allowed = _allowed_timeframes(s)

    if "5m" in allowed:
        start = int(current.timestamp()) // 300 * 300
        slugs.append(f"btc-updown-5m-{start}")

    if "15m" in allowed:
        start = int(current.timestamp()) // 900 * 900
        slugs.append(f"btc-updown-15m-{start}")

    if "1h" in allowed:
        eastern = current.astimezone(ZoneInfo("America/New_York"))
        hour = eastern.hour % 12 or 12
        ampm = "am" if eastern.hour < 12 else "pm"
        month = eastern.strftime("%B").lower()
        slugs.append(
            f"bitcoin-up-or-down-{month}-{eastern.day}-{eastern.year}-{hour}{ampm}-et"
        )

    return slugs


async def scan(client: AsyncPublicClient, s: Settings, limit: int = 20, pages: int = 5) -> list[Candidate]:
    """直接加载当前 BTC 5m / 15m / 1h 窗口，不分页扫描未来 recurring markets。"""
    del pages  # 保留 CLI 兼容性；当前窗口模式不再需要分页。
    out: list[Candidate] = []
    now = datetime.now(UTC)

    for slug in current_market_slugs(s, now):
        try:
            m = await client.get_market(slug=slug)
        except Exception as e:  # noqa: BLE001
            log.debug("当前市场 %s 尚不可用：%s", slug, e)
            continue

        why = passes_static_filters(m, s, now=now)
        if why:
            log.debug("跳过 %s：%s", m.slug, why)
            continue

        try:
            book = await fetch_book(client, m)
        except Exception as e:  # noqa: BLE001
            log.warning("获取盘口失败 %s：%s", m.slug, e)
            continue

        if book.spread is not None and book.spread > s.max_spread:
            log.debug("跳过 %s：实时点差 %.2f", m.slug, book.spread)
            continue

        out.append(Candidate(market=m, book=book))
        if len(out) >= limit:
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
    """Compact Jev state with exact intraday timestamps for short-horizon markets."""
    m = c.market
    desc = (m.description or "").strip()
    if len(desc) > s.description_max_chars:
        desc = desc[: s.description_max_chars].rsplit(" ", 1)[0] + " …"

    now = datetime.now(UTC)
    state: dict = {
        "question": m.question,
        "description": desc,
        "as_of_time": now.isoformat(),
        "primary_outcome": str(m.outcomes.yes.label or "Up"),
        "secondary_outcome": str(m.outcomes.no.label or "Down"),
        "timeframe": market_timeframe(m, s),
    }

    window = market_window(m, s)
    if window is not None:
        start, end = window
        state["market_start_time"] = start.isoformat()
        state["market_end_time"] = end.isoformat()
        state["seconds_until_resolution"] = max(0, int((end - now).total_seconds()))
    else:
        if m.state.start_date:
            start = m.state.start_date
            if start.tzinfo is None:
                start = start.replace(tzinfo=UTC)
            state["market_start_time"] = start.astimezone(UTC).isoformat()
        if m.state.end_date:
            end = m.state.end_date
            if end.tzinfo is None:
                end = end.replace(tzinfo=UTC)
            end = end.astimezone(UTC)
            state["market_end_time"] = end.isoformat()
            state["seconds_until_resolution"] = max(0, int((end - now).total_seconds()))

    if m.resolution and m.resolution.source:
        state["resolution_source"] = m.resolution.source
    mid = c.book.midpoint
    if mid is not None:
        state["market_implied_probability_primary"] = round(mid, 2)
    if brief is not None:
        state["evidence"] = brief.to_state(s.research_max_chars)
    return state
