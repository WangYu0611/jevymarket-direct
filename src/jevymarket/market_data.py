"""Realtime reference data for BTC short-term Polymarket markets."""

from __future__ import annotations

import asyncio
import json
import logging
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import httpx
from polymarket import AsyncPublicClient
from polymarket.streams import CryptoPricesChainlinkTwapSpec, CryptoPricesSpec

from .config import Settings
from .markets import market_timeframe, market_window

if TYPE_CHECKING:
    from .markets import Candidate

log = logging.getLogger(__name__)

GAMMA_EVENTS_URL = "https://gamma-api.polymarket.com/events"
POLYMARKET_EVENT_URL = "https://polymarket.com/event/{slug}"
BINANCE_KLINES_URLS = (
    "https://api.binance.com/api/v3/klines",
    "https://api.binance.us/api/v3/klines",
)


@dataclass(frozen=True)
class ShortTermSnapshot:
    target_price: float | None
    current_price: float | None
    seconds_left: int | None
    target_source: str | None
    current_source: str | None
    captured_at: datetime

    @property
    def delta_usd(self) -> float | None:
        if self.target_price is None or self.current_price is None:
            return None
        return self.current_price - self.target_price


def _float_or_none(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number > 0 else None


def extract_price_to_beat(payload: Any) -> float | None:
    """Extract Polymarket's canonical opening reference price from exact-event Gamma JSON."""
    price_keys = ("priceToBeat", "price_to_beat", "openPrice", "open_price")

    def walk(value: Any) -> float | None:
        if isinstance(value, dict):
            for key in price_keys:
                price = _float_or_none(value.get(key))
                if price is not None:
                    return price
            for child in value.values():
                found = walk(child)
                if found is not None:
                    return found
            return None

        if isinstance(value, list):
            for child in value:
                found = walk(child)
                if found is not None:
                    return found
            return None

        if isinstance(value, str) and value.lstrip().startswith(("{", "[")):
            try:
                return walk(json.loads(value))
            except json.JSONDecodeError:
                return None

        return None

    return walk(payload)


_NEXT_DATA_RE = re.compile(
    r'<script[^>]*\bid=["\']__NEXT_DATA__["\'][^>]*>(.*?)</script>',
    re.IGNORECASE | re.DOTALL,
)


def extract_live_open_price(next_data: Any, slug: str, window_start: datetime | None) -> float | None:
    """Find the exact React Query crypto-prices openPrice for this active window."""
    candidates: list[tuple[int, int, float]] = []
    start_epoch = str(int(window_start.timestamp())) if window_start is not None else ""
    start_iso = (
        window_start.astimezone(UTC).isoformat().replace("+00:00", "Z")
        if window_start else ""
    )
    slug_l = slug.lower()

    def walk(value: Any) -> None:
        if isinstance(value, dict):
            query_key = value.get("queryKey")
            if query_key is not None:
                key_text = json.dumps(query_key, ensure_ascii=False).lower()
                if "crypto-prices" in key_text:
                    state = value.get("state")
                    data = state.get("data") if isinstance(state, dict) else None
                    open_price = extract_price_to_beat(data)
                    if open_price is not None:
                        score = 4
                        context_text = json.dumps(value, ensure_ascii=False).lower()
                        if slug_l and slug_l in context_text:
                            score += 8
                        if start_epoch and start_epoch in context_text:
                            score += 6
                        if start_iso and start_iso.lower() in context_text:
                            score += 6
                        # Prefer the smallest matching query object when scores tie,
                        # avoiding a broad parent that contains neighboring windows.
                        candidates.append((score, -len(context_text), open_price))

            for child in value.values():
                walk(child)
        elif isinstance(value, list):
            for child in value:
                walk(child)

    walk(next_data)
    if not candidates:
        return None
    candidates.sort(reverse=True)
    best_score, _, best_price = candidates[0]
    return best_price if best_score >= 4 else None


def extract_live_open_price_from_html(
    html: str,
    slug: str,
    window_start: datetime | None,
) -> float | None:
    match = _NEXT_DATA_RE.search(html)
    if match is None:
        return None
    try:
        payload = json.loads(match.group(1))
    except json.JSONDecodeError:
        return None
    return extract_live_open_price(payload, slug, window_start)


async def fetch_polymarket_live_open_price(
    http: httpx.AsyncClient,
    slug: str,
    window_start: datetime | None,
) -> float | None:
    try:
        response = await http.get(
            POLYMARKET_EVENT_URL.format(slug=slug),
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 Chrome/140 Safari/537.36"
                )
            },
        )
        response.raise_for_status()
        return extract_live_open_price_from_html(response.text, slug, window_start)
    except Exception as exc:  # noqa: BLE001
        log.debug("Polymarket live openPrice unavailable for %s: %s", slug, exc)
        return None


def chainlink_twap_window_seconds(candidate: Candidate) -> int:
    """Read the market's own resolution wording/source instead of assuming 60s."""
    market = candidate.market
    text = " ".join([
        market.description or "",
        market.resolution.source if market.resolution else "",
    ]).lower()
    if "twap-30s" in text or "twap 30s" in text or "30-second" in text or "30 second" in text:
        return 30
    return 60


async def fetch_price_to_beat(
    http: httpx.AsyncClient,
    slug: str,
    *,
    timeframe: str,
    window_start: datetime | None,
) -> tuple[float | None, str | None]:
    """Fetch the canonical opening anchor for the exact Polymarket window."""
    # Active Chainlink markets expose the page's exact crypto-prices openPrice in
    # __NEXT_DATA__. This is the same anchor rendered as "Price to Beat" on the UI.
    if timeframe in {"5m", "15m"}:
        live = await fetch_polymarket_live_open_price(http, slug, window_start)
        if live is not None:
            return live, "Polymarket 页面 openPrice"

    # Gamma often gains priceToBeat metadata later (especially after resolution).
    try:
        response = await http.get(GAMMA_EVENTS_URL, params={"slug": slug})
        response.raise_for_status()
        raw = response.json()
        event = raw[0] if isinstance(raw, list) and raw else raw
        price = extract_price_to_beat(event)
        if price is not None:
            return price, "Polymarket priceToBeat"
    except Exception as exc:  # noqa: BLE001
        log.debug("Gamma priceToBeat unavailable for %s: %s", slug, exc)

    if timeframe == "1h" and window_start is not None:
        price = await fetch_binance_hour_open(http, window_start)
        if price is not None:
            return price, "Binance 1H open"

    return None, None


async def fetch_binance_hour_open(http: httpx.AsyncClient, start: datetime) -> float | None:
    start_utc = start.astimezone(UTC)
    start_ms = int(start_utc.timestamp() * 1000)
    params = {
        "symbol": "BTCUSDT",
        "interval": "1h",
        "startTime": start_ms,
        "limit": 1,
    }
    for url in BINANCE_KLINES_URLS:
        try:
            response = await http.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            if isinstance(data, list) and data and isinstance(data[0], list) and len(data[0]) > 1:
                price = _float_or_none(data[0][1])
                if price is not None:
                    return price
        except Exception as exc:  # noqa: BLE001
            log.debug("Binance 1H open unavailable from %s: %s", url, exc)
    return None


async def _first_stream_price(
    client: AsyncPublicClient,
    spec: CryptoPricesChainlinkTwapSpec | CryptoPricesSpec,
    timeout_seconds: float,
) -> float | None:
    try:
        async with await client.subscribe(spec) as stream:
            async with asyncio.timeout(timeout_seconds):
                async for event in stream:
                    price = _float_or_none(event.payload.value)
                    if price is not None:
                        return price
    except TimeoutError:
        return None
    except Exception as exc:  # noqa: BLE001
        log.debug("Realtime reference-price stream failed: %s", exc)
        return None
    return None


async def fetch_reference_prices(
    client: AsyncPublicClient,
    *,
    chainlink_windows: set[int],
    need_binance: bool,
    timeout_seconds: float = 3.0,
) -> tuple[dict[int, float | None], float | None]:
    tasks: list[asyncio.Task[float | None]] = []
    labels: list[str] = []

    for window_seconds in sorted(chainlink_windows):
        tasks.append(asyncio.create_task(_first_stream_price(
            client,
            CryptoPricesChainlinkTwapSpec(
                window_seconds=window_seconds,
                symbols=["btc/usd"],
            ),
            timeout_seconds,
        )))
        labels.append(f"chainlink-{window_seconds}")

    if need_binance:
        tasks.append(asyncio.create_task(_first_stream_price(
            client,
            CryptoPricesSpec(topic="prices.crypto.binance", symbols=["btcusdt"]),
            timeout_seconds,
        )))
        labels.append("binance")

    if not tasks:
        return {}, None

    values = await asyncio.gather(*tasks)
    by_label = dict(zip(labels, values, strict=True))
    chainlink = {
        window: by_label.get(f"chainlink-{window}")
        for window in chainlink_windows
    }
    return chainlink, by_label.get("binance")


async def fetch_short_term_snapshots(
    client: AsyncPublicClient,
    candidates: list[Candidate],
    settings: Settings,
    *,
    now: datetime | None = None,
    timeout_seconds: float = 3.0,
) -> dict[str, ShortTermSnapshot]:
    requested_at = now
    timeframes = {market_timeframe(c.market, settings) for c in candidates}
    chainlink_windows = {
        chainlink_twap_window_seconds(candidate)
        for candidate in candidates
        if market_timeframe(candidate.market, settings) in {"5m", "15m"}
    }
    need_binance = "1h" in timeframes

    chainlink_prices, binance_price = await fetch_reference_prices(
        client,
        chainlink_windows=chainlink_windows,
        need_binance=need_binance,
        timeout_seconds=timeout_seconds,
    )

    snapshots: dict[str, ShortTermSnapshot] = {}
    async with httpx.AsyncClient(timeout=4.0, follow_redirects=True) as http:
        target_tasks = []
        target_meta: list[tuple[Candidate, str | None, tuple[datetime, datetime] | None]] = []

        for candidate in candidates:
            timeframe = market_timeframe(candidate.market, settings)
            window = market_window(candidate.market, settings)
            target_meta.append((candidate, timeframe, window))
            target_tasks.append(fetch_price_to_beat(
                http,
                candidate.slug,
                timeframe=timeframe or "",
                window_start=window[0] if window else None,
            ))

        targets = await asyncio.gather(*target_tasks) if target_tasks else []

    captured_at = requested_at or datetime.now(UTC)
    if captured_at.tzinfo is None:
        captured_at = captured_at.replace(tzinfo=UTC)
    else:
        captured_at = captured_at.astimezone(UTC)

    for (candidate, timeframe, window), (target, target_source) in zip(
        target_meta, targets, strict=True
    ):
        if timeframe in {"5m", "15m"}:
            twap_window = chainlink_twap_window_seconds(candidate)
            current = chainlink_prices.get(twap_window)
            current_source = (
                f"Chainlink BTC/USD TWAP {twap_window}s"
                if current is not None else None
            )
        elif timeframe == "1h":
            current = binance_price
            current_source = "Binance BTC/USDT" if current is not None else None
        else:
            current = None
            current_source = None

        seconds_left = None
        if window is not None:
            seconds_left = max(0, int((window[1] - captured_at).total_seconds()))

        snapshots[candidate.slug] = ShortTermSnapshot(
            target_price=target,
            current_price=current,
            seconds_left=seconds_left,
            target_source=target_source,
            current_source=current_source,
            captured_at=captured_at,
        )

    return snapshots
