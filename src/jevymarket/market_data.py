"""Realtime reference data for BTC short-term Polymarket markets."""

from __future__ import annotations

import asyncio
import logging
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
    """Extract Polymarket's canonical opening reference price from raw Gamma event JSON."""
    if not isinstance(payload, dict):
        return None

    containers: list[dict[str, Any]] = [payload]
    for key in ("eventMetadata", "event_metadata", "metadata"):
        value = payload.get(key)
        if isinstance(value, dict):
            containers.append(value)

    markets = payload.get("markets")
    if isinstance(markets, list):
        for market in markets:
            if not isinstance(market, dict):
                continue
            containers.append(market)
            for key in ("eventMetadata", "event_metadata", "metadata"):
                value = market.get(key)
                if isinstance(value, dict):
                    containers.append(value)

    for container in containers:
        for key in ("priceToBeat", "price_to_beat", "openPrice", "open_price"):
            price = _float_or_none(container.get(key))
            if price is not None:
                return price
    return None


async def fetch_price_to_beat(
    http: httpx.AsyncClient,
    slug: str,
    *,
    timeframe: str,
    window_start: datetime | None,
) -> tuple[float | None, str | None]:
    """Fetch canonical Polymarket anchor; use Binance 1H open only for the hourly fallback."""
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

    # The hourly market resolves from Binance, so Binance's exact 1H open is a valid
    # fallback. We deliberately do NOT use Binance as a fallback for Chainlink 5m/15m.
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
    except (TimeoutError, asyncio.TimeoutError):
        return None
    except Exception as exc:  # noqa: BLE001
        log.debug("Realtime reference-price stream failed: %s", exc)
        return None
    return None


async def fetch_reference_prices(
    client: AsyncPublicClient,
    *,
    need_chainlink: bool,
    need_binance: bool,
    timeout_seconds: float = 3.0,
) -> tuple[float | None, float | None]:
    tasks: list[asyncio.Task[float | None]] = []
    labels: list[str] = []

    if need_chainlink:
        tasks.append(asyncio.create_task(_first_stream_price(
            client,
            CryptoPricesChainlinkTwapSpec(window_seconds=60, symbols=["btc/usd"]),
            timeout_seconds,
        )))
        labels.append("chainlink")

    if need_binance:
        tasks.append(asyncio.create_task(_first_stream_price(
            client,
            CryptoPricesSpec(topic="prices.crypto.binance", symbols=["btcusdt"]),
            timeout_seconds,
        )))
        labels.append("binance")

    if not tasks:
        return None, None

    values = await asyncio.gather(*tasks)
    by_label = dict(zip(labels, values, strict=True))
    return by_label.get("chainlink"), by_label.get("binance")


async def fetch_short_term_snapshots(
    client: AsyncPublicClient,
    candidates: list[Candidate],
    settings: Settings,
    *,
    now: datetime | None = None,
    timeout_seconds: float = 3.0,
) -> dict[str, ShortTermSnapshot]:
    captured_at = now or datetime.now(UTC)
    if captured_at.tzinfo is None:
        captured_at = captured_at.replace(tzinfo=UTC)
    else:
        captured_at = captured_at.astimezone(UTC)

    timeframes = {market_timeframe(c.market, settings) for c in candidates}
    need_chainlink = bool(timeframes & {"5m", "15m"})
    need_binance = "1h" in timeframes

    chainlink_price, binance_price = await fetch_reference_prices(
        client,
        need_chainlink=need_chainlink,
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

    for (candidate, timeframe, window), (target, target_source) in zip(
        target_meta, targets, strict=True
    ):
        if timeframe in {"5m", "15m"}:
            current = chainlink_price
            current_source = "Chainlink BTC/USD TWAP 60s" if current is not None else None
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
