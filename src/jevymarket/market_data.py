"""Authoritative short-term BTC reference data.

5m/15m:
- Current price: Polymarket RTDS Chainlink BTC/USD TWAP stream.
- Target price: first trusted Chainlink TWAP captured at the window boundary and
  persisted locally. If that anchor is missing, the window is not trade-ready.

1h:
- Target: Binance BTC/USDT 1H open.
- Current: Binance BTC/USDT realtime stream.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Callable

import httpx
from polymarket import AsyncPublicClient
from polymarket.streams import CryptoPricesChainlinkTwapSpec, CryptoPricesSpec

from .config import Settings
from .markets import market_timeframe, market_window
from .store import Store

if TYPE_CHECKING:
    from .markets import Candidate

log = logging.getLogger(__name__)

GAMMA_EVENTS_URL = "https://gamma-api.polymarket.com/events"
BINANCE_KLINES_URLS = (
    "https://api.binance.com/api/v3/klines",
    "https://api.binance.us/api/v3/klines",
)

_WINDOW_SECONDS = {"5m": 300, "15m": 900}


@dataclass(frozen=True)
class ShortTermSnapshot:
    target_price: float | None
    current_price: float | None
    seconds_left: int | None
    target_source: str | None
    current_source: str | None
    captured_at: datetime
    twap_window_seconds: int | None = None

    @property
    def delta_usd(self) -> float | None:
        if self.target_price is None or self.current_price is None:
            return None
        return self.current_price - self.target_price

    @property
    def delta_pct(self) -> float | None:
        if self.target_price is None or self.current_price is None or self.target_price == 0:
            return None
        return (self.current_price / self.target_price - 1.0) * 100.0

    @property
    def trade_ready(self) -> bool:
        return (
            self.target_price is not None
            and self.current_price is not None
            and self.seconds_left is not None
            and self.seconds_left > 0
        )

    def to_state(self) -> dict[str, Any]:
        return {
            "target_price": self.target_price,
            "current_price": self.current_price,
            "delta_usd": self.delta_usd,
            "delta_pct": self.delta_pct,
            "seconds_left": self.seconds_left,
            "target_source": self.target_source,
            "current_source": self.current_source,
            "twap_window_seconds": self.twap_window_seconds,
            "trade_ready": self.trade_ready,
        }


def _float_or_none(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number > 0 else None


def extract_price_to_beat(payload: Any) -> float | None:
    """Extract a canonical priceToBeat/openPrice value from Gamma JSON."""
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
        elif isinstance(value, list):
            for child in value:
                found = walk(child)
                if found is not None:
                    return found
        return None

    return walk(payload)


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


async def fetch_binance_hour_open(http: httpx.AsyncClient, start: datetime) -> float | None:
    start_ms = int(start.astimezone(UTC).timestamp() * 1000)
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


async def fetch_hour_target(
    http: httpx.AsyncClient,
    slug: str,
    window_start: datetime,
) -> tuple[float | None, str | None]:
    """Prefer Gamma's canonical anchor, then fall back to the Binance 1H open."""
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

    price = await fetch_binance_hour_open(http, window_start)
    if price is not None:
        return price, "Binance 1H open"
    return None, None


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
    store: Store | None = None,
    now: datetime | None = None,
    timeout_seconds: float = 3.0,
) -> dict[str, ShortTermSnapshot]:
    """Build authoritative snapshots. Missing Chainlink anchors stay missing."""
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

    hour_targets: dict[str, tuple[float | None, str | None]] = {}
    hour_candidates = [
        candidate for candidate in candidates
        if market_timeframe(candidate.market, settings) == "1h"
    ]
    if hour_candidates:
        async with httpx.AsyncClient(timeout=4.0, follow_redirects=True) as http:
            for candidate in hour_candidates:
                window = market_window(candidate.market, settings)
                if window is not None:
                    hour_targets[candidate.slug] = await fetch_hour_target(
                        http, candidate.slug, window[0]
                    )

    captured_at = requested_at or datetime.now(UTC)
    if captured_at.tzinfo is None:
        captured_at = captured_at.replace(tzinfo=UTC)
    else:
        captured_at = captured_at.astimezone(UTC)

    snapshots: dict[str, ShortTermSnapshot] = {}
    for candidate in candidates:
        timeframe = market_timeframe(candidate.market, settings)
        window = market_window(candidate.market, settings)

        target: float | None = None
        target_source: str | None = None
        twap_window: int | None = None

        if timeframe in {"5m", "15m"}:
            twap_window = chainlink_twap_window_seconds(candidate)
            anchor = store.get_price_anchor(candidate.slug, twap_window) if store else None
            if anchor is not None:
                target = float(anchor["price"])
                target_source = str(anchor["source"])
            current = chainlink_prices.get(twap_window)
            current_source = (
                f"Chainlink BTC/USD TWAP {twap_window}s"
                if current is not None else None
            )
        elif timeframe == "1h":
            target, target_source = hour_targets.get(candidate.slug, (None, None))
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
            twap_window_seconds=twap_window,
        )

    return snapshots


def _normalize_event_time(value: datetime | None) -> datetime:
    observed = value or datetime.now(UTC)
    if observed.tzinfo is None:
        return observed.replace(tzinfo=UTC)
    return observed.astimezone(UTC)


def record_chainlink_anchor_event(
    settings: Settings,
    store: Store,
    *,
    observed: datetime,
    price: float,
    twap_window: int,
) -> list[str]:
    """Record boundary events only; returns the newly inserted market slugs."""
    allowed = {
        value.strip().lower()
        for value in settings.allowed_timeframes.split(",")
        if value.strip().lower() in {"5m", "15m"}
    }
    grace = float(settings.anchor_capture_grace_seconds)
    observed = _normalize_event_time(observed)
    observed_ts = observed.timestamp()
    inserted_slugs: list[str] = []

    for timeframe in sorted(allowed):
        duration = _WINDOW_SECONDS[timeframe]
        start_epoch = int(observed_ts) // duration * duration
        elapsed = observed_ts - start_epoch
        if elapsed < 0 or elapsed > grace:
            continue

        slug = f"btc-updown-{timeframe}-{start_epoch}"
        inserted = store.put_price_anchor(
            slug=slug,
            timeframe=timeframe,
            window_start=float(start_epoch),
            twap_window=twap_window,
            price=price,
            observed_ts=observed_ts,
            source=f"Chainlink BTC/USD TWAP {twap_window}s 起始捕获",
        )
        if inserted:
            inserted_slugs.append(slug)
            log.info(
                "已捕获 %s 起始价：$%.2f（Chainlink TWAP %ss，延迟 %.2fs）",
                slug,
                price,
                twap_window,
                elapsed,
            )
    return inserted_slugs


async def watch_chainlink_anchors(
    settings: Settings,
    store: Store,
    *,
    on_capture: Callable[[str, int, float], None] | None = None,
) -> None:
    """Continuously capture trusted 5m/15m Chainlink anchors at window boundaries."""
    if not any(
        value.strip().lower() in {"5m", "15m"}
        for value in settings.allowed_timeframes.split(",")
    ):
        return

    specs = [
        CryptoPricesChainlinkTwapSpec(window_seconds=30, symbols=["btc/usd"]),
        CryptoPricesChainlinkTwapSpec(window_seconds=60, symbols=["btc/usd"]),
    ]

    while True:
        client = AsyncPublicClient()
        try:
            async with await client.subscribe(specs) as stream:
                async for event in stream:
                    observed = _normalize_event_time(event.timestamp)
                    price = _float_or_none(event.payload.value)
                    if price is None:
                        continue
                    twap_window = int(event.payload.window_seconds)
                    inserted_slugs = record_chainlink_anchor_event(
                        settings,
                        store,
                        observed=observed,
                        price=price,
                        twap_window=twap_window,
                    )
                    if on_capture is not None:
                        for slug in inserted_slugs:
                            on_capture(slug, twap_window, price)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            log.warning("Chainlink 起始价监听断开：%s；3 秒后重连", exc)
            await asyncio.sleep(3)
        finally:
            await client.close()
