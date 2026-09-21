"""Authoritative short-term BTC reference data and path features.

5m/15m:
- Resolution anchor/current: Polymarket RTDS Chainlink BTC/USD TWAP stream.
- Path history: Polymarket RTDS raw Chainlink BTC/USD stream.
- The window anchor is captured locally at the boundary and persisted.

1h:
- Resolution anchor/current/history: direct Binance BTC/USDT REST API.

Prediction-market odds are deliberately excluded from these features.
"""

from __future__ import annotations

import asyncio
import logging
import math
import statistics
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import httpx
from polymarket import AsyncPublicClient
from polymarket.streams import CryptoPricesChainlinkTwapSpec, CryptoPricesSpec

from .config import Settings
from .markets import market_timeframe, market_window
from .store import Store

if TYPE_CHECKING:
    from .markets import Candidate

log = logging.getLogger(__name__)

BINANCE_KLINES_URLS = (
    "https://api.binance.com/api/v3/klines",
    "https://api.binance.us/api/v3/klines",
)
BINANCE_TICKER_URLS = (
    "https://api.binance.com/api/v3/ticker/price",
    "https://api.binance.us/api/v3/ticker/price",
)

_WINDOW_SECONDS = {"5m": 300, "15m": 900}


@dataclass(frozen=True)
class PricePathFeatures:
    history_source: str
    sample_count: int
    history_span_seconds: int
    latest_sample_age_seconds: float | None
    return_30s_pct: float | None
    return_60s_pct: float | None
    return_180s_pct: float | None
    return_300s_pct: float | None
    realized_vol_60s_pct: float | None
    realized_vol_180s_pct: float | None
    realized_vol_300s_pct: float | None
    range_60s_pct: float | None
    range_180s_pct: float | None
    range_300s_pct: float | None
    up_tick_ratio_60s: float | None
    trend_60s_pct_per_min: float | None
    trend_180s_pct_per_min: float | None
    remaining_vol_pct: float | None
    distance_z: float | None
    feature_ready: bool

    def to_state(self) -> dict[str, Any]:
        return {
            "history_source": self.history_source,
            "sample_count": self.sample_count,
            "history_span_seconds": self.history_span_seconds,
            "latest_sample_age_seconds": self.latest_sample_age_seconds,
            "return_30s_pct": self.return_30s_pct,
            "return_60s_pct": self.return_60s_pct,
            "return_180s_pct": self.return_180s_pct,
            "return_300s_pct": self.return_300s_pct,
            "realized_vol_60s_pct": self.realized_vol_60s_pct,
            "realized_vol_180s_pct": self.realized_vol_180s_pct,
            "realized_vol_300s_pct": self.realized_vol_300s_pct,
            "range_60s_pct": self.range_60s_pct,
            "range_180s_pct": self.range_180s_pct,
            "range_300s_pct": self.range_300s_pct,
            "up_tick_ratio_60s": self.up_tick_ratio_60s,
            "trend_60s_pct_per_min": self.trend_60s_pct_per_min,
            "trend_180s_pct_per_min": self.trend_180s_pct_per_min,
            "remaining_vol_pct": self.remaining_vol_pct,
            "distance_z": self.distance_z,
            "feature_ready": self.feature_ready,
        }


@dataclass(frozen=True)
class ShortTermSnapshot:
    target_price: float | None
    current_price: float | None
    seconds_left: int | None
    target_source: str | None
    current_source: str | None
    captured_at: datetime
    twap_window_seconds: int | None = None
    path_features: PricePathFeatures | None = None

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
    def distance_bps(self) -> float | None:
        return None if self.delta_pct is None else self.delta_pct * 100.0

    @property
    def trade_ready(self) -> bool:
        return (
            self.target_price is not None
            and self.current_price is not None
            and self.seconds_left is not None
            and self.seconds_left > 0
            and self.path_features is not None
            and self.path_features.feature_ready
        )

    def to_state(self) -> dict[str, Any]:
        return {
            "target_price": self.target_price,
            "current_price": self.current_price,
            "delta_usd": self.delta_usd,
            "delta_pct": self.delta_pct,
            "distance_bps": self.distance_bps,
            "seconds_left": self.seconds_left,
            "target_source": self.target_source,
            "current_source": self.current_source,
            "twap_window_seconds": self.twap_window_seconds,
            "path_features": self.path_features.to_state() if self.path_features else None,
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


async def _binance_get_json(
    http: httpx.AsyncClient,
    urls: tuple[str, ...],
    *,
    params: dict[str, Any],
) -> Any:
    last_error: Exception | None = None
    for url in urls:
        try:
            response = await http.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            log.debug("Binance request failed from %s: %s", url, exc)
    if last_error is not None:
        log.debug("All Binance endpoints failed: %s", last_error)
    return None


async def fetch_binance_hour_open(http: httpx.AsyncClient, start: datetime) -> float | None:
    data = await _binance_get_json(
        http,
        BINANCE_KLINES_URLS,
        params={
            "symbol": "BTCUSDT",
            "interval": "1h",
            "startTime": int(start.astimezone(UTC).timestamp() * 1000),
            "limit": 1,
        },
    )
    if isinstance(data, list) and data and isinstance(data[0], list) and len(data[0]) > 1:
        return _float_or_none(data[0][1])
    return None


async def fetch_binance_current(http: httpx.AsyncClient) -> float | None:
    data = await _binance_get_json(
        http,
        BINANCE_TICKER_URLS,
        params={"symbol": "BTCUSDT"},
    )
    return _float_or_none(data.get("price")) if isinstance(data, dict) else None


async def fetch_binance_recent_history(
    http: httpx.AsyncClient,
    *,
    limit: int = 8,
) -> list[dict[str, float]]:
    """Return recent 1m BTCUSDT closes from direct Binance REST."""
    data = await _binance_get_json(
        http,
        BINANCE_KLINES_URLS,
        params={"symbol": "BTCUSDT", "interval": "1m", "limit": limit},
    )
    out: list[dict[str, float]] = []
    if not isinstance(data, list):
        return out
    now_ms = datetime.now(UTC).timestamp() * 1000.0
    for row in data:
        if not isinstance(row, list) or len(row) < 5:
            continue
        price = _float_or_none(row[4])
        if price is None:
            continue
        # Real Binance rows include close time at index 6. Skip the unfinished
        # current candle because we append the direct ticker price separately.
        if len(row) > 6:
            close_ms = float(row[6])
            if close_ms > now_ms:
                continue
            ts = close_ms / 1000.0
        else:
            ts = float(row[0]) / 1000.0
        out.append({"ts": ts, "price": price})
    return out


async def _first_stream_price(
    client: AsyncPublicClient,
    spec: CryptoPricesChainlinkTwapSpec,
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
        log.debug("Chainlink TWAP stream failed: %s", exc)
        return None
    return None


async def fetch_chainlink_twap_prices(
    client: AsyncPublicClient,
    *,
    windows: set[int],
    timeout_seconds: float = 3.0,
) -> dict[int, float | None]:
    tasks: list[asyncio.Task[float | None]] = []
    labels: list[int] = []
    for window_seconds in sorted(windows):
        tasks.append(asyncio.create_task(_first_stream_price(
            client,
            CryptoPricesChainlinkTwapSpec(
                window_seconds=window_seconds,
                symbols=["btc/usd"],
            ),
            timeout_seconds,
        )))
        labels.append(window_seconds)
    if not tasks:
        return {}
    values = await asyncio.gather(*tasks)
    return dict(zip(labels, values, strict=True))


def _normalize_samples(samples: list[dict[str, Any]]) -> list[tuple[float, float]]:
    by_ts: dict[int, float] = {}
    for sample in samples:
        ts = sample.get("ts")
        price = _float_or_none(sample.get("price"))
        if ts is None or price is None:
            continue
        by_ts[int(float(ts))] = price
    return [(float(ts), by_ts[ts]) for ts in sorted(by_ts)]


def _nearest_price(
    points: list[tuple[float, float]],
    target_ts: float,
    *,
    tolerance_seconds: float,
) -> float | None:
    if not points:
        return None
    ts, price = min(points, key=lambda item: abs(item[0] - target_ts))
    return price if abs(ts - target_ts) <= tolerance_seconds else None


def _window_points(
    points: list[tuple[float, float]],
    end_ts: float,
    window_seconds: int,
) -> list[tuple[float, float]]:
    cutoff = end_ts - window_seconds
    return [(ts, price) for ts, price in points if cutoff <= ts <= end_ts]


def _median_interval(points: list[tuple[float, float]]) -> float | None:
    if len(points) < 2:
        return None
    gaps = [
        points[i][0] - points[i - 1][0]
        for i in range(1, len(points))
        if points[i][0] > points[i - 1][0]
    ]
    return statistics.median(gaps) if gaps else None


def _return_pct(
    points: list[tuple[float, float]],
    end_ts: float,
    window_seconds: int,
) -> float | None:
    if not points:
        return None
    cadence = _median_interval(points)
    if cadence is not None and cadence > window_seconds * 0.8:
        # Do not fabricate a sub-cadence return from coarse bars.
        return None
    tolerance = 5.0
    if cadence is not None:
        tolerance = max(5.0, min(35.0, cadence * 0.6))
    start_price = _nearest_price(
        points,
        end_ts - window_seconds,
        tolerance_seconds=tolerance,
    )
    end_price = points[-1][1]
    if start_price is None or start_price <= 0:
        return None
    return (end_price / start_price - 1.0) * 100.0


def _realized_vol_pct(window: list[tuple[float, float]]) -> float | None:
    if len(window) < 4:
        return None
    returns = [
        math.log(window[i][1] / window[i - 1][1])
        for i in range(1, len(window))
        if window[i - 1][1] > 0 and window[i][1] > 0
    ]
    if len(returns) < 3:
        return None
    return math.sqrt(sum(r * r for r in returns)) * 100.0


def _range_pct(window: list[tuple[float, float]]) -> float | None:
    if len(window) < 2:
        return None
    prices = [price for _, price in window]
    low = min(prices)
    high = max(prices)
    return None if low <= 0 else (high / low - 1.0) * 100.0


def _up_tick_ratio(window: list[tuple[float, float]]) -> float | None:
    if len(window) < 3:
        return None
    deltas = [
        window[i][1] - window[i - 1][1]
        for i in range(1, len(window))
        if window[i][1] != window[i - 1][1]
    ]
    if not deltas:
        return None
    return sum(delta > 0 for delta in deltas) / len(deltas)


def _trend_pct_per_min(window: list[tuple[float, float]]) -> float | None:
    if len(window) < 4:
        return None
    t0 = window[0][0]
    xs = [(ts - t0) / 60.0 for ts, _ in window]
    ys = [math.log(price) for _, price in window if price > 0]
    if len(ys) != len(xs):
        return None
    xbar = statistics.fmean(xs)
    ybar = statistics.fmean(ys)
    denom = sum((x - xbar) ** 2 for x in xs)
    if denom <= 0:
        return None
    slope = sum((x - xbar) * (y - ybar) for x, y in zip(xs, ys, strict=True)) / denom
    return slope * 100.0


def _remaining_vol_and_z(
    points: list[tuple[float, float]],
    *,
    target_price: float | None,
    current_price: float | None,
    seconds_left: int | None,
) -> tuple[float | None, float | None]:
    recent = points[-301:]
    if len(recent) < 5 or target_price is None or current_price is None or not seconds_left:
        return None, None
    sum_sq = 0.0
    elapsed = 0.0
    for i in range(1, len(recent)):
        dt = recent[i][0] - recent[i - 1][0]
        if dt <= 0:
            continue
        r = math.log(recent[i][1] / recent[i - 1][1])
        sum_sq += r * r
        elapsed += dt
    if elapsed <= 0 or sum_sq <= 0:
        return None, None
    variance_rate = sum_sq / elapsed
    remaining_sigma = math.sqrt(variance_rate * seconds_left)
    if remaining_sigma <= 0:
        return None, None
    distance = math.log(current_price / target_price)
    return remaining_sigma * 100.0, distance / remaining_sigma


def compute_path_features(
    samples: list[dict[str, Any]],
    *,
    history_source: str,
    captured_at: datetime,
    target_price: float | None,
    current_price: float | None,
    seconds_left: int | None,
    min_history_seconds: int = 60,
    max_sample_age_seconds: float = 5.0,
) -> PricePathFeatures:
    points = _normalize_samples(samples)
    end_ts = captured_at.timestamp()
    latest_age = max(0.0, end_ts - points[-1][0]) if points else None
    history_span = int(points[-1][0] - points[0][0]) if len(points) >= 2 else 0

    w60 = _window_points(points, end_ts, 60)
    w180 = _window_points(points, end_ts, 180)
    w300 = _window_points(points, end_ts, 300)
    remaining_vol, distance_z = _remaining_vol_and_z(
        points,
        target_price=target_price,
        current_price=current_price,
        seconds_left=seconds_left,
    )

    rv60 = _realized_vol_pct(w60)
    rv180 = _realized_vol_pct(w180)
    rv300 = _realized_vol_pct(w300)
    cadence = _median_interval(points)
    if cadence is not None and cadence <= 5:
        recent_span = int(w60[-1][0] - w60[0][0]) if len(w60) >= 2 else 0
        recent_gaps = [
            w60[i][0] - w60[i - 1][0]
            for i in range(1, len(w60))
        ]
        max_recent_gap = max(recent_gaps, default=999.0)
        coverage_ready = (
            recent_span >= min(55, min_history_seconds)
            and max_recent_gap <= 5.0
        )
        volatility_ready = rv60 is not None
    else:
        coverage_ready = history_span >= max(180, min_history_seconds)
        volatility_ready = rv300 is not None or rv180 is not None

    feature_ready = (
        len(points) >= 4
        and history_span >= min_history_seconds
        and latest_age is not None
        and latest_age <= max_sample_age_seconds
        and coverage_ready
        and volatility_ready
    )

    return PricePathFeatures(
        history_source=history_source,
        sample_count=len(points),
        history_span_seconds=history_span,
        latest_sample_age_seconds=latest_age,
        return_30s_pct=_return_pct(points, end_ts, 30),
        return_60s_pct=_return_pct(points, end_ts, 60),
        return_180s_pct=_return_pct(points, end_ts, 180),
        return_300s_pct=_return_pct(points, end_ts, 300),
        realized_vol_60s_pct=rv60,
        realized_vol_180s_pct=rv180,
        realized_vol_300s_pct=rv300,
        range_60s_pct=_range_pct(w60),
        range_180s_pct=_range_pct(w180),
        range_300s_pct=_range_pct(w300),
        up_tick_ratio_60s=_up_tick_ratio(w60),
        trend_60s_pct_per_min=_trend_pct_per_min(w60),
        trend_180s_pct_per_min=_trend_pct_per_min(w180),
        remaining_vol_pct=remaining_vol,
        distance_z=distance_z,
        feature_ready=feature_ready,
    )


async def fetch_short_term_snapshots(
    client: AsyncPublicClient,
    candidates: list[Candidate],
    settings: Settings,
    *,
    store: Store | None = None,
    now: datetime | None = None,
    timeout_seconds: float = 3.0,
) -> dict[str, ShortTermSnapshot]:
    """Build authoritative snapshots plus recent price-path features."""
    captured_at = now or datetime.now(UTC)
    if captured_at.tzinfo is None:
        captured_at = captured_at.replace(tzinfo=UTC)
    else:
        captured_at = captured_at.astimezone(UTC)

    chainlink_windows = {
        chainlink_twap_window_seconds(candidate)
        for candidate in candidates
        if market_timeframe(candidate.market, settings) in {"5m", "15m"}
    }
    chainlink_prices = await fetch_chainlink_twap_prices(
        client,
        windows=chainlink_windows,
        timeout_seconds=timeout_seconds,
    )

    need_hour = any(
        market_timeframe(candidate.market, settings) == "1h"
        for candidate in candidates
    )
    binance_current: float | None = None
    binance_history: list[dict[str, float]] = []
    hour_targets: dict[str, float | None] = {}
    if need_hour:
        async with httpx.AsyncClient(timeout=4.0, follow_redirects=True) as http:
            binance_current, binance_history = await asyncio.gather(
                fetch_binance_current(http),
                fetch_binance_recent_history(http),
            )
            if binance_current is not None:
                binance_history.append({
                    "ts": captured_at.timestamp(),
                    "price": binance_current,
                })
            for candidate in candidates:
                if market_timeframe(candidate.market, settings) != "1h":
                    continue
                window = market_window(candidate.market, settings)
                if window is not None:
                    hour_targets[candidate.slug] = await fetch_binance_hour_open(http, window[0])

    snapshots: dict[str, ShortTermSnapshot] = {}
    for candidate in candidates:
        timeframe = market_timeframe(candidate.market, settings)
        window = market_window(candidate.market, settings)
        seconds_left = (
            max(0, int((window[1] - captured_at).total_seconds()))
            if window is not None else None
        )

        target: float | None = None
        target_source: str | None = None
        current: float | None = None
        current_source: str | None = None
        twap_window: int | None = None
        features: PricePathFeatures | None = None

        if timeframe in {"5m", "15m"}:
            twap_window = chainlink_twap_window_seconds(candidate)
            anchor = store.get_price_anchor(candidate.slug, twap_window) if store else None
            if anchor is not None:
                target = float(anchor["price"])
                target_source = str(anchor["source"])
            current = chainlink_prices.get(twap_window)
            current_source = (
                f"Polymarket RTDS · Chainlink BTC/USD TWAP {twap_window}s"
                if current is not None else None
            )
            samples = (
                store.get_price_samples(
                    source="chainlink_spot",
                    since_ts=captured_at.timestamp() - 330,
                )
                if store else []
            )
            features = compute_path_features(
                samples,
                history_source="Polymarket RTDS · Chainlink BTC/USD raw",
                captured_at=captured_at,
                target_price=target,
                current_price=current,
                seconds_left=seconds_left,
                min_history_seconds=settings.short_term_min_history_seconds,
                max_sample_age_seconds=settings.short_term_max_sample_age_seconds,
            )

        elif timeframe == "1h":
            target = hour_targets.get(candidate.slug)
            target_source = "Binance API · BTCUSDT 1H open" if target is not None else None
            current = binance_current
            current_source = "Binance API · BTCUSDT" if current is not None else None
            features = compute_path_features(
                binance_history,
                history_source="Binance API · BTCUSDT 1m K线",
                captured_at=captured_at,
                target_price=target,
                current_price=current,
                seconds_left=seconds_left,
                min_history_seconds=settings.short_term_min_history_seconds,
                max_sample_age_seconds=65.0,
            )

        snapshots[candidate.slug] = ShortTermSnapshot(
            target_price=target,
            current_price=current,
            seconds_left=seconds_left,
            target_source=target_source,
            current_source=current_source,
            captured_at=captured_at,
            twap_window_seconds=twap_window,
            path_features=features,
        )

    return snapshots


def _normalize_event_time(value: datetime | None) -> datetime:
    observed = value or datetime.now(UTC)
    if observed.tzinfo is None:
        return observed.replace(tzinfo=UTC)
    return observed.astimezone(UTC)


def _source_event_time(event: Any) -> datetime:
    """Prefer the price payload's source timestamp over WebSocket receive time."""
    payload_ts = getattr(getattr(event, "payload", None), "timestamp", None)
    if isinstance(payload_ts, (int, float)) and not isinstance(payload_ts, bool):
        seconds = float(payload_ts)
        if seconds > 10_000_000_000:
            seconds /= 1000.0
        try:
            return datetime.fromtimestamp(seconds, tz=UTC)
        except (OSError, OverflowError, ValueError):
            pass
    return _normalize_event_time(getattr(event, "timestamp", None))


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
            source=f"Polymarket RTDS · Chainlink BTC/USD TWAP {twap_window}s 起始捕获",
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
    """Capture Chainlink anchors and raw BTC/USD history continuously."""
    if not any(
        value.strip().lower() in {"5m", "15m"}
        for value in settings.allowed_timeframes.split(",")
    ):
        return

    specs = [
        CryptoPricesSpec(topic="prices.crypto.chainlink", symbols=["btc/usd"]),
        CryptoPricesChainlinkTwapSpec(window_seconds=30, symbols=["btc/usd"]),
        CryptoPricesChainlinkTwapSpec(window_seconds=60, symbols=["btc/usd"]),
    ]
    last_prune = 0.0

    while True:
        client = AsyncPublicClient()
        try:
            async with await client.subscribe(specs) as stream:
                async for event in stream:
                    observed = _source_event_time(event)
                    price = _float_or_none(event.payload.value)
                    if price is None:
                        continue
                    observed_ts = observed.timestamp()

                    if event.topic == "prices.crypto.chainlink":
                        store.put_price_sample(
                            source="chainlink_spot",
                            price=price,
                            observed_ts=observed_ts,
                        )
                    elif event.topic == "prices.crypto.chainlink.twap":
                        twap_window = int(event.payload.window_seconds)
                        store.put_price_sample(
                            source="chainlink_twap",
                            twap_window=twap_window,
                            price=price,
                            observed_ts=observed_ts,
                        )
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

                    if observed_ts - last_prune >= 300:
                        store.prune_price_samples(observed_ts - 21_600)
                        last_prune = observed_ts

        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            log.warning("Chainlink 实时价格监听断开：%s；3 秒后重连", exc)
            await asyncio.sleep(3)
        finally:
            await client.close()
