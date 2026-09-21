from datetime import UTC, datetime
from types import SimpleNamespace

import httpx
import respx

import jevymarket.market_data as market_data
from jevymarket.config import Settings
from jevymarket.market_data import (
    BINANCE_KLINES_URLS,
    BINANCE_TICKER_URLS,
    chainlink_twap_window_seconds,
    compute_path_features,
    fetch_binance_current,
    fetch_binance_hour_open,
    fetch_binance_recent_history,
    record_chainlink_anchor_event,
)
from jevymarket.store import Store


def test_chainlink_twap_window_comes_from_market_rules():
    market_30 = SimpleNamespace(
        description="Resolves using BTC/USD TWAP 30-second stream.",
        resolution=SimpleNamespace(source="https://data.chain.link/streams/btc-usd-twap-30s-streams"),
    )
    market_60 = SimpleNamespace(
        description="Resolves using Bitcoin TWAP.",
        resolution=SimpleNamespace(source="https://data.chain.link/streams/btc-usd-twap-60s-streams"),
    )
    assert chainlink_twap_window_seconds(SimpleNamespace(market=market_30)) == 30
    assert chainlink_twap_window_seconds(SimpleNamespace(market=market_60)) == 60


@respx.mock
async def test_binance_hour_open_is_direct_source():
    respx.get(BINANCE_KLINES_URLS[0]).mock(return_value=httpx.Response(
        200,
        json=[[1789966800000, "81200.50", "0", "0", "0"]],
    ))
    async with httpx.AsyncClient() as client:
        price = await fetch_binance_hour_open(
            client,
            datetime(2026, 9, 21, 5, 0, tzinfo=UTC),
        )
    assert price == 81200.50


@respx.mock
async def test_binance_current_and_history():
    respx.get(BINANCE_TICKER_URLS[0]).mock(
        return_value=httpx.Response(200, json={"symbol": "BTCUSDT", "price": "81321.25"})
    )
    respx.get(BINANCE_KLINES_URLS[0]).mock(return_value=httpx.Response(
        200,
        json=[
            [1000, "0", "0", "0", "81000.0"],
            [61000, "0", "0", "0", "81050.0"],
        ],
    ))
    async with httpx.AsyncClient() as client:
        current = await fetch_binance_current(client)
        history = await fetch_binance_recent_history(client, limit=2)
    assert current == 81321.25
    assert history == [
        {"ts": 1.0, "price": 81000.0},
        {"ts": 61.0, "price": 81050.0},
    ]


def test_compute_path_features_detects_momentum_and_volatility():
    captured = datetime(2026, 9, 21, 5, 5, 0, tzinfo=UTC)
    end_ts = int(captured.timestamp())
    samples = []
    for offset in range(-300, 1):
        # Upward drift plus alternating micro-noise gives non-zero realized vol.
        price = 81000.0 + (offset + 300) * 0.08 + (0.15 if offset % 2 else -0.15)
        samples.append({"ts": end_ts + offset, "price": price})

    features = compute_path_features(
        samples,
        history_source="test",
        captured_at=captured,
        target_price=81010.0,
        current_price=81024.0,
        seconds_left=120,
        min_history_seconds=60,
        max_sample_age_seconds=5,
    )

    assert features.feature_ready
    assert features.sample_count == 301
    assert features.history_span_seconds == 300
    assert features.return_60s_pct is not None and features.return_60s_pct > 0
    assert features.return_180s_pct is not None and features.return_180s_pct > 0
    assert features.realized_vol_60s_pct is not None and features.realized_vol_60s_pct > 0
    assert features.trend_60s_pct_per_min is not None and features.trend_60s_pct_per_min > 0
    assert features.remaining_vol_pct is not None and features.remaining_vol_pct > 0
    assert features.distance_z is not None and features.distance_z > 0


def test_stale_path_is_not_trade_ready():
    captured = datetime(2026, 9, 21, 5, 5, 0, tzinfo=UTC)
    end_ts = int(captured.timestamp())
    samples = [
        {"ts": end_ts - 80 + i, "price": 81000 + i * 0.1}
        for i in range(70)
    ]
    features = compute_path_features(
        samples,
        history_source="test",
        captured_at=captured,
        target_price=81000,
        current_price=81010,
        seconds_left=100,
        min_history_seconds=60,
        max_sample_age_seconds=5,
    )
    assert features.history_span_seconds >= 60
    assert features.latest_sample_age_seconds is not None
    assert features.latest_sample_age_seconds > 5
    assert not features.feature_ready


def test_records_chainlink_anchor_only_near_boundary(tmp_path):
    store = Store(tmp_path / "t.db")
    settings = Settings(
        allowed_timeframes="5m,15m,1h",
        anchor_capture_grace_seconds=3,
        _env_file=None,
    )

    inserted = record_chainlink_anchor_event(
        settings,
        store,
        observed=datetime(2026, 9, 21, 5, 0, 1, tzinfo=UTC),
        price=81603.80,
        twap_window=60,
    )
    assert "btc-updown-5m-1789966800" in inserted
    assert "btc-updown-15m-1789966800" in inserted

    five = store.get_price_anchor("btc-updown-5m-1789966800", 60)
    fifteen = store.get_price_anchor("btc-updown-15m-1789966800", 60)
    assert five and five["price"] == 81603.80
    assert fifteen and fifteen["price"] == 81603.80

    late = record_chainlink_anchor_event(
        settings,
        store,
        observed=datetime(2026, 9, 21, 5, 0, 4, tzinfo=UTC),
        price=81650.00,
        twap_window=30,
    )
    assert late == []
    assert store.get_price_anchor("btc-updown-5m-1789966800", 30) is None
    store.close()


def test_30s_and_60s_anchors_are_stored_separately(tmp_path):
    store = Store(tmp_path / "t.db")
    settings = Settings(
        allowed_timeframes="5m",
        anchor_capture_grace_seconds=3,
        _env_file=None,
    )
    observed = datetime(2026, 9, 21, 5, 5, 1, tzinfo=UTC)

    record_chainlink_anchor_event(
        settings, store, observed=observed, price=81000.0, twap_window=30
    )
    record_chainlink_anchor_event(
        settings, store, observed=observed, price=81010.0, twap_window=60
    )

    assert store.get_price_anchor("btc-updown-5m-1789967100", 30)["price"] == 81000.0
    assert store.get_price_anchor("btc-updown-5m-1789967100", 60)["price"] == 81010.0
    store.close()


def test_price_samples_store_one_value_per_second(tmp_path):
    store = Store(tmp_path / "t.db")
    store.put_price_sample(
        source="chainlink_spot",
        price=81000,
        observed_ts=100.1,
    )
    store.put_price_sample(
        source="chainlink_spot",
        price=81001,
        observed_ts=100.9,
    )
    store.put_price_sample(
        source="chainlink_spot",
        price=81002,
        observed_ts=101.2,
    )
    rows = store.get_price_samples(source="chainlink_spot", since_ts=99)
    assert rows == [
        {"ts": 100, "price": 81001.0},
        {"ts": 101, "price": 81002.0},
    ]
    store.close()



def test_source_event_time_prefers_payload_timestamp():
    outer = datetime(2026, 9, 21, 5, 0, 6, tzinfo=UTC)
    payload_ms = int(datetime(2026, 9, 21, 5, 0, 1, tzinfo=UTC).timestamp() * 1000)
    event = SimpleNamespace(
        timestamp=outer,
        payload=SimpleNamespace(timestamp=payload_ms),
    )
    observed = market_data._source_event_time(event)
    assert observed == datetime(2026, 9, 21, 5, 0, 1, tzinfo=UTC)
