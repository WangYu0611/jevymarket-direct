from datetime import UTC, datetime
from types import SimpleNamespace

import httpx
import respx

from jevymarket.config import Settings
from jevymarket.market_data import (
    BINANCE_KLINES_URLS,
    GAMMA_EVENTS_URL,
    chainlink_twap_window_seconds,
    extract_price_to_beat,
    fetch_hour_target,
    record_chainlink_anchor_event,
)
from jevymarket.store import Store


def test_extract_price_to_beat_from_event_metadata():
    payload = {
        "slug": "bitcoin-up-or-down-x",
        "eventMetadata": {"priceToBeat": "81307.08"},
    }
    assert extract_price_to_beat(payload) == 81307.08


def test_extract_price_to_beat_nested():
    payload = {
        "markets": [{
            "metadata": {
                "cryptoMarketConfig": {"openPrice": "81234.56"},
            },
        }]
    }
    assert extract_price_to_beat(payload) == 81234.56


@respx.mock
async def test_hour_target_prefers_gamma():
    respx.get(GAMMA_EVENTS_URL).mock(return_value=httpx.Response(
        200,
        json=[{"eventMetadata": {"priceToBeat": 81400.0}}],
    ))
    async with httpx.AsyncClient() as client:
        price, source = await fetch_hour_target(
            client,
            "bitcoin-up-or-down-september-21-2026-1am-et",
            datetime(2026, 9, 21, 5, 0, tzinfo=UTC),
        )
    assert price == 81400.0
    assert source == "Polymarket priceToBeat"


@respx.mock
async def test_hour_target_falls_back_to_binance_open():
    respx.get(GAMMA_EVENTS_URL).mock(return_value=httpx.Response(200, json=[{}]))
    respx.get(BINANCE_KLINES_URLS[0]).mock(return_value=httpx.Response(
        200,
        json=[[1789966800000, "81200.50", "0", "0", "0"]],
    ))
    async with httpx.AsyncClient() as client:
        price, source = await fetch_hour_target(
            client,
            "bitcoin-up-or-down-september-21-2026-1am-et",
            datetime(2026, 9, 21, 5, 0, tzinfo=UTC),
        )
    assert price == 81200.50
    assert source == "Binance 1H open"


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
