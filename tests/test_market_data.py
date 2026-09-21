from datetime import UTC, datetime

import httpx
import respx

from jevymarket.market_data import (
    BINANCE_KLINES_URLS,
    GAMMA_EVENTS_URL,
    extract_price_to_beat,
    fetch_price_to_beat,
)


def test_extract_price_to_beat_from_event_metadata():
    payload = {
        "slug": "btc-updown-5m-123",
        "eventMetadata": {"priceToBeat": "81307.08"},
    }
    assert extract_price_to_beat(payload) == 81307.08


def test_extract_price_to_beat_from_nested_json_string():
    payload = {
        "markets": [{
            "metadata": '{"cryptoMarketConfig":{"priceToBeat":"81234.56"}}',
        }]
    }
    assert extract_price_to_beat(payload) == 81234.56


@respx.mock
async def test_gamma_target_is_preferred_for_chainlink_market():
    respx.get(GAMMA_EVENTS_URL).mock(return_value=httpx.Response(
        200,
        json=[{"eventMetadata": {"priceToBeat": 81000.25}}],
    ))
    async with httpx.AsyncClient() as client:
        price, source = await fetch_price_to_beat(
            client,
            "btc-updown-5m-123",
            timeframe="5m",
            window_start=datetime(2026, 9, 21, 4, 0, tzinfo=UTC),
        )
    assert price == 81000.25
    assert source == "Polymarket priceToBeat"


@respx.mock
async def test_hourly_falls_back_to_binance_open():
    respx.get(GAMMA_EVENTS_URL).mock(return_value=httpx.Response(200, json=[{}]))
    respx.get(BINANCE_KLINES_URLS[0]).mock(return_value=httpx.Response(
        200,
        json=[[1789966800000, "81200.50", "0", "0", "0"]],
    ))
    async with httpx.AsyncClient() as client:
        price, source = await fetch_price_to_beat(
            client,
            "bitcoin-up-or-down-september-21-2026-1am-et",
            timeframe="1h",
            window_start=datetime(2026, 9, 21, 5, 0, tzinfo=UTC),
        )
    assert price == 81200.50
    assert source == "Binance 1H open"


@respx.mock
async def test_chainlink_market_does_not_use_binance_proxy():
    respx.get(GAMMA_EVENTS_URL).mock(return_value=httpx.Response(200, json=[{}]))
    async with httpx.AsyncClient() as client:
        price, source = await fetch_price_to_beat(
            client,
            "btc-updown-15m-123",
            timeframe="15m",
            window_start=datetime(2026, 9, 21, 4, 0, tzinfo=UTC),
        )
    assert price is None
    assert source is None
