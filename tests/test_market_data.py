import json
from datetime import UTC, datetime
from types import SimpleNamespace

import httpx
import respx

from jevymarket.market_data import (
    BINANCE_KLINES_URLS,
    GAMMA_EVENTS_URL,
    POLYMARKET_EVENT_URL,
    chainlink_twap_window_seconds,
    extract_live_open_price_from_html,
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


def test_live_open_price_uses_exact_crypto_prices_query():
    start = datetime(2026, 9, 21, 4, 15, tzinfo=UTC)
    next_data = {
        "props": {
            "pageProps": {
                "dehydratedState": {
                    "queries": [
                        {
                            "queryKey": [
                                "crypto-prices",
                                {"eventStartTime": "2026-09-21T04:10:00Z"},
                            ],
                            "state": {"data": {"openPrice": 80000.0}},
                        },
                        {
                            "queryKey": [
                                "crypto-prices",
                                {
                                    "eventStartTime": "2026-09-21T04:15:00Z",
                                    "slug": "btc-updown-5m-1789964100",
                                },
                            ],
                            "state": {"data": {"openPrice": 81307.08}},
                        },
                    ]
                }
            }
        }
    }
    html = (
        '<html><script crossorigin="" id="__NEXT_DATA__" type="application/json">'
        + json.dumps(next_data)
        + "</script></html>"
    )
    assert extract_live_open_price_from_html(
        html,
        "btc-updown-5m-1789964100",
        start,
    ) == 81307.08


@respx.mock
async def test_live_page_open_price_is_preferred_for_chainlink_market():
    slug = "btc-updown-5m-1789964100"
    next_data = {
        "props": {
            "pageProps": {
                "queries": [{
                    "queryKey": [
                        "crypto-prices",
                        {
                            "eventStartTime": "2026-09-21T04:15:00Z",
                            "slug": slug,
                        },
                    ],
                    "state": {"data": {"openPrice": 81307.08}},
                }]
            }
        }
    }
    page = (
        '<script id="__NEXT_DATA__" crossorigin="">'
        + json.dumps(next_data)
        + "</script>"
    )
    respx.get(POLYMARKET_EVENT_URL.format(slug=slug)).mock(
        return_value=httpx.Response(200, text=page)
    )
    async with httpx.AsyncClient() as client:
        price, source = await fetch_price_to_beat(
            client,
            slug,
            timeframe="5m",
            window_start=datetime(2026, 9, 21, 4, 15, tzinfo=UTC),
        )
    assert price == 81307.08
    assert source == "Polymarket 页面 openPrice"


@respx.mock
async def test_gamma_target_is_fallback_for_chainlink_market():
    slug = "btc-updown-5m-123"
    respx.get(POLYMARKET_EVENT_URL.format(slug=slug)).mock(
        return_value=httpx.Response(200, text="<html></html>")
    )
    respx.get(GAMMA_EVENTS_URL).mock(return_value=httpx.Response(
        200,
        json=[{"eventMetadata": {"priceToBeat": 81000.25}}],
    ))
    async with httpx.AsyncClient() as client:
        price, source = await fetch_price_to_beat(
            client,
            slug,
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
    slug = "btc-updown-15m-123"
    respx.get(POLYMARKET_EVENT_URL.format(slug=slug)).mock(
        return_value=httpx.Response(200, text="<html></html>")
    )
    respx.get(GAMMA_EVENTS_URL).mock(return_value=httpx.Response(200, json=[{}]))
    async with httpx.AsyncClient() as client:
        price, source = await fetch_price_to_beat(
            client,
            slug,
            timeframe="15m",
            window_start=datetime(2026, 9, 21, 4, 0, tzinfo=UTC),
        )
    assert price is None
    assert source is None


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
