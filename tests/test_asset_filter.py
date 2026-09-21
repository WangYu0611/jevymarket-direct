from types import SimpleNamespace

from jevymarket.config import Settings
from jevymarket.markets import market_asset_symbol, market_timeframe


def _settings(assets: str = "BTC", timeframes: str = "5m,15m,1h") -> Settings:
    return Settings(allowed_assets=assets, allowed_timeframes=timeframes, _env_file=None)


def _market(question: str, slug: str = "", description: str = "", category: str = "", tags=()):
    return SimpleNamespace(
        question=question,
        slug=slug,
        description=description,
        category=category,
        tags=tuple(SimpleNamespace(slug=tag_slug, label=label) for tag_slug, label in tags),
    )


def test_btc_asset_matches():
    s = _settings()
    assert market_asset_symbol(_market("Bitcoin Up or Down?", "btc-updown-5m-123"), s) == "BTC"
    assert market_asset_symbol(_market("BTC Up or Down?", "btc-updown-15m-123"), s) == "BTC"


def test_eth_and_sol_are_not_in_strategy():
    s = _settings()
    assert market_asset_symbol(_market("Ethereum Up or Down?", "eth-updown-5m-123"), s) is None
    assert market_asset_symbol(_market("Solana Up or Down?", "sol-updown-15m-123"), s) is None


def test_recognizes_5m_and_15m_slugs():
    s = _settings()
    assert market_timeframe(_market("Bitcoin", "btc-updown-5m-123"), s) == "5m"
    assert market_timeframe(_market("Bitcoin", "btc-updown-15m-123"), s) == "15m"


def test_recognizes_hourly_binance_market():
    s = _settings()
    m = _market(
        "Bitcoin Up or Down - September 21, 1PM ET",
        "bitcoin-up-or-down-september-21-2026-1pm-et",
        'This market uses the BTC/USDT 1 hour candle and the relevant "1H" candle.',
    )
    assert market_timeframe(m, s) == "1h"


def test_rejects_4h_and_daily_price_target_markets():
    s = _settings()
    assert market_timeframe(_market("BTC Up or Down 4h", "btc-updown-4h-123"), s) is None
    assert market_timeframe(_market("Will Bitcoin reach $85k?", "will-bitcoin-reach-85k"), s) is None


def test_timeframe_whitelist_can_be_narrowed():
    s = _settings(timeframes="15m")
    assert market_timeframe(_market("Bitcoin", "btc-updown-15m-123"), s) == "15m"
    assert market_timeframe(_market("Bitcoin", "btc-updown-5m-123"), s) is None
