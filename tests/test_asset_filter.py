from types import SimpleNamespace

from jevymarket.config import Settings
from jevymarket.markets import market_asset_symbol


def _settings(assets: str = "BTC,ETH,SOL") -> Settings:
    return Settings(allowed_assets=assets, _env_file=None)


def _market(question: str, slug: str = "", category: str = "", tags=()):
    return SimpleNamespace(
        question=question,
        slug=slug,
        category=category,
        tags=tuple(SimpleNamespace(slug=slug, label=label) for slug, label in tags),
    )


def test_matches_bitcoin_aliases():
    s = _settings()
    assert market_asset_symbol(_market("Will Bitcoin be above $100k?"), s) == "BTC"
    assert market_asset_symbol(_market("BTC up or down?", "btc-up-or-down"), s) == "BTC"


def test_matches_ethereum_aliases():
    s = _settings()
    assert market_asset_symbol(_market("Will Ethereum hit $5k?"), s) == "ETH"
    assert market_asset_symbol(_market("ETH above $4,000?"), s) == "ETH"
    assert market_asset_symbol(_market("Ether price target"), s) == "ETH"


def test_matches_solana_aliases():
    s = _settings()
    assert market_asset_symbol(_market("Will Solana reach $300?"), s) == "SOL"
    assert market_asset_symbol(_market("SOL above $250?", "sol-above-250"), s) == "SOL"


def test_short_tickers_do_not_match_substrings():
    s = _settings()
    assert market_asset_symbol(_market("Whether rates fall in September?"), s) is None
    assert market_asset_symbol(_market("Solar energy bill passes?"), s) is None


def test_non_crypto_market_is_rejected():
    assert market_asset_symbol(_market("Will voter turnout exceed 60%?"), _settings()) is None


def test_whitelist_can_be_narrowed():
    s = _settings("BTC")
    assert market_asset_symbol(_market("Bitcoin above $100k?"), s) == "BTC"
    assert market_asset_symbol(_market("Ethereum above $5k?"), s) is None
