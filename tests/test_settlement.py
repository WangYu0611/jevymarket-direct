from decimal import Decimal
from types import SimpleNamespace

from jevymarket.config import Settings
from jevymarket.settlement import resolved_market_from_market, settle_pending_markets
from jevymarket.store import Store


def _resolved_market(slug: str, *, up_wins: bool = True):
    yes_price = Decimal("1") if up_wins else Decimal("0")
    no_price = Decimal("0") if up_wins else Decimal("1")
    return SimpleNamespace(
        slug=slug,
        condition_id="0xcondition",
        description="BTC five minute market",
        state=SimpleNamespace(closed=True),
        outcomes=SimpleNamespace(
            yes=SimpleNamespace(label="Up", price=yes_price),
            no=SimpleNamespace(label="Down", price=no_price),
        ),
    )


def test_extracts_official_resolved_up_result():
    settings = Settings(allowed_timeframes="5m,15m,1h", _env_file=None)
    result = resolved_market_from_market(
        _resolved_market("btc-updown-5m-1789980000"),
        settings,
    )
    assert result is not None
    assert result.winner == "UP"
    assert result.up_won is True
    assert result.up_final_price == 1.0
    assert result.down_final_price == 0.0
    assert result.timeframe == "5m"


def test_refuses_ambiguous_closed_market():
    settings = Settings(allowed_timeframes="5m", _env_file=None)
    market = _resolved_market("btc-updown-5m-1789980000")
    market.outcomes.yes.price = Decimal("0.65")
    market.outcomes.no.price = Decimal("0.35")
    assert resolved_market_from_market(market, settings) is None


async def test_settle_pending_market_backfills_store(tmp_path):
    store = Store(tmp_path / "t.db")
    slug = "btc-updown-5m-1789980000"
    store.log_decision(
        slug=slug,
        condition_id="0xcondition",
        p_yes=0.8,
        jev_p_yes=0.6,
        timeframe="5m",
        midpoint=0.7,
        answerable=0.9,
        clarity=3,
        action="skip",
        state_json={
            "timeframe": "5m",
            "market_end_time": "2026-09-21T08:45:00+00:00",
            "quantitative_signal": {"p_up": 0.8},
        },
        raw_json={},
        ts=1.0,
    )

    class FakeClient:
        async def get_market(self, *, slug: str):
            return _resolved_market(slug)

    settings = Settings(allowed_timeframes="5m", _env_file=None)
    inserted = await settle_pending_markets(
        FakeClient(),
        settings,
        store,
        limit=10,
        grace_seconds=0,
    )
    assert inserted == 1
    result = store.get_market_result(slug)
    assert result is not None
    assert result["winner"] == "UP"
    assert result["up_won"] == 1
    store.close()
