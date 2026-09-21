from jevymarket.config import Settings
from jevymarket.signal import Book, JevView, Skip, Trade, evaluate, kelly_fraction, round_to_tick


def _settings(**kw) -> Settings:
    base = dict(openrouter_api_key="x", polymarket_private_key="y", _env_file=None)
    base.update(kw)
    return Settings(**base)


def _book(yes_ask=0.40, yes_bid=0.38, tick=0.01, min_size=5.0) -> Book:
    return Book(
        yes_token_id="YES",
        no_token_id="NO",
        yes_bid=yes_bid,
        yes_ask=yes_ask,
        no_bid=round(1 - yes_ask, 4),
        no_ask=round(1 - yes_bid, 4),
        tick_size=tick,
        min_order_size=min_size,
    )


def _view(p_yes=0.6, answerable=0.9, clarity=3) -> JevView:
    return JevView(p_yes=p_yes, answerable=answerable, clarity=clarity, clarity_mean=float(clarity),
                   clarity_confidence=0.8, model="typesafe/jev-1.13", cost=0.0001, raw={})


def test_kelly_basic():
    assert kelly_fraction(0.6, 0.4) > 0
    assert kelly_fraction(0.3, 0.4) == 0.0
    assert kelly_fraction(0.5, 0.0) == 0.0


def test_round_to_tick():
    assert round_to_tick(0.4137, 0.01) == 0.41
    assert round_to_tick(0.4137, 0.001) == 0.414


def test_buys_yes_when_jev_higher_than_ask():
    t = evaluate(_view(p_yes=0.60), _book(yes_ask=0.40), _settings())
    assert isinstance(t, Trade)
    assert t.outcome == "YES" and t.side == "BUY"
    assert t.price == 0.40
    assert t.usd <= 5.0 * 1.5


def test_buys_no_when_jev_lower_than_market():
    t = evaluate(_view(p_yes=0.20), _book(yes_ask=0.40, yes_bid=0.38), _settings())
    assert isinstance(t, Trade)
    assert t.outcome == "NO"
    assert t.p == 0.8


def test_skips_when_edge_too_small():
    r = evaluate(_view(p_yes=0.45), _book(yes_ask=0.40), _settings(min_edge=0.08))
    assert isinstance(r, Skip)


def test_skips_when_not_answerable():
    r = evaluate(_view(answerable=0.3), _book(), _settings())
    assert isinstance(r, Skip) and "answerable" in r.reason


def test_skips_when_unclear():
    r = evaluate(_view(clarity=1), _book(), _settings(min_clarity=2))
    assert isinstance(r, Skip) and "clarity" in r.reason


def test_per_trade_cap_respected():
    t = evaluate(_view(p_yes=0.95), _book(yes_ask=0.40, min_size=1), _settings(max_usd_per_trade=3))
    assert isinstance(t, Trade)
    assert t.usd <= 3.0 + 0.01


def test_refuses_when_min_size_blows_cap():
    r = evaluate(_view(p_yes=0.95), _book(yes_ask=0.90, min_size=100), _settings(max_usd_per_trade=3))
    assert isinstance(r, Skip)


def test_refuses_longshots_outside_band():
    # Jev says 15% NO vs a 6-cent ask: "edge" 0.09, but the ask is outside the band.
    r = evaluate(_view(p_yes=0.85), _book(yes_ask=0.94, yes_bid=0.93), _settings())
    assert isinstance(r, Skip) and "band" in r.reason
    # Widening the band lets it through.
    t = evaluate(_view(p_yes=0.85), _book(yes_ask=0.94, yes_bid=0.93), _settings(min_trade_price=0.01))
    assert isinstance(t, Trade) and t.outcome == "NO"


def test_up_down_labels_are_preserved():
    book = _book(yes_ask=0.40)
    book = Book(
        yes_token_id=book.yes_token_id,
        no_token_id=book.no_token_id,
        yes_bid=book.yes_bid,
        yes_ask=book.yes_ask,
        no_bid=book.no_bid,
        no_ask=book.no_ask,
        tick_size=book.tick_size,
        min_order_size=book.min_order_size,
        yes_label="Up",
        no_label="Down",
    )
    t = evaluate(_view(p_yes=0.60), book, _settings())
    assert isinstance(t, Trade)
    assert t.outcome == "UP"
