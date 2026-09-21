from types import SimpleNamespace

from jevymarket.markets import book_from_orderbooks


def _level(price: float, size: float):
    return SimpleNamespace(price=price, size=size)


def test_book_microstructure_depth_and_directional_imbalance():
    market = SimpleNamespace(
        outcomes=SimpleNamespace(
            yes=SimpleNamespace(token_id="UP", label="Up"),
            no=SimpleNamespace(token_id="DOWN", label="Down"),
        ),
        trading=SimpleNamespace(
            minimum_tick_size=0.01,
            minimum_order_size=5,
        ),
    )
    up = SimpleNamespace(
        asset_id="UP",
        bids=(_level(0.30, 100), _level(0.39, 50)),
        asks=(_level(0.50, 10), _level(0.42, 20)),
        tick_size=0.01,
        min_order_size=5,
    )
    down = SimpleNamespace(
        asset_id="DOWN",
        bids=(_level(0.50, 10), _level(0.59, 20)),
        asks=(_level(0.70, 100), _level(0.62, 50)),
        tick_size=0.01,
        min_order_size=5,
    )

    book = book_from_orderbooks(market, [up, down])
    assert book.yes_bid == 0.39
    assert book.yes_ask == 0.42
    assert book.no_bid == 0.59
    assert book.no_ask == 0.62

    # Far levels (0.30 Up bid, 0.70 Down ask) are outside the 5-cent band.
    assert abs(book.yes_bid_depth_5c_usd - 19.5) < 1e-9
    assert abs(book.yes_ask_depth_5c_usd - 8.4) < 1e-9
    assert abs(book.no_bid_depth_5c_usd - 11.8) < 1e-9
    assert abs(book.no_ask_depth_5c_usd - 31.0) < 1e-9
    assert book.directional_imbalance_5c is not None
    assert book.directional_imbalance_5c > 0
