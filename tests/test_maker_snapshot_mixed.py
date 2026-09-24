"""Mixed old/equal startup messages: exact no-op equality, never guessed depth."""
from copy import deepcopy

import pytest

from jevymarket.maker_book import BookGap
from jevymarket.maker_ingress import SnapshotBookCache

NOW = 1_800_000_000.0
SPECS = {"11": (.01, 5), "22": (.01, 5)}


def snapshot(token, ts, size):
    return {"event_type": "book", "market": "c", "asset_id": token, "timestamp": ts * 1000,
            "bids": [{"price": ".4", "size": str(size)}], "asks": [{"price": ".6", "size": "20"}]}


def seeded(offset=.001):
    cache = SnapshotBookCache("c", SPECS)
    cache.apply(snapshot("11", NOW, 10), NOW+.01, 100)
    cache.apply(snapshot("22", NOW+offset, 20), NOW+.01, 100)
    return cache


def delta():
    return {"event_type": "price_change", "market": "c", "timestamp": NOW * 1000,
            "price_changes": [{"asset_id": token, "side": "BUY", "price": ".4", "size": "10",
                               "best_bid": ".4", "best_ask": ".6"} for token in SPECS]}


@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize("offset", [.001, .002])
def test_mixed_old_equal_noop_preserves_every_cached_field(reverse, offset):
    cache, msg = seeded(offset), delta()
    if reverse:
        msg["price_changes"].reverse()
    before = deepcopy(cache.replay_state())
    objects = {t: id(b) for t, b in cache.books.items()}
    cache.apply(msg, NOW+.1, 100.1)
    assert cache.replay_state() == before
    assert {t: id(b) for t, b in cache.books.items()} == objects
    assert cache.last_discard["equal_noop_tokens"] == ["11"]
    assert cache.books["22"].bids[.4] == 20  # older size=10 must not rewind it
    assert not cache.books["11"].fresh(NOW+1.1, 101.1, 1)  # discard never refreshes


@pytest.mark.parametrize(("key", "value"), [
    ("size", "11"), ("size", "0"), ("price", ".41"),
    ("best_bid", ".41"), ("best_ask", ".59"),
    ("size", "NaN"), ("best_bid", None), ("side", "UNKNOWN"),
])
def test_equal_time_non_noop_or_malformed_row_never_discarded(key, value):
    cache, msg = seeded(), delta()
    msg["price_changes"][0][key] = value
    with pytest.raises(BookGap):
        cache.apply(msg, NOW+.1, 100.1)
    assert cache.last_discard is None and not any(b.ready for b in cache.books.values())


def test_equal_missing_bbo_stays_strict():
    cache, msg = seeded(), delta()
    del msg["price_changes"][0]["best_ask"]
    with pytest.raises(BookGap):
        cache.apply(msg, NOW+.1, 100.1)
    assert cache.last_discard is None


def test_equal_only_event_keeps_original_apply_and_receipt_semantics():
    cache, msg = seeded(0), delta()
    msg["price_changes"][1]["size"] = "21"
    cache.apply(msg, NOW+.1, 100.1)
    assert cache.last_discard is None
    assert cache.books["22"].bids[.4] == 21 and cache.books["22"].received_mono == 100.1
    assert not cache.snapshot_baselines


def test_same_generation_new_delta_closes_startup_exception():
    cache = seeded()
    event = delta()
    event["price_changes"] = event["price_changes"][:1]
    event["timestamp"] = (NOW+.05)*1000
    cache.apply(event, NOW+.06, 100.06)
    with pytest.raises(BookGap):
        cache.apply(delta(), NOW+.1, 100.1)
    assert cache.last_discard is None


def test_newer_and_older_token_mixture_is_not_a_noop_discard():
    cache, msg = seeded(), delta()
    msg["timestamp"] = (NOW+.0005)*1000
    with pytest.raises(BookGap):
        cache.apply(msg, NOW+.1, 100.1)
    assert cache.last_discard is None


@pytest.mark.parametrize(("wall", "mono"), [(NOW+5.1, 100.1), (NOW+.1, 105.1), (NOW+.1, 99)])
def test_invalid_age_or_clock_does_not_enter_discard(wall, mono):
    cache = seeded()
    with pytest.raises(BookGap):
        cache.apply(delta(), wall, mono)
    assert cache.last_discard is None


def test_all_strictly_older_behavior_unchanged():
    cache, msg = seeded(), delta()
    msg["timestamp"] = (NOW-.001)*1000
    cache.apply(msg, NOW+.1, 100.1)
    assert cache.last_discard["equal_noop_tokens"] == []
    assert cache.books["22"].bids[.4] == 20


def test_multiple_equal_rows_all_have_to_be_noops():
    cache, msg = seeded(), delta()
    msg["price_changes"].insert(1, dict(msg["price_changes"][0], size="15"))
    with pytest.raises(BookGap):
        cache.apply(msg, NOW+.1, 100.1)
    assert cache.last_discard is None
