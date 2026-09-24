import asyncio
from copy import deepcopy
from types import SimpleNamespace

import pytest

from jevymarket.maker_book import BookGap
from jevymarket.maker_ingress import SnapshotBookCache


def snapshot(token, ts=100, size=235.42):
    return dict(event_type="book", market="m", asset_id=token, timestamp=ts,
                bids=[dict(price="0.4", size=str(size))], asks=[dict(price="0.6", size="12")])


def delta(ts=99.999, size=225.42):
    return dict(event_type="price_change", market="m", timestamp=ts,
                price_changes=[dict(asset_id=t, side="BUY", price="0.4", size=str(size),
                                    best_bid="0.4", best_ask="0.6") for t in ("up", "down")])


def cache_ready():
    c = SnapshotBookCache("m", {t: (.01, 5) for t in ("up", "down")})
    for t in c.books:
        c.apply(snapshot(t), 100.1, 20)
    return c


@pytest.mark.parametrize("ts,size", [(99.999, 225.42), (99.999, 235.42), (99.984, 847.2)])
def test_startup_overlap_discards_without_mutating_snapshot(ts, size):
    c = cache_ready()
    before = deepcopy(c.replay_state())
    identities = {t: id(b) for t, b in c.books.items()}
    c.apply(delta(ts, size), 100.2, 20.1)
    assert c.last_discard["reason"] == "pre_snapshot_delta_discarded"
    assert c.replay_state() == before
    assert identities == {t: id(b) for t, b in c.books.items()}
    assert all(b.bids[.4] == 235.42 for b in c.books.values())


def test_equal_timestamp_update_is_not_discarded():
    c = cache_ready()
    c.apply(delta(100, 10), 100.2, 20.1)
    assert c.last_discard is None
    assert all(b.bids[.4] == 10 for b in c.books.values())
    assert not c.snapshot_baselines


def test_regression_after_new_delta_still_invalidates():
    c = cache_ready()
    c.apply(delta(100.001), 100.2, 20.1)
    with pytest.raises(BookGap):
        c.apply(delta(99.999), 100.3, 20.2)
    assert not any(b.ready for b in c.books.values())


def test_mixed_token_baselines_do_not_allow_partial_discard():
    c = cache_ready()
    # Use a new cache so the older second snapshot is itself legitimate.
    c = SnapshotBookCache("m", {t: (.01, 5) for t in ("up", "down")})
    c.apply(snapshot("up", 100), 100.1, 20)
    c.apply(snapshot("down", 99.998), 100.1, 20)
    with pytest.raises(BookGap):
        c.apply(delta(99.999), 100.2, 20.1)
    assert c.last_discard is None


@pytest.mark.parametrize("field,value", [("price", "nan"), ("price", "0"), ("size", "-1"),
                                         ("size", "inf"), ("side", "OTHER"), ("best_bid", "nan")])
def test_bad_old_row_is_not_silently_discarded(field, value):
    c = cache_ready()
    msg = delta()
    msg["price_changes"][0][field] = value
    with pytest.raises(BookGap):
        c.apply(msg, 100.2, 20.1)
    assert c.last_discard is None


@pytest.mark.parametrize("ts,wall,mono", [(94, 100.2, 20.1), (100.3, 100.2, 20.1),
                                       (99.999, 106, 26)])
def test_age_and_future_guards_are_unchanged(ts, wall, mono):
    c = cache_ready()
    with pytest.raises(BookGap):
        c.apply(delta(ts), wall, mono)
    assert c.last_discard is None


def test_no_snapshot_and_invalidated_generations_still_reject():
    c = cache_ready()
    c.invalidate()
    assert not c.snapshot_baselines
    with pytest.raises(BookGap):
        c.apply(delta(), 100.2, 20.1)


def test_missing_token_snapshot_still_rejects():
    c = SnapshotBookCache("m", {t: (.01, 5) for t in ("up", "down")})
    c.apply(snapshot("up"), 100.1, 20)
    with pytest.raises(BookGap):
        c.apply(delta(), 100.2, 20.1)


def test_bbo_mismatch_still_rejects_without_pruning_levels():
    c = cache_ready()
    msg = delta(100.001)
    for row in msg["price_changes"]:
        row.update(price="0.3", best_bid="0.3")
    with pytest.raises(BookGap) as error:
        c.apply(msg, 100.2, 20.1)
    assert str(error.value.__cause__) == "bbo_delta_mismatch"
    assert not c.last_discard
    assert dict(c.last_failure["before"]["books"]["up"]["bids"])[.4] == 235.42


def test_discard_does_not_refresh_expired_book():
    c = cache_ready()
    c.apply(delta(), 101.2, 21.1)
    assert c.last_discard
    assert not any(b.fresh(101.2, 21.1, 1) for b in c.books.values())


def test_discard_marker_is_cleared_by_next_nonbook_message():
    c = cache_ready()
    c.apply(delta(), 100.2, 20.1)
    c.apply(dict(event_type="last_trade_price", market="m"), 100.2, 20.1)
    assert c.last_discard is None


def test_new_snapshot_reopens_fence_only_for_its_token():
    c = cache_ready()
    c.apply(delta(100.001), 100.2, 20.1)
    c.apply(snapshot("up", 100.003), 100.3, 20.2)
    with pytest.raises(BookGap):
        c.apply(delta(100.002), 100.4, 20.3)


def test_transport_runtime_records_discard_not_success_or_trade():
    from jevymarket.maker import MakerRuntime

    runtime = object.__new__(MakerRuntime)
    records, trades = [], []
    runtime.store = SimpleNamespace(emit=lambda k, d: records.append((k, d)))
    runtime.session_id = "synthetic"
    runtime.engine = SimpleNamespace(on_trade=lambda *a: trades.append(a))
    runtime.signal = lambda: None
    c = cache_ready()
    runtime.apply_book_message(SimpleNamespace(condition="m", slug="test"), c, delta(), 100.2, 20.1)
    assert [k for k, _ in records] == ["book_discard"]
    assert not trades


@pytest.mark.parametrize("fail_at", ["send", "recv", "frame", "apply"])
def test_transport_risk_action_precedes_slow_close(monkeypatch, fail_at):
    import websockets.asyncio.client

    from jevymarket.maker import MakerRuntime

    async def scenario():
        runtime = object.__new__(MakerRuntime)
        c = cache_ready()
        order = SimpleNamespace(slug="test", active_ts=1, uncertain=False)
        calls = []
        runtime.engine = SimpleNamespace(active=lambda: order, halted=False,
            cancel=lambda *a: calls.append("cancel"), changed=lambda *a: calls.append("uncertain"))
        runtime.signal = lambda: calls.append("signal")
        closed = asyncio.Event()
        release = asyncio.Event()

        class Socket:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                closed.set()
                await release.wait()

            async def send(self, msg):
                if fail_at == "send":
                    raise ValueError("synthetic")

            async def recv(self):
                if fail_at == "recv":
                    raise ValueError("synthetic")
                return "frame"

        async def messages(*args):
            if fail_at == "frame":
                raise ValueError("synthetic")
            return [{}]

        def apply(*args):
            raise BookGap("bbo_delta_mismatch")

        runtime.stream_messages = messages
        runtime.apply_book_message = apply
        monkeypatch.setattr(websockets.asyncio.client, "connect", lambda *a, **k: Socket())
        task = asyncio.create_task(runtime.books(SimpleNamespace(end=float("inf"), slug="test"), c))
        try:
            await asyncio.wait_for(closed.wait(), 1)
            assert "cancel" in calls and "uncertain" in calls
            assert order.uncertain and runtime.engine.halted
            assert not any(b.ready for b in c.books.values())
            assert not task.done()  # Close handshake is still blocked.
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    asyncio.run(scenario())
