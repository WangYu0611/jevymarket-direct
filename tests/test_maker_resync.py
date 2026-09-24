"""Deterministic split-update and snapshot recovery; no live network or orders."""
import asyncio
import json
from copy import deepcopy
from types import SimpleNamespace

import pytest

from jevymarket.maker_book import BookGap
from jevymarket.maker_ingress import SnapshotBookCache
from jevymarket.maker_resync import SnapshotResync, read_resync_books, root_reason

NOW = 1800000000.0
NS = 1000000000000
SPECS = {"11": (.01, 5), "22": (.01, 5)}


def book(token="11", bid=".4", ts=NOW, hash_="h11"):
    return {"event_type": "book", "market": "c", "asset_id": token, "timestamp": ts * 1000,
            "hash": hash_, "bids": [{"price": bid, "size": "10"}],
            "asks": [{"price": ".7", "size": "20"}]}


def change(price=".6", ts=NOW+.1, hash_="h"):
    return {"event_type": "price_change", "market": "c", "timestamp": ts*1000,
        "price_changes": [{"asset_id": t, "side": "BUY", "price": price, "size": "0",
                           "best_bid": ".4", "best_ask": ".7", "hash": hash_+t} for t in SPECS]}


def prepare():
    cache = SnapshotBookCache("c", SPECS)
    for token in SPECS:
        msg = book(token, ".6")
        msg["bids"] += [{"price": ".5", "size": "5"}, {"price": ".4", "size": "9"}]
        cache.apply(msg, NOW, 10)
    with pytest.raises(BookGap) as exc:
        cache.apply(change(), NOW+.2, 10.2)
    assert root_reason(exc.value) == "bbo_delta_mismatch"
    cache.invalidate()
    return cache, SnapshotResync(cache, change(), NOW+.2, 10.2, NS)


def snap(token="11", **kwargs):
    return book(token, ts=NOW+.1, hash_="h"+token, **kwargs)


def test_pair_is_published_atomically_and_preserves_object_identity():
    c, r = prepare()
    old = dict(c.books)
    assert not r.feed(snap(), NOW+.201, 10.201, NS+1000000)
    assert not any(b.ready or b.bids for b in c.books.values())
    assert r.feed(snap("22"), NOW+.202, 10.202, NS+2000000) is True
    assert all(c.books[t] is old[t] for t in SPECS)
    assert all(b.bid == .4 and b.ask == .7 for b in c.books.values())
    assert r.method == "matching_full_snapshots"
    assert c.books["11"].received_mono == 10.201


def test_split_delta_needs_explicit_next_version_and_keeps_boundary():
    c, r = prepare()
    assert not r.feed(change(".5"), NOW+.201, 10.201, NS+1000000)
    assert not any(b.ready for b in c.books.values())
    boundary = change(".3", NOW+.102, "next")
    boundary["price_changes"][0]["size"] = "2"
    assert r.feed(boundary, NOW+.202, 10.202, NS+2000000) == "reprocess"
    assert r.method == "validated_split_delta"
    assert c.books["11"].received_mono == 10.201  # NOT boundary receipt time
    assert c.books["11"].bids == {.4: 9}
    c.apply(boundary, NOW+.202, 10.202)
    assert c.books["11"].bids == {.4: 9, .3: 2}  # boundary applied exactly once


def test_missing_delta_cannot_be_fixed_by_next_version():
    c, r = prepare()
    with pytest.raises(BookGap):
        r.feed(change(".3", NOW+.102, "next"), NOW+.202, 10.202, NS+2000000)
    assert not any(b.ready for b in c.books.values())


@pytest.mark.parametrize("mutation", [
    lambda m: m.update(hash="other"),
    lambda m: m.update(timestamp=(NOW+.101)*1000),
    lambda m: m.update(timestamp=(NOW+.099)*1000),
    lambda m: m.update(market="other"),
    lambda m: m.update(asset_id="33"),
    lambda m: m.update(bids=[{"price": ".41", "size": "10"}]),
    lambda m: m.update(bids=[{"price": ".4", "size": "NaN"}]),
    lambda m: m.update(bids=[{"price": ".4", "size": "-1"}]),
    lambda m: m.update(bids=[{"price": ".8", "size": "1"}]),
    lambda m: m.update(bids=[]),
    lambda m: m.update(asks=None),
    lambda m: m.update(bids=[{"price": ".4", "size": "1"}]*2),
])
def test_bad_snapshot_never_restores_eligibility(mutation):
    c, r = prepare()
    msg = snap()
    mutation(msg)
    with pytest.raises(BookGap):
        r.feed(msg, NOW+.201, 10.201, NS+1000000)
    assert not any(b.ready or b.bids or b.asks for b in c.books.values())


@pytest.mark.parametrize("kind", ["tick_size_change", "market_resolved", "last_trade_price", "best_bid_ask"])
def test_unexpected_interleaving_reconnects_not_silently_ignored(kind):
    c, r = prepare()
    with pytest.raises(BookGap):
        r.feed({"event_type": kind, "market": "c", "timestamp": (NOW+.1)*1000},
               NOW+.201, 10.201, NS+1000000)
    assert not any(b.ready for b in c.books.values())


@pytest.mark.parametrize("elapsed", [100000000, 100000001, 500000000])
def test_timeout_is_absolute_and_invalidates_even_one_staged_snapshot(elapsed):
    c, r = prepare()
    r.feed(snap(), NOW+.201, 10.201, NS+1000000)
    with pytest.raises(BookGap, match="resync_timeout"):
        r.remaining(NS+elapsed)
    assert not any(b.ready for b in c.books.values())


def test_regressed_local_clock_fails_closed():
    c, r = prepare()
    with pytest.raises(BookGap, match="resync_clock_invalid"):
        r.remaining(NS-1)
    assert not any(b.ready for b in c.books.values())


def test_external_generation_invalidation_cannot_be_undone():
    c, r = prepare()
    c.invalidate()
    with pytest.raises(BookGap, match="resync_generation_changed"):
        r.feed(snap(), NOW+.201, 10.201, NS+1000000)
    assert not any(b.ready for b in c.books.values())


def test_snapshot_then_delta_for_same_token_is_ambiguous():
    c, r = prepare()
    r.feed(snap(), NOW+.201, 10.201, NS+1000000)
    with pytest.raises(BookGap, match="resync_delta_after_snapshot"):
        r.feed(change(".5"), NOW+.202, 10.202, NS+2000000)
    assert not any(b.ready for b in c.books.values())


def test_duplicate_snapshot_fails_closed():
    _, r = prepare()
    r.feed(snap(), NOW+.201, 10.201, NS+1000000)
    with pytest.raises(BookGap, match="resync_duplicate_snapshot"):
        r.feed(snap(), NOW+.202, 10.202, NS+2000000)


def test_source_age_and_quote_age_are_not_relaxed_or_refreshed():
    c, r = prepare()
    r.feed(snap(), NOW+1.2, 10.201, NS+1000000)
    assert r.feed(snap("22"), NOW+1.201, 10.202, NS+2000000)
    assert all(b.ready and not b.fresh(NOW+1.201, 10.202, 1) for b in c.books.values())
    assert c.books["11"].ts == NOW+.1


@pytest.mark.parametrize("wall", [NOW, NOW+5.2])
def test_future_or_over_five_second_source_cannot_restore(wall):
    c, r = prepare()
    with pytest.raises(BookGap):
        r.feed(snap(), wall, 10.201, NS+1000000)
    assert not any(b.ready for b in c.books.values())


@pytest.mark.parametrize("mutation", [
    lambda m: m["price_changes"][0].pop("hash"),
    lambda m: m["price_changes"].pop(),
    lambda m: m["price_changes"][0].update(hash=""),
    lambda m: m["price_changes"][0].update(best_bid="NaN"),
    lambda m: m["price_changes"][0].update(size="NaN"),
    lambda m: m["price_changes"][0].update(side="oops"),
])
def test_ineligible_trigger_keeps_original_reconnect_path(mutation):
    c, _ = prepare()
    m = change()
    mutation(m)
    with pytest.raises((ValueError, KeyError)):
        SnapshotResync(c, m, NOW+.2, 10.2, NS)
    assert not any(b.ready for b in c.books.values())


def test_buffer_bound_and_no_deadline_extension(monkeypatch):
    from jevymarket import maker_resync
    c, r = prepare()
    monkeypatch.setattr(maker_resync, "RESYNC_MESSAGES", 4)
    r.feed(change(".5"), NOW+.201, 10.201, NS+1000000)
    with pytest.raises(BookGap, match="resync_limit"):
        r.feed(change(".5"), NOW+.202, 10.202, NS+2000000)
    assert r.started_ns == NS and not any(b.ready for b in c.books.values())


class Clock:
    def __init__(self):
        self.elapsed = 0.0

    def time(self):
        return NOW + .2 + self.elapsed

    def monotonic(self):
        return 10.2 + self.elapsed

    def perf_counter_ns(self):
        return NS + round(self.elapsed*1e9)


def runtime_fixture(clock):
    c, _ = prepare()
    # Reconstruct the actual pre-rejection state for the runtime's first frame.
    before = c.last_failure["before"]
    for token, data in before["books"].items():
        b = c.books[token]
        b.bids, b.asks = dict(data["bids"]), dict(data["asks"])
        b.ts, b.received_mono, b.ready = data["ts"], data["received_mono"], True
    events, applied = [], []
    rt = SimpleNamespace(c=SimpleNamespace(max_book_age_seconds=1), session_id="test", protected=False,
        store=SimpleNamespace(emit=lambda k,d: events.append((k,d))), signal=lambda: None,
        engine=SimpleNamespace(halted=True, risk_reserved=5, orders=["existing-uncertain-paper-order"]))

    async def messages(ws, raw, name):
        data=json.loads(raw)
        if not isinstance(data, list):
            data=[data]
        return data

    def apply(market, cache, msg, wall, mono):
        applied.append(deepcopy(msg))
        cache.apply(msg, wall, mono)

    def protect(market, cache):
        rt.protected=True
        cache.invalidate()

    rt.stream_messages=messages
    rt.apply_book_message=apply
    rt.protect_book_failure=protect
    return rt,c,events,applied


@pytest.mark.asyncio
@pytest.mark.parametrize("one_frame", [False, True])
async def test_reader_protects_before_next_await_and_handles_same_frame(one_frame):
    clock=Clock()
    rt,c,events,applied=runtime_fixture(clock)
    market=SimpleNamespace(condition="c",slug="btc-updown-5m-1800000000",end=NOW+1)
    messages=[change(),snap(),snap("22"),change(".3", NOW+.102, "next")]
    frames=[messages] if one_frame else [[m] for m in messages]
    calls=[]

    async def recv():
        if calls and not one_frame:
            assert rt.protected
        if not frames:
            assert all(b.ready for b in c.books.values())
            raise ConnectionError("end")
        calls.append(1)
        clock.elapsed += .001
        return json.dumps(frames.pop(0))

    with pytest.raises(ConnectionError):
        await read_resync_books(rt,market,c,SimpleNamespace(recv=recv),SimpleNamespace(done=lambda:False),
                                lambda m:m,"test",clock=clock)
    assert [x[1]["state"] for x in events if x[0]=="book_resync"]==["started","recovered"]
    assert len(applied)==2  # trigger + boundary, snapshots were quarantined
    assert rt.engine.halted and rt.engine.risk_reserved==5 and len(rt.engine.orders)==1


@pytest.mark.asyncio
async def test_reader_batch_boundary_is_processed_exactly_once():
    clock=Clock()
    rt,c,events,applied=runtime_fixture(clock)
    frames=[[change(),change(".5"),change(".3",NOW+.102,"next")]]

    async def recv():
        if not frames:
            raise ConnectionError("done")
        return json.dumps(frames.pop())

    with pytest.raises(ConnectionError):
        await read_resync_books(rt,SimpleNamespace(end=NOW+1,slug="s"),c,
            SimpleNamespace(recv=recv),SimpleNamespace(done=lambda:False),lambda m:m,"test",clock=clock)
    assert len(applied)==2
    assert events[-1][1]["method"]=="validated_split_delta"


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [ConnectionError, asyncio.CancelledError])
async def test_reader_incomplete_connection_never_leaves_ready_book(failure):
    clock=Clock()
    rt,c,events,_=runtime_fixture(clock)
    frames=[[change(),snap()]]

    async def recv():
        if not frames:
            assert rt.protected and not any(b.ready for b in c.books.values())
            raise failure()
        return json.dumps(frames.pop())

    with pytest.raises(failure):
        await read_resync_books(rt,SimpleNamespace(end=NOW+1,slug="s"),c,
            SimpleNamespace(recv=recv),SimpleNamespace(done=lambda:False),lambda m:m,"test",clock=clock)
    assert events[-1][1]["state"]=="aborted"
    assert not any(b.ready for b in c.books.values())


@pytest.mark.asyncio
async def test_reader_early_timeout_does_not_end_before_deadline():
    clock=Clock()
    rt,c,events,_=runtime_fixture(clock)
    count=0

    async def recv():
        nonlocal count
        count+=1
        if count==1:
            return json.dumps([change()])
        clock.elapsed=.099 if count==2 else .101
        raise TimeoutError()

    with pytest.raises(BookGap,match="resync_timeout"):
        await read_resync_books(rt,SimpleNamespace(end=NOW+1,slug="s"),c,
            SimpleNamespace(recv=recv),SimpleNamespace(done=lambda:False),lambda m:m,"test",clock=clock)
    assert count==3 and events[-1][1]["state"]=="aborted"
    assert not any(b.ready for b in c.books.values())


def test_same_hash_cannot_have_contradictory_bbo():
    c,r=prepare()
    r.feed(change(".5"), NOW+.201, 10.201, NS+1000000)
    msg=change(".3")
    msg["price_changes"][0]["best_bid"]=".3"
    with pytest.raises(BookGap,match="resync_version_mismatch"):
        r.feed(msg,NOW+.202,10.202,NS+2000000)
    assert not any(b.ready for b in c.books.values())


def test_summary_is_scoped_readonly_and_distinguishes_stale_recovery(tmp_path):
    import sqlite3

    from jevymarket.maker_resync import resync_summary
    path=tmp_path/"events.db"
    with sqlite3.connect(path) as conn:
        conn.execute("CREATE TABLE maker_events(id INTEGER PRIMARY KEY, kind TEXT,data TEXT)")
        for row in [{"state":"recovered","elapsed_ms":99,"method":"old"},
                    {"state":"started"},
                    {"state":"recovered","elapsed_ms":.5,"method":"matching_full_snapshots","quote_fresh":False},
                    {"state":"started"},{"state":"aborted"}]:
            conn.execute("INSERT INTO maker_events(kind,data) VALUES('book_resync',?)", (json.dumps(row),))
        conn.execute("INSERT INTO maker_events(kind,data) VALUES('book_resync_input','NOT_JSON')")
    before=path.read_bytes()
    with sqlite3.connect(path.resolve().as_uri()+"?mode=ro", uri=True) as conn:
        conn.execute("PRAGMA query_only=ON")
        result=resync_summary(conn,2)
    assert result["started"]==2 and result["recovered"]==result["aborted"]==1
    assert result["max_recovery_wait_ms"]==.5 and result["recovered_but_quote_stale"]==1
    assert "old" not in result["recovery_methods"] and before==path.read_bytes()
