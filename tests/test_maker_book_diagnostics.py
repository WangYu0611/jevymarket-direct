"""Recovery/evidence regressions; keep the late-maker trading policy unchanged."""
import json
import sqlite3
from dataclasses import asdict
from types import SimpleNamespace

import pytest

from jevymarket.maker_book import BookCache, BookGap
from jevymarket.maker_config import MakerConfig
from jevymarket.maker_diagnostics import (
    ReactionWindow,
    diagnostic_report,
    merge_reactions,
    safe_book_message,
)
from jevymarket.maker_protocol import IO_REVISION, error_reason

NOW = 1_800_000_291.0
SPECS = {"11": (.01, 5), "22": (.01, 5)}


def snapshot(token="11", ts=NOW, bids=None, asks=None):
    return {"event_type": "book", "market": "c", "asset_id": token, "timestamp": ts * 1000,
            "bids": [{"price": ".4", "size": "10"}] if bids is None else bids,
            "asks": [{"price": ".6", "size": "20"}] if asks is None else asks}


def cache_fixture():
    cache = BookCache("c", SPECS)
    for token in SPECS:
        cache.apply(snapshot(token), NOW, 100)
    return cache


def bad_delta():
    return {"event_type": "price_change", "market": "c", "timestamp": (NOW+.1)*1000,
            "price_changes": [{"asset_id": "11", "side": "BUY", "price": ".41", "size": "5",
                               "best_bid": ".5", "best_ask": ".6"}]}


def test_new_generation_resets_watermarks_but_requires_snapshots():
    c = cache_fixture()
    old = c.generation
    c.invalidate()
    assert c.generation > old
    assert all(b.ts == 0 and b.received_mono == 0 and not b.ready for b in c.books.values())
    c.apply(snapshot(ts=NOW-.1), NOW+.2, 100.2)
    assert c.books["11"].fresh(NOW+.2, 100.2, 1)
    assert not c.books["22"].ready


def test_old_snapshot_is_still_rejected_in_same_generation():
    c = cache_fixture()
    with pytest.raises(BookGap) as exc:
        c.apply(snapshot(ts=NOW-.1), NOW+.2, 100.2)
    assert error_reason(exc.value) == "out_of_order_snapshot"
    assert c.last_failure["before"]["books"]["11"]["ts"] == NOW
    assert not any(b.ready for b in c.books.values())


def test_resubscribe_does_not_make_stale_snapshots_usable():
    c = cache_fixture()
    c.invalidate()
    with pytest.raises(BookGap) as exc:
        c.apply(snapshot(ts=NOW-6), NOW, 100)
    assert error_reason(exc.value) == "stale_or_future_book_message"


def test_delta_cannot_seed_missing_snapshot():
    c = BookCache("c", SPECS)
    with pytest.raises(BookGap) as exc:
        c.apply(bad_delta(), NOW+.2, 100.2)
    assert error_reason(exc.value) == "delta_without_snapshot_or_out_of_order"


def test_failure_keeps_pre_event_and_attempted_state_without_partial_commit():
    c = cache_fixture()
    with pytest.raises(BookGap) as exc:
        c.apply(bad_delta(), NOW+.2, 100.2)
    assert error_reason(exc.value) == "bbo_delta_mismatch"
    context = c.last_failure
    assert context["before"]["books"]["11"]["bids"] == [[.4, 10]]
    assert [.41, 5] in context["candidate"]["books"]["11"]["bids"]
    assert not any(b.bids or b.asks or b.ready for b in c.books.values())
    json.dumps(context, allow_nan=False)


def test_rejection_can_be_replayed_from_captured_state():
    c = cache_fixture()
    with pytest.raises(BookGap):
        c.apply(bad_delta(), NOW+.2, 100.2)
    before = json.loads(json.dumps(c.last_failure))["before"]
    replay = BookCache(before["condition"], SPECS)
    replay.generation = before["generation"]
    for token, data in before["books"].items():
        b = replay.books[token]
        b.bids, b.asks = dict(data["bids"]), dict(data["asks"])
        b.ts, b.received_mono, b.ready = data["ts"], data["received_mono"], data["ready"]
    with pytest.raises(BookGap) as exc:
        replay.apply(bad_delta(), NOW+.2, 100.2)
    assert error_reason(exc.value) == "bbo_delta_mismatch"


def test_successful_delta_keeps_existing_object_and_deletes_zero_levels():
    c = cache_fixture()
    b = c.books["11"]
    msg = bad_delta()
    msg["price_changes"][0].update(price=".4", size="0", best_bid="0")
    c.apply(msg, NOW+.2, 100.2)
    assert c.books["11"] is b and b.bid is None and b.ask == .6
    assert c.last_failure is None


def test_tick_notice_captures_old_constraints_then_preserves_new_tick():
    c = cache_fixture()
    with pytest.raises(BookGap) as exc:
        c.apply({"event_type": "tick_size_change", "market": "c", "asset_id": "11",
                 "timestamp": NOW*1000, "new_tick_size": ".001"}, NOW, 100)
    assert error_reason(exc.value) == "tick_changed_resubscribe"
    assert c.last_failure["before"]["books"]["11"]["tick"] == .01
    assert c.books["11"].tick == .001 and not c.books["11"].ready


@pytest.mark.parametrize(("bids", "asks", "shape", "status"), [
    ([], [], "empty", "valid_empty"),
    ([{"price": ".99", "size": "10"}], [], "bid_only", "valid_bid_only"),
    ([], [{"price": ".01", "size": "10"}], "ask_only", "valid_ask_only"),
    (None, None, "two_sided", "ready"),
])
def test_fresh_one_sided_and_empty_books_are_not_missing_data(bids, asks, shape, status):
    c = BookCache("c", SPECS)
    c.apply(snapshot(bids=bids, asks=asks), NOW, 100)
    b = c.books["11"]
    d = b.diagnostic(NOW+.2, 100.2, 1)
    assert d["shape"] == shape and d["status"] == status and d["timely"]
    assert d["trade_eligible"] == (shape == "two_sided")
    if shape != "two_sided":
        assert not b.fresh(NOW+.2, 100.2, 1)  # no strategy relaxation


def test_missing_and_invalidated_statuses_are_different():
    c = BookCache("c", SPECS)
    assert c.books["11"].diagnostic(NOW, 100, 1)["status"] == "awaiting_snapshot"
    c.apply(snapshot(), NOW, 100)
    c.invalidate()
    d = c.books["11"].diagnostic(NOW, 100, 1)
    assert d["status"] == "invalidated" and d["source_age_seconds"] is None


@pytest.mark.parametrize(("wall", "mono", "status"), [
    (NOW+2, 100.1, "stale_source"), (NOW+.1, 102, "stale_receive"),
    (NOW-.1, 100, "future_source"), (NOW, 99, "receive_clock_invalid"),
])
def test_clock_and_shape_are_distinct_diagnostics(wall, mono, status):
    d = cache_fixture().books["11"].diagnostic(wall, mono, 1)
    assert d["status"] == status and d["shape"] == "two_sided" and not d["trade_eligible"]


@pytest.mark.parametrize("value", [None, 1, [None], ["broken"], [{"price": "NaN", "size": "5"}]])
def test_malformed_snapshot_fails_closed_and_evidence_serializes(value):
    c = cache_fixture()
    msg = snapshot()
    msg["bids"] = value
    with pytest.raises(BookGap):
        c.apply(msg, NOW, 100)
    json.dumps(safe_book_message(msg), allow_nan=False)
    assert not any(b.ready for b in c.books.values())


def test_whitelist_handles_nonfinite_and_does_not_export_extra_fields():
    msg = snapshot()
    msg.update(authorization="NEVER_EXPORT", error_body="NEVER_EXPORT", price=float("inf"))
    msg["bids"] = [{"price": float("nan"), "size": "5", "key": "NEVER_EXPORT"}, None]
    safe = safe_book_message(msg)
    assert "NEVER_EXPORT" not in json.dumps(safe, allow_nan=False)
    assert safe["price"] is None and "diagnostic_sanitization" in safe


def test_reaction_windows_measure_no_order_events_and_keep_watchdog_separate():
    meter = ReactionWindow()
    for latency in (1, 2, 100, 100.01):
        meter.add(latency, True)
    meter.add(.4, False)
    report = merge_reactions([json.loads(json.dumps(meter.take()))])
    assert report["event"]["samples"] == 4 and report["event"]["over_100ms"] == 1
    assert report["event"]["p50_upper_ms"] == 2
    assert report["event"]["p99_upper_ms"] == 100.1
    assert report["watchdog"]["samples"] == 1 and meter.take()["groups"] == {}


@pytest.mark.parametrize("value", [-1, float("nan"), float("inf")])
def test_invalid_latency_is_not_published(value):
    with pytest.raises(ValueError):
        ReactionWindow().add(value, True)


def test_absent_latency_is_missing_not_zero():
    report = merge_reactions([])
    assert report["event"]["samples"] == 0 and report["event"]["p99_upper_ms"] is None


def test_histogram_overflow_uses_observed_upper_bound():
    meter = ReactionWindow()
    meter.add(20000, True)
    report = merge_reactions([meter.take()])
    assert report["event"]["p99_upper_ms"] == 20000


def make_db(path):
    with sqlite3.connect(path) as conn:
        conn.executescript("""CREATE TABLE maker_meta(id INTEGER PRIMARY KEY,data TEXT);
            CREATE TABLE maker_orders(id TEXT PRIMARY KEY,data TEXT);
            CREATE TABLE maker_events(id INTEGER PRIMARY KEY,ts REAL,kind TEXT,data TEXT);""")
        conn.execute("INSERT INTO maker_meta VALUES(1,?)", (json.dumps({"config": asdict(MakerConfig()), "paper_only": True}),))
        rows = [("runtime", {"io_revision": "v6-io-r2"}),
                ("source_error", {"reason": "old_failure"}),
                ("runtime", {"io_revision": "v6-io-r3"})]
        rows += [("book_reject", {"reason": "bbo_delta_mismatch", "event": bad_delta()}) for _ in range(5)]
        meter = ReactionWindow()
        meter.add(1.2, True)
        rows.append(("reaction_window", meter.take()))
        for i, (kind, data) in enumerate(rows, 1):
            conn.execute("INSERT INTO maker_events VALUES(?,?,?,?)", (i, NOW+i, kind, json.dumps(data)))
        # Huge successful-market payloads must not be parsed for small diagnostics.
        conn.execute("INSERT INTO maker_events(ts,kind,data) VALUES(?,?,?)", (NOW+20, "book_event", "NOT_JSON"))


def test_diagnose_latest_session_readonly_and_exclusive_export(tmp_path):
    path = tmp_path/"maker.db"
    make_db(path)
    before = path.read_bytes()
    out = tmp_path/"diag.json.gz"
    report = diagnostic_report(path, out)
    summary = report["summary"]
    assert summary["start_event_id"] == 3 and summary["source_errors_latest_run"] == {}
    assert summary["ledger_orders_all_runs"] == 0
    assert summary["book_rejections_latest_run"] == {"bbo_delta_mismatch": 5}
    assert len(report["reject_examples"]) == 2 and summary["reject_examples_omitted"] == 3
    assert summary["local_reaction_all_evaluations"]["event"]["samples"] == 1
    assert path.read_bytes() == before and out.stat().st_size < 10000
    with pytest.raises(FileExistsError):
        diagnostic_report(path, out)


def test_diagnose_does_not_create_missing_database(tmp_path):
    path = tmp_path/"missing.db"
    with pytest.raises(ValueError):
        diagnostic_report(path)
    assert not path.exists()


def test_diagnose_rejects_legacy_without_mutation(tmp_path):
    path = tmp_path/"legacy.db"
    with sqlite3.connect(path) as conn:
        conn.execute("CREATE TABLE fast_orders(id INTEGER)")
    before = path.read_bytes()
    with pytest.raises(ValueError):
        diagnostic_report(path)
    assert before == path.read_bytes()


def test_runtime_records_rejected_message_before_outer_reconnect_clears_cache():
    from jevymarket.maker import MakerRuntime
    from jevymarket.maker_engine import Market
    emitted = []
    store = SimpleNamespace(emit=lambda k, v: emitted.append((k, v)), load_orders=lambda: [])
    rt = MakerRuntime(MakerConfig(), store)
    m = Market("btc-updown-5m-1800000000", "c", 1800000000, 60, "11", "22")
    c = cache_fixture()
    with pytest.raises(BookGap):
        rt.apply_book_message(m, c, bad_delta(), NOW+.2, 100.2)
    assert len(emitted) == 1
    kind, d = emitted[0]
    assert kind == "book_reject" and d["io_revision"] == IO_REVISION
    assert d["event"] == bad_delta() and d["reason"] == "bbo_delta_mismatch"
    assert d["cache_failure"]["before"]["books"]["11"]["bids"] == [[.4, 10]]
    assert rt.engine.orders == []


def test_runtime_successful_message_stays_a_success_event():
    from jevymarket.maker import MakerRuntime
    from jevymarket.maker_engine import Market
    emitted = []
    store = SimpleNamespace(emit=lambda k, v: emitted.append((k, v)), load_orders=lambda: [])
    rt = MakerRuntime(MakerConfig(), store)
    m = Market("btc-updown-5m-1800000000", "c", 1800000000, 60, "11", "22")
    c = BookCache("c", SPECS)
    rt.apply_book_message(m, c, snapshot(), NOW, 100)
    assert [k for k, _ in emitted] == ["book_event"]
    assert c.books["11"].ready


def qualified_runtime():
    from jevymarket.maker import MakerRuntime
    from jevymarket.maker_engine import Market
    from jevymarket.maker_model import Estimate
    store = SimpleNamespace(emit=lambda k, v: None, load_orders=lambda: [])
    rt = MakerRuntime(MakerConfig(), store)
    rt.market = Market("btc-updown-5m-1800000000", "c", 1800000000, 60, "11", "22")
    rt.cache = BookCache("c", SPECS)
    for token, bid, ask in (("11", .94, .95), ("22", .05, .06)):
        rt.cache.apply(snapshot(token, bids=[{"price": bid, "size": 10}],
                               asks=[{"price": ask, "size": 20}]), NOW, 100)
    rt.clock_ok, rt.clock_ts, rt.metadata_ts = True, NOW, NOW
    est = Estimate(.98, NOW, NOW-.1, NOW-.1, 100, 110, 109, 2, 0)
    rt.reference = SimpleNamespace(estimate=lambda *args: est)
    return rt


def test_observe_only_never_submits_even_qualified_quote():
    rt = qualified_runtime()
    before = asdict(rt.c)
    rt.observe_only = True
    rt.react(NOW, 100)
    assert rt.reason == "observe_only_qualified"
    assert rt.engine.orders == [] and asdict(rt.c) == before
    # Explicit ordinary mode still uses the original entry predicate.
    rt.observe_only = False
    rt.react(NOW, 100)
    assert len(rt.engine.orders) == 1


def test_observe_only_preserves_inflight_risk_until_cancel_ack():
    rt = qualified_runtime()
    rt.react(NOW, 100)
    rt.react(NOW+.11, 100.11)
    assert rt.engine.active().state == "active"
    reserved = rt.engine.exposure()
    rt.observe_only = True
    rt.react(NOW+.12, 100.12)
    assert rt.engine.active().state == "cancel_pending" and rt.engine.exposure() == reserved
    rt.react(NOW+.25, 100.25)
    assert rt.engine.active() is None and len(rt.engine.orders) == 1


def test_full_offline_run_records_no_order_reactions_and_observe_mode(tmp_path):
    import asyncio

    from jevymarket.maker import MakerRuntime
    from jevymarket.maker_store import MakerStore
    path = tmp_path/"observe.db"
    rt = MakerRuntime(MakerConfig(), MakerStore(path, MakerConfig()))

    async def fake_reader(*args):
        while True:
            rt.signal()
            await asyncio.sleep(.01)

    # Every read worker is replaced; no HTTP or WebSocket requests are sent.
    rt.references = rt.discover = rt.clock = rt.settlements = fake_reader
    asyncio.run(rt.run(seconds=.15, observe_only=True))
    report = diagnostic_report(path)
    summary = report["summary"]
    assert summary["runtime"]["observe_only"] is True
    assert summary["local_reaction_all_evaluations"]["event"]["samples"] > 0
    assert summary["ledger_orders_all_runs"] == 0


def test_two_token_bad_event_does_not_commit_either_side():
    c = cache_fixture()
    event = bad_delta()
    event["price_changes"].insert(0, {"asset_id": "22", "side": "BUY", "price": ".45", "size": "9",
                                      "best_bid": ".45", "best_ask": ".6"})
    with pytest.raises(BookGap):
        c.apply(event, NOW+.2, 100.2)
    before = c.last_failure["before"]["books"]
    assert before["11"]["bids"] == before["22"]["bids"] == [[.4, 10]]
    assert not any(b.ready or b.bids or b.asks for b in c.books.values())


def test_diagnostics_distinguish_book_shape_in_health_text():
    from jevymarket.maker_model import ReferenceCache
    from jevymarket.maker_protocol import data_health, health_text
    rt = qualified_runtime()
    # Use the actual reference object for health traversal.
    refs = ReferenceCache(rt.c)
    rt.reference = refs
    rt.cache.apply(snapshot("11", bids=[{"price": ".99", "size": "10"}], asks=[]), NOW, 100)
    h = data_health(rt, NOW, 100)
    assert h["book_details"]["UP"]["shape"] == "bid_only"
    assert "新鲜/仅买盘" in health_text(h)
