"""Offline-only BBO evidence contracts. No test connects to public market feeds."""
import asyncio
import gzip
import hashlib
import inspect
import json
from copy import deepcopy
from types import SimpleNamespace

import pytest

from jevymarket import maker_bbo_evidence as evidence
from jevymarket import maker_bbo_probe as probe
from jevymarket.maker_bbo_evidence import BBOEvidence, encode, inspect_frame

NOW = 1_800_000_000.0
MONO = 1_000_000_000_000
SPECS = {"11": (.01, 5), "22": (.01, 5)}


@pytest.fixture
def collector_clock(monkeypatch):
    """Control collector retries, not pytest/asyncio's process-global clocks.

    These orchestration tests mock discovery/read_connection. Immediate wait_for
    models that completed I/O; the real reader has its own transport test below.
    """
    class Clock:
        elapsed_ns = 0
        early_first_wake = False

        def __init__(self):
            self.sleeps = []

        def advance(self, seconds):
            self.elapsed_ns += round(seconds * 1e9)

        def monotonic(self):
            return self.elapsed_ns / 1e9

        def time(self):
            return NOW + self.monotonic()

        def perf_counter_ns(self):
            return MONO + self.elapsed_ns

        async def sleep(self, delay):
            # Reproduce an early timer wake explicitly rather than relying on OS
            # resolution. Only the first wake is early; the next reaches expiry.
            elapsed = round(delay * 1e9)
            if self.early_first_wake and not self.sleeps:
                elapsed = max(0, elapsed - 1_000_000)
            self.sleeps.append(delay)
            self.elapsed_ns += elapsed
            await asyncio.sleep(0)

    async def immediate(awaitable, timeout):
        assert timeout > 0
        return await awaitable

    clock = Clock()
    # Replace only the probe's module references, not shared stdlib attributes.
    monkeypatch.setattr(probe, "time", clock)
    monkeypatch.setattr(probe, "asyncio", SimpleNamespace(
        wait_for=immediate, sleep=clock.sleep, CancelledError=asyncio.CancelledError))
    return clock


def book(token="11", bid=".72", ask=".73", ts=NOW):
    return {"event_type": "book", "market": "c", "asset_id": token, "timestamp": ts * 1000,
            "bids": [{"price": bid, "size": "10"}], "asks": [{"price": ask, "size": "32"}]}


def mismatch(ts=NOW + .1):
    return {"event_type": "price_change", "market": "c", "timestamp": ts * 1000,
            "price_changes": [{"asset_id": "11", "price": ".69", "size": "155.16", "side": "BUY",
                               "best_bid": ".69", "best_ask": ".73"}]}


def seeded():
    c = BBOEvidence("c", SPECS, "test-session")
    c.feed(encode([book(), book("22", ".27", ".28")]), NOW, MONO)
    return c


def trigger(c, later=None):
    messages = [mismatch()]
    if later is not None:
        messages.append(later)
    return c.feed(encode(messages), NOW + .2, MONO + 200_000_000)


def test_frame_tail_survives_rejection_and_cache_stays_invalid():
    c = seeded()
    later = book(bid=".69", ts=NOW + .1)
    assert trigger(c, later) == "capture"
    assert c.episode["trigger_message_index"] == 0
    assert c.episode["trigger_frame"]["messages"] == [mismatch(), later]
    assert not any(b.ready for b in c.cache.books.values())
    c.feed(encode(book("22", ".31", ".32", NOW + .2)), NOW + .3, MONO + 300_000_000)
    assert not any(b.ready for b in c.cache.books.values())  # NO tail-based recovery.
    result = c.finish("tail_complete", MONO + 2_200_000_000)
    assert result["post_window_complete"]
    assert result["following"]["first_later_book"]["11"]["frame_id"] == 2
    assert result["following"]["first_later_book"]["11"]["message_index"] == 1
    assert result["following"]["first_later_book"]["22"]["frame_id"] == 3


def test_rejection_state_is_precise_and_not_changed_by_tail():
    c = seeded()
    trigger(c)
    failure = deepcopy(c.episode["failure"])
    assert failure["before"]["books"]["11"]["bids"] == [[.72, 10.0]]
    assert [.69, 155.16] in failure["candidate"]["books"]["11"]["bids"]
    c.feed(encode(book(bid=".69", ts=NOW + .3)), NOW + .4, MONO + 400_000_000)
    assert c.episode["failure"] == failure


def test_hash_order_and_array_envelope_preserved():
    raw = encode([book(), mismatch()])
    f, messages = inspect_frame(raw, 7, NOW, MONO)
    assert f["wire_sha256"] == hashlib.sha256(raw).hexdigest()
    assert f["wire_bytes"] == len(raw) and f["array_envelope"]
    assert f["messages"] == messages and f["message_count"] == 2
    assert inspect_frame(encode(book()), 8, NOW, MONO)[0]["array_envelope"] is False


def test_custom_bbo_and_trades_are_retained_not_converted_to_fills():
    c = seeded()
    trigger(c)
    msgs = [{"event_type": "best_bid_ask", "market": "c", "asset_id": "11", "timestamp": NOW * 1000,
             "best_bid": ".69", "best_ask": ".73", "spread": ".04"},
            {"event_type": "last_trade_price", "market": "c", "asset_id": "11", "timestamp": NOW * 1000,
             "price": ".72", "size": "10", "side": "SELL"}]
    c.feed(encode(msgs), NOW + .4, MONO + 400_000_000)
    result = c.finish("tail_complete", MONO + 3_000_000_000)
    assert result["following"]["first_later_bbo"]["11"]["best_bid"] == ".69"
    assert len(result["following"]["first_trades"]) == 1
    assert result["trading_enabled"] is False


@pytest.mark.parametrize("raw", [b"\xff", b"not json", b"[1]", b"null", b'{"error":"SECRET"}',
                                 b'[{"event_type":"book","bids":null}]'])
def test_bad_frames_fail_explicitly_without_raw_error_body(raw):
    frame, _ = inspect_frame(raw, 1, NOW, MONO)
    assert frame.get("parse_error")
    assert b"SECRET" not in encode(frame)


def test_private_and_unknown_fields_never_leave_public_projection():
    m = book()
    m.update(authorization="SECRET", api_key="SECRET", description="SECRET")
    m["bids"][0]["secret"] = "SECRET"
    frame, _ = inspect_frame(encode(m), 1, NOW, MONO)
    assert b"SECRET" not in encode(frame)
    assert frame["messages"][0] == book()


@pytest.mark.parametrize("value", [float("nan"), float("inf"), True, None])
def test_nonfinite_or_invalid_scalars_cannot_be_claimed_replayable(value):
    raw = json.dumps(dict(book(), price=value))
    frame, _ = inspect_frame(raw, 1, NOW, MONO)
    assert frame["parse_error"] == "lossy_protocol_fields"
    encode(frame)  # Strict JSON serialization remains possible.


def test_oversized_frame_is_bounded_and_marked():
    frame, msgs = inspect_frame(b"x" * (evidence.FRAME_BYTES + 1), 1, NOW, MONO)
    assert frame["parse_error"] == "frame_size_limit" and not msgs
    assert len(encode(frame)) < 1000


def test_pre_ring_has_explicit_omission_count():
    c = seeded()
    for i in range(50):
        c.feed("PONG", NOW, MONO + i)
    trigger(c)
    assert len(c.episode["pre_frames"]) == 32
    assert c.episode["pre_frames_omitted"] == 19


def test_episode_size_limit_never_claims_complete_tail(monkeypatch):
    c = seeded()
    trigger(c)
    monkeypatch.setattr(evidence, "EPISODE_BYTES", c.episode_bytes + 1)
    assert c.feed("PONG", NOW + .5, MONO + 500_000_000) == "stop"
    r = c.finish("episode_size_limit", MONO + 3_000_000_000)
    assert not r["post_window_complete"] and r["omitted_tail_frames"] == 1


@pytest.mark.parametrize("reason", ["transport_ConnectionClosed", "market_boundary", "run_deadline", "interrupted"])
def test_incomplete_tail_has_honest_termination(reason):
    c = seeded()
    trigger(c)
    assert not c.finish(reason, MONO + 3_000_000_000)["post_window_complete"]


def test_elapsed_close_time_cannot_make_short_tail_complete():
    c = seeded()
    trigger(c)
    assert not c.finish("tail_complete", MONO + 500_000_000)["post_window_complete"]


def test_startup_old_delta_is_discarded_not_bbo_incident():
    c = seeded()
    old = mismatch(NOW - .001)
    assert c.feed(encode(old), NOW + .1, MONO + 100_000_000) == "observe"
    assert c.episode is None and c.counts["discard"] == 1
    assert c.cache.books["11"].bid == .72


def test_other_rejections_are_not_counted_as_bbo():
    c = BBOEvidence("c", SPECS, "none")
    assert trigger(c) == "stop"
    assert c.episode is None
    assert c.stop_reason == "delta_without_snapshot_or_out_of_order"


def test_other_market_message_is_not_misattributed():
    c = seeded()
    m = mismatch()
    m["market"] = "other"
    c.feed(encode(m), NOW + .2, MONO + 200_000_000)
    assert c.episode is None


@pytest.mark.asyncio
async def test_reader_keeps_receiving_after_bbo_on_same_socket(monkeypatch, collector_clock):
    # Exercise real tasks/wait_for, but do not race three frames against 5ms.
    monkeypatch.setattr(probe, "asyncio", asyncio)
    c = BBOEvidence("c", SPECS, "ws-test")
    now = collector_clock.time()
    messages = [encode([book(ts=now), book("22", ".27", ".28", now)]),
                encode([mismatch(now)]), encode(book(bid=".69", ts=now))]
    calls, sent = [], []

    async def recv():
        calls.append(1)
        if messages:
            return messages.pop(0)
        collector_clock.advance(2)
        return "PONG"

    async def send(data):
        sent.append(data)

    ws = SimpleNamespace(recv=recv, send=send)
    reason = await probe.read_connection(ws, c, now + 300, collector_clock.monotonic() + 30)
    assert reason == "tail_complete" and len(calls) >= 3
    assert c.episode["post_frames"][0]["messages"][0]["event_type"] == "book"
    assert not any(b.ready for b in c.cache.books.values())
    assert all(s == "PING" or json.loads(s)["type"] == "market" for s in sent)


@pytest.mark.asyncio
@pytest.mark.parametrize(("seconds", "early_wake", "expected", "termination"), [
    pytest.param(.05, False, 1, "run_deadline", id="deadline-after-first-failure"),
    pytest.param(.05, True, 2, "run_deadline", id="early-wake-allows-retry"),
    pytest.param(10, False, 3, "incident_limit_with_partial_tails", id="partial-incident-cap"),
])
async def test_network_error_preserves_partial_incident_before_close(
    monkeypatch, collector_clock, seconds, early_wake, expected, termination,
):
    collector_clock.early_first_wake = early_wake
    market = {"condition": "c", "specs": SPECS, "end": collector_clock.time() + 300}
    captures, closed = [], []

    async def discovery(*args):
        return market

    async def reader(ws, capture, *args):
        wall, ns = collector_clock.time(), collector_clock.perf_counter_ns()
        capture.feed(encode([book(ts=wall), book("22", ts=wall)]), wall, ns)
        capture.feed(encode(mismatch(wall)), wall, ns)
        captures.append(capture)
        raise ConnectionError("SECRET")

    class Context:
        async def __aenter__(self):
            return object()

        async def __aexit__(self, *args):
            # Every retry must finish its own incident BEFORE websocket teardown.
            if captures:
                episode = captures[-1].episode
                assert episode["end_reason"] == "transport_ConnectionError"
                assert episode["post_window_complete"] is False
                assert episode["post_seconds_observed"] == 0
                closed.append(episode["session_id"])

    monkeypatch.setattr(probe, "discover", discovery)
    monkeypatch.setattr(probe, "read_connection", reader)
    r = await probe.collect(seconds, 1, connector=lambda *a, **kw: Context(), http_factory=lambda **kw: Context())
    assert len(r["incidents"]) == len(r["sessions"]) == len(captures) == expected
    assert len({x["session_id"] for x in r["incidents"]}) == expected
    assert {x["session_id"] for x in r["incidents"]} <= set(closed)
    assert all(x["end_reason"] == "transport_ConnectionError" for x in r["incidents"])
    assert all(x["post_window_complete"] is False for x in r["incidents"])
    assert all(s["end_reason"] == "transport_ConnectionError" for s in r["sessions"])
    assert all(not b.ready for c in captures for b in c.cache.books.values())
    assert r["summary"]["complete_post_windows"] == 0
    assert r["termination"] == termination
    assert r["summary"]["root_cause_verified"] is False
    assert r["summary"]["profit_experiment_enabled"] is False
    assert b"SECRET" not in encode(r)


@pytest.mark.asyncio
async def test_public_get_rejects_order_endpoints_before_http():
    class HTTP:
        async def get(self, *args, **kwargs):
            pytest.fail("must not send request")
    with pytest.raises(ValueError):
        await probe.public_get(HTTP(), probe.CLOB + "/order")


def test_output_existing_file_prevents_network(tmp_path, monkeypatch):
    out = tmp_path / "exists.json.gz"
    out.write_bytes(b"old")
    monkeypatch.setattr(probe, "collect", lambda *a: pytest.fail("must not collect"))
    with pytest.raises(FileExistsError):
        probe.main(["--observe-only", "--out", str(out)])
    assert out.read_bytes() == b"old"


def test_output_roundtrip_no_order_engine_or_database(tmp_path, monkeypatch):
    async def fake(*args):
        return {"format": evidence.PROBE_REVISION, "summary": {"incidents": 0}, "trading_enabled": False}
    monkeypatch.setattr(probe, "collect", fake)
    out = tmp_path / "probe.json.gz"
    probe.main(["--observe-only", "--out", str(out)])
    with gzip.open(out, "rt") as f:
        r = json.load(f)
    assert not r["trading_enabled"]
    for module in (probe, evidence):
        source = inspect.getsource(module)
        assert "import sqlite3" not in source and "maker_engine import" not in source
        assert "maker_store import" not in source and "load_settings" not in source


@pytest.mark.parametrize("args", [[], ["--observe-only", "--seconds", "0"],
                                  ["--observe-only", "--seconds", "181"],
                                  ["--observe-only", "--episodes", "4"]])
def test_cli_limits_before_network(args):
    with pytest.raises(SystemExit):
        probe.main(args)


@pytest.mark.parametrize("price", ["NaN", "Infinity", "not-a-number", "1.1"])
def test_bad_snapshot_in_tail_cannot_poison_export(price):
    c = seeded()
    trigger(c, book(bid=price))
    result = c.finish("tail_complete", MONO + 3_000_000_000)
    assert result["following"]["first_later_book"] == {}
    encode(result)


@pytest.mark.asyncio
async def test_discovery_reads_only_metadata_and_never_seeds_http_depth(monkeypatch):
    condition = "0x" + "a" * 64
    calls = []

    async def get(http, url, **params):
        calls.append((url, params))
        if url.startswith(probe.GAMMA):
            return {"slug": "btc-updown-5m-1800000000", "active": True, "closed": False,
                    "acceptingOrders": True, "outcomes": '["Up","Down"]', "clobTokenIds": '["11","22"]',
                    "conditionId": condition, "endDate": "2027-01-15T08:05:00+00:00"}
        return {"asset_id": params["token_id"], "market": condition, "tick_size": ".01", "min_order_size": "5",
                "bids": [{"price": ".5", "size": "999"}], "asks": []}

    monkeypatch.setattr(probe, "public_get", get)
    m = await probe.discover(None, NOW)
    assert m["tokens"] == {"UP": "11", "DOWN": "22"} and len(calls) == 3
    assert "bids" not in m and m["specs"] == SPECS
    c = BBOEvidence(m["condition"], m["specs"], "discovery")
    assert not any(b.ready or b.bids for b in c.cache.books.values())


@pytest.mark.asyncio
async def test_collect_auto_stops_at_target_and_excludes_close_wait(monkeypatch, collector_clock):
    market = {"condition": "c", "specs": SPECS, "end": collector_clock.time() + 300}
    captures = []

    async def discovery(*args):
        return market

    async def reader(ws, capture, *args):
        now, ns = collector_clock.time(), collector_clock.perf_counter_ns()
        capture.feed(encode([book(ts=now), book("22", ts=now)]), now, ns)
        capture.feed(encode(mismatch(now)), now, ns)
        collector_clock.advance(2)  # Preserve the actual two-second tail contract.
        captures.append(capture)
        return "tail_complete"

    class Context:
        async def __aenter__(self):
            return object()

        async def __aexit__(self, *args):
            if captures:
                assert captures[-1].episode["post_window_complete"]
                assert captures[-1].episode["post_seconds_observed"] == 2
                collector_clock.advance(10)  # Closing is not part of tail observation.

    monkeypatch.setattr(probe, "discover", discovery)
    monkeypatch.setattr(probe, "read_connection", reader)
    r = await probe.collect(30, 1, connector=lambda *a, **kw: Context(), http_factory=lambda **kw: Context())
    assert r["termination"] == "target_complete"
    assert len(r["sessions"]) == len(r["incidents"]) == 1
    assert r["incidents"][0]["post_seconds_observed"] == 2
    assert r["sessions"][0]["end_reason"] == "tail_complete"
    assert collector_clock.sleeps == []  # No extra connection after reaching target.
    assert r["summary"]["root_cause_verified"] is False


@pytest.mark.asyncio
async def test_zero_incident_report_does_not_claim_repair(monkeypatch, collector_clock):
    async def broken(*args):
        raise ConnectionError("PRIVATE_PROXY_BODY")

    class Context:
        async def __aenter__(self):
            return object()

        async def __aexit__(self, *args):
            pass

    monkeypatch.setattr(probe, "discover", broken)
    r = await probe.collect(.01, 1, http_factory=lambda **kw: Context())
    assert r["summary"]["complete_post_windows"] == 0
    assert r["summary"]["root_cause_verified"] is False
    assert r["incidents"] == [] and len(r["sessions"]) == 1
    assert r["termination"] == "run_deadline"
    assert b"PRIVATE_PROXY_BODY" not in encode(r)
    assert r["sessions"][0]["end_reason"] == "setup_ConnectionError"


@pytest.mark.asyncio
async def test_collect_retries_partial_then_stops_at_complete_target(monkeypatch, collector_clock):
    market = {"condition": "c", "specs": SPECS, "end": collector_clock.time() + 300}
    captures = []

    async def discovery(*args):
        return market

    async def reader(ws, capture, *args):
        wall, ns = collector_clock.time(), collector_clock.perf_counter_ns()
        capture.feed(encode([book(ts=wall), book("22", ts=wall)]), wall, ns)
        capture.feed(encode(mismatch(wall)), wall, ns)
        captures.append(capture)
        if len(captures) == 1:
            raise ConnectionError("PRIVATE_BODY")
        collector_clock.advance(2)
        return "tail_complete"

    class Context:
        async def __aenter__(self):
            return object()

        async def __aexit__(self, *args):
            if captures:
                episode = captures[-1].episode
                complete = len(captures) == 2
                assert episode["post_window_complete"] is complete
                assert episode["end_reason"] == ("tail_complete" if complete else "transport_ConnectionError")

    monkeypatch.setattr(probe, "discover", discovery)
    monkeypatch.setattr(probe, "read_connection", reader)
    r = await probe.collect(30, 1, connector=lambda *a, **kw: Context(), http_factory=lambda **kw: Context())
    assert len(r["sessions"]) == len(r["incidents"]) == len(captures) == 2
    assert [x["post_window_complete"] for x in r["incidents"]] == [False, True]
    assert [x["post_seconds_observed"] for x in r["incidents"]] == [0, 2]
    assert [x["end_reason"] for x in r["sessions"]] == ["transport_ConnectionError", "tail_complete"]
    assert r["summary"]["complete_post_windows"] == 1
    assert r["termination"] == "target_complete" and collector_clock.sleeps == [1.0]
    assert r["summary"]["root_cause_verified"] is False
    assert r["summary"]["profit_experiment_enabled"] is False
    assert b"PRIVATE_BODY" not in encode(r)
