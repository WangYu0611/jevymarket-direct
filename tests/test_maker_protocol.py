"""Framing regressions from the first v6 live public-data startup."""
import asyncio
import json
import time
from collections import deque
from types import SimpleNamespace

import pytest

from jevymarket.maker_protocol import (
    clock_retry_seconds,
    clock_sample,
    data_health,
    decode_stream_frame,
    error_reason,
    health_text,
)


@pytest.mark.parametrize("raw", ["", " \r\n\t ", b"", b"\r\n"])
def test_empty_frames_are_not_json_or_quotes(raw):
    assert decode_stream_frame(raw) == ([], "empty", None)


@pytest.mark.parametrize(("raw", "reply"), [
    ("PING", "PONG"), ("ping", "pong"), (" Ping ", "PONG"),
    (b"PING", "PONG"), (b"ping\r\n", "pong"),
])
def test_ping_case_and_bytes(raw, reply):
    assert decode_stream_frame(raw) == ([], "ping", reply)


@pytest.mark.parametrize("raw", ["PONG", "pong", " Pong ", b"PONG", b"pong\n"])
def test_pong_case_and_bytes(raw):
    assert decode_stream_frame(raw) == ([], "pong", None)


@pytest.mark.parametrize("raw", ['{"topic":"x","payload":{}}', b'{"topic":"x","payload":{}}',
                                 '[{"topic":"x","payload":{}}]'])
def test_json_object_bytes_and_batch(raw):
    assert decode_stream_frame(raw) == ([{"topic": "x", "payload": {}}], None, None)


@pytest.mark.parametrize(("raw", "reason"), [
    ("NOT A QUOTE", "non_json_stream_frame"), ('{"broken":', "non_json_stream_frame"),
    (b"\xff", "invalid_stream_encoding"), (None, "invalid_stream_frame_type"),
    ("null", "invalid_stream_message_shape"), ('"pong"', "invalid_stream_message_shape"),
    ("42", "invalid_stream_message_shape"), ('[{},"bad"]', "invalid_stream_message_shape"),
    ('{"error":"never export this body"}', "stream_error_response"),
])
def test_unknown_or_malformed_frames_still_fail_closed(raw, reason):
    with pytest.raises(ValueError) as result:
        decode_stream_frame(raw)
    assert error_reason(result.value) == reason


def test_bookgap_root_cause_not_hidden_or_arbitrary_text_exposed():
    inner = ValueError("bbo_delta_mismatch")
    outer = ValueError("invalid_or_inconsistent_book")
    outer.__cause__ = inner
    assert error_reason(outer) == "bbo_delta_mismatch"
    assert error_reason(ValueError("secret-token-in-URL")) == "unclassified"
    inner.__cause__ = outer  # Robust even if a dependency makes a cause cycle.
    assert error_reason(outer) in {"bbo_delta_mismatch", "invalid_or_inconsistent_book"}


def test_clock_quantisation_does_not_claim_precise_offset():
    sample = clock_sample(1000.0, 1000.7, 1000.9, .2)
    assert sample["acceptable"] and sample["whole_second_clock"]
    assert sample["offset_lower_seconds"] < 0 < sample["offset_upper_seconds"]


@pytest.mark.parametrize(("stamp", "before", "after", "rtt", "reason"), [
    (1000, 1000, 1001.2, 1.2, "clock_rtt_exceeded"),
    (1010, 1000, 1000.2, .2, "clock_offset_exceeded"),
    (990, 1000, 1000.2, .2, "clock_offset_exceeded"),
    (1000, 1000, 1002, .2, "clock_wall_jump"),
    (1000, 1001, 1000, .2, "clock_wall_jump"),
])
def test_clock_failure_modes_keep_guard(stamp, before, after, rtt, reason):
    result = clock_sample(stamp, before, after, rtt)
    assert not result["acceptable"] and result["reason"] == reason


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), None, True, "1000"])
def test_invalid_clock_values(bad):
    with pytest.raises(ValueError):
        clock_sample(bad, 1000, 1000.1, .1)


def test_clock_retry_has_fast_first_retry_and_bounded_backoff():
    assert [clock_retry_seconds(n) for n in range(7)] == [30, 2, 4, 8, 16, 30, 30]


def test_outside_entry_slot_still_exposes_missing_data():
    def missing(*args):
        raise ValueError("missing_anchor_or_reference")
    refs = SimpleNamespace(samples={k: deque() for k in ("raw", "twap30", "twap60")},
                           anchors={}, estimate=missing)
    c = SimpleNamespace(max_reference_age_seconds=5, max_book_age_seconds=1)
    market = SimpleNamespace(start=1000, end=1300, window=60)
    runtime = SimpleNamespace(c=c, reference=refs, market=market, cache=None,
                              clock_ok=True, clock_ts=1000, clock_info={}, metadata_ts=1000)
    h = data_health(runtime, 1001, 1)
    assert not h["ready"] and not h["anchor_present"]
    assert h["reference_reason"] == "missing_anchor_or_reference"
    assert "raw=0条" in health_text(h) and "新鲜盘口=0/2" in health_text(h)


class FakeSocket:
    def __init__(self, frames):
        self.frames, self.sent = deque(frames), []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    async def send(self, value):
        self.sent.append(value)

    async def recv(self):
        await asyncio.sleep(0)
        if not self.frames:
            raise asyncio.CancelledError
        return self.frames.popleft()


def runtime_fixture():
    from jevymarket.maker import MakerRuntime
    from jevymarket.maker_config import MakerConfig
    emitted = []
    store = SimpleNamespace(emit=lambda k, v: emitted.append((k, v)), load_orders=lambda: [])
    return MakerRuntime(MakerConfig(), store), emitted


def test_integration_reference_stream_empty_and_heartbeats_preserve_history(monkeypatch):
    import jevymarket.maker as maker
    import websockets.asyncio.client as client
    runtime, emitted = runtime_fixture()
    now = time.time()
    runtime.reference.add("raw", now - 1, 100, now)
    quote = {"topic": maker.TOPICS["raw"], "type": "update",
             "payload": {"symbol": "btc/usd", "timestamp": now * 1000, "value": 101}}
    ws = FakeSocket(["", b"PONG", "ping", json.dumps(quote), "\n", "pong"])
    monkeypatch.setattr(client, "connect", lambda *a, **k: ws)
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(runtime.references("raw"))
    assert len(runtime.reference.samples["raw"]) == 2
    assert runtime.reference.samples["raw"][-1][1] == 101
    assert not any(k == "source_error" for k, _ in emitted)
    assert "pong" in ws.sent and runtime.stream_controls["raw:empty"] == 2


def test_integration_heartbeats_do_not_refresh_valid_quote_deadline(monkeypatch):
    import jevymarket.maker as maker
    import websockets.asyncio.client as client
    runtime, _ = runtime_fixture()
    clock = [0.0]
    ws = FakeSocket(["PONG"])
    original_recv = ws.recv
    async def recv():
        clock[0] = 21.0
        return await original_recv()
    ws.recv = recv
    monkeypatch.setattr(client, "connect", lambda *a, **k: ws)
    monkeypatch.setattr(maker, "time", SimpleNamespace(time=time.time, monotonic=lambda: clock[0]))
    seen = []
    def stop_on_error(stage, exc):
        seen.append((stage, error_reason(exc)))
        raise asyncio.CancelledError
    runtime.error = stop_on_error
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(runtime.references("raw"))
    assert seen == [("raw", "reference_silent")]


def test_integration_orderbook_controls_do_not_drop_snapshots(monkeypatch):
    import jevymarket.maker as maker
    import websockets.asyncio.client as client
    from jevymarket.maker_book import BookCache
    from jevymarket.maker_engine import Market
    runtime, emitted = runtime_fixture()
    now = time.time()
    m = Market("btc-updown-5m-1800000000", "c", int(now), 60, "11", "22")
    cache = BookCache("c", {"11": (.01, 5), "22": (.01, 5)})
    snapshots = [{"event_type": "book", "market": "c", "asset_id": token, "timestamp": now * 1000,
                  "bids": [{"price": ".4", "size": "10"}], "asks": [{"price": ".6", "size": "10"}]}
                 for token in ("11", "22")]
    ws = FakeSocket(["", " pong ", json.dumps(snapshots), b"ping"])
    monkeypatch.setattr(client, "connect", lambda *a, **k: ws)
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(runtime.books(m, cache))
    assert sum(k == "book_event" for k, _ in emitted) == 2
    assert not any(k == "source_error" for k, _ in emitted)
    assert "pong" in ws.sent


def test_integration_clock_failure_does_not_reuse_previous_pass(monkeypatch):
    import jevymarket.maker as maker
    runtime, emitted = runtime_fixture()
    runtime.clock_ok = True
    async def timestamp(*args, **kwargs):
        return 1010
    monkeypatch.setattr(maker, "public_json", timestamp)
    walls, monos = iter([1000, 1000.2]), iter([10, 10.2, 10.3])
    monkeypatch.setattr(maker, "time", SimpleNamespace(time=lambda: next(walls), monotonic=lambda: next(monos)))
    assert not asyncio.run(runtime.clock_once(None))
    assert not runtime.clock_ok
    assert emitted[-1][1]["reason"] == "clock_offset_exceeded"


def test_integration_time_read_bypasses_caches_without_changing_price_reads():
    from jevymarket.maker import CLOB, public_json
    calls = []
    async def get(url, **kwargs):
        calls.append((url, kwargs))
        return SimpleNamespace(raise_for_status=lambda: None, json=lambda: 1000)
    async def run():
        http = SimpleNamespace(get=get)
        await public_json(http, CLOB + "/time")
        await public_json(http, CLOB + "/book", token_id="11")
    asyncio.run(run())
    assert calls[0][1]["headers"]["Cache-Control"] == "no-cache"
    assert "headers" not in calls[1][1]
