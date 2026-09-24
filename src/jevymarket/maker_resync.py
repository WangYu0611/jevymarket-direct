"""Fail-closed, same-version book recovery. No inferred depth or order fills."""
from __future__ import annotations

import asyncio
import json
import time
from copy import deepcopy

from .maker_book import BookGap
from .maker_ingress import SnapshotBookCache
from .maker_model import finite, source_time

RESYNC_SECONDS = .1  # Bounded engineering timeout, not an exchange latency claim.
RESYNC_MESSAGES = 256
RESYNC_LEVELS = 20000
RESYNC_REASONS = frozenset({
    "resync_timeout", "resync_clock_invalid", "resync_generation_changed",
    "resync_unexpected_message", "resync_version_mismatch", "resync_bbo_mismatch",
    "resync_delta_after_snapshot", "resync_duplicate_snapshot", "resync_limit",
    "resync_stale_snapshot", "resync_invalid_message",
})


def root_reason(exc):
    result, seen = "unclassified", set()
    while exc is not None and id(exc) not in seen:
        seen.add(id(exc))
        if len(exc.args) == 1 and isinstance(exc.args[0], str):
            if exc.args[0] in RESYNC_REASONS or exc.args[0] == "bbo_delta_mismatch":
                result = exc.args[0]
        exc = exc.__cause__ or exc.__context__
    return result


def version(row):
    stamp = row.get("hash")
    if not isinstance(stamp, str) or not 1 <= len(stamp) <= 128:
        raise ValueError("resync_invalid_message")
    bid, ask = finite(row.get("best_bid")), finite(row.get("best_ask"))
    if not 0 < bid < ask < 1:
        raise ValueError("resync_invalid_message")
    return stamp, bid, ask


class SnapshotResync:
    """Quarantine until full snapshots or a closed split-delta batch validate.

    Only a two-token price_change BBO rejection with complete, consistent version
    metadata is eligible. The opaque hash is compared, not recomputed or assumed
    cryptographically verified. A timestamp alone never authorizes recovery.
    """
    def __init__(self, cache, msg, wall, mono, perf_ns, max_age=1.0):
        self.cache = cache
        self.generation = cache.generation
        self.ts = source_time(msg["timestamp"])
        self.started_ns = perf_ns
        self.last_ns = perf_ns
        self.expected = {}
        self.messages = 0
        self.seen = set()
        self.done = False
        self.max_age = finite(max_age)
        if self.max_age <= 0 or not 0 <= wall - self.ts <= 5 or not 0 <= mono:
            raise ValueError("resync_invalid_message")
        if msg.get("event_type") != "price_change" or msg.get("market") != cache.condition:
            raise ValueError("resync_invalid_message")
        if any(b.ready or b.bids or b.asks for b in cache.books.values()):
            raise ValueError("resync_invalid_message")  # caller MUST invalidate first
        rows = msg["price_changes"]
        if not isinstance(rows, list) or not 0 < len(rows) <= RESYNC_MESSAGES:
            raise ValueError("resync_invalid_message")
        for row in rows:
            token = str(row["asset_id"])
            if token not in cache.books or row.get("side") not in {"BUY", "SELL"}:
                raise ValueError("resync_invalid_message")
            cache._level(row["price"], row["size"])
            item = version(row)
            if token in self.expected and self.expected[token] != item:
                raise ValueError("resync_version_mismatch")
            self.expected[token] = item
        if len(cache.books) != 2 or set(self.expected) != set(cache.books):
            raise ValueError("resync_invalid_message")
        self.specs = {t: (b.tick, b.minimum) for t, b in cache.books.items()}
        self.staged = SnapshotBookCache(cache.condition, self.specs)
        self.before = deepcopy(cache.last_failure["before"])
        self.deltas = deepcopy(rows)
        self.last_delta_wall, self.last_delta_mono = wall, mono
        self.method = None

    def abort(self, reason):
        self.cache.invalidate()
        self.staged.invalidate()
        self.done = True
        raise BookGap(reason)

    def remaining(self, perf_ns):
        if self.done or self.cache.generation != self.generation:
            self.abort("resync_generation_changed")
        if perf_ns < self.last_ns:
            self.abort("resync_clock_invalid")
        self.last_ns = perf_ns
        left = RESYNC_SECONDS - (perf_ns - self.started_ns) / 1e9
        if left <= 0:
            self.abort("resync_timeout")
        return left

    def feed(self, msg, wall, mono, perf_ns):
        """Return True for snapshots, "reprocess" for a closed validated delta batch."""
        self.remaining(perf_ns)
        try:
            self.messages += 1
            if self.messages > RESYNC_MESSAGES:
                self.abort("resync_limit")
            if not isinstance(msg, dict) or msg.get("market") != self.cache.condition:
                self.abort("resync_unexpected_message")
            ts = source_time(msg.get("timestamp"))
            if not 0 <= wall - ts <= 5 or not 0 <= wall - self.ts <= 5:
                self.abort("resync_stale_snapshot")
            kind = msg.get("event_type")
            if kind != "price_change" and ts != self.ts:
                self.abort("resync_version_mismatch")
            if kind == "price_change":
                rows = msg.get("price_changes")
                if not isinstance(rows, list) or not 0 < len(rows) <= RESYNC_MESSAGES:
                    self.abort("resync_invalid_message")
                incoming = {}
                for row in rows:
                    token = str(row["asset_id"])
                    if token not in self.expected or row.get("side") not in {"BUY", "SELL"}:
                        self.abort("resync_invalid_message")
                    item = version(row)
                    if token in incoming and item != incoming[token]:
                        self.abort("resync_version_mismatch")
                    incoming[token] = item
                    self.cache._level(row["price"], row["size"])
                same = ts == self.ts and all(v == self.expected[t] for t, v in incoming.items())
                if not same:
                    if ts == self.ts and any(v[0] == self.expected[t][0] and v != self.expected[t]
                                             for t, v in incoming.items()):
                        self.abort("resync_version_mismatch")
                    if ts < self.ts or set(incoming) != set(self.expected) or self.seen:
                        self.abort("resync_version_mismatch")
                    # A next version closes the previous split update. Rebuild
                    # ONLY explicit rows, then run the UNCHANGED batch validator.
                    batch = SnapshotBookCache(self.cache.condition, self.specs)
                    for token, data in self.before["books"].items():
                        b = batch.books[token]
                        b.bids, b.asks = dict(data["bids"]), dict(data["asks"])
                        b.ts, b.received_mono = data["ts"], data["received_mono"]
                        b.ready, b.ever_snapshot = data["ready"], data["ready"]
                    batch.apply({"event_type": "price_change", "market": self.cache.condition,
                        "timestamp": self.ts, "price_changes": self.deltas},
                        self.last_delta_wall, self.last_delta_mono)
                    self.staged = batch
                    self.method = "validated_split_delta"
                    self._publish(wall, mono)
                    return "reprocess"  # boundary message is NOT consumed/lost
                if any(t in self.seen for t in incoming):
                    self.abort("resync_delta_after_snapshot")
                if len(self.deltas) + len(rows) > RESYNC_MESSAGES:
                    self.abort("resync_limit")
                self.deltas.extend(deepcopy(rows))
                self.last_delta_wall, self.last_delta_mono = wall, mono
                return False
            if kind != "book":
                self.abort("resync_unexpected_message")
            token = str(msg.get("asset_id"))
            if token not in self.expected or msg.get("hash") != self.expected[token][0]:
                self.abort("resync_version_mismatch")
            if token in self.seen:
                self.abort("resync_duplicate_snapshot")
            for key in ("bids", "asks"):
                levels = msg.get(key)
                if not isinstance(levels, list) or len(levels) > RESYNC_LEVELS:
                    self.abort("resync_limit")
                # Duplicate price levels make a full snapshot ambiguous.
                prices = [finite(row["price"]) for row in levels]
                if len(prices) != len(set(prices)):
                    self.abort("resync_invalid_message")
            self.staged.apply(msg, wall, mono)  # strict levels, age, order, spread
            b = self.staged.books[token]
            if (b.bid, b.ask) != self.expected[token][1:]:
                self.abort("resync_bbo_mismatch")
            self.seen.add(token)
            if self.seen != set(self.expected):
                return False
            self.method = "matching_full_snapshots"
            self._publish(wall, mono)
            return True
        except (KeyError, TypeError, ValueError) as exc:
            if not self.done:
                self.cache.invalidate()
                self.staged.invalidate()
                self.done = True
            if isinstance(exc, BookGap):
                raise
            raise BookGap("resync_invalid_message") from None

    def _publish(self, wall, mono):
        if not all(b.fresh(wall, mono, 5.0) for b in self.staged.books.values()):
            self.abort("resync_stale_snapshot")
        # Preserve actual receipt times; never refresh data on a heartbeat or
        # boundary message. Strategy's independent 1s quote-age test is unchanged.
        for token, source in self.staged.books.items():
            target = self.cache.books[token]
            target.bids, target.asks = source.bids, source.asks
            target.ts, target.received_mono = source.ts, source.received_mono
            target.ready, target.ever_snapshot = True, True
        self.cache.snapshot_baselines = self.staged.snapshot_baselines.copy()
        self.cache.last_discard = None
        self.done = True


async def read_resync_books(runtime, market, cache, ws, ping, safe_event, revision, *, clock=time):
    """Same connection; immediate risk protection; bounded recovery or reconnect."""
    pending = None

    def record(state, **data):
        runtime.store.emit("book_resync", {"state": state, "io_revision": revision,
            "session_id": runtime.session_id, "slug": market.slug, **data})

    try:
        while clock.time() < market.end:
            if ping.done():
                ping.result()
            timeout = min(20.0, max(.001, market.end - clock.time()))
            if pending is not None:
                timeout = min(timeout, pending.remaining(clock.perf_counter_ns()))
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout)
            except TimeoutError:
                if pending is not None:
                    pending.remaining(clock.perf_counter_ns())
                    continue  # early timer wake is not proof of deadline expiry
                raise
            for msg in await runtime.stream_messages(ws, raw, "orderbook"):
                wall, mono, ns = clock.time(), clock.monotonic(), clock.perf_counter_ns()
                if wall >= market.end:
                    return
                if pending is not None:
                    runtime.store.emit("book_resync_input", {"session_id": runtime.session_id,
                        "slug": market.slug, "received_ts": wall, "received_mono": mono,
                        "event": safe_event(msg)})
                    outcome = pending.feed(msg, wall, mono, ns)
                    if outcome:
                        if clock.perf_counter_ns() - pending.started_ns >= RESYNC_SECONDS * 1e9:
                            pending.abort("resync_timeout")
                        record("recovered", method=pending.method, elapsed_ms=(ns - pending.started_ns) / 1e6,
                               messages=pending.messages, source_ts=pending.ts,
                               source_age_seconds=wall - pending.ts, generation=cache.generation,
                               quote_fresh=all(b.fresh(wall, mono, runtime.c.max_book_age_seconds)
                                               for b in cache.books.values()))
                        pending = None
                        runtime.signal()
                    if outcome != "reprocess":
                        continue
                try:
                    runtime.apply_book_message(market, cache, msg, wall, mono)
                except BookGap as exc:
                    if root_reason(exc) != "bbo_delta_mismatch":
                        raise
                    # The failed event was already saved by apply_book_message.
                    # Keep old order risk UNKNOWN even after data recovery.
                    runtime.protect_book_failure(market, cache)
                    try:
                        pending = SnapshotResync(cache, msg, wall, mono, ns, runtime.c.max_book_age_seconds)
                    except (KeyError, TypeError, ValueError):
                        record("ineligible")
                        raise exc from exc.__cause__
                    record("started", source_ts=pending.ts, generation=cache.generation)
    finally:
        if pending is not None:
            # Includes cancellation, connection loss, parse errors and boundary.
            cache.invalidate()
            record("aborted", elapsed_ms=max(0, clock.perf_counter_ns() - pending.started_ns) / 1e6)


def resync_summary(conn, start_id):
    """SELECT only, within the caller's existing read-only diagnostic snapshot."""
    counts = {"started": 0, "recovered": 0, "aborted": 0, "ineligible": 0}
    methods, max_ms, stale = {}, None, 0
    for (payload,) in conn.execute(
        "SELECT data FROM maker_events WHERE id>=? AND kind='book_resync' ORDER BY id", (start_id,)
    ):
        row = json.loads(payload)
        state = row.get("state")
        if state in counts:
            counts[state] += 1
        if state == "recovered":
            method = row.get("method", "unknown")
            methods[method] = methods.get(method, 0) + 1
            elapsed = finite(row["elapsed_ms"])
            max_ms = elapsed if max_ms is None else max(max_ms, elapsed)
            stale += row.get("quote_fresh") is False
    return dict(counts, recovery_methods=methods, max_recovery_wait_ms=max_ms,
                recovered_but_quote_stale=stale,
                scope="Local quarantine/recovery, not exchange acknowledgement or profitability")
