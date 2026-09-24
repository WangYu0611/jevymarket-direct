"""Bounded public-frame evidence. No strategy, order, database or network access."""
from __future__ import annotations

import hashlib
import json
import math
from collections import Counter, deque

from .maker_ingress import SnapshotBookCache
from .maker_model import finite

PROBE_REVISION = "v6-bbo-probe-r1"
FRAME_BYTES = 1_048_576
PRE_BYTES = 1_048_576
EPISODE_BYTES = 6_291_456
TOP_KEYS = ("event_type", "market", "asset_id", "timestamp", "price", "size", "side", "hash",
            "transaction_hash", "fee_rate_bps", "old_tick_size", "new_tick_size", "best_bid", "best_ask", "spread")
ROW_KEYS = ("asset_id", "price", "size", "side", "hash", "best_bid", "best_ask")
REASONS = frozenset({"invalid_book_shape", "missing_numeric_value", "nonfinite_numeric_value",
    "invalid_source_time", "invalid_level", "stale_or_future_book_message", "out_of_order_snapshot",
    "delta_without_snapshot_or_out_of_order", "unknown_book_side", "bbo_delta_mismatch",
    "invalid_tick", "tick_changed_resubscribe", "crossed_book", "invalid_or_inconsistent_book"})


def encode(value):
    return json.dumps(value, ensure_ascii=True, allow_nan=False, separators=(",", ":")).encode("utf-8")


def reason_code(exc):
    code, seen = "unclassified", set()
    while exc is not None and id(exc) not in seen:
        seen.add(id(exc))
        if len(exc.args) == 1 and isinstance(exc.args[0], str) and exc.args[0] in REASONS:
            code = exc.args[0]
        exc = exc.__cause__ or exc.__context__
    return code


def project_message(msg):
    """Public protocol fields only; explicit markers for lossy relevant fields."""
    changes = set()

    def scalar(value):
        if isinstance(value, str):
            if len(value) > 512:
                changes.add("string_truncated")
            return value[:512]
        if type(value) in (float, int) and math.isfinite(value):
            return value
        changes.add("invalid_scalar")
        return None

    out = {k: scalar(msg[k]) for k in TOP_KEYS if k in msg}
    for key in ("bids", "asks", "price_changes"):
        if key not in msg:
            continue
        rows = msg[key]
        if not isinstance(rows, list):
            out[key] = None
            changes.add("invalid_array")
            continue
        if len(rows) > 20000:
            changes.add("array_truncated")
        out[key] = []
        for row in rows[:20000]:
            if not isinstance(row, dict):
                out[key].append(None)
                changes.add("invalid_row")
            else:
                out[key].append({k: scalar(row[k]) for k in ROW_KEYS if k in row})
    if changes:
        out["projection_warnings"] = sorted(changes)
    return out


def inspect_frame(raw, frame_id, wall, mono_ns):
    """One ws.recv payload is one frame record; array order and indexes survive.

    This preserves application-message boundaries, not TCP packets or WebSocket
    fragmentation. Wire bytes are hashed, never copied as arbitrary text.
    """
    wire = raw if isinstance(raw, bytes) else raw.encode("utf-8")
    frame = {"frame_id": frame_id, "received_wall": wall, "received_perf_ns": mono_ns,
             "wire_bytes": len(wire), "wire_sha256": hashlib.sha256(wire).hexdigest(), "messages": []}
    if len(wire) > FRAME_BYTES:
        frame["parse_error"] = "frame_size_limit"
        return frame, []
    try:
        text = wire.decode("utf-8").strip()
        if not text or text.lower() in {"ping", "pong"}:
            frame["control"] = text.lower() or "empty"
            return frame, []
        value = json.loads(text)
        frame["array_envelope"] = isinstance(value, list)
        messages = value if isinstance(value, list) else [value]
        frame["message_count"] = len(messages)
        if len(messages) > 1024 or any(not isinstance(m, dict) for m in messages):
            frame["parse_error"] = "invalid_or_oversized_message_array"
            return frame, []
        frame["messages"] = [project_message(m) for m in messages]
        if any("projection_warnings" in m for m in frame["messages"]):
            frame["parse_error"] = "lossy_protocol_fields"
        if any("error" in m or m.get("type") == "error" or m.get("event_type") == "error" for m in messages):
            frame["parse_error"] = "stream_error_response"
        return frame, messages
    except (UnicodeError, ValueError, TypeError, RecursionError, OverflowError):
        frame["parse_error"] = "invalid_json_or_encoding"
        return frame, []


class BBOEvidence:
    """Strict trigger detector, then RAW-PROJECTION capture only until tail ends.

    After a BBO mismatch the cache remains invalid; later messages are retained
    without being applied to any book. No recovery or trade permission is implied.
    """
    def __init__(self, condition, specs, session_id, post_seconds=2.0):
        self.cache = SnapshotBookCache(condition, specs)
        self.session_id = session_id
        self.post_seconds = post_seconds
        self.counts = Counter()
        self.pre = deque()
        self.pre_bytes = 0
        self.pre_omitted = 0
        self.frame_id = 0
        self.episode = None
        self.episode_bytes = 0
        self.trigger_ns = None
        self.last_frame = None
        self.stop_reason = None

    def _remember(self, frame):
        size = len(encode(frame))
        self.pre.append((frame, size))
        self.pre_bytes += size
        while len(self.pre) > 32 or self.pre_bytes > PRE_BYTES:
            _, removed = self.pre.popleft()
            self.pre_bytes -= removed
            self.pre_omitted += 1

    def feed(self, raw, wall, mono_ns):
        self.frame_id += 1
        frame, messages = inspect_frame(raw, self.frame_id, wall, mono_ns)
        self.last_frame = frame
        self.counts["frames"] += 1
        self.counts["messages"] += frame.get("message_count", 0)
        if self.episode is not None:
            size = len(encode(frame))
            if self.episode_bytes + size > EPISODE_BYTES:
                self.stop_reason = "episode_size_limit"
                self.episode["omitted_tail_frames"] += 1
                return "stop"
            self.episode["post_frames"].append(frame)
            self.episode_bytes += size
            if frame.get("parse_error"):
                self.stop_reason = frame["parse_error"]
                return "stop"
            return "capture"
        if frame.get("parse_error"):
            self.stop_reason = frame["parse_error"]
            return "stop"
        if frame.get("control"):
            self.counts["control:" + frame["control"]] += 1
        for index, msg in enumerate(messages):
            try:
                self.cache.apply(msg, wall, mono_ns / 1e9)
            except (ValueError, KeyError, TypeError) as exc:
                reason = reason_code(exc)
                self.counts["reject:" + reason] += 1
                if reason != "bbo_delta_mismatch":
                    self.stop_reason = reason
                    return "stop"
                self.trigger_ns = mono_ns
                self.episode = {"session_id": self.session_id, "condition": self.cache.condition,
                    "trigger_frame_id": self.frame_id, "trigger_message_index": index,
                    "reason": reason, "failure": self.cache.last_failure,
                    "pre_frames": [f for f, _ in self.pre], "pre_frames_omitted": self.pre_omitted,
                    "trigger_frame": frame, "post_frames": [], "omitted_tail_frames": 0,
                    "post_seconds_requested": self.post_seconds, "post_window_complete": False,
                    "trading_enabled": False}
                self.episode_bytes = len(encode(self.episode))
                if self.episode_bytes > EPISODE_BYTES:
                    # Keep a bounded failure marker rather than lose the entire run.
                    self.episode["failure"] = None
                    self.episode["pre_frames"] = []
                    self.episode["evidence_truncated"] = True
                    self.stop_reason = "trigger_size_limit"
                    return "stop"
                return "capture"  # SAME frame's remaining messages are already retained.
            self.counts["discard" if self.cache.last_discard else "checked_messages"] += 1
            if sum(len(b.bids) + len(b.asks) for b in self.cache.books.values()) > 20000:
                self.cache.invalidate()
                self.stop_reason = "cache_level_limit"
                return "stop"
        self._remember(frame)
        return "observe"

    def tail_due(self, mono_ns):
        return self.trigger_ns is not None and mono_ns - self.trigger_ns >= self.post_seconds * 1e9

    def finish(self, reason, mono_ns):
        if self.episode is None:
            return None
        e = self.episode
        e["end_reason"] = reason
        e["post_seconds_observed"] = max(0.0, (mono_ns - self.trigger_ns) / 1e9)
        e["post_window_complete"] = bool(reason == "tail_complete" and self.tail_due(mono_ns)
            and not self.stop_reason and not e.get("evidence_truncated"))
        e["following"] = following_evidence(e)
        return e


def following_evidence(episode):
    """Locate later snapshots/trades/BBO. Correlation only, never a cause verdict."""
    kinds, snapshots, bbo, trades = Counter(), {}, {}, []
    frame = episode["trigger_frame"]
    index = episode["trigger_message_index"]
    trigger = frame["messages"][index]
    expected = {}
    for row in trigger.get("price_changes", []):
        expected[str(row.get("asset_id"))] = {k: row.get(k) for k in ("best_bid", "best_ask")}
    frames = [(frame, index + 1)] + [(f, 0) for f in episode["post_frames"]]
    for f, start in frames:
        for i, m in enumerate(f["messages"][start:], start):
            if m.get("market") != episode["condition"]:
                continue
            kind, token = m.get("event_type"), str(m.get("asset_id"))
            kinds[str(kind)] += 1
            loc = {"frame_id": f["frame_id"], "message_index": i, "source_timestamp": m.get("timestamp"),
                   "receive_lag_ms": (f["received_perf_ns"] - frame["received_perf_ns"]) / 1e6}
            if kind == "book" and token not in snapshots and not m.get("projection_warnings"):
                try:
                    bids, asks = [], []
                    for key, levels in (("bids", bids), ("asks", asks)):
                        for row in m[key]:
                            price, size = finite(row["price"]), finite(row["size"])
                            if not 0 < price < 1 or size < 0:
                                raise ValueError("invalid_later_book")
                            if size > 0:
                                levels.append(price)
                    loc.update(bid=max(bids, default=None), ask=min(asks, default=None))
                    loc["trigger_reported_bbo"] = expected.get(token)
                    snapshots[token] = loc
                except (ValueError, KeyError, TypeError):
                    pass  # Frame remains in evidence, not a validated snapshot.
            elif kind == "best_bid_ask" and token not in bbo:
                bbo[token] = dict(loc, best_bid=m.get("best_bid"), best_ask=m.get("best_ask"))
            elif kind == "last_trade_price" and len(trades) < 12:
                trades.append(dict(loc, token=token, price=m.get("price"), size=m.get("size"), side=m.get("side")))
    return {"event_counts": dict(kinds), "first_later_book": snapshots,
            "first_later_bbo": bbo, "first_trades": trades,
            "interpretation": "Later payloads, not atomic snapshots, verified recovery, private fills or a root-cause verdict."}
