"""Public stream framing and diagnostic clocks. No trading/price fallbacks."""
from __future__ import annotations

import json
import math

IO_REVISION = "v6-io-r2"
ERROR_REASONS = frozenset({
    "non_json_stream_frame", "invalid_stream_encoding", "invalid_stream_frame_type",
    "invalid_stream_message_shape", "stream_error_response", "reference_silent",
    "invalid_reference_payload", "twap_message_window_mismatch",
    "missing_numeric_value", "nonfinite_numeric_value", "invalid_source_time",
    "invalid_level", "stale_or_future_book_message", "out_of_order_snapshot",
    "delta_without_snapshot_or_out_of_order", "unknown_book_side", "bbo_delta_mismatch",
    "invalid_tick", "tick_changed_resubscribe", "crossed_book", "invalid_or_inconsistent_book",
    "market_closed_verify_official_result", "clock_wall_jump", "clock_rtt_exceeded",
    "clock_offset_exceeded", "invalid_clock_sample", "clock_request_failed",
    "missing_anchor_or_reference", "history_missing", "history_gap", "stale_reference",
    "raw_behind_twap", "raw_twap_alignment_failure", "insufficient_volatility_history",
    "window_ended", "market_unavailable", "book_metadata_mismatch", "invalid_order_constraints",
    "unknown_resolution_source", "ambiguous_or_missing_twap_window", "wrong_market",
    "market_not_accepting", "market_window_mismatch", "ambiguous_outcome_mapping",
    "invalid_token_ids", "invalid_condition_id",
})


def decode_stream_frame(raw: str | bytes) -> tuple[list[dict], str | None, str | None]:
    """Return messages, control kind, optional text reply.

    Blank frames and application heartbeats are not quotes. They must not reset
    the valid-BTC deadline or clear reference history. Unknown nonempty frames
    still fail closed; never swallow malformed market updates.
    """
    if isinstance(raw, bytes):
        try:
            raw = raw.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ValueError("invalid_stream_encoding") from exc
    if not isinstance(raw, str):
        raise ValueError("invalid_stream_frame_type")
    text = raw.strip()
    if not text:
        return [], "empty", None
    if text.lower() in {"ping", "pong"}:
        reply = ("pong" if text == "ping" else "PONG") if text.lower() == "ping" else None
        return [], text.lower(), reply
    try:
        data = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValueError("non_json_stream_frame") from exc
    messages = data if isinstance(data, list) else [data]
    if any(not isinstance(msg, dict) for msg in messages):
        raise ValueError("invalid_stream_message_shape")
    for msg in messages:
        if "error" in msg or msg.get("type") == "error" or msg.get("event_type") == "error":
            raise ValueError("stream_error_response")
    return messages, None, None


def error_reason(exc: BaseException) -> str:
    """Expose only a known local code, never arbitrary exception text/headers."""
    seen, reasons = set(), []
    while exc is not None and id(exc) not in seen:
        seen.add(id(exc))
        if len(exc.args) == 1 and isinstance(exc.args[0], str) and exc.args[0] in ERROR_REASONS:
            reasons.append(exc.args[0])
        exc = exc.__cause__ or exc.__context__
    return reasons[-1] if reasons else "unclassified"


def clock_sample(stamp: float, before: float, after: float, rtt: float) -> dict:
    """Keep r1 thresholds; separate transport latency from clock offset.

    The integer /time value is quantised. Bounds below are diagnostic uncertainty,
    NOT a precise NTP offset and NOT applied to any event/source timestamp.
    """
    values = (stamp, before, after, rtt)
    if any(isinstance(x, bool) or not isinstance(x, (float, int)) or not math.isfinite(x) for x in values):
        raise ValueError("invalid_clock_sample")
    if stamp <= 0 or rtt < 0:
        raise ValueError("invalid_clock_sample")
    if after < before or abs(after - before - rtt) > .5:
        reason = "clock_wall_jump"
    elif rtt >= 1:
        reason = "clock_rtt_exceeded"
    elif not before - 1.5 <= stamp <= after + .5:
        reason = "clock_offset_exceeded"
    else:
        reason = "ok"
    quantum = 1.0 if stamp == int(stamp) else 0.0
    return {"rtt_seconds": rtt, "offset_seconds": stamp - (before + after) / 2,
            "offset_lower_seconds": stamp - after, "offset_upper_seconds": stamp + quantum - before,
            "whole_second_clock": bool(quantum), "acceptable": reason == "ok", "reason": reason}


def clock_retry_seconds(failures: int) -> float:
    return 30.0 if failures <= 0 else min(30.0, 2.0 ** min(failures, 5))


def data_health(runtime, wall: float, mono: float) -> dict:
    """Independent diagnostics even while the strategy is outside its entry slot."""
    c, refs, market, cache = runtime.c, runtime.reference, runtime.market, runtime.cache
    sources = {}
    for name, rows in refs.samples.items():
        age = wall - rows[-1][0] if rows else None
        sources[name] = {"samples": len(rows), "span_seconds": rows[-1][0] - rows[0][0] if rows else 0,
                         "source_age_seconds": age,
                         "fresh": age is not None and 0 <= age <= c.max_reference_age_seconds}
    anchor = refs.anchors.get((market.start, market.window)) if market else None
    fresh_books = sum(b.fresh(wall, mono, c.max_book_age_seconds) for b in cache.books.values()) if cache else 0
    clock_ok = runtime.clock_ok and 0 <= wall - runtime.clock_ts <= 60
    reference_reason = "market_unavailable"
    if market:
        try:
            refs.estimate(market.start, market.window, wall)
            reference_reason = "ready"
        except ValueError as exc:
            reference_reason = error_reason(exc)
    return {"sources": sources, "anchor_present": anchor is not None, "anchor_price": anchor,
            "fresh_book_sides": fresh_books, "reference_reason": reference_reason,
            "clock_ok": clock_ok, "clock_age_seconds": wall - runtime.clock_ts if runtime.clock_ts else None,
            "clock_sample": runtime.clock_info,
            "ready": bool(clock_ok and market and market.start <= wall < market.end
                          and 0 <= wall - runtime.metadata_ts <= 45
                          and fresh_books == 2 and reference_reason == "ready")}


def health_text(health: dict) -> str:
    def age(value):
        return "缺失" if value is None else f"{value:.1f}s"
    raw = health["sources"]["raw"]
    twap30, twap60 = (health["sources"][name] for name in ("twap30", "twap60"))
    clock = health["clock_sample"]
    clock_reason = "已核对" if health["clock_ok"] else clock.get("reason", "未核对")
    return (f"数据就绪={'是' if health['ready'] else '否'} | raw={raw['samples']}条/{raw['span_seconds']:.0f}s"
            f"/年龄{age(raw['source_age_seconds'])} | TWAP30/60年龄="
            f"{age(twap30['source_age_seconds'])}/{age(twap60['source_age_seconds'])}"
            f" | 目标={'已捕获' if health['anchor_present'] else '待边界'}"
            f" | 新鲜盘口={health['fresh_book_sides']}/2 | 参考检查={health['reference_reason']} | 时钟={clock_reason}")
