"""Small read-only diagnostics and bounded local latency histograms; no trading."""
from __future__ import annotations

import gzip
import json
import math
import sqlite3
from collections import Counter, deque
from pathlib import Path

from .maker_precision import TransportFailures
from .maker_resync import resync_summary


class ReactionWindow:
    """All coalesced signal evaluations, including when no order exists.

    0.1ms histogram buckets, bounded at 10 seconds. This is a LOCAL signal to
    action measurement, not exchange/network acknowledgement or per-WS-frame SLA.
    Watchdog compute-only samples are counted separately.
    """
    def __init__(self):
        self.groups = {}

    def add(self, elapsed_ms: float, event_driven: bool):
        if not math.isfinite(elapsed_ms) or elapsed_ms < 0:
            raise ValueError("invalid_reaction_measurement")
        name = "event" if event_driven else "watchdog"
        group = self.groups.setdefault(name, {"count": 0, "over_100ms": 0, "max_ms": 0.0, "bins": Counter()})
        group["count"] += 1
        group["over_100ms"] += elapsed_ms > 100
        group["max_ms"] = max(group["max_ms"], elapsed_ms)
        group["bins"][min(100001, math.ceil(elapsed_ms * 10))] += 1

    def take(self) -> dict:
        groups, self.groups = self.groups, {}
        return {"scope": "coalesced_local_signal_to_action;watchdog_compute_separate;not_exchange_ack",
                "histogram_quantum_ms": .1, "groups": groups}


def merge_reactions(windows) -> dict:
    groups = {}
    for window in windows:
        for name, source in window.get("groups", {}).items():
            if name not in {"event", "watchdog"}:
                continue
            group = groups.setdefault(name, {"count": 0, "over_100ms": 0, "max_ms": 0.0, "bins": Counter()})
            group["count"] += source["count"]
            group["over_100ms"] += source["over_100ms"]
            group["max_ms"] = max(group["max_ms"], source["max_ms"])
            group["bins"].update({int(k): v for k, v in source["bins"].items()})
    result = {}
    for name in ("event", "watchdog"):
        group = groups.get(name, {"count": 0, "over_100ms": 0, "max_ms": 0, "bins": {}})
        row = {"samples": group["count"], "over_100ms": group["over_100ms"],
               "max_ms": group["max_ms"] if group["count"] else None}
        for label, p in (("p50_upper_ms", .5), ("p95_upper_ms", .95), ("p99_upper_ms", .99)):
            row[label] = None
            if not group["count"]:
                continue
            rank, accumulated = math.ceil(p * group["count"]), 0
            for bucket, count in sorted(group["bins"].items()):
                accumulated += count
                if accumulated >= rank:
                    row[label] = bucket / 10 if bucket <= 100000 else group["max_ms"]
                    break
        result[name] = row
    result["scope"] = "本地合并信号至动作；定时检查单列；无订单也采样；非网络/交易所回执"
    result["quantile_method"] = "0.1ms直方图桶上界；超过10秒的桶使用实测最大值上界"
    return result


def safe_book_message(msg: dict) -> dict:
    """Whitelist evidence even for rejected malformed public frames.

    Invalid/nonfinite scalars are marked instead of crashing the journal. Long
    strings/arrays are bounded with explicit truncation; no arbitrary extra keys,
    headers, error bodies or credentials are included.
    """
    changed = []

    def scalar(value):
        if isinstance(value, str):
            if len(value) > 512:
                changed.append("long_string")
            return value[:512]
        if type(value) in (int, float) and math.isfinite(value):
            return value
        changed.append("non_scalar_or_nonfinite")
        return None

    keys = ("event_type", "market", "asset_id", "timestamp", "price", "size", "side", "hash",
            "transaction_hash", "fee_rate_bps", "old_tick_size", "new_tick_size")
    out = {k: scalar(msg[k]) for k in keys if k in msg}
    for name in ("bids", "asks", "price_changes"):
        if name not in msg:
            continue
        if not isinstance(msg[name], list):
            out[name] = None
            changed.append("invalid_array")
            continue
        if len(msg[name]) > 20000:
            changed.append("array_truncated")
        out[name] = []
        for row in msg[name][:20000]:
            if not isinstance(row, dict):
                out[name].append(None)
                changed.append("invalid_row")
                continue
            out[name].append({k: scalar(row[k]) for k in
                             ("asset_id", "price", "size", "side", "best_bid", "best_ask", "hash") if k in row})
    if changed:
        out["diagnostic_sanitization"] = sorted(set(changed))
    return out


def diagnostic_report(path: Path, out: Path | None = None) -> dict:
    """Query just the latest run's diagnostic event kinds, not its huge WS log.

    No schema initialization, settlement, network, private credentials or writes
    to the SQLite source. Export creates a new file exclusively. A maximum of two
    rejects per reason and six total, 2MB combined, are included as evidence.
    """
    path = Path(path).resolve()
    if not path.is_file():
        raise ValueError("找不到Maker数据库，不会创建空数据库")
    conn = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=5)
    try:
        conn.execute("PRAGMA query_only=ON")
        conn.execute("BEGIN")
        tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if not {"maker_meta", "maker_events", "maker_orders"} <= tables:
            raise ValueError("不是Maker数据库；不会迁移或修改旧数据库")
        meta_row = conn.execute("SELECT data FROM maker_meta WHERE id=1").fetchone()
        if not meta_row:
            raise ValueError("Maker协议缺失")
        meta = json.loads(meta_row[0])
        run = conn.execute("SELECT id,ts,data FROM maker_events WHERE kind='runtime' ORDER BY id DESC LIMIT 1").fetchone()
        start_id, start_ts, runtime = (run[0], run[1], json.loads(run[2])) if run else (0, None, {})
        counts = dict(conn.execute("SELECT kind,count(*) FROM maker_events WHERE id>=? GROUP BY kind", (start_id,)))
        errors, rejects, reasons, statuses, shapes, clocks = (Counter() for _ in range(6))
        reaction_windows, examples, last_observation = [], [], None
        example_counts, example_bytes, omitted = Counter(), 0, 0
        transport = TransportFailures()
        late = deque(maxlen=24)
        for event_id, ts, kind, data in conn.execute(
            "SELECT id,ts,kind,data FROM maker_events WHERE id>=? AND kind IN "
            "('source_error','book_reject','observation','clock','reaction_window') ORDER BY id", (start_id,)
        ):
            d = json.loads(data)
            if kind == "source_error":
                transport.add(d)
                errors["|".join(str(d.get(k, "unknown")) for k in ("stage", "code", "reason"))] += 1
            elif kind == "book_reject":
                reason = d.get("reason", "unknown")
                rejects[reason] += 1
                size = len(data.encode("utf-8"))
                if len(examples) < 6 and example_counts[reason] < 2 and example_bytes + size <= 2_000_000:
                    examples.append({"event_id": event_id, "received_ts": ts, "data": d})
                    example_counts[reason] += 1
                    example_bytes += size
                else:
                    omitted += 1
            elif kind == "clock":
                clocks[d.get("reason", "not_recorded")] += 1
            elif kind == "reaction_window":
                reaction_windows.append(d)
            elif kind == "observation":
                last_observation = d
                reasons[d.get("reason", "unknown")] += 1
                for b in d.get("data_health", {}).get("book_details", {}).values():
                    statuses[b["status"]] += 1
                    shapes[b["shape"]] += 1
                slug = d.get("slug")
                if slug:
                    try:
                        left = int(slug.rsplit("-", 1)[1]) + 300 - d["ts"]
                    except (ValueError, KeyError, TypeError):
                        continue
                    config = meta.get("config", {})
                    if config.get("cancel_before_end_seconds", 2) < left <= config.get("entry_seconds", 10):
                        late.append({"ts": d["ts"], "slug": slug, "seconds_left": left,
                                     "reason": d.get("reason"), "estimate": d.get("estimate"),
                                     "book": d.get("book"), "data_health": d.get("data_health")})
        # The reporting scope is explicitly the latest runtime marker, not all
        # old failures accumulated in the database. Orders remain cumulative.
        summary = {"paper_only": True, "runtime": runtime, "start_event_id": start_id, "start_ts": start_ts,
                   "resync_latest_run": resync_summary(conn, start_id),
                   "measurement_latest_run": runtime.get("measurement", {"clock": "legacy_or_unrecorded", "resolution_seconds": None}),
                   "http_failures_latest_run": transport.report(),
                   "event_counts_latest_run": counts, "source_errors_latest_run": dict(errors),
                   "book_rejections_latest_run": dict(rejects), "observation_reasons_latest_run": dict(reasons),
                   "token_book_status_counts": dict(statuses), "token_book_shape_counts": dict(shapes),
                   "clock_results_latest_run": dict(clocks), "local_reaction_all_evaluations": merge_reactions(reaction_windows),
                   "ledger_orders_all_runs": conn.execute("SELECT count(*) FROM maker_orders").fetchone()[0],
                   "reject_examples_included": len(examples), "reject_examples_omitted": omitted}
        report = {"format": "v6-book-diagnostic-r3", "meta": meta, "summary": summary,
                  "last_observation": last_observation, "late_observation_examples": list(late), "reject_examples": examples,
                  "limitations": ["当前最近一次runtime运行；不是所有历史运行的收益统计",
                                  "单边盘口仍不符合本策略资格，不伪造缺失卖价，也未改变最后10秒规则",
                                  "旧版本未保存失败触发帧，无法事后补回；本报告只含有界样例，完整事件仍在原数据库",
                                  "桶分位数为本地延迟近似上界，不是100ms实盘撤单证明",
                                  "observe_only=true时主动禁止新模拟订单，不应以报价次数验收；原有订单风险不会清除",
                                  "没有报价或成交可以是合法过滤；本报告不能证明盈利"]}
    finally:
        conn.close()
    if out:
        out = Path(out)
        out.parent.mkdir(parents=True, exist_ok=True)
        opener = gzip.open if out.suffix == ".gz" else open
        with opener(out, "xt", encoding="utf-8") as handle:
            json.dump(report, handle, ensure_ascii=False, allow_nan=False, separators=(",", ":"))
    return report
