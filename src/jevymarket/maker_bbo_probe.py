"""Public BTC5m BBO probe. No keys, strategy engine, orders, or SQLite access.

python -m jevymarket.maker_bbo_probe --observe-only --seconds 180 --episodes 3
"""
from __future__ import annotations

import argparse
import asyncio
import gzip
import json
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .maker_bbo_evidence import EPISODE_BYTES, FRAME_BYTES, PROBE_REVISION, BBOEvidence, encode
from .maker_model import finite

GAMMA = "https://gamma-api.polymarket.com"
CLOB = "https://clob.polymarket.com"
MARKET_WS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


async def public_get(http, url, **params):
    # There is deliberately no configurable host, API credential, or write verb.
    if not (url == CLOB + "/book" or url.startswith(GAMMA + "/markets/slug/btc-updown-5m-")):
        raise ValueError("non_public_probe_endpoint")
    r = await http.get(url, params=params, timeout=4)
    r.raise_for_status()
    return r.json()


def array(value):
    return json.loads(value) if isinstance(value, str) else value


async def discover(http, wall):
    start = int(wall) // 300 * 300
    slug = f"btc-updown-5m-{start}"
    m = await public_get(http, GAMMA + "/markets/slug/" + slug)
    labels, tokens = array(m.get("outcomes")), array(m.get("clobTokenIds"))
    if (m.get("slug") != slug or m.get("active") is not True or m.get("closed") is not False
            or m.get("acceptingOrders") is not True or not isinstance(labels, list)
            or not isinstance(tokens, list) or len(labels) != 2 or len(tokens) != 2
            or {str(x).upper() for x in labels} != {"UP", "DOWN"}):
        raise ValueError("invalid_btc5m_market")
    tokens = [str(t) for t in tokens]
    condition = str(m.get("conditionId", ""))
    if (len(set(tokens)) != 2 or any(not t.isdigit() for t in tokens)
            or len(condition) != 66 or not condition.startswith("0x")
            or any(c not in "0123456789abcdefABCDEF" for c in condition[2:])):
        raise ValueError("invalid_market_identifiers")
    end = datetime.fromisoformat(m["endDate"].replace("Z", "+00:00"))
    if end.tzinfo is None or abs(end.timestamp() - start - 300) > 1:
        raise ValueError("invalid_market_end")
    specs = {}
    for token in tokens:
        book = await public_get(http, CLOB + "/book", token_id=token)
        if str(book.get("asset_id")) != token or book.get("market") != condition:
            raise ValueError("book_metadata_mismatch")
        tick, minimum = finite(book["tick_size"]), finite(book["min_order_size"])
        if not 0 < tick < 1 or minimum <= 0:
            raise ValueError("invalid_order_constraints")
        specs[token] = (tick, minimum)
    # HTTP depth is NEVER used as the WS baseline. Await the actual WS snapshots.
    return {"slug": slug, "start": start, "end": start + 300, "condition": condition,
            "tokens": dict(zip((str(x).upper() for x in labels), tokens, strict=True)), "specs": specs}


async def heartbeat(ws):
    while True:
        await ws.send("PING")
        await asyncio.sleep(10)


async def read_connection(ws, capture, market_end, deadline):
    """A BBO reject does NOT exit this reader. Keep same socket and frame tail."""
    ping = asyncio.create_task(heartbeat(ws))
    try:
        await ws.send(encode({"assets_ids": list(capture.cache.books), "type": "market",
                              "custom_feature_enabled": True}).decode())
        while True:
            mono_ns, wall = time.perf_counter_ns(), time.time()
            if capture.tail_due(mono_ns):
                return "tail_complete"
            if time.monotonic() >= deadline:
                return "run_deadline"
            if wall >= market_end:
                return "market_boundary"
            if ping.done():
                ping.result()
                return "heartbeat_stopped"
            remaining = min(1.0, deadline - time.monotonic(), market_end - wall)
            if capture.trigger_ns is not None:
                remaining = min(remaining, capture.post_seconds - (mono_ns - capture.trigger_ns) / 1e9)
            try:
                raw = await asyncio.wait_for(ws.recv(), max(.001, remaining))
            except TimeoutError:
                continue
            wall, mono_ns = time.time(), time.perf_counter_ns()
            status = capture.feed(raw, wall, mono_ns)
            if status == "stop":
                return capture.stop_reason
            # Application heartbeat reply; never use it as a price update.
            if capture.last_frame.get("control") == "ping":
                await asyncio.wait_for(ws.send("PONG"), 1)
    finally:
        ping.cancel()
        await asyncio.gather(ping, return_exceptions=True)


async def collect(seconds=180, episodes=3, *, connector=None, http_factory=None):
    import httpx
    from websockets.asyncio.client import connect

    connector = connect if connector is None else connector
    http_factory = httpx.AsyncClient if http_factory is None else http_factory
    report = {"format": PROBE_REVISION, "observe_only": True, "trading_enabled": False,
        "started_wall": time.time(), "limits": {"seconds": seconds, "episodes": episodes,
        "post_seconds": 2, "frame_bytes": FRAME_BYTES, "episode_bytes": EPISODE_BYTES,
        "connections": 20}, "sessions": [], "incidents": [], "termination": "run_deadline",
        "limitations": ["Passive standalone collector; no order engine or source database access.",
            "Application ws.recv boundaries and message order, not TCP/fragment boundaries.",
            "Public-field projection plus wire hash; not verbatim wire contents. Prehistory is bounded.",
            "Post-trigger cache stays invalid; later frames are evidence only, not validated recovery.",
            "No root-cause, profitability, private-fill or exchange-latency guarantee."]}
    deadline = time.monotonic() + seconds
    try:
        async with http_factory(timeout=4) as http:
            while time.monotonic() < deadline and len(report["sessions"]) < 20:
                session = {"session_id": uuid.uuid4().hex, "started_wall": time.time()}
                report["sessions"].append(session)
                capture = None
                reason = "unknown"
                try:
                    market = await asyncio.wait_for(discover(http, time.time()), max(.001, deadline - time.monotonic()))
                    session["market"] = market
                    if time.time() >= market["end"]:
                        reason = "market_boundary"
                        continue
                    capture = BBOEvidence(market["condition"], market["specs"], session["session_id"])
                    async with connector(MARKET_WS, open_timeout=min(5, max(.001, deadline - time.monotonic())),
                                         close_timeout=1, ping_interval=None, max_size=FRAME_BYTES, max_queue=64) as ws:
                        # Finish BEFORE __aexit__: close-handshake time is not tail observation.
                        try:
                            reason = await read_connection(ws, capture, market["end"], deadline)
                        except asyncio.CancelledError:
                            reason = "interrupted"
                            raise
                        except Exception as exc:
                            reason = "transport_" + type(exc).__name__
                        finally:
                            incident = capture.finish(reason, time.perf_counter_ns())
                            if incident is not None:
                                report["incidents"].append(incident)
                except asyncio.CancelledError:
                    reason = "interrupted"
                    raise
                except Exception as exc:
                    # No exception body/headers/credentials are copied to the report.
                    reason = "setup_" + type(exc).__name__
                finally:
                    session["end_reason"] = reason
                    session["ended_wall"] = time.time()
                    if capture is not None:
                        session["counts"] = dict(capture.counts)
                        if capture.episode is None and capture.stop_reason:
                            frame = capture.last_frame
                            session["last_frame"] = frame if len(encode(frame)) <= 32768 else {
                                k: frame[k] for k in ("frame_id", "wire_bytes", "wire_sha256", "parse_error") if k in frame}
                            if session["last_frame"] is not frame:
                                session["failure_frame_omitted_for_size"] = True
                complete = sum(x["post_window_complete"] for x in report["incidents"])
                if complete >= episodes:
                    report["termination"] = "target_complete"
                    break
                # Total incidents, including interrupted tails, are bounded too.
                if len(report["incidents"]) >= 3:
                    report["termination"] = "incident_limit_with_partial_tails"
                    break
                if time.monotonic() < deadline:
                    await asyncio.sleep(min(1.0, deadline - time.monotonic()))
            else:
                if len(report["sessions"]) >= 20:
                    report["termination"] = "connection_limit"
    except asyncio.CancelledError:
        report["termination"] = "interrupted"
    report["ended_wall"] = time.time()
    report["summary"] = {"incidents": len(report["incidents"]),
        "complete_post_windows": sum(x["post_window_complete"] for x in report["incidents"]),
        "connections_attempted": len(report["sessions"]), "termination": report["termination"],
        "root_cause_verified": False, "profit_experiment_enabled": False}
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description="BBO定向取证：纯公共数据，无模拟或真实订单，不读写旧数据库")
    parser.add_argument("--observe-only", action="store_true", required=True)
    parser.add_argument("--seconds", type=int, default=180, choices=range(1, 181), metavar="1..180")
    parser.add_argument("--episodes", type=int, default=3, choices=(1, 2, 3))
    parser.add_argument("--out", type=Path)
    args = parser.parse_args(argv)
    stamp = datetime.now(timezone(timedelta(hours=8))).strftime("%Y%m%d_%H%M%S_%f")
    out = args.out or Path(f"v6_bbo_probe_{stamp}.json.gz")
    if not out.name.endswith(".json.gz"):
        parser.error("输出文件必须以.json.gz结尾")
    out.parent.mkdir(parents=True, exist_ok=True)
    # Reserve exclusively BEFORE any public read; never silently overwrite.
    with out.open("xb") as raw:
        print(f"{PROBE_REVISION} | 独立只观察 | 收集BBO冲突后连续消息 | 不生成订单、不修改旧库", flush=True)
        report = asyncio.run(collect(args.seconds, args.episodes))
        payload = encode(report)
        if len(payload) > 24 * 1024 * 1024:
            raise ValueError("probe_output_size_limit")
        with gzip.GzipFile(fileobj=raw, mode="wb", mtime=0) as handle:
            handle.write(payload)
    print(json.dumps(report["summary"], ensure_ascii=False, indent=2))
    print(f"已导出 → {out.resolve()}\n这是定向取证结果，不代表BBO修复通过或恢复收益实验。", flush=True)


if __name__ == "__main__":
    main()
