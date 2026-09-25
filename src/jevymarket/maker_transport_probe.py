"""Bounded public HTTP diagnostics; no order engine, WS, credentials or database."""
from __future__ import annotations

import argparse
import asyncio
import gzip
import json
import os
import socket
import ssl
import sys
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.request import getproxies

import httpx

from .run_paths import run_output_path

REVISION = "v6-public-transport-probe-r1"
BODY_LIMIT = 65536
SAFE_ERROR_TYPES = (
    ssl.SSLCertVerificationError, ssl.SSLError, socket.gaierror,
    ConnectionRefusedError, ConnectionResetError, TimeoutError,
    httpx.ConnectTimeout, httpx.ReadTimeout, httpx.WriteTimeout, httpx.PoolTimeout,
    httpx.ProxyError, httpx.ConnectError, httpx.ReadError, httpx.WriteError,
    httpx.RemoteProtocolError, httpx.LocalProtocolError, httpx.HTTPStatusError,
    httpx.InvalidURL, OSError, ValueError, RuntimeError,
)


def safe_error(exc: BaseException) -> dict:
    """Fixed class labels and numeric OS codes only; no exception text or URLs."""
    todo, seen, classes, codes, win_codes = [exc], set(), [], set(), set()
    omitted = False
    while todo and len(seen) < 12:
        item = todo.pop(0)
        if id(item) in seen:
            continue
        seen.add(id(item))
        classes.append(next((t.__name__ for t in SAFE_ERROR_TYPES if isinstance(item, t)), "other_exception"))
        if isinstance(item, OSError):
            for name, dest in (("errno", codes), ("winerror", win_codes)):
                value = getattr(item, name, None)
                if type(value) is int and abs(value) <= 2**31:
                    dest.add(value)
        cause = item.__cause__ or item.__context__
        if cause is not None:
            todo.append(cause)
        if isinstance(item, BaseExceptionGroup):
            omitted |= len(item.exceptions) > 4
            todo.extend(item.exceptions[:4])
    return {"classes": classes, "errno_codes": sorted(codes), "winerror_codes": sorted(win_codes),
            "chain_truncated": omitted or bool(todo)}


def environment_receipt() -> dict:
    # getproxies can consult Windows system settings. Retain booleans, NOT URLs.
    proxies = getproxies()
    keys = ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY", "SSL_CERT_FILE", "SSL_CERT_DIR")
    return {"python_version": ".".join(map(str, sys.version_info[:3])),
            "environment_present": {k: bool(os.environ.get(k) or os.environ.get(k.lower())) for k in keys},
            "system_proxy_entries_present": {k: bool(proxies.get(k)) for k in ("http", "https", "all", "no")},
            "monotonic_resolution_seconds": time.get_clock_info("monotonic").resolution,
            "perf_counter_resolution_seconds": time.get_clock_info("perf_counter").resolution,
            "trust_env": True, "tls_verification": True,
            "proxy_values_cert_paths_headers_and_exception_text_exported": False}


def endpoint(label: str, wall: float) -> str:
    if label == "clob_time":
        return "https://clob.polymarket.com/time"
    if label == "gamma_market":
        start = int(wall) // 300 * 300
        return f"https://gamma-api.polymarket.com/markets/slug/btc-updown-5m-{start}"
    raise ValueError("unsupported_public_endpoint")


async def sample(http, label: str) -> dict:
    started = time.perf_counter_ns()
    row = {"endpoint": label, "started_wall": time.time(), "status_code": None, "ok": False}
    url = endpoint(label, row["started_wall"])

    async def request():
        # Same verified, environment-aware HTTP client defaults as Maker.
        # No arbitrary host, redirects, proxy bypass, auth, or raw-body output.
        headers = {"Cache-Control": "no-cache", "Pragma": "no-cache"} if label == "clob_time" else {}
        async with http.stream("GET", url, headers=headers, timeout=4) as response:
            row["status_code"] = response.status_code
            response.raise_for_status()
            size = 0
            async for block in response.aiter_bytes():
                size += len(block)
                if size > BODY_LIMIT:
                    row["failure"] = "response_size_limit"
                    return
            row["ok"] = True  # HTTP success, NOT market-data/WS/strategy validation.

    try:
        await asyncio.wait_for(request(), 5.0)
    except Exception as exc:
        row["error"] = safe_error(exc)
    row["elapsed_ms"] = (time.perf_counter_ns() - started) / 1e6
    return row


async def collect(rounds: int = 6, *, client_factory=None, pause=asyncio.sleep) -> dict:
    if type(rounds) is not int or not 1 <= rounds <= 6:
        raise ValueError("rounds_must_be_1_to_6")
    report = {"format": REVISION, "observe_only": True, "trading_enabled": False,
              "started_wall": time.time(), "rounds_requested": rounds, "samples": [],
              "limitations": ["Current public HTTP connectivity only, not a reconstruction of old errors.",
                              "Success does not validate WS backlog, BBO recovery, profitability or exchange latency.",
                              "An absent nested cause cannot be reconstructed; class labels are not a root-cause verdict.",
                              "Proxy/certificate settings are preserved, never bypassed or changed."]}
    try:
        report["environment"] = environment_receipt()
        factory = httpx.AsyncClient if client_factory is None else client_factory
        async with factory(timeout=4) as http:
            for index in range(rounds):
                for label in ("clob_time", "gamma_market"):
                    row = await sample(http, label)
                    row["round"] = index + 1
                    report["samples"].append(row)
                if index + 1 < rounds:
                    await pause(1.0)
    except asyncio.CancelledError:
        report["interrupted"] = True
    except Exception as exc:
        report["setup_error"] = safe_error(exc)
    report["ended_wall"] = time.time()
    report["summary"] = {"requests": len(report["samples"]),
        "successes": sum(r["ok"] for r in report["samples"]),
        "failures": sum(not r["ok"] for r in report["samples"]),
        "failure_endpoints": dict(Counter(r["endpoint"] for r in report["samples"] if not r["ok"])),
        "setup_failed": "setup_error" in report, "profit_experiment_enabled": False}
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description="公共HTTP定向检查：不读写旧库、不下单、不更改代理或证书")
    parser.add_argument("--observe-only", action="store_true", required=True)
    parser.add_argument("--rounds", type=int, choices=range(1, 7), default=6)
    parser.add_argument("--out", type=Path)
    args = parser.parse_args(argv)
    stamp = datetime.now(timezone(timedelta(hours=8))).strftime("%Y%m%d_%H%M%S_%f")
    out = run_output_path(args.out, f"v6_transport_check_{stamp}.json.gz")
    if not out.name.endswith(".json.gz"):
        parser.error("输出必须以.json.gz结尾")
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("xb") as raw:
        print(f"{REVISION} | 独立公共HTTP检查 | 不生成订单、不修改旧库", flush=True)
        report = asyncio.run(collect(args.rounds))
        with gzip.GzipFile(fileobj=raw, mode="wb", mtime=0) as handle:
            handle.write(json.dumps(report, ensure_ascii=False, allow_nan=False).encode("utf-8"))
    print(json.dumps(report["summary"], ensure_ascii=False, indent=2))
    print(f"已导出 → {out.resolve()}\nHTTP检查完成不代表行情、BBO或收益实验验收通过。", flush=True)


if __name__ == "__main__":
    main()
