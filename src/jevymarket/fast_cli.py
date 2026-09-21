"""Default CLI for the isolated, paper-only BTC 5m experiment."""

from __future__ import annotations

import asyncio
import json
import logging
import math
import statistics
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

import typer
from polymarket import AsyncPublicClient
from rich.console import Console
from rich.table import Table

from .cli import app as legacy_app
from .fast_runner import run_fast, settle_some
from .fast_store import FastStore, probability_metrics, unit_replay
from .fast_strategy import VERSION, fast_settings

app = typer.Typer(help="BTC 5m 专用：10秒量化 + Jev异步影子对照。当前仅支持 dry-run。", no_args_is_help=True)
app.add_typer(legacy_app, name="legacy", help="旧版命令/历史数据；不参与默认5m运行。")
console = Console()


def settings():
    s = fast_settings()
    logging.basicConfig(level=getattr(logging, s.log_level.upper(), logging.INFO),
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    return s


@contextmanager
def single_instance(db_path: str):
    """OS-held lock, released on exit/crash; an old lock FILE is harmless."""
    import os

    path = Path(db_path).resolve().with_suffix(".fast.lock")
    with path.open("a+b") as handle:
        handle.seek(0, 2)
        if handle.tell() == 0:
            handle.write(b"0")
            handle.flush()
        handle.seek(0)
        try:
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            raise ValueError("此数据库已有一个5m进程在运行，请勿重复启动。") from exc
        try:
            yield
        finally:
            handle.seek(0)
            if os.name == "nt":
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


@app.command()
def run(
    dry_run: bool = typer.Option(False, "--dry-run", help="必须显式指定；此版本不会真实下单。"),
    loop: float = typer.Option(10.0, "--loop", min=1, help="目标开始到开始的间隔；超时跳过，不叠加等待。"),
    experiment: str = typer.Option(VERSION, "--experiment", help="新实验名；修改参数时必须使用新名称。"),
    no_jev: bool = typer.Option(False, "--no-jev", help="关闭Jev影子；必须与已有实验参数一致。"),
    once: bool = typer.Option(False, "--once", help="仅检查一轮，用于诊断。"),
):
    """只分析当前 BTC 5m；每市场最多一笔模拟订单，之后继续记录。"""
    if not dry_run:
        raise typer.BadParameter("必须添加 --dry-run；新策略尚未开放真实交易。")
    if not experiment.strip() or experiment.startswith("v2"):
        raise typer.BadParameter("请使用非 v2 的新实验名称，保留旧实验边界。")
    s = settings()
    try:
        with single_instance(s.db_path):
            asyncio.run(run_fast(s, console, interval=loop, version=experiment, jev_enabled=not no_jev, once=once))
    except KeyboardInterrupt:
        console.print("已停止；已落盘数据保留。", markup=False)
    except ValueError as exc:
        console.print(str(exc), markup=False)
        raise typer.Exit(1) from None


def metric(value: float | None, digits: int = 4) -> str:
    return "—" if value is None else f"{value:.{digits}f}"


def pct(value: float | None) -> str:
    return "—" if value is None else f"{100 * value:.1f}%"


def print_models(title: str, rows: list[dict], fields: list[tuple[str, str]]) -> None:
    table = Table(title=title)
    for c in ("模型", "样本", "独立市场", "Brier↓", "LogLoss↓", "方向命中率"):
        table.add_column(c)
    for name, field in fields:
        m = probability_metrics(rows, field)
        table.add_row(name, str(m["n"]), str(m["markets"]), metric(m["brier"]),
                      metric(m["logloss"]), pct(m["accuracy"]))
    console.print(table)


def print_replays(title: str, series: list[tuple[str, dict]]) -> None:
    table = Table(title=title)
    for c in ("策略/口径", "交易数", "赢", "命中率", "毛PnL", "毛ROI"):
        table.add_column(c)
    for name, r in series:
        table.add_row(name, str(r["trades"]), str(r["wins"]),
                      pct(r["wins"] / r["trades"] if r["trades"] else None),
                      f"${r['pnl']:+.2f}", pct(r["roi"]))
    console.print(table)


@app.command()
def stats(
    experiment: str = typer.Option(VERSION, "--experiment", help="只统计指定的新实验，默认v3。"),
    no_settle: bool = typer.Option(False, "--no-settle", help="仅查看已落盘结果，不请求官方结算。"),
):
    """当前5m实验统计；旧v2不混入。旧数据请使用 legacy stats。"""
    s = settings()
    store = FastStore(s.db_path)
    try:
        parameters = store.parameters(experiment)
        if parameters is None:
            console.print(f"实验 {experiment} 尚无数据；旧v2数据未删除，可用 uv run jevymarket legacy stats 查看。", markup=False)
            return
        if not no_settle:
            async def settle():
                async with AsyncPublicClient() as client:
                    return await settle_some(client, s, store, experiment, limit=500)
            n = asyncio.run(settle())
            console.print(f"新增官方结算 {n} 个5m市场", markup=False)
        rows = store.observations(experiment)
        cps = [r for r in rows if r["checkpoint"] is not None]
        resolved = [r for r in rows if r["quant_p"] is not None and r["up_won"] is not None]
        common = [r for r in cps if r["up_won"] is not None and r["quant_p"] is not None and r["market_p"] is not None]
        paired = [r for r in common if r["jev_status"] == "ok" and r["jev_p"] is not None]
        console.print(f"统计范围：{experiment} | 仅BTC 5m | 目标节拍 {parameters['interval_seconds']:g}s", markup=False)
        console.print(
            f"观察轮数={len(rows)} 有效量化轮数={sum(r['quant_p'] is not None for r in rows)} "
            f"有效独立市场={len({r['slug'] for r in rows if r['quant_p'] is not None})} "
            f"已结算有效独立市场={len({r['slug'] for r in resolved})} checkpoint={len(cps)} "
            f"Jev成功={sum(r['jev_status'] == 'ok' for r in cps)}/{len(cps)}；"
            "无Jev回复不会填成0，也不会借用前一条概率。", markup=False,
        )
        if rows:
            start, end = datetime.fromtimestamp(rows[0]["ts"]), datetime.fromtimestamp(rows[-1]["ts"])
            console.print(f"记录时段：{start:%Y-%m-%d %H:%M:%S} → {end:%Y-%m-%d %H:%M:%S}；进程段数={len({r['session'] for r in rows})}", markup=False)
            gaps = [json.loads(r["payload_json"]).get("tick_gap_seconds") for r in rows]
            gaps = [g for g in gaps if g is not None and math.isfinite(g)]
            if gaps:
                p95 = sorted(gaps)[min(len(gaps) - 1, math.ceil(0.95 * len(gaps)) - 1)]
                console.print(f"实际轮询间隔：中位数 {statistics.median(gaps):.2f}s / P95 {p95:.2f}s / 最大 {max(gaps):.2f}s（不含停机间隔）", markup=False)
        print_models("全部已结算有效checkpoint：量化 vs 市场（同样本）", common,
                     [("Φ(Z)", "quant_p"), ("Polymarket", "market_p")])
        print_models("Jev成功回复的共同checkpoint子集（不等于全部样本）", paired,
                     [("Φ(Z)", "quant_p"), ("Jev", "jev_p"), ("Polymarket", "market_p")])
        orders = store.orders(experiment)
        settled_orders = [r for r in orders if r["up_won"] is not None]
        wins = sum((r["outcome"] == "UP") == bool(r["up_won"]) for r in settled_orders)
        stake = sum(r["usd"] for r in settled_orders)
        profit = sum((r["size"] if (r["outcome"] == "UP") == bool(r["up_won"]) else 0) - r["usd"] for r in settled_orders)
        unit_profit = sum((1 / r["price"] if (r["outcome"] == "UP") == bool(r["up_won"]) else 0) - 1 for r in settled_orders)
        count = len(settled_orders)
        console.print(f"模拟订单={len(orders)} 已结算={count} 投入=${stake:.2f}；未计手续费/滑点/真实成交。", markup=False)
        print_replays("量化执行拆分：先对齐入场，再比较仓位", [
            ("实际模拟/Kelly", {"trades": count, "wins": wins, "pnl": profit, "roi": profit / stake if stake else None}),
            ("同一批订单同入场/固定$1", {"trades": count, "wins": wins, "pnl": unit_profit, "roi": unit_profit / count if count else None}),
            ("所有10秒有效观察/首次$1", unit_replay(resolved, parameters)),
            ("仅固定checkpoint/首次$1", unit_replay(common, parameters)),
        ])
        eligible = [r for r in paired if unit_replay([r], parameters)["trades"]]
        blocked = sum(r["jev_answerable"] < parameters["min_answerable"] or r["jev_clarity"] < parameters["min_clarity"] for r in eligible)
        console.print(f"Jev门控诊断：共同checkpoint量化信号={len(eligible)}，旧门控拒绝={blocked}；阈值 answerable>={parameters['min_answerable']}、clarity>={parameters['min_clarity']}。", markup=False)
        print_replays("Jev影子反事实：请求时盘口；未模拟响应延迟，不是可执行回测", [
            ("A 纯Φ(Z)/共同子集", unit_replay(paired, parameters)),
            ("B Φ(Z)+旧Jev门控", unit_replay(paired, parameters, jev_gate=True)),
            ("C Jev概率", unit_replay(paired, parameters, field="jev_p")),
        ])
        cp_table = Table(title="Checkpoint覆盖与概率质量（同一市场的多行不是独立市场）")
        for c in ("Checkpoint", "记录", "已结算量化/市场共同样本", "Φ(Z) Brier", "市场 Brier"):
            cp_table.add_column(c)
        for cp in (240, 180, 120, 60, 30):
            selected = [r for r in common if r["checkpoint"] == cp]
            cp_table.add_row(f"T-{cp}s", str(sum(r["checkpoint"] == cp for r in cps)), str(len(selected)),
                             metric(probability_metrics(selected, "quant_p")["brier"]),
                             metric(probability_metrics(selected, "market_p")["brier"]))
        console.print(cp_table)
        calibration = Table(title="已结算checkpoint校准（样本相关，非独立重复试验）")
        for c in ("Φ(Z)区间", "样本", "独立市场", "预测均值", "实际Up比例"):
            calibration.add_column(c)
        for bucket in range(10):
            selected = [r for r in common if min(9, int(r["quant_p"] * 10)) == bucket]
            n = len(selected)
            calibration.add_row(f"{bucket / 10:.1f}–{(bucket + 1) / 10:.1f}", str(n), str(len({r['slug'] for r in selected})),
                                metric(sum(r["quant_p"] for r in selected) / n if n else None, 3),
                                pct(sum(r["up_won"] for r in selected) / n if n else None))
        console.print(calibration)
        console.print("所有$1对照均忽略最小订单量、敞口限制及真实成交；仅用于诊断，不能当作实盘收益。", markup=False)
    finally:
        store.close()
