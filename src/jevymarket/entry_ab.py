"""Prospective paired paper accounts: continuous vs checkpoint first entry.

Run with ``python -m jevymarket.entry_ab``. This is opt-in; v3 is unchanged.
Both accounts consume the same FastRunner observations, not separate API reads.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import time
import uuid
from contextlib import AsyncExitStack
from datetime import UTC, datetime
from pathlib import Path

from polymarket import AsyncPublicClient
from rich.console import Console
from rich.table import Table

from .fast_runner import FastRunner, JevShadow, settlement_worker, settle_some
from .fast_store import FastStore, dump, probability_metrics
from .fast_strategy import CHECKPOINTS, experiment_parameters, next_deadline
from .jev import JevClient
from .market_data import watch_chainlink_anchors
from .signal import Trade

VERSION = "v4-btc5m-entry-ab"
ARMS = ("continuous", "checkpoint")
LABELS = {"continuous": "连续首次入场", "checkpoint": "仅checkpoint首次入场"}
PROTOCOL = {
    "entry_ab_protocol": 1,
    "arms": list(ARMS),
    "account_limits": "independent_equal_caps",
    "entry_policy": "first_qualifying_observation_per_market_per_arm",
    "start_policy": "next_full_market_after_experiment_creation",
    "review_target_resolved_markets": 300,
}
SCHEMA = """
CREATE TABLE IF NOT EXISTS entry_ab_orders (
    version TEXT NOT NULL, arm TEXT NOT NULL, slug TEXT NOT NULL,
    observation_id INTEGER NOT NULL, ts REAL NOT NULL, outcome TEXT NOT NULL,
    price REAL NOT NULL, size REAL NOT NULL, usd REAL NOT NULL,
    PRIMARY KEY(version, arm, slug)
);
CREATE TABLE IF NOT EXISTS entry_ab_events (
    version TEXT NOT NULL, arm TEXT NOT NULL, observation_id INTEGER NOT NULL,
    status TEXT NOT NULL,
    PRIMARY KEY(version, arm, observation_id)
);
"""


def market_start(slug: str) -> int:
    if not slug.startswith("btc-updown-5m-"):
        raise ValueError("对照实验只支持BTC 5m")
    return int(slug.rsplit("-", 1)[1])


class EntryStore(FastStore):
    """Separate account ledgers; inherited observations/settlement remain shared."""

    def __init__(self, path):
        super().__init__(path)
        self.conn.executescript(SCHEMA)

    def entry_start(self, version: str) -> int:
        row = self.conn.execute("SELECT created_ts FROM fast_experiments WHERE version=?", (version,)).fetchone()
        if row is None:
            raise ValueError("实验未注册")
        return (int(row[0]) // 300 + 1) * 300

    def paper_order(self, version: str, slug: str, observation_id: int, trade: Trade,
                    *, max_exposure: float) -> str:
        """Called by FastRunner only after its valid signal and freshness checks.

        One synchronous transaction records BOTH independent accounts. No use of
        future labels, no online calls, and no writing to v3/live order tables.
        """
        obs = self.conn.execute(
            "SELECT * FROM fast_observations WHERE id=? AND version=? AND slug=?",
            (observation_id, version, slug),
        ).fetchone()
        if obs is None or obs["status"] != "trade_signal":
            raise ValueError("模拟订单必须对应本实验的有效信号")
        if trade.outcome not in ("UP", "DOWN") or not all(math.isfinite(v) and v > 0 for v in
                                                         (trade.price, trade.size, trade.usd, max_exposure)):
            raise ValueError("模拟订单数值无效")
        if trade.price >= 1 or abs(trade.price * trade.size - trade.usd) > 0.00011:
            raise ValueError("模拟订单金额/价格不一致")
        params = self.parameters(version) or {}
        if params.get("entry_ab_protocol") != 1:
            raise ValueError("禁止将入场对照写入旧实验")
        now = time.time()
        if not 0 <= now - obs["ts"] <= params["short_term_max_sample_age_seconds"]:
            return "报价已过期：两组均不入场"
        if now >= market_start(slug) + 300:
            return "窗口已结束：两组均不入场"
        if market_start(slug) < self.entry_start(version):
            return "共同预热：从实验创建后的下一个完整5m窗口开始入场"
        messages = []
        with self.conn:
            for arm in ARMS:
                if arm == "checkpoint" and obs["checkpoint"] is None:
                    status = "outside_checkpoint"
                elif self.conn.execute(
                    "SELECT 1 FROM entry_ab_orders WHERE version=? AND arm=? AND slug=?",
                    (version, arm, slug),
                ).fetchone():
                    status = "already_entered"
                else:
                    exposure = self.conn.execute(
                        """SELECT COALESCE(SUM(o.usd),0) FROM entry_ab_orders o
                        LEFT JOIN market_results r ON r.slug=o.slug
                        WHERE o.version=? AND o.arm=? AND r.slug IS NULL""", (version, arm),
                    ).fetchone()[0]
                    if exposure + trade.usd > max_exposure:
                        status = "exposure_limit"
                    else:
                        self.conn.execute("INSERT INTO entry_ab_orders VALUES (?,?,?,?,?,?,?,?,?)",
                                          (version, arm, slug, observation_id, now, trade.outcome,
                                           trade.price, trade.size, trade.usd))
                        status = "entered"
                self.conn.execute("INSERT OR IGNORE INTO entry_ab_events VALUES (?,?,?,?)",
                                  (version, arm, observation_id, status))
                text = {"outside_checkpoint": "非checkpoint，仅观察", "already_entered": "已有首单，仅观察",
                        "exposure_limit": "本组敞口上限，跳过", "entered":
                        f"模拟买入 {trade.outcome} {trade.size:.2f}份 @ {trade.price:.3f}，${trade.usd:.2f}"}[status]
                messages.append(f"{LABELS[arm]}：{text}")
        return "；".join(messages)

    def paired_orders(self, version: str) -> list[dict]:
        return [dict(r) for r in self.conn.execute(
            """SELECT a.*, o.seconds_left, o.checkpoint, o.quant_p, r.up_won
            FROM entry_ab_orders a JOIN fast_observations o ON o.id=a.observation_id
            LEFT JOIN market_results r ON r.slug=a.slug
            WHERE a.version=? ORDER BY a.ts,a.arm""", (version,),
        )]


def order_profit(row: dict | None, *, unit: bool = False) -> float:
    """A non-entry is zero only for a separately confirmed resolved market."""
    if row is None:
        return 0.0
    if row["up_won"] is None:
        raise ValueError("未结算订单不能当亏损或零收益")
    won = (row["outcome"] == "UP") == bool(row["up_won"])
    return (1 / row["price"] if won else 0) - 1 if unit else (row["size"] if won else 0) - row["usd"]


def summary(orders: list[dict]) -> dict:
    settled = [o for o in orders if o["up_won"] is not None]
    profits = [order_profit(o) for o in settled]
    units = [order_profit(o, unit=True) for o in settled]
    stake = sum(o["usd"] for o in settled)
    winners = sorted((p for p in units if p > 0), reverse=True)
    return {"orders": len(orders), "settled": len(settled), "pending": len(orders) - len(settled),
            "wins": sum(p > 0 for p in profits), "stake": stake, "gross_pnl": sum(profits),
            "gross_roi": sum(profits) / stake if stake else None, "unit_pnl": sum(units),
            "unit_roi": sum(units) / len(units) if units else None,
            "unit_pnl_without_top3_wins": sum(units) - sum(winners[:3]),
            "cost_stress": [{"assumed_fraction_of_entry_notional": rate,
                             "scenario_pnl": sum(profits) - rate * stake}
                            for rate in (0.02, 0.05)]}


# Whitelist only research values. Never export keys, wallets, whole settings,
# arbitrary API responses, Jev raw JSON, or database paths.
OBS_FIELDS = ("id", "slug", "ts", "seconds_left", "checkpoint", "quant_p", "market_p", "yes_ask", "no_ask",
              "status", "jev_status", "jev_p", "jev_answerable", "jev_clarity", "jev_requested_ts", "jev_received_ts",
              "up_won")
PATH_FIELDS = ("sample_count", "history_span_seconds", "latest_sample_age_seconds", "distance_z",
               "return_30s_pct", "return_60s_pct", "return_180s_pct", "return_300s_pct", "realized_vol_60s_pct",
               "realized_vol_180s_pct", "realized_vol_300s_pct", "remaining_vol_pct", "up_tick_ratio_60s",
               "trend_60s_pct_per_min")
BOOK_FIELDS = ("yes_bid", "yes_ask", "no_bid", "no_ask", "directional_imbalance_5c", "yes_bid_depth_5c_usd",
               "yes_ask_depth_5c_usd", "no_bid_depth_5c_usd", "no_ask_depth_5c_usd", "tick_size", "min_order_size")
ORDER_FIELDS = ("slug", "arm", "observation_id", "ts", "outcome", "price", "size", "usd", "seconds_left",
                "checkpoint", "quant_p", "up_won")


def safe_observation(row: dict) -> dict:
    data = {key: row.get(key) for key in OBS_FIELDS}
    payload = json.loads(row.get("payload_json") or "{}")
    snap = payload.get("snapshot") or {}
    path = snap.get("path_features") or {}
    book = payload.get("book") or {}
    data["snapshot"] = {key: snap.get(key) for key in ("target_price", "current_price", "twap_window_seconds")}
    data["path"] = {key: path.get(key) for key in PATH_FIELDS}
    data["book"] = {key: book.get(key) for key in BOOK_FIELDS}
    data["tick_gap_seconds"] = payload.get("tick_gap_seconds")
    return data


def report(store: EntryStore, version: str) -> dict:
    params = store.parameters(version)
    if not params or params.get("entry_ab_protocol") != 1:
        raise ValueError("没有此入场对照实验；旧v3请使用原stats，或本模块audit-v3命令导出")
    start = store.entry_start(version)
    # One SQLite read transaction yields a coherent report if a runner is active.
    store.conn.execute("BEGIN")
    try:
        rows = store.observations(version)
        orders = store.paired_orders(version)
        events = [dict(r) for r in store.conn.execute(
            "SELECT arm,status,COUNT(*) AS n FROM entry_ab_events WHERE version=? GROUP BY arm,status", (version,),
        )]
    finally:
        store.conn.rollback()
    markets: dict[str, dict] = {}
    for row in rows:
        if market_start(row["slug"]) < start:
            continue
        m = markets.setdefault(row["slug"], {"slug": row["slug"], "up_won": row["up_won"],
                                             "valid_observations": 0, "checkpoints": []})
        m["valid_observations"] += row["quant_p"] is not None
        if row["checkpoint"] is not None:
            m["checkpoints"].append(row["checkpoint"])
    by_arm = {arm: {o["slug"]: o for o in orders if o["arm"] == arm} for arm in ARMS}
    for slug, m in markets.items():
        m["included"] = m["up_won"] is not None and m["valid_observations"] > 0
        m["complete_checkpoints"] = set(m["checkpoints"]) == set(CHECKPOINTS)
        for arm in ARMS:
            order = by_arm[arm].get(slug)
            m[arm] = {key: order.get(key) for key in ORDER_FIELDS} if order else None
        if m["included"]:
            m["unit_difference_checkpoint_minus_continuous"] = (
                order_profit(by_arm["checkpoint"].get(slug), unit=True)
                - order_profit(by_arm["continuous"].get(slug), unit=True))
    paired = [m for m in markets.values() if m["included"]]
    complete = [m for m in paired if m["complete_checkpoints"]]
    both = [m for m in paired if m["continuous"] is not None and m["checkpoint"] is not None]
    valid = [r for r in rows if r["quant_p"] is not None]
    cps = [r for r in valid if r["checkpoint"] is not None and r["up_won"] is not None and r["market_p"] is not None]
    jev_cps = [r for r in cps if r["jev_status"] == "ok" and r["jev_p"] is not None]
    included_slugs = {m["slug"] for m in paired}
    arms = {arm: summary([o for o in orders if o["arm"] == arm and
                         (o["up_won"] is None or o["slug"] in included_slugs)]) for arm in ARMS}
    gaps = [json.loads(r["payload_json"]).get("tick_gap_seconds") for r in rows]
    gaps = sorted(g for g in gaps if g is not None and math.isfinite(g))
    return {"version": version, "generated_ts": time.time(), "entry_start_ts": start,
            "protocol": PROTOCOL, "parameters": {k: params.get(k) for k in (
                "min_edge", "min_trade_price", "max_trade_price", "max_spread", "max_usd_per_trade",
                "max_open_exposure_usd", "kelly_fraction", "short_term_min_history_seconds",
                "short_term_max_sample_age_seconds", "interval_seconds", "jev_mode", "jev_model")},
            "observations": len(rows), "valid_observations": len(valid),
            "time_range": [rows[0]["ts"], rows[-1]["ts"]] if rows else [],
            "tick_gaps": {"p50": gaps[len(gaps) // 2], "p95": gaps[math.ceil(.95 * len(gaps)) - 1],
                          "max": gaps[-1]} if gaps else {},
            "resolved_common_markets": len(paired), "observed_markets_after_start": len(markets),
            "complete_checkpoint_markets": len(complete), "both_entered_markets": len(both),
            "opposite_direction_markets": sum(m["continuous"]["outcome"] != m["checkpoint"]["outcome"] for m in both),
            "unit_paired_difference": sum(m["unit_difference_checkpoint_minus_continuous"] for m in paired),
            "complete_checkpoint_unit_difference": sum(m["unit_difference_checkpoint_minus_continuous"] for m in complete),
            "arms": arms, "events": events,
            "checkpoint_models": {f: probability_metrics(cps, f) for f in ("quant_p", "market_p")},
            "jev_common_models": {f: probability_metrics(jev_cps, f) for f in ("quant_p", "market_p", "jev_p")},
            "markets": list(markets.values()), "research_observations": [safe_observation(r) for r in rows],
            "limitations": ["前向账面模拟，不是实盘或成交仿真", "毛收益不含实际手续费/滑点/队列/深度成交",
                            "2%/5%仅为投入金额成本压力假设，不是Polymarket费率",
                            "固定$1是同一已记录订单归一化，未结算单不计入收益",
                            "同一市场多次观察相关；300市场只是预定复核点，不保证统计显著",
                            "无有效行情市场排除于收益对照，需结合覆盖率解释；完整checkpoint子集仅作敏感性检查"]}


def write_report(path: str, data: dict) -> None:
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    # Exclusive creation protects an earlier evidence export from silent overwrite.
    with destination.open("x", encoding="utf-8") as handle:
        handle.write(dump(data))
        handle.write("\n")


def print_report(data: dict, console: Console) -> None:
    console.print(f"入场同场对照：{data['version']}；仅5m；均为模拟；没有改变Φ(Z)/edge/Kelly", markup=False)
    console.print(f"观察={data['observations']} 有效={data['valid_observations']}；已结算共同市场="
                  f"{data['resolved_common_markets']} / 首次复核目标300；完整checkpoint市场="
                  f"{data['complete_checkpoint_markets']}；间隔统计={data['tick_gaps']}", markup=False)
    table = Table(title="前向记录的两组独立模拟账户（不是事后挑选入场）")
    for col in ("规则", "已结算/待定", "赢", "投入$", "毛PnL$", "毛ROI", "同订单$1毛PnL", "去掉最大3笔赢单后$1PnL"):
        table.add_column(col)
    for arm in ARMS:
        m = data["arms"][arm]
        roi = "—" if m["gross_roi"] is None else f"{m['gross_roi']:.1%}"
        table.add_row(LABELS[arm], f"{m['settled']}/{m['pending']}", str(m["wins"]), f"{m['stake']:.2f}",
                      f"{m['gross_pnl']:+.2f}", roi, f"{m['unit_pnl']:+.2f}", f"{m['unit_pnl_without_top3_wins']:+.2f}")
    console.print(table)
    console.print(f"同一已结算有效市场，不入场记0：checkpoint－连续 $1合计差="
                  f"{data['unit_paired_difference']:+.2f}；双方都有订单={data['both_entered_markets']}，"
                  f"其中方向不同={data['opposite_direction_markets']}。", markup=False)
    console.print(f"仅完整checkpoint市场的$1合计差={data['complete_checkpoint_unit_difference']:+.2f}（敏感性检查）。", markup=False)
    for arm in ARMS:
        cases = data["arms"][arm]["cost_stress"]
        console.print(f"{LABELS[arm]}成本压力假设：扣投入2%后PnL={cases[0]['scenario_pnl']:+.2f}；"
                      f"扣5%后PnL={cases[1]['scenario_pnl']:+.2f}；不是实际净收益。", markup=False)
    console.print("对照事件计数：" + str(data["events"]), markup=False)
    console.print("概率质量与逐笔入场/结果/特征见导出JSON。毛收益不能当实盘收益，不因短期盈利提前结束。", markup=False)


async def run_pair(s, console: Console, *, version: str, interval: float = 10, once: bool = False) -> None:
    if not s.dry_run or s.allowed_timeframes != "5m":
        raise ValueError("对照仅支持5m模拟")
    if not version.strip() or version.startswith(("v2", "v3")):
        raise ValueError("请使用v4或其他新实验名称，禁止覆盖v2/v3")
    if interval != 10:
        raise ValueError("本轮入场对照固定10秒，不同时调整频率")
    s = s.model_copy(update={"strategy_version": version})
    store = EntryStore(s.db_path)
    tasks = []
    try:
        actual_jev = bool(s.typesafe_api_key)
        store.ensure_experiment(version, dict(experiment_parameters(s, interval, actual_jev), **PROTOCOL))
        entry_start = datetime.fromtimestamp(store.entry_start(version), UTC).astimezone()
        async with AsyncExitStack() as stack:
            public = await stack.enter_async_context(AsyncPublicClient())
            settlement = await stack.enter_async_context(AsyncPublicClient())
            jev = None
            if actual_jev:
                jev = await stack.enter_async_context(JevClient(
                    s.typesafe_api_key, model=s.jev_model, base_url=s.typesafe_base_url,
                    timeout=s.jev_timeout_seconds, max_retries=s.jev_max_retries))
            shadow = JevShadow(jev, store, console)
            runner = FastRunner(s, store, public, shadow, console, version=version, session=uuid.uuid4().hex)
            tasks = [asyncio.create_task(watch_chainlink_anchors(s, store), name="chainlink"),
                     asyncio.create_task(shadow.work(), name="jev-shadow"),
                     asyncio.create_task(settlement_worker(settlement, s, store, version), name="settlement")]
            console.print(f"实验 {version} | 仅BTC 5m | 10s | 连续/仅checkpoint两组独立模拟账户 | "
                          f"Jev={'影子' if actual_jev else '未启用'} | 入场窗口从 {entry_start:%Y-%m-%d %H:%M:%S %z} 起", markup=False)
            clock = asyncio.get_running_loop().time
            deadline = clock()
            try:
                while True:
                    for task in tasks:
                        if task.done():
                            task.result()
                            raise RuntimeError(f"任务意外结束：{task.get_name()}")
                    await runner.tick(budget_seconds=8)
                    if once:
                        break
                    deadline, skipped = next_deadline(deadline, clock(), interval)
                    if skipped:
                        console.print(f"跳过{skipped}个超时节拍；不重叠、不补旧单", markup=False)
                    await asyncio.sleep(max(0, deadline - clock()))
            finally:
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                while not shadow.queue.empty():
                    observation_id, _, _ = shadow.queue.get_nowait()
                    store.mark_jev(observation_id, "interrupted")
                    shadow.queue.task_done()
    finally:
        store.close()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="BTC 5m 前向入场对照，仅模拟；不改变旧v3命令")
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("run")
    run.add_argument("--dry-run", action="store_true", required=True)
    run.add_argument("--loop", type=float, default=10)
    run.add_argument("--experiment", default=VERSION)
    run.add_argument("--once", action="store_true")
    stats = commands.add_parser("stats")
    stats.add_argument("--experiment", default=VERSION)
    stats.add_argument("--no-settle", action="store_true")
    stats.add_argument("--out", help="导出白名单研究JSON；不覆盖已有文件")
    audit = commands.add_parser("audit-v3")
    audit.add_argument("--experiment", default="v3-btc5m-10s-quant-shadow")
    audit.add_argument("--out", required=True)
    args = parser.parse_args(argv)
    # Import the shared settings/lock only after argument validation. No secrets
    # are displayed, no secure client is built, and default v3 commands stay intact.
    from .fast_cli import settings, single_instance

    console = Console()
    s = settings()
    try:
        if args.command == "run":
            with single_instance(s.db_path):
                asyncio.run(run_pair(s, console, version=args.experiment, interval=args.loop, once=args.once))
            return
        if not Path(s.db_path).is_file():
            raise ValueError("找不到已有数据库；检查当前工作目录，不创建空数据冒充结果")
        store = EntryStore(s.db_path)
        try:
            if args.command == "audit-v3":
                if store.parameters(args.experiment) is None:
                    raise ValueError("找不到指定v3实验")
                store.conn.execute("BEGIN")
                try:
                    rows = store.observations(args.experiment)
                    orders = store.orders(args.experiment)
                finally:
                    store.conn.rollback()
                data = {"version": args.experiment, "purpose": "旧实验逐笔审计，非新验证样本",
                        "research_observations": [safe_observation(r) for r in rows],
                        "orders": [{k: r.get(k) for k in ORDER_FIELDS} for r in orders]}
                write_report(args.out, data)
                console.print(f"已导出旧v3审计：{len(rows)}条观察、{len(orders)}笔模拟单 → {args.out}", markup=False)
                return
            params = store.parameters(args.experiment)
            if not params or params.get("entry_ab_protocol") != 1:
                raise ValueError("对照实验尚无数据；先运行本模块run，不要用旧v3汇总代替")
            if not args.no_settle:
                async def settle():
                    async with AsyncPublicClient() as client:
                        return await settle_some(client, s, store, args.experiment, limit=500)
                console.print(f"新增官方结算 {asyncio.run(settle())} 个5m市场", markup=False)
            data = report(store, args.experiment)
            print_report(data, console)
            if args.out:
                write_report(args.out, data)
                console.print(f"已导出白名单研究数据 → {args.out}", markup=False)
        finally:
            store.close()
    except KeyboardInterrupt:
        console.print("已停止；两组模拟订单与观察数据已保留。", markup=False)
    except (ValueError, OSError) as exc:
        parser.exit(1, f"{exc}\n")


if __name__ == "__main__":
    main()
