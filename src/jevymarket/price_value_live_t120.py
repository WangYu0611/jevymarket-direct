"""V8.1 T-120 live canary/session.

Only the B arm (Quant + Jev confirmation + refreshed real ask) can ever place
real orders, and only in the T-120 slot. T-110/T-100 remain paper-only.

Live modes fail closed on Polymarket geoblock, account preflight, stale signal,
price-value recheck, or uncertain placement state.
"""
from __future__ import annotations

import argparse
import asyncio
import gzip
import json
import time
import uuid
from contextlib import AsyncExitStack
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
from polymarket import AsyncPublicClient, AsyncSecureClient
from polymarket.models.clob.order_response import AcceptedOrder
from rich.console import Console
from rich.panel import Panel

from .config import load_settings
from .fast_cli import single_instance
from .fast_runner import settlement_worker
from .fast_strategy import fast_settings, local_snapshot, next_deadline
from .jev import JevClient
from .maker_live_canary import (
    _valid_private_key,
    account_preflight,
    fetch_order_state,
    read_user_stream,
    safe_error,
    safe_response,
)
from .market_data import watch_chainlink_anchors
from .price_value_early_forward import (
    INTERVAL_SECONDS,
    EarlyRunner,
    EarlyStore,
    print_rejections,
    print_scoreboard,
)
from .price_value_early_forward import (
    REVISION as PAPER_REVISION,
)
from .price_value_early_forward import (
    build_report as build_paper_report,
)
from .price_value_forward import CRYPTO_TAKER_FEE_RATE, ValueDecision, jev_quality, quant_direction, value_decision
from .run_paths import run_output_path
from .signal import JevView, quantitative_up_probability

REVISION = "v8.1-t120-live-r1"
CONFIRM_ONE = "ONE_REAL_T120_QUANT_JEV_ASK"
CONFIRM_SESSION = "LIVE_T120_QUANT_JEV_ASK_SESSION"
LIVE_SLOT = 120
MAX_ORDER_USD = 5.0
MAX_SESSION_ORDERS = 24
MAX_SESSION_NOTIONAL_USD = 120.0
DEFAULT_SESSION_SECONDS = 86_400

LIVE_SCHEMA = """
CREATE TABLE IF NOT EXISTS v81_live_orders (
    id INTEGER PRIMARY KEY,
    slug TEXT NOT NULL UNIQUE,
    slot INTEGER NOT NULL,
    created_ts REAL NOT NULL,
    updated_ts REAL NOT NULL,
    state TEXT NOT NULL,
    condition_id TEXT NOT NULL,
    token_id TEXT NOT NULL,
    direction TEXT NOT NULL,
    quant_p REAL NOT NULL,
    jev_p REAL NOT NULL,
    ask REAL NOT NULL,
    edge REAL NOT NULL,
    planned_notional REAL NOT NULL,
    max_spend REAL NOT NULL,
    order_id TEXT,
    response_json TEXT,
    order_state_json TEXT,
    user_events_json TEXT,
    error_json TEXT
);
"""


def load_json_gz(path: Path) -> dict:
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        return json.load(handle)


def evaluate_live_gate(path: Path) -> dict:
    report = load_json_gz(path)
    arm = report.get("arms", {}).get("B_quant_jev_taker", {})
    checks = {
        "v81_report": str(report.get("format", "")).startswith("v8.1-early-window-value"),
        "settled_at_least_30": int(arm.get("settled", 0)) >= 30,
        "win_rate_over_65pct": (arm.get("win_rate") or 0) > .65,
        "positive_net_pnl": (arm.get("net_pnl_estimated") or 0) > 0,
        "positive_after_1tick_stress": (
            arm.get("stress_plus_1tick", {}).get("net_pnl_estimated") or 0
        ) > 0,
    }
    canary_ready = all(checks.values())
    session_ready = canary_ready and bool(arm.get("gate", {}).get("passed"))
    return {
        "canary_ready": canary_ready,
        "session_ready": session_ready,
        "checks": checks,
        "paper_b": {
            k: arm.get(k)
            for k in (
                "trades", "settled", "wins", "losses", "win_rate",
                "net_pnl_estimated", "net_roi_estimated",
                "net_pnl_minus_top3_positive_contributions",
            )
        },
    }


async def geoblock_check() -> dict:
    """Fail closed; never expose the returned IP in reports."""
    async with httpx.AsyncClient(timeout=5.0, follow_redirects=True) as client:
        response = await client.get("https://polymarket.com/api/geoblock")
        response.raise_for_status()
        payload = response.json()
    if not isinstance(payload, dict) or not isinstance(payload.get("blocked"), bool):
        raise ValueError("invalid_geoblock_response")
    return {
        "blocked": payload["blocked"],
        "country": str(payload.get("country") or "")[:8],
        "region": str(payload.get("region") or "")[:16],
    }


class LiveStore(EarlyStore):
    def __init__(self, path):
        super().__init__(path)
        self.conn.executescript(LIVE_SCHEMA)

    def live_rows(self) -> list[dict]:
        return [dict(r) for r in self.conn.execute(
            "SELECT * FROM v81_live_orders ORDER BY id"
        ).fetchall()]

    def unresolved_live_rows(self) -> list[dict]:
        terminal = ("filled", "no_fill", "rejected")
        return [r for r in self.live_rows() if r["state"] not in terminal]

    def reserve_live_intent(
        self, *, slug: str, condition_id: str, token_id: str, direction: str,
        quant_p: float, jev_p: float, ask: float, edge: float, notional: float,
    ) -> bool:
        now = time.time()
        with self.conn:
            cur = self.conn.execute(
                """INSERT OR IGNORE INTO v81_live_orders
                (slug,slot,created_ts,updated_ts,state,condition_id,token_id,direction,
                 quant_p,jev_p,ask,edge,planned_notional,max_spend)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    slug, LIVE_SLOT, now, now, "intent", condition_id, token_id,
                    direction, quant_p, jev_p, ask, edge, notional, MAX_ORDER_USD,
                ),
            )
        return cur.rowcount == 1

    def update_live(self, slug: str, state: str, **fields) -> None:
        allowed = {
            "order_id", "response_json", "order_state_json",
            "user_events_json", "error_json",
        }
        bad = set(fields) - allowed
        if bad:
            raise ValueError("invalid_live_update_fields")
        assignments = ["state=?", "updated_ts=?"]
        values: list[object] = [state, time.time()]
        for key, value in fields.items():
            assignments.append(f"{key}=?")
            values.append(value)
        values.append(slug)
        with self.conn:
            self.conn.execute(
                f"UPDATE v81_live_orders SET {','.join(assignments)} WHERE slug=?",
                values,
            )

    def live_attempt_count(self) -> int:
        return len(self.live_rows())

    def planned_notional_total(self) -> float:
        return sum(float(r["planned_notional"]) for r in self.live_rows())



def market_order_kwargs(token_id: str, decision: ValueDecision) -> dict:
    """Exact live BUY contract: FAK, no worse than refreshed ask, all-in spend <= USD 5."""
    if decision.notional_usd <= 0 or decision.notional_usd > MAX_ORDER_USD + 1e-9:
        raise ValueError("live_notional_exceeds_cap")
    return {
        "token_id": token_id,
        "side": "BUY",
        "amount": str(decision.notional_usd),
        "max_spend": str(MAX_ORDER_USD),
        "max_price": str(decision.ask),
        "order_type": "FAK",
    }


def token_for_direction(cand, direction: str) -> str:
    return cand.book.yes_token_id if direction == "UP" else cand.book.no_token_id


class LiveT120Runner(EarlyRunner):
    def __init__(self, *args, secure_client: AsyncSecureClient, live_mode: str, **kwargs):
        super().__init__(*args, **kwargs)
        self.secure_client = secure_client
        self.live_mode = live_mode
        self.live_halted = False
        self.live_done = asyncio.Event()

    async def _pre_submit_checks(self, slug: str, condition_id: str) -> tuple[bool, str]:
        try:
            geo = await geoblock_check()
        except Exception:
            return False, "geoblock_unverified"
        if geo["blocked"]:
            return False, "geoblocked"

        try:
            balance = await self.secure_client.get_balance_allowance(asset_type="COLLATERAL")
            raw = float(balance.balance)
            balance_usd = raw / 1e6
        except Exception:
            return False, "balance_check_failed"
        if balance_usd + 1e-9 < MAX_ORDER_USD:
            return False, "insufficient_collateral"

        try:
            positions = await self.secure_client.list_positions(
                user=str(self.secure_client.wallet), market=[condition_id], status="OPEN"
            ).first_page()
        except Exception:
            return False, "position_check_failed"
        if positions.items:
            return False, "existing_market_position"

        try:
            open_orders = await self.secure_client.list_open_orders(market=condition_id).first_page()
        except Exception:
            return False, "open_order_check_failed"
        if open_orders.items:
            return False, "existing_market_open_order"
        return True, "ok"

    async def _place_fak(
        self, *, slug: str, condition_id: str,
        direction: str, quant_p: float, view: JevView,
        decision: ValueDecision,
    ) -> None:
        store: LiveStore = self.store
        if self.live_halted:
            return
        if self.live_mode == "one" and store.live_attempt_count() >= 1:
            return
        if self.live_mode == "session":
            if store.live_attempt_count() >= MAX_SESSION_ORDERS:
                return
            if store.planned_notional_total() + decision.notional_usd > MAX_SESSION_NOTIONAL_USD + 1e-9:
                return

        ok, reason = await self._pre_submit_checks(slug, condition_id)
        if not ok:
            self.console.print(Panel(
                f"{slug}\n[bold red]REAL SKIP[/] · {reason}",
                title="[bold red] T120 LIVE GUARD [/]",
                border_style="red", expand=False,
            ))
            if reason in {"geoblocked", "geoblock_unverified"}:
                self.live_halted = True
            return

        order_box: dict[str, str] = {}
        user_events: list[dict] = []
        stop_stream, stream_ready = asyncio.Event(), asyncio.Event()
        stream_task = asyncio.create_task(
            read_user_stream(
                self.secure_client, condition_id, order_box,
                user_events, stop_stream, stream_ready,
            ),
            name="v81-live-user-stream",
        )
        try:
            try:
                await asyncio.wait_for(stream_ready.wait(), 2.0)
            except TimeoutError:
                self.console.print(Panel(
                    f"{slug}\n[bold yellow]REAL SKIP[/] · user_stream_not_ready",
                    title="[bold yellow] T120 LIVE PRE-SUBMIT [/]",
                    border_style="yellow", expand=False,
                ))
                return
            if stream_task.done():
                self.console.print(Panel(
                    f"{slug}\n[bold yellow]REAL SKIP[/] · user_stream_stopped",
                    title="[bold yellow] T120 LIVE PRE-SUBMIT [/]",
                    border_style="yellow", expand=False,
                ))
                return

            # Final public refresh after the private stream is ready.
            try:
                cand = await self.confirmation_read(slug)
                fresh = local_snapshot(cand, self.s, self.store)
                fresh_quant = quantitative_up_probability(fresh)
            except Exception as exc:
                self.console.print(Panel(
                    f"{slug}\n[bold yellow]REAL SKIP[/] · final_public_refresh_failed · "
                    f"{safe_error(exc)['classes'][0]}",
                    title="[bold yellow] T120 LIVE PRE-SUBMIT [/]",
                    border_style="yellow", expand=False,
                ))
                return

            fresh_direction = quant_direction(fresh_quant)
            if fresh_quant is None or fresh_direction != direction:
                self.console.print(Panel(
                    f"{slug}\n[bold yellow]REAL SKIP[/] · final_quant_invalidated",
                    title="[bold yellow] T120 LIVE PRE-SUBMIT [/]",
                    border_style="yellow", expand=False,
                ))
                return
            if not jev_quality(view, self.s):
                self.console.print(Panel(
                    f"{slug}\n[bold yellow]REAL SKIP[/] · final_jev_quality_failed",
                    title="[bold yellow] T120 LIVE PRE-SUBMIT [/]",
                    border_style="yellow", expand=False,
                ))
                return

            fresh_decision, fresh_reason = value_decision(
                fresh_quant, fresh_direction, cand.book, self.s, fee_rate=self.fee_rate
            )
            if fresh_decision is None:
                self.console.print(Panel(
                    f"{slug}\n[bold yellow]REAL SKIP[/] · final_value_invalidated:{fresh_reason}",
                    title="[bold yellow] T120 LIVE PRE-SUBMIT [/]",
                    border_style="yellow", expand=False,
                ))
                return

            fresh_token = token_for_direction(cand, direction)
            if not store.reserve_live_intent(
                slug=slug,
                condition_id=cand.condition_id,
                token_id=fresh_token,
                direction=direction,
                quant_p=fresh_quant,
                jev_p=view.p_yes,
                ask=fresh_decision.ask,
                edge=fresh_decision.edge,
                notional=fresh_decision.notional_usd,
            ):
                return
            store.update_live(slug, "placing")
            started = time.perf_counter_ns()
            try:
                response = await self.secure_client.place_market_order(
                    **market_order_kwargs(fresh_token, fresh_decision)
                )
            except Exception as exc:
                self.live_halted = True
                store.update_live(
                    slug, "unknown_after_submit",
                    error_json=json.dumps({
                        "elapsed_ms": (time.perf_counter_ns() - started) / 1e6,
                        "error": safe_error(exc),
                    }),
                )
                if self.live_mode == "one":
                    self.live_done.set()
                return

            safe = safe_response(response)
            order_id = str(response.order_id) if isinstance(response, AcceptedOrder) else None
            if order_id:
                order_box["id"] = order_id
            store.update_live(
                slug,
                "submitted" if isinstance(response, AcceptedOrder) else "rejected",
                order_id=order_id,
                response_json=json.dumps({
                    **safe,
                    "elapsed_ms": (time.perf_counter_ns() - started) / 1e6,
                    "max_price": fresh_decision.ask,
                    "max_spend": MAX_ORDER_USD,
                }),
            )
            if not isinstance(response, AcceptedOrder):
                if self.live_mode == "one":
                    self.live_done.set()
                return

            settle_error = None
            try:
                await self.secure_client.wait_for_order_fill_settlement(response, timeout_s=15)
            except Exception as exc:
                settle_error = safe_error(exc)

            await asyncio.sleep(.5)
            state = None
            try:
                state = await fetch_order_state(self.secure_client, order_id)
            except Exception as exc:
                settle_error = settle_error or safe_error(exc)

            await asyncio.sleep(.75)
            matched = float((state or {}).get("size_matched") or 0)
            immediate_trade_ids = [str(x) for x in (response.trade_ids or ())]
            stream_fill = any(row.get("type") == "trade" for row in user_events)
            final_state = "filled" if matched > 0 or immediate_trade_ids or stream_fill else "no_fill"
            store.update_live(
                slug,
                final_state,
                order_state_json=json.dumps(state) if state else None,
                user_events_json=json.dumps({
                    "events": user_events,
                    "immediate_trade_ids": immediate_trade_ids,
                }),
                error_json=json.dumps(settle_error) if settle_error else None,
            )
            self.console.print(Panel(
                f"{slug}\n"
                f"方向={direction} | Quant={fresh_quant:.3f} | Jev={view.p_yes:.3f}\n"
                f"ASK上限={fresh_decision.ask:.3f} | max spend=USD {MAX_ORDER_USD:.2f}\n"
                f"matched={matched} | state={final_state}",
                title="[bold green] REAL T120 QUANT+JEV+ASK [/]",
                border_style="green", expand=False,
            ))
            if self.live_mode == "one":
                self.live_done.set()
        finally:
            stop_stream.set()
            stream_task.cancel()
            await asyncio.gather(stream_task, return_exceptions=True)

    async def after_b_value_decision(
        self, *, observation_id: int, slug: str, slot: int, original_direction: str,
        view: JevView, fresh_quant: float, cand, fresh, decision, reason: str,
    ) -> None:
        del observation_id, fresh, reason
        if slot != LIVE_SLOT or decision is None:
            return
        if self.live_halted:
            return
        await self._place_fak(
            slug=slug,
            condition_id=cand.condition_id,
            direction=original_direction,
            quant_p=fresh_quant,
            view=view,
            decision=decision,
        )


def build_live_report(store: LiveStore, paper_gate: dict, mode: str, geoblock: dict, preflight: dict) -> dict:
    paper = build_paper_report(store, PAPER_REVISION)
    rows = store.live_rows()
    return {
        "format": REVISION,
        "paper_only": False,
        "real_order_logic": "T-120 only; B Quant+Jev confirmation; final refreshed ask; FAK BUY.",
        "max_order_usd": MAX_ORDER_USD,
        "max_session_orders": 1 if mode == "one" else MAX_SESSION_ORDERS,
        "max_session_notional_usd": MAX_ORDER_USD if mode == "one" else MAX_SESSION_NOTIONAL_USD,
        "paper_gate": paper_gate,
        "geoblock": geoblock,
        "preflight": preflight,
        "paper_observation_report": paper,
        "live_orders": [
            {
                k: row.get(k) for k in (
                    "slug", "slot", "created_ts", "updated_ts", "state",
                    "direction", "quant_p", "jev_p", "ask", "edge",
                    "planned_notional", "max_spend", "order_id",
                    "response_json", "order_state_json", "user_events_json",
                    "error_json",
                )
            }
            for row in rows
        ],
        "limitations": [
            "Real mode only opens a position when current geoblock says trading is permitted.",
            "T-110/T-100 are paper-only and can never call the live placement path.",
            "A/C paper arms can never call the live placement path.",
            "FAK max_price prevents fills above the final refreshed ask; max_spend is capped at USD 5.",
            "A placement exception is treated as uncertain and halts additional live trading.",
        ],
    }


async def run(
    *, seconds: int, db: Path, secure: AsyncSecureClient,
    settings, mode: str, paper_gate: dict, geoblock: dict, preflight: dict,
) -> dict:
    store = LiveStore(db)
    if store.unresolved_live_rows():
        raise ValueError("unresolved_prior_live_intent")
    tasks = []
    console = Console()
    try:
        # Reuse the exact V8.1 manifest so paper observations remain directly comparable.
        from .price_value_early_forward import experiment_parameters
        store.ensure_experiment(PAPER_REVISION, experiment_parameters(settings, INTERVAL_SECONDS, CRYPTO_TAKER_FEE_RATE))

        async with AsyncExitStack() as stack:
            public = await stack.enter_async_context(AsyncPublicClient())
            confirm_public = await stack.enter_async_context(AsyncPublicClient())
            settlement = await stack.enter_async_context(AsyncPublicClient())
            jev = await stack.enter_async_context(JevClient(
                settings.typesafe_api_key, model=settings.jev_model,
                base_url=settings.typesafe_base_url,
                timeout=settings.jev_timeout_seconds,
                max_retries=settings.jev_max_retries,
            ))
            from .price_value_forward import V8JevWorker
            shadow = V8JevWorker(jev, store, console)
            runner = LiveT120Runner(
                settings, store, public, shadow, console,
                version=PAPER_REVISION, session=uuid.uuid4().hex,
                fee_rate=CRYPTO_TAKER_FEE_RATE,
                confirm_public=confirm_public,
                secure_client=secure,
                live_mode=mode,
            )
            shadow.on_result = runner.on_jev_result
            tasks = [
                asyncio.create_task(watch_chainlink_anchors(settings, store), name="chainlink"),
                asyncio.create_task(shadow.work(), name="jev-live-t120"),
                asyncio.create_task(
                    settlement_worker(settlement, settings, store, PAPER_REVISION),
                    name="settlement",
                ),
            ]
            console.print(Panel(
                "[bold]REAL-MONEY CANARY[/]\n"
                "真钱路径：[bold cyan]T-120 ONLY[/]\n"
                "条件：[bold]Quant + Jev + response-time ASK[/]\n"
                "T-110/T-100：只记录，不下单\n"
                f"单笔总支出硬上限：USD {MAX_ORDER_USD:.2f}\n"
                f"模式：{mode}",
                title=f"[bold red] {REVISION} [/]",
                border_style="red", expand=False,
            ))

            clock = asyncio.get_running_loop().time
            deadline = clock()
            stop_at = clock() + seconds
            next_board = clock()
            while clock() < stop_at:
                for task in tasks:
                    if task.done():
                        task.result()
                        raise RuntimeError(f"task_stopped:{task.get_name()}")
                await runner.tick(budget_seconds=min(8.0, INTERVAL_SECONDS * .8))
                if clock() >= next_board:
                    paper = build_paper_report(store, PAPER_REVISION)
                    print_scoreboard(console, paper)
                    print_rejections(console, paper)
                    console.print(
                        f"[bold red]REAL attempts={store.live_attempt_count()} "
                        f"planned_notional=USD {store.planned_notional_total():.2f}[/]"
                    )
                    next_board = clock() + 60
                if mode == "one" and runner.live_done.is_set():
                    break
                deadline, _ = next_deadline(deadline, clock(), INTERVAL_SECONDS)
                await asyncio.sleep(max(0.0, min(deadline, stop_at) - clock()))
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        while "shadow" in locals() and not shadow.queue.empty():
            observation_id, *_ = shadow.queue.get_nowait()
            store.mark_jev(observation_id, "interrupted")
            shadow.queue.task_done()
        report = build_live_report(store, paper_gate, mode, geoblock, preflight)
        store.close()
    return report


async def async_main(args) -> dict:
    gate = evaluate_live_gate(args.paper_report)
    if args.mode == "one" and not gate["canary_ready"]:
        return {"format": REVISION, "termination": "paper_canary_gate_failed", "paper_gate": gate}
    if args.mode == "session" and not gate["session_ready"]:
        return {"format": REVISION, "termination": "paper_session_gate_failed", "paper_gate": gate}

    try:
        geo = await geoblock_check()
    except Exception as exc:
        return {
            "format": REVISION,
            "termination": "geoblock_unverified",
            "geoblock_error": safe_error(exc),
            "paper_gate": gate,
        }
    if geo["blocked"]:
        return {
            "format": REVISION,
            "termination": "geoblocked",
            "geoblock": geo,
            "paper_gate": gate,
        }

    raw = load_settings()
    if not _valid_private_key(raw.polymarket_private_key):
        return {"format": REVISION, "termination": "private_key_missing_or_invalid", "geoblock": geo, "paper_gate": gate}

    try:
        secure = await AsyncSecureClient.create(
            private_key=raw.polymarket_private_key,
            wallet=raw.polymarket_wallet or None,
        )
    except Exception as exc:
        return {"format": REVISION, "termination": "client_create_failed", "error": safe_error(exc), "geoblock": geo, "paper_gate": gate}

    try:
        try:
            preflight = await account_preflight(secure)
        except Exception as exc:
            return {"format": REVISION, "termination": "preflight_failed", "error": safe_error(exc), "geoblock": geo, "paper_gate": gate}
        if preflight["open_orders_present"]:
            return {"format": REVISION, "termination": "existing_open_orders", "preflight": preflight, "geoblock": geo, "paper_gate": gate}
        if not preflight["trading_approved"]:
            return {"format": REVISION, "termination": "trading_not_approved", "preflight": preflight, "geoblock": geo, "paper_gate": gate}
        if preflight["balance_usd"] is None or preflight["balance_usd"] < MAX_ORDER_USD:
            return {"format": REVISION, "termination": "insufficient_balance", "preflight": preflight, "geoblock": geo, "paper_gate": gate}

        if args.check_only:
            return {"format": REVISION, "termination": "check_only_complete", "preflight": preflight, "geoblock": geo, "paper_gate": gate}

        settings = fast_settings().model_copy(update={"db_path": str(args.db), "strategy_version": PAPER_REVISION})
        return await run(
            seconds=args.seconds,
            db=args.db,
            secure=secure,
            settings=settings,
            mode=args.mode,
            paper_gate=gate,
            geoblock=geo,
            preflight=preflight,
        )
    finally:
        await secure.close()



def resolve_live_paths(
    db_arg: Path | None, resume_db: Path | None, out_arg: Path | None, stamp: str,
) -> tuple[Path, Path, bool]:
    if db_arg is not None and resume_db is not None:
        raise ValueError("cannot_use_db_and_resume_db_together")
    if resume_db is not None:
        db = Path(resume_db)
        if not db.is_file():
            raise ValueError("resume_database_not_found")
        out = run_output_path(out_arg, f"v81_live_t120_resume_{stamp}.json.gz")
        if out.exists():
            raise FileExistsError("report_already_exists")
        return db, out, True
    db = run_output_path(db_arg, f"jevymarket.v81-live-t120_{stamp}.db")
    out = run_output_path(out_arg, f"v81_live_t120_{stamp}.json.gz")
    if db.exists() or out.exists():
        raise FileExistsError("live_output_already_exists")
    return db, out, False


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="V8.1 T-120 Quant+Jev+ASK真钱canary；T-110/T-100只记录"
    )
    modes = parser.add_mutually_exclusive_group(required=True)
    modes.add_argument("--check-only", action="store_true")
    modes.add_argument("--live-one", action="store_true")
    modes.add_argument("--live-session", action="store_true")
    parser.add_argument("--confirm", default="")
    parser.add_argument("--paper-report", type=Path, required=True)
    parser.add_argument("--seconds", type=int, default=DEFAULT_SESSION_SECONDS)
    paths = parser.add_mutually_exclusive_group()
    paths.add_argument("--db", type=Path)
    paths.add_argument("--resume-db", type=Path)
    parser.add_argument("--out", type=Path)
    args = parser.parse_args(argv)

    if not 300 <= args.seconds <= 86_400:
        parser.error("seconds必须在300到86400之间")

    if args.live_one:
        if args.confirm != CONFIRM_ONE:
            parser.error(f"--live-one必须显式添加 --confirm {CONFIRM_ONE}")
        args.mode = "one"
    elif args.live_session:
        if args.confirm != CONFIRM_SESSION:
            parser.error(f"--live-session必须显式添加 --confirm {CONFIRM_SESSION}")
        args.mode = "session"
    else:
        args.mode = "check"

    stamp = datetime.now(timezone(timedelta(hours=8))).strftime("%Y%m%d_%H%M%S_%f")
    try:
        args.db, out, resumed = resolve_live_paths(args.db, args.resume_db, args.out, stamp)
    except (ValueError, FileExistsError) as exc:
        parser.error(str(exc))

    if args.check_only:
        result = asyncio.run(async_main(args))
    else:
        with single_instance(str(args.db)):
            result = asyncio.run(async_main(args))
    result["resumed_existing_database"] = resumed
    with gzip.open(out, "xt", encoding="utf-8") as handle:
        json.dump(result, handle, ensure_ascii=False, allow_nan=False, separators=(",", ":"))
    print(json.dumps({
        k: result.get(k)
        for k in ("format", "termination", "geoblock", "preflight", "paper_gate")
    }, ensure_ascii=False, indent=2))
    print(f"已导出 → {out.resolve()}", flush=True)


if __name__ == "__main__":
    main()
