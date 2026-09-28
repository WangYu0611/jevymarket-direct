"""Runtime adapter for the V8.3 tiered candidate filter.

The existing authenticated V8.2 order path is reused unchanged. This adapter
only changes candidate selection, fallback sizing, T-110 observation routing,
and report metadata.
"""
from __future__ import annotations

import asyncio
import json
import time
from contextvars import ContextVar

from . import price_value_live_t120 as v82live
from .fast_runner import current_slug
from .fast_strategy import local_snapshot
from .price_value_early_forward import EarlyRunner
from .price_value_forward import (
    CRYPTO_TAKER_FEE_RATE,
    ValueDecision,
    arm_metrics,
    quant_direction,
    value_decision as original_value_decision,
)
from .price_value_t120_main import V82Runner, build_report as build_paper_report
from .signal import quantitative_up_probability
from .v83_candidate_filter import (
    CandidateTier,
    classify_candidate,
    minimum_share_decision,
)

REVISION = "v8.3-tiered-live-r1"
PAPER_REVISION = "v8.3-tiered-observation-r1"
MAX_ORDERS = 5
MAX_SESSION_NOTIONAL_USD = 25.0
RECHECK_DELAYS = (0.15, 0.25, 0.35)
RECHECK_REASONS = {"incomplete_side_book", "ask_outside_price_band"}

V83_META_SCHEMA = """
CREATE TABLE IF NOT EXISTS v83_candidate_meta (
    slug TEXT PRIMARY KEY,
    slot INTEGER NOT NULL,
    preliminary_tier TEXT NOT NULL,
    final_tier TEXT,
    quant_side REAL NOT NULL,
    jev_side REAL NOT NULL,
    preliminary_ask REAL NOT NULL,
    preliminary_edge REAL NOT NULL,
    created_ts REAL NOT NULL,
    updated_ts REAL NOT NULL
);
"""


class V83LiveStore(v82live.LiveStore):
    def __init__(self, path):
        super().__init__(path)
        self.active_slot = 120
        self.conn.executescript(V83_META_SCHEMA)

    def record_live_event(
        self, *, slug: str, stage: str, outcome: str, reason: str,
    ) -> None:
        with self.conn:
            self.conn.execute(
                """INSERT INTO v82_live_events(slug,slot,ts,stage,outcome,reason)
                   VALUES (?,?,?,?,?,?)""",
                (slug, self.active_slot, time.time(), stage, outcome, reason[:160]),
            )

    def reserve_live_intent(
        self, *, slug: str, condition_id: str, token_id: str, direction: str,
        quant_p: float, jev_p: float, ask: float, edge: float, notional: float,
    ) -> bool:
        now = time.time()
        with self.conn:
            cur = self.conn.execute(
                """INSERT OR IGNORE INTO v82_live_orders
                (slug,slot,created_ts,updated_ts,state,condition_id,token_id,direction,
                 quant_p,jev_p,ask,edge,planned_notional,max_spend)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    slug, self.active_slot, now, now, "intent", condition_id,
                    token_id, direction, quant_p, jev_p, ask, edge, notional,
                    v82live.MAX_ORDER_USD,
                ),
            )
        return cur.rowcount == 1

    def set_candidate_meta(
        self, *, slug: str, slot: int, tier: CandidateTier,
        ask: float, edge: float,
    ) -> None:
        now = time.time()
        with self.conn:
            self.conn.execute(
                """INSERT INTO v83_candidate_meta
                (slug,slot,preliminary_tier,final_tier,quant_side,jev_side,
                 preliminary_ask,preliminary_edge,created_ts,updated_ts)
                VALUES (?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(slug) DO UPDATE SET
                    slot=excluded.slot,
                    preliminary_tier=excluded.preliminary_tier,
                    quant_side=excluded.quant_side,
                    jev_side=excluded.jev_side,
                    preliminary_ask=excluded.preliminary_ask,
                    preliminary_edge=excluded.preliminary_edge,
                    updated_ts=excluded.updated_ts""",
                (
                    slug, slot, tier.name, None, tier.quant_side, tier.jev_side,
                    ask, edge, now, now,
                ),
            )

    def set_final_tier(self, slug: str, tier: str | None) -> None:
        with self.conn:
            self.conn.execute(
                "UPDATE v83_candidate_meta SET final_tier=?,updated_ts=? WHERE slug=?",
                (tier, time.time(), slug),
            )

    def candidate_meta(self) -> list[dict]:
        return [dict(row) for row in self.conn.execute(
            "SELECT * FROM v83_candidate_meta ORDER BY created_ts"
        ).fetchall()]


_FINAL_CONTEXT: ContextVar[dict | None] = ContextVar("v83_final_context", default=None)


def final_value_decision(
    p_up, direction, book, settings, *, fee_rate=CRYPTO_TAKER_FEE_RATE,
):
    decision, reason = original_value_decision(
        p_up, direction, book, settings, fee_rate=fee_rate
    )
    context = _FINAL_CONTEXT.get()
    if decision is None or context is None:
        return decision, reason

    tier = classify_candidate(
        slot=context["slot"],
        direction=direction,
        quant_p=p_up,
        jev_p=context["jev_p"],
        decision=decision,
    )
    if not tier.accepted:
        context["final_tier"] = None
        context["final_reason"] = tier.reason
        return None, "v83_final_gate:" + tier.reason

    use_minimum = bool(context["preliminary_minimum"] or tier.minimum_shares)
    if use_minimum:
        try:
            decision = minimum_share_decision(
                decision,
                hard_cap_usd=v82live.MAX_ORDER_USD,
                fee_rate=fee_rate,
            )
        except ValueError as exc:
            context["final_tier"] = None
            context["final_reason"] = str(exc)
            return None, "v83_final_sizing:" + str(exc)

    context["final_tier"] = tier.name
    context["final_reason"] = "value_ready"
    return decision, "value_ready"


class V83Runner(v82live.LiveT120Runner):
    async def tick(self, *, budget_seconds: float = 8) -> None:
        slug = current_slug()
        market_start = int(slug.rsplit("-", 1)[1])
        approximate_left = int(market_start + 300 - time.time())
        if 100 < approximate_left <= 110:
            await EarlyRunner.tick(self, budget_seconds=budget_seconds)
            return
        await V82Runner.tick(self, budget_seconds=budget_seconds)

    async def _recover_value_candidate(
        self, *, slug: str, original_direction: str, reason: str,
    ):
        if reason not in RECHECK_REASONS:
            return None
        last_reason = reason
        for delay in RECHECK_DELAYS:
            await asyncio.sleep(delay)
            try:
                cand = await self.confirmation_read(slug)
                fresh = local_snapshot(cand, self.s, self.store)
                fresh_quant = quantitative_up_probability(fresh)
                direction = quant_direction(fresh_quant)
                if fresh_quant is None or direction != original_direction:
                    return None
                decision, last_reason = original_value_decision(
                    fresh_quant,
                    direction,
                    cand.book,
                    self.s,
                    fee_rate=self.fee_rate,
                )
                if decision is not None:
                    return cand, fresh, fresh_quant, decision
                if last_reason not in RECHECK_REASONS:
                    return None
            except Exception:
                continue
        return None

    async def after_b_value_decision(
        self, *, observation_id: int, slug: str, slot: int,
        original_direction: str, view, fresh_quant: float, cand, fresh,
        decision: ValueDecision | None, reason: str,
    ) -> None:
        del observation_id
        if slot not in {120, 110} or self.live_halted:
            return

        store: V83LiveStore = self.store
        store.active_slot = slot

        if decision is None:
            recovered = await self._recover_value_candidate(
                slug=slug,
                original_direction=original_direction,
                reason=reason,
            )
            if recovered is None:
                store.record_live_event(
                    slug=slug,
                    stage="v83_gate",
                    outcome="skip",
                    reason="paper_value_rejected:" + reason,
                )
                return
            cand, fresh, fresh_quant, decision = recovered

        tier = classify_candidate(
            slot=slot,
            direction=original_direction,
            quant_p=fresh_quant,
            jev_p=view.p_yes,
            decision=decision,
        )
        if not tier.accepted:
            store.record_live_event(
                slug=slug,
                stage="v83_gate",
                outcome="skip",
                reason=tier.reason,
            )
            return

        store.set_candidate_meta(
            slug=slug,
            slot=slot,
            tier=tier,
            ask=decision.ask,
            edge=decision.edge,
        )
        store.record_live_event(
            slug=slug,
            stage="v83_gate",
            outcome="pass",
            reason=tier.name,
        )

        context = {
            "slot": slot,
            "jev_p": view.p_yes,
            "preliminary_minimum": tier.minimum_shares,
            "preliminary_tier": tier.name,
            "final_tier": None,
            "final_reason": None,
        }
        token = _FINAL_CONTEXT.set(context)
        try:
            await self._place_fak(
                slug=slug,
                condition_id=cand.condition_id,
                direction=original_direction,
                quant_p=fresh_quant,
                view=view,
                decision=decision,
            )
        finally:
            _FINAL_CONTEXT.reset(token)
            store.set_final_tier(slug, context["final_tier"])

        if store.live_attempt_count() >= MAX_ORDERS:
            self.live_halted = True


def build_live_report(
    store: V83LiveStore, paper_gate: dict, mode: str,
    geoblock: dict, preflight: dict,
) -> dict:
    paper = build_paper_report(store, PAPER_REVISION)
    t110_rows = store.slot_trades(PAPER_REVISION, "B_quant_jev_taker", 110)
    paper["shadow_research"]["t110_retired"] = False
    paper["shadow_research"]["metrics"]["t110_v83_observed_b"] = arm_metrics(t110_rows)

    events = store.live_events()
    rows = store.live_rows()
    meta = {row["slug"]: row for row in store.candidate_meta()}
    event_summary: dict[str, int] = {}
    for event in events:
        key = f"{event['stage']}:{event['outcome']}:{event['reason']}"
        event_summary[key] = event_summary.get(key, 0) + 1

    live_orders = []
    for row in rows:
        item = {
            key: row.get(key) for key in (
                "slug", "slot", "created_ts", "updated_ts", "state",
                "direction", "quant_p", "jev_p", "ask", "edge",
                "planned_notional", "max_spend", "order_id",
                "response_json", "order_state_json", "user_events_json",
                "error_json",
            )
        }
        item["v83_meta"] = meta.get(row["slug"])
        live_orders.append(item)

    return {
        "format": REVISION,
        "paper_only": False,
        "real_order_logic": (
            "T-120 strong-Jev or market-extreme tier; strict T-110 fallback; "
            "final refreshed ASK; FAK BUY."
        ),
        "max_order_usd": v82live.MAX_ORDER_USD,
        "max_session_orders": 1 if mode == "one" else MAX_ORDERS,
        "max_session_notional_usd": (
            v82live.MAX_ORDER_USD if mode == "one" else MAX_SESSION_NOTIONAL_USD
        ),
        "paper_gate": paper_gate,
        "sdk_contract": v82live.live_sdk_contract(),
        "geoblock": geoblock,
        "preflight": preflight,
        "paper_observation_report": paper,
        "live_event_summary": event_summary,
        "live_events": [
            {key: event.get(key) for key in (
                "slug", "slot", "ts", "stage", "outcome", "reason"
            )}
            for event in events
        ],
        "candidate_meta": list(meta.values()),
        "live_orders": live_orders,
        "limitations": [
            "V8.3 thresholds are retrospective and require forward validation.",
            "T-110 evidence is small; fallback orders use minimum exchange shares.",
            "A placement exception remains fail-closed and halts later orders.",
            "No filter can guarantee a winning trade.",
        ],
    }
