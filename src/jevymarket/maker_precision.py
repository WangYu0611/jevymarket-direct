"""High-resolution reaction measurement and bounded HTTP failure summaries."""
from __future__ import annotations

import time

MEASUREMENT_REVISION = "v6-perf-counter-r1"
HTTP_STAGES = frozenset({"metadata", "server_clock", "settlement"})
SAFE_CLASSES = frozenset({
    "SSLCertVerificationError", "SSLError", "gaierror", "ConnectionRefusedError",
    "ConnectionResetError", "TimeoutError", "ConnectTimeout", "ReadTimeout",
    "WriteTimeout", "PoolTimeout", "ProxyError", "ConnectError", "ReadError",
    "WriteError", "RemoteProtocolError", "LocalProtocolError", "HTTPStatusError",
    "InvalidURL", "OSError", "ValueError", "RuntimeError", "other_exception",
})


def clock_receipt(clock=time):
    return {"revision": MEASUREMENT_REVISION, "clock": "perf_counter_ns",
            "resolution_seconds": clock.get_clock_info("perf_counter").resolution,
            "lifecycle_clock": "monotonic",
            "lifecycle_resolution_seconds": clock.get_clock_info("monotonic").resolution,
            "histogram_quantum_ms": .1,
            "scope": "Coalesced local signal-to-action; watchdog compute separate; not WS/network/exchange latency"}


def run_reaction(runtime, wall, mono, *, clock=time):
    """Keep clock domains separate: only reaction measurement uses perf ns.

    Order/cache lifecycle calls still receive the original monotonic seconds.
    The existing 100ms budget is measured more precisely, not relaxed. The first
    coalesced signal is consumed once; no-order and watchdog evaluations remain.
    """
    start = clock.perf_counter_ns()
    triggered = runtime.wake_at_ns
    runtime.wake_at_ns = None
    origin = start if triggered is None else triggered

    def elapsed_seconds(stamp):
        if stamp < origin:
            runtime.engine.cancel("measurement_clock_invalid", wall, mono)
            raise RuntimeError("measurement_clock_invalid")
        return (stamp - origin) / 1e9

    lag = elapsed_seconds(start)
    budget = runtime.c.reaction_budget_seconds
    if lag > budget:
        runtime.engine.cancel("reaction_budget_missed", wall, mono)
        runtime.store.emit("reaction", {"elapsed_ms": lag * 1000, "over_budget": True})
    else:
        runtime.react(wall, mono)
        elapsed = elapsed_seconds(clock.perf_counter_ns())
        if triggered is not None and runtime.engine.active():
            runtime.store.emit("reaction", {"elapsed_ms": elapsed * 1000,
                                           "over_budget": elapsed > budget})
        if elapsed > budget:
            runtime.engine.cancel("compute_budget_missed", clock.time(), clock.monotonic())
    measured_ms = elapsed_seconds(clock.perf_counter_ns()) * 1000
    runtime.reaction_window.add(measured_ms, triggered is not None)


class TransportFailures:
    """Bounded, re-whitelisted summaries; old absent causes stay unrecorded."""
    def __init__(self):
        self.total = 0
        self.without_detail = 0
        self.omitted_group_events = 0
        self.groups = {}

    def add(self, row):
        stage = row.get("stage")
        if stage not in HTTP_STAGES:
            return
        self.total += 1
        detail = row.get("transport_detail")
        if not isinstance(detail, dict) or not isinstance(detail.get("classes"), list):
            self.without_detail += 1
            return
        labels = tuple(x if isinstance(x, str) and x in SAFE_CLASSES else "other_exception"
                       for x in detail["classes"][:12])

        def codes(name):
            values = detail.get(name)
            if not isinstance(values, list):
                return ()
            return tuple(sorted({x for x in values[:12] if type(x) is int and abs(x) <= 2**31}))

        key = (stage, labels, codes("errno_codes"), codes("winerror_codes"),
               detail.get("chain_truncated") is True or len(detail["classes"]) > 12)
        if key not in self.groups and len(self.groups) >= 16:
            self.omitted_group_events += 1
            return
        self.groups[key] = self.groups.get(key, 0) + 1

    def report(self):
        return {"events": self.total, "without_recorded_detail": self.without_detail,
                "omitted_group_events": self.omitted_group_events,
                "groups": [{"stage": k[0], "classes": list(k[1]), "errno_codes": list(k[2]),
                            "winerror_codes": list(k[3]), "chain_truncated": k[4], "events": count}
                           for k, count in self.groups.items()],
                "scope": "Recorded HTTP-stage failures in latest run; no old cause reconstruction or success-rate denominator"}
