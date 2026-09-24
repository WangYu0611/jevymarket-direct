"""Validated public CLOB L2 cache. Public trades are evidence, not private fills."""
from __future__ import annotations

from dataclasses import dataclass, field, replace

from .maker_model import finite, source_time


class BookGap(ValueError):
    pass


@dataclass
class TokenBook:
    token: str
    tick: float
    minimum: float
    bids: dict[float, float] = field(default_factory=dict)
    asks: dict[float, float] = field(default_factory=dict)
    ts: float = 0.0
    received_mono: float = 0.0
    ready: bool = False
    ever_snapshot: bool = False

    @property
    def bid(self):
        return max(self.bids, default=None)

    @property
    def ask(self):
        return min(self.asks, default=None)

    def fresh(self, now: float, mono: float, age: float) -> bool:
        return (self.ready and 0 <= now - self.ts <= age
                and 0 <= mono - self.received_mono <= age
                and self.bid is not None and self.ask is not None and self.bid < self.ask)

    def ahead(self, price: float) -> float:
        return sum(q for p, q in self.bids.items() if p >= price)

    def diagnostic(self, wall: float, mono: float, age: float) -> dict:
        bid, ask = self.bid, self.ask
        shape = ("unknown" if not self.ready else "two_sided" if bid is not None and ask is not None
                 else "bid_only" if bid is not None else "ask_only" if ask is not None else "empty")
        source_age = wall - self.ts if self.ready else None
        receive_age = mono - self.received_mono if self.ready else None
        snapshot = "ready" if self.ready else "invalidated" if self.ever_snapshot else "awaiting_snapshot"
        timely = bool(self.ready and 0 <= source_age <= age and 0 <= receive_age <= age)
        if not self.ready:
            status = snapshot
        elif source_age < 0:
            status = "future_source"
        elif receive_age < 0:
            status = "receive_clock_invalid"
        elif source_age > age:
            status = "stale_source"
        elif receive_age > age:
            status = "stale_receive"
        elif shape != "two_sided":
            status = "valid_" + shape
        elif bid >= ask:
            status = "crossed"
        else:
            status = "ready"
        return {"snapshot_state": snapshot, "shape": shape, "status": status,
                "source_age_seconds": source_age, "receive_age_seconds": receive_age,
                "timely": timely, "trade_eligible": self.fresh(wall, mono, age),
                "bid": bid, "ask": ask, "bid_levels": len(self.bids), "ask_levels": len(self.asks)}


class BookCache:
    def __init__(self, condition: str, specs: dict[str, tuple[float, float]]):
        self.condition = condition
        self.books = {t: TokenBook(t, *s) for t, s in specs.items()}
        self.generation = 0
        self.last_failure: dict | None = None

    def invalidate(self):
        self.generation += 1
        for book in self.books.values():
            book.ready = False
            book.bids.clear()
            book.asks.clear()
            # A new subscription requires its own full snapshots. Do not apply
            # the discarded generation's ordering watermark to that baseline.
            # Within a generation, source-clock order remains strictly checked.
            book.ts = 0.0
            book.received_mono = 0.0

    def replay_state(self, books=None) -> dict:
        books = self.books if books is None else books
        return {"condition": self.condition, "generation": self.generation,
                "books": {t: {"tick": b.tick, "minimum": b.minimum, "ts": b.ts,
                              "received_mono": b.received_mono, "ready": b.ready,
                              "bids": [[p, q] for p, q in b.bids.items()],
                              "asks": [[p, q] for p, q in b.asks.items()]}
                          for t, b in books.items()}}

    @staticmethod
    def _level(price, size):
        p, q = finite(price), finite(size)
        if not 0 < p < 1 or q < 0:
            raise BookGap("invalid_level")
        return p, q

    def apply(self, msg: dict, wall: float, mono: float) -> None:
        """Stage an event; commit only after all checks. Capture rejects first.

        BBO semantics/age guards deliberately remain unchanged until real reject
        frames support a different interpretation. Diagnostics cannot authorize
        trading on partial, malformed, stale, or out-of-order data.
        """
        kind = msg.get("event_type")
        if kind not in {"book", "price_change", "tick_size_change"}:
            return
        if msg.get("market") != self.condition:
            return
        self.last_failure = None
        staged = {}

        def mutable(token):
            original = self.books.get(str(token))
            if original is None:
                return None
            if original.token not in staged:
                staged[original.token] = replace(original, bids=original.bids.copy(), asks=original.asks.copy())
            return staged[original.token]

        try:
            ts = source_time(msg.get("timestamp"))
            if ts > wall or wall - ts > 5:
                raise BookGap("stale_or_future_book_message")
            touched = []
            if kind == "book":
                b = mutable(msg.get("asset_id"))
                if b is None:
                    return
                if ts < b.ts:
                    raise BookGap("out_of_order_snapshot")
                bids, asks = {}, {}
                for label, dest in (("bids", bids), ("asks", asks)):
                    if not isinstance(msg[label], list):
                        raise BookGap("invalid_book_shape")
                    for row in msg[label]:
                        if not isinstance(row, dict):
                            raise BookGap("invalid_book_shape")
                        p, q = self._level(row["price"], row["size"])
                        if q:
                            dest[p] = q
                b.bids, b.asks, b.ready, b.ever_snapshot = bids, asks, True, True
                touched = [b]
            elif kind == "price_change":
                if not isinstance(msg["price_changes"], list):
                    raise BookGap("invalid_book_shape")
                for row in msg["price_changes"]:
                    if not isinstance(row, dict):
                        raise BookGap("invalid_book_shape")
                    b = mutable(row.get("asset_id"))
                    if b is None:
                        continue
                    if not b.ready or ts < b.ts:
                        raise BookGap("delta_without_snapshot_or_out_of_order")
                    if row.get("side") not in {"BUY", "SELL"}:
                        raise BookGap("unknown_book_side")
                    p, q = self._level(row["price"], row["size"])
                    levels = b.bids if row["side"] == "BUY" else b.asks
                    if q:
                        levels[p] = q
                    else:
                        levels.pop(p, None)
                    touched.append(b)
                for row in msg["price_changes"]:
                    b = staged.get(str(row.get("asset_id")))
                    if b is None:
                        continue
                    for label, actual in (("best_bid", b.bid), ("best_ask", b.ask)):
                        if label in row:
                            expected = finite(row[label])
                            # CLOB may represent an empty side as 0 or 1.
                            if actual is None and expected in (0, 1):
                                continue
                            if actual is None or abs(actual - expected) > 1e-8:
                                raise BookGap("bbo_delta_mismatch")
            else:
                b = mutable(msg.get("asset_id"))
                if b is None:
                    return
                tick = finite(msg["new_tick_size"])
                if not 0 < tick < 1:
                    raise BookGap("invalid_tick")
                b.tick = tick
                raise BookGap("tick_changed_resubscribe")
            for b in touched:
                if b.bid is not None and b.ask is not None and b.bid >= b.ask:
                    raise BookGap("crossed_book")
                b.ts, b.received_mono = ts, mono
            for token, b in staged.items():
                # Preserve object identity for code holding a TokenBook reference.
                original = self.books[token]
                original.bids, original.asks = b.bids, b.asks
                original.ready, original.ever_snapshot = b.ready, b.ever_snapshot
                original.ts, original.received_mono = b.ts, b.received_mono
        except (KeyError, TypeError, ValueError) as exc:
            self.last_failure = {"before": self.replay_state(), "candidate": self.replay_state(staged)}
            # A valid tick notice must survive resubscription; no old price levels do.
            if isinstance(exc, BookGap) and exc.args == ("tick_changed_resubscribe",):
                for token, b in staged.items():
                    self.books[token].tick = b.tick
            self.invalidate()
            raise BookGap("invalid_or_inconsistent_book") from exc


def trade_message(msg: dict, condition: str, wall: float) -> dict | None:
    if msg.get("event_type") != "last_trade_price" or msg.get("market") != condition:
        return None
    ts, price, size = source_time(msg.get("timestamp")), finite(msg.get("price")), finite(msg.get("size"))
    if not 0 <= wall - ts <= 1 or not 0 < price < 1 or size <= 0 or msg.get("side") not in {"BUY", "SELL"}:
        return None
    token = str(msg.get("asset_id"))
    # Tuple dedupe can undercount identical executions; never count them twice.
    key = "|".join(map(str, (msg.get("transaction_hash", ""), token, ts, price, size, msg["side"])))
    return dict(key=key, token=token, ts=ts, price=price, size=size, side=msg["side"])
