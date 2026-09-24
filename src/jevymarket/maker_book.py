"""Validated public CLOB L2 cache. Public trades are evidence, not private fills."""
from __future__ import annotations

from dataclasses import dataclass, field

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


class BookCache:
    def __init__(self, condition: str, specs: dict[str, tuple[float, float]]):
        self.condition = condition
        self.books = {t: TokenBook(t, *s) for t, s in specs.items()}
        self.generation = 0

    def invalidate(self):
        self.generation += 1
        for book in self.books.values():
            book.ready = False
            book.bids.clear()
            book.asks.clear()

    @staticmethod
    def _level(price, size):
        p, q = finite(price), finite(size)
        if not 0 < p < 1 or q < 0:
            raise BookGap("invalid_level")
        return p, q

    def apply(self, msg: dict, wall: float, mono: float) -> None:
        """Apply snapshots/deltas atomically per event; invalidate on inconsistency."""
        kind = msg.get("event_type")
        if kind not in {"book", "price_change", "tick_size_change"}:
            return
        if msg.get("market") != self.condition:
            return
        try:
            ts = source_time(msg.get("timestamp"))
            if ts > wall or wall - ts > 5:
                raise BookGap("stale_or_future_book_message")
            touched = []
            if kind == "book":
                b = self.books.get(str(msg.get("asset_id")))
                if b is None:
                    return
                if ts < b.ts:
                    raise BookGap("out_of_order_snapshot")
                bids, asks = {}, {}
                for label, dest in (("bids", bids), ("asks", asks)):
                    for row in msg[label]:
                        p, q = self._level(row["price"], row["size"])
                        if q:
                            dest[p] = q
                b.bids, b.asks, b.ready = bids, asks, True
                touched = [b]
            elif kind == "price_change":
                for row in msg["price_changes"]:
                    b = self.books.get(str(row.get("asset_id")))
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
                    b = self.books.get(str(row.get("asset_id")))
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
                b = self.books.get(str(msg.get("asset_id")))
                if b is None:
                    return
                tick = finite(msg["new_tick_size"])
                if not 0 < tick < 1:
                    raise BookGap("invalid_tick")
                b.tick = tick
                # Old open orders must be reevaluated; require a fresh snapshot.
                self.invalidate()
                raise BookGap("tick_changed_resubscribe")
            for b in touched:
                if b.bid is not None and b.ask is not None and b.bid >= b.ask:
                    raise BookGap("crossed_book")
                b.ts, b.received_mono = ts, mono
        except (KeyError, TypeError, ValueError) as exc:
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
