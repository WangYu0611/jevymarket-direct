"""Snapshot handoff only: never apply an older delta or infer missing depth."""
from __future__ import annotations

from .maker_book import BookCache
from .maker_model import finite, source_time


class SnapshotBookCache(BookCache):
    """Fence startup deltas strictly older than ALL affected fresh snapshots.

    The base validator remains strict. The exception here is a read-only discard
    before any post-snapshot delta has been accepted for the affected tokens.
    Equal timestamps, mixed old/new token rows, malformed data, and regressions
    after an accepted delta still go through the original validator.
    """

    def __init__(self, condition, specs):
        super().__init__(condition, specs)
        self.snapshot_baselines = {}
        self.last_discard = None

    def invalidate(self):
        super().invalidate()
        self.snapshot_baselines.clear()
        self.last_discard = None

    def replay_state(self, books=None):
        state = super().replay_state(books)
        state["snapshot_baselines"] = {t: list(stamp) for t, stamp in self.snapshot_baselines.items()}
        return state

    def _snapshot_overlap(self, msg, wall, mono):
        if msg.get("event_type") != "price_change" or msg.get("market") != self.condition:
            return None
        rows = msg.get("price_changes")
        if not isinstance(rows, list) or not rows:
            return None
        try:
            ts = source_time(msg.get("timestamp"))
            if not 0 <= wall - ts <= 5:
                return None
            snapshots = {}
            for row in rows:
                if not isinstance(row, dict):
                    return None
                token = str(row.get("asset_id"))
                book = self.books.get(token)
                baseline = self.snapshot_baselines.get(token)
                if book is None or baseline is None or not book.ready:
                    return None
                if (book.ts, book.received_mono) != baseline or not ts < baseline[0]:
                    return None
                if not (0 <= wall - book.ts <= 5 and 0 <= mono - book.received_mono <= 5):
                    return None
                if row.get("side") not in {"BUY", "SELL"}:
                    return None
                self._level(row["price"], row["size"])
                for label in ("best_bid", "best_ask"):
                    if label in row and not 0 <= finite(row[label]) <= 1:
                        return None
                snapshots[token] = baseline[0]
        except (KeyError, TypeError, ValueError):
            # Malformed messages must reach the strict validator, not disappear.
            return None
        return {"reason": "pre_snapshot_delta_discarded", "source_ts": ts,
                "snapshot_source_ts": snapshots, "rows": len(rows)}

    def apply(self, msg, wall, mono):
        self.last_discard = None
        covered = self._snapshot_overlap(msg, wall, mono)
        if covered is not None:
            # No levels, generation, ordering watermark or freshness are changed.
            self.last_discard = covered
            return
        super().apply(msg, wall, mono)
        if msg.get("market") != self.condition:
            return
        if msg.get("event_type") == "book":
            token = str(msg.get("asset_id"))
            book = self.books.get(token)
            if book is not None and book.ready:
                self.snapshot_baselines[token] = (book.ts, book.received_mono)
        elif msg.get("event_type") == "price_change":
            for row in msg.get("price_changes", []):
                self.snapshot_baselines.pop(str(row.get("asset_id")), None)
