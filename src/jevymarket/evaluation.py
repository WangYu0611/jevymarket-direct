"""Independent checkpoint sampling for short-horizon strategy evaluation."""

from __future__ import annotations

CHECKPOINTS_BY_TIMEFRAME: dict[str, tuple[int, ...]] = {
    "5m": (240, 180, 120, 60, 30),
    "15m": (600, 300, 180, 120, 60, 30),
    "1h": (1800, 900, 600, 300, 120, 60),
}


def checkpoint_for(
    timeframe: str,
    seconds_left: int | None,
    *,
    tolerance_seconds: int = 20,
) -> int | None:
    if seconds_left is None or seconds_left <= 0:
        return None
    checkpoints = CHECKPOINTS_BY_TIMEFRAME.get(timeframe)
    if not checkpoints:
        return None
    nearest = min(checkpoints, key=lambda value: abs(value - seconds_left))
    return nearest if abs(nearest - seconds_left) <= tolerance_seconds else None
