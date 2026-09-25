"""Default location for generated research/run artifacts."""
from __future__ import annotations

from pathlib import Path

RUNS_DIR = Path("runs")


def run_output_path(explicit: Path | None, filename: str) -> Path:
    """Return explicit path unchanged, otherwise place output under ./runs.

    Parents are created here so SQLite lock/WAL sidecars and exclusive report
    creation work even before a store object has initialized the database.
    """
    path = Path(explicit) if explicit is not None else RUNS_DIR / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    return path
