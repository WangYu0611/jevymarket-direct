# Maker timing A/B (v6-window-ab-r1)

This experiment compares timing only on one shared public-data stream.

- Arm A: allow candidate quotes when `10 < seconds_left <= 30`.
- Arm B: allow candidate quotes when `20 < seconds_left <= 60`.
- All confidence, price, spread, model, freshness and sizing rules are identical.
- The runner is observe-only and creates no paper or live orders.
- It samples shadow eligibility once per second while either arm is active.
- Report both candidate rate and candidate markets. Raw counts alone are not comparable because Arm B is longer.
- The report also preserves book-gate categories so fresh one-sided books are not mislabeled as network staleness.

Run:

```powershell
uv run --frozen python -m jevymarket.maker_window_ab --seconds 1800
```

The generated `v6_window_ab_*.json.gz` is the artifact to review. Do not use this experiment as a profitability or fill claim.
