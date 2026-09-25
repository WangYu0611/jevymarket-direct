# v6 paper 45→30 trial

This paper-only runner is the next execution test after the same-feed timing A/B.

Why this window:
- The preceding 30-minute A/B produced zero candidates in 30→10 seconds.
- All six candidate samples occurred in one market between roughly T-42s and T-37s.
- 45→30s contains that observed cluster while stopping new exposure well before expiry.

Only timing changes. The model, 0.92 confidence threshold, 1s book freshness, price/spread rules, sizing, queue model, BBO recovery and risk limits are unchanged.

Run:

```powershell
uv run --frozen python -m jevymarket.maker_paper_45to30 --seconds 3600
```

This creates a new database and `runs/v6_paper_45to30_*.json.gz`; both live under `runs/` by default. The report contains paper orders, execution events, statistics and the latest diagnostic summary.

This is not live trading and is not evidence of profitability.
