# v6 one-order live canary

This tool is deliberately separate from the paper engine.

It has two phases:

1. `--check-only`: authenticate the existing Polymarket account, inspect collateral balance, existing open orders, and trading approvals. It never places an order.
2. `--live-one`: at most one real BTC5m maker order per run, using the same 45→30 second signal window. Real-money mode requires the exact confirmation phrase.

Hard guards:

- BUY only.
- Post-only is mandatory.
- Maximum stake is $5.
- Maximum one real order per run.
- Authenticated user stream must be connected before placement.
- The order is held sticky for at most 5 seconds instead of chasing every desired-price tick.
- Any real fill triggers cancellation of the unfilled remainder.
- Public signal invalidation, book generation change/disconnect, crossing risk, or T-30 hard cutoff triggers cancellation.
- Cancellation is checked through authenticated `get_order()`; if not verified, the process keeps retrying until the market ends.
- Reports omit private keys, CLOB credentials, wallet addresses, user-stream owner fields, exception text, and headers.
- Generated DB/report files default to `runs/`.

The tool reads the existing local `.env` fields:

```dotenv
POLYMARKET_PRIVATE_KEY=...
POLYMARKET_WALLET=...
```

Never paste those values into chat or commit them.

Preflight only:

```powershell
uv run --frozen python -m jevymarket.maker_live_canary --check-only
```

A real-money canary is intentionally not the default. Only after preflight is clean:

```powershell
uv run --frozen python -m jevymarket.maker_live_canary --live-one --confirm ONE_REAL_POST_ONLY_ORDER --seconds 900
```

Only run live mode where your account and jurisdiction are permitted to trade. Do not bypass platform or regional restrictions.

This canary measures actual order acceptance, authenticated size matched, and cancel acknowledgement. It is not a profitability test and does not enable unrestricted live automation.
