# V8.1 T-120 Live Canary

This module is the first authenticated live path for the V8.1 price-value strategy.

It is intentionally narrower than the paper strategy.

## Real-money rule

A real order can be attempted only when **all** of these are true:

- slot is exactly T-120 (111..120 seconds left);
- Quant is high confidence on one side;
- Jev passes answerability/clarity;
- Jev direction agrees with Quant;
- after Jev returns, the public book and Quant are refreshed;
- refreshed Quant is still high confidence on the same side;
- predicted-side best ask is still within the existing V8.1 price band;
- Quant minus refreshed ask still meets the existing 8% minimum edge;
- spread and 5-cent depth still pass;
- final refresh immediately before submission still passes the same rules.

T-110 and T-100 continue to be recorded by the V8.1 paper logic, but they can
never reach the live placement hook.

Only the B arm (Quant + Jev + response-time ASK) can reach live placement.
A and C remain paper-only.

## Order type and hard caps

Live BUY uses the official SDK market-order workflow:

- order type: FAK;
- max price: final refreshed best ask;
- max spend: USD 5;
- one real attempt per market.

The FAK order may fill partially and cancel the remainder automatically.

For a future full live session, hard upper bounds are:

- 24 real attempts;
- USD 120 cumulative planned notional;
- USD 5 maximum spend per order.

## Paper gates

`--live-one` is an infrastructure canary. Because real money is T-120-only, it
uses the T-120 subset of the supplied V8.1 B arm. It requires at least 20 settled
T-120 simulated trades, >65% win rate, positive estimated net PnL, and positive
+1-tick price-stress PnL.

`--live-session` is stricter. The **T-120 subset itself** must pass the complete
V8.1 profitability gate: >=50 settled T-120 trades, >65% win rate, positive net
PnL, positive PnL after removing the three largest winners, and positive +1-tick
stress PnL. T-110/T-100 results cannot unlock the real session.

## Geographic compliance

Before authenticated client creation, the program checks:

```text
https://polymarket.com/api/geoblock
```

Blocked or unverified geoblock status fails closed. There is no command-line
override.

Only run where opening new positions is permitted by Polymarket and applicable
law. Do not use VPNs, proxies, or other methods to bypass platform restrictions.

## Preflight

Check account connectivity, collateral balance, approvals, open orders, paper
gate, and geoblock without placing an order:

```powershell
uv run --frozen python -m jevymarket.price_value_live_t120 \
    --check-only \
    --paper-report "runs\v81_early_value_forward_....json.gz"
```

## First real canary

The first real test should be one order only:

```powershell
uv run --frozen python -m jevymarket.price_value_live_t120 \
    --live-one \
    --confirm ONE_REAL_T120_QUANT_JEV_ASK \
    --paper-report "runs\v81_early_value_forward_....json.gz" \
    --seconds 86400
```

The process may wait until a valid T-120 B signal appears. It stops after the
first real filled order. If an order is rejected/no-fill, that market is not
retried.

Generated artifacts default to `runs/`.

Do not upload `.env`, private keys, wallet recovery material, or API secrets.

## 24-hour live session

The code includes a session mode, but it is hard-blocked until the full B paper
gate passes:

```powershell
uv run --frozen python -m jevymarket.price_value_live_t120 \
    --live-session \
    --confirm LIVE_T120_QUANT_JEV_ASK_SESSION \
    --paper-report "runs\<fully-qualified-v81-report>.json.gz" \
    --seconds 86400
```

The session cannot exceed 24 real attempts or USD 120 planned notional.

A placement exception after submission is treated as uncertain and halts all
further live trading. The operator must review the account before starting a new
live run.


## Resume after a clean stop

Live DBs can be resumed only when every prior live row is terminal
(`filled`, `no_fill`, or `rejected`). Any unresolved intent/placement state
fails closed and must be reviewed manually before another live run.

```powershell
uv run --frozen python -m jevymarket.price_value_live_t120 \
    --live-one \
    --confirm ONE_REAL_T120_QUANT_JEV_ASK \
    --paper-report "runs\v81_early_value_forward_....json.gz" \
    --resume-db "runs\jevymarket.v81-live-t120_....db" \
    --seconds 86400
```
