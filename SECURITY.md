# Security

This bot signs on-chain approvals and places real orders with a private key read from `.env`.

- Use a **dedicated wallet** with only the funds you are willing to lose. The exposure caps in
  `.env` are the only brake.
- `.env` and `*.db` are git-ignored. Keep it that way; the SQLite log contains your market
  activity.
- API keys are sent only to `openrouter.ai`; the private key is used locally by the Polymarket
  SDK for EIP-712 order signing and (for `setup`) approval transactions.

To report a vulnerability, email the maintainer privately rather than opening a public issue.
There is no bug bounty.
