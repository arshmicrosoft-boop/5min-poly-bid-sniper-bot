# 🎯 Polymarket 5-Minute Poly-Bid Sniper

> **A patient, post-only maker bot for BTC/ETH/SOL/XRP/DOGE 5-minute UP/DOWN markets.**
> Decided sides only. No chasing. No market orders. Ever.

[![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-blue.svg)](#requirements)
[![Live mode locked](https://img.shields.io/badge/live-mode%20locked-red.svg)](#safety--qualification-gates)
[![Dry Run Default](https://img.shields.io/badge/dry--run-default-yellow.svg)](#safety)

---

## 🧠 Strategy in 60 seconds

Polymarket runs 5-minute BTC/ETH/SOL/XRP/DOGE UP/DOWN markets. Most of the time they're a coin flip. In the **final 60 seconds**, one side usually "decides" — its best ask climbs past **$0.95**. That's our signal.

We **do not** market-buy at 96¢. We **rest a passive BUY limit on the bid**, capped at **$0.95**, and let the book come to us. If filled at 95¢ and the side resolves to $1.00, that's **~5.26%** per win, maker-fee-free.

- One entry per market, per coin. Hold to expiry.
- Stand down if the side stops being decided (ask drops below 0.95).
- Stop opening bids at 15s remaining; cancel resting orders at 10s remaining.
- Dry-run does not claim fills, win rate, or PnL without trade-print and official-resolution data.
- Reconcile positions and open orders through Polymarket's current unified SDK (Data API v2); never double-submit.
- Live order placement stays hard-disabled until confirmed fills, official resolution, and realized loss limits are durably reconciled.

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        5min_95_bid_sniper.py                        │
│  ┌──────────────┐  ┌──────────────┐  ┌────────────────────────┐     │
│  │ Market       │  │ Concurrent   │  │ Per-Coin State Machine │     │
│  │ Discovery    │→ │ Book Poller  │→ │  (favored side, order, │     │
│  │ (Gamma API)  │  │ (ThreadPool) │  │   entered, fills)      │     │
│  └──────────────┘  └──────────────┘  └────────────────────────┘     │
│         │                  │                      │                 │
│         ▼                  ▼                      ▼                 │
│   ┌──────────┐      ┌──────────┐         ┌──────────────┐           │
│  │ CLOB REST│      │ Order    │         │ Exposure Cap │           │
│  │ (books)  │      │ Manager  │         │ (global $)   │           │
│  └──────────┘      └──────────┘         └──────────────┘           │
└─────────────────────────────────────────────────────────────────────┘
         │                                    │
         ▼                                    ▼
   ┌──────────┐                       ┌──────────────┐
   │ nice_funcs│ (sizing, token IDs)   │  .env creds  │
   └──────────┘                       └──────────────┘
```

### Concurrency model
- **ThreadPoolExecutor** (`BOOK_WORKERS`) polls every discovered coin's UP/DOWN books in parallel.
- **Per-coin state dicts** (`market_states`, `market_books`) — no shared mutable order state across coins.
- **Global exposure cap** (`MAX_TOTAL_EXPOSURE_USD`) guards total resting notional across all coins.
- **Market rollover** cancels all unfilled orders atomically before re-discovering.

### Order lifecycle
```
IDLE → decided side detected → rest bid (post-only, GTD with expiry)
  → re-check every 3s → price drift? cancel+resubmit
  → fill detected from the CLOB order status → ENTERED (hold to expiry)
  → 15s no-new-entry → 10s cancel resting → CUTOFF
  → market rollover → cancel each known order ID → carry held-position exposure
```

### Safety layers
1. **Dry run default** — `DRY_RUN=true` unless explicitly disabled. No orders sent.
2. **Post-only** — every order is `post_only=True`; never crosses the spread.
3. **Price cap** — never bids above `MAX_BUY_PRICE` (default 95¢).
4. **Exposure cap** — resting plus filled notional ≤ `MAX_TOTAL_EXPOSURE_USD` (default $5), carried across market windows. Filled-position reserves are not released during the session because authoritative settlement accounting is not implemented; after a fill this can stop further entries.
5. **Timeout-assumed-live** — if a POST times out, treat as potentially live and block duplicates.
6. **Cancel-and-confirm** — only release a reservation after the unified SDK confirms the exact order ID was canceled and the order status is terminal; `NOT_FOUND` remains unresolved.
7. **One entry per market** — `entered` flag prevents re-entry.

---

## 📁 Project structure

```
poly/
├── 5min_95_bid_sniper.py   # live multi-coin bot (main entry point)
├── nice_funcs.py           # calculate_shares, get_token_id
├── backtest/
│   ├── backtest.py         # recorded-CLOB replay engine (queue-aware)
│   ├── backtest_rigorous.py# 12-gate qualification harness
│   ├── real_data.py        # Binance kline + BS book model (model-based)
│   └── sniper_backtest.py  # Strike-Cross + 95c sniper (model-based)
├── requirements.txt
├── .env.example
└── README.md
```

---

## ⚙️ Configuration

Edit `.env` (copy from `.env.example`):

```dotenv
PRIVATE_KEY_AUG14=0xYOUR_PRIVATE_KEY
PUBLIC_KEY_AUG14=0xYOUR_POLYMARKET_WALLET_ADDRESS
API_KEY_AUG14=
API_SECRET_AUG14=
PASSPHRASE_AUG14=
DRY_RUN=true
TARGET_NOTIONAL_USD=1.0
MAX_ORDER_NOTIONAL_USD=5.0
MAX_TOTAL_EXPOSURE_USD=5.0
MAX_SESSION_LOSS_USD=8.0
MAX_POSITION_LOSS_USD=4.50
SOFT_ENTRY_CUTOFF_SEC=15
STOP_THRESHOLD_SEC=10
```

The repository pins `polymarket-client` (the current unified SDK). Older `py-clob-client-v2` and `web3` packages are not used by this bot.

Strategy constants in `5min_95_bid_sniper.py`:

| Setting | Default | Meaning |
|---|---:|---|
| `TARGET_NOTIONAL_USD` | `$1.00` | Desired notional before minimum-share rounding; actual order can be larger |
| `MAX_ORDER_NOTIONAL_USD` | `$5.00` | Hard maximum notional for one order; excess is rejected |
| `ENTRY_THRESHOLD` | `0.95` | Ask must be ≥ this to be "decided" |
| `MAX_BUY_PRICE` | `0.95` | Hard cap on bid price |
| `ENTRY_WINDOW_SEC` | `60` | Only act in final 60s |
| `SOFT_ENTRY_CUTOFF_SEC` | `15` | Stop opening new orders below this; existing orders are still managed |
| `STOP_THRESHOLD_SEC` | `10` | Cancel resting orders at/below this threshold |
| `ORDER_UPDATE_INTERVAL` | `1` | Re-check cadence (seconds) |
| `SIGNATURE_TYPE` | `2` | Must match the funded wallet; default is the configured AUG14 Safe wallet |
| `MAX_TOTAL_EXPOSURE_USD` | `5.0` | Global resting plus filled exposure cap |
| `MAX_SESSION_LOSS_USD` | `8.0` | Dry-run loss limit; stops the session and cancels resting orders |
| `MAX_POSITION_LOSS_USD` | `4.50` | Marked-loss alert/halt for new entries; does not exit held positions |

---

## 🚀 Run

```powershell
python -m pip install -r requirements.txt
Copy-Item .env.example .env   # then edit .env
python .\5min_95_bid_sniper.py
```

Press `Ctrl+C` to stop. Unfilled orders are cancelled on exit. Dashboard shows per-coin books, phase, session stats, recent events.

---

## 📊 Backtesting

Two paths — pick by accuracy need:

### A. Recorded CLOB replay (real, recommended)
```powershell
python .\backtest\backtest.py record --output .\data\market_data.jsonl
python .\backtest\backtest_rigorous.py --input .\data\market_data.jsonl --days 30
```
- Captures real books, trades, and `market_resolved` (Chainlink settlement). The recorder
  is implemented in `poly5m/ws_collector.py` (WS market stream + Binance candles).
- Book snapshots are coalesced to 1s and filtered to the levels the strategy reads
  (bids >= 0.85, asks <= 0.15, plus the touch), so a 14-day file is ~8 GB rather
  than the ~90 GB the raw tape would be. Use `--bucket 2.0` or `--max-mb` to trade
  resolution for size.
- Queue-aware fills, latency, cancel-confirm, fees, slippage.
- 12 qualification gates: walk-forward, block sign-flip, bootstrap CI, placebo, stress, kill-switch, plateau, etc.

### B. Model-based (Binance + Black-Scholes, indicative only)
```powershell
python .\backtest\sniper_backtest.py
```
Fast, but uses Binance TWAP settlement and synthetic books — **not** real Polymarket data.

---

## 🛡️ Safety & qualification gates

**Live mode is locked in code.** `DRY_RUN=false` exits before account actions because the bot still lacks durable, authoritative accounting that joins every partial fill, fee, and official market settlement to the session-loss guard. Dry-run monitors live books and manages simulated orders in memory, but does not claim fills or settlement PnL from quote touches. No backtest result currently qualifies this implementation for live trading.

Backtest gates are necessary but not sufficient for live use. The backtest report is a dataset-level result only; it cannot certify the live bot or unlock live order placement. The live lock also requires verified account reconciliation, durable fill/fee/settlement accounting, and enforced loss limits.

A strategy is considered for further evaluation only if **all** gates pass:

| # | Gate | Threshold |
|---|---|---|
| 1 | Positive net PnL in ≥3/4 OOS windows | ≥3 |
| 2 | Block sign-flip p-value | < 0.05 |
| 3 | Lead rate < 50% **or** (WR > 54% & counter-lead WR ≥ 50%) | pass |
| 4 | Inverted (placebo) WR | < 40% |
| 5 | Stress (slip×2, +25bp, latency+500ms, fills −20%) net PnL | > 0 |
| 6 | Max drawdown on $10 bankroll | ≤ $6 |
| 7 | Parameter plateau (0.70–0.95 profitable) | ≥ 4 of 6 |
| 8 | Fill rate 50% still profitable | > 0 |
| 9 | Cross-strategy overlap | < 30% |
| A | Bootstrap CI lower bound > 95% breakeven | pass |
| B | Max lose streak | ≤ 3 |
| C | Walk-forward decay | ≤ 25% |

**Red flags (auto-fail):** 100% lead rate with ~50% continuation WR; inverted WR ≥ base WR; slippage 2× printing identical PnL; decay > 25%; MaxDD > kill-switch.

---

## Historical backtest figures

Older versions of this README listed a 30-day result of 98.54% win rate and `PASS / READY`. Those figures are unverified and must not be used as evidence that this bot or its current filters are profitable. The live filters, paper-fill behavior, and historical backtest assumptions are not equivalent. The current dry-run deliberately does not score quote touches as fills or infer settlement from cached books. A defensible result requires recorded trade prints, queue-aware fills, and the official resolved outcome for each market.

---

## 📜 License & disclaimer

Educational/research software. Live mode is intentionally disabled pending authoritative fill and settlement accounting. Keep `DRY_RUN=true`; never commit `.env`.
