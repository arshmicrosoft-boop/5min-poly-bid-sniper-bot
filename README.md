# Polymarket 95-Cent Bid Sniper

A Python command-line bot for Polymarket 5-minute crypto UP/DOWN markets. It scans BTC, ETH, SOL, XRP, and DOGE markets for the patient, post-only maker strategy described in the [Moon Dev walkthrough](https://moondev.com/polymarket-95-cent-bid-sniper). The local project includes the dashboard, main loop, and helper module needed to run the strategy.

## How the strategy works

For each listed coin market, the bot waits until the final 60 seconds, then looks for an outcome whose best ask is at least `$0.95`. If one qualifies, it proposes a post-only BUY limit order at the best bid, capped at `$0.95`. It polls the coin books concurrently, checks orders about every three seconds, and stops entering at five seconds remaining. Each coin has independent signal and order state, with at most one entry per coin per window. A coin market that is not listed or cannot be fetched is skipped and rediscovered periodically.

At a `$0.95` purchase price, a share that resolves to `$1.00` has a gross price gain of about 5.26% on the amount paid, before fees and other costs. This is not a return guarantee. Polymarket's five-share minimum means the default `$1.00` target can require about `$4.75` at the price cap.

## Project files

```text
.
|-- .env.example
|-- .gitignore
|-- requirements.txt
|-- 5min_95_bid_sniper.py
|-- backtest/
`-- nice_funcs.py
```

## Requirements

- Python 3.10 or newer
- A network connection for public Polymarket market data
- Wallet credentials only if you deliberately enable live order mode

Install the Python dependencies from the repository root:

```powershell
python -m pip install -r requirements.txt
```

## Configure credentials

Create a local `.env` from the example:

```powershell
Copy-Item .env.example .env
```

For dry run, leave `DRY_RUN=true`; wallet values are not needed. To enable live orders, set `DRY_RUN=false` and configure:

```dotenv
PRIVATE_KEY_AUG14=0xYOUR_PRIVATE_KEY
PUBLIC_KEY_AUG14=0xYOUR_POLYMARKET_WALLET_ADDRESS
```

The API credential values are optional. When all three are blank, the CLOB client attempts to derive API credentials from the wallet key.

Keep `.env` private. It is excluded by `.gitignore`; never commit or share a real private key, API secret, or passphrase.

## Run

From the repository root in PowerShell:

```powershell
python .\5min_95_bid_sniper.py
```

The terminal dashboard is split into panels: market window and progress bar, capital and risk (exposure gauge, open bids, holdings, max loss/profit), a scanner table for all five assets, session telemetry (fill rate, paper win rate/PnL, timeouts, skips), execution log, and discovery log. It also reports polling, CLOB, order, cancel, and UI latency. Press `Ctrl+C` to stop. Set `SNIPER_ASCII_UI=1` to force ASCII borders on terminals that cannot print box-drawing characters.

**Dry run is the default.** It reads live public market data and keeps simulated order state in process memory. It does not connect to your account or place orders. A paper order is marked filled when the live best ask reaches or crosses its bid; this book-touch model assumes a full fill and does not model queue priority, available size, or actual trades. At window rollover, paper results use the last cached book and a 98¢ bid threshold as a directional outcome proxy, not official settlement. Paper state disappears when the process exits. No `.env` file or credentials are needed for dry run.

Run the dry mode:

```powershell
python .\5min_95_bid_sniper.py
```

To enable live order submission, set `DRY_RUN=false` in `.env` and configure valid wallet credentials. The dashboard then labels the mode `LIVE MAKER`. The default strategy settings are in `5min_95_bid_sniper.py`:

| Setting | Default |
| --- | ---: |
| Target bet size | `$1.00` (subject to five-share minimum) |
| Decided-side ask threshold | `$0.95` |
| Minimum same-side bid | `$0.88` |
| Maximum opposite-side bid | `$0.08` |
| Maximum buy price | `$0.95` |
| Early-window maximum bid (over 30s left) | `$0.93` |
| Entry window | Final 60 seconds |
| Stop/cancel threshold | 5 seconds remaining |
| Order recheck interval | 3 seconds |
| Maximum total exposure | `$10.00` |

The bot can place separate orders for qualifying coins in the same window until the `$10.00` exposure cap is reached. At the five-share minimum and `$0.95`, this generally allows two simultaneous bids (`$9.50` total); which signals get the available exposure currently depends on market discovery order.

Signals also require a same-side bid of at least `$0.88` and an opposite-side bid no higher than `$0.08`. A rapid ask drop of 2¢ or more blocks a new bid for that book update. Existing orders bypass the normal recheck delay when the signal fails these validity checks, allowing an immediate stand-down cancellation.

## Order-state handling

In live mode, the bot checks order status before replacing a resting order. If the CLOB request times out and the bot cannot tell whether an order was accepted, it treats the order as potentially live and blocks duplicate submissions. It also checks positions to detect fills. A network or API failure can delay status updates or cancellation, so monitor the dashboard and your Polymarket account while it runs.

## Source walkthrough

The strategy and code walkthrough are published at [Moon Dev: Polymarket 95-cent Bid Sniper](https://moondev.com/polymarket-95-cent-bid-sniper). The local `main()` loop and terminal dashboard complete portions the walkthrough leaves as placeholders. The root-level `nice_funcs.py` supplies its helper import.
## Backtesting with recorded data

This strategy depends on historical Polymarket order-book state for its 95-cent signal and passive fills. Polymarket does not provide a historical CLOB book archive, so Binance price candles alone cannot produce valid entry or fill results. `backtest.py record` captures Polymarket book/trade events together with real Binance BTCUSDT 1-second candles for future replay.

Install dependencies, then start recording:

```powershell
python -m pip install -r requirements.txt
python .\backtest.py record --output .\data\market_data_windowed.jsonl
```

Leave the recorder running across markets. When you have enough completed/resolved markets, stop it with `Ctrl+C` and run:

```powershell
python .\backtest.py report --input .\data\market_data_windowed.jsonl --trades-out .\results\trades.csv
```

To test entries from 55¢ through 95¢, in 5¢ increments:

```powershell
python .\backtest.py sweep --input .\data\market_data.jsonl --days 14
```

The sweep models the stated hypothesis: in each 5-minute market, select the first UP or DOWN ask that reaches X, assume a fill at X, and score it against Polymarket's recorded resolution. This is an idealized fill assumption; it does not claim a resting 95¢ bid would actually fill. It reports signal/fill count, resolved sample count, wins, win rate with a 95% Wilson interval, net PnL after modeled latency slippage, and maximum drawdown. It ignores bankroll limits so a small starting balance cannot silently suppress trades. The CSV is written to `results/price_sweep.csv`.

There are 288 five-minute windows in a full day. A 288-trades/day result is only supported if every market window has a recorded qualifying ask and resolution; missing book history cannot be filled in from Binance data. The output shows recorded window coverage and the theoretical window count. Use `--fill-mode queue` for a separate, more conservative passive-queue estimate.

The separate `report` command compares queue-estimated baseline and stress fills. Its passive queue estimate cannot know true queue priority or hidden liquidity.

Binance BTCUSDT is recorded as a real underlying-price cross-check. PnL uses Polymarket's recorded winning token when available, since BTC up/down markets resolve using Chainlink TWAP rather than Binance spot. The Binance direction comparison is informational and is not used to decide wins.
