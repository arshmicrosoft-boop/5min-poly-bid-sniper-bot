# Backtest Audit — Reconciliation, Fill Model, and Validation Plan

## 1. Reconciliation: 0.95 headline vs. sweep row

Two different numbers appear for the 0.95 threshold in `backtest/results/backtest_30d_report.md`:

- Headline block: 1,236 trades / 98.54% WR / +$228.49
- Sweep row:       1,149 trades / 97.48% WR / +$151.54

**Which is correct:** the sweep row is the authoritative, reproducible figure. It is produced by
`backtest_rigorous.py` via `run_backtest(..., threshold=0.95, cap=0.95, days=...)`, which is the
code path that emits per-threshold metrics.

**Why they differ:** the headline block is a hand-written summary (it even cites "Trades (filled)"
and "Avg profit per win ~$0.25" derived from the same run but rounded/aggregated differently, and
it was written against a different effective day window / filtering than the sweep). The `report`
subcommand in `backtest.py` is a stub (`print("Report: run backtest_rigorous.py instead.")`), so
the headline was never produced by a runnable command. **Do not use the headline as the benchmark.**
Use the sweep row: 1,149 trades / 97.48% WR / +$151.54.

Action: treat every headline-derived claim (e.g. "Max DD $11.57", "Max lose streak 1",
"Bootstrap CI [97.27%, 98.81%]") as unverified until re-run by `backtest_rigorous.py` and saved
to `results/rigorous_report.json`.

## 2. How "modeled queue fills" works (code: `run_backtest` in backtest.py)

- **Trigger:** a side qualifies when its best ask >= threshold (0.55..0.95). The first qualifying
  side (highest ask) is selected; side can switch if it stops qualifying.
- **Target price:** `min(best_bid, best_ask - 0.01, cap)` — i.e. the bid must be at or better than
  best bid, and at least 1¢ below the ask. So the resting bid is at the best bid or better.
- **Queue position:** modeled. At submit, `queue = sum(bids[side] prices >= target_price)` — the
  size ahead in the queue at that price level.
- **Fill:** on a `last_trade_price` event with taker side SELL on the same side, if trade price
  <= order price, queue is decremented by traded size. Fill occurs only when queue <= 0, and the
  filled size is `min(remaining shares, qty - queue_before)`. A `fill_rate` multiplier (default 1.0)
  further gates fills stochastically.
- **Late fills:** an order placed inside the final 5s is cancelled (cutoff check at end_ms - 5s).
  However, there is NO explicit check that a fill happens before the outcome is "effectively
  known"; fills are accepted up to the cutoff, which is a real risk (adverse selection).
- **Post-through:** if the book trades through the bid price (price < order price), queue is set to 0
  and the order is treated as filled (subject to fill_rate). This assumes a fill rather than a skip.
- **No post-fill verification:** there is no rejection of fills where the bid was skipped because
  price jumped over it without a print at that level.

**Gaps vs. reality:** (a) queue priority among same-price orders is not modeled beyond size;
(b) partial fills cancel the remainder after latency_ms; (c) fills are assumed whenever price
trades through, which overstates fills in fast markets; (d) no adverse-selection conditioning.

## 3. Out-of-sample validation (not yet run — no later dataset available)

Required: a separate, later 7–14 day recorded CLOB dataset (`data/market_data_oos.jsonl`) not used
for any parameter selection. Run:

    python backtest/backtest_rigorous.py --input data/market_data_oos.jsonl --days 14 --out results/oos_report.json

Do not tune the threshold on this period. Report the same per-threshold sweep.

## 4. Cost stress (implemented)

`run_backtest` now accepts `extra_cost_per_share` (dollars/share) added to entry cost. The rigorous
script prints a COST STRESS SWEEP for +0.5¢ and +1.0¢ per share across all thresholds, plus
win rate and whether net PnL stays positive. Re-run after obtaining data.

## 5. Fill rate reported separately (implemented)

The sweep table now includes a `fillRate` column (fills / orders). Compare fill rate to win rate:
a high win rate with low fill rate is not deployable edge.

## 6. Independence check (not yet implemented)

5-minute windows are contiguous and may share underlying autocorrelation. To check, re-run the
sweep on a strided subset (e.g., every other window, or windows spaced >= 10 min apart) and
compare win rate / PnL / drawdown. Implementation pending: add `--stride N` to rigorous script.

## 7. Minimum evidence before moving from dry-run to live

Do NOT recommend a live threshold until ALL of the following hold on out-of-sample data:

1. Net PnL > 0 under +1.0¢/share cost stress for at least 3 of the 9 thresholds in 0.70–0.80.
2. Fill rate reported and >= 60% at the chosen threshold.
3. Win rate bootstrap 95% CI lower bound > breakeven at chosen threshold.
4. Max drawdown <= $6 kill-switch on the OOS period.
5. Max lose streak <= 3.
6. Walk-forward: >= 3 of 4 quartiles positive net PnL, decay <= 25%.
7. Block sign-flip p < 0.05 (signal not noise).
8. Inverted (placebo) WR < 40%.
9. Cost-stress (+1.0¢) still positive.
10. No overlapping-window inflation: strided subset reproduces the conclusion.
11. Queue-aware fill model: bid at best bid or better, explicit queue priority, no post-cutoff fills,
    no fills when price jumps through without a print.
12. At least 2 consecutive weeks of dry-run live (paper) data matching backtest assumptions.

Until then: keep the bot in dry-run. Treat 0.70–0.80 as the investigation range, not a setting.