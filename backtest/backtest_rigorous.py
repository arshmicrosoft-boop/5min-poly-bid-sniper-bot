"""Rigorous gate-check backtest for the 95-cent sniper.

Runs baseline + stress + fill-rate stress + threshold plateau on recorded
Polymarket CLOB data and prints PASS/FAIL for every gate in backtesting.md.
Settlement uses Polymarket's recorded winning token (Chainlink), not Binance.
"""

from __future__ import annotations

import argparse
import json
import math
import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import backtest as bt

BREAKEVEN = 0.95
BANKROLL = 10.0
KILL_SWITCH = 6.0


def bootstrap_wr_ci(trades, iters=10000, seed=7):
    rng = random.Random(seed)
    n = len(trades)
    if n == 0:
        return None, None
    outcomes = [1 if t["won"] else 0 for t in trades]
    samples = []
    for _ in range(iters):
        s = sum(rng.choice(outcomes) for _ in range(n)) / n
        samples.append(s)
    samples.sort()
    return samples[int(0.025 * iters)], samples[int(0.975 * iters)]


def summarize_extra(trades, aux):
    t = [x for x in trades if "net_pnl" in x]
    n = len(t)
    wins = sum(bool(x["won"]) for x in t)
    wr = wins / n if n else 0.0
    inverted_wr = (n - wins) / n if n else 0.0
    # lead rate = agreement with Binance direction among trades with that info
    with_bin = [x for x in t if x.get("binance_up") is not None]
    lead_rate = (sum((x["side"] == "UP") == x["binance_up"] for x in with_bin) / len(with_bin)) if with_bin else None
    # counter-lead trades: selected side disagrees with Binance direction
    counter = [x for x in with_bin if (x["side"] == "UP") != x["binance_up"]]
    counter_wr = (sum(bool(x["won"]) for x in counter) / len(counter)) if counter else None
    # bootstrap CI
    blo, bhi = bootstrap_wr_ci(t)
    # block sign-flip (reuse bt)
    outcomes = [1.0 if x["won"] else -1.0 for x in t]
    block_z, block_p = None, None
    if len(outcomes) >= 48:
        bs = 12
        blocks = [sum(outcomes[i:i + bs]) for i in range(0, len(outcomes) - bs + 1, bs)]
        observed = sum(blocks)
        rng = random.Random(781)
        perm = [sum(v * rng.choice((-1, 1)) for v in blocks) for _ in range(10000)]
        extreme = sum(abs(v) >= abs(observed) for v in perm)
        block_p = (extreme + 1) / 10001
        var = sum(v * v for v in perm) / len(perm)
        block_z = observed / math.sqrt(var) if var > 0 else 0.0
    # walk-forward
    ordered = sorted(t, key=lambda x: x["start_ms"])
    wf = None
    if len(ordered) >= 4:
        windows = [ordered[i * len(ordered) // 4:(i + 1) * len(ordered) // 4] for i in range(4)]
        wf = []
        for i, w in enumerate(windows, 1):
            net = sum(x["net_pnl"] for x in w)
            winsw = sum(bool(x["won"]) for x in w)
            wf.append({"w": i, "n": len(w), "wins": winsw, "wr": winsw / len(w) if w else 0.0, "net": net})
    positive_windows = sum(1 for w in (wf or []) if w["net"] > 0)
    mean_pnl = (sum(w["net"] for w in (wf or [])) / len(wf)) if wf else 0.0
    decay = ((mean_pnl - (wf[-1]["net"] if wf else 0)) / abs(mean_pnl)) if (wf and mean_pnl > 0) else None
    # losing streak
    streak = max_streak = 0
    for x in ordered:
        streak = 0 if x["won"] else streak + 1
        max_streak = max(max_streak, streak)
    # max dd
    equity = peak = max_dd = 0.0
    for x in ordered:
        equity += x["net_pnl"]
        peak = max(peak, equity)
        max_dd = max(max_dd, peak - equity)
    return {
        "n": n, "wins": wins, "losses": n - wins, "wr": wr,
        "net_pnl": sum(x["net_pnl"] for x in t),
        "fees": sum(x["fees"] for x in t),
        "slippage": sum(x["slippage"] for x in t),
        "max_dd": max_dd, "max_lose_streak": max_streak,
        "inverted_wr": inverted_wr,
        "lead_rate": lead_rate,
        "counter_lead_wr": counter_wr,
        "counter_lead_n": len(counter),
        "bootstrap_lo": blo, "bootstrap_hi": bhi,
        "block_z": block_z, "block_p": block_p,
        "walk_forward": wf, "positive_windows": positive_windows,
        "decay": decay,
        "capital_skips": aux.get("_capital_skips", 0),
        "markets_total": aux.get("_markets_total", aux.get("_markets_with_orders", 0) + aux.get("_capital_skips", 0)),
        "markets_traded": aux.get("_markets_with_fills", aux.get("_markets_traded", 0)),
        "markets_resolved": aux.get("_markets_with_resolution", 0),
        "fill_rate": (aux.get("_markets_with_fills", 0) / aux.get("_markets_with_orders", 1)) if aux.get("_markets_with_orders") else None,
        "kill_switch": aux.get("_kill_switch", False),
        "bankroll_end": aux.get("_bankroll_end", BANKROLL),
    }


def run_all(args):
    data_path = Path(args.input)
    if not data_path.exists():
        print(f"ERROR: data file not found: {data_path}")
        print("Record real CLOB data first: python backtest.py record --output data/market_data.jsonl")
        sys.exit(1)

    dataset = bt.load_dataset(data_path)

    # Baseline
    base_trades = bt.run_backtest(data_path, args.latency_ms, 1.0, 0.0, 1.0, args.seed,
                                 days=args.days, dataset=dataset)
    base_aux = next((x for x in base_trades if "_bankroll_end" in x), {})
    base_m = summarize_extra(base_trades, base_aux)

    # Stress: latency+500, slip x2, 25bp fee, fill rate -20%
    stress_trades = bt.run_backtest(data_path, args.latency_ms + 500, 2.0, 0.0025, 0.8, args.seed,
                                   days=args.days, dataset=dataset)
    stress_aux = next((x for x in stress_trades if "_bankroll_end" in x), {})
    stress_m = summarize_extra(stress_trades, stress_aux)

    # Fill-rate stress: 50% fills (gate 8)
    # run_backtest doesn't expose fill_rate param in signature? it does: fill_rate
    fill50_trades = bt.run_backtest(data_path, args.latency_ms, 1.0, 0.0, 0.5, args.seed,
                                    days=args.days, dataset=dataset)
    fill50_aux = next((x for x in fill50_trades if "_bankroll_end" in x), {})
    fill50_m = summarize_extra(fill50_trades, fill50_aux)

    # Threshold plateau sweep
    prices = [round(x / 100, 2) for x in range(55, 100, 5)]
    sweep = []
    for p in prices:
        tr = bt.run_backtest(data_path, args.latency_ms, 1.0, 0.0, 1.0, args.seed,
                             days=args.days, threshold=p, cap=p, dataset=dataset)
        m = summarize_extra(tr, next((x for x in tr if "_bankroll_end" in x), {}))
        sweep.append((p, m))

    # ---- Print report ----
    print("=" * 78)
    print("RIGOROUS 95-CENT SNIPER BACKTEST — REAL RECORDED CLOB DATA")
    print("=" * 78)
    print(f"Input: {data_path} | days={args.days} | seed={args.seed} | latency={args.latency_ms}ms")
    print(f"Recorded markets: {len(dataset)} | with resolution: {sum(1 for v in dataset.values() if any((r.get('event',{}).get('event_type') or r.get('event',{}).get('type'))=='market_resolved' for r in v['events']))}")
    print()

    def show(label, m):
        print(f"--- {label} ---")
        print(f"Trades: {m['n']} | Wins: {m['wins']} | Losses: {m['losses']} | WR: {m['wr']:.2%}")
        print(f"Net PnL: ${m['net_pnl']:+.4f} | Fees: ${m['fees']:.4f} | Slippage: ${m['slippage']:.4f}")
        print(f"Max DD: ${m['max_dd']:.4f} | Max lose streak: {m['max_lose_streak']} | Capital skips: {m['capital_skips']}")
        print(f"Inverted (placebo) WR: {m['inverted_wr']:.2%}")
        lr = "n/a" if m['lead_rate'] is None else f"{m['lead_rate']:.2%}"
        cl = "n/a" if m['counter_lead_wr'] is None else f"{m['counter_lead_wr']:.2%}"
        print(f"Lead rate (Binance agreement): {lr} | counter-lead n={m['counter_lead_n']} WR={cl}")
        print(f"Bootstrap 95% CI: [{m['bootstrap_lo']:.2%}, {m['bootstrap_hi']:.2%}]" if m['bootstrap_lo'] is not None else "Bootstrap CI: n/a")
        print(f"Block sign-flip: Z={m['block_z']:.3f} p={m['block_p']:.4f}" if m['block_p'] is not None else "Block sign-flip: unavailable (<48 trades)")
        if m['walk_forward']:
            print("Walk-forward (chronological quartiles):")
            for w in m['walk_forward']:
                print(f"  W{w['w']}: n={w['n']:4d} wins={w['wins']:4d} WR={w['wr']:.2%} net=${w['net']:+.4f}")
            print(f"  Positive windows: {m['positive_windows']}/4 | decay: {m['decay']:.2%}" if m['decay'] is not None else f"  Positive windows: {m['positive_windows']}/4")
        print(f"Fill rate: {m['fill_rate']:.2%}" if m['fill_rate'] is not None else "Fill rate: n/a")
        print(f"Kill-switch triggered: {'YES' if m['kill_switch'] else 'no'} | Ending cash: ${m['bankroll_end']:.4f}")
        print()

    show("BASELINE (maker fee=0, queue-modeled fills)", base_m)
    show("STRESS (latency+500ms, slip x2, 25bp fee, fill rate -20%)", stress_m)
    show("FILL-RATE STRESS (50% fills)", fill50_m)

    print("--- THRESHOLD PLATEAU (maker fee=0) ---")
    print(f"{'price':>6} {'trades':>7} {'wins':>6} {'WR':>7} {'netPnL':>10} {'maxDD':>8}")
    for p, m in sweep:
        print(f"{p:>6.2f} {m['n']:>7d} {m['wins']:>6d} {m['wr']:>6.2%} ${m['net_pnl']:>8.2f} ${m['max_dd']:>6.2f}")
    print()

    # ---- GATES ----
    print("=" * 78)
    print("QUALIFICATION GATES (backtesting.md)")
    print("=" * 78)
    gates = []

    def gate(name, cond, note=""):
        status = "PASS" if cond else "FAIL"
        gates.append((name, status, note))
        print(f"[{status}] {name}" + (f"  -- {note}" if note else ""))

    # 1. Positive net PnL in >=3 of 4 OOS windows
    gate("1. Positive net PnL in >=3/4 OOS windows",
         base_m["positive_windows"] >= 3,
         f"{base_m['positive_windows']}/4 positive")

    # 2. Block sign-flip significant
    if base_m["block_p"] is None:
        gate("2. Block sign-flip p < 0.05", False, "unavailable (<48 trades)")
    else:
        gate("2. Block sign-flip p < 0.05", base_m["block_p"] < 0.05,
             f"p={base_m['block_p']:.4f}")

    # 3. Lead rate < 50% OR continuation WR > 54% with counter-lead WR >= 50%
    cond3 = (base_m["lead_rate"] is not None and base_m["lead_rate"] < 0.5) or \
            (base_m["wr"] > 0.54 and (base_m["counter_lead_wr"] is None or base_m["counter_lead_wr"] >= 0.5))
    gate("3. Lead rate <50% OR (WR>54% & counter-lead WR>=50%)",
         cond3,
         f"lead={base_m['lead_rate']}, WR={base_m['wr']:.2%}, counter-lead WR={base_m['counter_lead_wr']}")

    # 4. Inverted (placebo) WR significantly lower than base (< 40%)
    gate("4. Inverted/placebo WR < 40%", base_m["inverted_wr"] < 0.40,
         f"inverted WR={base_m['inverted_wr']:.2%}")

    # 5. Survive stress with positive net PnL
    gate("5. Survive cost stress (slip x2 + 25bp + latency+500 + fill -20%)",
         stress_m["net_pnl"] > 0,
         f"stress net=${stress_m['net_pnl']:+.4f}")

    # 6. Max DD <= $6 kill-switch
    gate("6. Max drawdown <= $6 kill-switch", base_m["max_dd"] <= KILL_SWITCH,
         f"maxDD=${base_m['max_dd']:.4f}")

    # 7. Parameter plateau (not isolated spike)
    profitable_neighbors = sum(1 for p, m in sweep if p >= 0.70 and m["net_pnl"] > 0)
    gate("7. Parameter plateau (>=4 of 0.70-0.95 profitable)",
         profitable_neighbors >= 4,
         f"{profitable_neighbors}/6 profitable in 0.70-0.95")

    # 8. Fill rate 50% still profitable
    gate("8. Fill rate 50% still profitable", fill50_m["net_pnl"] > 0,
         f"50%-fill net=${fill50_m['net_pnl']:+.4f}")

    # 9. Overlap < 30% — single strategy, N/A
    gate("9. Cross-strategy overlap < 30%", True, "N/A (single strategy)")

    # Additional: bootstrap CI lower > breakeven
    if base_m["bootstrap_lo"] is not None:
        gate("A. Bootstrap CI lower > breakeven (95%)",
             base_m["bootstrap_lo"] > BREAKEVEN,
             f"CI lower={base_m['bootstrap_lo']:.2%}")

    # Additional: max lose streak <= 3
    gate("B. Max lose streak <= 3", base_m["max_lose_streak"] <= 3,
         f"streak={base_m['max_lose_streak']}")

    # Additional: walk-forward decay <= 25%
    if base_m["decay"] is not None:
        gate("C. Walk-forward decay <= 25%", base_m["decay"] <= 0.25,
             f"decay={base_m['decay']:.2%}")

    print()
    fails = [g for g in gates if g[1] == "FAIL"]
    print(f"Gates: {len(gates) - len(fails)}/{len(gates)} PASS")
    if fails:
        print("FAILED GATES:")
        for name, _, note in fails:
            print(f"  - {name} ({note})")
        print("VERDICT: FAIL — not ready for live.")
    else:
        print("VERDICT: PASS / READY — all gates cleared.")

    # save
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps({
        "baseline": base_m, "stress": stress_m, "fill50": fill50_m,
        "sweep": [{"price": p, **m} for p, m in sweep],
        "gates": [{"name": g[0], "status": g[1], "note": g[2]} for g in gates],
        "verdict": "PASS" if not fails else "FAIL",
    }, indent=2, default=str))
    print(f"\nFull report saved: {out.resolve()}")


def main():
    parser = argparse.ArgumentParser(description="Rigorous gate-check backtest on recorded Polymarket CLOB data.")
    parser.add_argument("--input", default="data/market_data.jsonl")
    parser.add_argument("--days", type=int, default=14)
    parser.add_argument("--seed", type=int, default=41)
    parser.add_argument("--latency-ms", type=int, default=150)
    parser.add_argument("--out", default="results/rigorous_report.json")
    args = parser.parse_args()
    run_all(args)


if __name__ == "__main__":
    main()
