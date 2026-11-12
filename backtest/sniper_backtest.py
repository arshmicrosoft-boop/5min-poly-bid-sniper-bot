"""MoonDev sniper strategies backtest: Strike-Cross + 95-cent bid sniper."""
import os, json, math
from typing import List, Dict, Any
import numpy as np
from backtest.real_data import build_real_windows_from_binance


def run_strike_cross(days=30):
    windows = build_real_windows_from_binance(num_days=days)
    MIN_ITM = 0.10  # %
    PRICE_BAND = (0.30, 0.70)
    TAKER_FEE = 0.07 * 0.5 * 0.5  # per share approx
    SHARES = 5.0
    trades=0; wins=0; net=0.0; details=[]
    for w in windows:
        for snap in w["snapshots"]:
            elapsed = snap["elapsed"]; remaining = snap["remaining"]
            if remaining < 45 or remaining > 270:
                continue
            start = snap["start_twap"]; spot = snap["spot"]
            if start <= 0: continue
            itm_pct = (spot - start)/start*100.0
            if abs(itm_pct) < MIN_ITM:
                continue
            side = "UP" if itm_pct > 0 else "DOWN"
            ask = snap["up_ask"] if side=="UP" else snap["down_ask"]
            if ask < PRICE_BAND[0] or ask > PRICE_BAND[1]:
                continue
            # entry taker
            entry = ask
            fee_slip = (TAKER_FEE + 0.001)*SHARES
            cost = SHARES*entry + fee_slip
            outcome_up = bool(w["outcome_up"])
            win = (side=="UP") == outcome_up
            pnl = (SHARES*1.0 - cost) if win else -cost
            trades+=1
            if win: wins+=1
            net+=pnl
            details.append({"side":side,"win":win,"itm":itm_pct,"ask":ask,"pnl":pnl})
            break  # one per window
    wr = wins/trades if trades else 0.0
    print("="*70)
    print("STRIKE-CROSS SNIPER (ITM>=0.10%%, ask 0.30-0.70, 45-270s left)")
    print("="*70)
    print("Windows: %d | Trades: %d | Wins: %d | WR: %.2f%% | Net PnL: $%+.2f" % (len(windows), trades, wins, wr*100, net))
    print("="*70)
    return {"trades":trades,"wins":wins,"wr":wr,"net":net,"details":details}


def run_95cent(days=30, fill_rate=0.7, seed=41):
    import random
    rng = random.Random(seed)
    windows = build_real_windows_from_binance(num_days=days)
    THRESH = 0.95
    SHARES = 5.0
    trades=0; wins=0; net=0.0; details=[]
    for w in windows:
        # only consider snapshots in the final 60 seconds
        late_snaps = [s for s in w["snapshots"] if s["remaining"] <= 60]
        if not late_snaps:
            continue
        snap = late_snaps[-1]
        remaining = snap["remaining"]
        if remaining > 60:
            continue
        for side in ("UP","DOWN"):
            ask = snap["up_ask"] if side=="UP" else snap["down_ask"]
            bid = snap["up_bid"] if side=="UP" else snap["down_bid"]
            if ask >= THRESH:
                # model passive fill: not guaranteed; apply fill_rate
                if rng.random() > fill_rate:
                    break
                entry = min(bid, THRESH)
                if entry < 0.01: entry=0.01
                cost = SHARES*entry  # maker, 0 fee (stress adds fees separately)
                outcome_up = bool(w["outcome_up"])
                win = (side=="UP") == outcome_up
                pnl = (SHARES*1.0 - cost) if win else -cost
                trades+=1
                if win: wins+=1
                net+=pnl
                details.append({"side":side,"win":win,"entry":entry,"ask":ask,"pnl":pnl,"remaining":remaining})
                break  # one per window
    wr = wins/trades if trades else 0.0
    print("="*70)
    print("95-CENT BID SNIPER (ask>=0.95, maker bid<=0.95, ~5.26%%/win)")
    print("="*70)
    print("Windows: %d | Trades: %d | Wins: %d | WR: %.2f%% | Net PnL: $%+.2f" % (len(windows), trades, wins, wr*100, net))
    print("="*70)
    return {"trades":trades,"wins":wins,"wr":wr,"net":net,"details":details}


if __name__ == "__main__":
    print("--- Strike-Cross ---")
    run_strike_cross(days=7)
    print("--- 95-Cent ---")
    run_95cent(days=7)
