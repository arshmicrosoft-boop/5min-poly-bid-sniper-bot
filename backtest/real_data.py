"""Fetch real BTC 1m klines from Binance and build approximate 5-min windows.

NOTE: Polymarket books are NOT available here, so UP/DOWN ask/bid are derived
from the Binance spot-vs-strike probability. Settlement uses Binance close vs
open (an approximation of Chainlink TWAP). Results from this are indicative only.
"""

import time, math, requests
from datetime import datetime, timezone


def fetch_binance_klines(start_ms, end_ms):
    out = []
    cur = start_ms
    while cur < end_ms:
        r = requests.get("https://api.binance.com/api/v3/klines",
                         params={"symbol": "BTCUSDT", "interval": "1m",
                                 "startTime": cur, "endTime": end_ms, "limit": 1000},
                         timeout=15)
        r.raise_for_status()
        data = r.json()
        if not data:
            break
        out.extend(data)
        cur = data[-1][0] + 60000
        time.sleep(0.2)
    return out


def _prob_up(spot, strike):
    # simple: probability up scales with % move, clamped
    if strike <= 0:
        return 0.5
    pct = (spot - strike) / strike * 100.0
    # ~0.5% move => ~0.85 prob
    p = 0.5 + max(-0.45, min(0.45, pct * 0.18))
    return p


def build_real_windows_from_binance(num_days=30):
    end_ms = int(time.time() * 1000)
    start_ms = end_ms - num_days * 86400 * 1000
    klines = fetch_binance_klines(start_ms, end_ms)
    # index by minute
    by_min = {int(k[0]): k for k in klines}
    windows = []
    # 5-min boundaries aligned to epoch
    first = (start_ms // 300000) * 300000
    ts = first
    while ts + 300000 <= end_ms:
        mins = [ts + i * 60000 for i in range(5)]
        cands = [by_min.get(m) for m in mins]
        if any(c is None for c in cands):
            ts += 300000
            continue
        strike = float(cands[0][1])  # open of first minute
        # snapshots at elapsed 0,60,120,180,240
        snaps = []
        for i in range(5):
            c = cands[i]
            spot = float(c[4])  # close
            elapsed = i * 60
            remaining = 300 - elapsed
            p = _prob_up(spot, strike)
            up_ask = min(0.99, max(0.01, p + 0.02))
            down_ask = min(0.99, max(0.01, (1 - p) + 0.02))
            up_bid = max(0.01, up_ask - 0.01)
            down_bid = max(0.01, down_ask - 0.01)
            snaps.append({
                "elapsed": elapsed, "remaining": remaining,
                "start_twap": strike, "spot": spot,
                "up_ask": up_ask, "up_bid": up_bid,
                "down_ask": down_ask, "down_bid": down_bid,
            })
        outcome_up = float(cands[-1][4]) >= strike
        windows.append({"snapshots": snaps, "outcome_up": outcome_up})
        ts += 300000
    return windows
