"""Stage 2: Real data loaders (Binance klines + approximated Polymarket books).

NOTE: Polymarket books are approximated from Binance via Black-Scholes binary
MM. Settlement uses Binance TWAP (approximates Chainlink). Model-based only.
"""

import time
import math
from typing import List, Dict, Optional
from scipy.stats import norm
import requests


def fetch_binance_klines(symbol: str = "BTCUSDT", interval: str = "1m", limit: int = 1000, start_time_ms: Optional[int] = None) -> List[Dict]:
    url = "https://api.binance.com/api/v3/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    if start_time_ms is not None:
        params["startTime"] = start_time_ms
    try:
        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
        out = []
        for k in r.json():
            out.append({
                "open_time": k[0],
                "open": float(k[1]),
                "high": float(k[2]),
                "low": float(k[3]),
                "close": float(k[4]),
                "volume": float(k[5]),
                "taker_buy_base": float(k[9]),
            })
        return out
    except Exception:
        return []


def bs_binary_mm_prob(spot: float, strike: float, vol_1s: float, remaining_sec: float) -> float:
    tau = max(1.0, remaining_sec)
    if vol_1s <= 0 or strike <= 0:
        return 0.5
    z = math.log(spot / strike) / (vol_1s * math.sqrt(tau) + 1e-9)
    return float(norm.cdf(z))


def build_real_windows_from_binance(num_days: int = 7) -> List[Dict]:
    candles_needed = num_days * 24 * 60
    all_candles = []
    end = int(time.time() * 1000)
    while len(all_candles) < candles_needed:
        batch = fetch_binance_klines(limit=1000, start_time_ms=end - 1000 * 60 * 1000)
        if not batch:
            break
        all_candles = batch + all_candles
        end = batch[0]["open_time"] - 1
        if len(all_candles) >= candles_needed:
            break
    all_candles = all_candles[-candles_needed:]

    # align to 5-min boundaries (epoch seconds % 300 == 0)
    # find first candle whose open_time is on a 5-min boundary
    start_idx = 0
    for i, c in enumerate(all_candles):
        if c["open_time"] % 300000 == 0:
            start_idx = i
            break
    aligned = all_candles[start_idx:]

    # per-window realized vol from the same window's minute closes
    windows = []
    i = 0
    n = len(aligned)
    while i + 5 <= n:
        chunk = aligned[i:i + 5]
        strike = chunk[0]["open"]
        closes = [c["close"] for c in chunk]
        rets = [math.log(closes[j] / closes[j - 1]) for j in range(1, len(closes)) if closes[j - 1] > 0]
        sigma_1m = (sum(r * r for r in rets) / len(rets)) ** 0.5 if rets else 0.008
        vol_1s = sigma_1m / math.sqrt(60.0)

        end_twap = closes[-1]
        outcome_up = 1 if end_twap >= strike else 0

        snaps = []
        for k in range(5):
            c = chunk[k]
            elapsed = (k + 1) * 60
            remaining = 300 - elapsed
            if remaining <= 0:
                remaining = 1
            spot = c["close"]
            mm_prob = bs_binary_mm_prob(spot, strike, vol_1s, remaining)
            spread = 0.012 + 0.008 * (1.0 - remaining / 300.0)
            up_bid = max(0.001, mm_prob - spread / 2)
            up_ask = min(0.999, mm_prob + spread / 2)
            down_bid = max(0.001, (1 - mm_prob) - spread / 2)
            down_ask = min(0.999, (1 - mm_prob) + spread / 2)
            ofi = (2.0 * c["taker_buy_base"] / c["volume"]) - 1.0 if c["volume"] > 0 else 0.0
            lag_ret = math.log(spot / strike) if strike > 0 else 0.0
            obi = max(-0.6, min(0.6, 0.35 * (1 if lag_ret > 0 else -1 if lag_ret < 0 else 0) + 0.1 * ofi))
            snaps.append({
                "elapsed": elapsed,
                "remaining": remaining,
                "spot": spot,
                "start_twap": strike,
                "curr_twap": spot,
                "realized_vol_1s": vol_1s,
                "order_flow_imbalance": ofi,
                "up_bid": up_bid,
                "up_ask": up_ask,
                "down_bid": down_bid,
                "down_ask": down_ask,
                "mm_prob": mm_prob,
                "bid_sizes": [10.0 * (1 + obi), 8.0 * (1 + obi), 6.0 * (1 + obi)],
                "ask_sizes": [10.0 * (1 - obi), 8.0 * (1 - obi), 6.0 * (1 - obi)],
            })
        from datetime import datetime, timezone
        dt = datetime.fromtimestamp(chunk[0]["open_time"] / 1000.0, tz=timezone.utc)
        windows.append({
            "window_start_ts": chunk[0]["open_time"] // 1000,
            "start_twap": strike,
            "end_twap": end_twap,
            "outcome_up": outcome_up,
            "quarter_label": f"{dt.year}-Q{(dt.month - 1) // 3 + 1}",
            "snapshots": snaps,
        })
        i += 5
    return windows


def fetch_polymarket_prices_history(token_id: str, interval: str = "1m", fidelity: int = 1) -> List[Dict]:
    url = "https://clob.polymarket.com/prices-history"
    params = {"market": token_id, "interval": interval, "fidelity": fidelity}
    try:
        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
        data = r.json()
        return data.get("history", data) if isinstance(data, dict) else data
    except Exception:
        return []


def load_recorded_windows(jsonl_dir: str = "data") -> List[Dict]:
    import glob, os, json
    windows_map: Dict[int, List[dict]] = {}
    for path in glob.glob(os.path.join(jsonl_dir, "live_snapshots_*.jsonl")):
        with open(path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                rec = json.loads(line)
                wts = rec.get("window")
                if wts is None:
                    continue
                windows_map.setdefault(wts, []).append(rec)
    out = []
    for wts, recs in windows_map.items():
        recs.sort(key=lambda r: r.get("ts", 0))
        snaps = []
        for r in recs:
            ts = r.get("ts", wts)
            elapsed = max(0.0, min(300.0, float(ts) - float(wts)))
            remaining = max(0.0, 300.0 - elapsed)
            snaps.append({
                "elapsed": elapsed,
                "remaining": remaining,
                "spot": r.get("price", 0.0),
                "start_twap": recs[0].get("twap", 0.0) if recs else 0.0,
                "curr_twap": r.get("twap", 0.0),
                "realized_vol_1s": 0.0001,
                "order_flow_imbalance": 0.0,
                "up_bid": r.get("up_bid", 0.5),
                "up_ask": r.get("up_ask", 0.5),
                "down_bid": r.get("down_bid", 0.5),
                "down_ask": r.get("down_ask", 0.5),
            })
        out.append({
            "window_start_ts": wts,
            "start_twap": recs[0].get("twap", 0.0) if recs else 0.0,
            "end_twap": recs[-1].get("twap", 0.0) if recs else 0.0,
            "outcome_up": 1 if (recs and recs[-1].get("twap", 0) >= recs[0].get("twap", 0)) else 0,
            "snapshots": snaps,
        })
    return out
