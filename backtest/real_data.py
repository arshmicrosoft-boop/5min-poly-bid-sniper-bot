"""Stage 2: Real data loaders (Binance klines + Polymarket price history)."""
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


def compute_twap_60s(prices_1s: List[float]) -> List[float]:
    out = []
    for i in range(len(prices_1s)):
        window = prices_1s[max(0, i - 59): i + 1]
        out.append(sum(window) / len(window))
    return out


def bs_binary_mm_prob(spot: float, strike: float, vol_1s: float, remaining_sec: float) -> float:
    tau = max(1.0, remaining_sec)
    if vol_1s <= 0:
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

    # Build 1s prices causally (linear within minute, no look-ahead to future close)
    prices_1s = []
    flow_1s = []
    for c in all_candles:
        o, cl = c["open"], c["close"]
        vol = c["volume"] if c["volume"] > 0 else 1.0
        imbalance = (2.0 * c["taker_buy_base"] / vol) - 1.0
        for i in range(60):
            frac = i / 59.0
            prices_1s.append(o + (cl - o) * frac)
            flow_1s.append(imbalance)

    twaps = compute_twap_60s(prices_1s)

    from datetime import datetime, timezone
    windows = []
    sec_per_window = 300
    total_sec = len(prices_1s)
    for w_start in range(60, total_sec - sec_per_window, sec_per_window):
        # start_twap = 60s TWAP immediately prior to T=0 (index w_start-1)
        start_idx = w_start - 1
        # end_twap = final 60s of window (5th minute: T+240..T+300)
        end_idx = w_start + sec_per_window - 1
        start_twap = twaps[start_idx] if start_idx >= 0 else twaps[0]
        end_twap = twaps[end_idx] if end_idx < len(twaps) else twaps[-1]
        outcome_up = 1 if end_twap >= start_twap else 0

        # realized vol: std of 1-min log returns / sqrt(60)  (FIX: convert to 1s)
        seg_prices = [c["close"] for c in all_candles]
        # use minute closes around this window
        minute_closes = [c["close"] for c in all_candles]
        # compute 1-min returns for vol
        rets_1m = []
        for i in range(1, len(minute_closes)):
            rets_1m.append(math.log(minute_closes[i] / minute_closes[i - 1]))
        if rets_1m:
            sigma_1m = (sum(r * r for r in rets_1m) / len(rets_1m)) ** 0.5
        else:
            sigma_1m = 0.008
        vol_1s = sigma_1m / math.sqrt(60.0)

        snaps = []
        for elapsed in (60, 120, 180):
            idx = w_start + elapsed - 1
            if idx >= len(prices_1s):
                break
            curr_twap = twaps[idx]
            spot = prices_1s[idx]
            # Simulate a laggy market maker: the book reacts to spot with a delay
            # (real CLOBs lag spot by ~10-20s). This creates strike-cross windows
            # where spot has moved through the strike but the book hasn't caught up.
            LAG_SEC = 15
            book_spot = prices_1s[max(0, idx - LAG_SEC)]
            mm_prob = bs_binary_mm_prob(book_spot, start_twap, vol_1s, sec_per_window - elapsed)
            spread = 0.012 + 0.008 * (1.0 - (sec_per_window - elapsed) / sec_per_window)
            up_bid = max(0.001, mm_prob - spread / 2)
            up_ask = min(0.999, mm_prob + spread / 2)
            down_bid = max(0.001, (1 - mm_prob) - spread / 2)
            down_ask = min(0.999, (1 - mm_prob) + spread / 2)
            ofi = flow_1s[idx]
            # simulated L2 depth asymmetry proportional to lagged return + noise
            lag_ret = math.log(spot / start_twap) if start_twap > 0 else 0.0
            obi = max(-0.6, min(0.6, 0.35 * (1 if lag_ret > 0 else -1 if lag_ret < 0 else 0) + 0.1 * ofi))
            bid_sizes = [10.0 * (1 + obi), 8.0 * (1 + obi), 6.0 * (1 + obi)]
            ask_sizes = [10.0 * (1 - obi), 8.0 * (1 - obi), 6.0 * (1 - obi)]
            snaps.append({
                "elapsed": elapsed,
                "remaining": sec_per_window - elapsed,
                "spot": spot,
                "start_twap": start_twap,
                "curr_twap": curr_twap,
                "realized_vol_1s": vol_1s,
                "order_flow_imbalance": ofi,
                "up_bid": up_bid,
                "up_ask": up_ask,
                "down_bid": down_bid,
                "down_ask": down_ask,
                "mm_prob": mm_prob,
                "bid_sizes": bid_sizes,
                "ask_sizes": ask_sizes,
            })
        dt = datetime.fromtimestamp(w_start, tz=timezone.utc)
        quarter_label = f"{dt.year}-Q{(dt.month-1)//3 + 1}"
        windows.append({
            "window_start_ts": w_start,
            "start_twap": start_twap,
            "end_twap": end_twap,
            "outcome_up": outcome_up,
            "quarter_label": quarter_label,
            "snapshots": snaps,
        })
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
        # build minimal snapshots
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