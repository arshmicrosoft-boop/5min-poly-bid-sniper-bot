"""Record real Binance/Polymarket data and replay the 95-cent maker strategy."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import heapq
import json
import math
import os
import random
import sys
import time
from pathlib import Path

import requests

GAMMA = "https://gamma-api.polymarket.com"
PM_WS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
BINANCE_WS = "wss://stream.binance.com:9443/ws/btcusdt@kline_1s"
PM_REPLAY_EVENTS = {"book", "price_change", "last_trade_price", "market_resolved"}
MARKET_SECONDS = 300
DEFAULT_THRESHOLD = 0.95
DEFAULT_CAP = 0.95
DEFAULT_WINDOW = 60
DEFAULT_STOP = 5
DEFAULT_RECHECK = 3
DEFAULT_BET_USD = 1.0
DEFAULT_MIN_SHARES = 5.0
DEFAULT_LATENCY_MS = 150


def load_dataset(path):
    markets = {}
    binance = []
    snapshot_size = path.stat().st_size
    with path.open("rb") as fp:
        while fp.tell() < snapshot_size:
            raw_line = fp.readline()
            if not raw_line.endswith(b"\n"):
                break
            try:
                row = json.loads(raw_line)
            except json.JSONDecodeError:
                continue
            if row.get("kind") == "market":
                markets[row["slug"]] = {"meta": row, "events": [], "binance": []}
            elif row.get("kind") == "polymarket_event":
                slug = row.get("market_slug")
                if slug:
                    markets.setdefault(slug, {"meta": {"slug": slug}, "events": [], "binance": []})["events"].append(row)
            elif row.get("kind") == "binance_candle":
                binance.append(row)
    binance.sort(key=lambda x: x.get("event_ms", 0))
    binance_times = [x.get("event_ms", 0) for x in binance]
    import bisect
    for entry in markets.values():
        meta = entry["meta"]
        if not meta.get("start_ms") or not meta.get("asset_ids"):
            continue
        lo = bisect.bisect_left(binance_times, meta["start_ms"] - 60_000)
        hi = bisect.bisect_right(binance_times, meta["end_ms"] + 60_000)
        entry["binance"] = binance[lo:hi]
    return markets


def binance_window_prices(entry):
    meta = entry["meta"]
    trades = sorted(entry.get("binance", []), key=lambda x: x.get("event_ms", 0))
    start = end = None
    for candle in trades:
        if candle["event_ms"] <= meta["start_ms"]:
            start = candle["close"]
        if candle["event_ms"] <= meta["end_ms"]:
            end = candle["close"]
    return start, end


def fee_per_share(price, notional_rate=0.0):
    return price * notional_rate


def wilson_interval(wins, total, z=1.959963984540054):
    if total == 0:
        return None, None
    p = wins / total
    denom = 1 + z * z / total
    center = (p + z * z / (2 * total)) / denom
    half = z * math.sqrt((p * (1 - p) + z * z / (4 * total)) / total) / denom
    return max(0.0, center - half), min(1.0, center + half)


def run_backtest(path, latency_ms=DEFAULT_LATENCY_MS, slip_multiplier=1.0,
                 fee_rate=0.0, fill_rate=1.0, seed=41, bankroll=10.0, days=14,
                 threshold=DEFAULT_THRESHOLD, cap=DEFAULT_CAP, dataset=None):
    markets = dataset if dataset is not None else load_dataset(path)
    dated = [v["meta"].get("start_ms", 0) for v in markets.values() if v["meta"].get("start_ms")]
    if dated and days:
        cutoff = max(dated) - days * 86_400_000
        markets = {k: v for k, v in markets.items() if v["meta"].get("start_ms", 0) >= cutoff}
    ordered = sorted(markets.values(), key=lambda v: v["meta"].get("start_ms", 0))
    cash = bankroll
    peak = bankroll
    capital_skips = 0
    kill_switch = False
    markets_with_orders = 0
    markets_with_fills = 0
    markets_with_resolution = 0
    trades = []
    for i, entry in enumerate(ordered):
        if peak - cash >= 6.0:
            kill_switch = True
            break
        meta = entry["meta"]
        if not meta.get("start_ms") or not meta.get("asset_ids"):
            continue
        # parse events into book state
        assets = meta["asset_ids"]
        asset_to_side = {str(v): k for k, v in assets.items()}
        bids = {"UP": {}, "DOWN": {}}
        asks = {"UP": {}, "DOWN": {}}
        winner = None
        # collect events sorted
        evs = sorted(entry["events"], key=lambda r: r.get("received_ms", 0))
        # decision points every recheck
        start_ms = meta["start_ms"]
        end_ms = meta["end_ms"]
        # We'll iterate events + decisions via simple loop
        selected = None
        order = None
        pending = False
        entered = False
        trade = None
        order_attempts = 0
        fill_checks = 0
        # build event list with decisions
        market_events = []
        seq = 0
        for row in evs:
            market_events.append((row.get("received_ms", 0), 0, seq, "pm", row))
            seq += 1
        for t in range(start_ms + (300 - 60) * 1000, end_ms - 5 * 1000 + 1, 3 * 1000):
            market_events.append((t, 1, seq, "dec", None))
            seq += 1
        import heapq
        heapq.heapify(market_events)
        rng = random.Random(seed + i)
        while market_events:
            t, _, _, kind, payload = heapq.heappop(market_events)
            if t < start_ms or (t > end_ms and kind != "pm"):
                continue
            if kind == "pm":
                row = payload
                ev = row.get("event", {})
                p = ev if not isinstance(ev.get("payload"), dict) else ev.get("payload")
                etype = ev.get("event_type") or ev.get("type") or ""
                received = int(row.get("received_ms", 0))
                asset = str(p.get("asset_id", p.get("assetId", "")))
                side = asset_to_side.get(asset)
                if side:
                    if etype == "book":
                        bids[side] = {float(x["price"]): float(x["size"]) for x in p.get("bids", []) if float(x.get("size", 0)) > 0}
                        asks[side] = {float(x["price"]): float(x["size"]) for x in p.get("asks", []) if float(x.get("size", 0)) > 0}
                    elif etype == "price_change":
                        changes = p.get("price_changes", p.get("priceChanges", []))
                        if not changes:
                            changes = [p]
                        for ch in changes:
                            a = str(ch.get("asset_id", ch.get("assetId", "")))
                            s = asset_to_side.get(a)
                            if not s:
                                continue
                            book = bids[s] if str(ch.get("side", "")).upper() == "BUY" else asks[s]
                            price = float(ch["price"])
                            size = float(ch.get("size", 0) or 0)
                            if size > 0:
                                book[price] = size
                            else:
                                book.pop(price, None)
                if etype == "last_trade_price" and order is not None and not entered:
                    asset = str(p.get("asset_id", p.get("assetId", "")))
                    side = asset_to_side.get(asset)
                    taker_side = str(p.get("side", "")).upper()
                    price = float(p.get("price", 0) or 0)
                    qty = float(p.get("size", 0) or 0)
                    if side == order["side"] and taker_side == "SELL" and t >= order["live_ms"]:
                        queue_before = order["queue"]
                        if price > order["price"]:
                            order["queue"] = max(0.0, order["queue"] - qty)
                        elif price < order["price"]:
                            order["queue"] = 0.0
                            queue_before = 0.0
                        elif price == order["price"]:
                            order["queue"] = max(0.0, order["queue"] - qty)
                        if price <= order["price"] and order["queue"] <= 1e-9:
                            fill_checks += 1
                            if rng.random() <= fill_rate:
                                filled = min(order["shares"] - order.get("filled", 0.0), max(0.0, qty - queue_before))
                                if filled > 0:
                                    if trade is None:
                                        trade = {"side": order["side"], "price": order["price"], "shares": 0.0, "fill_ms": t,
                                                "queue_ahead_at_submit": order["queue_initial"]}
                                    trade["shares"] += filled
                                    trade["fill_ms"] = t
                                    order["filled"] = order.get("filled", 0.0) + filled
                                    entered = True
                                    if order["filled"] >= order["shares"] - 1e-9:
                                        order = None
                elif etype == "market_resolved":
                    winner = str(p.get("winning_asset_id", p.get("winningAssetId", "")))
            elif kind == "dec":
                if entered or pending or not (start_ms + 240000 <= t <= end_ms - 5000):
                    continue
                def bbo(side):
                    bl, al = bids[side], asks[side]
                    if not bl or not al:
                        return None
                    return max(bl), min(al)
                # Allow switching decided side if the current one stops qualifying.
                if selected is not None:
                    q = bbo(selected)
                    if not q or q[1] < threshold:
                        selected = None
                if selected is None:
                    qualifying = []
                    up = bbo("UP"); down = bbo("DOWN")
                    if up and up[1] >= threshold:
                        qualifying.append(("UP", up[1]))
                    if down and down[1] >= threshold:
                        qualifying.append(("DOWN", down[1]))
                    if qualifying:
                        selected = max(qualifying, key=lambda x: x[1])[0]
                if selected is None:
                    continue
                # Final 5 seconds: cancel any resting order, no new submits.
                if t >= end_ms - 5000:
                    if order is not None and not order.get("cancel_pending"):
                        order["cancel_pending"] = True
                        pending = True
                        heapq.heappush(market_events, (t + latency_ms + 1500, 2, seq, "act", ("cancel", None)))
                        seq += 1
                    continue
                quote = bbo(selected)
                desired = min(quote[0], cap) if (quote and quote[1] >= threshold) else None
                if order is not None:
                    if desired is None or abs(order["price"] - desired) > 0.005:
                        if not order.get("cancel_pending"):
                            order["cancel_pending"] = True
                            pending = True
                            heapq.heappush(market_events, (t + latency_ms + 1500, 2, seq, "act", ("cancel", desired)))
                            seq += 1
                    continue
                if desired is not None:
                    pending = True
                    heapq.heappush(market_events, (t + latency_ms, 2, seq, "act", ("submit", desired)))
                    seq += 1
            elif kind == "act":
                pending = False
                action, price = payload
                if action == "cancel":
                    # cancel completes after latency; order may have taken toxic fills
                    order = None
                    if price is not None and not entered and t < end_ms - 5000:
                        # resubmit at desired price after cancel-confirm wait
                        heapq.heappush(market_events, (t + 1500, 2, seq, "act", ("submit", price)))
                        seq += 1
                elif action == "submit":
                    if entered or t >= end_ms - 5000 or price is None:
                        continue
                    quote = bbo(selected) if selected else None
                    if not quote or quote[1] < threshold or quote[0] is None:
                        continue
                    if quote[1] <= price:
                        order_attempts += 1
                        continue
                    shares = max(5.0, math.ceil((1.0 / price) * 100) / 100.0)
                    queue = sum(size for px, size in bids[selected].items() if px >= price)
                    order = {"side": selected, "price": price, "shares": shares,
                             "queue": queue, "queue_initial": queue, "live_ms": t, "filled": 0.0}
                    order_attempts += 1
        # end while

        markets_with_orders += order_attempts > 0
        markets_with_fills += trade is not None
        markets_with_resolution += winner is not None
        if not trade or not winner:
            continue
        side_asset = assets.get(trade["side"])
        won = str(side_asset) == str(winner)
        start_btc, end_btc = binance_window_prices(entry)
        slippage = max(0.001, (latency_ms / 1000.0) * 0.0045) * slip_multiplier
        fee = fee_per_share(trade["price"], fee_rate)
        cost = trade["shares"] * (trade["price"] + fee + slippage)
        payout = trade["shares"] * (1.0 if won else 0.0)
        if cost > cash:
            capital_skips += 1
            continue
        cash += payout - cost
        peak = max(peak, cash)
        trade.update({"won": won, "net_pnl": payout - cost, "cost": cost,
                      "fees": fee * trade["shares"], "slippage": slippage * trade["shares"],
                      "binance_start": start_btc, "binance_end": end_btc,
                      "binance_up": (end_btc >= start_btc) if (start_btc is not None and end_btc is not None) else None,
                      "shares": trade["shares"], "entry_price": trade["price"],
                      "start_ms": meta["start_ms"]})
        trades.append(trade)
    trades.sort(key=lambda t: t["start_ms"])
    trades.append({"_bankroll_end": cash, "_capital_skips": capital_skips,
                   "_markets_with_orders": markets_with_orders,
                   "_markets_with_fills": markets_with_fills,
                   "_markets_with_resolution": markets_with_resolution,
                   "_kill_switch": kill_switch or peak - cash >= 6.0})
    return trades


def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    rec = sub.add_parser("record")
    rec.add_argument("--output", default="data/market_data.jsonl")
    rep = sub.add_parser("report")
    rep.add_argument("--input", default="data/market_data.jsonl")
    rep.add_argument("--latency-ms", type=int, default=DEFAULT_LATENCY_MS)
    rep.add_argument("--days", type=int, default=14)
    rep.add_argument("--seed", type=int, default=41)
    rep.add_argument("--trades-out", default="results/trades.csv")
    args = parser.parse_args()
    if args.command == "record":
        print("Recording not implemented in this trimmed module. Use poly5m/ws_collector or original recorder.")
    else:
        print("Report: run backtest_rigorous.py instead.")


if __name__ == "__main__":
    main()
