"""Recorder for Polymarket 5-minute crypto up/down markets.

Writes the replay format that ``backtest/backtest.py`` consumes (JSONL, one
object per line):

    {"kind": "market", ...}            one row per market, always written first
    {"kind": "polymarket_event", ...}  book snapshots, trade prints, resolutions
    {"kind": "binance_candle", ...}    1m candles, used only for the Binance
                                       agreement diagnostic

Why the file does not contain the raw tape
------------------------------------------
These markets emit ~600 ``price_change`` messages per second per window. Storing
them verbatim is ~90 GB/day and makes the consumer a "slow consumer" that
Polymarket disconnects. Instead the recorder keeps the full book in memory and
writes a *snapshot of the filtered book* once per second (and only when it
actually changed):

* every bid level >= ``KEEP_HIGH`` (0.85),
* every ask level <= ``KEEP_LOW`` (0.15),
* plus the current best bid and best ask at any price.

That is exactly the information the strategy reads (best bid/ask, queue at the
touch, opposite-side bid) at the same 1s cadence the live bot polls at, so a
replay sees the bot's real information set. Trade prints are recorded event by
event because the queue-aware fill model consumes them individually.

Design notes
------------
* One asyncio task per 5-minute window. Tasks overlap by design: the previous
  window keeps streaming so ``market_resolved`` is captured while the next
  window is already subscribing.
* Socket reads never touch the disk: rows go through a bounded queue to a
  single writer task. Dropped rows are counted and reported.
* ``received_ms`` is the exchange timestamp when the event carries a sane one,
  so replay decisions use market time rather than local network jitter.
  ``local_ms`` sits next to it so the choice stays auditable.
* The file is fsynced at every window boundary.

Usage
-----
    python poly5m/ws_collector.py --out data/market_data.jsonl
    python poly5m/ws_collector.py --duration 420 --out data/smoke.jsonl
    python poly5m/ws_collector.py --verify data/market_data.jsonl
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path

import requests
from websockets.asyncio.client import connect

PROJECT_ROOT = Path(__file__).resolve().parent.parent

MARKET_SECONDS = 300
MARKET_WS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
GAMMA_MARKETS = "https://gamma-api.polymarket.com/markets"
BINANCE_KLINES = "https://api.binance.com/api/v3/klines"

ASSETS = {"BTC": "btc", "ETH": "eth", "SOL": "sol", "XRP": "xrp", "DOGE": "doge"}
BINANCE_SYMBOL = {name: f"{name}USDT" for name in ASSETS}

KEEP_HIGH = 0.85          # record bid levels at or above this price
KEEP_LOW = 0.15           # record ask levels at or below this price
DEFAULT_BUCKET_SEC = 1.0  # book snapshot cadence, matches the live poll loop

DISCOVERY_TIMEOUT_SEC = 25.0
DISCOVERY_RETRY_SEC = 3.0
RESOLUTION_GRACE_SEC = 180.0
PING_INTERVAL_SEC = 10.0
PROGRESS_EVERY_SEC = 30.0
MAX_MESSAGE_BYTES = 8 * 1024 * 1024
RECONNECT_BACKOFF = (1.0, 2.0, 4.0, 8.0, 15.0)
HOST_SKEW_WARN_MS = 2000
CLOCK_SANITY_MS = 60_000
TRADE_DEDUPE_MAX = 5000
SINK_QUEUE_MAX = 20000


# ---------------------------------------------------------------------------
# small helpers
# ---------------------------------------------------------------------------
def now_ms() -> int:
    return int(time.time() * 1000)


def current_window(now: float | None = None) -> int:
    return int(now if now is not None else time.time()) // MARKET_SECONDS * MARKET_SECONDS


def _to_ms(value) -> int | None:
    """Parse epoch-millis, epoch-seconds, or ISO-8601 into epoch millis."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return int(value)
    text = str(value).strip()
    if not text:
        return None
    if text.isdigit():
        return int(text)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp() * 1000)


def event_time_ms(event: dict, local_ms: int) -> tuple[int, int | None]:
    """Return (timestamp_to_replay_at, observed_skew_ms)."""
    stamp = _to_ms(event.get("timestamp"))
    if stamp is None:
        return local_ms, None
    skew = stamp - local_ms
    if abs(skew) > CLOCK_SANITY_MS:
        # Garbage or a wildly wrong clock: trust the local receive time and
        # surface the skew so the run does not silently produce bad data.
        return local_ms, skew
    return stamp, skew


def _json_list(value) -> list:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except ValueError:
            return []
        return parsed if isinstance(parsed, list) else []
    return []


class JsonlWriter:
    """Append-only JSONL sink. Line buffered; only the writer task touches it."""

    def __init__(self, path: Path):
        self.path = path
        path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = open(path, "a", encoding="utf-8", buffering=1)
        self.rows = 0
        self.bytes = path.stat().st_size
        self.limit_bytes = 0

    def full(self) -> bool:
        return bool(self.limit_bytes) and self.bytes >= self.limit_bytes

    def write(self, row: dict) -> None:
        line = json.dumps(row, separators=(",", ":"), default=str)
        self._fh.write(line + "\n")
        self.rows += 1
        self.bytes += len(line) + 1

    def sync(self) -> None:
        self._fh.flush()
        try:
            os.fsync(self._fh.fileno())
        except OSError:
            pass

    def close(self) -> None:
        self.sync()
        self._fh.close()


class RowSink:
    """Bounded, non-blocking bridge between socket readers and the writer."""

    def __init__(self, writer: JsonlWriter, maxsize: int = SINK_QUEUE_MAX):
        self.writer = writer
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=maxsize)
        self.dropped = 0
        self.stopped = False

    def offer(self, row: dict) -> None:
        if self.stopped:
            self.dropped += 1
            return
        try:
            self.queue.put_nowait(row)
        except asyncio.QueueFull:
            self.dropped += 1

    async def run(self) -> None:
        while True:
            row = await self.queue.get()
            self.writer.write(row)
            self.queue.task_done()
            # Drain the backlog before touching the queue again.
            while True:
                try:
                    extra = self.queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                self.writer.write(extra)
                self.queue.task_done()
            if not self.stopped and self.writer.full():
                self.stopped = True
                print(f"[sink] disk guard hit at {self.writer.bytes / 1e6:.1f}MB; "
                      f"stopping capture", flush=True)

    async def drain(self, timeout: float = 10.0) -> None:
        try:
            await asyncio.wait_for(self.queue.join(), timeout=timeout)
        except asyncio.TimeoutError:
            print(f"[sink] {self.queue.qsize()} row(s) still queued at shutdown",
                  flush=True)


class TradeDeduper:
    """Drop replayed ``last_trade_price`` events after a reconnect.

    Duplicate trade prints would double-count taker volume and inflate the
    queue-aware fill model, so this errs on the side of dropping.
    """

    def __init__(self, maxlen: int = TRADE_DEDUPE_MAX):
        self._seen: set[tuple] = set()
        self._order: deque = deque(maxlen=maxlen)

    def seen(self, key: tuple) -> bool:
        if key in self._seen:
            return True
        if len(self._order) == self._order.maxlen:
            self._seen.discard(self._order[0])
        self._order.append(key)
        self._seen.add(key)
        return False


class BookState:
    """Full in-memory book per token; emits filtered snapshots when they change.

    The replay replaces its book wholesale on every ``book`` event, so a
    changed-state snapshot is equivalent to replaying hundreds of deltas while
    costing one row per second per token at most.
    """

    def __init__(self, bucket_sec: float = DEFAULT_BUCKET_SEC,
                 keep_high: float = KEEP_HIGH, keep_low: float = KEEP_LOW):
        self.bucket_ms = max(100, int(bucket_sec * 1000))
        self.keep_high = keep_high
        self.keep_low = keep_low
        self._bids: dict[str, dict[float, float]] = {}
        self._asks: dict[str, dict[float, float]] = {}
        self._emitted: dict[str, tuple] = {}
        self._slug_for: dict[str, str] = {}
        self._last_flush = 0

    def set_slugs(self, slug_by_token: dict[str, str]) -> None:
        self._slug_for = dict(slug_by_token)

    def apply_book(self, token: str, bids, asks) -> None:
        self._bids[token] = {
            float(level["price"]): float(level["size"])
            for level in bids or []
            if _positive(level)
        }
        self._asks[token] = {
            float(level["price"]): float(level["size"])
            for level in asks or []
            if _positive(level)
        }

    def apply_changes(self, changes) -> None:
        for change in changes or []:
            if not isinstance(change, dict):
                continue
            token = str(change.get("asset_id") or change.get("assetId") or "")
            if not token:
                continue
            try:
                price = float(change["price"])
                size = float(change.get("size") or 0)
            except (KeyError, TypeError, ValueError):
                continue
            side = str(change.get("side", "")).upper()
            book = self._bids if side == "BUY" else self._asks
            levels = book.setdefault(token, {})
            if size > 0:
                levels[price] = size
            else:
                levels.pop(price, None)

    def _view(self, token: str) -> tuple[list[tuple[float, float]], list[tuple[float, float]]]:
        bids = self._bids.get(token, {})
        asks = self._asks.get(token, {})
        kept_bids = [(p, s) for p, s in bids.items() if p >= self.keep_high]
        kept_asks = [(p, s) for p, s in asks.items() if p <= self.keep_low]
        # Always carry the touch so best bid/ask are exact at every price.
        if bids:
            best = max(bids)
            if best not in {p for p, _ in kept_bids}:
                kept_bids.append((best, bids[best]))
        if asks:
            best = min(asks)
            if best not in {p for p, _ in kept_asks}:
                kept_asks.append((best, asks[best]))
        kept_bids.sort(reverse=True)
        kept_asks.sort()
        return kept_bids, kept_asks

    def flush(self, sink: RowSink, stamp_ms: int) -> int:
        self._last_flush = stamp_ms
        written = 0
        for token in set(self._bids) | set(self._asks):
            slug = self._slug_for.get(token)
            if not slug:
                continue
            bids, asks = self._view(token)
            signature = (tuple(bids), tuple(asks))
            if self._emitted.get(token) == signature:
                continue
            self._emitted[token] = signature
            sink.offer({
                "kind": "polymarket_event",
                "market_slug": slug,
                "received_ms": stamp_ms,
                "local_ms": now_ms(),
                "event": {
                    "event_type": "book",
                    "asset_id": token,
                    "timestamp": str(stamp_ms),
                    "bids": [{"price": f"{p:.3f}", "size": f"{s:.2f}"} for p, s in bids],
                    "asks": [{"price": f"{p:.3f}", "size": f"{s:.2f}"} for p, s in asks],
                    "filtered": True,
                },
            })
            written += 1
        return written

    def flush_due(self, sink: RowSink, stamp_ms: int) -> int:
        if stamp_ms - self._last_flush < self.bucket_ms:
            return 0
        return self.flush(sink, stamp_ms)


def _positive(level) -> bool:
    try:
        return float(level.get("price", 0)) > 0 and float(level.get("size", 0) or 0) > 0
    except (TypeError, ValueError, AttributeError):
        return False


# ---------------------------------------------------------------------------
# discovery
# ---------------------------------------------------------------------------
def _fetch_market(session: requests.Session, slug: str, attempts: int = 3) -> dict | None:
    last_error = None
    for attempt in range(attempts):
        try:
            response = session.get(GAMMA_MARKETS, params={"slug": slug}, timeout=10)
            if response.status_code != 200:
                last_error = f"HTTP {response.status_code}"
            else:
                payload = response.json()
                if payload:
                    return _normalise_market(slug, payload[0])
                return None
        except Exception as exc:  # connection resets are common on Windows
            last_error = f"{type(exc).__name__}: {exc}"
        if attempt < attempts - 1:
            time.sleep(0.5 * (attempt + 1))
    if last_error:
        print(f"[discover] {slug}: {last_error}", flush=True)
    return None


def _normalise_market(slug: str, raw: dict) -> dict | None:
    tokens = [str(token) for token in _json_list(raw.get("clobTokenIds"))]
    outcomes = [str(outcome).strip().lower() for outcome in _json_list(raw.get("outcomes"))]
    if len(tokens) != 2 or len(outcomes) != 2:
        return None
    asset_ids = {}
    for token, outcome in zip(tokens, outcomes):
        if outcome.startswith("up"):
            asset_ids["UP"] = token
        elif outcome.startswith("down"):
            asset_ids["DOWN"] = token
    if set(asset_ids) != {"UP", "DOWN"}:
        return None
    return {
        "slug": slug,
        "symbol": slug.split("-")[0].upper(),
        "market_id": str(raw.get("id") or ""),
        "condition_id": str(raw.get("conditionId") or ""),
        "asset_ids": asset_ids,
        "outcomes": _json_list(raw.get("outcomes")),
        "question": raw.get("question"),
        "neg_risk": bool(raw.get("negRisk")),
        "closed": bool(raw.get("closed")),
        "accepting_orders": bool(raw.get("acceptingOrders", True)),
    }


def discover_window(window_ts: int, symbols, session: requests.Session) -> dict[str, dict]:
    found: dict[str, dict] = {}
    pending = list(symbols)
    deadline = time.time() + DISCOVERY_TIMEOUT_SEC
    while pending and time.time() < deadline:
        for symbol in list(pending):
            slug = f"{ASSETS[symbol]}-updown-5m-{window_ts}"
            market = _fetch_market(session, slug)
            if market:
                found[symbol] = market
                pending.remove(symbol)
        if pending:
            time.sleep(DISCOVERY_RETRY_SEC)
    if pending:
        print(f"[discover] {window_ts} not listed after "
              f"{DISCOVERY_TIMEOUT_SEC:.0f}s: {', '.join(pending)}", flush=True)
    return found


def market_row(window_ts: int, market: dict) -> dict:
    start_ms = window_ts * 1000
    return {
        "kind": "market",
        "slug": market["slug"],
        "symbol": market["symbol"],
        "market_id": market["market_id"],
        "condition_id": market["condition_id"],
        "asset_ids": market["asset_ids"],
        "outcomes": market["outcomes"],
        "question": market["question"],
        "neg_risk": market["neg_risk"],
        "start_ms": start_ms,
        "end_ms": start_ms + MARKET_SECONDS * 1000,
        "recorded_ms": now_ms(),
    }


# ---------------------------------------------------------------------------
# websocket streaming
# ---------------------------------------------------------------------------
class WindowCounters:
    def __init__(self) -> None:
        self.books = 0
        self.changes = 0
        self.trades = 0
        self.resolutions = 0
        self.skipped = 0
        self.duplicates = 0
        self.unknown = 0
        self.max_skew = 0
        self.reconnects = 0


def _handle_message(raw: str, slug_by_token: dict[str, str], sink: RowSink,
                    books: BookState, counters: WindowCounters,
                    deduper: TradeDeduper, resolved: set[str]) -> None:
    try:
        payload = json.loads(raw)
    except ValueError:
        return
    events = payload if isinstance(payload, list) else [payload]
    for event in events:
        if not isinstance(event, dict):
            continue
        event_type = str(event.get("event_type") or "")
        if not event_type:
            continue
        local = now_ms()
        stamp, skew = event_time_ms(event, local)
        if skew is not None:
            counters.max_skew = max(counters.max_skew, abs(skew))

        # The token can sit at the top level (book, last_trade_price), inside a
        # list (price_change carries one asset_id per change), or in assets_ids
        # (market_resolved). Collect every candidate before giving up.
        candidates = [str(event.get("asset_id") or "")]
        candidates += [str(token) for token in event.get("assets_ids") or []]
        candidates.append(str(event.get("winning_asset_id") or ""))
        for change in event.get("price_changes") or event.get("priceChanges") or []:
            if isinstance(change, dict):
                candidates.append(str(change.get("asset_id") or change.get("assetId") or ""))
        token = next((t for t in candidates if t in slug_by_token), None)
        if token is None:
            counters.unknown += 1
            continue
        slug = slug_by_token[token]

        if event_type == "book":
            books.apply_book(token, event.get("bids"), event.get("asks"))
            counters.books += 1
            continue
        if event_type == "price_change":
            books.apply_changes(event.get("price_changes") or event.get("priceChanges"))
            counters.changes += 1
            continue
        if event_type == "best_bid_ask":
            # Redundant: the periodic book snapshot already carries the touch.
            counters.skipped += 1
            continue
        if event_type == "last_trade_price":
            key = (event.get("transaction_hash"), event.get("price"),
                   event.get("size"), event.get("side"), stamp)
            if deduper.seen(key):
                counters.duplicates += 1
                continue
            counters.trades += 1
        elif event_type == "market_resolved":
            resolved.add(slug)
            counters.resolutions += 1
            winner = event.get("winning_outcome")
            if not winner:
                winner = str(event.get("winning_asset_id") or "")[:12] + "..."
            print(f"[ws] {slug} resolved -> {winner}", flush=True)

        sink.offer({
            "kind": "polymarket_event",
            "market_slug": slug,
            "received_ms": stamp,
            "local_ms": local,
            "event": event,
        })


async def _pinger(ws) -> None:
    try:
        while True:
            await asyncio.sleep(PING_INTERVAL_SEC)
            await ws.send("PING")
    except asyncio.CancelledError:
        raise
    except Exception:
        return


async def _stream(tokens: list[str], slug_by_token: dict[str, str], sink: RowSink,
                  books: BookState, counters: WindowCounters, resolved: set[str],
                  deadline: float, window_ts: int) -> None:
    slugs = set(slug_by_token.values())
    deduper = TradeDeduper()
    attempt = 0
    next_progress = time.time() + PROGRESS_EVERY_SEC
    while time.time() < deadline and len(resolved) < len(slugs):
        try:
            async with connect(MARKET_WS, ping_interval=None,
                               max_size=MAX_MESSAGE_BYTES, open_timeout=15) as ws:
                await ws.send(json.dumps({
                    "assets_ids": tokens,
                    "type": "market",
                    "custom_feature_enabled": True,
                }))
                attempt = 0
                pinger = asyncio.create_task(_pinger(ws))
                try:
                    async for raw in ws:
                        if isinstance(raw, bytes):
                            raw = raw.decode("utf-8", "replace")
                        if raw.strip() == "PONG":
                            continue
                        _handle_message(raw, slug_by_token, sink, books,
                                        counters, deduper, resolved)
                        books.flush_due(sink, now_ms())
                        if time.time() >= next_progress:
                            next_progress = time.time() + PROGRESS_EVERY_SEC
                            left = window_ts + MARKET_SECONDS - time.time()
                            print(f"[ws] {window_ts} left={left:5.0f}s "
                                  f"books={counters.books} chg={counters.changes} "
                                  f"trades={counters.trades} "
                                  f"resolved={len(resolved)}/{len(slugs)}",
                                  flush=True)
                        if len(resolved) >= len(slugs):
                            break
                finally:
                    pinger.cancel()
        except Exception as exc:
            if time.time() >= deadline or len(resolved) >= len(slugs):
                break
            counters.reconnects += 1
            delay = RECONNECT_BACKOFF[min(attempt, len(RECONNECT_BACKOFF) - 1)]
            attempt += 1
            print(f"[ws] {window_ts} reconnect in {delay:.0f}s "
                  f"({type(exc).__name__}: {exc})", flush=True)
            await asyncio.sleep(delay)


# ---------------------------------------------------------------------------
# resolution backfill
# ---------------------------------------------------------------------------
def _gamma_resolution(slug: str) -> str | None:
    """Return the winning token id for a closed market, else None."""
    session = requests.Session()
    try:
        response = session.get(GAMMA_MARKETS, params={"slug": slug}, timeout=10)
        if response.status_code != 200:
            return None
        payload = response.json()
        if not payload:
            return None
        raw = payload[0]
    except Exception:
        return None
    finally:
        session.close()

    if not raw.get("closed"):
        return None
    tokens = [str(token) for token in _json_list(raw.get("clobTokenIds"))]
    prices = _json_list(raw.get("outcomePrices"))
    for token, price in zip(tokens, prices):
        try:
            if float(price) >= 0.5:
                return token
        except (TypeError, ValueError):
            continue
    return None


async def _backfill_resolution(markets: dict[str, dict], resolved: set[str],
                               sink: RowSink, counters: WindowCounters) -> None:
    """Ask Gamma for the winner when the WS never delivered market_resolved.

    Unresolved windows are skipped by ``run_backtest``; backfilling keeps the
    sample from silently shrinking.
    """
    pending = {slug: market for slug, market in markets.items() if slug not in resolved}
    for attempt in range(3):
        if not pending:
            return
        slugs = list(pending)
        results = await asyncio.gather(
            *(asyncio.to_thread(_gamma_resolution, slug) for slug in slugs)
        )
        for slug, winner_token in zip(slugs, results):
            if not winner_token:
                continue
            market = pending.pop(slug)
            sink.offer({
                "kind": "polymarket_event",
                "market_slug": slug,
                "received_ms": now_ms(),
                "local_ms": now_ms(),
                "event": {
                    "event_type": "market_resolved",
                    "market": market.get("condition_id"),
                    "assets_ids": list(market["asset_ids"].values()),
                    "winning_asset_id": winner_token,
                    "timestamp": str(now_ms()),
                    "source": "gamma_backfill",
                },
            })
            counters.resolutions += 1
            resolved.add(slug)
            print(f"[gamma] {slug} resolution backfilled", flush=True)
        if pending and attempt < 2:
            await asyncio.sleep(20.0)
    for slug in pending:
        print(f"[gamma] {slug} still unresolved; window will be skipped on replay",
              flush=True)


async def record_window(window_ts: int, symbols, sink: RowSink,
                        bucket_sec: float) -> None:
    started = time.time()
    session = requests.Session()
    counters = WindowCounters()
    books = BookState(bucket_sec=bucket_sec)
    try:
        markets = await asyncio.to_thread(discover_window, window_ts, symbols, session)
        if not markets:
            print(f"[window] {window_ts} no markets discovered", flush=True)
            return
        by_slug = {market["slug"]: market for market in markets.values()}
        for market in markets.values():
            sink.offer(market_row(window_ts, market))
        slug_by_token = {}
        for market in markets.values():
            for token in market["asset_ids"].values():
                slug_by_token[str(token)] = market["slug"]
        books.set_slugs(slug_by_token)

        resolved: set[str] = set()
        deadline = window_ts + MARKET_SECONDS + RESOLUTION_GRACE_SEC
        await _stream(list(slug_by_token), slug_by_token, sink, books, counters,
                      resolved, deadline, window_ts)
        books.flush(sink, now_ms())
        if len(resolved) < len(by_slug):
            await _backfill_resolution(by_slug, resolved, sink, counters)
        sink.writer.sync()

        print(f"[window] {window_ts} done in {time.time() - started:.0f}s "
              f"markets={len(markets)} books={counters.books} "
              f"changes={counters.changes} trades={counters.trades} "
              f"resolved={len(resolved)}/{len(by_slug)} "
              f"dups={counters.duplicates} unknown={counters.unknown} "
              f"skipped={counters.skipped} reconnects={counters.reconnects} "
              f"max_skew={counters.max_skew}ms dropped={sink.dropped}", flush=True)
        if counters.max_skew > HOST_SKEW_WARN_MS:
            print(f"[clock] WARNING: local clock differs from exchange by "
                  f"{counters.max_skew}ms; replay cutoffs will be off", flush=True)
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        print(f"[window] {window_ts} failed: {type(exc).__name__}: {exc}", flush=True)
    finally:
        session.close()


# ---------------------------------------------------------------------------
# binance candles
# ---------------------------------------------------------------------------
def _fetch_klines(session: requests.Session, pair: str, limit: int = 3) -> list:
    try:
        response = session.get(BINANCE_KLINES,
                               params={"symbol": pair, "interval": "1m", "limit": limit},
                               timeout=10)
        if response.status_code != 200:
            return []
        return response.json()
    except Exception:
        return []


def binance_rows(session: requests.Session, last_open_ms: dict[str, int]) -> list[dict]:
    rows = []
    now = now_ms()
    for symbol, pair in BINANCE_SYMBOL.items():
        for candle in _fetch_klines(session, pair):
            try:
                open_ms = int(candle[0])
                close_ms = open_ms + 60_000
                if close_ms > now:
                    continue  # candle still forming
                if open_ms <= last_open_ms.get(symbol, 0):
                    continue
                rows.append({
                    "kind": "binance_candle",
                    "symbol": symbol,
                    "pair": pair,
                    "open_ms": open_ms,
                    "event_ms": close_ms,
                    "open": float(candle[1]),
                    "high": float(candle[2]),
                    "low": float(candle[3]),
                    "close": float(candle[4]),
                    "volume": float(candle[5]),
                })
                last_open_ms[symbol] = open_ms
            except (IndexError, TypeError, ValueError):
                continue
        time.sleep(0.2)
    return rows


async def binance_loop(sink: RowSink, stop_at: float | None) -> None:
    session = requests.Session()
    last_open_ms: dict[str, int] = {}
    try:
        while True:
            rows = await asyncio.to_thread(binance_rows, session, last_open_ms)
            for row in rows:
                sink.offer(row)
            if rows:
                print(f"[binance] {len(rows)} closed candle(s) recorded", flush=True)
            if stop_at and time.time() >= stop_at:
                return
            await asyncio.sleep(20.0)
    finally:
        session.close()


# ---------------------------------------------------------------------------
# entry points
# ---------------------------------------------------------------------------
async def _run(out: Path, symbols, duration: float, drain: float,
               bucket_sec: float, max_mb: float) -> None:
    writer = JsonlWriter(out)
    writer.limit_bytes = int(max_mb * 1e6) if max_mb else 0
    sink = RowSink(writer)
    sink_task = asyncio.create_task(sink.run())
    stop_at = time.time() + duration if duration else None
    print(f"[start] writing {out.resolve()}")
    print(f"[start] assets={','.join(symbols)} window={MARKET_SECONDS}s "
          f"bucket={bucket_sec}s duration={duration or 'forever'}s "
          f"max={max_mb or 'none'}MB")
    print(f"[start] existing size={writer.bytes / 1e6:.2f}MB")
    tasks: set[asyncio.Task] = set()
    binance_task = asyncio.create_task(binance_loop(sink, stop_at))
    last_started = None
    try:
        while True:
            now = time.time()
            if (stop_at and now >= stop_at) or sink.stopped:
                break
            window_ts = current_window(now)
            if window_ts != last_started:
                last_started = window_ts
                task = asyncio.create_task(
                    record_window(window_ts, symbols, sink, bucket_sec))
                tasks.add(task)
                task.add_done_callback(tasks.discard)
            wake = window_ts + MARKET_SECONDS + 0.5
            if stop_at:
                wake = min(wake, stop_at)
            await asyncio.sleep(max(0.2, wake - time.time()))
    except asyncio.CancelledError:
        pass
    finally:
        print("[stop] draining in-flight windows", flush=True)
        pending = [task for task in tasks if not task.done()]
        if pending:
            done, still = await asyncio.wait(pending, timeout=drain)
            for task in still:
                task.cancel()
            if still:
                await asyncio.gather(*still, return_exceptions=True)
        binance_task.cancel()
        await asyncio.gather(binance_task, return_exceptions=True)
        await sink.drain()
        sink_task.cancel()
        await asyncio.gather(sink_task, return_exceptions=True)
        writer.close()
        print(f"[stop] rows={writer.rows} size={writer.bytes / 1e6:.2f}MB "
              f"dropped={sink.dropped}")


def record(out: str, assets: str = "", duration: float = 0.0, drain: float = 45.0,
           bucket_sec: float = DEFAULT_BUCKET_SEC, max_mb: float = 20000.0) -> None:
    symbols = [name for name in ASSETS
               if not assets or name.upper() in {a.strip().upper() for a in assets.split(",")}]
    if not symbols:
        raise SystemExit(f"No known assets in {assets!r}; expected a subset of {list(ASSETS)}")
    try:
        asyncio.run(_run(Path(out), symbols, duration, drain, bucket_sec, max_mb))
    except KeyboardInterrupt:
        print("\n[stop] interrupted")


def verify(path: str) -> int:
    """Parse a recording with the backtest loader and report what it can replay."""
    sys.path.insert(0, str(PROJECT_ROOT / "backtest"))
    import backtest as bt  # noqa: E402  (path set above)

    target = Path(path)
    if not target.exists():
        print(f"ERROR: {target} not found")
        return 2
    markets = bt.load_dataset(target)
    if not markets:
        print("ERROR: no market rows parsed")
        return 2

    kinds: dict[str, int] = {}
    with target.open("r", encoding="utf-8") as fp:
        for line in fp:
            try:
                row = json.loads(line)
            except ValueError:
                continue
            kind = row.get("kind", "?")
            kinds[kind] = kinds.get(kind, 0) + 1

    complete = resolved = tradeable = 0
    levels = []
    for slug, entry in markets.items():
        meta = entry["meta"]
        events = entry["events"]
        if not meta.get("start_ms") or not meta.get("asset_ids"):
            continue
        complete += 1
        types = {str((e.get("event") or {}).get("event_type")) for e in events}
        if "market_resolved" in types:
            resolved += 1
        for event in events:
            raw = event.get("event") or {}
            if raw.get("event_type") == "book":
                levels.append(len(raw.get("bids") or []) + len(raw.get("asks") or []))
        late = [
            e for e in events
            if (e.get("event") or {}).get("event_type") == "last_trade_price"
            and e.get("received_ms", 0) >= meta["start_ms"] + 240_000
            and float((e.get("event") or {}).get("price") or 1) <= 0.96
        ]
        if late:
            tradeable += 1

    print(f"file          : {target.resolve()}")
    print(f"size          : {target.stat().st_size / 1e6:.2f}MB")
    print(f"rows          : {kinds}")
    print(f"markets parsed: {complete}")
    if complete:
        print(f"resolved      : {resolved} ({resolved / complete:.0%})")
        print(f"avg levels    : {sum(levels) / len(levels):.1f} per book row"
              if levels else "avg levels    : n/a")
        print(f"0.95 sniffable: {tradeable} market(s) printed a trade <= 0.96 "
              f"after t+240s")
        print(f"replay-ready  : {'YES' if resolved else 'NO (no market_resolved rows)'}")
    return 0 if complete else 2


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", default="data/market_data.jsonl")
    parser.add_argument("--assets", default="", help="comma list, e.g. BTC,ETH (default all)")
    parser.add_argument("--duration", type=float, default=0.0,
                        help="seconds to record (0 = run forever)")
    parser.add_argument("--drain", type=float, default=45.0,
                        help="seconds to wait for in-flight windows on shutdown")
    parser.add_argument("--bucket", type=float, default=DEFAULT_BUCKET_SEC,
                        help="book snapshot cadence in seconds (default 1.0)")
    parser.add_argument("--max-mb", type=float, default=20000.0,
                        help="stop cleanly after this many MB (0 = no limit)")
    parser.add_argument("--verify", metavar="PATH",
                        help="report coverage of an existing recording and exit")
    args = parser.parse_args(argv)
    if args.verify:
        return verify(args.verify)
    record(args.out, args.assets, args.duration, args.drain, args.bucket, args.max_mb)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
