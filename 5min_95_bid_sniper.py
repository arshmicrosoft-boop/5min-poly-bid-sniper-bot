#!/opt/anaconda3/envs/tflow/bin/python
"""
5-MINUTE 95c BID SNIPER v1.0  (AUG14 account)
A patient, last-60-second entry bot for BTC 5-minute UP/DOWN markets.
WHY THIS IS DIFFERENT FROM THE BUZZER BEATER:
  The old 5min_buzzer_beater.py entered at a 0.70 "favored" ask and CHASED THE
  ASK (aggressive taker). 70c is NOT decided — soggybiscuits2 got wrecked. This bot
  fixes both mistakes:
    1. Only acts when a side is genuinely DECIDED — its ask is >= 0.95.
    2. We DO NOT market order / chase the ask. We SIT ON THE BID (passive maker)
       and see if we can get filled. We never pay more than 0.95 a share, which
       locks in a >= ~5.3% gain if the side resolves to $1.00.
STRATEGY (the whole thing in 5 lines):
  - Wait until <= 60 seconds left in the current 5-min market.
  - Find the side whose ask >= 0.95 (the market has basically already decided).
  - Rest a BUY limit on the bid: price = min(best_bid, 0.95). Never above 0.95.
  - Re-place every few seconds to stay on the bid. Stand down if the side stops
    being decided (ask drops < 0.95). One fill per market, then hold to expiry.
  - 95c -> 100c = 5.26% per win. We just want 5% every 5 minutes.
NOTE ON SIZE: Polymarket enforces a 5-share minimum per order. At 0.95 that is
  ~$4.75, so the minimum-share rule can exceed TARGET_NOTIONAL_USD. The order
  is rejected if its actual notional exceeds MAX_ORDER_NOTIONAL_USD.
MULTI-ACCOUNT:
  Set to the AUG14 wallet (signature_type=2 / Gnosis Safe). Change ACCOUNT_SUFFIX
  to run elsewhere.
"""

import sys
import os
import time
import itertools
import json
import copy
import threading
import shutil
import math
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import deque
import requests
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from termcolor import colored
try:
    import msvcrt
except ImportError:
    msvcrt = None

# Auto re-exec with tflow python if we're in the wrong env
TFLOW_PYTHON = "/opt/anaconda3/envs/tflow/bin/python"
if os.path.exists(TFLOW_PYTHON) and sys.executable != TFLOW_PYTHON:
    os.execv(TFLOW_PYTHON, [TFLOW_PYTHON] + sys.argv)

# ============================================================================
# PATH / ENV SETUP
# ============================================================================
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(PROJECT_ROOT, '.env'))

# Safe by default: dry run keeps market data live but never submits account actions.
DRY_RUN = os.getenv("DRY_RUN", "true").strip().lower() not in {"0", "false", "no", "off"}
PAPER_ORDERS = {}
PAPER_ORDER_SEQUENCE = itertools.count(1)
PAPER_TRADE_EVENTS = deque(maxlen=20000)
PAPER_TRADE_EVENT_LOCK = threading.Lock()
PAPER_TRADE_DEDUPE = deque(maxlen=5000)
PAPER_TRADE_DEDUPE_SET = set()
PAPER_TRADE_GAP = False
TRADE_STREAM_STATUS = "waiting"
TRADE_STREAM_TOKENS = ()
TRADE_STREAM_LOCK = threading.Lock()
TRADE_STREAM_STOP = threading.Event()
OPEN_ORDER_COSTS = {}
OPEN_ORDER_LOCK = threading.RLock()


def _set_order_cost(key, value):
    with OPEN_ORDER_LOCK:
        OPEN_ORDER_COSTS[str(key)] = float(value)


def _pop_order_cost(key):
    with OPEN_ORDER_LOCK:
        return OPEN_ORDER_COSTS.pop(str(key), None)


def _total_order_cost():
    with OPEN_ORDER_LOCK:
        return sum(OPEN_ORDER_COSTS.values())


def _snapshot_order_costs():
    with OPEN_ORDER_LOCK:
        return dict(OPEN_ORDER_COSTS)


def _replace_order_costs(values):
    with OPEN_ORDER_LOCK:
        OPEN_ORDER_COSTS.clear()
        OPEN_ORDER_COSTS.update(values)

sys.path.insert(0, PROJECT_ROOT)
from nice_funcs import calculate_shares

# ============================================================================
# ACCOUNT CONFIGURATION (this bot = AUG14)
# ============================================================================
ACCOUNT_SUFFIX = "_AUG14"

PRIVATE_KEY_ENV_NAME = f"PRIVATE_KEY{ACCOUNT_SUFFIX}"
PUBLIC_KEY_ENV_NAME = f"PUBLIC_KEY{ACCOUNT_SUFFIX}"
API_KEY_ENV_NAME = f"API_KEY{ACCOUNT_SUFFIX}"
API_SECRET_ENV_NAME = f"API_SECRET{ACCOUNT_SUFFIX}" if ACCOUNT_SUFFIX else "SECRET"
PASSPHRASE_ENV_NAME = f"PASSPHRASE{ACCOUNT_SUFFIX}"

# Do not infer the wallet signature type from an arbitrary env-name suffix.
# The operator must set this explicitly for the configured funded wallet.
SIGNATURE_TYPE = int(os.getenv("SIGNATURE_TYPE", "2"))

# ============================================================================
# STRATEGY CONFIGURATION
# ============================================================================
TARGET_NOTIONAL_USD = float(os.getenv("TARGET_NOTIONAL_USD", os.getenv("BET_SIZE_USD", "1.0")))
MAX_ORDER_NOTIONAL_USD = float(os.getenv("MAX_ORDER_NOTIONAL_USD", "5.0"))
DRY_RUN_FEE_RATE = float(os.getenv("DRY_RUN_FEE_RATE", "0.0"))
DRY_RUN_SLIPPAGE = float(os.getenv("DRY_RUN_SLIPPAGE", "0.001"))
ENTRY_THRESHOLD = float(os.getenv("ENTRY_THRESHOLD", "0.95"))
MAX_BUY_PRICE = float(os.getenv("MAX_BUY_PRICE", "0.95"))
MAX_TOTAL_EXPOSURE_USD = float(os.getenv("MAX_TOTAL_EXPOSURE_USD", "5.0"))
MAX_SESSION_LOSS_USD = float(os.getenv("MAX_SESSION_LOSS_USD", "8.0"))
MAX_POSITION_LOSS_USD = float(os.getenv("MAX_POSITION_LOSS_USD", "4.50"))
SIM_BANKROLL_USD = float(os.getenv("SIM_BANKROLL_USD", "10.0"))
MIN_DECIDED_BID = float(os.getenv("MIN_DECIDED_BID", "0.88"))
MAX_OPPOSITE_BID = float(os.getenv("MAX_OPPOSITE_BID", "0.08"))
EARLY_WINDOW_MAX_BUY_PRICE = float(os.getenv("EARLY_WINDOW_MAX_BUY_PRICE", "0.93"))
ENTRY_WINDOW_SEC = int(os.getenv("ENTRY_WINDOW_SEC", "60"))
STOP_THRESHOLD_SEC = int(os.getenv("STOP_THRESHOLD_SEC", "10"))
SOFT_ENTRY_CUTOFF_SEC = int(os.getenv("SOFT_ENTRY_CUTOFF_SEC", "15"))
ORDER_UPDATE_INTERVAL = int(os.getenv("ORDER_UPDATE_INTERVAL", "1"))
PRICE_HOLD_TOLERANCE = float(os.getenv("PRICE_HOLD_TOLERANCE", "0.005"))
KILL_SWITCH_FILE = os.getenv("KILL_SWITCH_FILE", "KILL")

MARKET_DURATION = 300
ET = timezone(timedelta(hours=-5))
CRYPTO_ASSETS = {"BTC": "btc", "ETH": "eth", "SOL": "sol", "XRP": "xrp", "DOGE": "doge"}
BOOK_WORKERS = len(CRYPTO_ASSETS) * 2
DISCOVERY_RETRY_SEC = 15
BOOK_STALE_SEC = 5.0

TARGET_GAIN_PCT = (1.0 / MAX_BUY_PRICE - 1.0) * 100.0

# ============================================================================
# SESSION STATS
# ============================================================================
SESSION_ENTRIES = 0
SESSION_SKIPS = 0
SESSION_ATTEMPTS = 0
SESSION_WINDOWS = 0
SESSION_SIGNALS = 0
SESSION_REPRICES = 0
SESSION_CUTOFF_CANCELS = 0
SESSION_TIMEOUTS = 0
SESSION_PAPER_WINS = 0
SESSION_PAPER_LOSSES = 0
SESSION_PAPER_PNL = 0.0
SESSION_PAPER_MAX_LOSS = 0.0
SESSION_PAPER_MAX_PROFIT = 0.0
SESSION_FILL_PRICES = []
SESSION_PAPER_PEAK_PNL = 0.0
SESSION_MAX_DD = 0.0
SESSION_CURRENT_STREAK = 0
SESSION_ENTRY_HALT_REASON = None
SESSION_LATE_FILLS = 0
SESSION_START = time.time()

# ============================================================================
# FUN ZONE
# ============================================================================
HYPE_MESSAGES = [
    "LFG soggybsicuits2",
    "5% every 5 minutes",
    "We don't chase, we rest on the bid",
    "Only the decided ones, only 95c+",
    "Patience pays the maker",
    "No market orders. Ever.",
    "Sniping the last nickel",
    "Let the gamblers fight at 50c",
    "Decided = our zone",
    "Sit. Wait. Fill. Hold.",
    "AUG14 stays winning",
    "Ice in the veins",
    "The bid is our friend",
    "Built different",
    "Cook mode: PATIENT",
    "5.3% per win, stack em",
    "One bullet per market",
    "soggybiscuits2 = the closer",
]

_DASHBOARD_FRAME = itertools.count()
DASHBOARD_RENDERED = False
_DASHBOARD_SNAPSHOT_LOCK = threading.Lock()
_DASHBOARD_SNAPSHOT = None
_DASHBOARD_STOP = threading.Event()

EVENT_LOG = deque(maxlen=6)
ORDER_LOG = deque(maxlen=5)
_SESSION_LOG_LOCK = threading.Lock()
_THREAD_LOCAL = threading.local()
_PING_LOCK = threading.Lock()
CLOB_PING_MS = deque(maxlen=30)

# Round-trip timings surfaced on the dashboard. CLOB_PING_MS holds the public
# book-fetch latency; these track the account-side calls (live mode only) and
# the time spent building each dashboard frame.
ORDER_LATENCY_MS = deque(maxlen=30)
CANCEL_LATENCY_MS = deque(maxlen=30)
RENDER_LATENCY_MS = deque(maxlen=30)
_LATENCY_LOCK = threading.Lock()


def _record_latency(bucket, started):
    elapsed_ms = (time.perf_counter() - started) * 1000.0
    with _LATENCY_LOCK:
        bucket.append(elapsed_ms)


def _http_session():
    """Return a keep-alive HTTP session scoped to the current worker thread."""
    session = getattr(_THREAD_LOCAL, "session", None)
    if session is None:
        session = requests.Session()
        _THREAD_LOCAL.session = session
    return session


def log_event(msg, color="white"):
    ts = datetime.now().strftime("%H:%M:%S")
    order_event = any(
        marker in msg.lower()
        for marker in (
            "dry run:", "resting", "order live", "matched", "crossed", "cancel",
            "paper fill", "paper win", "paper loss", "paper outcome", "critical:",
            "exposure cap", "skipping bid",
        )
    )
    if order_event:
        ORDER_LOG.append((ts, msg, color))
        try:
            with _SESSION_LOG_LOCK:
                with open(
                    os.path.join(PROJECT_ROOT, "sniper_session.log"),
                    "a",
                    encoding="utf-8",
                ) as fp:
                    fp.write(f"{datetime.now().isoformat(timespec='seconds')}  {msg}\n")
        except OSError:
            # Session logging must never interrupt order management.
            pass
    else:
        EVENT_LOG.append((ts, msg, color))


# ============================================================================
# POLYMARKET CLIENT HELPERS (account-aware)
# ============================================================================
_CLIENT_CACHE = None
_CLIENT_LOCK = threading.Lock()


def _build_client():
    global _CLIENT_CACHE
    if DRY_RUN:
        raise RuntimeError("CLOB account client is disabled while DRY_RUN is enabled")
    if _CLIENT_CACHE is not None:
        return _CLIENT_CACHE
    with _CLIENT_LOCK:
        if _CLIENT_CACHE is not None:
            return _CLIENT_CACHE

        from polymarket import ApiKeyCreds, SecureClient

        key = os.getenv(PRIVATE_KEY_ENV_NAME)
        browser_address = os.getenv(PUBLIC_KEY_ENV_NAME)

        if not key or not browser_address:
            print(colored(
                f"Missing {PRIVATE_KEY_ENV_NAME} or {PUBLIC_KEY_ENV_NAME} in .env!",
                "red",
            ))
            sys.exit(1)

        api_key = os.getenv(API_KEY_ENV_NAME)
        api_secret = (
            os.getenv(API_SECRET_ENV_NAME)
            or os.getenv(f"SECRET{ACCOUNT_SUFFIX}")
            or os.getenv("SECRET")
        )
        passphrase = os.getenv(PASSPHRASE_ENV_NAME)

        if api_key and api_secret and passphrase:
            creds = ApiKeyCreds(key=api_key, secret=api_secret, passphrase=passphrase)
        else:
            creds = None

        client = SecureClient.create(
            private_key=key,
            wallet=browser_address,
            credentials=creds,
        )
        from polymarket._internal.wallet import signature_type_for
        inferred_signature_type = int(signature_type_for(client._ctx.wallet_type))
        if inferred_signature_type != SIGNATURE_TYPE:
            raise RuntimeError(
                f"SIGNATURE_TYPE={SIGNATURE_TYPE} does not match wallet-derived "
                f"signature type {inferred_signature_type}"
            )

        _CLIENT_CACHE = client
        return _CLIENT_CACHE


def get_order_status_for_id(order_id):
    if not order_id or order_id == 'timeout-assumed-live':
        return None
    if DRY_RUN:
        order = PAPER_ORDERS.get(order_id)
        if not order:
            return {'status': 'NOT_FOUND', 'size_matched': 0.0, 'original_size': 0.0,
                    'price': 0.0, 'has_match': False, 'is_filled': False}
        size_matched = float(order.get('size_matched', 0.0))
        return {'status': order['status'], 'size_matched': size_matched,
                'original_size': order['size'], 'price': order['price'],
                'has_match': size_matched > 0,
                'is_filled': size_matched >= float(order['size']) > 0}
    client = _build_client()
    for attempt in range(3):
        try:
            order = client.get_order(order_id=str(order_id))
            if not order:
                return {
                    'status': 'NOT_FOUND',
                    'size_matched': 0.0,
                    'original_size': 0.0,
                    'price': 0.0,
                    'has_match': False,
                    'is_filled': False,
                }
            if isinstance(order, dict):
                size_matched = float(order.get('size_matched', 0) or 0)
                original_size = float(order.get('original_size', 0) or 0)
                status = str(order.get('status', 'unknown'))
                price = float(order.get('price', 0) or 0)
            else:
                size_matched = float(getattr(order, 'size_matched', 0) or 0)
                original_size = float(getattr(order, 'original_size', 0) or 0)
                status = str(getattr(order, 'status', 'unknown'))
                price = float(getattr(order, 'price', 0) or 0)
            return {
                'status': status.upper(),
                'size_matched': size_matched,
                'original_size': original_size,
                'price': price,
                'has_match': size_matched > 0,
                'is_filled': (
                    status.upper() in ('MATCHED', 'FILLED')
                    or (original_size > 0 and size_matched >= original_size - 1e-8)
                ),
            }
        except Exception as e:
            err = str(e)
            if any(code in err for code in ['500', '502', '503', '504', 'timeout', 'Timeout']):
                time.sleep(0.5 * (attempt + 1))
                continue
            log_event(f"get_order failed: {type(e).__name__}", "yellow")
            return "ERROR"
    return "ERROR"


def place_limit_order(token_id, side, price, size, neg_risk=False, expiration_ts=None):
    if DRY_RUN:
        order_id = f"paper-{next(PAPER_ORDER_SEQUENCE)}"
        PAPER_ORDERS[order_id] = {
            'token_id': str(token_id), 'side': side.upper(), 'price': float(price),
            'size': float(size), 'size_matched': 0.0, 'status': 'LIVE',
            'created_at': time.time(), 'queue_ahead': 0.0,
        }
        _set_order_cost(order_id, float(price) * float(size))
        log_event(f"DRY RUN: simulated {side} @ ${price:.4f} x {size} (memory only)", "yellow")
        return {'orderID': order_id, 'status': 'LIVE', 'simulated': True}

    client = _build_client()

    log_event(f"Resting {side} @ ${price:.4f} x {size} shares (post-only)", "cyan")

    started = time.perf_counter()
    try:
        if not expiration_ts or int(expiration_ts) < int(time.time()) + 210:
            raise ValueError("GTD expiry must be at least 210 seconds ahead")
        response = client.place_limit_order(
            asset_id=str(token_id),
            price=float(price),
            size=float(size),
            side=side.upper(),
            post_only=True,
            expiration=int(expiration_ts),
        )
        if not getattr(response, "ok", False):
            log_event(f"Order rejected: {getattr(response, 'message', 'unknown rejection')}", "red")
            return {}
        if response and getattr(response, "order_id", None):
            oid = str(response.order_id)
            log_event(f"Order LIVE — id {str(oid)[:10]}...", "green")
            return {"orderID": oid, "status": str(response.status).upper(), "success": True}
        log_event("Order response did not contain a confirmed order ID", "red")
        return {}
    except Exception as e:
        err = str(e)
        err_lower = err.lower()
        if 'duplicat' in err_lower:
            log_event(
                "CRITICAL: CLOB reported a duplicate submission; order state is unknown, "
                "retaining exposure and halting entries",
                "red",
            )
            return {'orderID': 'timeout-assumed-live', 'status': 'UNKNOWN', 'duplicate': True}
        transport_ambiguous = isinstance(e, requests.RequestException) or any(
            marker in err_lower for marker in (
                'timeout', 'connection', 'temporarily unavailable', 'remote disconnected',
                'protocolerror', 'request exception',
            )
        )
        if transport_ambiguous:
            log_event("post_order timed out — assuming it landed", "yellow")
            return {'orderID': 'timeout-assumed-live', 'status': 'LIVE'}
        log_event(f"post_order failed: {type(e).__name__}: {e}", "red")
        return {}
    finally:
        _record_latency(ORDER_LATENCY_MS, started)


def cancel_token_orders_acct(token_id, order_id=None):
    if DRY_RUN:
        for oid, order in list(PAPER_ORDERS.items()):
            if (order['token_id'] == str(token_id) and order['status'] == 'LIVE'
                    and (order_id is None or oid == str(order_id))):
                order['status'] = 'CANCELED'
                _pop_order_cost(oid)
        log_event("DRY RUN: simulated cancellation (memory only)", "yellow")
        return True

    client = _build_client()
    started = time.perf_counter()
    try:
        if not order_id or order_id == 'timeout-assumed-live':
            log_event("Refusing account-wide cancellation without a confirmed order ID", "red")
            return False
        response = client.cancel_order(order_id=str(order_id))
        if str(order_id) not in {str(value) for value in response.canceled}:
            log_event(f"Cancel was not confirmed for {str(order_id)[:10]}...", "red")
            return False
        log_event(f"Cancel requested for order {str(order_id)[:10]}...", "yellow")
        return True
    except Exception as e:
        log_event(f"Cancel failed: {type(e).__name__}: {e}", "yellow")
        return False
    finally:
        _record_latency(CANCEL_LATENCY_MS, started)


# ============================================================================
# MARKET DATA HELPERS
# ============================================================================
def get_current_market_timestamp():
    now = int(time.time())
    return (now // MARKET_DURATION) * MARKET_DURATION


def get_time_remaining(market_ts):
    return MARKET_DURATION - (int(time.time()) - market_ts)


def get_order_book(token_id):
    for attempt in range(3):
        try:
            response = _http_session().get(
                "https://clob.polymarket.com/book",
                params={'token_id': token_id},
                timeout=10,
            )
            if response.status_code == 429:
                time.sleep(0.5 * (attempt + 1))
                continue
            if response.status_code != 200:
                return None
            data = response.json()
            bids = data.get('bids', [])
            asks = data.get('asks', [])
            if not bids and not asks:
                return None
            best_bid = max((float(level['price']) for level in bids), default=None)
            best_ask = min((float(level['price']) for level in asks), default=None)
            return {
                'best_bid': best_bid,
                'best_ask': best_ask,
                'bid_queue_shares': sum(
                    float(level.get('size', 0) or 0) for level in bids
                    if best_bid is not None and abs(float(level['price']) - best_bid) < 1e-9
                ),
                'bid_depth_shares': sum(
                    float(level.get('size', 0) or 0) for level in bids
                    if best_bid is not None and float(level['price']) >= best_bid - 0.05
                ),
                'ask_depth_shares': sum(
                    float(level.get('size', 0) or 0) for level in asks
                    if best_ask is not None and float(level['price']) <= best_ask + 0.05
                ),
            }
        except (requests.RequestException, ValueError, KeyError, TypeError):
            if attempt < 2:
                time.sleep(0.5 * (attempt + 1))
                continue
            return None
    return None


def get_order_books_batch(token_ids):
    """Fetch many public CLOB books in one POST /books snapshot."""
    token_ids = [str(token_id) for token_id in token_ids]
    if not token_ids:
        return {}
    for attempt in range(3):
        try:
            response = _http_session().post(
                "https://clob.polymarket.com/books",
                json=[{"token_id": token_id} for token_id in token_ids],
                timeout=6,
            )
            if response.status_code == 429 or response.status_code >= 500:
                time.sleep(0.5 * (attempt + 1))
                continue
            if response.status_code != 200:
                raise requests.RequestException(
                    f"batch books returned HTTP {response.status_code}")
            payload = response.json()
            if not isinstance(payload, list):
                raise ValueError("batch books response was not a list")
            books = {}
            for index, raw_book in enumerate(payload):
                token_id = str(
                    raw_book.get('asset_id')
                    or raw_book.get('token_id')
                or ''
                )
                if not token_id:
                    raise ValueError("batch book response row has no token ID")
                bids = raw_book.get('bids', [])
                asks = raw_book.get('asks', [])
                if not bids and not asks:
                    books[token_id] = None
                    continue
                best_bid = max((float(level['price']) for level in bids), default=None)
                best_ask = min((float(level['price']) for level in asks), default=None)
                books[token_id] = {
                    'best_bid': best_bid,
                    'best_ask': best_ask,
                    'bid_queue_shares': sum(
                        float(level.get('size', 0) or 0) for level in bids
                        if best_bid is not None and abs(float(level['price']) - best_bid) < 1e-9
                    ),
                    'bid_depth_shares': sum(
                        float(level.get('size', 0) or 0) for level in bids
                        if float(level['price']) >= 0.90
                    ),
                    'ask_depth_shares': sum(
                        float(level.get('size', 0) or 0) for level in asks
                        if best_ask is not None and float(level['price']) <= best_ask + 0.05
                    ),
                    'bid_levels': [
                        (float(level['price']), float(level.get('size', 0) or 0))
                        for level in bids
                    ],
                }
            return books
        except (requests.RequestException, ValueError, TypeError):
            if attempt < 2:
                time.sleep(0.5 * (attempt + 1))
                continue
            log_event("Batch order-book request failed", "yellow")
            return {token_id: None for token_id in token_ids}
    return {token_id: None for token_id in token_ids}


def get_market_info(market_ts, symbol):
    """Fetch one coin's exact 5-minute market, mapping token IDs by outcome label."""
    prefix = CRYPTO_ASSETS[symbol]
    market_slug = f"{prefix}-updown-5m-{market_ts}"
    for attempt in range(2):
        try:
            response = _http_session().get(
                "https://gamma-api.polymarket.com/markets",
                params={'slug': market_slug, 'closed': 'false', 'active': 'true'},
                timeout=8,
            )
            if response.status_code != 200:
                return None
            markets = response.json()
            if not markets:
                return None
            market = markets[0]
            token_ids = market.get('clobTokenIds', [])
            outcomes = market.get('outcomes', [])
            if isinstance(token_ids, str):
                token_ids = json.loads(token_ids)
            if isinstance(outcomes, str):
                outcomes = json.loads(outcomes)
            if not isinstance(token_ids, list) or len(token_ids) != 2:
                return None
            if not isinstance(outcomes, list) or len(outcomes) != 2:
                return None
            token_by_outcome = {
                str(outcome).strip().lower(): str(token_ids[index])
                for index, outcome in enumerate(outcomes)
            }
            up_token = token_by_outcome.get('up')
            down_token = token_by_outcome.get('down')
            if not up_token or not down_token:
                return None
            return {
                'market_id': market.get('id'),
                'symbol': symbol,
                'up_token_id': up_token,
                'down_token_id': down_token,
                'question': market.get('question', market_slug),
                'slug': market_slug,
                'neg_risk': market.get('negRisk', False),
            }
        except (requests.RequestException, ValueError, TypeError) as exc:
            if attempt == 1:
                log_event(f"{symbol} market lookup failed: {type(exc).__name__}", "yellow")
            else:
                time.sleep(0.35)
    return None


def _account_positions_by_token():
    if DRY_RUN:
        return {}
    user_address = os.getenv(PUBLIC_KEY_ENV_NAME)
    if not user_address:
        return None
    try:
        # Data API v1 /positions was retired. The unified SDK handles the v2
        # cursor-based response and parses its enveloped payload.
        positions_page_stream = _build_client().list_positions(
            user=user_address,
            page_size=100,
        )
        positions = {}
        page_count = 0
        for page in positions_page_stream:
            page_count += 1
            if page.limit_reached:
                log_event("positions pagination depth limit reached; account exposure is unknown", "red")
                return None
            for pos in page.items:
                asset = str(pos.asset_id)
                size = float(pos.current_size)
                if asset and size > 0:
                    # Use the API's fee-inclusive total basis where available.
                    basis = float(pos.total_cost_usdc)
                    if not math.isfinite(basis) or basis < 0:
                        return None
                    previous = positions.get(asset, {'shares': 0.0, 'cost': 0.0})
                    positions[asset] = {
                        'shares': previous['shares'] + size,
                        'cost': previous['cost'] + basis,
                    }
            if page.has_more and not page.next_cursor:
                log_event("positions pagination cursor missing; account exposure is unknown", "red")
                return None
            if page_count > 1000:
                log_event("positions pagination exceeded safety page limit", "red")
                return None
        return positions
    except Exception as e:
        log_event(f"positions fetch failed: {e}", "yellow")
        return None


def position_shares_for_token(token_id):
    positions = _account_positions_by_token()
    if positions is None:
        return None
    return float(positions.get(str(token_id), {}).get('shares', 0.0))


def _reconcile_startup_positions(market_states):
    """Reserve account-wide position cost before allowing another entry."""
    global SESSION_ENTRY_HALT_REASON
    if DRY_RUN:
        return
    positions = _account_positions_by_token()
    if positions is None:
        raise RuntimeError("Startup blocked: account positions could not be reconciled safely")
    for token_id, position in positions.items():
        _set_order_cost(f"position:{token_id}", position['cost'])
    for symbol, state in market_states.items():
        info = state.get("market_info") or {}
        holdings = []
        for side, key in (("UP", "up_token_id"), ("DOWN", "down_token_id")):
            token_id = info.get(key)
            if not token_id:
                continue
            shares = positions.get(str(token_id), {}).get('shares', 0.0)
            if not shares or shares <= 0:
                continue
            holdings.append((side, str(token_id), float(shares)))
        if holdings:
            total_shares = sum(shares for _, _, shares in holdings)
            if len(holdings) > 1:
                SESSION_ENTRY_HALT_REASON = (
                    f"{symbol} has positions on both outcomes; account state requires manual review"
                )
            state["favored_side"] = holdings[0][0] if len(holdings) == 1 else "BOTH"
            state["favored_token_id"] = holdings[0][1] if len(holdings) == 1 else None
            state["entered"] = True
            state["filled_shares"] = total_shares
            state["entry_fill_price"] = MAX_BUY_PRICE
            state["filled_cost"] = sum(positions[token_id]['cost'] for _, token_id, _ in holdings)
            state["signal_counted"] = True
            exposure_key = f"startup:{symbol}:position"
            state["exposure_key"] = exposure_key
            log_event(
                f"{symbol} startup reconciliation: adopted {total_shares:.2f} "
                f"{'/'.join(side for side, _, _ in holdings)} "
                f"shares already on book (reserved ${state['filled_cost']:.2f})",
                "yellow",
            )


# ============================================================================
# CORE 95c BID SNIPER LOGIC
# ============================================================================
def pick_decided_side(up_book, down_book):
    up_ask = up_book['best_ask'] if up_book else None
    up_bid = up_book['best_bid'] if up_book else None
    down_ask = down_book['best_ask'] if down_book else None
    down_bid = down_book['best_bid'] if down_book else None

    up_qualifies = (
        up_ask is not None and up_ask >= ENTRY_THRESHOLD
        and up_bid is not None and up_bid >= MIN_DECIDED_BID
        and (down_bid is None or down_bid <= MAX_OPPOSITE_BID)
    )
    down_qualifies = (
        down_ask is not None and down_ask >= ENTRY_THRESHOLD
        and down_bid is not None and down_bid >= MIN_DECIDED_BID
        and (up_bid is None or up_bid <= MAX_OPPOSITE_BID)
    )

    if up_qualifies and down_qualifies:
        return ("UP", up_book) if up_ask >= down_ask else ("DOWN", down_book)
    if up_qualifies:
        return "UP", up_book
    if down_qualifies:
        return "DOWN", down_book
    return None, None


def compute_bid_target(favored_book, time_left=None, opposite_book=None):
    if not favored_book:
        return None
    best_ask = favored_book.get('best_ask')
    best_bid = favored_book.get('best_bid')
    if best_bid is None or best_bid < MIN_DECIDED_BID:
        return None
    opposite_bid = opposite_book.get('best_bid') if opposite_book else None
    if opposite_bid is not None and opposite_bid > MAX_OPPOSITE_BID:
        return None
    if best_ask is None:
        if best_bid < ENTRY_THRESHOLD:
            return None
        best_ask = 1.00
    elif best_ask < ENTRY_THRESHOLD:
        return None
    # Keep the post-only BUY strictly below the current best ask.
    max_maker_bid = round(best_ask - 0.01, 3)
    time_tier_cap = (
        min(MAX_BUY_PRICE, EARLY_WINDOW_MAX_BUY_PRICE)
        if time_left is not None and time_left > 30
        else MAX_BUY_PRICE
    )
    target = min(best_bid, max_maker_bid, time_tier_cap)
    return round(target, 3) if target >= 0.01 else None


def _update_ask_momentum(market_states, market_books):
    """Remember one-tick ask drops so entries avoid an actively falling side."""
    for symbol, state in market_states.items():
        up_book, down_book = market_books.get(symbol, (None, None))
        previous = state.setdefault('previous_asks', {})
        drops = state.setdefault('sharp_ask_drop', {})
        for side, book in (("UP", up_book), ("DOWN", down_book)):
            ask = book.get('best_ask') if book else None
            old_ask = previous.get(side)
            drops[side] = (
                ask is not None and old_ask is not None
                and ask <= old_ask - 0.02
            )
            if ask is not None:
                previous[side] = ask


def _current_exposure_usd():
    return _total_order_cost()


def _exposure_limit_usd():
    """Apply both configured portfolio cap and simulated available bankroll."""
    if DRY_RUN:
        return min(MAX_TOTAL_EXPOSURE_USD, max(0.0, SIM_BANKROLL_USD + SESSION_PAPER_PNL))
    return MAX_TOTAL_EXPOSURE_USD


def _place_fresh_bid(state, token_id, target_price, favored_book=None):
    global SESSION_ATTEMPTS, SESSION_TIMEOUTS, SESSION_ENTRY_HALT_REASON
    queue_ahead = 0.0
    if DRY_RUN:
        with TRADE_STREAM_LOCK:
            stream_status = TRADE_STREAM_STATUS
        if stream_status != "connected" or PAPER_TRADE_GAP:
            log_event(
                f"Paper bid skipped: public trade stream is {stream_status} or has a data gap",
                "yellow",
            )
            return
        bid_levels = (favored_book or {}).get("bid_levels")
        if bid_levels is None:
            log_event("Paper bid skipped: book depth unavailable for queue estimate", "yellow")
            return
        queue_ahead = sum(
            float(level_size) for level_price, level_size in bid_levels
            if float(level_price) >= target_price - 1e-9
        )
    shares = calculate_shares(TARGET_NOTIONAL_USD, target_price)
    if shares <= 0:
        return
    order_cost = target_price * shares
    if order_cost > MAX_ORDER_NOTIONAL_USD + 1e-9:
        log_event(
            f"Order notional ${order_cost:.2f} exceeds MAX_ORDER_NOTIONAL_USD "
            f"${MAX_ORDER_NOTIONAL_USD:.2f}; bid rejected",
            "red",
        )
        return
    current = _current_exposure_usd()
    exposure_limit = _exposure_limit_usd()
    if current + order_cost > exposure_limit:
        log_event(
            f"Exposure cap reached (${current:.2f}+${order_cost:.2f} > ${exposure_limit:.2f}); skipping bid",
            "yellow",
        )
        return
    SESSION_ATTEMPTS += 1
    # Unified SDK's GTD contract requires >= 3 minutes from submission. Keep
    # the exchange-side expiry finite and add clock/transport headroom; normal
    # cutoff cancellation must still remove the order before market expiry.
    expiration_ts = max(
        int(state['market_ts'] + MARKET_DURATION + 60),
        int(time.time()) + 240,
    )
    resp = place_limit_order(
        token_id, "BUY", target_price, shares,
        neg_risk=state['neg_risk'], expiration_ts=expiration_ts,
    )
    if resp:
        oid = resp.get('orderID')
        if resp.get('success') and not oid:
            log_event("Order accepted without an ID; treating it as uncertain", "yellow")
            oid = 'timeout-assumed-live'
        if oid and oid != 'timeout-assumed-live':
            if DRY_RUN and oid in PAPER_ORDERS:
                PAPER_ORDERS[oid]["queue_ahead"] = queue_ahead
            state['last_order_id'] = oid
            state['last_order_price'] = target_price
            state['open_order_price'] = target_price
            state['exposure_key'] = str(oid)
            _set_order_cost(oid, order_cost)
        elif oid == 'timeout-assumed-live':
            SESSION_TIMEOUTS += 1
            SESSION_ENTRY_HALT_REASON = (
                "order submission outcome is unknown; no additional orders will be sent"
            )
            state['last_order_id'] = 'timeout-assumed-live'
            state['last_order_price'] = target_price
            state['open_order_price'] = target_price
            exposure_key = f"timeout:{state['market_info']['slug']}"
            state['exposure_key'] = exposure_key
            _set_order_cost(exposure_key, order_cost)


def new_market_state(market_ts, market_info):
    """Create independent order/entry state for one asset's market window."""
    return {
        'market_ts': market_ts,
        'market_info': market_info,
        'neg_risk': False,
        'favored_side': None,
        'favored_token_id': None,
        'favored_since': 0.0,
        'entered': False,
        'entry_fill_price': None,
        'filled_shares': 0.0,
        'filled_cost': 0.0,
        'signal_counted': False,
        'last_order_id': None,
        'exposure_key': None,
        'last_order_price': None,
        'open_order_price': None,
        'last_order_update': 0.0,
    }


def _set_trade_stream_tokens(market_states):
    """Publish the active window's outcome token IDs to the public WS reader."""
    global TRADE_STREAM_TOKENS
    tokens = tuple(sorted({
        str(state.get("market_info", {}).get(key))
        for state in market_states.values()
        for key in ("up_token_id", "down_token_id")
        if state.get("market_info", {}).get(key)
    }))
    with TRADE_STREAM_LOCK:
        if tokens != TRADE_STREAM_TOKENS:
            TRADE_STREAM_TOKENS = tokens


def _paper_trade_stream_worker():
    """Receive public CLOB trade prints; never block the order or UI thread."""
    global TRADE_STREAM_STATUS, PAPER_TRADE_GAP, SESSION_ENTRY_HALT_REASON
    from websockets.sync.client import connect

    url = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    backoff = 1.0
    had_connection = False
    while not TRADE_STREAM_STOP.is_set():
        with TRADE_STREAM_LOCK:
            tokens = TRADE_STREAM_TOKENS
        if not tokens:
            TRADE_STREAM_STATUS = "waiting for markets"
            TRADE_STREAM_STOP.wait(0.5)
            continue
        try:
            with connect(
                url,
                ping_interval=20,
                ping_timeout=20,
                open_timeout=10,
                close_timeout=3,
                max_size=8 * 1024 * 1024,
                max_queue=1000,
            ) as ws:
                ws.send(json.dumps({
                    "assets_ids": list(tokens),
                    "type": "market",
                    "custom_feature_enabled": True,
                }))
                TRADE_STREAM_STATUS = "connected"
                had_connection = True
                backoff = 1.0
                while not TRADE_STREAM_STOP.is_set():
                    with TRADE_STREAM_LOCK:
                        current_tokens = TRADE_STREAM_TOKENS
                    if current_tokens != tokens:
                        break
                    try:
                        raw = ws.recv(timeout=1)
                    except TimeoutError:
                        continue
                    events = json.loads(raw)
                    for event in events if isinstance(events, list) else [events]:
                        if not isinstance(event, dict):
                            continue
                        event_type = str(event.get("event_type") or "")
                        token = str(event.get("asset_id") or "")
                        if event_type != "last_trade_price" or token not in tokens:
                            continue
                        price = float(event.get("price") or 0)
                        size = float(event.get("size") or 0)
                        side = str(event.get("side") or "").upper()
                        if not (math.isfinite(price) and math.isfinite(size) and price > 0 and size > 0):
                            continue
                        received_ms = int(time.time() * 1000)
                        tx_hash = str(event.get("transaction_hash") or "")
                        key = (tx_hash, token, price, size, side) if tx_hash else (
                            token, price, size, side, str(event.get("timestamp") or "")
                        )
                        with PAPER_TRADE_EVENT_LOCK:
                            if key in PAPER_TRADE_DEDUPE_SET:
                                continue
                            if len(PAPER_TRADE_DEDUPE) == PAPER_TRADE_DEDUPE.maxlen:
                                PAPER_TRADE_DEDUPE_SET.discard(PAPER_TRADE_DEDUPE[0])
                            PAPER_TRADE_DEDUPE.append(key)
                            PAPER_TRADE_DEDUPE_SET.add(key)
                            if len(PAPER_TRADE_EVENTS) == PAPER_TRADE_EVENTS.maxlen:
                                PAPER_TRADE_GAP = True
                            PAPER_TRADE_EVENTS.append({
                                "asset_id": token,
                                "price": price,
                                "size": size,
                                "side": side,
                                "received_ms": received_ms,
                            })
            if TRADE_STREAM_STOP.is_set():
                break
            with TRADE_STREAM_LOCK:
                changed_tokens = TRADE_STREAM_TOKENS != tokens
            if changed_tokens:
                TRADE_STREAM_STATUS = "subscribing"
                continue
            TRADE_STREAM_STATUS = "reconnecting"
            if had_connection:
                with PAPER_TRADE_EVENT_LOCK:
                    has_resting = any(o.get("status") == "LIVE" for o in PAPER_ORDERS.values())
                if has_resting:
                    PAPER_TRADE_GAP = True
                    SESSION_ENTRY_HALT_REASON = (
                        "paper trade stream disconnected with resting orders; fills are unknown"
                    )
                    log_event(f"CRITICAL: {SESSION_ENTRY_HALT_REASON}", "red")
            had_connection = True
        except Exception as exc:
            TRADE_STREAM_STATUS = "reconnecting"
            if had_connection:
                with PAPER_TRADE_EVENT_LOCK:
                    has_resting = any(o.get("status") == "LIVE" for o in PAPER_ORDERS.values())
                if has_resting:
                    PAPER_TRADE_GAP = True
                    SESSION_ENTRY_HALT_REASON = (
                        "paper trade stream failed with resting orders; fills are unknown"
                    )
                    log_event(f"CRITICAL: {SESSION_ENTRY_HALT_REASON}", "red")
            had_connection = True
            log_event(f"Paper trade stream unavailable: {type(exc).__name__}", "yellow")
            TRADE_STREAM_STOP.wait(backoff)
            backoff = min(15.0, backoff * 2)


def _simulate_paper_fills(market_states, market_books):
    """Apply only subsequent SELL trade prints after estimated queue ahead clears."""
    if not DRY_RUN:
        return
    if PAPER_TRADE_GAP:
        for order in PAPER_ORDERS.values():
            if order.get("status") == "LIVE":
                order["status"] = "UNKNOWN"
        return
    with PAPER_TRADE_EVENT_LOCK:
        trades = list(PAPER_TRADE_EVENTS)
        PAPER_TRADE_EVENTS.clear()
    if not trades:
        return
    states_by_order = {
        str(state.get("last_order_id")): (symbol, state)
        for symbol, state in market_states.items()
        if state.get("last_order_id")
    }
    for trade in trades:
        if trade["side"] != "SELL":
            continue
        for order_id, (symbol, state) in states_by_order.items():
            order = PAPER_ORDERS.get(order_id)
            if not order or order.get("status") != "LIVE":
                continue
            if str(order.get("token_id")) != trade["asset_id"]:
                continue
            if trade["received_ms"] < int(float(order["created_at"]) * 1000):
                continue
            if trade["price"] > float(order["price"]) + 1e-9:
                continue
            queue_before = max(0.0, float(order.get("queue_ahead") or 0.0))
            queue_consumed = min(queue_before, trade["size"])
            order["queue_ahead"] = queue_before - queue_consumed
            fillable = max(0.0, trade["size"] - queue_consumed)
            remaining = max(0.0, float(order["size"]) - float(order.get("size_matched") or 0.0))
            matched = min(remaining, fillable)
            if matched <= 0:
                continue
            order["size_matched"] = float(order.get("size_matched") or 0.0) + matched
            order["status"] = (
                "MATCHED" if order["size_matched"] >= float(order["size"]) - 1e-8
                else "LIVE"
            )
            log_event(
                f"{symbol} paper SELL print filled {matched:.2f}sh @ ${order['price']:.3f} "
                f"(queue remaining {order['queue_ahead']:.2f})",
                "green",
            )


def _update_entry_halt(market_states, market_books):
    """Halt new entries after session or marked position loss limits are hit."""
    global SESSION_ENTRY_HALT_REASON
    if SESSION_ENTRY_HALT_REASON:
        return
    if DRY_RUN and SESSION_PAPER_PNL <= -MAX_SESSION_LOSS_USD:
        SESSION_ENTRY_HALT_REASON = f"session loss reached ${SESSION_PAPER_PNL:.2f}"
    else:
        for symbol, state in market_states.items():
            if state.get('last_order_id') == 'timeout-assumed-live':
                SESSION_ENTRY_HALT_REASON = (
                    f"{symbol} order submit is ambiguous; waiting for exchange-side GTD expiry"
                )
                break
            if not state.get('entered'):
                continue
            side = state.get('favored_side')
            up_book, down_book = market_books.get(symbol, (None, None))
            held_book = up_book if side == "UP" else down_book if side == "DOWN" else None
            bid = held_book.get('best_bid') if held_book else None
            shares = float(state.get('filled_shares') or 0.0)
            cost = float(state.get('filled_cost') or 0.0)
            if bid is None or shares <= 0 or cost <= 0:
                continue
            marked_pnl = shares * float(bid) - cost
            if DRY_RUN:
                marked_pnl -= (float(bid) * DRY_RUN_FEE_RATE + DRY_RUN_SLIPPAGE) * shares
            if marked_pnl <= -MAX_POSITION_LOSS_USD:
                SESSION_ENTRY_HALT_REASON = (
                    f"{symbol} marked loss reached ${marked_pnl:.2f} "
                    f"(limit -${MAX_POSITION_LOSS_USD:.2f})"
                )
                break
    if SESSION_ENTRY_HALT_REASON:
        log_event(
            f"CRITICAL: {SESSION_ENTRY_HALT_REASON}; halting new entries and canceling resting bids",
            "red",
        )


def _short_address(address):
    if not address:
        return "missing"
    return f"{address[:6]}...{address[-4:]}"


def _validate_configuration():
    numeric_values = {
        "TARGET_NOTIONAL_USD": TARGET_NOTIONAL_USD,
        "MAX_ORDER_NOTIONAL_USD": MAX_ORDER_NOTIONAL_USD,
        "MAX_TOTAL_EXPOSURE_USD": MAX_TOTAL_EXPOSURE_USD,
        "MAX_SESSION_LOSS_USD": MAX_SESSION_LOSS_USD,
        "MAX_POSITION_LOSS_USD": MAX_POSITION_LOSS_USD,
        "SIM_BANKROLL_USD": SIM_BANKROLL_USD,
        "ENTRY_THRESHOLD": ENTRY_THRESHOLD,
        "MAX_BUY_PRICE": MAX_BUY_PRICE,
        "MIN_DECIDED_BID": MIN_DECIDED_BID,
        "MAX_OPPOSITE_BID": MAX_OPPOSITE_BID,
        "EARLY_WINDOW_MAX_BUY_PRICE": EARLY_WINDOW_MAX_BUY_PRICE,
        "DRY_RUN_FEE_RATE": DRY_RUN_FEE_RATE,
        "DRY_RUN_SLIPPAGE": DRY_RUN_SLIPPAGE,
    }
    invalid = [name for name, value in numeric_values.items() if not math.isfinite(value)]
    invalid.extend(
        name for name, value in (
            ("TARGET_NOTIONAL_USD", TARGET_NOTIONAL_USD),
            ("MAX_ORDER_NOTIONAL_USD", MAX_ORDER_NOTIONAL_USD),
            ("MAX_TOTAL_EXPOSURE_USD", MAX_TOTAL_EXPOSURE_USD),
            ("MAX_SESSION_LOSS_USD", MAX_SESSION_LOSS_USD),
            ("MAX_POSITION_LOSS_USD", MAX_POSITION_LOSS_USD),
            ("SIM_BANKROLL_USD", SIM_BANKROLL_USD),
        ) if value <= 0
    )
    if not (0 < ENTRY_THRESHOLD < 1 and 0 < MAX_BUY_PRICE <= 1):
        invalid.append("ENTRY_THRESHOLD/MAX_BUY_PRICE")
    if not (0 < EARLY_WINDOW_MAX_BUY_PRICE <= MAX_BUY_PRICE):
        invalid.append("EARLY_WINDOW_MAX_BUY_PRICE")
    if not (0 <= MAX_OPPOSITE_BID < 1 and 0 <= MIN_DECIDED_BID < 1):
        invalid.append("MIN_DECIDED_BID/MAX_OPPOSITE_BID")
    if SIGNATURE_TYPE not in (0, 1, 2):
        invalid.append("SIGNATURE_TYPE (must match the funded wallet: 0, 1, or 2)")
    if DRY_RUN_FEE_RATE < 0 or DRY_RUN_SLIPPAGE < 0:
        invalid.append("DRY_RUN_FEE_RATE/DRY_RUN_SLIPPAGE")
    if not (0 <= STOP_THRESHOLD_SEC < ENTRY_WINDOW_SEC < MARKET_DURATION):
        invalid.append("STOP_THRESHOLD_SEC/ENTRY_WINDOW_SEC")
    if not (STOP_THRESHOLD_SEC < SOFT_ENTRY_CUTOFF_SEC < ENTRY_WINDOW_SEC):
        invalid.append("STOP_THRESHOLD_SEC/SOFT_ENTRY_CUTOFF_SEC/ENTRY_WINDOW_SEC")
    if ORDER_UPDATE_INTERVAL <= 0:
        invalid.append("ORDER_UPDATE_INTERVAL")
    if invalid:
        raise SystemExit("Invalid configuration values: " + ", ".join(dict.fromkeys(invalid)))

    if DRY_RUN:
        return
    missing = [name for name in (PRIVATE_KEY_ENV_NAME, PUBLIC_KEY_ENV_NAME) if not os.getenv(name)]
    placeholders = [
        name for name in (PRIVATE_KEY_ENV_NAME, PUBLIC_KEY_ENV_NAME)
        if os.getenv(name) and ("YOUR_" in os.getenv(name).upper() or "..." in os.getenv(name))
    ]
    if missing:
        print(colored("Configuration incomplete. Copy .env.example to .env and fill in:", "red", attrs=["bold"]))
        for name in missing:
            print(f"  - {name}")
        raise SystemExit(2)
    if placeholders:
        print(colored("Replace the placeholder values in .env before launching:", "red", attrs=["bold"]))
        for name in placeholders:
            print(f"  - {name}")
        raise SystemExit(2)
    raise SystemExit(
        "Live mode is disabled: this script does not yet track authoritative live "
        "fills and realized settlement PnL, so its session-loss limit cannot be enforced. "
        "Keep DRY_RUN=true until live settlement accounting is implemented and verified."
    )


def _resolve_paper_window(market_states, market_books):
    """Close paper-window accounting without inventing settlement outcomes."""
    global SESSION_PAPER_WINS, SESSION_PAPER_LOSSES, SESSION_PAPER_PNL
    global SESSION_PAPER_MAX_LOSS, SESSION_PAPER_MAX_PROFIT
    global SESSION_PAPER_PEAK_PNL, SESSION_MAX_DD, SESSION_CURRENT_STREAK
    if not DRY_RUN:
        return
    for symbol, state in market_states.items():
        if not state.get('entered') or state.get('paper_resolved'):
            continue
        if not state.get('paper_filled'):
            state['paper_resolved'] = True
            continue
        shares = float(state.get('filled_shares') or 0.0)
        cost = float(state.get('filled_cost') or 0.0)
        if shares <= 0 or cost <= 0:
            state['paper_resolved'] = True
            continue
        up_book, down_book = market_books.get(symbol, (None, None))
        if up_book is None:
            up_book = state.get('_last_up_book')
        if down_book is None:
            down_book = state.get('_last_down_book')
        favored = up_book if state.get('favored_side') == "UP" else down_book
        opposite = down_book if state.get('favored_side') == "UP" else up_book
        favored_bid = favored.get('best_bid') if favored else None
        opposite_bid = opposite.get('best_bid') if opposite else None
        # A cached quote (even at 0.99) is not an official market resolution.
        # Never score proxy wins/losses or feed them into the session loss guard.
        state['paper_resolved'] = True
        log_event(f"{symbol} paper fill excluded from WR/PnL: no official resolution data", "yellow")


# ============================================================================
# DASHBOARD RENDERING
# ============================================================================
# Every line is built from (text, colour, background, bold) segments and padded
# on the plain text, never on the ANSI escape codes. That keeps the frame and
# the table columns aligned no matter how much colour is applied.


def _detect_unicode():
    """Use box-drawing glyphs only when the terminal can encode them."""
    if os.getenv("SNIPER_ASCII_UI", "").strip().lower() in {"1", "true", "yes", "on"}:
        return False
    encoding = getattr(sys.stdout, "encoding", None) or "utf-8"
    try:
        "─│╭╮╰╯█░▁▂▃▄▅▆▇·¢…→".encode(encoding)
    except (UnicodeEncodeError, LookupError):
        return False
    return True


_UI_UNICODE = _detect_unicode()
_G = (
    {
        'h': '─', 'v': '│', 'tl': '╭', 'tr': '╮', 'bl': '╰', 'br': '╯',
        'fill': '█', 'empty': '░', 'dot': '·', 'cent': '¢', 'arrow': '→',
        'ellipsis': '…', 'spark': '▁▂▃▄▅▆▇█', 'spinner': '⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏',
    }
    if _UI_UNICODE else
    {
        'h': '-', 'v': '|', 'tl': '+', 'tr': '+', 'bl': '+', 'br': '+',
        'fill': '#', 'empty': '.', 'dot': '.', 'cent': 'c', 'arrow': '->',
        'ellipsis': '...', 'spark': '_.:-=+*#', 'spinner': 'oO0.',
    }
)

C_BORDER = "dark_grey"
C_TITLE = "light_cyan"
C_MUTED = "light_grey"
C_TEXT = "white"
C_ACCENT = "cyan"
C_GOOD = "green"
C_WARN = "yellow"
C_BAD = "red"

SCANNER_HEADERS = {
    "ASSET": "ASSET",
    "UP": "UP BID/ASK",
    "DOWN": "DOWN BID/ASK",
    "LEAD": "LEAD",
    "GAP": "GAP",
    "STRIKE": "STRIKE Δ",
}


def _seg(text, color=None, bold=False, background=None):
    return (str(text), color, background, bold)


def _flatten(segments):
    """Expand helpers that return segment lists (e.g. `_kpi`) inside a row."""
    for segment in segments:
        if isinstance(segment, list):
            yield from _flatten(segment)
        else:
            yield segment


def _width(segments):
    return sum(len(segment[0]) for segment in _flatten(segments))


def _fit(segments, width, align="left"):
    """Clip or pad a segment list to exactly `width` visible characters."""
    width = max(0, int(width))
    fitted, used = [], 0
    for text, color, background, bold in _flatten(segments):
        if used >= width:
            break
        if not text:
            continue
        take = min(len(text), width - used)
        fitted.append((text[:take], color, background, bold))
        used += take
    filler = width - used
    if filler > 0:
        pad = (" " * filler, None, None, False)
        if align == "right":
            fitted.insert(0, pad)
        else:
            fitted.append(pad)
    return fitted


def _paint(segments):
    parts = []
    for text, color, background, bold in segments:
        if not text:
            continue
        if color or background or bold:
            parts.append(
                colored(text, color, background, attrs=["bold"] if bold else None)
            )
        else:
            parts.append(text)
    return "".join(parts)


def _line(segments, width, align="left"):
    return _paint(_fit(segments, width, align))


def _trim(text, limit):
    text = str(text)
    if limit <= 0:
        return ""
    if len(text) <= limit:
        return text
    tail = _G['ellipsis']
    if limit <= len(tail):
        return text[:limit]
    return text[: limit - len(tail)] + tail


def _clock(seconds):
    seconds = max(0, int(seconds))
    return f"{seconds // 60:02d}:{seconds % 60:02d}"


def _panel(title, rows, width, right=None):
    """Boxed panel; every returned line is exactly `width` cells wide."""
    inner = width - 2
    border = C_BORDER
    if right and len(right) > width // 2:
        right = None
    prefix_width = 2 + (len(title) + 1 if title else 0)
    suffix_width = len(right) + 3 if right else 1
    fills = inner - prefix_width - suffix_width
    if fills < 1:
        right = None
        suffix_width = 1
        fills = inner - prefix_width - suffix_width
    if fills < 1:
        title = _trim(title, max(0, inner - 4)) if title else title
        prefix_width = 2 + (len(title) + 1 if title else 0)
        fills = max(0, inner - prefix_width - suffix_width)

    top = [_seg(_G['tl'], border)]
    if title:
        top += [_seg(_G['h'], border), _seg(" "), _seg(title, C_TITLE, bold=True), _seg(" ")]
    else:
        top.append(_seg(_G['h'], border))
    top.append(_seg(_G['h'] * fills, border))
    if right:
        top += [_seg(" "), _seg(right, C_MUTED), _seg(" "), _seg(_G['h'], border)]
    else:
        top.append(_seg(_G['h'], border))
    top.append(_seg(_G['tr'], border))

    content = width - 4
    lines = [_paint(top)]
    for row in rows:
        lines.append(_paint(
            [_seg(_G['v'], border), _seg(" ")]
            + _fit(row, content)
            + [_seg(" "), _seg(_G['v'], border)]
        ))
    lines.append(_paint([
        _seg(_G['bl'], border),
        _seg(_G['h'] * inner, border),
        _seg(_G['br'], border),
    ]))
    return lines


def _split_row(left, right, width, gap=2):
    """Left content plus a right-aligned tail, dropped whole when space is tight."""
    if right:
        left_width = _width(left)
        right_width = _width(right)
        if left_width + gap + right_width <= width:
            return left + [_seg(" " * (width - left_width - right_width))] + right
    return _fit(left, width)


_STRIKE_CACHE = {}
_STRIKE_LOCK = threading.Lock()
_STRIKE_TTL = 30.0
_STRIKE_FUTURES = {}
_STRIKE_FUTURE_LOCK = threading.Lock()
_STRIKE_EXECUTOR = ThreadPoolExecutor(max_workers=5, thread_name_prefix="dashboard-data")
_STRIKE_PAIRS = {
    "BTC": "BTCUSDT", "ETH": "ETHUSDT", "SOL": "SOLUSDT",
    "XRP": "XRPUSDT", "DOGE": "DOGEUSDT",
}


def _strike_delta(symbol):
    pair = _STRIKE_PAIRS.get(symbol)
    if not pair:
        return None
    now = time.time()
    with _STRIKE_LOCK:
        cached = _STRIKE_CACHE.get(symbol)
        if cached and now - cached[0] < _STRIKE_TTL:
            return cached[1]
    text = None
    try:
        kline = _http_session().get(
            "https://api.binance.com/api/v3/klines",
            params={"symbol": pair, "interval": "5m", "limit": 1},
            timeout=2,
        ).json()
        spot = float(
            _http_session().get(
                "https://api.binance.com/api/v3/ticker/price",
                params={"symbol": pair},
                timeout=2,
            ).json()["price"]
        )
        open_price = float(kline[0][1])
        if open_price > 0:
            delta = (spot - open_price) / open_price * 100.0
            text = f"{delta:+.2f}%"
    except Exception:
        text = None
    with _STRIKE_LOCK:
        _STRIKE_CACHE[symbol] = (now, text)
    return text


def _strike_delta_cached(symbol):
    """Return cached Binance telemetry and refresh it without blocking the UI loop."""
    now = time.time()
    with _STRIKE_LOCK:
        cached = _STRIKE_CACHE.get(symbol)
        if cached and now - cached[0] < _STRIKE_TTL:
            return cached[1]
    with _STRIKE_FUTURE_LOCK:
        future = _STRIKE_FUTURES.get(symbol)
        if future is None or future.done():
            _STRIKE_FUTURES[symbol] = _STRIKE_EXECUTOR.submit(_strike_delta, symbol)
    return cached[1] if cached else None


def _strike_color(text):
    if not text:
        return C_MUTED
    try:
        value = float(text.rstrip("%"))
    except ValueError:
        return C_MUTED
    magnitude = abs(value)
    if magnitude >= 0.20:
        return C_GOOD
    if magnitude >= 0.08:
        return C_WARN
    return C_BAD


def _money(value):
    return f"${value:.2f}"


def _signed_money(value):
    if abs(value) < 0.005:
        return "$0.00"
    return f"{'+' if value > 0 else '-'}${abs(value):.2f}"


def _kpi(label, value, color=C_TEXT, bold=True):
    return [_seg(label.upper() + " ", C_MUTED), _seg(value, color, bold=bold)]


def _gauge(ratio, width, color):
    ratio = max(0.0, min(1.0, ratio))
    filled = int(round(ratio * width))
    segments = [_seg(_G['fill'] * filled, color)]
    if width > filled:
        segments.append(_seg(_G['empty'] * (width - filled), C_MUTED))
    return segments


def _latency_color(value):
    if value is None:
        return C_MUTED
    if value < 150:
        return C_GOOD
    if value < 400:
        return C_TEXT
    if value < 800:
        return C_WARN
    return C_BAD


def _stats(bucket, lock):
    with lock:
        values = list(bucket)
    if not values:
        return None
    ordered = sorted(values)
    return {
        'last': values[-1],
        'avg': sum(values) / len(values),
        'min': ordered[0],
        'max': ordered[-1],
        'p95': ordered[min(len(ordered) - 1, int(round(0.95 * (len(ordered) - 1))))],
        'n': len(values),
    }


def _sparkline(samples, width):
    blocks = _G['spark']
    data = [float(value) for value in samples if value is not None][-width:]
    if not data:
        return [_seg(_G['dot'] * width, C_MUTED)]
    low, high = min(data), max(data)
    span = high - low
    if span <= 1e-9:
        indexes = [len(blocks) // 2] * len(data)
    else:
        indexes = [
            int(round((value - low) / span * (len(blocks) - 1)))
            for value in data
        ]
    return [_seg("".join(blocks[index] for index in indexes), C_ACCENT)]


def draw_dashboard(market_states, market_books, time_left):
    """Render the live market, risk, execution, and session telemetry dashboard."""
    global DASHBOARD_RENDERED
    if not sys.stdout.isatty():
        return

    render_started = time.perf_counter()
    size = shutil.get_terminal_size((120, 40))
    # Keep one column spare: writing the last cell of a row makes many terminals
    # auto-wrap, which would push the whole dashboard down by a line.
    width = min(118, max(24, size.columns - 1))
    content_w = width - 4

    elapsed = max(0, int(time.time() - SESSION_START))
    runtime = f"{elapsed // 3600:02d}:{(elapsed // 60) % 60:02d}:{elapsed % 60:02d}"
    left = (
        max(0, min(MARKET_DURATION, int(time_left)))
        if time_left is not None else MARKET_DURATION
    )

    first_state = next(iter(market_states.values()), None)
    market_ts = (first_state or {}).get('market_ts') or get_current_market_timestamp()
    window_start = datetime.fromtimestamp(market_ts, ET).strftime("%H:%M:%S")
    window_end = datetime.fromtimestamp(market_ts + MARKET_DURATION, ET).strftime("%H:%M:%S")

    if left <= STOP_THRESHOLD_SEC:
        phase_label, phase_color, poll_interval = "CUTOFF / STANDING DOWN", C_BAD, 1.0
    elif left <= ENTRY_WINDOW_SEC:
        phase_label, phase_color, poll_interval = "SNIPE WINDOW ACTIVE", C_GOOD, 1.0
    elif left <= ENTRY_WINDOW_SEC + 10:
        phase_label, phase_color, poll_interval = "ARMING FAST POLL", C_WARN, 1.0
    else:
        phase_label, phase_color, poll_interval = "WAITING FOR FINAL 60s", C_MUTED, 3.0

    to_cutoff = max(0, left - STOP_THRESHOLD_SEC)
    countdown = _clock(to_cutoff)
    if left <= STOP_THRESHOLD_SEC:
        countdown_color = C_BAD
    elif to_cutoff <= 15:
        countdown_color = C_WARN
    elif left <= ENTRY_WINDOW_SEC:
        countdown_color = C_GOOD
    else:
        countdown_color = C_TEXT

    if left > ENTRY_WINDOW_SEC:
        entry_note = f"entry opens in {left - ENTRY_WINDOW_SEC}s"
    elif left > STOP_THRESHOLD_SEC:
        entry_note = f"entry closes in {to_cutoff}s"
    else:
        entry_note = "bids canceled, standing down"

    account = ACCOUNT_SUFFIX.lstrip("_") or "default"
    address = os.getenv(PUBLIC_KEY_ENV_NAME, "")
    short_address = f"{address[:6]}{_G['dot']}{_G['dot']}{address[-4:]}" if len(address) >= 12 else "unavailable"
    spinner = _G['spinner'][next(_DASHBOARD_FRAME) % len(_G['spinner'])]

    # ---- latency telemetry -------------------------------------------------
    api = _stats(CLOB_PING_MS, _PING_LOCK)
    ui = _stats(RENDER_LATENCY_MS, _LATENCY_LOCK)
    with _PING_LOCK:
        ping_samples = list(CLOB_PING_MS)
    api_text = f"{api['avg']:.0f}ms" if api else "--"
    api_color = _latency_color(api['avg'] if api else None)
    ui_text = f"{ui['last']:.0f}ms" if ui else "--"
    with _LATENCY_LOCK:
        order_sample = ORDER_LATENCY_MS[-1] if ORDER_LATENCY_MS else None
        cancel_sample = CANCEL_LATENCY_MS[-1] if CANCEL_LATENCY_MS else None

    # ---- capital and risk --------------------------------------------------
    # Reservations include unfilled bids and filled shares. This split is for
    # display only; the exposure cap uses the same underlying reservation ledger.
    open_bid_cost = 0.0
    open_bid_shares = 0.0
    filled_cost = sum(float(s.get('filled_cost') or 0.0) for s in market_states.values())
    filled_shares = sum(float(s.get('filled_shares') or 0.0) for s in market_states.values())
    # Resting paper orders remain in OPEN_ORDER_COSTS and are shown in the
    # open-bid row below; do not add them to filled holdings a second time.
    for state in market_states.values():
        order_id = state.get('last_order_id')
        exposure_key = str(state.get('exposure_key') or order_id or '')
        reserved = _snapshot_order_costs().get(exposure_key, 0.0)
        if order_id and reserved:
            open_bid_cost += max(0.0, reserved - float(state.get('filled_cost') or 0.0))
            order_price = float(state.get('last_order_price') or 0.0)
            if order_price > 0:
                open_bid_shares += max(0.0, reserved - float(state.get('filled_cost') or 0.0)) / order_price
    total_exposure = _current_exposure_usd()
    other_reserved = max(0.0, total_exposure - open_bid_cost - filled_cost)
    limit = max(0.0, _exposure_limit_usd())
    exposure_ratio = min(1.0, total_exposure / limit) if limit else 1.0
    free_capacity = max(0.0, limit - total_exposure)
    exposure_color = (
        C_BAD if exposure_ratio >= 0.9 else
        C_WARN if exposure_ratio >= 0.7 else
        C_GOOD
    )

    # ---- header ------------------------------------------------------------
    mode_badge = (
        _seg(" LIVE MAKER ", "black", True, "on_light_red")
        if not DRY_RUN else
        _seg(" DRY RUN ", "black", True, "on_light_yellow")
    )
    tagline = [
        _seg(f"last-{ENTRY_WINDOW_SEC}s maker; no new bids <{SOFT_ENTRY_CUTOFF_SEC}s", C_MUTED),
        _seg(f" {_G['dot']} ", C_BORDER),
        _seg(f"{MAX_BUY_PRICE * 100:.0f}{_G['cent']} cap", C_MUTED),
        _seg(f" {_G['dot']} ", C_BORDER),
        _seg(f"{len(CRYPTO_ASSETS)} assets", C_MUTED),
    ]
    latency_bits = [
        _kpi("poll", f"{poll_interval:.1f}s"),
        _seg("  "),
        _kpi("api", api_text, api_color),
    ]
    if ping_samples:
        latency_bits += [_seg(" ")] + _sparkline(ping_samples, 8)
    latency_bits += [
        _seg("  "),
        _kpi("ui", ui_text, _latency_color(ui['last'] if ui else None)),
    ]
    if order_sample is not None:
        latency_bits += [_seg("  "), _kpi("ord", f"{order_sample:.0f}ms", _latency_color(order_sample))]
    if cancel_sample is not None:
        latency_bits += [_seg("  "), _kpi("cxl", f"{cancel_sample:.0f}ms", _latency_color(cancel_sample))]
    if _width(tagline) + 2 + _width(latency_bits) > content_w:
        # Prefer the core numbers over the sparkline and account-side timings.
        latency_bits = [
            _kpi("poll", f"{poll_interval:.1f}s"),
            _seg("  "),
            _kpi("api", api_text, api_color),
            _seg("  "),
            _kpi("ui", ui_text, _latency_color(ui['last'] if ui else None)),
        ]

    bankroll_label = "sim" if DRY_RUN else "usdc"
    bankroll_text = (
        f"${SIM_BANKROLL_USD + SESSION_PAPER_PNL:.2f}" if DRY_RUN else "USDC --"
    )

    header_rows = [
        _split_row(
            [
                _seg(spinner + " ", C_ACCENT, bold=True),
                mode_badge,
                _seg(" "),
                _seg(account.upper(), C_TEXT, bold=True),
                _seg(f" {_G['dot']} ", C_BORDER),
                _seg(short_address, C_MUTED),
            ],
            [
                _kpi("uptime", runtime),
                _seg("   "),
                _kpi(bankroll_label, bankroll_text, C_ACCENT),
            ],
            content_w,
        ),
        _split_row(tagline, latency_bits, content_w),
    ]

    # ---- market window -----------------------------------------------------
    progress = min(1.0, max(0.0, (MARKET_DURATION - left) / MARKET_DURATION))
    bar_w = max(8, content_w - 7)
    entry_ratio = (MARKET_DURATION - ENTRY_WINDOW_SEC) / MARKET_DURATION
    cutoff_ratio = (MARKET_DURATION - STOP_THRESHOLD_SEC) / MARKET_DURATION
    filled_cells = int(round(progress * bar_w))
    accent_end = min(filled_cells, int(round(entry_ratio * bar_w)))
    good_end = min(filled_cells, int(round(cutoff_ratio * bar_w)))
    bar_segments = [_seg("[", C_BORDER)]
    if accent_end:
        bar_segments.append(_seg(_G['fill'] * accent_end, C_ACCENT))
    if good_end > accent_end:
        bar_segments.append(_seg(_G['fill'] * (good_end - accent_end), C_GOOD))
    if filled_cells > good_end:
        bar_segments.append(_seg(_G['fill'] * (filled_cells - good_end), C_BAD))
    if bar_w > filled_cells:
        bar_segments.append(_seg(_G['empty'] * (bar_w - filled_cells), C_MUTED))
    bar_segments.append(_seg("]", C_BORDER))
    bar_segments.append(_seg(f" {progress * 100:3.0f}%", C_TEXT, bold=True))

    timer_left = [
        _seg(countdown, countdown_color, bold=True),
        _seg("  to cutoff", C_MUTED),
        _seg("   "),
        _seg(phase_label, phase_color, bold=True),
    ]
    timer_right = [_seg(entry_note, C_MUTED)]
    if _width(timer_left) + 2 + _width(timer_right) > content_w:
        # Narrow terminal: shorten the phase wording before clipping it.
        short_phase = {
            "CUTOFF / STANDING DOWN": "CUTOFF",
            "SNIPE WINDOW ACTIVE": "SNIPING",
            "ARMING FAST POLL": "ARMING",
            "WAITING FOR FINAL 60s": "WAITING",
        }
        timer_left = [
            _seg(countdown, countdown_color, bold=True),
            _seg("  "),
            _seg(short_phase.get(phase_label, phase_label), phase_color, bold=True),
        ]
        timer_right = [_seg(f"{left}s left", C_MUTED)]
    timer_rows = [
        _split_row(timer_left, timer_right, content_w),
        _split_row(bar_segments, [_kpi("id", market_ts)], content_w),
    ]

    # ---- risk rows ---------------------------------------------------------
    gauge_w = 12 if content_w >= 76 else 8 if content_w >= 52 else 0
    risk_row_1_left = _kpi("exposure", f"${total_exposure:.2f} / ${limit:.2f}")
    if gauge_w:
        risk_row_1_left += [_seg("  ")] + _gauge(exposure_ratio, gauge_w, exposure_color)
        risk_row_1_left += [_seg(f" {exposure_ratio * 100:3.0f}%", exposure_color, bold=True)]
    else:
        risk_row_1_left += [_seg("  "), _seg(f"{exposure_ratio * 100:.0f}%", exposure_color, bold=True)]
    risk_row_1_right = [
        _kpi("free", f"${free_capacity:.2f}", C_GOOD if free_capacity > 0 else C_MUTED),
        _seg("  " + _G['dot'] + "  ", C_BORDER),
        _kpi("roi/fill", f"+{TARGET_GAIN_PCT:.2f}%", C_GOOD),
    ]
    if _width(risk_row_1_left) + 2 + _width(risk_row_1_right) > content_w:
        risk_row_1_right = risk_row_1_right[:1]
    risk_row_1 = _split_row(risk_row_1_left, risk_row_1_right, content_w)
    risk_chunks = [
        _kpi("open", f"{open_bid_shares:.2f}sh")
        + [_seg(f" ({_money(open_bid_cost)})", C_MUTED)],
        _kpi("holding", f"{filled_shares:.2f}sh")
        + [_seg(f" ({_money(filled_cost)})", C_MUTED)],
        _kpi("payout", f"${filled_shares:.2f}"),
    ]
    if other_reserved > 0.005:
        risk_chunks.append(_kpi("reserved", _money(other_reserved), C_WARN))
    # Keep chunks whole: on narrow terminals drop the least useful tail rather
    # than slicing a figure in half.
    risk_row_2_left = []
    for chunk in risk_chunks:
        separator = 4 if risk_row_2_left else 0
        if separator and _width(risk_row_2_left) + separator + _width(chunk) > content_w:
            break
        if separator:
            risk_row_2_left.append(_seg(" " * separator))
        risk_row_2_left += chunk
    # Max loss/profit reflect current exposure (resting + filled) so they are
    # accurate and non-zero while orders are live, not only after resolution.
    total_cost = open_bid_cost + filled_cost
    total_shares = open_bid_shares + filled_shares
    if DRY_RUN:
        reserve = total_cost * DRY_RUN_FEE_RATE + DRY_RUN_SLIPPAGE * total_shares
    else:
        reserve = 0.0
    max_loss_val = total_cost + reserve
    max_profit_val = max(0.0, total_shares * 1.0 - total_cost - reserve)
    loss_value = _signed_money(-max_loss_val)
    profit_value = _signed_money(max_profit_val)
    loss_color = C_BAD if filled_cost else C_MUTED
    profit_color = C_GOOD if filled_shares > filled_cost else C_MUTED
    loss_profit_variants = [
        [
            _kpi("max loss", loss_value, loss_color),
            _seg("  " + _G['dot'] + "  ", C_BORDER),
            _kpi("max profit", profit_value, profit_color),
        ],
        [
            _kpi("loss", loss_value, loss_color),
            _seg("  " + _G['dot'] + "  ", C_BORDER),
            _kpi("profit", profit_value, profit_color),
        ],
        [
            _kpi("loss", loss_value, loss_color),
            _seg(" / ", C_BORDER),
            _kpi("gain", profit_value, profit_color),
        ],
    ]
    loss_profit_bits = next(
        (variant for variant in loss_profit_variants if _width(variant) <= content_w),
        loss_profit_variants[-1],
    )
    if _width(risk_row_2_left) + 2 + _width(loss_profit_bits) <= content_w:
        risk_rows = [risk_row_1, _split_row(risk_row_2_left, loss_profit_bits, content_w)]
    else:
        risk_rows = [
            risk_row_1,
            _fit(risk_row_2_left, content_w),
            _fit(loss_profit_bits, content_w),
        ]

    # ---- scanner -----------------------------------------------------------
    # The five-column table needs ~56 visible cells plus room for a status
    # message, so drop LEAD/GAP only below that.
    compact = width < 76
    wide = width >= 96 and not compact
    gap = 1 if compact else 2
    quote_sep = "" if compact else " "
    quote_width = 12 if compact else 13
    columns = (
        [("ASSET", 5), ("UP", quote_width), ("DOWN", quote_width), ("LEAD", 6), ("GAP", 9)]
        if not compact else
        [("ASSET", 6), ("UP", quote_width), ("DOWN", quote_width)]
    )
    if wide:
        columns = columns + [("STRIKE", 9)]
    while columns and content_w - sum(w for _, w in columns) - gap * len(columns) < 10:
        columns = columns[:-1]
    status_w = max(
        8,
        content_w - sum(w for _, w in columns) - gap * len(columns),
    )

    def table_row(cells, status):
        segments = []
        for key, col_width in columns:
            text, color, bold = cells.get(key, ("", C_MUTED, False))
            segments.append(_seg(_trim(text, col_width).ljust(col_width), color, bold))
            segments.append(_seg(" " * gap))
        segments.append(_seg(_trim(status[0], status_w).ljust(status_w), status[1]))
        return segments

    headers = dict(SCANNER_HEADERS)
    if wide:
        headers["STRIKE"] = "STRIKE Δ"
    scanner_rows = [table_row(
        {key: (headers[key], C_MUTED, True) for key, _ in columns},
        ("EXECUTION", C_MUTED),
    ), [_seg(_G['h'] * content_w, C_BORDER)]]

    def side_quote(bid, ask):
        bid_text = f"{bid:.3f}" if bid is not None else "---"
        ask_text = f"{ask:.3f}" if ask is not None else "---"
        return f"{bid_text}{quote_sep}/{quote_sep}{ask_text}"

    for symbol in CRYPTO_ASSETS:
        state = market_states.get(symbol)
        up_book, down_book = market_books.get(symbol, (None, None))
        up_bid = up_book.get('best_bid') if up_book else None
        up_ask = up_book.get('best_ask') if up_book else None
        down_bid = down_book.get('best_bid') if down_book else None
        down_ask = down_book.get('best_ask') if down_book else None
        up_text = side_quote(up_bid, up_ask)
        down_text = side_quote(down_bid, down_ask)

        qualified_side, _ = pick_decided_side(up_book, down_book)
        # A missing ask must not make the opposite side with a tiny ask appear
        # to lead when the other outcome has a strong bid (e.g. DOWN bid .99,
        # UP ask .01). Use the strongest available quote for the lead label;
        # keep the actual ask separate because entry qualification is ask-based.
        side_evidence = []
        for side, bid, ask in (("UP", up_bid, up_ask), ("DOWN", down_bid, down_ask)):
            quotes = [quote for quote in (bid, ask) if quote is not None]
            if quotes:
                side_evidence.append((side, max(quotes)))
        leading_side, leading_quote = max(
            side_evidence, key=lambda pair: pair[1], default=(None, None)
        )
        leading_ask = up_ask if leading_side == "UP" else down_ask if leading_side == "DOWN" else None
        signal_side = qualified_side or leading_side
        signal_ask = up_ask if signal_side == "UP" else down_ask if signal_side == "DOWN" else None
        if state is None:
            scanner_rows.append(table_row(
                {
                    "ASSET": (symbol, C_TEXT, True),
                    "UP": (up_text, C_MUTED, False),
                    "DOWN": (down_text, C_MUTED, False),
                    "LEAD": ("--", C_MUTED, False),
                    "GAP": ("--", C_MUTED, False),
                },
                ("discovery pending", C_MUTED),
            ))
            continue

        if qualified_side:
            up_color = C_GOOD if qualified_side == "UP" else C_TEXT
            down_color = C_GOOD if qualified_side == "DOWN" else C_TEXT
            lead_color = C_GOOD
        elif up_book or down_book:
            up_color = C_TEXT if up_book else C_MUTED
            down_color = C_TEXT if down_book else C_MUTED
            lead_color = C_WARN if leading_quote is not None and leading_quote >= ENTRY_THRESHOLD else C_TEXT
        else:
            up_color = down_color = lead_color = C_MUTED

        age = None
        if state.get('favored_side') and state.get('favored_since'):
            age = max(0, int(time.time() - state['favored_since']))
        if qualified_side:
            gap_text = f"+{max(0.0, (signal_ask - ENTRY_THRESHOLD) * 100):.1f}{_G['cent']}"
            if age is not None:
                gap_text += f" {age}s"
            gap_color = C_GOOD
        elif leading_ask is not None and leading_ask >= ENTRY_THRESHOLD:
            gap_text, gap_color = "FILTERED", C_WARN
        elif leading_ask is not None:
            gap_text = f"-{(ENTRY_THRESHOLD - leading_ask) * 100:.1f}{_G['cent']}"
            gap_color = C_MUTED
        else:
            gap_text, gap_color = "--", C_MUTED

        if state.get('position_check_unknown') and not state.get('entered'):
            status = ("POSITIONS UNKNOWN / PAUSED", C_BAD)
        elif state.get('entered'):
            status = (
                f"HOLD {float(state.get('filled_shares') or 0):.2f}sh "
                f"@ {float(state.get('entry_fill_price') or 0):.3f}",
                C_GOOD,
            )
        elif state.get('last_order_id'):
            kind = "SIM BID" if DRY_RUN else "LIVE BID"
            price = float(state.get('last_order_price') or 0.0)
            shares = calculate_shares(TARGET_NOTIONAL_USD, price) if price > 0 else 0.0
            edge = shares * (1.0 - price)
            book = up_book if state.get('favored_side') == "UP" else down_book
            queue = (book or {}).get('bid_queue_shares') or 0
            q_text = f"  Q {queue:.0f}sh" if queue else ""
            color = C_ACCENT if DRY_RUN else C_GOOD
            status = (f"{kind} {price:.3f} · +${edge:.2f}{q_text}", color)
        elif qualified_side:
            try:
                favored_book = up_book if qualified_side == "UP" else down_book
                opposite_book = down_book if qualified_side == "UP" else up_book
                target = compute_bid_target(favored_book, left, opposite_book)
                if target is None:
                    status = ("SIGNAL / NO BID", C_WARN)
                else:
                    shares = calculate_shares(TARGET_NOTIONAL_USD, target)
                    if total_exposure + target * shares > limit:
                        status = ("SIGNAL / CAPPED", C_WARN)
                    else:
                        edge = shares * (1.0 - target)
                        top_bid = up_bid if qualified_side == "UP" else down_bid
                        q_note = " (-1¢ Q)" if (top_bid is not None and target < top_bid - 0.005) else ""
                        queue = (favored_book or {}).get('bid_queue_shares') or 0
                        q_text = f"  Q {queue:.0f}sh" if queue else ""
                        status = (
                            f"ARMED {target:.3f}{q_note} (in {to_cutoff}s){q_text}",
                            C_WARN,
                        )
            except (TypeError, ValueError, KeyError):
                status = ("SIGNAL", C_GOOD)
        elif state.get('sharp_ask_drop', {}).get(state.get('favored_side')):
            status = ("ARMED / 2c ASK DROP", C_WARN)
        elif leading_side is not None and leading_ask is None:
            status = ("LEAD / ASK MISSING", C_WARN)
        elif leading_ask is None:
            status = ("NO BOOK", C_MUTED)
        elif leading_ask >= ENTRY_THRESHOLD:
            status = ("FILTERED / THIN BOOK", C_WARN)
        else:
            status = (
                f"CHOP / {(ENTRY_THRESHOLD - leading_ask) * 100:.1f}{_G['cent']} AWAY",
                C_MUTED,
            )

        strike_text = _strike_delta_cached(symbol) if wide else None
        cells = {
            "ASSET": (symbol, C_TEXT, True),
            "UP": (up_text, up_color, False),
            "DOWN": (down_text, down_color, False),
            "LEAD": (leading_side or "--", lead_color, True),
            "GAP": (gap_text, gap_color, False),
        }
        if wide:
            cells["STRIKE"] = (strike_text or "--", _strike_color(strike_text), False)
        scanner_rows.append(table_row(cells, status))

    # ---- session telemetry -------------------------------------------------
    fill_rate = (SESSION_ENTRIES / SESSION_SIGNALS * 100.0) if SESSION_SIGNALS else 0.0
    if DRY_RUN:
        result_bits = [
            _kpi("dry-run fills", "unverified / not scored", C_WARN),
            _seg("  "),
            _kpi("settlement", "not connected", C_MUTED),
        ]
    else:
        result_bits = [_seg("settlement not tracked yet", C_MUTED)]

    telemetry_rows = [
        _fit([
            _kpi("windows", SESSION_WINDOWS),
            _seg("   "),
            _kpi("signals", SESSION_SIGNALS, C_ACCENT if SESSION_SIGNALS else C_TEXT),
            _seg("   "),
            _kpi("fills", SESSION_ENTRIES, C_GOOD if SESSION_ENTRIES else C_TEXT),
            _seg("   "),
            _kpi("fill rate", f"{fill_rate:.0f}%", C_GOOD if fill_rate >= 50 else C_TEXT),
            _seg("   "),
            _kpi("attempts", SESSION_ATTEMPTS),
            _seg("   "),
            _kpi("reprices", SESSION_REPRICES),
            _seg("   "),
            _kpi("late fills", SESSION_LATE_FILLS, C_BAD if SESSION_LATE_FILLS else C_TEXT),
        ], content_w),
        _split_row(
            result_bits,
            [
                _kpi("timeouts", SESSION_TIMEOUTS, C_BAD if SESSION_TIMEOUTS else C_TEXT),
                _seg("   "),
                _kpi("cutoff cancels", SESSION_CUTOFF_CANCELS),
                _seg("   "),
                _kpi("skips", SESSION_SKIPS),
                _seg("   "),
                _kpi("max dd", _signed_money(-SESSION_MAX_DD),
                     C_BAD if SESSION_MAX_DD > 0 else C_MUTED),
                _seg("   "),
                _kpi("streak", f"{SESSION_CURRENT_STREAK:+d}",
                     C_GOOD if SESSION_CURRENT_STREAK > 0 else C_BAD if SESSION_CURRENT_STREAK < 0 else C_MUTED),
                _seg("   "),
                _kpi(
                    "kill" if DRY_RUN else "manual kill",
                    f"-${MAX_SESSION_LOSS_USD:.0f}" if DRY_RUN else "k/q",
                    C_MUTED,
                ),
            ],
            content_w,
        ),
    ]

    # ---- logs --------------------------------------------------------------
    log_limit = 4 if size.lines >= 42 else 3 if size.lines >= 36 else 2 if size.lines >= 30 else 1
    severity_colors = {
        "green": C_GOOD, "yellow": C_WARN, "red": C_BAD,
        "cyan": C_ACCENT, "white": C_TEXT,
    }

    def log_rows(entries, limit):
        rows = []
        for ts, message, color in list(entries)[-limit:]:
            rows.append([
                _seg(ts + " ", C_MUTED),
                _seg(_G['v'] + " ", severity_colors.get(color, C_MUTED)),
                _seg(_trim(message, content_w - 12), C_TEXT),
            ])
        if not rows:
            rows.append([_seg("waiting for events", C_MUTED)])
        while len(rows) < limit:
            rows.append([])
        return rows

    frame_lines = []
    header_panel = _panel("POLYMARKET 5M BID SNIPER", header_rows, width, right="v1.1")
    market_panel = _panel(
        "MARKET WINDOW", timer_rows, width,
        right=f"{window_start} {_G['arrow']} {window_end} ET",
    )
    risk_panel = _panel("CAPITAL & RISK", risk_rows, width)
    scanner_panel = _panel(
        "SCANNER", scanner_rows, width,
        right=f"{len(market_states)}/{len(CRYPTO_ASSETS)} markets",
    )
    telemetry_panel = _panel(
        "SESSION TELEMETRY", telemetry_rows, width,
        right="paper proxy" if DRY_RUN else "live",
    )

    budget = max(12, size.lines)
    frame_lines = header_panel + market_panel + risk_panel + scanner_panel
    # Fill the remaining rows with secondary panels and shrink the logs to the
    # space that is actually left, so a shorter terminal never scrolls the
    # dashboard and never loses a whole panel just because its full height
    # does not fit.
    room = budget - 1 - len(frame_lines)
    if len(telemetry_panel) <= room:
        frame_lines += telemetry_panel
        room -= len(telemetry_panel)
    for title, entries, right in (
        ("EXECUTION LOG", ORDER_LOG, "sniper_session.log"),
        ("SYSTEM & DISCOVERY", EVENT_LOG, None),
    ):
        rows_wanted = min(log_limit, room - 2)
        if rows_wanted < 1:
            continue
        panel = _panel(title, log_rows(entries, rows_wanted), width, right=right)
        if len(panel) <= room:
            frame_lines += panel
            room -= len(panel)
    if len(frame_lines) + 1 > budget:
        # Very short terminal: keep the headline risk figures and shed scanner
        # rows from the bottom instead of losing the capital panel entirely.
        risk_headline = _panel(
            "CAPITAL & RISK",
            [risk_row_1, _fit(loss_profit_bits, content_w)],
            width,
        )
        kept = len(scanner_rows)
        trimmed_panel = scanner_panel
        while kept > 3:
            if (
                len(header_panel) + len(market_panel)
                + len(risk_headline) + len(trimmed_panel) + 1 <= budget
            ):
                break
            kept -= 1
            trimmed_panel = _panel(
                "SCANNER", scanner_rows[:kept], width,
                right=f"{len(market_states)}/{len(CRYPTO_ASSETS)} markets",
            )
        frame_lines = header_panel + market_panel + risk_headline + trimmed_panel
    if len(frame_lines) + 1 > budget:
        frame_lines = header_panel + market_panel + scanner_panel

    footer_left = [
        _seg("k", C_TEXT, bold=True), _seg(" / ", C_BORDER), _seg("q", C_TEXT, bold=True),
        _seg("  kill switch", C_MUTED),
        _seg(f" {_G['dot']} ", C_BORDER),
        _seg("ctrl+c", C_TEXT, bold=True), _seg("  safe shutdown", C_MUTED),
    ]
    footer_right = [
        _seg(
            "dry-run state is memory only" if DRY_RUN
            else "cancels live orders before exit",
            C_MUTED,
        )
    ]
    if _width(footer_left) + 2 + _width(footer_right) > width:
        footer_left = [
            _seg("k", C_TEXT, bold=True), _seg("/", C_BORDER), _seg("q", C_TEXT, bold=True),
            _seg("  kill", C_MUTED),
            _seg(f" {_G['dot']} ", C_BORDER),
            _seg("ctrl+c", C_TEXT, bold=True), _seg("  exit", C_MUTED),
        ]
    if len(frame_lines) < budget:
        frame_lines.append(_line(_split_row(footer_left, footer_right, width), width))

    duration_ms = (time.perf_counter() - render_started) * 1000.0
    with _LATENCY_LOCK:
        RENDER_LATENCY_MS.append(duration_ms)

    frame_text = "\n".join(frame_lines)
    if DASHBOARD_RENDERED:
        sys.stdout.write("\033[H" + frame_text + "\033[J")
    else:
        # Clear once on startup; later frames overwrite the same terminal area.
        sys.stdout.write("\033[2J\033[H\033[?25l" + frame_text + "\033[J")
        DASHBOARD_RENDERED = True
    sys.stdout.flush()


def _publish_dashboard_snapshot(market_states, market_books):
    """Publish a local copy so the renderer never reads mutable bot state."""
    global _DASHBOARD_SNAPSHOT
    snapshot = (copy.deepcopy(market_states), copy.deepcopy(market_books))
    with _DASHBOARD_SNAPSHOT_LOCK:
        _DASHBOARD_SNAPSHOT = snapshot


def _dashboard_worker():
    """Render independently from order management; no network calls run here."""
    while not _DASHBOARD_STOP.is_set():
        with _DASHBOARD_SNAPSHOT_LOCK:
            snapshot = _DASHBOARD_SNAPSHOT
        if snapshot is not None:
            states, books = snapshot
            first_state = next(iter(states.values()), None)
            market_ts = (first_state or {}).get('market_ts')
            time_left = get_time_remaining(market_ts) if market_ts else None
            try:
                draw_dashboard(states, books, time_left)
            except Exception as exc:
                log_event(f"Dashboard render failed: {type(exc).__name__}", "yellow")
        _DASHBOARD_STOP.wait(0.2)


def draw_scoreboard(market_states):
    """Print a compact session summary when the bot stops."""
    columns = shutil.get_terminal_size((90, 24)).columns
    width = max(48, min(76, columns - 1))
    content_w = width - 4
    elapsed = max(0, int(time.time() - SESSION_START))
    runtime = f"{elapsed // 3600:02d}:{(elapsed // 60) % 60:02d}:{elapsed % 60:02d}"
    result = (
        "paper fills / settlement unverified" if DRY_RUN else "settlement not tracked"
    )
    rows = [
        _split_row(_kpi("runtime", runtime), [_seg(result, C_MUTED)], content_w),
        _split_row(
            _kpi("fills", SESSION_ENTRIES, C_GOOD if SESSION_ENTRIES else C_TEXT)
            + [_seg("   ")]
            + _kpi("attempts", SESSION_ATTEMPTS)
            + [_seg("   ")]
            + _kpi("skips", SESSION_SKIPS),
            [],
            content_w,
        ),
    ]
    for symbol, market_state in market_states.items():
        if market_state.get('entered'):
            side = str(market_state.get('favored_side') or '')
            rows.append(_split_row(
                [
                    _seg(symbol, C_TEXT, bold=True),
                    _seg("  holding ", C_MUTED),
                    _seg(side, C_GOOD, bold=True),
                    _seg(" "),
                    _seg(f"{float(market_state.get('filled_shares') or 0):.2f}sh", C_TEXT, bold=True),
                    _seg(" @ ", C_MUTED),
                    _seg(f"{float(market_state.get('entry_fill_price') or 0):.3f}", C_TEXT),
                ],
                [],
                content_w,
            ))
        elif market_state.get('last_order_id'):
            rows.append([
                _seg(symbol, C_TEXT, bold=True),
                _seg(
                    "  open order remains, verify on Polymarket" if not DRY_RUN
                    else "  simulated order remains in memory",
                    C_WARN,
                ),
            ])
    print()
    for line in _panel("SESSION SCOREBOARD", rows, width, right="closed"):
        print(line)
    print()


def _cancel_and_confirm(token_id, order_id, wait_sec=0.6, exposure_key=None):
    """Cancel an order and classify whether it filled during the cancellation race."""
    exposure_key = str(exposure_key or order_id)
    if DRY_RUN:
        paper_status = get_order_status_for_id(order_id)
        if paper_status and paper_status.get('has_match'):
            matched_cost = float(paper_status.get('size_matched') or 0.0) * float(paper_status.get('price') or 0.0)
            if matched_cost > 0:
                _set_order_cost(exposure_key, matched_cost)
            return "FILLED", paper_status
        cancel_token_orders_acct(token_id, order_id=order_id)
        _pop_order_cost(exposure_key)
        return "CANCELED", {
            'status': 'CANCELED', 'size_matched': 0.0, 'original_size': 0.0,
            'price': 0.0, 'has_match': False, 'is_filled': False,
        }

    uncertain_order = order_id == 'timeout-assumed-live'
    if uncertain_order:
        # No ID means no safe order-specific cancel. Do not cancel every order
        # for this token or infer a fill from an account-wide position balance.
        return "FAILED", {'status': 'UNKNOWN', 'has_match': False, 'is_filled': False}
    canceled = cancel_token_orders_acct(
        token_id, order_id=order_id
    )
    if wait_sec > 0:
        time.sleep(wait_sec)

    result = get_order_status_for_id(order_id)
    if not result or result == "ERROR":
        return "FAILED", None
    if result.get('has_match'):
        if result.get('status') == 'LIVE' and not result.get('is_filled'):
            return "FAILED", result
        matched_cost = float(result.get('size_matched') or 0) * float(result.get('price') or 0)
        if matched_cost > 0:
            _set_order_cost(exposure_key, matched_cost)
        return "FILLED", result
    if result.get('status') in ('CANCELED', 'CANCELLED'):
        _pop_order_cost(exposure_key)
        return "CANCELED", result
    return "FAILED", result


def _clear_order_state(market_state):
    market_state['last_order_id'] = None
    market_state['last_order_price'] = None
    market_state['open_order_price'] = None
    market_state['exposure_key'] = None


def _record_fill(symbol, market_state, fill_status, preserve_open_order=False):
    """Record matched quantity and retain its capital reservation across windows."""
    global SESSION_ENTRIES, SESSION_FILL_PRICES, SESSION_LATE_FILLS
    if not fill_status:
        return
    matched = float(fill_status.get('size_matched') or 0.0)
    if matched <= 0 and fill_status.get('is_filled'):
        matched = float(fill_status.get('original_size') or 0.0)
    price = float(fill_status.get('price') or market_state.get('last_order_price') or 0.0)
    exposure_key = str(market_state.get('exposure_key') or market_state.get('last_order_id') or '')
    if matched > 0 and price > 0 and exposure_key and not preserve_open_order:
        token_id = market_state.get('favored_token_id')
        position_key = f"position:{token_id}" if token_id else exposure_key
        if position_key != exposure_key:
            _pop_order_cost(exposure_key)
        _set_order_cost(position_key, matched * price)
        SESSION_FILL_PRICES.append(price)
    if matched > market_state.get('filled_shares', 0.0):
        market_state['filled_shares'] = matched
        market_state['filled_cost'] = matched * price
    if not market_state.get('entered'):
        SESSION_ENTRIES += 1
        market_state['entered_at'] = time.time()
        if get_time_remaining(market_state.get('market_ts', 0)) <= STOP_THRESHOLD_SEC:
            market_state['late_fill'] = True
            SESSION_LATE_FILLS += 1
            log_event(
                f"CRITICAL: {symbol} late fill detected inside final {STOP_THRESHOLD_SEC}s; "
                "position remains exposed to expiry",
                "red",
            )
    market_state['entered'] = True
    market_state['entry_fill_price'] = price
    log_event(f"{symbol} matched {matched:.2f} shares @ ${price:.3f}; holding to expiry", "green")


def _poll_books_batch(market_states):
    """Poll all known UP/DOWN tokens together and map books back to markets."""
    token_refs = {}
    for symbol, state in market_states.items():
        info = state['market_info']
        for side, key in (("UP", "up_token_id"), ("DOWN", "down_token_id")):
            token_refs[str(info[key])] = (symbol, side)
    if not token_refs:
        return {}
    started = time.perf_counter()
    try:
        books_by_token = get_order_books_batch(token_refs)
        books_by_market = {symbol: {"UP": None, "DOWN": None} for symbol in market_states}
        for token_id, (symbol, side) in token_refs.items():
            books_by_market[symbol][side] = books_by_token.get(token_id)
        return {symbol: (books["UP"], books["DOWN"]) for symbol, books in books_by_market.items()}
    except Exception as exc:
        log_event(f"Batch order-book request failed: {type(exc).__name__}", "yellow")
        return {symbol: (None, None) for symbol in market_states}
    finally:
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        with _PING_LOCK:
            CLOB_PING_MS.append(elapsed_ms)


def reconcile_open_orders(market_states):
    """Fail closed if the account has any pre-existing open order."""
    if DRY_RUN:
        return True
    client = _build_client()
    open_order_count = 0
    for page in client.list_open_orders():
        if page.limit_reached:
            raise RuntimeError(
                "Startup blocked: open-order pagination limit reached; account state is incomplete"
            )
        open_order_count += len(page.items)
        if page.has_more and not page.next_cursor:
            raise RuntimeError("Startup blocked: open-order pagination cursor is missing")
    if open_order_count:
        raise RuntimeError(
            f"Startup blocked: account has {open_order_count} pre-existing open order(s). "
            "Inspect/cancel them manually, then restart. No orders were canceled."
        )
    return True


def main():
    global SESSION_SKIPS, SESSION_ENTRIES, SESSION_WINDOWS, SESSION_SIGNALS
    global SESSION_REPRICES, SESSION_CUTOFF_CANCELS
    global SESSION_PAPER_WINS, SESSION_PAPER_LOSSES, SESSION_PAPER_PNL
    global SESSION_ENTRY_HALT_REASON
    _validate_configuration()
    market_states = {}
    market_books = {}
    executor = ThreadPoolExecutor(max_workers=BOOK_WORKERS, thread_name_prefix="market-data")
    _DASHBOARD_STOP.clear()
    dashboard_thread = threading.Thread(
        target=_dashboard_worker,
        name="dashboard-renderer",
        daemon=True,
    )
    dashboard_thread.start()
    current_market_ts = None
    order_account_reconciled = False
    next_discovery_at = 0.0
    next_book_poll_at = 0.0
    last_book_poll_interval = 3.0
    book_future = None
    book_future_market_ts = None
    book_future_started_at = 0.0
    market_books_updated_at = 0.0
    log_event(
        "Multi-crypto bot started in DRY RUN; no account orders will be sent" if DRY_RUN
        else "Multi-crypto bot started in LIVE ORDER mode",
        "yellow" if DRY_RUN else "green",
    )

    def settle_cancel(symbol, market_state, outcome, status):
        order_id = market_state.get('last_order_id')
        if outcome == "FILLED":
            _record_fill(symbol, market_state, status)
            _clear_order_state(market_state)
            return
        if outcome == "CANCELED":
            _pop_order_cost(market_state.get('exposure_key') or order_id)
            _clear_order_state(market_state)
            log_event(f"{symbol} resting order canceled", "yellow")
            return
        if status and status.get('has_match'):
            # A partial fill exists, but the remainder is not confirmed canceled.
            # Count the position once and keep the full reservation until canceled.
            _record_fill(symbol, market_state, status, preserve_open_order=True)
        log_event(f"{symbol} order cancellation not confirmed; retaining exposure reservation", "red")

    try:
        while True:
            # ---- Kill switch: file trigger (create `KILL` file) or press 'k'/'q' ----
            kill_switch_path = (
                None if not KILL_SWITCH_FILE else
                KILL_SWITCH_FILE if os.path.isabs(KILL_SWITCH_FILE)
                else os.path.join(PROJECT_ROOT, KILL_SWITCH_FILE)
            )
            loss_limit_hit = DRY_RUN and SESSION_PAPER_PNL <= -MAX_SESSION_LOSS_USD
            kill_triggered = (
                (kill_switch_path is not None and os.path.exists(kill_switch_path))
                or loss_limit_hit
                or os.getenv("KILL_SWITCH", "").strip().lower() in {"1", "true", "yes", "on"}
            )
            if not kill_triggered and msvcrt is not None:
                try:
                    while msvcrt.kbhit():
                        ch = msvcrt.getch()
                        if ch in (b"k", b"K", b"q", b"Q"):
                            kill_triggered = True
                except Exception:
                    pass
            if kill_triggered:
                reason = "paper session loss limit reached" if loss_limit_hit else "kill switch triggered"
                log_event(f"{reason} - canceling all open orders and shutting down", "red")
                cancel_futures = {
                    executor.submit(
                        _cancel_and_confirm,
                        ms["favored_token_id"],
                        ms["last_order_id"],
                        0.2,
                        ms.get("exposure_key"),
                    ): (sym, ms)
                    for sym, ms in market_states.items()
                    if ms.get("last_order_id") and ms.get("favored_token_id")
                }
                for future in as_completed(cancel_futures):
                    sym, ms = cancel_futures[future]
                    try:
                        outcome, status = future.result()
                    except Exception:
                        outcome, status = "FAILED", None
                    settle_cancel(sym, ms, outcome, status)
                try:
                    if os.path.exists(kill_switch_path):
                        os.remove(kill_switch_path)
                except Exception:
                    pass
                break
            now = time.time()
            market_ts = get_current_market_timestamp()

            if market_ts != current_market_ts:
                SESSION_WINDOWS += 1
                previous_states = market_states
                cancel_futures = {
                    executor.submit(
                        _cancel_and_confirm,
                        old_state['favored_token_id'],
                        old_state['last_order_id'],
                        0.2,
                        old_state.get('exposure_key'),
                    ): (symbol, old_state)
                    for symbol, old_state in previous_states.items()
                    if old_state.get('last_order_id') and old_state.get('favored_token_id')
                }
                unresolved = []
                for future in as_completed(cancel_futures):
                    symbol, old_state = cancel_futures[future]
                    try:
                        outcome, status = future.result()
                    except Exception as exc:
                        outcome, status = "FAILED", None
                        log_event(f"{symbol} rollover cancel failed: {type(exc).__name__}", "red")
                    settle_cancel(symbol, old_state, outcome, status)
                    if outcome == "FAILED":
                        unresolved.append((symbol, old_state))

                # Keep filled positions and unconfirmed orders in the global
                # exposure ledger across windows. Only an order-specific,
                # terminal CANCELED response releases a reservation.
                if unresolved:
                    log_event(
                        f"{len(unresolved)} rollover order(s) unresolved; reservations retained",
                        "red",
                    )
                _resolve_paper_window(previous_states, market_books)
                for paper_id, paper_order in list(PAPER_ORDERS.items()):
                    if paper_order.get('status') != 'LIVE':
                        PAPER_ORDERS.pop(paper_id, None)

                current_market_ts = market_ts
                order_account_reconciled = False
                market_states = {}
                market_books = {}
                book_future_started_at = 0.0
                market_books_updated_at = 0.0
                next_book_poll_at = 0.0
                last_book_poll_interval = 3.0
                log_event(f"New multi-crypto market window {market_ts}", "cyan")
                futures = {
                    executor.submit(get_market_info, market_ts, symbol): symbol
                    for symbol in CRYPTO_ASSETS
                }
                for future in as_completed(futures):
                    symbol = futures[future]
                    try:
                        info = future.result()
                    except Exception as exc:
                        info = None
                        log_event(f"{symbol} market discovery failed: {type(exc).__name__}", "yellow")
                    if info:
                        market_states[symbol] = new_market_state(market_ts, info)
                        market_states[symbol]['neg_risk'] = info.get('neg_risk', False)
                        market_books[symbol] = (None, None)
                        log_event(f"{symbol} market tokens loaded", "green")
                next_discovery_at = time.time() + DISCOVERY_RETRY_SEC
                if not market_states:
                    log_event("No target crypto markets listed yet; will retry", "yellow")
                if market_states and not DRY_RUN:
                    if not order_account_reconciled:
                        reconcile_open_orders(market_states)
                        order_account_reconciled = True
                    _reconcile_startup_positions(market_states)

            missing = [symbol for symbol in CRYPTO_ASSETS if symbol not in market_states]
            # Market discovery can block on network timeouts. Stop retrying
            # before the cutoff so it cannot delay cancellation of live bids.
            if missing and now >= next_discovery_at and get_time_remaining(market_ts) > STOP_THRESHOLD_SEC:
                futures = {
                    executor.submit(get_market_info, market_ts, symbol): symbol
                    for symbol in missing
                }
                newly_discovered = {}
                for future in as_completed(futures):
                    symbol = futures[future]
                    try:
                        info = future.result()
                    except Exception as exc:
                        info = None
                        log_event(f"{symbol} market discovery failed: {type(exc).__name__}", "yellow")
                    if info:
                        market_states[symbol] = new_market_state(market_ts, info)
                        market_states[symbol]['neg_risk'] = info.get('neg_risk', False)
                        market_books[symbol] = (None, None)
                        newly_discovered[symbol] = market_states[symbol]
                        log_event(f"{symbol} market tokens loaded", "green")
                # Markets discovered after startup need the same live-position
                # check as markets found in the initial lookup. Otherwise a
                # delayed listing could be entered despite an existing holding.
                if newly_discovered and not DRY_RUN:
                    if not order_account_reconciled:
                        reconcile_open_orders(newly_discovered)
                        order_account_reconciled = True
                    _reconcile_startup_positions(newly_discovered)
                next_discovery_at = time.time() + DISCOVERY_RETRY_SEC

            time_left = get_time_remaining(market_ts)
            if time_left <= STOP_THRESHOLD_SEC:
                # Do not wait on book requests before canceling at cutoff. All
                # assets cancel concurrently; each order gets its own status check.
                cutoff_futures = {
                    executor.submit(
                        _cancel_and_confirm,
                        market_state['favored_token_id'],
                        market_state['last_order_id'],
                        0.15,
                        market_state.get('exposure_key'),
                    ): (symbol, market_state)
                    for symbol, market_state in market_states.items()
                    if market_state.get('last_order_id') and market_state.get('favored_token_id')
                }
                for future in as_completed(cutoff_futures):
                    symbol, market_state = cutoff_futures[future]
                    try:
                        outcome, status = future.result()
                    except Exception as exc:
                        outcome, status = "FAILED", None
                        log_event(f"{symbol} cutoff cancel failed: {type(exc).__name__}", "red")
                    if outcome == "CANCELED":
                        SESSION_CUTOFF_CANCELS += 1
                    settle_cancel(symbol, market_state, outcome, status)
                _publish_dashboard_snapshot(market_states, market_books)
                time.sleep(1.0)
                continue

            # Keep the terminal refreshing every second while pacing network
            # polls at 3s outside the entry window and 1s near/inside it.
            time_left = get_time_remaining(market_ts)
            book_poll_interval = 1.0 if time_left <= ENTRY_WINDOW_SEC + 10 else 3.0
            if book_poll_interval < last_book_poll_interval:
                next_book_poll_at = 0.0
            last_book_poll_interval = book_poll_interval
            if book_future is not None and book_future.done():
                try:
                    fresh_books = book_future.result()
                    poll_age = time.monotonic() - book_future_started_at
                    if book_future_market_ts == market_ts and poll_age <= BOOK_STALE_SEC:
                        market_books = fresh_books
                        market_books_updated_at = time.monotonic()
                        for _symbol, _books in market_books.items():
                            if _symbol in market_states:
                                market_states[_symbol]['_last_up_book'] = _books[0] if isinstance(_books, tuple) else None
                                market_states[_symbol]['_last_down_book'] = _books[1] if isinstance(_books, tuple) else None
                        _update_ask_momentum(market_states, market_books)
                    elif book_future_market_ts == market_ts:
                        market_books = {symbol: (None, None) for symbol in market_states}
                        market_books_updated_at = 0.0
                        log_event(f"Discarded stale book batch ({poll_age:.1f}s old)", "yellow")
                except Exception as exc:
                    log_event(f"Batch order-book poll failed: {type(exc).__name__}", "yellow")
                book_future = None
                book_future_market_ts = None
                book_future_started_at = 0.0
            if market_books and (
                market_books_updated_at <= 0.0
                or time.monotonic() - market_books_updated_at > BOOK_STALE_SEC
            ):
                market_books = {symbol: (None, None) for symbol in market_states}
                for state in market_states.values():
                    state['_last_up_book'] = None
                    state['_last_down_book'] = None
            if time.monotonic() >= next_book_poll_at and book_future is None:
                # Keep network waits off the order-management loop so slow book
                # requests cannot delay cutoff or emergency cancellation.
                state_snapshot = {
                    symbol: {'market_info': state['market_info']}
                    for symbol, state in market_states.items()
                }
                book_future = executor.submit(_poll_books_batch, state_snapshot)
                book_future_market_ts = market_ts
                book_future_started_at = time.monotonic()
                next_book_poll_at = time.monotonic() + book_poll_interval

            _simulate_paper_fills(market_states, market_books)
            _update_entry_halt(market_states, market_books)

            now = time.time()
            time_left = get_time_remaining(market_ts)
            in_window = ENTRY_WINDOW_SEC >= time_left > STOP_THRESHOLD_SEC
            new_entry_candidates = []
            asset_priority = {symbol: index for index, symbol in enumerate(CRYPTO_ASSETS)}
            for symbol in CRYPTO_ASSETS:
                market_state = market_states.get(symbol)
                if market_state is None:
                    continue
                if not in_window:
                    continue
                if market_state.get('position_check_unknown') and not market_state.get('entered'):
                    continue
                up_book, down_book = market_books.get(symbol, (None, None))
                side = market_state.get('favored_side')
                favored_book = up_book if side == "UP" else down_book if side == "DOWN" else None
                opposite_book = down_book if side == "UP" else up_book if side == "DOWN" else None
                target_now = compute_bid_target(favored_book, time_left, opposite_book) if favored_book else None
                urgent_stand_down = bool(market_state.get('last_order_id')) and favored_book is not None and target_now is None
                if now - market_state['last_order_update'] < ORDER_UPDATE_INTERVAL and not urgent_stand_down:
                    continue
                market_state['last_order_update'] = now

                # Uncertain submissions have no queryable order ID. Never
                # duplicate them; discover/cancel them through position polling.
                if market_state.get('last_order_id') == 'timeout-assumed-live':
                    # Unknown submission has no safe order-specific cancel ID.
                    # Keep its reserved notional and halt the session until the
                    # exchange-side GTD expires; never infer ownership from positions.
                    continue

                order_id = market_state.get('last_order_id')
                if order_id:
                    status = get_order_status_for_id(order_id)
                    if status == "ERROR" or status is None:
                        SESSION_ENTRY_HALT_REASON = (
                            f"{symbol} order status is unavailable; retaining exposure and halting entries"
                        )
                        log_event(f"CRITICAL: {SESSION_ENTRY_HALT_REASON}", "red")
                        continue
                    has_match = status.get('has_match', False) or status.get('is_filled', False)
                    if has_match:
                        if status.get('status') == 'LIVE' and not status.get('is_filled'):
                            outcome, final_status = _cancel_and_confirm(
                                market_state['favored_token_id'], order_id, 0.15,
                                market_state.get('exposure_key'),
                            )
                            if outcome == "FILLED":
                                _record_fill(symbol, market_state, final_status)
                                _clear_order_state(market_state)
                            elif outcome == "CANCELED":
                                _record_fill(symbol, market_state, status)
                                _clear_order_state(market_state)
                            else:
                                _record_fill(
                                    symbol, market_state, final_status or status,
                                    preserve_open_order=True,
                                )
                            continue
                        _record_fill(symbol, market_state, status)
                        _clear_order_state(market_state)
                        continue

                    if status.get('status') in ('CANCELED', 'CANCELLED'):
                        _pop_order_cost(market_state.get('exposure_key') or order_id)
                        _clear_order_state(market_state)
                        market_state['favored_side'] = None
                        market_state['favored_token_id'] = None
                        order_id = None
                    elif status.get('status') == 'LIVE':
                        # Check fills even if the opposite outcome has no book.
                        # Only use a fresh selected-side book for repricing/cancel.
                        if market_state.get('entered'):
                            outcome, final_status = _cancel_and_confirm(
                                market_state['favored_token_id'], order_id, 0.15,
                                market_state.get('exposure_key'),
                            )
                            if outcome == "FILLED":
                                _record_fill(symbol, market_state, final_status)
                                _clear_order_state(market_state)
                            continue
                        if SESSION_ENTRY_HALT_REASON:
                            outcome, final_status = _cancel_and_confirm(
                                market_state['favored_token_id'], order_id, 0.15,
                                market_state.get('exposure_key'),
                            )
                            if outcome == "FILLED":
                                _record_fill(symbol, market_state, final_status)
                                _clear_order_state(market_state)
                            elif outcome == "CANCELED":
                                _clear_order_state(market_state)
                            continue
                        favored_book = up_book if market_state.get('favored_side') == "UP" else down_book
                        if favored_book is None:
                            continue
                        opposite_book = down_book if market_state.get('favored_side') == "UP" else up_book
                        target_price = compute_bid_target(favored_book, time_left, opposite_book)
                        old_price = status.get('price') or market_state['last_order_price']
                        if target_price is None:
                            outcome, final_status = _cancel_and_confirm(
                                market_state['favored_token_id'], order_id, 0.15,
                                market_state.get('exposure_key'),
                            )
                            if outcome == "FILLED":
                                _record_fill(symbol, market_state, final_status)
                                _clear_order_state(market_state)
                            elif outcome == "CANCELED":
                                _clear_order_state(market_state)
                                market_state['favored_side'] = None
                                market_state['favored_token_id'] = None
                            continue
                        if market_state.get('sharp_ask_drop', {}).get(market_state.get('favored_side')):
                            continue
                        if old_price and abs(old_price - target_price) > PRICE_HOLD_TOLERANCE:
                            outcome, final_status = _cancel_and_confirm(
                                market_state['favored_token_id'], order_id, 0.15,
                                market_state.get('exposure_key'),
                            )
                            if outcome == "FILLED":
                                _record_fill(symbol, market_state, final_status)
                                _clear_order_state(market_state)
                            elif outcome == "CANCELED":
                                _clear_order_state(market_state)
                                SESSION_REPRICES += 1
                                _place_fresh_bid(market_state, market_state['favored_token_id'], target_price)
                            continue
                        continue
                    else:
                        SESSION_ENTRY_HALT_REASON = (
                            f"{symbol} order status {status.get('status')!r} is not terminal or LIVE; "
                            "retaining exposure and halting entries"
                        )
                        log_event(f"CRITICAL: {SESSION_ENTRY_HALT_REASON}", "red")
                        continue

                if market_state.get('entered'):
                    continue
                if market_state.get('favored_side') is None:
                    if up_book is None and down_book is None:
                        continue
                    side, _ = pick_decided_side(up_book, down_book)
                    if side is None:
                        SESSION_SKIPS += 1
                        continue
                    if not market_state.get('signal_counted'):
                        SESSION_SIGNALS += 1
                        market_state['signal_counted'] = True
                    if market_state.get('favored_side') != side:
                        market_state['favored_side'] = side
                        market_state['favored_since'] = time.time()
                    token_key = 'up_token_id' if side == "UP" else 'down_token_id'
                    market_state['favored_token_id'] = market_state['market_info'][token_key]
                    log_event(f"{symbol} {side} crossed the decided threshold", "green")

                favored_book = up_book if market_state['favored_side'] == "UP" else down_book
                opposite_book = down_book if market_state.get('favored_side') == "UP" else up_book
                target_price = compute_bid_target(favored_book, time_left, opposite_book)
                if target_price is not None:
                    if time_left <= SOFT_ENTRY_CUTOFF_SEC:
                        continue
                    if market_state.get('sharp_ask_drop', {}).get(market_state.get('favored_side')):
                        if now - market_state.get('last_velocity_log', 0.0) >= 5.0:
                            log_event(
                                f"{symbol} entry held: ask dropped by >=2c on the latest book update",
                                "yellow",
                            )
                            market_state['last_velocity_log'] = now
                        continue
                    best_bid = favored_book.get('best_bid') if favored_book else None
                    ask = favored_book.get('best_ask') if favored_book else None
                    queue_shares = favored_book.get('bid_queue_shares', 0.0) if favored_book else 0.0
                    # Resolve simultaneous signals consistently: strongest ask
                    # first, then orders already at the top bid, then the
                    # shortest queue, with CRYPTO_ASSETS as the stable tie-break.
                    rank = (
                        float(ask) if ask is not None else 0.0,
                        int(best_bid is not None and target_price >= float(best_bid) - 0.005),
                        -float(queue_shares or 0.0),
                        -asset_priority.get(symbol, len(asset_priority)),
                    )
                    new_entry_candidates.append(
                        (rank, symbol, market_state, market_state['favored_token_id'], target_price)
                    )
                elif not market_state.get('last_order_id') and not market_state.get('entered'):
                    market_state['favored_side'] = None
                    market_state['favored_token_id'] = None

            for _rank, _symbol, _state, _token_id, _target_price in sorted(
                new_entry_candidates, key=lambda candidate: candidate[0], reverse=True
            ):
                if SESSION_ENTRY_HALT_REASON:
                    break
                _place_fresh_bid(_state, _token_id, _target_price)

            _publish_dashboard_snapshot(market_states, market_books)
            time.sleep(1.0)

    except KeyboardInterrupt:
        pending = {
            executor.submit(
                _cancel_and_confirm,
                market_state['favored_token_id'],
                market_state['last_order_id'],
                0.15,
                market_state.get('exposure_key'),
            ): (symbol, market_state)
            for symbol, market_state in market_states.items()
            if market_state.get('last_order_id') and market_state.get('favored_token_id')
        }
        for future in as_completed(pending):
            symbol, market_state = pending[future]
            try:
                outcome, status = future.result()
            except Exception:
                outcome, status = "FAILED", None
            settle_cancel(symbol, market_state, outcome, status)
        _DASHBOARD_STOP.set()
        dashboard_thread.join(timeout=1.0)
        print(colored("\nStopped by user.", "yellow"))
        draw_scoreboard(market_states)
    except Exception as exc:
        log_event(f"Fatal main-loop error: {type(exc).__name__}: {exc}; canceling open orders", "red")
        pending = {
            executor.submit(
                _cancel_and_confirm,
                state['favored_token_id'],
                state['last_order_id'],
                0.15,
                state.get('exposure_key'),
            ): (symbol, state)
            for symbol, state in market_states.items()
            if state.get('last_order_id') and state.get('favored_token_id')
        }
        for future in as_completed(pending):
            symbol, state = pending[future]
            try:
                outcome, status = future.result()
            except Exception as cancel_exc:
                outcome, status = "FAILED", None
                log_event(f"{symbol} fatal-error cancel failed: {type(cancel_exc).__name__}", "red")
            settle_cancel(symbol, state, outcome, status)
        _DASHBOARD_STOP.set()
        dashboard_thread.join(timeout=1.0)
        draw_scoreboard(market_states)
        raise
    finally:
        _DASHBOARD_STOP.set()
        dashboard_thread.join(timeout=1.0)
        executor.shutdown(wait=False, cancel_futures=True)
        _STRIKE_EXECUTOR.shutdown(wait=False, cancel_futures=True)
        if DASHBOARD_RENDERED:
            sys.stdout.write("\033[?25h\n")
            sys.stdout.flush()


if __name__ == "__main__":
    main()
