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
  ~$4.75, so BET_SIZE_USD=1.0 will be bumped up to that floor — can't go lower.
MULTI-ACCOUNT:
  Set to the AUG14 wallet (signature_type=2 / Gnosis Safe). Change ACCOUNT_SUFFIX
  to run elsewhere.
"""

import sys
import os
import time
import random
import itertools
import json
import threading
import shutil
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
OPEN_ORDER_COSTS = {}

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

SIGNATURE_TYPE = 1 if ACCOUNT_SUFFIX == "" else 2

# ============================================================================
# STRATEGY CONFIGURATION
# ============================================================================
BET_SIZE_USD = float(os.getenv("BET_SIZE_USD", "1.0"))
ENTRY_THRESHOLD = float(os.getenv("ENTRY_THRESHOLD", "0.95"))
MAX_BUY_PRICE = float(os.getenv("MAX_BUY_PRICE", "0.95"))
MAX_TOTAL_EXPOSURE_USD = float(os.getenv("MAX_TOTAL_EXPOSURE_USD", "10.0"))
ENTRY_WINDOW_SEC = int(os.getenv("ENTRY_WINDOW_SEC", "60"))
STOP_THRESHOLD_SEC = int(os.getenv("STOP_THRESHOLD_SEC", "5"))
ORDER_UPDATE_INTERVAL = int(os.getenv("ORDER_UPDATE_INTERVAL", "1"))
PRICE_HOLD_TOLERANCE = float(os.getenv("PRICE_HOLD_TOLERANCE", "0.005"))
KILL_SWITCH_FILE = os.getenv("KILL_SWITCH_FILE", "KILL")

MARKET_DURATION = 300
ET = timezone(timedelta(hours=-5))
CRYPTO_ASSETS = {"BTC": "btc", "ETH": "eth", "SOL": "sol", "XRP": "xrp", "DOGE": "doge"}
BOOK_WORKERS = len(CRYPTO_ASSETS) * 2
DISCOVERY_RETRY_SEC = 15

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

SPINNER_FRAMES = itertools.cycle(["o", "O", "0", "."])
_DASHBOARD_FRAME = itertools.count()
ROCKET_FRAMES = itertools.cycle(["^   ", " ^  ", "  ^ ", "   ^", "  ^ ", " ^  "])

EVENT_LOG = deque(maxlen=6)
ORDER_LOG = deque(maxlen=5)
_SESSION_LOG_LOCK = threading.Lock()
_THREAD_LOCAL = threading.local()
_PING_LOCK = threading.Lock()
CLOB_PING_MS = deque(maxlen=30)


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

        from py_clob_client_v2.client import ClobClient
        from py_clob_client_v2.clob_types import ApiCreds
        from py_clob_client_v2.constants import POLYGON
        from web3 import Web3

        key = os.getenv(PRIVATE_KEY_ENV_NAME)
        browser_address = os.getenv(PUBLIC_KEY_ENV_NAME)

        if not key or not browser_address:
            print(colored(
                f"Missing {PRIVATE_KEY_ENV_NAME} or {PUBLIC_KEY_ENV_NAME} in .env!",
                "red",
            ))
            sys.exit(1)

        try:
            browser_wallet = Web3.toChecksumAddress(browser_address)
        except AttributeError:
            browser_wallet = Web3.to_checksum_address(browser_address)

        client = ClobClient(
            host="https://clob.polymarket.com",
            chain_id=POLYGON,
            key=key,
            signature_type=SIGNATURE_TYPE,
            funder=browser_wallet,
        )

        api_key = os.getenv(API_KEY_ENV_NAME)
        api_secret = (
            os.getenv(API_SECRET_ENV_NAME)
            or os.getenv(f"SECRET{ACCOUNT_SUFFIX}")
            or os.getenv("SECRET")
        )
        passphrase = os.getenv(PASSPHRASE_ENV_NAME)

        if api_key and api_secret and passphrase:
            creds = ApiCreds(api_key=api_key, api_secret=api_secret, api_passphrase=passphrase)
            client.set_api_creds(creds=creds)
        else:
            creds = client.create_or_derive_api_creds()
            client.set_api_creds(creds=creds)

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
        return {'status': order['status'], 'size_matched': 0.0,
                'original_size': order['size'], 'price': order['price'],
                'has_match': False, 'is_filled': False}
    client = _build_client()
    for attempt in range(3):
        try:
            order = client.get_order(order_id)
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


def place_limit_order(token_id, side, price, size, neg_risk=False):
    if DRY_RUN:
        order_id = f"paper-{next(PAPER_ORDER_SEQUENCE)}"
        PAPER_ORDERS[order_id] = {
            'token_id': str(token_id), 'side': side.upper(), 'price': float(price),
            'size': float(size), 'status': 'LIVE', 'created_at': time.time(),
        }
        log_event(f"DRY RUN: simulated {side} @ ${price:.4f} x {size} (memory only)", "yellow")
        return {'orderID': order_id, 'status': 'LIVE', 'simulated': True}

    from py_clob_client_v2.clob_types import OrderArgs, OrderType, PartialCreateOrderOptions

    client = _build_client()

    order_args = OrderArgs(
        token_id=str(token_id),
        price=float(price),
        size=float(size),
        side=side.upper(),
    )

    log_event(f"Resting {side} @ ${price:.4f} x {size} shares (post-only)", "cyan")

    options = PartialCreateOrderOptions(neg_risk=True) if neg_risk else None

    try:
        response = client.create_and_post_order(
            order_args,
            options=options,
            order_type=OrderType.GTC,
            post_only=True,
        )
        if response and (response.get('orderID') or response.get('success')):
            oid = response.get('orderID', '')
            log_event(f"Order LIVE — id {str(oid)[:10]}...", "green")
            return response
        log_event(f"Order rejected: {response}", "red")
        return response if response else {}
    except Exception as e:
        err = str(e)
        if 'Request exception' in err or 'Duplicated' in err or 'ReadTimeout' in err or 'timeout' in err.lower():
            log_event("post_order timed out — assuming it landed", "yellow")
            return {'orderID': 'timeout-assumed-live', 'status': 'LIVE'}
        log_event(f"post_order failed: {type(e).__name__}: {e}", "red")
        return {}


def cancel_token_orders_acct(token_id, order_id=None):
    if DRY_RUN:
        for oid, order in list(PAPER_ORDERS.items()):
            if (order['token_id'] == str(token_id) and order['status'] == 'LIVE'
                    and (order_id is None or oid == str(order_id))):
                order['status'] = 'CANCELED'
                OPEN_ORDER_COSTS.pop(oid, None)
        log_event("DRY RUN: simulated cancellation (memory only)", "yellow")
        return True

    from py_clob_client_v2.clob_types import OrderMarketCancelParams, OrderPayload
    client = _build_client()
    try:
        if order_id and order_id != 'timeout-assumed-live':
            client.cancel_order(OrderPayload(orderID=str(order_id)))
        else:
            client.cancel_market_orders(OrderMarketCancelParams(asset_id=str(token_id)))
        log_event(f"Cancelled orders on {str(token_id)[:10]}...", "yellow")
        return True
    except Exception as e:
        log_event(f"Cancel failed: {type(e).__name__}: {e}", "yellow")
        return False


# ============================================================================
# MARKET DATA HELPERS
# ============================================================================
def get_current_market_timestamp():
    now = int(time.time())
    return (now // MARKET_DURATION) * MARKET_DURATION


def get_time_remaining(market_ts):
    return MARKET_DURATION - (int(time.time()) - market_ts)


def get_order_book(token_id):
    response = _http_session().get(
        "https://clob.polymarket.com/book",
        params={'token_id': token_id},
        timeout=10,
    )
    if response.status_code != 200:
        return None
    data = response.json()
    bids = data.get('bids', [])
    asks = data.get('asks', [])
    if not bids and not asks:
        return None
    return {
        'best_bid': max((float(level['price']) for level in bids), default=None),
        'best_ask': min((float(level['price']) for level in asks), default=None),
    }


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


def position_shares_for_token(token_id):
    if DRY_RUN:
        return 0.0
    user_address = os.getenv(PUBLIC_KEY_ENV_NAME)
    if not user_address:
        return None
    try:
        resp = _http_session().get(
            "https://data-api.polymarket.com/positions",
            params={'user': user_address, 'limit': 500, 'sortBy': 'CURRENT', 'sortDirection': 'DESC'},
            timeout=8,
        )
        if resp.status_code != 200:
            return None
        for pos in resp.json():
            if str(pos.get('asset')) == str(token_id):
                size = float(pos.get('size', 0) or 0)
                if size > 0:
                    return size
        return 0.0
    except Exception as e:
        log_event(f"positions fetch failed: {e}", "yellow")
        return None


# ============================================================================
# CORE 95c BID SNIPER LOGIC
# ============================================================================
def pick_decided_side(up_book, down_book):
    up_ask = up_book['best_ask'] if up_book else None
    down_ask = down_book['best_ask'] if down_book else None

    up_qualifies = up_ask is not None and up_ask >= ENTRY_THRESHOLD
    down_qualifies = down_ask is not None and down_ask >= ENTRY_THRESHOLD

    if up_qualifies and down_qualifies:
        return ("UP", up_book) if up_ask >= down_ask else ("DOWN", down_book)
    if up_qualifies:
        return "UP", up_book
    if down_qualifies:
        return "DOWN", down_book
    return None, None


def compute_bid_target(favored_book):
    if not favored_book:
        return None
    best_ask = favored_book.get('best_ask')
    best_bid = favored_book.get('best_bid')
    if best_bid is None:
        return None
    if best_ask is None:
        if best_bid < ENTRY_THRESHOLD:
            return None
        best_ask = 1.00
    elif best_ask < ENTRY_THRESHOLD:
        return None
    # Keep the post-only BUY strictly below the current best ask.
    max_maker_bid = round(best_ask - 0.01, 3)
    target = min(best_bid, max_maker_bid, MAX_BUY_PRICE)
    return round(target, 3) if target >= 0.01 else None


def _current_exposure_usd():
    return sum(OPEN_ORDER_COSTS.values())


def _place_fresh_bid(state, token_id, target_price):
    global SESSION_ATTEMPTS, SESSION_TIMEOUTS
    shares = calculate_shares(BET_SIZE_USD, target_price)
    if shares <= 0:
        return
    order_cost = target_price * shares
    current = _current_exposure_usd()
    if current + order_cost > MAX_TOTAL_EXPOSURE_USD:
        log_event(
            f"Exposure cap reached (${current:.2f}+${order_cost:.2f} > ${MAX_TOTAL_EXPOSURE_USD:.2f}); skipping bid",
            "yellow",
        )
        return
    SESSION_ATTEMPTS += 1
    resp = place_limit_order(token_id, "BUY", target_price, shares, neg_risk=state['neg_risk'])
    if resp:
        oid = resp.get('orderID')
        if oid and oid != 'timeout-assumed-live':
            state['last_order_id'] = oid
            state['last_order_price'] = target_price
            state['open_order_price'] = target_price
            state['exposure_key'] = str(oid)
            OPEN_ORDER_COSTS[str(oid)] = order_cost
        elif oid == 'timeout-assumed-live':
            SESSION_TIMEOUTS += 1
            state['last_order_id'] = 'timeout-assumed-live'
            state['last_order_price'] = target_price
            state['open_order_price'] = target_price
            exposure_key = f"timeout:{state['market_info']['slug']}"
            state['exposure_key'] = exposure_key
            OPEN_ORDER_COSTS[exposure_key] = order_cost


def new_market_state(market_ts, market_info):
    """Create independent order/entry state for one asset's market window."""
    return {
        'market_ts': market_ts,
        'market_info': market_info,
        'neg_risk': False,
        'favored_side': None,
        'favored_token_id': None,
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


def _short_address(address):
    if not address:
        return "missing"
    return f"{address[:6]}...{address[-4:]}"


def _validate_configuration():
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


def _draw_panel(title, rows, color="cyan"):
    width = max(60, min(120, shutil.get_terminal_size((120, 24)).columns))
    frame = next(_DASHBOARD_FRAME)
    print(colored("+" + "-" * (width - 2) + "+", color))
    print(colored("| " + title[:width - 4].ljust(width - 4) + " |", color, attrs=["bold"]))
    print(colored("+" + "-" * (width - 2) + "+", color))
    for row in rows:
        # Keep the display inside the frame and avoid terminal wrapping.
        clipped = row[:width - 4].ljust(width - 4)
        lower = row.lower()
        row_color, attrs = None, []
        if row.startswith(("POLYMARKET", "MULTI-CRYPTO", "SESSION TELEMETRY", "CAPITAL & RISK", "ORDER & EXECUTION", "SYSTEM & DISCOVERY")):
            row_color, attrs = "cyan", ["bold"]
        elif "phase:" in lower and "cutoff" in lower:
            row_color, attrs = "red", ["bold"]
        elif "phase:" in lower and "snipe window" in lower:
            row_color, attrs = ("green", ["bold"]) if frame % 2 else ("white", ["bold"])
        elif "phase:" in lower:
            row_color = "yellow"
        elif any(marker in lower for marker in ("[decided", " hold ", "filled", " matched ")):
            row_color = "green"
        elif any(marker in lower for marker in ("capped", " timeout", "failed", "error", " live bid")):
            row_color = "red"
        elif any(marker in lower for marker in ("chop", "undecided", "watching", "away)", "waiting")):
            row_color = "yellow"
        elif row.startswith(("=", "-")):
            row_color = "cyan"
        if row_color:
            print(colored("| " + clipped + " |", row_color, attrs=attrs))
        else:
            print("| " + clipped + " |")
    print(colored("+" + "-" * (width - 2) + "+", color))


def draw_dashboard(market_states, market_books, time_left):
    """Render the live market, risk, execution, and session telemetry dashboard."""
    if not sys.stdout.isatty():
        return

    elapsed = max(0, int(time.time() - SESSION_START))
    left = max(0, int(time_left)) if time_left is not None else MARKET_DURATION
    runtime = f"{elapsed // 3600:02d}:{(elapsed // 60) % 60:02d}:{elapsed % 60:02d}"
    market_ts = next(iter(market_states.values())).get('market_ts') if market_states else get_current_market_timestamp()
    window_start = datetime.fromtimestamp(market_ts, ET).strftime("%H:%M:%S")
    window_end = datetime.fromtimestamp(market_ts + MARKET_DURATION, ET).strftime("%H:%M:%S")
    if left <= STOP_THRESHOLD_SEC:
        phase, poll_interval = "CUTOFF - STANDING DOWN", 1.0
    elif left <= ENTRY_WINDOW_SEC:
        phase, poll_interval = "SNIPE WINDOW ACTIVE", 1.0
    elif left <= ENTRY_WINDOW_SEC + 10:
        phase, poll_interval = "PREPARING FAST POLL", 1.0
    else:
        phase, poll_interval = "WAITING FOR FINAL 60s", 3.0
    progress = int(max(0, min(24, round((MARKET_DURATION - left) / MARKET_DURATION * 24))))
    timer_bar = "#" * progress + "." * (24 - progress)
    spinner = next(SPINNER_FRAMES)
    account = ACCOUNT_SUFFIX.lstrip("_") or "default"
    address = os.getenv(PUBLIC_KEY_ENV_NAME, "")
    short_address = f"{address[:6]}..{address[-4:]}" if len(address) >= 12 else "unavailable"
    mode = "DRY RUN" if DRY_RUN else "LIVE MAKER"
    with _PING_LOCK:
        ping = sum(CLOB_PING_MS) / len(CLOB_PING_MS) if CLOB_PING_MS else None
    ping_text = f"{ping:.0f}ms ({'OK' if ping < 500 else 'SLOW'})" if ping is not None else "-- (waiting)"

    # Reservations include unfilled bids and filled shares. This split is for
    # display only; the exposure cap uses the same underlying reservation ledger.
    open_bid_cost = 0.0
    open_bid_shares = 0.0
    filled_cost = sum(float(s.get('filled_cost') or 0.0) for s in market_states.values())
    filled_shares = sum(float(s.get('filled_shares') or 0.0) for s in market_states.values())
    for state in market_states.values():
        order_id = state.get('last_order_id')
        exposure_key = str(state.get('exposure_key') or order_id or '')
        reserved = OPEN_ORDER_COSTS.get(exposure_key, 0.0)
        if order_id and reserved:
            open_bid_cost += max(0.0, reserved - float(state.get('filled_cost') or 0.0))
            order_price = float(state.get('last_order_price') or 0.0)
            if order_price > 0:
                open_bid_shares += max(0.0, reserved - float(state.get('filled_cost') or 0.0)) / order_price
    total_exposure = _current_exposure_usd()
    other_reserved = max(0.0, total_exposure - open_bid_cost - filled_cost)
    exposure_ratio = min(1.0, total_exposure / MAX_TOTAL_EXPOSURE_USD) if MAX_TOTAL_EXPOSURE_USD > 0 else 1.0
    exposure_bar = "#" * round(exposure_ratio * 20) + "." * (20 - round(exposure_ratio * 20))
    rows = [
        f"{spinner} POLYMARKET 5M 95c BID SNIPER v1.1 | ACCT: {account} ({short_address}) | MODE: {mode}",
        f"UPTIME: {runtime} | POLL: {poll_interval:.1f}s ({'FAST' if poll_interval == 1.0 else 'SLOW'}) | CLOB PING: {ping_text} | LOG: sniper_session.log",
        "=" * 100,
        f"WINDOW: {window_start} -> {window_end} ET (id: {market_ts}) | PHASE: {phase}",
        f"TIMER: [{timer_bar}] {left:03d}s left (Entry <= {ENTRY_WINDOW_SEC}s | Hard Cutoff @ {STOP_THRESHOLD_SEC:02d}s)",
        "-" * 100,
        "CAPITAL & RISK METRICS",
        f"USDC Balance: unavailable | Window Exposure: ${total_exposure:.2f} / ${MAX_TOTAL_EXPOSURE_USD:.2f} [{exposure_bar}]",
        f"KILL SWITCH: press 'k'/'q' in this terminal, or `echo > {KILL_SWITCH_FILE}` (env KILL_SWITCH=1). Cancels all open orders and exits.",
        f"Open Bids: {open_bid_shares:.2f}sh (${open_bid_cost:.2f}) | Filled Holding: {filled_shares:.2f}sh (${filled_cost:.2f}) | Other Reserved: ${other_reserved:.2f}",
        f"Est Payout: ${filled_shares:.2f} if all current filled shares resolve to $1 | Resolved PnL / win rate: unavailable (settlements not tracked)",
        "=" * 100,
        "MULTI-CRYPTO SCANNER & EXECUTION MATRIX",
        "ASSET | UP (BID / ASK) | DOWN (BID / ASK) | FAVORED / 95c GAP | EXECUTION STATUS",
        "-" * 100,
    ]
    for symbol in CRYPTO_ASSETS:
        market_state = market_states.get(symbol)
        if not market_state:
            rows.append(f"{symbol:<5} | --- / ---       | --- / ---         | unavailable       | market discovery pending")
            continue
        up_book, down_book = market_books.get(symbol, (None, None))
        def quote(book):
            if not book:
                return "---", "---"
            bid, ask = book.get('best_bid'), book.get('best_ask')
            return f"{bid:.3f}" if bid is not None else "---", f"{ask:.3f}" if ask is not None else "---"
        up_bid, up_ask = quote(up_book)
        down_bid, down_ask = quote(down_book)
        available_asks = [(up_book or {}).get('best_ask'), (down_book or {}).get('best_ask')]
        sides = [("UP", available_asks[0]), ("DOWN", available_asks[1])]
        leading_side, leading_ask = max(
            ((side, ask) for side, ask in sides if ask is not None),
            key=lambda pair: pair[1], default=(None, None),
        )
        decided = leading_ask is not None and leading_ask >= ENTRY_THRESHOLD
        if decided:
            gap_text = f"{leading_side} [DECIDED +{(leading_ask - ENTRY_THRESHOLD) * 100:.0f}c]"
        elif leading_ask is not None:
            gap_text = f"{leading_side} ({(ENTRY_THRESHOLD - leading_ask) * 100:.1f}c away)"
        else:
            gap_text = "no asks / unavailable"

        if market_state.get('entered'):
            status_text = f"HOLD {market_state.get('filled_shares', 0):.2f}sh @ ${market_state.get('entry_fill_price') or 0:.3f}"
        elif market_state.get('last_order_id'):
            kind = "SIM BID" if DRY_RUN else "LIVE BID"
            status_text = f"{kind} {market_state.get('last_order_price') or 0:.3f}"
        else:
            status_text = "WATCHING"
            if decided:
                try:
                    price = compute_bid_target(up_book if leading_side == "UP" else down_book)
                    shares = calculate_shares(BET_SIZE_USD, price) if price else 0.0
                    order_cost = price * shares if price else 0.0
                    if total_exposure + order_cost > MAX_TOTAL_EXPOSURE_USD:
                        status_text = f"CAPPED (${total_exposure:.2f}/${MAX_TOTAL_EXPOSURE_USD:.2f})"
                    elif price:
                        status_text = f"SIGNAL / BID {price:.3f}"
                except (TypeError, ValueError):
                    pass
            elif leading_ask is None:
                status_text = "NO BOOK / UNDECIDED"
            elif leading_ask < ENTRY_THRESHOLD:
                status_text = "CHOP / UNDECIDED"
        rows.append(
            f"{symbol:<5} | {up_bid} / {up_ask}       | {down_bid} / {down_ask}         | {gap_text:<19} | {status_text}"
        )
    fill_rate = (SESSION_ENTRIES / SESSION_SIGNALS * 100.0) if SESSION_SIGNALS else 0.0
    rows.extend([
        "=" * 100,
        "SESSION TELEMETRY",
        f"Windows Seen: {SESSION_WINDOWS:<4} Signals (>=95c): {SESSION_SIGNALS:<4} Order Attempts: {SESSION_ATTEMPTS:<4} Reprices: {SESSION_REPRICES}",
        f"Fills: {SESSION_ENTRIES:<4} Fill Rate: {fill_rate:5.1f}% Cutoff Cancels: {SESSION_CUTOFF_CANCELS:<4} Timeouts: {SESSION_TIMEOUTS}",
        "-" * 100,
        "ORDER & EXECUTION LOG (persistent in sniper_session.log; last 5 shown)",
    ])
    if ORDER_LOG:
        rows.extend(f"{ts}  {msg}" for ts, msg, _ in ORDER_LOG)
    else:
        rows.append("No order actions yet.")
    rows.extend(["-" * 100, "SYSTEM & DISCOVERY LOG"])
    if EVENT_LOG:
        rows.extend(f"{ts}  {msg}" for ts, msg, _ in EVENT_LOG)
    else:
        rows.append("Starting up; waiting for market data.")

    print("\033[2J\033[H", end="")
    _draw_panel("POLYMARKET 5-MINUTE BID SNIPER", rows)
    footer = "[Ctrl+C] Safe shutdown: cancel live bids in parallel before exit" if not DRY_RUN else "[Ctrl+C] Safe shutdown: clear simulated bids; dry-run state is memory only"
    color = "yellow" if left <= ENTRY_WINDOW_SEC else "green"
    print(colored(footer, color))


def draw_scoreboard(market_states):
    """Print a compact session summary when the bot stops."""
    elapsed = max(0, int(time.time() - SESSION_START))
    runtime = f"{elapsed // 3600:02d}:{(elapsed // 60) % 60:02d}:{elapsed % 60:02d}"
    print(colored("\nSESSION SCOREBOARD", "cyan", attrs=["bold"]))
    print(colored("-" * 40, "cyan"))
    print(f"Runtime:          {runtime}")
    print(f"Filled entries:   {SESSION_ENTRIES}")
    print(f"Order attempts:   {SESSION_ATTEMPTS}")
    print(f"No-signal checks: {SESSION_SKIPS}")
    for symbol, market_state in market_states.items():
        if market_state.get('entered'):
            print(f"{symbol} held {market_state.get('favored_side')} @ ${market_state.get('entry_fill_price') or 0:.3f}")
        elif market_state.get('last_order_id'):
            suffix = "simulated order remains in memory" if DRY_RUN else "verify/cancel on Polymarket"
            print(colored(f"{symbol}: open order {suffix}", "yellow"))
    print(colored("-" * 40, "cyan"))


def _cancel_and_confirm(token_id, order_id, wait_sec=0.6, exposure_key=None):
    """Cancel an order and classify whether it filled during the cancellation race."""
    exposure_key = str(exposure_key or order_id)
    if DRY_RUN:
        cancel_token_orders_acct(token_id, order_id=order_id)
        OPEN_ORDER_COSTS.pop(exposure_key, None)
        return "CANCELED", {
            'status': 'CANCELED', 'size_matched': 0.0, 'original_size': 0.0,
            'price': 0.0, 'has_match': False, 'is_filled': False,
        }

    uncertain_order = order_id == 'timeout-assumed-live'
    canceled = cancel_token_orders_acct(
        token_id, order_id=None if uncertain_order else order_id
    )
    if wait_sec > 0:
        time.sleep(wait_sec)

    if uncertain_order:
        # A timed-out submit has no order ID to query; cancel all orders for its
        # token and use the positions endpoint to detect a fill before clearing risk.
        if not canceled:
            return "FAILED", None
        shares = position_shares_for_token(token_id)
        if shares is None:
            return "FAILED", None
        if shares > 0:
            return "FILLED", {
                'status': 'CANCELED', 'size_matched': shares, 'original_size': shares,
                'price': 0.0, 'has_match': True, 'is_filled': True,
            }
        OPEN_ORDER_COSTS.pop(exposure_key, None)
        return "CANCELED", {
            'status': 'CANCELED', 'size_matched': 0.0, 'original_size': 0.0,
            'price': 0.0, 'has_match': False, 'is_filled': False,
        }

    result = get_order_status_for_id(order_id)
    if not result or result == "ERROR":
        return "FAILED", None
    if result.get('has_match'):
        if result.get('status') == 'LIVE' and not result.get('is_filled'):
            return "FAILED", result
        matched_cost = float(result.get('size_matched') or 0) * float(result.get('price') or 0)
        if matched_cost > 0:
            OPEN_ORDER_COSTS[exposure_key] = matched_cost
        return "FILLED", result
    if result.get('status') in ('CANCELED', 'CANCELLED', 'NOT_FOUND'):
        OPEN_ORDER_COSTS.pop(exposure_key, None)
        return "CANCELED", result
    return "FAILED", result


def _clear_order_state(market_state):
    market_state['last_order_id'] = None
    market_state['last_order_price'] = None
    market_state['open_order_price'] = None
    market_state['exposure_key'] = None


def _record_fill(symbol, market_state, fill_status, preserve_open_order=False):
    """Record any matched quantity while keeping its cost reserved this window."""
    global SESSION_ENTRIES
    if not fill_status:
        return
    matched = float(fill_status.get('size_matched') or 0.0)
    if matched <= 0 and fill_status.get('is_filled'):
        matched = float(fill_status.get('original_size') or 0.0)
    price = float(fill_status.get('price') or market_state.get('last_order_price') or 0.0)
    exposure_key = str(market_state.get('exposure_key') or market_state.get('last_order_id') or '')
    if matched > 0 and price > 0 and exposure_key and not preserve_open_order:
        OPEN_ORDER_COSTS[exposure_key] = matched * price
    if matched > market_state.get('filled_shares', 0.0):
        market_state['filled_shares'] = matched
        market_state['filled_cost'] = matched * price
    if not market_state.get('entered'):
        SESSION_ENTRIES += 1
    market_state['entered'] = True
    market_state['entry_fill_price'] = price
    log_event(f"{symbol} matched {matched:.2f} shares @ ${price:.3f}; holding to expiry", "green")


def _poll_book(token_id, side_label):
    started = time.perf_counter()
    try:
        return get_order_book(token_id)
    except Exception as exc:
        log_event(f"{side_label} book request failed: {type(exc).__name__}", "yellow")
        return None
    finally:
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        with _PING_LOCK:
            CLOB_PING_MS.append(elapsed_ms)


def main():
    global SESSION_SKIPS, SESSION_ENTRIES, SESSION_WINDOWS, SESSION_SIGNALS
    global SESSION_REPRICES, SESSION_CUTOFF_CANCELS
    _validate_configuration()
    market_states = {}
    market_books = {}
    executor = ThreadPoolExecutor(max_workers=BOOK_WORKERS, thread_name_prefix="market-data")
    current_market_ts = None
    next_discovery_at = 0.0
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
            OPEN_ORDER_COSTS.pop(str(market_state.get('exposure_key') or order_id), None)
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
            kill_triggered = (
                os.path.exists(KILL_SWITCH_FILE)
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
                log_event("KILL SWITCH triggered - canceling all open orders and shutting down", "red")
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
                    if os.path.exists(KILL_SWITCH_FILE):
                        os.remove(KILL_SWITCH_FILE)
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

                # Retry ambiguous leftovers with an asset-wide cancel at the
                # boundary. Filled-position reservations are scoped to the old
                # five-minute window and are released before the new one starts.
                fallback_futures = {
                    executor.submit(cancel_token_orders_acct, old_state['favored_token_id']): (symbol, old_state)
                    for symbol, old_state in unresolved
                    if old_state.get('favored_token_id')
                }
                unresolved_costs = {}
                for future in as_completed(fallback_futures):
                    symbol, old_state = fallback_futures[future]
                    try:
                        canceled = future.result()
                    except Exception:
                        canceled = False
                    if not canceled:
                        log_event(f"{symbol} rollover asset-wide cancel failed", "red")
                        exposure_key = str(old_state.get('exposure_key') or old_state.get('last_order_id'))
                        if exposure_key in OPEN_ORDER_COSTS:
                            unresolved_costs[exposure_key] = OPEN_ORDER_COSTS[exposure_key]
                OPEN_ORDER_COSTS.clear()
                OPEN_ORDER_COSTS.update(unresolved_costs)

                current_market_ts = market_ts
                market_states = {}
                market_books = {}
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

            missing = [symbol for symbol in CRYPTO_ASSETS if symbol not in market_states]
            # Market discovery can block on network timeouts. Stop retrying
            # before the cutoff so it cannot delay cancellation of live bids.
            if missing and now >= next_discovery_at and get_time_remaining(market_ts) > STOP_THRESHOLD_SEC:
                futures = {
                    executor.submit(get_market_info, market_ts, symbol): symbol
                    for symbol in missing
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
                draw_dashboard(market_states, market_books, time_left)
                time.sleep(1.0)
                continue

            # Poll all discovered books concurrently. A book may validly have
            # only bids or only asks; each outcome is evaluated independently.
            book_futures = {}
            for symbol, market_state in market_states.items():
                info = market_state['market_info']
                book_futures[executor.submit(_poll_book, info['up_token_id'], f"{symbol} UP")] = (symbol, "UP")
                book_futures[executor.submit(_poll_book, info['down_token_id'], f"{symbol} DOWN")] = (symbol, "DOWN")
            fresh_books = {symbol: {"UP": None, "DOWN": None} for symbol in market_states}
            for future in as_completed(book_futures):
                symbol, side = book_futures[future]
                try:
                    fresh_books[symbol][side] = future.result()
                except Exception as exc:
                    log_event(f"{symbol} {side} book poll failed: {type(exc).__name__}", "yellow")
            for symbol, books in fresh_books.items():
                market_books[symbol] = (books["UP"], books["DOWN"])

            now = time.time()
            time_left = get_time_remaining(market_ts)
            in_window = ENTRY_WINDOW_SEC >= time_left > STOP_THRESHOLD_SEC
            for symbol, market_state in market_states.items():
                if not in_window or now - market_state['last_order_update'] < ORDER_UPDATE_INTERVAL:
                    continue
                market_state['last_order_update'] = now
                up_book, down_book = market_books.get(symbol, (None, None))

                # Uncertain submissions have no queryable order ID. Never
                # duplicate them; discover/cancel them through position polling.
                if market_state.get('last_order_id') == 'timeout-assumed-live':
                    token_id = market_state['favored_token_id']
                    shares = position_shares_for_token(token_id)
                    if shares is None:
                        continue
                    if shares > 0:
                        outcome, status = _cancel_and_confirm(
                            token_id, 'timeout-assumed-live', 0.15,
                            market_state.get('exposure_key'),
                        )
                        if outcome == "FILLED":
                            _record_fill(symbol, market_state, status)
                            _clear_order_state(market_state)
                        elif outcome == "CANCELED":
                            _record_fill(symbol, market_state, {
                                'size_matched': shares, 'price': market_state.get('last_order_price'),
                                'has_match': True,
                            })
                            _clear_order_state(market_state)
                        elif outcome == "FAILED":
                            _record_fill(symbol, market_state, {
                                'size_matched': shares, 'price': market_state.get('last_order_price'),
                                'has_match': True,
                            }, preserve_open_order=True)
                        continue

                    favored_book = up_book if market_state.get('favored_side') == "UP" else down_book
                    if favored_book is not None and compute_bid_target(favored_book) is None:
                        outcome, status = _cancel_and_confirm(
                            token_id, 'timeout-assumed-live', 0.15,
                            market_state.get('exposure_key'),
                        )
                        if outcome == "FILLED":
                            _record_fill(symbol, market_state, status)
                            _clear_order_state(market_state)
                        elif outcome == "CANCELED":
                            _clear_order_state(market_state)
                            market_state['favored_side'] = None
                            market_state['favored_token_id'] = None
                            log_event(f"{symbol} uncertain order canceled (side undecided)", "yellow")
                    continue

                order_id = market_state.get('last_order_id')
                if order_id:
                    status = get_order_status_for_id(order_id)
                    if status == "ERROR" or status is None:
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

                    if status.get('status') in ('CANCELED', 'CANCELLED', 'NOT_FOUND'):
                        OPEN_ORDER_COSTS.pop(str(market_state.get('exposure_key') or order_id), None)
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
                        favored_book = up_book if market_state.get('favored_side') == "UP" else down_book
                        if favored_book is None:
                            continue
                        target_price = compute_bid_target(favored_book)
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
                    market_state['favored_side'] = side
                    token_key = 'up_token_id' if side == "UP" else 'down_token_id'
                    market_state['favored_token_id'] = market_state['market_info'][token_key]
                    log_event(f"{symbol} {side} crossed the decided threshold", "green")

                favored_book = up_book if market_state['favored_side'] == "UP" else down_book
                target_price = compute_bid_target(favored_book)
                if target_price is not None:
                    _place_fresh_bid(market_state, market_state['favored_token_id'], target_price)

            draw_dashboard(market_states, market_books, time_left)
            time.sleep(1.0 if time_left <= ENTRY_WINDOW_SEC + 10 else 3.0)

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
        print(colored("\nStopped by user.", "yellow"))
        draw_scoreboard(market_states)
    finally:
        executor.shutdown(wait=False, cancel_futures=True)


if __name__ == "__main__":
    main()
