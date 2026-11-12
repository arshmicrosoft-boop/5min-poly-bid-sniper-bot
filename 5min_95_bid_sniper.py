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
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import deque
import requests
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from termcolor import colored

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
BET_SIZE_USD = 1.0
ENTRY_THRESHOLD = 0.95
MAX_BUY_PRICE = 0.95
ENTRY_WINDOW_SEC = 60
STOP_THRESHOLD_SEC = 5
ORDER_UPDATE_INTERVAL = 3
PRICE_HOLD_TOLERANCE = 0.005

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
ROCKET_FRAMES = itertools.cycle(["^   ", " ^  ", "  ^ ", "   ^", "  ^ ", " ^  "])

EVENT_LOG = deque(maxlen=6)


def log_event(msg, color="white"):
    ts = datetime.now().strftime("%H:%M:%S")
    EVENT_LOG.append((ts, msg, color))


# ============================================================================
# POLYMARKET CLIENT HELPERS (account-aware)
# ============================================================================
_CLIENT_CACHE = None


def _build_client():
    global _CLIENT_CACHE
    if DRY_RUN:
        raise RuntimeError("CLOB account client is disabled while DRY_RUN is enabled")
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
    return client


def get_order_status_for_id(order_id):
    if not order_id or order_id == 'timeout-assumed-live':
        return None
    if DRY_RUN:
        order = PAPER_ORDERS.get(order_id)
        if not order:
            return {'status': 'NOT_FOUND', 'size_matched': 0.0, 'original_size': 0.0,
                    'price': 0.0, 'is_filled': False}
        return {'status': order['status'], 'size_matched': 0.0,
                'original_size': order['size'], 'price': order['price'], 'is_filled': False}
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
                'is_filled': size_matched > 0,
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


def cancel_token_orders_acct(token_id):
    if DRY_RUN:
        for order in PAPER_ORDERS.values():
            if order['token_id'] == str(token_id) and order['status'] == 'LIVE':
                order['status'] = 'CANCELED'
        log_event("DRY RUN: simulated cancellation (memory only)", "yellow")
        return True

    from py_clob_client_v2.clob_types import OrderMarketCancelParams
    client = _build_client()
    try:
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
    response = requests.get(
        "https://clob.polymarket.com/book",
        params={'token_id': token_id},
        timeout=10,
    )
    if response.status_code != 200:
        return None
    data = response.json()
    bids = data.get('bids', [])
    asks = data.get('asks', [])
    if not bids or not asks:
        return None
    return {
        'best_bid': float(bids[-1]['price']),
        'best_ask': float(asks[-1]['price']),
    }


def get_market_info(market_ts, symbol):
    """Fetch one coin's exact 5-minute market, mapping token IDs by outcome label."""
    prefix = CRYPTO_ASSETS[symbol]
    market_slug = f"{prefix}-updown-5m-{market_ts}"
    for attempt in range(2):
        try:
            response = requests.get(
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
        return 0.0
    try:
        resp = requests.get(
            "https://data-api.polymarket.com/positions",
            params={'user': user_address, 'limit': 500, 'sortBy': 'CURRENT', 'sortDirection': 'DESC'},
            timeout=8,
        )
        if resp.status_code != 200:
            return 0.0
        for pos in resp.json():
            if str(pos.get('asset')) == str(token_id):
                size = float(pos.get('size', 0) or 0)
                if size > 0:
                    return size
        return 0.0
    except Exception as e:
        log_event(f"positions fetch failed: {e}", "yellow")
        return 0.0


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
    if favored_book['best_ask'] < ENTRY_THRESHOLD:
        return None
    target = min(favored_book['best_bid'], MAX_BUY_PRICE)
    return round(target, 3)


def _place_fresh_bid(state, token_id, target_price):
    global SESSION_ATTEMPTS
    shares = calculate_shares(BET_SIZE_USD, target_price)
    if shares <= 0:
        return
    SESSION_ATTEMPTS += 1
    resp = place_limit_order(token_id, "BUY", target_price, shares, neg_risk=state['neg_risk'])
    if resp:
        oid = resp.get('orderID')
        if oid and oid != 'timeout-assumed-live':
            state['last_order_id'] = oid
            state['last_order_price'] = target_price
            state['open_order_price'] = target_price
        elif oid == 'timeout-assumed-live':
            state['last_order_id'] = 'timeout-assumed-live'
            state['last_order_price'] = target_price
            state['open_order_price'] = target_price


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
        'last_order_id': None,
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
    width = 78
    print(colored("+" + "-" * (width - 2) + "+", color))
    print(colored("| " + title[:width - 4].ljust(width - 4) + " |", color, attrs=["bold"]))
    print(colored("+" + "-" * (width - 2) + "+", color))
    for row in rows:
        # Keep the display inside the frame and avoid terminal wrapping.
        print("| " + row[:width - 4].ljust(width - 4) + " |")
    print(colored("+" + "-" * (width - 2) + "+", color))


def draw_dashboard(market_states, market_books, time_left):
    """Render all discovered coin markets together in one compact dashboard."""
    if not sys.stdout.isatty():
        return

    elapsed = max(0, int(time.time() - SESSION_START))
    left = max(0, int(time_left)) if time_left is not None else MARKET_DURATION
    runtime = f"{elapsed // 3600:02d}:{(elapsed // 60) % 60:02d}:{elapsed % 60:02d}"
    rows = [
        f"Mode: {'DRY RUN (no orders sent)' if DRY_RUN else 'LIVE ORDERS'}   |   Coins: {len(market_states)}/{len(CRYPTO_ASSETS)}   |   Runtime {runtime}",
        f"Final {ENTRY_WINDOW_SEC}s only | ask >= ${ENTRY_THRESHOLD:.2f} | post-only bid <= ${MAX_BUY_PRICE:.2f} | {left:3d}s left",
        "SYMBOL  UP bid/ask       DOWN bid/ask     SIGNAL / ORDER",
    ]
    for symbol in CRYPTO_ASSETS:
        market_state = market_states.get(symbol)
        if not market_state:
            rows.append(f"{symbol:<6} market unavailable in this window")
            continue
        up_book, down_book = market_books.get(symbol, (None, None))
        def quote(book):
            return f"{book['best_bid']:.3f}/{book['best_ask']:.3f}" if book else "---/---"
        side = market_state.get('favored_side') or "watching"
        if market_state.get('last_order_id'):
            kind = "SIM" if DRY_RUN else "LIVE"
            order = f"{kind} {side} @ {market_state.get('last_order_price') or 0:.3f}"
        else:
            order = "no order"
        rows.append(f"{symbol:<6} {quote(up_book):<15} {quote(down_book):<15} {side} / {order}")
    rows.extend([
        f"Session: {SESSION_ENTRIES} fills   {SESSION_ATTEMPTS} order attempts   {SESSION_SKIPS} no-signal checks",
        "Recent events:",
    ])
    if EVENT_LOG:
        rows.extend(f"{ts}  {msg}" for ts, msg, _ in EVENT_LOG)
    else:
        rows.append("Starting up; waiting for market data.")

    print("\033[2J\033[H", end="")
    _draw_panel("POLYMARKET MULTI-CRYPTO 5-MINUTE BID SNIPER", rows)
    footer = "Dry-run order state is in memory only; Ctrl+C clears it." if DRY_RUN else "Ctrl+C stops the bot. Check for any remaining live orders."
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


def _cancel_and_confirm(token_id, order_id):
    """Cancel and verify the order before allowing another order to be sent."""
    if DRY_RUN:
        order = PAPER_ORDERS.get(order_id)
        if order:
            order['status'] = 'CANCELED'
        log_event("DRY RUN: simulated order canceled", "yellow")
        return True
    if not cancel_token_orders_acct(token_id):
        return False
    time.sleep(1.0)
    result = get_order_status_for_id(order_id)
    return bool(result and result != "ERROR" and result.get('status') in ('CANCELED', 'CANCELLED', 'NOT_FOUND'))


def _poll_book(token_id, side_label):
    try:
        return get_order_book(token_id)
    except Exception as exc:
        log_event(f"{side_label} book request failed: {type(exc).__name__}", "yellow")
        return None


def legacy_main():
    global SESSION_SKIPS, SESSION_ENTRIES
    _validate_configuration()
    reset_market_state()
    up_book = down_book = None
    log_event(
        "Bot started in DRY RUN; no account orders will be sent" if DRY_RUN
        else "Bot started in LIVE ORDER mode",
        "yellow" if DRY_RUN else "green",
    )

    try:
        while True:
            now = time.time()
            market_ts = get_current_market_timestamp()
            time_left = get_time_remaining(market_ts)

            # New five-minute market: cancel any unfilled order from the old one,
            # then fetch the new market metadata with brief retries.
            if market_ts != state['market_ts']:
                old_token = state.get('favored_token_id')
                old_order = state.get('last_order_id')
                if old_token and old_order and not state.get('entered'):
                    if old_order == 'timeout-assumed-live':
                        cancel_token_orders_acct(old_token)
                    else:
                        _cancel_and_confirm(old_token, old_order)
                reset_market_state()
                state['market_ts'] = market_ts
                up_book = down_book = None
                log_event(f"New market window {market_ts}", "cyan")
                for attempt in range(8):
                    try:
                        info = get_market_info(market_ts)
                    except Exception as exc:
                        info = None
                        log_event(f"Market lookup failed: {type(exc).__name__}", "yellow")
                    if info:
                        state['market_info'] = info
                        state['neg_risk'] = info.get('neg_risk', False)
                        log_event("Market tokens loaded", "green")
                        break
                    time.sleep(1.0)
                if not state['market_info']:
                    log_event("Market not available yet; will retry", "yellow")

            info = state.get('market_info')
            if info:
                up_book = _poll_book(info['up_token_id'], "UP")
                down_book = _poll_book(info['down_token_id'], "DOWN")

            time_left = get_time_remaining(market_ts)
            in_window = ENTRY_WINDOW_SEC >= time_left > STOP_THRESHOLD_SEC

            # Stop opening orders at the cutoff. Cancel an unfilled resting order
            # and clear it only after the CLOB confirms cancellation.
            if (not state['entered'] and time_left <= STOP_THRESHOLD_SEC
                    and state['last_order_id'] and state['favored_token_id']):
                oid = state['last_order_id']
                if oid == 'timeout-assumed-live':
                    cancel_token_orders_acct(state['favored_token_id'])
                    log_event("Cutoff reached; cancel requested for unverified order", "yellow")
                elif _cancel_and_confirm(state['favored_token_id'], oid):
                    state['last_order_id'] = None
                    state['last_order_price'] = None
                    state['open_order_price'] = None
                    log_event("Cutoff reached; resting order canceled", "yellow")

            if (info and not state['entered'] and in_window and up_book and down_book
                    and now - state['last_order_update'] >= ORDER_UPDATE_INTERVAL):
                state['last_order_update'] = now

                if state['favored_side'] is None:
                    side, _ = pick_decided_side(up_book, down_book)
                    if side:
                        state['favored_side'] = side
                        state['favored_token_id'] = info['up_token_id'] if side == "UP" else info['down_token_id']
                        log_event(f"{side} crossed the decided threshold", "green")
                    else:
                        SESSION_SKIPS += 1

                if state['favored_side'] is not None:
                    token_id = state['favored_token_id']
                    favored_book = up_book if state['favored_side'] == "UP" else down_book
                    shares_held = position_shares_for_token(token_id)

                    if shares_held > 0:
                        state['entered'] = True
                        state['entry_fill_price'] = state['last_order_price'] or 0.0
                        SESSION_ENTRIES += 1
                        log_event(f"Position detected ({shares_held:.2f} shares); holding to expiry", "green")
                    elif state['last_order_id'] == 'timeout-assumed-live':
                        # The post timed out, so do not risk submitting a duplicate.
                        state['entered'] = True
                    elif state['last_order_id']:
                        oid = state['last_order_id']
                        status = get_order_status_for_id(oid)
                        if status == "ERROR" or status is None:
                            pass  # Cannot prove order state; do not submit another.
                        elif status['is_filled']:
                            state['entered'] = True
                            state['entry_fill_price'] = status['price'] or state['last_order_price'] or 0.0
                            SESSION_ENTRIES += 1
                            log_event("Order filled; holding to expiry", "green")
                        elif status['status'] == 'LIVE':
                            target_price = compute_bid_target(favored_book)
                            old_price = status['price'] or state['last_order_price']
                            if target_price is None:
                                if _cancel_and_confirm(token_id, oid):
                                    state['last_order_id'] = None
                                    state['last_order_price'] = None
                                    state['open_order_price'] = None
                                    log_event("Side no longer decided; order canceled", "yellow")
                            elif old_price and abs(old_price - target_price) > PRICE_HOLD_TOLERANCE:
                                if _cancel_and_confirm(token_id, oid):
                                    state['last_order_id'] = None
                                    state['last_order_price'] = None
                                    state['open_order_price'] = None
                                    _place_fresh_bid(state, token_id, target_price)
                        elif status['status'] in ('CANCELED', 'CANCELLED', 'NOT_FOUND'):
                            state['last_order_id'] = None
                            state['last_order_price'] = None
                            state['open_order_price'] = None
                            target_price = compute_bid_target(favored_book)
                            if target_price is not None:
                                _place_fresh_bid(state, token_id, target_price)
                    else:
                        target_price = compute_bid_target(favored_book)
                        if target_price is not None:
                            _place_fresh_bid(state, token_id, target_price)

            draw_dashboard(up_book, down_book, time_left)
            time.sleep(1.0)

    except KeyboardInterrupt:
        if state.get('last_order_id') and state.get('favored_token_id') and not state.get('entered'):
            cancel_token_orders_acct(state['favored_token_id'])
        print(colored("\nStopped by user.", "yellow"))
        draw_scoreboard()


def main():
    global SESSION_SKIPS, SESSION_ENTRIES
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

    try:
        while True:
            now = time.time()
            market_ts = get_current_market_timestamp()
            time_left = get_time_remaining(market_ts)

            if market_ts != current_market_ts:
                for old_state in market_states.values():
                    order_id = old_state.get('last_order_id')
                    token_id = old_state.get('favored_token_id')
                    if order_id and token_id and not old_state.get('entered'):
                        if order_id == 'timeout-assumed-live':
                            cancel_token_orders_acct(token_id)
                        else:
                            _cancel_and_confirm(token_id, order_id)
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
            if missing and now >= next_discovery_at:
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

            # Poll all discovered token books concurrently to keep the shared
            # entry window responsive when a public endpoint is slow.
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
                previous = market_books.get(symbol, (None, None))
                market_books[symbol] = (books["UP"] or previous[0], books["DOWN"] or previous[1])

            time_left = get_time_remaining(market_ts)
            in_window = ENTRY_WINDOW_SEC >= time_left > STOP_THRESHOLD_SEC
            for symbol, market_state in market_states.items():
                up_book, down_book = market_books.get(symbol, (None, None))

                if (not market_state['entered'] and time_left <= STOP_THRESHOLD_SEC
                        and market_state['last_order_id'] and market_state['favored_token_id']):
                    order_id = market_state['last_order_id']
                    if order_id == 'timeout-assumed-live':
                        cancel_token_orders_acct(market_state['favored_token_id'])
                        log_event(f"{symbol} cutoff reached; cancel requested for uncertain order", "yellow")
                    elif _cancel_and_confirm(market_state['favored_token_id'], order_id):
                        market_state['last_order_id'] = None
                        market_state['last_order_price'] = None
                        market_state['open_order_price'] = None
                        log_event(f"{symbol} cutoff reached; order canceled", "yellow")

                if not (in_window and up_book and down_book and not market_state['entered']
                        and now - market_state['last_order_update'] >= ORDER_UPDATE_INTERVAL):
                    continue
                market_state['last_order_update'] = now
                if market_state['favored_side'] is None:
                    side, _ = pick_decided_side(up_book, down_book)
                    if side:
                        market_state['favored_side'] = side
                        token_key = 'up_token_id' if side == "UP" else 'down_token_id'
                        market_state['favored_token_id'] = market_state['market_info'][token_key]
                        log_event(f"{symbol} {side} crossed the decided threshold", "green")
                    else:
                        SESSION_SKIPS += 1

                if market_state['favored_side'] is None:
                    continue
                token_id = market_state['favored_token_id']
                favored_book = up_book if market_state['favored_side'] == "UP" else down_book
                shares_held = position_shares_for_token(token_id)
                if shares_held > 0:
                    market_state['entered'] = True
                    market_state['entry_fill_price'] = market_state['last_order_price'] or 0.0
                    SESSION_ENTRIES += 1
                    log_event(f"{symbol} position detected ({shares_held:.2f} shares); holding to expiry", "green")
                    continue
                if market_state['last_order_id'] == 'timeout-assumed-live':
                    market_state['entered'] = True
                    continue
                if market_state['last_order_id']:
                    order_id = market_state['last_order_id']
                    status = get_order_status_for_id(order_id)
                    if status == "ERROR" or status is None:
                        continue
                    if status['is_filled']:
                        market_state['entered'] = True
                        market_state['entry_fill_price'] = status['price'] or market_state['last_order_price'] or 0.0
                        SESSION_ENTRIES += 1
                        log_event(f"{symbol} order filled; holding to expiry", "green")
                        continue
                    if status['status'] == 'LIVE':
                        target_price = compute_bid_target(favored_book)
                        old_price = status['price'] or market_state['last_order_price']
                        if target_price is None:
                            if _cancel_and_confirm(token_id, order_id):
                                market_state['last_order_id'] = None
                                market_state['last_order_price'] = None
                                market_state['open_order_price'] = None
                                log_event(f"{symbol} side no longer decided; order canceled", "yellow")
                        elif old_price and abs(old_price - target_price) > PRICE_HOLD_TOLERANCE:
                            if _cancel_and_confirm(token_id, order_id):
                                market_state['last_order_id'] = None
                                market_state['last_order_price'] = None
                                market_state['open_order_price'] = None
                                _place_fresh_bid(market_state, token_id, target_price)
                    elif status['status'] in ('CANCELED', 'CANCELLED', 'NOT_FOUND'):
                        market_state['last_order_id'] = None
                        market_state['last_order_price'] = None
                        market_state['open_order_price'] = None
                        target_price = compute_bid_target(favored_book)
                        if target_price is not None:
                            _place_fresh_bid(market_state, token_id, target_price)
                else:
                    target_price = compute_bid_target(favored_book)
                    if target_price is not None:
                        _place_fresh_bid(market_state, token_id, target_price)

            draw_dashboard(market_states, market_books, time_left)
            time.sleep(1.0)

    except KeyboardInterrupt:
        for market_state in market_states.values():
            if market_state.get('last_order_id') and market_state.get('favored_token_id') and not market_state.get('entered'):
                cancel_token_orders_acct(market_state['favored_token_id'])
        print(colored("\nStopped by user.", "yellow"))
        draw_scoreboard(market_states)
    finally:
        executor.shutdown(wait=False, cancel_futures=True)


if __name__ == "__main__":
    main()
