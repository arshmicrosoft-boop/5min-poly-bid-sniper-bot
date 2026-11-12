"""Small market and sizing helpers used by the 95-cent bid sniper."""

import json
import math

import requests


def calculate_shares(bet_size_usd, price):
    """Return a share quantity respecting Polymarket's 5-share minimum."""
    price = float(price)
    if price <= 0 or price > 1:
        return 0.0
    requested = float(bet_size_usd) / price
    # Round upward to hundredths so the resulting quantity still meets budget
    # sizing after decimal precision, while honoring the exchange minimum.
    return max(5.0, math.ceil(requested * 100) / 100.0)


def get_token_id(market_id):
    """Fetch Gamma market outcome token IDs in UP/DOWN outcome order.

    Return shape matches the caller's expectation: [market_id, up_id, down_id].
    """
    response = requests.get(
        f"https://gamma-api.polymarket.com/markets/{market_id}", timeout=10
    )
    if response.status_code != 200:
        return []
    market = response.json()
    token_ids = market.get("clobTokenIds", [])
    outcomes = market.get("outcomes", [])
    if isinstance(token_ids, str):
        token_ids = json.loads(token_ids)
    if isinstance(outcomes, str):
        outcomes = json.loads(outcomes)
    if not isinstance(token_ids, list) or len(token_ids) < 2:
        return []
    if isinstance(outcomes, list) and len(outcomes) == len(token_ids):
        indexed = {str(name).strip().lower(): token_ids[i] for i, name in enumerate(outcomes)}
        up_id = next((v for k, v in indexed.items() if k in ("up", "yes")), None)
        down_id = next((v for k, v in indexed.items() if k in ("down", "no")), None)
        if up_id and down_id:
            return [market_id, str(up_id), str(down_id)]
    # Token ordering is not a substitute for explicit outcome labels.
    return []
