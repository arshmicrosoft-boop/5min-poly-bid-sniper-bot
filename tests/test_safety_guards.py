import importlib.util
import unittest
from pathlib import Path
from unittest.mock import patch

import nice_funcs

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("sniper_under_test", ROOT / "5min_95_bid_sniper.py")
sniper = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(sniper)


class SafetyGuardTests(unittest.TestCase):
    def setUp(self):
        self.original_dry_run = sniper.DRY_RUN
        self.original_client = sniper._CLIENT_CACHE
        self.original_paper_orders = dict(sniper.PAPER_ORDERS)
        self.original_costs = sniper._snapshot_order_costs()
        self.original_entries = sniper.SESSION_ENTRIES
        self.original_late_fills = sniper.SESSION_LATE_FILLS
        self.original_fill_prices = list(sniper.SESSION_FILL_PRICES)
        self.original_halt_reason = sniper.SESSION_ENTRY_HALT_REASON

    def tearDown(self):
        sniper.DRY_RUN = self.original_dry_run
        sniper._CLIENT_CACHE = self.original_client
        sniper.PAPER_ORDERS.clear()
        sniper.PAPER_ORDERS.update(self.original_paper_orders)
        sniper._replace_order_costs(self.original_costs)
        sniper.SESSION_ENTRIES = self.original_entries
        sniper.SESSION_LATE_FILLS = self.original_late_fills
        sniper.SESSION_FILL_PRICES[:] = self.original_fill_prices
        sniper.SESSION_ENTRY_HALT_REASON = self.original_halt_reason

    def test_live_configuration_remains_hard_disabled(self):
        sniper.DRY_RUN = False
        with patch.dict("os.environ", {
            sniper.PRIVATE_KEY_ENV_NAME: "0x" + "1" * 64,
            sniper.PUBLIC_KEY_ENV_NAME: "0x" + "2" * 40,
        }, clear=False):
            with self.assertRaisesRegex(SystemExit, "Live mode is disabled"):
                sniper._validate_configuration()

    def test_paper_place_and_cancel_never_build_account_client(self):
        sniper.DRY_RUN = True
        with patch.object(sniper, "_build_client", side_effect=AssertionError("account client called")):
            response = sniper.place_limit_order("token-a", "BUY", 0.93, 5)
            self.assertTrue(response["simulated"])
            self.assertTrue(sniper.cancel_token_orders_acct("token-a", response["orderID"]))
        self.assertEqual(sniper.PAPER_ORDERS[response["orderID"]]["status"], "CANCELED")

    def test_unknown_order_cancel_never_cancels_every_order_for_token(self):
        sniper.DRY_RUN = False
        with patch.object(sniper, "cancel_token_orders_acct", side_effect=AssertionError("token-wide cancel called")):
            outcome, status = sniper._cancel_and_confirm("token-a", "timeout-assumed-live")
        self.assertEqual(outcome, "FAILED")
        self.assertEqual(status["status"], "UNKNOWN")

    def test_noop_cancel_and_not_found_status_keep_reservation(self):
        sniper.DRY_RUN = False
        sniper._set_order_cost("order-1", 4.75)
        with patch.object(sniper, "cancel_token_orders_acct", return_value=True), patch.object(
            sniper,
            "get_order_status_for_id",
            return_value={"status": "NOT_FOUND", "has_match": False, "is_filled": False},
        ):
            outcome, _ = sniper._cancel_and_confirm("token-a", "order-1", wait_sec=0)
        self.assertEqual(outcome, "FAILED")
        self.assertEqual(sniper._snapshot_order_costs()["order-1"], 4.75)

    def test_startup_reserves_positions_from_prior_markets(self):
        sniper.DRY_RUN = False
        positions = {"old-token": {"shares": 5.0, "cost": 4.4}}
        with patch.object(sniper, "_account_positions_by_token", return_value=positions):
            sniper._reconcile_startup_positions({})
        self.assertEqual(sniper._snapshot_order_costs()["position:old-token"], 4.4)

    def test_fill_transfers_order_reserve_to_token_position_key(self):
        sniper._set_order_cost("order-2", 4.75)
        state = {
            "exposure_key": "order-2", "last_order_id": "order-2",
            "last_order_price": 0.95, "favored_token_id": "token-b",
            "market_ts": sniper.get_current_market_timestamp(), "entered": False,
            "filled_shares": 0.0,
        }
        sniper._record_fill("BTC", state, {
            "size_matched": 5.0, "price": 0.95, "has_match": True,
        })
        costs = sniper._snapshot_order_costs()
        self.assertNotIn("order-2", costs)
        self.assertEqual(costs["position:token-b"], 4.75)

    def test_unlabeled_outcomes_do_not_guess_up_down_order(self):
        response = type("Response", (), {
            "status_code": 200,
            "json": lambda self: {"clobTokenIds": ["token-a", "token-b"], "outcomes": []},
        })()
        with patch.object(nice_funcs.requests, "get", return_value=response):
            self.assertEqual(nice_funcs.get_token_id("market-1"), [])

    def test_cached_quotes_never_create_settlement_pnl(self):
        sniper.DRY_RUN = True
        old = (sniper.SESSION_PAPER_WINS, sniper.SESSION_PAPER_LOSSES, sniper.SESSION_PAPER_PNL)
        states = {"BTC": {
            "entered": True, "paper_filled": True, "paper_resolved": False,
            "filled_shares": 5.0, "filled_cost": 4.65, "favored_side": "UP",
            "paper_fill_price": 0.93,
        }}
        books = {"BTC": ({"best_bid": 0.99}, {"best_bid": 0.01})}
        sniper._resolve_paper_window(states, books)
        self.assertEqual(
            (sniper.SESSION_PAPER_WINS, sniper.SESSION_PAPER_LOSSES, sniper.SESSION_PAPER_PNL),
            old,
        )
        self.assertTrue(states["BTC"]["paper_resolved"])


if __name__ == "__main__":
    unittest.main()
