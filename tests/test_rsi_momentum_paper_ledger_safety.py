import copy
import importlib
import math
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import pandas as pd

from scripts import rsi_momentum_paper_ledger as ledger


class RebalanceSafetyTests(unittest.TestCase):
    def test_hist_dir_can_be_overridden_for_tickertape_dataset(self):
        with tempfile.TemporaryDirectory() as td:
            with mock.patch.dict(
                os.environ,
                {"RSI_LEDGER_HIST_DIR": td, "RSI_LEDGER_MIN_ROWS": "200"},
            ):
                reloaded = importlib.reload(ledger)
                self.assertEqual(reloaded.HIST_DIR, Path(td))
                self.assertEqual(reloaded.MIN_PRICE_ROWS, 200)
        importlib.reload(ledger)

    def make_state(self):
        return ledger.PortfolioState(
            cash=100.0,
            positions={"OLD_A": 10.0, "OLD_B": 5.0},
            cost_basis={"OLD_A": 9.0, "OLD_B": 19.0},
            last_rebalance_date="2026-07-01",
            trade_log=[{"date": "2026-07-01", "action": "MARKER"}],
            realized_pnl=7.0,
        )

    def test_missing_held_price_aborts_without_mutating_state(self):
        state = self.make_state()
        before = copy.deepcopy(state.to_dict())
        picks = [f"NEW_{i}" for i in range(8)]
        prices = pd.Series({"OLD_A": 11.0, **{p: 20.0 + i for i, p in enumerate(picks)}})

        with self.assertRaises(ledger.RebalanceDataError):
            ledger.execute_rebalance(state, picks, prices, "2026-07-17")

        self.assertEqual(state.to_dict(), before)

    def test_undersized_signal_aborts_without_mutating_state(self):
        state = self.make_state()
        before = copy.deepcopy(state.to_dict())
        picks = ["NEW_A", "NEW_B"]
        prices = pd.Series({"OLD_A": 11.0, "OLD_B": 21.0, "NEW_A": 30.0, "NEW_B": 40.0})

        with self.assertRaisesRegex(ledger.RebalanceDataError, "exactly 8"):
            ledger.execute_rebalance(state, picks, prices, "2026-07-17")

        self.assertEqual(state.to_dict(), before)

    def test_oversized_signal_aborts_without_mutating_state(self):
        state = self.make_state()
        before = copy.deepcopy(state.to_dict())
        picks = [f"NEW_{i}" for i in range(9)]
        prices = pd.Series({
            "OLD_A": 11.0,
            "OLD_B": 21.0,
            **{p: 30.0 + i for i, p in enumerate(picks)},
        })

        with self.assertRaisesRegex(ledger.RebalanceDataError, "exactly 8"):
            ledger.execute_rebalance(state, picks, prices, "2026-07-17")

        self.assertEqual(state.to_dict(), before)

    def test_non_finite_pick_price_aborts_without_mutating_state(self):
        state = self.make_state()
        before = copy.deepcopy(state.to_dict())
        picks = [f"NEW_{i}" for i in range(8)]
        values = {p: 20.0 + i for i, p in enumerate(picks)}
        values[picks[-1]] = math.nan
        prices = pd.Series({"OLD_A": 11.0, "OLD_B": 21.0, **values})

        with self.assertRaises(ledger.RebalanceDataError):
            ledger.execute_rebalance(state, picks, prices, "2026-07-17")

        self.assertEqual(state.to_dict(), before)

    def test_runtime_error_after_validation_does_not_partially_mutate_state(self):
        state = self.make_state()
        state.cost_basis["OLD_A"] = "invalid-cost-basis"
        before = copy.deepcopy(state.to_dict())
        picks = [f"NEW_{i}" for i in range(8)]
        prices = pd.Series({
            "OLD_A": 11.0,
            "OLD_B": 21.0,
            **{p: 30.0 + i for i, p in enumerate(picks)},
        })

        with self.assertRaises(TypeError):
            ledger.execute_rebalance(state, picks, prices, "2026-07-17")

        self.assertEqual(state.to_dict(), before)

    def test_complete_rebalance_sells_and_buys_all_targets(self):
        state = self.make_state()
        picks = [f"NEW_{i}" for i in range(8)]
        prices = pd.Series({"OLD_A": 11.0, "OLD_B": 21.0, **{p: 20.0 + i for i, p in enumerate(picks)}})

        result = ledger.execute_rebalance(state, picks, prices, "2026-07-17")

        self.assertIs(result, state)
        self.assertEqual(set(state.positions), set(picks))
        self.assertEqual(state.last_rebalance_date, "2026-07-17")
        self.assertEqual(len([t for t in state.trade_log if t.get("action") == "SELL"]), 2)
        self.assertEqual(len([t for t in state.trade_log if t.get("action") == "BUY"]), 8)
        self.assertNotEqual(state.realized_pnl, 7.0)


if __name__ == "__main__":
    unittest.main()
