import copy
import importlib
import json
import math
import os
import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest import mock

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

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

        with self.assertRaises((TypeError, ValueError)):
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

    def test_state_first_commit_survives_output_failure_and_rebuilds_projection(self):
        now = datetime.fromisoformat("2026-07-21T10:00:30+05:30")
        fixture = json.loads(
            (Path(__file__).parent / "fixtures" / "live_prices_v1.json").read_text()
        )
        sig = {
            "schema_version": "paper_signal_v2_target_weights",
            "signal_id": "signal-commit",
            "signal_date": "2026-07-17",
            "modeled_execution_date": None,
            "modeled_execution_open": None,
            "target_weights": {"AAA": 0.6, "BBB": 0.3},
            "target_cash_weight": 0.1,
        }
        with tempfile.TemporaryDirectory() as td:
            state_path = Path(td) / "state.json"
            output_path = Path(td) / "latest.json"
            state = ledger.PortfolioState(cash=100_000.0)

            def fail_output(path, payload):
                if Path(path) == output_path:
                    raise OSError("projection failed")
                ledger.atomic_write_json(path, payload)

            with self.assertRaisesRegex(OSError, "projection failed"):
                ledger.commit_signal_run(
                    state, sig, fixture, now,
                    state_path=state_path, output_path=output_path,
                    writer=fail_output,
                )

            persisted = ledger.PortfolioState.from_dict(json.loads(state_path.read_text()))
            self.assertEqual(persisted.last_consumed_signal_id, "signal-commit")
            self.assertEqual(persisted.state_revision, 1)
            self.assertFalse(output_path.exists())

            replayed = ledger.commit_signal_run(
                persisted, sig, fixture, now,
                state_path=state_path, output_path=output_path,
            )
            self.assertFalse(replayed)
            self.assertEqual(json.loads(output_path.read_text())["state_revision"], 1)
            self.assertEqual(len(persisted.trade_log), len(state.trade_log))

    def test_failure_before_state_replace_changes_neither_file(self):
        now = datetime.fromisoformat("2026-07-21T10:00:30+05:30")
        fixture = json.loads((Path(__file__).parent / "fixtures" / "live_prices_v1.json").read_text())
        sig = {
            "schema_version": "paper_signal_v2_target_weights", "signal_id": "signal-fail",
            "signal_date": "2026-07-17", "target_weights": {"AAA": 0.6, "BBB": 0.3},
            "target_cash_weight": 0.1, "modeled_execution_date": None,
            "modeled_execution_open": None,
        }
        with tempfile.TemporaryDirectory() as td:
            state_path = Path(td) / "state.json"; output_path = Path(td) / "latest.json"
            state_path.write_text('{"sentinel": true}\n'); output_path.write_text('{"old": true}\n')
            with self.assertRaises(OSError):
                ledger.commit_signal_run(
                    ledger.PortfolioState(cash=100_000), sig, fixture, now,
                    state_path=state_path, output_path=output_path,
                    writer=mock.Mock(side_effect=OSError("state failed")),
                )
            self.assertEqual(json.loads(state_path.read_text()), {"sentinel": True})
            self.assertEqual(json.loads(output_path.read_text()), {"old": True})

    def test_reader_rejects_state_output_revision_mismatch(self):
        with self.assertRaisesRegex(ledger.StateRevisionError, "revision mismatch"):
            ledger.validate_projection_revision(
                {"state_revision": 3}, {"state_revision": 2}
            )


if __name__ == "__main__":
    unittest.main()
