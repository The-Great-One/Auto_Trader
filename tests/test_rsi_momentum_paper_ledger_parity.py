import copy
import json
import math
import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import rsi_momentum_paper_ledger as ledger


FIXTURE = Path(__file__).parent / "fixtures" / "live_prices_v1.json"
NOW = datetime.fromisoformat("2026-07-21T10:00:30+05:30")


def signal(weights=None, signal_id="signal-1", modeled=True):
    weights = weights or {"AAA": 0.6, "BBB": 0.3}
    return {
        "schema_version": "paper_signal_v2_target_weights",
        "signal_id": signal_id,
        "signal_date": "2026-07-17",
        "modeled_execution_date": "2026-07-20" if modeled else None,
        "modeled_execution_open": {"AAA": 98.0, "BBB": 205.0} if modeled else None,
        "target_weights": weights,
        "target_cash_weight": 1.0 - sum(weights.values()),
    }


def snapshot():
    return json.loads(FIXTURE.read_text())


class TargetWeightLedgerParityTests(unittest.TestCase):
    def test_inverse_vol_weights_produce_non_equal_actual_notionals(self):
        state = ledger.PortfolioState(cash=100_000.0)
        ledger.execute_target_rebalance(state, signal(), snapshot(), NOW, cost_bps=10)

        notionals = {s: q * snapshot()["prices"][s] for s, q in state.positions.items()}
        self.assertGreater(notionals["AAA"], notionals["BBB"] * 1.9)
        self.assertAlmostEqual(notionals["AAA"], 60_000, delta=200)
        self.assertAlmostEqual(notionals["BBB"], 30_000, delta=200)

    def test_unchanged_holdings_trade_only_target_delta(self):
        state = ledger.PortfolioState(
            cash=25_000.0, positions={"AAA": 500.0, "CCC": 200.0},
            cost_basis={"AAA": 90.0, "CCC": 45.0},
        )
        ledger.execute_target_rebalance(state, signal(), snapshot(), NOW, cost_bps=0)
        trades = state.trade_log

        self.assertFalse(any(t["symbol"] == "AAA" and t["action"] == "SELL" for t in trades))
        self.assertTrue(any(t["symbol"] == "AAA" and t["action"] == "BUY" for t in trades))
        self.assertEqual(next(t for t in trades if t["symbol"] == "CCC")["shares"], 200)

    def test_actual_fill_time_is_now_and_historical_prices_are_diagnostics_only(self):
        snap = snapshot()
        state = ledger.PortfolioState(cash=100_000.0)
        ledger.execute_target_rebalance(state, signal(), snap, NOW, cost_bps=0)

        buy = next(t for t in state.trade_log if t["symbol"] == "AAA")
        self.assertEqual(buy["price"], 100.0)
        self.assertEqual(buy["actual_fill"], 100.0)
        self.assertEqual(buy["actual_notional"], buy["gross"])
        self.assertEqual(buy["filled_at"], NOW.isoformat())
        self.assertEqual(buy["date"], "2026-07-21")
        self.assertEqual(buy["modeled_open"], 98.0)
        self.assertAlmostEqual(buy["slippage_bps"], (100 / 98 - 1) * 10_000)

    def test_sell_slippage_is_side_adjusted_and_missing_model_is_null(self):
        state = ledger.PortfolioState(
            cash=0, positions={"CCC": 100.0}, cost_basis={"CCC": 40.0}
        )
        ledger.execute_target_rebalance(
            state, signal(), snapshot(), NOW, cost_bps=0,
            modeled_sell_opens={"CCC": 52.0},
        )
        sell = next(t for t in state.trade_log if t["symbol"] == "CCC")
        self.assertAlmostEqual(sell["slippage_bps"], (52 / 50 - 1) * 10_000)

        state2 = ledger.PortfolioState(cash=0, positions={"CCC": 100.0}, cost_basis={"CCC": 40.0})
        ledger.execute_target_rebalance(state2, signal(), snapshot(), NOW, cost_bps=0)
        self.assertIsNone(state2.trade_log[0]["modeled_open"])
        self.assertIsNone(state2.trade_log[0]["slippage_bps"])

    def test_fees_use_actual_delta_notional_and_rounding_reports_deviation(self):
        state = ledger.PortfolioState(cash=100_000.0)
        ledger.execute_target_rebalance(state, signal(), snapshot(), NOW, cost_bps=10)

        for trade in state.trade_log:
            self.assertAlmostEqual(trade["cost"], trade["gross"] * 0.001, places=2)
        self.assertGreaterEqual(state.cash, 0)
        self.assertTrue(all(float(q).is_integer() for q in state.positions.values()))
        self.assertEqual(set(state.target_weight_deviations), {"AAA", "BBB"})
        self.assertTrue(all("actual_weight" in d and "deviation" in d for d in state.target_weight_deviations.values()))

    def test_replayed_signal_id_is_idempotent(self):
        state = ledger.PortfolioState(cash=100_000.0)
        ledger.execute_target_rebalance(state, signal(), snapshot(), NOW)
        before = copy.deepcopy(state.to_dict())

        result = ledger.execute_target_rebalance(state, signal(), snapshot(), NOW)

        self.assertFalse(result)
        self.assertEqual(state.to_dict(), before)

    def test_projection_identity_must_match_authoritative_state(self):
        state = ledger.PortfolioState(state_revision=3, last_consumed_signal_id="signal-3")
        projection = ledger.state_projection(state, NOW)
        projection["last_consumed_signal_id"] = "other-signal"
        with self.assertRaisesRegex(ledger.StateRevisionError, "identity mismatch"):
            ledger.validate_projection_revision(state.to_dict(), projection)

    def test_snapshot_contract_rejects_bad_schema_identity_awareness_and_values(self):
        required = {"AAA", "BBB", "CCC"}
        mutations = [
            ("schema", lambda x: x.update(schema_version="wrong")),
            ("id", lambda x: x.update(snapshot_id="")),
            ("generated", lambda x: x.update(generated_at="2026-07-21T10:00:00")),
            ("price", lambda x: x["prices"].update(AAA=math.nan)),
            ("time", lambda x: x["price_times"].update(AAA="2026-07-21T09:59:58")),
            ("coverage", lambda x: x["prices"].pop("CCC")),
        ]
        for label, mutate in mutations:
            with self.subTest(label=label):
                snap = snapshot(); mutate(snap)
                with self.assertRaises(ledger.RebalanceDataError):
                    ledger.validate_quote_snapshot(snap, required, NOW, max_age_sec=60)

    def test_fill_snapshot_is_strictly_fresh_even_after_close(self):
        snap = snapshot()
        after_close = datetime.fromisoformat("2026-07-21T16:00:00+05:30")
        with self.assertRaisesRegex(ledger.RebalanceDataError, "stale"):
            ledger.validate_quote_snapshot(snap, {"AAA"}, after_close, max_age_sec=60)

    def test_stale_or_missing_quote_aborts_without_consuming_signal(self):
        for mutate in (
            lambda x: x["price_times"].update(AAA="2026-07-21T09:00:00+05:30"),
            lambda x: x["prices"].pop("AAA"),
        ):
            state = ledger.PortfolioState(cash=100_000.0)
            before = state.to_dict(); snap = snapshot(); mutate(snap)
            with self.assertRaises(ledger.RebalanceDataError):
                ledger.execute_target_rebalance(state, signal(), snap, NOW, max_age_sec=60)
            self.assertEqual(state.to_dict(), before)


if __name__ == "__main__":
    unittest.main()
