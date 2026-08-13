import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.atomic_io import atomic_write_json
from scripts import rsi_momentum_paper_shadow as shadow
from scripts.signal_schema import build_paper_signal, params_fingerprint


class ShadowPublicationSafetyTests(unittest.TestCase):
    def test_params_fingerprint_is_canonical_and_ignores_labels_and_metadata(self):
        left = {"top_n": 2, "periods": [22, 44], "label": "champion", "metadata": {"run": 1}}
        right = {"metadata": {"run": 99}, "periods": [22, 44], "top_n": 2, "name": "renamed"}

        self.assertEqual(params_fingerprint(left), params_fingerprint(right))

    def test_paper_signal_v2_matches_committed_contract_fixture(self):
        fixture = json.loads(
            (Path(__file__).parent / "fixtures" / "paper_signal_v2.json").read_text()
        )

        actual = build_paper_signal(
            params={"periods": [22, 44], "top_n": 2},
            signal_date="2026-07-17",
            target_weights={"BBB": 0.3, "AAA": 0.6},
            target_cash_weight=0.1,
            modeled_execution_date="2026-07-20",
            modeled_execution_open={"BBB": 222.0, "AAA": 111.0},
            metadata={"vol_lookback": 10},
        )

        self.assertEqual(actual, fixture)

    def test_target_weights_are_normalized_with_explicit_cash(self):
        signal = build_paper_signal(
            params={"top_n": 2},
            signal_date="2026-07-17",
            target_weights={"AAA": 6.0, "BBB": 3.0},
            target_cash_weight=1.0,
        )

        self.assertEqual(signal["target_weights"], {"AAA": 0.6, "BBB": 0.3})
        self.assertEqual(signal["target_cash_weight"], 0.1)
        self.assertAlmostEqual(sum(signal["target_weights"].values()) + signal["target_cash_weight"], 1.0)

    def test_modeled_execution_uses_only_real_next_session_opens(self):
        dates = pd.to_datetime(["2026-07-17", "2026-07-20"])
        opens = pd.DataFrame({"AAA": [100.0, 111.0], "BBB": [200.0, np.nan]}, index=dates)
        closes = pd.DataFrame({"AAA": [105.0, 115.0], "BBB": [205.0, 225.0]}, index=dates)

        execution_date, execution_open = shadow.modeled_execution(
            opens, closes, pd.Timestamp("2026-07-17"), ["AAA", "BBB"]
        )

        self.assertEqual(execution_date, "2026-07-20")
        self.assertEqual(execution_open, {"AAA": 111.0})
        self.assertNotIn("BBB", execution_open)  # never substitute its D+1 close

    def test_latest_close_signal_remains_actionable_without_a_future_open(self):
        dates = pd.to_datetime(["2026-07-16", "2026-07-17"])
        opens = pd.DataFrame({"AAA": [100.0, 101.0]}, index=dates)
        closes = pd.DataFrame({"AAA": [105.0, 106.0]}, index=dates)

        execution_date, execution_open = shadow.modeled_execution(
            opens, closes, pd.Timestamp("2026-07-17"), ["AAA"]
        )

        self.assertIsNone(execution_date)
        self.assertIsNone(execution_open)

    def test_atomic_writer_preserves_prior_output_when_replace_fails(self):
        with tempfile.TemporaryDirectory() as td:
            output = Path(td) / "signal.json"
            output.write_text('{"old": true}\n')

            with mock.patch("scripts.atomic_io.os.replace", side_effect=OSError("replace failed")):
                with self.assertRaisesRegex(OSError, "replace failed"):
                    atomic_write_json(output, {"new": True})

            self.assertEqual(json.loads(output.read_text()), {"old": True})
            self.assertEqual(list(Path(td).iterdir()), [output])

    def test_latest_row_with_narrow_coverage_is_rejected(self):
        columns = [f"SYM_{i}" for i in range(100)]
        prices = pd.DataFrame(
            [np.full(100, 100.0), np.array([101.0, 102.0] + [np.nan] * 98)],
            index=pd.to_datetime(["2026-07-16", "2026-07-17"]),
            columns=columns,
        )

        error = shadow.signal_data_quality_error(
            prices,
            picks=[f"SYM_{i}" for i in range(8)],
            top_n=8,
            min_fresh_symbols=50,
            min_fresh_coverage=0.8,
            as_of=pd.Timestamp("2026-07-17"),
        )

        self.assertIn("fresh symbols", error)
        self.assertIn("2/100", error)

    def test_incomplete_or_duplicate_picks_are_rejected(self):
        prices = pd.DataFrame(
            np.full((2, 60), 100.0),
            index=pd.to_datetime(["2026-07-16", "2026-07-17"]),
            columns=[f"SYM_{i}" for i in range(60)],
        )

        incomplete = shadow.signal_data_quality_error(
            prices, picks=["SYM_0", "SYM_1"], top_n=8,
            min_fresh_symbols=50, min_fresh_coverage=0.8,
            as_of=pd.Timestamp("2026-07-17"),
        )
        duplicate = shadow.signal_data_quality_error(
            prices, picks=["SYM_0"] * 8, top_n=8,
            min_fresh_symbols=50, min_fresh_coverage=0.8,
            as_of=pd.Timestamp("2026-07-17"),
        )

        self.assertIn("exactly 8 unique picks", incomplete)
        self.assertIn("exactly 8 unique picks", duplicate)

    def test_all_symbols_on_an_old_latest_date_are_rejected(self):
        prices = pd.DataFrame(
            np.full((2, 60), 100.0),
            index=pd.to_datetime(["2026-06-23", "2026-06-24"]),
            columns=[f"SYM_{i}" for i in range(60)],
        )

        error = shadow.signal_data_quality_error(
            prices,
            picks=[f"SYM_{i}" for i in range(8)],
            top_n=8,
            min_fresh_symbols=50,
            min_fresh_coverage=0.8,
            max_data_age_days=5,
            as_of=pd.Timestamp("2026-07-17"),
        )

        self.assertIn("latest price date 2026-06-24 is stale", error)

    def test_healthy_complete_candidate_is_accepted(self):
        prices = pd.DataFrame(
            np.full((2, 60), 100.0),
            index=pd.to_datetime(["2026-07-16", "2026-07-17"]),
            columns=[f"SYM_{i}" for i in range(60)],
        )
        picks = [f"SYM_{i}" for i in range(8)]

        self.assertIsNone(shadow.signal_data_quality_error(
            prices, picks=picks, top_n=8,
            min_fresh_symbols=50, min_fresh_coverage=0.8,
            as_of=pd.Timestamp("2026-07-17"),
        ))

    def test_main_preserves_existing_output_when_candidate_is_invalid(self):
        with tempfile.TemporaryDirectory() as td:
            out_dir = Path(td)
            output = out_dir / "paper_shadow_rsi_momentum_latest.json"
            sentinel = {"latest_signal": {"date": "2026-06-24", "picks": [f"OLD_{i}" for i in range(8)]}}
            output.write_text(json.dumps(sentinel))
            prices = pd.DataFrame(
                np.full((2, 60), 100.0),
                index=pd.to_datetime(["2026-07-16", "2026-07-17"]),
                columns=[f"SYM_{i}" for i in range(60)],
            )
            invalid = {"error": "fresh symbols 2/60 below safety threshold"}

            with mock.patch.object(shadow, "OUT_DIR", out_dir), \
                 mock.patch.object(shadow, "find_hist_dir", return_value=td), \
                 mock.patch.object(
                     shadow,
                     "load_market_data",
                     return_value={"open": pd.DataFrame(), "close": prices},
                 ), \
                 mock.patch.object(shadow, "compute_rotation", return_value=invalid):
                rc = shadow.main()

            self.assertEqual(rc, 1)
            self.assertEqual(json.loads(output.read_text()), sentinel)


if __name__ == "__main__":
    unittest.main()
