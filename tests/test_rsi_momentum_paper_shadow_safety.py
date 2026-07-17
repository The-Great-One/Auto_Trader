import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import numpy as np
import pandas as pd

from scripts import rsi_momentum_paper_shadow as shadow


class ShadowPublicationSafetyTests(unittest.TestCase):
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
                 mock.patch.object(shadow, "load_hist", return_value=prices), \
                 mock.patch.object(shadow, "compute_rotation", return_value=invalid):
                rc = shadow.main()

            self.assertEqual(rc, 1)
            self.assertEqual(json.loads(output.read_text()), sentinel)


if __name__ == "__main__":
    unittest.main()
