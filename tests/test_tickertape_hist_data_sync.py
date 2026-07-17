import tempfile
import unittest
from pathlib import Path

import pandas as pd

from scripts import tickertape_hist_data_sync as sync


class TickertapeHistDataSyncTests(unittest.TestCase):
    def test_sid_map_from_screener_extracts_all_exact_tickers(self):
        payload = {
            "results": [
                {"sid": "RELI", "stock": {"info": {"ticker": "RELIANCE"}}},
                {"sid": "INFY", "stock": {"info": {"ticker": "INFY"}}},
            ]
        }
        self.assertEqual(
            sync.sid_map_from_screener(payload),
            {"RELIANCE": "RELI", "INFY": "INFY"},
        )

    def test_sid_map_from_screener_omits_ambiguous_tickers(self):
        payload = {
            "results": [
                {"sid": "ONE", "stock": {"info": {"ticker": "FOCUS"}}},
                {"sid": "TWO", "stock": {"info": {"ticker": "FOCUS"}}},
                {"sid": "INFY", "stock": {"info": {"ticker": "INFY"}}},
            ]
        }
        self.assertEqual(sync.sid_map_from_screener(payload), {"INFY": "INFY"})

    def test_select_exact_stock_sid_requires_exact_ticker(self):
        payload = {
            "stocks": [
                {"ticker": "CAPLIPOINT", "sid": "CAPL"},
                {"ticker": "CAPLIN", "sid": "OTHER"},
            ]
        }
        self.assertEqual(sync.select_exact_stock_sid("caplipoint", payload), "CAPL")
        self.assertIsNone(sync.select_exact_stock_sid("MISSING", payload))

    def test_select_exact_stock_sid_rejects_ambiguous_matches(self):
        payload = {
            "stocks": [
                {"ticker": "ABC", "sid": "ONE"},
                {"ticker": "ABC", "sid": "TWO"},
            ]
        }
        with self.assertRaises(sync.TickertapeDataError):
            sync.select_exact_stock_sid("ABC", payload)

    def test_chart_to_frame_validates_sid_and_builds_daily_close_rows(self):
        raw = [{
            "sid": "CAPL",
            "points": [
                {"ts": "2026-07-15T00:00:00.000Z", "lp": 2600.0, "v": 100000},
                {"ts": "2026-07-16T00:00:00.000Z", "lp": 2671.5, "v": 136463},
            ],
        }]
        frame = sync.chart_to_frame(
            raw,
            expected_sid="CAPL",
            as_of=pd.Timestamp("2026-07-17"),
            max_age_days=5,
            min_points=2,
        )
        self.assertEqual(list(frame.columns), ["Date", "Open", "High", "Low", "Close", "Volume"])
        self.assertEqual(frame["Close"].tolist(), [2600.0, 2671.5])
        self.assertEqual(frame["Volume"].tolist(), [100000.0, 136463.0])
        self.assertTrue((frame[["Open", "High", "Low"]].to_numpy() == frame[["Close", "Close", "Close"]].to_numpy()).all())

        with self.assertRaisesRegex(sync.TickertapeDataError, "SID mismatch"):
            sync.chart_to_frame(
                raw,
                expected_sid="WRONG",
                as_of=pd.Timestamp("2026-07-17"),
                max_age_days=5,
                min_points=2,
            )

    def test_chart_to_frame_rejects_stale_or_short_data(self):
        stale = [{"sid": "CAPL", "points": [
            {"ts": "2026-06-01T00:00:00.000Z", "lp": 2500, "v": 100},
            {"ts": "2026-06-02T00:00:00.000Z", "lp": 2510, "v": 200},
        ]}]
        with self.assertRaisesRegex(sync.TickertapeDataError, "stale"):
            sync.chart_to_frame(
                stale, expected_sid="CAPL", as_of=pd.Timestamp("2026-07-17"),
                max_age_days=5, min_points=2,
            )

        fresh_but_short = [{"sid": "CAPL", "points": [
            {"ts": "2026-07-16T00:00:00.000Z", "lp": 2500, "v": 100},
        ]}]
        with self.assertRaisesRegex(sync.TickertapeDataError, "only 1 points"):
            sync.chart_to_frame(
                fresh_but_short, expected_sid="CAPL", as_of=pd.Timestamp("2026-07-17"),
                max_age_days=5, min_points=2,
            )

    def test_small_universe_coverage_gate_never_requires_more_than_total(self):
        self.assertEqual(sync.required_success_count(3, 0.66), 3)
        self.assertEqual(sync.required_success_count(100, 0.8), 80)

    def test_output_history_defaults_to_tickertape_only(self):
        seed = pd.DataFrame({
            "Date": pd.to_datetime(["2024-01-01"]),
            "Close": [100.0],
        })
        recent = pd.DataFrame({
            "Date": pd.to_datetime(["2026-07-15", "2026-07-16"]),
            "Open": [105.0, 106.0], "High": [105.0, 106.0],
            "Low": [105.0, 106.0], "Close": [105.0, 106.0],
            "Volume": [3000.0, 4000.0],
        })
        pure = sync.compose_output_history(seed, recent, include_seed_history=False)
        hybrid = sync.compose_output_history(seed, recent, include_seed_history=True)
        self.assertEqual(len(pure), 2)
        self.assertEqual(len(hybrid), 3)
        self.assertEqual(pure["Date"].min(), pd.Timestamp("2026-07-15"))

    def test_merge_with_seed_overlays_tickertape_and_preserves_history(self):
        seed = pd.DataFrame({
            "Date": pd.to_datetime(["2024-01-01", "2026-07-15"]),
            "Open": [90.0, 99.0], "High": [110.0, 101.0],
            "Low": [80.0, 98.0], "Close": [100.0, 100.0],
            "Volume": [1000.0, 2000.0],
        })
        latest = pd.DataFrame({
            "Date": pd.to_datetime(["2026-07-15", "2026-07-16"]),
            "Open": [105.0, 106.0], "High": [105.0, 106.0],
            "Low": [105.0, 106.0], "Close": [105.0, 106.0],
            "Volume": [3000.0, 4000.0],
        })
        merged = sync.merge_with_seed(seed, latest)
        self.assertEqual(merged["Date"].dt.strftime("%Y-%m-%d").tolist(), ["2024-01-01", "2026-07-15", "2026-07-16"])
        self.assertEqual(float(merged.loc[merged["Date"] == pd.Timestamp("2026-07-15"), "Close"].iloc[0]), 105.0)
        self.assertFalse(merged["Date"].duplicated().any())


if __name__ == "__main__":
    unittest.main()
