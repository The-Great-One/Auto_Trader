from __future__ import annotations

import math
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.rsi_224466_rotation_lab import load_ohlc_prices, metrics, rsi_dataframe


def _write_feather(path, rows):
    pd.DataFrame(rows).to_feather(path)


def test_load_ohlc_prices_aligns_real_open_and_close_after_timezone_normalization(tmp_path):
    _write_feather(
        tmp_path / "aaa.feather",
        {
            "DaTe": ["2024-01-02T09:15:00+05:30", "2024-01-03T09:15:00+05:30"],
            "OpEn": [100.0, 110.0],
            "ClOsE": [105.0, 115.0],
        },
    )
    _write_feather(
        tmp_path / "bbb.feather",
        {
            "DATE": [pd.Timestamp("2024-01-02"), pd.Timestamp("2024-01-04")],
            "OPEN": [200.0, 220.0],
            "CLOSE": [205.0, 225.0],
        },
    )

    ohlc, context = load_ohlc_prices(tmp_path, min_rows=2, min_end_date="", symbols=None, max_symbols=0)

    assert list(ohlc) == ["open", "close"]
    assert ohlc["open"].index.tz is None
    assert ohlc["open"].loc[pd.Timestamp("2024-01-02 03:45"), "AAA"] == 100.0
    assert ohlc["close"].loc[pd.Timestamp("2024-01-04"), "BBB"] == 225.0
    assert context["symbols_loaded"] == 2
    assert context["skipped"]["missing_ohlc"] == 0


def test_load_ohlc_prices_deduplicates_normalized_dates_by_last_observation(tmp_path):
    _write_feather(
        tmp_path / "aaa.feather",
        {
            "date": ["2024-01-02T00:00:00Z", "2024-01-02T00:00:00+00:00", "2024-01-03T00:00:00Z"],
            "open": [90.0, 100.0, 110.0],
            "close": [95.0, 105.0, 115.0],
        },
    )

    ohlc, context = load_ohlc_prices(tmp_path, min_rows=2, min_end_date="", symbols=None, max_symbols=0)

    assert ohlc["open"].loc[pd.Timestamp("2024-01-02"), "AAA"] == 100.0
    assert context["duplicate_rows_dropped"] == 1


def test_load_ohlc_prices_never_synthesizes_open_from_close(tmp_path):
    _write_feather(
        tmp_path / "aaa.feather",
        {"date": pd.date_range("2024-01-01", periods=2), "close": [100.0, 101.0]},
    )

    with pytest.raises(SystemExit, match="No usable symbols"):
        load_ohlc_prices(tmp_path, min_rows=2, min_end_date="", symbols=None, max_symbols=0)


def test_rsi_dataframe_has_explicit_monotonic_edges():
    dates = pd.bdate_range("2024-01-01", periods=6)
    prices = pd.DataFrame({"UP": [1, 2, 3, 4, 5, 6], "DOWN": [6, 5, 4, 3, 2, 1]}, index=dates)

    result = rsi_dataframe(prices, period=5)

    assert result.loc[dates[-1], "UP"] == 100.0
    assert result.loc[dates[-1], "DOWN"] == 0.0


def test_metrics_contract_reports_active_cagr_and_calendar_inclusive_xirr():
    dates = pd.bdate_range("2024-01-01", periods=5)
    returns = pd.Series([0.10, 0.0, 0.0, 0.0, 0.0], index=dates)
    weights = pd.DataFrame({"AAA": [1.0, 0.0, 0.0, 0.0, 0.0]}, index=dates)
    turnover = pd.Series([1.0, 1.0, 0.0, 0.0, 0.0], index=dates)

    result = metrics(
        "cash_gap_case",
        returns,
        weights,
        turnover,
        {"rebalance": "ME", "top_n": 1, "cost_bps": 0, "regime": "test", "symbols_loaded": 1},
    )

    active_only_years = 1 / 252
    full_calendar_years = 5 / 252
    expected_active_cagr = ((1.10 ** (1 / active_only_years)) - 1) * 100
    expected_xirr = ((1.10 ** (1 / full_calendar_years)) - 1) * 100

    assert result.cagr_pct == round(expected_active_cagr, 2)
    assert result.xirr_pct == round(expected_xirr, 2)
    assert result.xirr_pct < result.cagr_pct
    assert math.isfinite(result.xirr_pct)
