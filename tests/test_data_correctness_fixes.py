"""Regression tests for the paper-trader data-correctness fixes.

Covers:
  1. rejection of synthetic zero-volume flat holiday rows in the loader
  2. rejection of synthetic union dates (single-symbol vendor calendar padding)
  3. raw-vs-filled freshness: the bounded ffill cannot mask a stale cache
  4. trading-day-aware, weekend/synthetic-date rejection in the freshness guard
  5. explicit Instruments.feather status (sector cap / mcap filter not silent)
  6. clear universe/pass counts in published diagnostics
  7. ledger stale-signal protection
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.rsi_224466_rotation_lab import (
    drop_zero_volume_flat_rows,
    load_ohlc_prices,
)
from scripts import rsi_momentum_paper_shadow as shadow
from scripts import rsi_momentum_paper_ledger as ledger


# ── 1. synthetic zero-volume flat holiday rows ─────────────────


def test_drop_zero_volume_flat_rows_removes_holiday_padding():
    df = pd.DataFrame(
        {
            "date": pd.to_datetime(
                ["2026-09-29", "2026-09-30", "2026-10-01", "2026-10-02"]
            ),
            "open": [100.0, 101.0, 102.0, 102.0],
            "close": [101.0, 102.0, 103.0, 103.0],
            "volume": [1000, 1200, 1100, 0],
        }
    )
    cleaned, dropped = drop_zero_volume_flat_rows(df)
    assert dropped == 1
    assert pd.Timestamp("2026-10-02") not in set(cleaned["date"])
    assert len(cleaned) == 3


def test_zero_volume_but_moved_close_is_kept():
    # A real (if odd) session with volume 0 but a different close must survive;
    # the drop rule requires BOTH zero volume AND a flat close.
    df = pd.DataFrame(
        {
            "date": pd.to_datetime(["2026-09-30", "2026-10-01"]),
            "close": [100.0, 105.0],
            "volume": [500, 0],
        }
    )
    cleaned, dropped = drop_zero_volume_flat_rows(df)
    assert dropped == 0
    assert len(cleaned) == 2


def test_real_flat_session_with_volume_is_kept():
    df = pd.DataFrame(
        {
            "date": pd.to_datetime(["2026-09-30", "2026-10-01"]),
            "close": [100.0, 100.0],
            "volume": [500, 700],
        }
    )
    cleaned, dropped = drop_zero_volume_flat_rows(df)
    assert dropped == 0
    assert len(cleaned) == 2


def test_files_without_volume_column_are_untouched():
    df = pd.DataFrame(
        {
            "date": pd.to_datetime(["2026-09-30", "2026-10-01", "2026-10-02"]),
            "close": [100.0, 100.0, 100.0],
        }
    )
    cleaned, dropped = drop_zero_volume_flat_rows(df)
    assert dropped == 0
    assert len(cleaned) == 3


def test_loader_rejects_etf_holiday_row_and_reports_diagnostics(tmp_path):
    # Production scenario: four "clean" symbols whose real history ends Thu
    # 2026-10-01, plus ETF feathers carrying a trailing zero-volume flat row
    # for Fri 2026-10-02 (a holiday the market never traded). Without the fix,
    # 2026-10-02 became the matrix's "latest" synthetic session.
    dates = pd.bdate_range("2026-09-21", periods=9)  # ends Thu 2026-10-01
    synthetic_day = pd.Timestamp("2026-10-02")
    rng = np.random.default_rng(7)
    for sym in ["AAA", "BBB", "CCC", "DDD"]:
        close = 100 + np.cumsum(rng.normal(0, 0.5, len(dates)))
        pd.DataFrame(
            {
                "date": dates,
                "open": close - 0.1,
                "close": close,
                "volume": [100000] * len(dates),
            }
        ).to_feather(tmp_path / f"{sym}.feather")

    etf = pd.DataFrame(
        {
            "date": list(dates) + [synthetic_day],
            "open": list(50 + np.cumsum(rng.normal(0, 0.2, len(dates)))) + [50.0],
            "close": list(50 + np.cumsum(rng.normal(0, 0.2, len(dates)))) + [51.0],
            "volume": [50000] * len(dates) + [0],
        }
    )
    # force the synthetic row flat vs its own previous close
    etf.loc[etf.index[-1], "close"] = etf.loc[etf.index[-2], "close"]
    etf.loc[etf.index[-1], "open"] = etf.loc[etf.index[-2], "close"]
    etf.to_feather(tmp_path / "GOLDBEES.feather")

    ohlc, ctx = load_ohlc_prices(
        tmp_path, min_rows=5, min_end_date="", symbols=None, max_symbols=0
    )
    # The synthetic row is dropped at the file level...
    assert ctx["synthetic_rows_dropped"] == 1
    assert ctx["synthetic_rows_by_symbol"] == {"GOLDBEES": 1}
    # ...and the resulting single-symbol date never becomes the matrix latest.
    assert ctx["date_range"][1] == "2026-10-01"
    assert ohlc["close"].index.max() == pd.Timestamp("2026-10-01")
    assert ohlc["close"].index[-1].dayofweek < 5


# ── 2. synthetic union dates ────────────────────────────────────


def test_union_date_observed_by_one_symbol_is_rejected(tmp_path):
    # AAA/BBB/CCC share a 10-session calendar; ONE symbol has an extra
    # eleventh date (vendor calendar padding). Union coverage for that date is
    # 1/4 < 0.5 → rejected.
    dates = pd.bdate_range("2026-09-21", periods=10)
    extra = dates[-1] + pd.Timedelta(days=3)  # a lone extra session
    for sym, idx in [("AAA", dates), ("BBB", dates), ("CCC", dates), ("DDD", list(dates) + [extra])]:
        close = np.linspace(100, 110, len(idx))
        pd.DataFrame(
            {"date": idx, "open": close - 0.1, "close": close, "volume": [1000] * len(idx)}
        ).to_feather(tmp_path / f"{sym}.feather")

    ohlc, ctx = load_ohlc_prices(
        tmp_path, min_rows=5, min_end_date="", symbols=None, max_symbols=0
    )
    assert str(extra.date()) in ctx["union_dates_dropped"]
    assert ctx["union_dates_dropped_count"] == 1
    assert extra not in ohlc["close"].index
    assert ohlc["close"].index.max() == dates[-1]


# ── 3/4. freshness guard: raw-vs-filled and trading-day awareness ──


def _matrix(dates, n=60, fill_last=False):
    data = np.full((len(dates), n), 100.0)
    return pd.DataFrame(data, index=pd.DatetimeIndex(dates), columns=[f"SYM_{i}" for i in range(n)])


def test_filled_matrix_cannot_mask_stale_raw_data():
    raw_dates = pd.bdate_range(end="2026-09-25", periods=3)  # stale: ends Thursday
    raw = _matrix(raw_dates)
    filled_dates = list(raw_dates) + [pd.Timestamp("2026-09-28")]  # ffill fabricated Monday
    filled = _matrix(filled_dates)

    error = shadow.signal_data_quality_error(
        filled,
        picks=[f"SYM_{i}" for i in range(8)],
        top_n=8,
        min_fresh_symbols=50,
        min_fresh_coverage=0.8,
        max_data_age_days=0,
        as_of=pd.Timestamp("2026-09-28"),
        raw_prices=raw,
    )
    assert error is not None
    assert "stale" in error
    assert "2026-09-25" in error

    # Without raw data the filled matrix would have passed — that was the bug.
    assert (
        shadow.signal_data_quality_error(
            filled,
            picks=[f"SYM_{i}" for i in range(8)],
            top_n=8,
            min_fresh_symbols=50,
            min_fresh_coverage=0.8,
            max_data_age_days=0,
            as_of=pd.Timestamp("2026-09-28"),
        )
        is None
    )


def test_weekend_latest_date_is_rejected_as_synthetic():
    weekend = _matrix([pd.Timestamp("2026-10-03"), pd.Timestamp("2026-10-04")])  # Sat/Sun
    error = shadow.signal_data_quality_error(
        weekend,
        picks=[f"SYM_{i}" for i in range(8)],
        top_n=8,
        as_of=pd.Timestamp("2026-10-05"),
    )
    assert error is not None
    assert "not a trading day" in error


def test_age_is_measured_in_trading_days_not_calendar_days():
    # Friday data checked on Monday: 1 trading day old, 3 calendar days old.
    prices = _matrix([pd.Timestamp("2026-10-02"), pd.Timestamp("2026-10-05")])
    assert (
        shadow.signal_data_quality_error(
            _matrix([pd.Timestamp("2026-09-30"), pd.Timestamp("2026-10-02")]),
            picks=[f"SYM_{i}" for i in range(8)],
            top_n=8,
            max_data_age_days=1,
            as_of=pd.Timestamp("2026-10-05"),
        )
        is None  # Fri→Mon is 1 trading day, within the 1-day budget
    )
    # 2 trading days stale (Wed data checked Fri) exceeds a 1-day budget even
    # though only 2 calendar days passed.
    error = shadow.signal_data_quality_error(
        _matrix([pd.Timestamp("2026-09-29"), pd.Timestamp("2026-09-30")]),
        picks=[f"SYM_{i}" for i in range(8)],
        top_n=8,
        max_data_age_days=1,
        as_of=pd.Timestamp("2026-10-02"),
    )
    assert error is not None
    assert "2 trading days" in error
    assert prices is not None  # (first matrix only used to document the setup)


# ── 5. Instruments.feather status is explicit ───────────────────


def test_instruments_status_reports_missing_file(tmp_path, monkeypatch):
    monkeypatch.setattr(shadow, "INSTRUMENTS_FILE", tmp_path / "absent.feather")
    status = shadow.instruments_status()
    assert status["available"] is False
    assert status["sector_cap_enforced"] is False
    assert status["mcap_filter_enforced"] is False
    assert "DISABLED" in status["warning"]


def test_instruments_status_reports_active_constraints(tmp_path, monkeypatch):
    path = tmp_path / "Instruments.feather"
    pd.DataFrame(
        {"Symbol": ["AAA"], "Sector": ["IT"], "MarketCapCr": [10000.0]}
    ).to_feather(path)
    monkeypatch.setattr(shadow, "INSTRUMENTS_FILE", path)
    status = shadow.instruments_status()
    assert status["available"] is True
    assert status["sector_cap_enforced"] is True
    assert status["mcap_filter_enforced"] is True
    assert status["warning"] is None


# ── 6. published diagnostics: universe counts ───────────────────


def test_compute_rotation_publishes_universe_counts_and_loader_diagnostics(monkeypatch, tmp_path):
    end = pd.Timestamp.today().normalize()
    end = end - pd.tseries.offsets.BDay(0)  # roll back to a business day
    dates = pd.bdate_range(end=end, periods=760)
    rng = np.random.default_rng(3)
    prices = pd.DataFrame(
        100 * np.exp(np.cumsum(rng.normal(0.0005, 0.01, (len(dates), 60)), axis=0)),
        index=dates,
        columns=[f"SYM{i:02d}" for i in range(60)],
    )
    opens = prices.shift(1).fillna(prices.iloc[0])

    monkeypatch.setattr(shadow, "INSTRUMENTS_FILE", tmp_path / "absent.feather")

    result = shadow.compute_rotation(
        prices,
        opens,
        raw_prices=prices,
        loader_context={"synthetic_rows_dropped": 3, "synthetic_rows_by_symbol": {"X": 3}},
    )
    assert "error" not in result, result.get("error")
    meta = result["metadata"]
    diag = meta["latest_signal_diagnostics"]
    assert diag["symbols_loaded"] == 60
    assert diag["symbols_screened"] == 60  # post-universe-filter count, not post-momentum
    assert diag["symbols_passing_filters"] <= 60
    assert meta["instruments"]["available"] is False
    assert meta["instruments"]["sector_cap_enforced"] is False
    assert meta["loader_diagnostics"]["synthetic_rows_dropped"] == 3
    assert meta["loader_diagnostics"]["raw_latest_date"] == str(dates[-1].date())
    assert len(result["target_weights"]) == 8


# ── 7. ledger stale-signal protection ───────────────────────────


def test_ledger_rejects_signal_stale_by_trading_days():
    error = ledger.signal_staleness_error(
        "2026-09-21", "2026-10-05", max_age_trading_days=7
    )
    assert error is not None
    assert "stale" in error
    assert "trading days" in error


def test_ledger_accepts_recent_signal_across_weekend():
    # Saturday shadow run → Monday ledger tick: 0 trading-day age from Fri data.
    assert (
        ledger.signal_staleness_error(
            "2026-10-02", "2026-10-05", max_age_trading_days=7
        )
        is None
    )


def test_ledger_rejects_future_dated_signal():
    error = ledger.signal_staleness_error("2026-10-06", "2026-10-05")
    assert error is not None
    assert "future" in error


def test_ledger_staleness_check_disabled_with_zero_budget():
    assert (
        ledger.signal_staleness_error(
            "2020-01-01", "2026-10-05", max_age_trading_days=0
        )
        is None
    )


def test_ledger_load_prices_drops_synthetic_holiday_rows(tmp_path):
    dates = pd.bdate_range("2024-01-01", periods=360)
    close = np.linspace(100, 120, len(dates))
    df = pd.DataFrame(
        {
            "date": list(dates) + [dates[-1] + pd.Timedelta(days=1)],
            "close": list(close) + [close[-1]],
            "volume": [1000] * len(dates) + [0],
        }
    )
    df.to_feather(tmp_path / "AAA.feather")
    prices = ledger.load_prices(tmp_path, min_rows=100)
    assert prices.index.max() == dates[-1]
    assert prices.index[-1].dayofweek < 5
