from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.signal_schema import build_paper_signal

FIXTURES = Path(__file__).parent / "fixtures"


def test_local_signal_builder_matches_versioned_golden_contract() -> None:
    ohlc = json.loads((FIXTURES / "parity_ohlc.json").read_text())
    expected = json.loads((FIXTURES / "expected_signal_v2.json").read_text())
    actual = build_paper_signal(
        params={"top_n": 2, "vol_weight": True, "vol_lookback": 10},
        signal_date=ohlc["signal_date"],
        target_weights={"AAA": 0.6, "BBB": 0.3},
        target_cash_weight=0.1,
        modeled_execution_date=ohlc["modeled_execution_date"],
        modeled_execution_open=ohlc["modeled_opens"],
        metadata={"weighting_method": "inverse_volatility", "vol_lookback": 10},
    )
    assert actual == expected
