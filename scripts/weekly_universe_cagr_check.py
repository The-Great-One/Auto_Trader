#!/usr/bin/env python3
"""Weekly 5 year CAGR check for the current production equity strategy."""

from __future__ import annotations

import importlib
import json
import os
import sys
from collections import Counter
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from statistics import median

import pandas as pd

os.environ.setdefault("AT_DISABLE_FILE_LOGGING", "1")
os.environ.setdefault("AT_LAB_HISTORY_PERIOD", os.getenv("AT_WEEKLY_CAGR_HISTORY_PERIOD", "5y"))
os.environ.setdefault("AT_LAB_MIN_BARS", os.getenv("AT_WEEKLY_CAGR_MIN_BARS", "1000"))

ROOT = Path(__file__).resolve().parents[1]
REPORTS = ROOT / "reports"
STATUS_DIR = ROOT / "intermediary_files" / "lab_status"
STATUS_PATH = STATUS_DIR / "weekly_universe_cagr_status.json"
REPORTS.mkdir(exist_ok=True)
STATUS_DIR.mkdir(exist_ok=True)

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from Auto_Trader import utils as at_utils  # noqa: E402
from Auto_Trader.StrongFundamentalsStockList import goodStocks  # noqa: E402

lab = importlib.import_module("scripts.weekly_strategy_lab")


def write_status(**updates) -> dict:
    current = {}
    if STATUS_PATH.exists():
        try:
            current = json.loads(STATUS_PATH.read_text())
        except Exception:
            current = {}
    current.update(updates)
    current["updated_at"] = datetime.now().isoformat()
    STATUS_PATH.write_text(json.dumps(current, indent=2))
    return current


def iso_week_key(now: datetime) -> str:
    iso = now.isocalendar()
    return f"{iso.year}_W{iso.week:02d}"


def build_universe(limit: int | None = None) -> tuple[list[str], dict]:
    df = goodStocks()
    if df is None or df.empty:
        raise RuntimeError("Strong fundamentals universe is empty")

    df["Symbol"] = df["Symbol"].astype(str).str.upper().str.strip()
    df["AssetClass"] = df["AssetClass"].astype(str).str.upper().str.strip()
    include_etfs = os.getenv("AT_WEEKLY_CAGR_INCLUDE_ETFS", "1").strip().lower() not in {"0", "false", "no"}

    if not include_etfs:
        df = df[df["AssetClass"] == "EQUITY"].copy()

    symbols = df["Symbol"].dropna().unique().tolist()
    if limit is not None:
        symbols = symbols[: max(1, int(limit))]
        df = df[df["Symbol"].isin(symbols)].copy()

    counts = df["AssetClass"].value_counts(dropna=False).to_dict()
    return symbols, {
        "include_etfs": include_etfs,
        "requested_symbols": len(symbols),
        "asset_class_counts": counts,
        "sample_symbols": symbols[:20],
    }


def load_data(symbols: list[str], min_history_bars: int) -> tuple[dict[str, pd.DataFrame], dict]:
    data_map: dict[str, pd.DataFrame] = {}
    skipped: dict[str, str] = {}
    spans_years: list[float] = []
    total = max(1, len(symbols))

    for idx, symbol in enumerate(symbols, start=1):
        write_status(
            status="running",
            phase="loading_history",
            current_symbol=symbol,
            symbols_total=total,
            symbols_index=idx,
            symbols_loaded=len(data_map),
            progress_pct=round(((idx - 1) / total) * 100.0, 1),
        )
        df = lab._load_symbol_history(symbol)
        if df is None or df.empty:
            skipped[symbol] = "missing_or_empty"
            continue
        if len(df) < min_history_bars:
            skipped[symbol] = f"too_short:{len(df)}"
            continue
        try:
            ind = at_utils.Indicators(df)
            data_map[symbol] = ind
            span_years = (pd.to_datetime(ind["Date"].iloc[-1]) - pd.to_datetime(ind["Date"].iloc[0])).days / 365.25
            spans_years.append(float(span_years))
        except Exception as exc:
            skipped[symbol] = f"indicator_failed:{exc}"

    skip_reasons = Counter(reason.split(":", 1)[0] for reason in skipped.values())
    return data_map, {
        "loaded_symbols": list(data_map.keys()),
        "skipped_symbols": skipped,
        "skip_reason_counts": dict(skip_reasons),
        "median_span_years": round(float(median(spans_years)), 3) if spans_years else 0.0,
        "min_span_years": round(float(min(spans_years)), 3) if spans_years else 0.0,
        "max_span_years": round(float(max(spans_years)), 3) if spans_years else 0.0,
    }


def build_payload(now: datetime, universe_meta: dict, data_context: dict, result) -> dict:
    start_capital = 100000.0 * max(1, len(result.symbols_tested))
    years = float(data_context.get("median_span_years") or 0.0)
    ending_value = float(result.final_value)
    cagr_pct = None
    if years > 0 and start_capital > 0 and ending_value > 0:
        cagr_pct = round((((ending_value / start_capital) ** (1.0 / years)) - 1.0) * 100.0, 2)

    report_week = os.getenv("AT_WEEKLY_CAGR_REPORT_WEEK", iso_week_key(now))
    coverage_pct = round((len(result.symbols_tested) / max(1, universe_meta["requested_symbols"])) * 100.0, 2)

    return {
        "generated_at": now.isoformat(),
        "report_week": report_week,
        "production_rule_model": "BUY=RULE_SET_7, SELL=RULE_SET_2",
        "history_period": os.getenv("AT_LAB_HISTORY_PERIOD", "5y"),
        "min_history_bars": int(os.getenv("AT_LAB_MIN_BARS", "1000")),
        "universe": {
            **universe_meta,
            "tested_symbols": len(result.symbols_tested),
            "coverage_pct": coverage_pct,
        },
        "data_context": {
            **data_context,
            "loaded_symbols_count": len(data_context.get("loaded_symbols", [])),
            "skipped_symbols_count": len(data_context.get("skipped_symbols", {})),
            "loaded_symbols_sample": data_context.get("loaded_symbols", [])[:25],
        },
        "backtest": asdict(result),
        "annualized_years": round(years, 3),
        "cagr_pct": cagr_pct,
    }


def write_reports(payload: dict) -> tuple[Path, Path]:
    week_key = payload["report_week"]
    out_json = REPORTS / f"weekly_universe_cagr_{week_key}.json"
    out_md = REPORTS / f"weekly_universe_cagr_{week_key}.md"

    out_json.write_text(json.dumps(payload, indent=2))

    backtest = payload["backtest"]
    universe = payload["universe"]
    data_context = payload["data_context"]
    lines = [
        f"# Weekly Universe CAGR Check, {payload['report_week']}",
        "",
        f"- Generated at: **{payload['generated_at']}**",
        f"- Rule model: **{payload['production_rule_model']}**",
        f"- History period: **{payload['history_period']}**",
        f"- Min history bars: **{payload['min_history_bars']}**",
        f"- Requested universe: **{universe['requested_symbols']}**",
        f"- Tested symbols: **{universe['tested_symbols']}** ({universe['coverage_pct']}%)",
        f"- Asset classes: **{universe['asset_class_counts']}**",
        f"- Median history span: **{payload['annualized_years']} years**",
        f"- Strategy total return: **{backtest['total_return_pct']}%**",
        f"- Strategy CAGR: **{payload['cagr_pct']}%**",
        f"- Max drawdown: **{backtest['max_drawdown_pct']}%**",
        f"- Trades: **{backtest['trades']}**",
        f"- Win rate: **{backtest['win_rate_pct']}%**",
        "",
        "## Skips",
        f"- Skip reasons: **{data_context['skip_reason_counts']}**",
    ]
    out_md.write_text("\n".join(lines) + "\n")
    return out_json, out_md


def main() -> int:
    now = datetime.now()
    limit_raw = os.getenv("AT_WEEKLY_CAGR_LIMIT", "").strip()
    limit = int(limit_raw) if limit_raw else None
    min_history_bars = lab.configured_min_history_bars(default=1000)

    write_status(
        status="running",
        phase="building_universe",
        message="building current strong-fundamentals universe",
        history_period=os.getenv("AT_LAB_HISTORY_PERIOD", "5y"),
        min_history_bars=min_history_bars,
    )
    symbols, universe_meta = build_universe(limit=limit)
    write_status(
        status="running",
        phase="loading_history",
        message="loading price history for current universe",
        requested_symbols=universe_meta["requested_symbols"],
        asset_class_counts=universe_meta["asset_class_counts"],
    )
    data_map, data_context = load_data(symbols, min_history_bars=min_history_bars)
    if not data_map:
        raise RuntimeError("No symbols had enough history for the weekly CAGR check")

    write_status(
        status="running",
        phase="backtesting",
        message="running baseline production strategy across current universe",
        tested_symbols=len(data_map),
    )
    result = lab.run_variant("baseline_current", data_map, {}, {}, rnn_params={"enabled": False}, rnn_models={})
    payload = build_payload(now, universe_meta, data_context, result)
    out_json, out_md = write_reports(payload)

    write_status(
        status="done",
        phase="completed",
        message="weekly universe CAGR check completed",
        report_json=str(out_json),
        report_md=str(out_md),
        requested_symbols=payload["universe"]["requested_symbols"],
        tested_symbols=payload["universe"]["tested_symbols"],
        coverage_pct=payload["universe"]["coverage_pct"],
        cagr_pct=payload["cagr_pct"],
        total_return_pct=payload["backtest"]["total_return_pct"],
        max_drawdown_pct=payload["backtest"]["max_drawdown_pct"],
    )

    print(json.dumps(payload, indent=2))
    print(f"Saved: {out_json}")
    print(f"Saved: {out_md}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
