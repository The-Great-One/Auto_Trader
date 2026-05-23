#!/usr/bin/env python3
"""Leakage-safe Kronos signal pilot for Auto_Trader.

This is a research-only harness. It never places orders and never changes live
rules. It loads local Kite feather history, adds a forecast feature layer, and
compares live-parity baseline results against BUY-signal filters.

Modes:
- technical_proxy: fast smoke-test proxy with the same feature contract. This is
  not Kronos alpha; it validates the harness and filter/backtest plumbing.
- kronos: optional real Kronos inference using a local checkout specified by
  --kronos-repo or KRONOS_REPO. Heavy; use small symbol/stride settings first.
"""

from __future__ import annotations

import argparse
import contextlib
import importlib
import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterator

import numpy as np
import pandas as pd

os.environ.setdefault("AT_DISABLE_FILE_LOGGING", "1")
os.environ.setdefault("AT_RESEARCH_MODE", "1")

ROOT = Path(__file__).resolve().parents[1]
HIST_DIR = ROOT / "intermediary_files" / "Hist_Data"
REPORTS = ROOT / "reports"
REPORTS.mkdir(exist_ok=True)

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from Auto_Trader.utils import Indicators  # noqa: E402
from scripts.weekly_universe_cagr_check import run_baseline_detailed  # noqa: E402

lab = importlib.import_module("scripts.weekly_strategy_lab")

FEATURE_RETURN = "KRONOS_EXPECTED_RETURN_PCT"
FEATURE_RANGE = "KRONOS_EXPECTED_RANGE_PCT"
FEATURE_CONF = "KRONOS_CONFIDENCE"
FEATURE_SOURCE = "KRONOS_FEATURE_SOURCE"


@dataclass
class Metrics:
    total_return_pct: float
    cagr_pct: float
    max_drawdown_pct: float
    trades: int
    win_rate_pct: float
    sharpe: float
    active_symbols: int


def _normalize_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    cmap = {str(c).lower(): c for c in df.columns}
    out = pd.DataFrame(
        {
            "Date": pd.to_datetime(df[cmap.get("date", "Date")], errors="coerce"),
            "Open": pd.to_numeric(df.get(cmap.get("open", "Open")), errors="coerce"),
            "High": pd.to_numeric(df.get(cmap.get("high", "High")), errors="coerce"),
            "Low": pd.to_numeric(df.get(cmap.get("low", "Low")), errors="coerce"),
            "Close": pd.to_numeric(df.get(cmap.get("close", "Close")), errors="coerce"),
            "Volume": pd.to_numeric(df.get(cmap.get("volume", "Volume"), 0), errors="coerce").fillna(0),
        }
    ).dropna(subset=["Date", "High", "Low", "Close"])
    out["Open"] = out["Open"].fillna(out["Close"])
    return out.sort_values("Date").drop_duplicates(subset=["Date"], keep="last").reset_index(drop=True)


def load_kite_symbols(limit: int, min_rows: int) -> dict[str, pd.DataFrame]:
    data_map: dict[str, pd.DataFrame] = {}
    skipped = 0
    for fp in sorted(HIST_DIR.glob("*.feather")):
        if limit and len(data_map) >= limit:
            break
        try:
            raw = pd.read_feather(fp)
            norm = _normalize_ohlcv(raw)
            if len(norm) < min_rows:
                skipped += 1
                continue
            enriched = Indicators(norm)
            if enriched is None or len(enriched) < min_rows:
                skipped += 1
                continue
            data_map[fp.stem.upper()] = enriched.sort_values("Date").reset_index(drop=True)
        except Exception:
            skipped += 1
    print(f"Loaded {len(data_map)} Kite-cache symbols; skipped {skipped}")
    return data_map


def add_technical_proxy_features(data_map: dict[str, pd.DataFrame], lookback: int, pred_len: int) -> dict[str, pd.DataFrame]:
    """Fast feature-contract proxy. Uses only past candles at each row."""
    out: dict[str, pd.DataFrame] = {}
    for symbol, df in data_map.items():
        x = df.copy()
        close = pd.to_numeric(x["Close"], errors="coerce")
        high = pd.to_numeric(x["High"], errors="coerce")
        low = pd.to_numeric(x["Low"], errors="coerce")
        momentum = close.pct_change(max(2, min(lookback, 60))).replace([np.inf, -np.inf], np.nan)
        recent_vol = close.pct_change().rolling(max(5, min(lookback, 40))).std().replace(0, np.nan)
        atr_like = ((high - low) / close.replace(0, np.nan)).rolling(max(5, min(pred_len, 20))).mean()
        # Scale a past-only directional prior into the same units real Kronos will emit.
        x[FEATURE_RETURN] = (momentum * 100.0).clip(-25, 25)
        x[FEATURE_RANGE] = (atr_like * 100.0 * max(1, pred_len) ** 0.5).clip(0, 50)
        x[FEATURE_CONF] = (momentum.abs() / (recent_vol * max(1, pred_len) ** 0.5)).clip(0, 5).fillna(0)
        x[FEATURE_SOURCE] = "technical_proxy"
        out[symbol] = x
    return out


class KronosAdapter:
    def __init__(self, repo: Path, model_name: str, tokenizer_name: str, max_context: int, device: str):
        if not repo.exists():
            raise FileNotFoundError(f"Kronos repo not found: {repo}")
        sys.path.insert(0, str(repo))
        from model import Kronos, KronosPredictor, KronosTokenizer  # type: ignore

        self.tokenizer = KronosTokenizer.from_pretrained(tokenizer_name)
        self.model = Kronos.from_pretrained(model_name)
        if device and hasattr(self.model, "to"):
            self.model = self.model.to(device)
        self.predictor = KronosPredictor(self.model, self.tokenizer, max_context=max_context, device=device)
        self.max_context = max_context

    def forecast_one(self, hist: pd.DataFrame, future_timestamps: pd.Series, pred_len: int, sample_count: int) -> tuple[float, float]:
        x = hist.tail(self.max_context).copy()
        kdf = pd.DataFrame(
            {
                "open": x["Open"].astype(float),
                "high": x["High"].astype(float),
                "low": x["Low"].astype(float),
                "close": x["Close"].astype(float),
                "volume": x.get("Volume", pd.Series(0, index=x.index)).astype(float),
                "amount": (x["Close"].astype(float) * x.get("Volume", pd.Series(0, index=x.index)).astype(float)),
            }
        ).reset_index(drop=True)
        pred = self.predictor.predict(
            df=kdf,
            x_timestamp=pd.to_datetime(x["Date"]).reset_index(drop=True),
            y_timestamp=pd.to_datetime(future_timestamps).reset_index(drop=True),
            pred_len=pred_len,
            T=1.0,
            top_p=0.9,
            sample_count=sample_count,
            verbose=False,
        )
        start_close = float(x["Close"].iloc[-1])
        pred_close = pd.to_numeric(pred["close"], errors="coerce")
        pred_high = pd.to_numeric(pred.get("high", pred_close), errors="coerce")
        pred_low = pd.to_numeric(pred.get("low", pred_close), errors="coerce")
        expected_ret = ((float(pred_close.iloc[-1]) / start_close) - 1.0) * 100.0 if start_close > 0 else 0.0
        expected_range = ((float(pred_high.max()) - float(pred_low.min())) / start_close) * 100.0 if start_close > 0 else 0.0
        return expected_ret, expected_range


def add_kronos_features(
    data_map: dict[str, pd.DataFrame],
    repo: Path,
    model_name: str,
    tokenizer_name: str,
    lookback: int,
    pred_len: int,
    stride: int,
    sample_count: int,
    device: str,
) -> dict[str, pd.DataFrame]:
    adapter = KronosAdapter(repo, model_name, tokenizer_name, lookback, device)
    out: dict[str, pd.DataFrame] = {}
    for symbol, df in data_map.items():
        x = df.copy()
        x[FEATURE_RETURN] = np.nan
        x[FEATURE_RANGE] = np.nan
        x[FEATURE_CONF] = 0.0
        x[FEATURE_SOURCE] = "kronos"
        anchors = range(max(lookback, 260), len(x) - pred_len, max(1, stride))
        print(f"Kronos inference: {symbol} anchors={len(list(anchors))}")
        for i in range(max(lookback, 260), len(x) - pred_len, max(1, stride)):
            hist = x.iloc[: i + 1]
            y_ts = x["Date"].iloc[i + 1 : i + 1 + pred_len]
            try:
                eret, erange = adapter.forecast_one(hist, y_ts, pred_len, sample_count)
                x.loc[x.index[i], FEATURE_RETURN] = eret
                x.loc[x.index[i], FEATURE_RANGE] = erange
            except Exception as exc:
                print(f"WARN {symbol} idx={i}: {str(exc)[:120]}")
        x[FEATURE_RETURN] = x[FEATURE_RETURN].ffill(limit=max(1, stride - 1)).fillna(0.0)
        x[FEATURE_RANGE] = x[FEATURE_RANGE].ffill(limit=max(1, stride - 1)).fillna(0.0)
        denom = pd.to_numeric(x[FEATURE_RANGE], errors="coerce").abs().replace(0, np.nan)
        x[FEATURE_CONF] = (pd.to_numeric(x[FEATURE_RETURN], errors="coerce").abs() / denom).clip(0, 5).fillna(0.0)
        out[symbol] = x
    return out


@contextlib.contextmanager
def kronos_buy_filter(min_return_pct: float, min_confidence: float) -> Iterator[None]:
    original = lab.RULE_SET_7.buy_or_sell

    def wrapped_buy_or_sell(df, row, hold_df):
        sig = original(df, row, hold_df)
        if str(sig).upper() != "BUY":
            return sig
        expected = float(row.get(FEATURE_RETURN, np.nan))
        confidence = float(row.get(FEATURE_CONF, 0.0) or 0.0)
        if not np.isfinite(expected) or expected < min_return_pct or confidence < min_confidence:
            return "HOLD"
        return sig

    lab.RULE_SET_7.buy_or_sell = wrapped_buy_or_sell
    try:
        yield
    finally:
        lab.RULE_SET_7.buy_or_sell = original


def metrics_from_run(result, details: dict, sim_meta: dict) -> Metrics:
    eq = sim_meta.get("portfolio_equity")
    if eq is not None and len(eq) > 20:
        s = pd.Series(eq, dtype=float) if not isinstance(eq, pd.Series) else eq.astype(float)
        final = float(s.iloc[-1])
        first = float(s.iloc[0])
        years = len(s) / 252.0
        cagr = ((final / max(first, 1e-9)) ** (1.0 / max(years, 0.01)) - 1.0) * 100.0
        peak = s.cummax()
        dd = float(((s - peak) / peak * 100.0).min())
        rets = s.pct_change().dropna()
        sharpe = float(rets.mean() / rets.std() * np.sqrt(252)) if len(rets) > 5 and rets.std() > 0 else 0.0
    else:
        cagr = 0.0
        dd = float(getattr(result, "max_drawdown_pct", 0.0) or 0.0)
        sharpe = 0.0
    return Metrics(
        total_return_pct=float(getattr(result, "total_return_pct", 0.0) or 0.0),
        cagr_pct=round(float(cagr), 3),
        max_drawdown_pct=round(dd, 3),
        trades=int(getattr(result, "trades", 0) or 0),
        win_rate_pct=float(getattr(result, "win_rate_pct", 0.0) or 0.0),
        sharpe=round(sharpe, 3),
        active_symbols=sum(1 for v in details.values() if int(v.get("trades", 0) or 0) > 0),
    )


def run_once(data_map: dict[str, pd.DataFrame], min_return_pct: float | None, min_confidence: float) -> Metrics:
    if min_return_pct is None:
        result, details, sim_meta = run_baseline_detailed(data_map)
    else:
        with kronos_buy_filter(min_return_pct, min_confidence):
            result, details, sim_meta = run_baseline_detailed(data_map)
    return metrics_from_run(result, details, sim_meta)


def fold_ranges(data_map: dict[str, pd.DataFrame], folds: int) -> list[tuple[str, str]]:
    dates = sorted({pd.to_datetime(d) for df in data_map.values() for d in df["Date"].tolist()})
    if not dates or folds <= 1:
        return []
    # Leave the earliest 30% as warmup/training context; validate on later contiguous folds.
    start_idx = int(len(dates) * 0.30)
    eval_dates = dates[start_idx:]
    chunks = np.array_split(np.array(eval_dates, dtype="datetime64[ns]"), folds)
    ranges = []
    for chunk in chunks:
        if len(chunk) == 0:
            continue
        ranges.append((pd.Timestamp(chunk[0]).strftime("%Y-%m-%d"), pd.Timestamp(chunk[-1]).strftime("%Y-%m-%d")))
    return ranges


@contextlib.contextmanager
def signal_window(start: str | None, end: str | None) -> Iterator[None]:
    old_start = os.environ.get("AT_BACKTEST_SIGNAL_START_DATE")
    old_end = os.environ.get("AT_BACKTEST_SIGNAL_END_DATE")
    if start:
        os.environ["AT_BACKTEST_SIGNAL_START_DATE"] = start
    if end:
        os.environ["AT_BACKTEST_SIGNAL_END_DATE"] = end
    try:
        yield
    finally:
        if old_start is None:
            os.environ.pop("AT_BACKTEST_SIGNAL_START_DATE", None)
        else:
            os.environ["AT_BACKTEST_SIGNAL_START_DATE"] = old_start
        if old_end is None:
            os.environ.pop("AT_BACKTEST_SIGNAL_END_DATE", None)
        else:
            os.environ["AT_BACKTEST_SIGNAL_END_DATE"] = old_end


def write_markdown(path: Path, payload: dict) -> None:
    rows = []
    for row in payload["results"]:
        m = row["metrics"]
        rows.append(
            f"| {row['name']} | {row.get('min_return_pct', '—')} | {row.get('min_confidence', '—')} | "
            f"{m['cagr_pct']:.2f}% | {m['total_return_pct']:.2f}% | {m['max_drawdown_pct']:.2f}% | "
            f"{m['trades']} | {m['win_rate_pct']:.1f}% | {m['sharpe']:.2f} |"
        )
    md = [
        "# Kronos Signal Pilot",
        "",
        f"Generated: `{payload['generated_at']}`",
        f"Mode: `{payload['mode']}`",
        f"Symbols: `{payload['symbols_loaded']}`",
        "",
        "Research-only. BUY signals execute next open through the existing live-parity backtest; Kronos/proxy values are read only from rows available at signal time.",
        "",
        "| Variant | Min forecast return | Min confidence | CAGR | Return | Max DD | Trades | Win | Sharpe |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
        *rows,
        "",
    ]
    if payload.get("folds"):
        md.extend(["## Walk-forward folds", ""])
        for fold in payload["folds"]:
            md.append(f"- `{fold['start']}` → `{fold['end']}`")
            for row in fold["results"]:
                m = row["metrics"]
                md.append(f"  - {row['name']}: CAGR {m['cagr_pct']:.2f}%, trades {m['trades']}, DD {m['max_drawdown_pct']:.2f}%")
        md.append("")
    path.write_text("\n".join(md))


def main() -> int:
    parser = argparse.ArgumentParser(description="Kronos forecast-feature pilot for Auto_Trader")
    parser.add_argument("--mode", choices=["technical_proxy", "kronos"], default="technical_proxy")
    parser.add_argument("--symbols", type=int, default=20)
    parser.add_argument("--min-rows", type=int, default=1000)
    parser.add_argument("--lookback", type=int, default=512)
    parser.add_argument("--pred-len", type=int, default=5)
    parser.add_argument("--stride", type=int, default=20)
    parser.add_argument("--sample-count", type=int, default=1)
    parser.add_argument("--thresholds", default="0,1,2", help="Comma-separated min forecast return pct filters")
    parser.add_argument("--min-confidence", type=float, default=0.0)
    parser.add_argument("--folds", type=int, default=0)
    parser.add_argument("--kronos-repo", default=os.getenv("KRONOS_REPO", ""))
    parser.add_argument("--model", default="NeoQuasar/Kronos-small")
    parser.add_argument("--tokenizer", default="NeoQuasar/Kronos-Tokenizer-base")
    parser.add_argument("--device", default=os.getenv("KRONOS_DEVICE", "cpu"))
    parser.add_argument("--output", default="")
    args = parser.parse_args()

    # Research sizing defaults mirror current CAGR hunt conventions.
    os.environ.setdefault("AT_LAB_MODE", "1")
    os.environ.setdefault("AT_BACKTEST_VOL_SIZING_ENABLED", "1")
    os.environ.setdefault("AT_BACKTEST_RISK_PER_TRADE_PCT", "0.02")
    os.environ.setdefault("AT_BACKTEST_ATR_STOP_MULT", "2.5")
    os.environ.setdefault("AT_BACKTEST_MAX_POSITION_NOTIONAL_PCT", "0.10")
    os.environ.setdefault("AT_MAX_SINGLE_SYMBOL_WEIGHT", "0.15")
    os.environ.setdefault("AT_PORTFOLIO_BAND", "0.10")

    data_map = load_kite_symbols(limit=args.symbols, min_rows=args.min_rows)
    if not data_map:
        raise SystemExit("No Kite-cache symbols loaded")

    if args.mode == "technical_proxy":
        data_map = add_technical_proxy_features(data_map, args.lookback, args.pred_len)
    else:
        if not args.kronos_repo:
            raise SystemExit("--kronos-repo or KRONOS_REPO is required for --mode kronos")
        data_map = add_kronos_features(
            data_map,
            Path(args.kronos_repo).expanduser(),
            args.model,
            args.tokenizer,
            args.lookback,
            args.pred_len,
            args.stride,
            args.sample_count,
            args.device,
        )

    thresholds = [float(x.strip()) for x in args.thresholds.split(",") if x.strip()]
    generated_at = datetime.now().isoformat(timespec="seconds")
    payload = {
        "generated_at": generated_at,
        "mode": args.mode,
        "symbols_loaded": len(data_map),
        "feature_contract": {
            "return_pct": FEATURE_RETURN,
            "range_pct": FEATURE_RANGE,
            "confidence": FEATURE_CONF,
            "source": FEATURE_SOURCE,
            "leakage_control": "features are computed from candles up to and including signal row; execution remains next open",
        },
        "args": vars(args),
        "results": [],
        "folds": [],
    }

    print("Running baseline...")
    baseline = run_once(data_map, None, args.min_confidence)
    payload["results"].append({"name": "baseline_current", "metrics": baseline.__dict__})
    print(f"baseline_current CAGR={baseline.cagr_pct:.2f}% trades={baseline.trades}")

    for threshold in thresholds:
        print(f"Running forecast filter min_return={threshold:.2f}%...")
        m = run_once(data_map, threshold, args.min_confidence)
        payload["results"].append(
            {
                "name": f"kronos_filter_ret_ge_{threshold:g}",
                "min_return_pct": threshold,
                "min_confidence": args.min_confidence,
                "metrics": m.__dict__,
            }
        )
        print(f"  CAGR={m.cagr_pct:.2f}% trades={m.trades} DD={m.max_drawdown_pct:.2f}%")

    for start, end in fold_ranges(data_map, args.folds):
        fold_payload = {"start": start, "end": end, "results": []}
        print(f"Fold {start} -> {end}")
        with signal_window(start, end):
            bm = run_once(data_map, None, args.min_confidence)
            fold_payload["results"].append({"name": "baseline_current", "metrics": bm.__dict__})
            for threshold in thresholds:
                fm = run_once(data_map, threshold, args.min_confidence)
                fold_payload["results"].append({"name": f"kronos_filter_ret_ge_{threshold:g}", "min_return_pct": threshold, "min_confidence": args.min_confidence, "metrics": fm.__dict__})
        payload["folds"].append(fold_payload)

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_json = Path(args.output) if args.output else REPORTS / f"kronos_signal_pilot_{stamp}.json"
    if not out_json.is_absolute():
        out_json = ROOT / out_json
    out_json.write_text(json.dumps(payload, indent=2, default=str))
    out_md = out_json.with_suffix(".md")
    write_markdown(out_md, payload)
    latest_json = REPORTS / "kronos_signal_pilot_latest.json"
    latest_md = REPORTS / "kronos_signal_pilot_latest.md"
    latest_json.write_text(out_json.read_text())
    latest_md.write_text(out_md.read_text())
    print(f"Wrote {out_json}")
    print(f"Wrote {out_md}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
