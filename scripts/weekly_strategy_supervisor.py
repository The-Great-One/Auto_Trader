#!/usr/bin/env python3
import importlib
import json
import os
import re
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import sys

# run from repo root expected
REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

HIST = REPO / "intermediary_files" / "Hist_Data" / "NIFTYETF.feather"
ENV_FILE = Path("/home/ubuntu/.autotrader_env")


def _load_price_data() -> pd.DataFrame:
    if HIST.exists():
        df = pd.read_feather(HIST)
        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
            df = df.sort_values("Date")
        return df.tail(260).reset_index(drop=True)

    # Fallback for lean servers without Hist_Data cache
    try:
        import yfinance as yf

        d = yf.download("NIFTYBEES.NS", period="2y", interval="1d", auto_adjust=False, progress=False)
        if d is None or d.empty:
            raise RuntimeError("yfinance returned empty data")
        d = d.reset_index().rename(columns={"Date": "Date", "Open": "Open", "High": "High", "Low": "Low", "Close": "Close", "Volume": "Volume"})
        return d[["Date", "Open", "High", "Low", "Close", "Volume"]].tail(260).reset_index(drop=True)
    except Exception as e:
        raise SystemExit(f"Missing data: {HIST} and yfinance fallback failed: {e}")


@dataclass
class Result:
    rule: str
    total_return_pct: float
    trades: int


def _load_env_flags() -> dict:
    env = {}
    if ENV_FILE.exists():
        for line in ENV_FILE.read_text().splitlines():
            line = line.strip()
            if not line.startswith("export "):
                continue
            k, _, v = line[7:].partition("=")
            env[k.strip()] = v.strip().strip('"')
    return env


def _write_env_flags(flags: dict):
    lines = []
    if ENV_FILE.exists():
        for line in ENV_FILE.read_text().splitlines():
            if re.match(r"\s*export\s+RULE_SET_[27]=", line):
                continue
            lines.append(line)
    lines.append(f'export RULE_SET_2="{flags.get("RULE_SET_2", "")}"')
    lines.append(f'export RULE_SET_7="{flags.get("RULE_SET_7", "")}"')
    ENV_FILE.write_text("\n".join([x for x in lines if x is not None]).strip() + "\n")


def _simulate(rule_name: str, df: pd.DataFrame) -> Result:
    # isolate RULE_SET_2 JSON side effects
    state_dir = tempfile.mkdtemp(prefix="at_weekly_state_")
    os.environ["AT_STATE_DIR"] = state_dir

    utils = importlib.import_module("Auto_Trader.utils")
    rule_mod = importlib.import_module(f"Auto_Trader.{rule_name}")

    dfi = utils.Indicators(df.copy()).dropna().reset_index(drop=True)
    if dfi.empty:
        return Result(rule_name, 0.0, 0)

    cash = 100000.0
    qty = 0
    avg = 0.0
    trades = 0

    for i in range(60, len(dfi)):
        part = dfi.iloc[: i + 1].copy()
        row = part.iloc[-1]

        if qty > 0:
            holdings = pd.DataFrame(
                [{
                    "instrument_token": int(row.get("instrument_token", 1626369)),
                    "tradingsymbol": "NIFTYETF",
                    "average_price": avg,
                    "quantity": qty,
                    "t1_quantity": 0,
                    "bars_in_trade": i,
                }]
            )
        else:
            holdings = pd.DataFrame(columns=["instrument_token", "tradingsymbol", "average_price", "quantity", "t1_quantity", "bars_in_trade"])

        row_dict = row.to_dict()
        row_dict.setdefault("instrument_token", 1626369)
        signal = str(rule_mod.buy_or_sell(part, row_dict, holdings)).upper()
        price = float(row["Close"])

        if signal == "BUY" and qty == 0 and price > 0:
            qty = int(cash // price)
            if qty > 0:
                cash -= qty * price
                avg = price
                trades += 1
        elif signal == "SELL" and qty > 0:
            cash += qty * price
            qty = 0
            avg = 0.0
            trades += 1

    if qty > 0:
        cash += qty * float(dfi.iloc[-1]["Close"])

    ret = (cash - 100000.0) / 100000.0 * 100.0
    return Result(rule_name, float(ret), trades)


def main():
    df = _load_price_data()

    candidates = ["RULE_SET_2", "RULE_SET_7"]
    results = [_simulate(c, df) for c in candidates]
    results.sort(key=lambda x: x.total_return_pct, reverse=True)
    best = results[0]

    current = _load_env_flags()
    current_rule = "RULE_SET_7" if current.get("RULE_SET_7") else "RULE_SET_2"
    current_ret = next((r.total_return_pct for r in results if r.rule == current_rule), None)

    # switch only if current is not profitable and best is clearly better
    switched = False
    if current_ret is not None and current_ret <= 0 and best.rule != current_rule and best.total_return_pct > current_ret + 0.25:
        if best.rule == "RULE_SET_2":
            _write_env_flags({"RULE_SET_2": "1", "RULE_SET_7": ""})
        else:
            _write_env_flags({"RULE_SET_2": "", "RULE_SET_7": "1"})
        subprocess.run(["sudo", "systemctl", "restart", "auto_trade.service"], check=False)
        switched = True

    out = {
        "current_rule": current_rule,
        "current_return_pct": current_ret,
        "best_rule": best.rule,
        "best_return_pct": best.total_return_pct,
        "results": [r.__dict__ for r in results],
        "switched": switched,
    }

    reports = REPO / "reports"
    reports.mkdir(exist_ok=True)
    p = reports / "weekly_strategy_supervisor.json"
    p.write_text(json.dumps(out, indent=2))
    print(json.dumps(out))


if __name__ == "__main__":
    main()
