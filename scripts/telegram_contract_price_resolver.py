#!/usr/bin/env python3
"""Dedicated Telegram option contract price resolver.

This is intentionally separate from the Telegram paper ledger. It only resolves a
Telegram option suggestion to a live non-expired Kite NFO contract and returns the
current price metadata needed by the ledger. Missing or expired contracts are
reported as drop statuses so the ledger does not keep unresolved placeholders.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from kiteconnect import KiteConnect  # type: ignore

_secrets: dict[str, Any] = {}
exec((ROOT / "Auto_Trader" / "my_secrets.py").read_text(encoding="utf-8"), _secrets)
API_KEY = _secrets["API_KEY"]
TOKEN_PATH = ROOT / "intermediary_files" / "access_token.json"


def read_access_token() -> str:
    payload = json.loads(TOKEN_PATH.read_text(encoding="utf-8"))
    if isinstance(payload, str):
        return payload
    for key in ("access_token", "token", "data"):
        val = payload.get(key) if isinstance(payload, dict) else None
        if isinstance(val, str) and val:
            return val
    raise RuntimeError("access_token_missing_or_unreadable")

VALID_SIDES = {"CE", "PE"}


def _parse_date(value: Any) -> date | None:
    if not value:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    text = str(value).strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(timezone.utc).date()
    except Exception:
        try:
            return date.fromisoformat(text[:10])
        except Exception:
            return None


def _inst_expiry(inst: dict[str, Any]) -> date | None:
    expiry = inst.get("expiry")
    if expiry is None:
        return None
    if isinstance(expiry, date):
        return expiry
    return _parse_date(expiry)


def resolve_contract(kite: KiteConnect, call: dict[str, Any]) -> dict[str, Any]:
    symbol = str(call.get("symbol") or "").strip().upper()
    side = str(call.get("option_side") or call.get("side") or "").strip().upper()
    strike_raw = call.get("option_strike") or call.get("strike")
    as_of = _parse_date(call.get("captured_at") or call.get("tracking_started_at") or call.get("date")) or date.today()
    today = date.today()

    if not symbol:
        return {"status": "drop", "reason": "symbol_missing"}
    if side not in VALID_SIDES:
        return {"status": "drop", "reason": "invalid_or_missing_side", "symbol": symbol, "side": side}
    try:
        strike = float(strike_raw)
    except Exception:
        return {"status": "drop", "reason": "invalid_or_missing_strike", "symbol": symbol, "side": side}

    candidates: list[dict[str, Any]] = []
    expired_matches: list[dict[str, Any]] = []
    for inst in kite.instruments("NFO"):
        if str(inst.get("name") or "").strip().upper() != symbol:
            continue
        if str(inst.get("instrument_type") or "").strip().upper() != side:
            continue
        try:
            inst_strike = float(inst.get("strike") or 0.0)
        except Exception:
            continue
        if inst_strike != strike:
            continue
        expiry = _inst_expiry(inst)
        if expiry is None:
            continue
        row = {
            "tradingsymbol": inst.get("tradingsymbol"),
            "instrument_token": inst.get("instrument_token"),
            "exchange_token": inst.get("exchange_token"),
            "expiry": expiry.isoformat(),
            "strike": inst_strike,
            "side": side,
            "lot_size": int(inst.get("lot_size") or 0),
            "name": inst.get("name"),
        }
        if expiry < today or expiry < as_of:
            expired_matches.append(row)
            continue
        candidates.append(row)

    if not candidates:
        if expired_matches:
            return {
                "status": "drop",
                "reason": "contract_expired",
                "symbol": symbol,
                "side": side,
                "strike": strike,
                "latest_expired_expiry": sorted(expired_matches, key=lambda r: r["expiry"])[-1]["expiry"],
            }
        return {"status": "drop", "reason": "contract_not_found", "symbol": symbol, "side": side, "strike": strike}

    candidates.sort(key=lambda r: (r["expiry"], r["tradingsymbol"] or ""))
    contract = candidates[0]
    last_price = 0.0
    if contract.get("tradingsymbol"):
        try:
            payload = kite.ltp([f"NFO:{contract['tradingsymbol']}"])
            item = payload.get(f"NFO:{contract['tradingsymbol']}") or {}
            last_price = float(item.get("last_price") or 0.0)
        except Exception:
            last_price = 0.0
    contract["last_price"] = last_price
    return {
        "status": "ok",
        "symbol": symbol,
        "side": side,
        "strike": strike,
        "contract": contract,
        "nearest_expiry": contract.get("expiry"),
        "last_price": last_price,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Resolve one Telegram option suggestion to a live Kite contract price.")
    parser.add_argument("--json", help="Call JSON. Use '-' to read stdin.")
    parser.add_argument("--symbol")
    parser.add_argument("--side")
    parser.add_argument("--strike", type=float)
    parser.add_argument("--captured-at")
    args = parser.parse_args()

    if args.json == "-":
        call = json.loads(sys.stdin.read() or "{}")
    elif args.json:
        call = json.loads(args.json)
    else:
        call = {
            "symbol": args.symbol,
            "option_side": args.side,
            "option_strike": args.strike,
            "captured_at": args.captured_at,
        }

    kite = KiteConnect(api_key=API_KEY)
    kite.set_access_token(read_access_token())
    print(json.dumps(resolve_contract(kite, call), default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
