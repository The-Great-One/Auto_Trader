#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from Auto_Trader.my_secrets import API_KEY  # type: ignore
from Auto_Trader.utils import read_session_data  # type: ignore
from kiteconnect import KiteConnect  # type: ignore


def main() -> int:
    symbol = (sys.argv[1] if len(sys.argv) > 1 else "").strip().upper()
    side_filter = (sys.argv[2] if len(sys.argv) > 2 else "").strip().upper()
    if not symbol:
        print(json.dumps({"error": "symbol_required"}))
        return 2
    if side_filter and side_filter not in {"CE", "PE"}:
        print(json.dumps({"error": "invalid_side", "side": side_filter}))
        return 2

    kite = KiteConnect(api_key=API_KEY)
    kite.set_access_token(read_session_data())
    instruments = kite.instruments("NFO")
    today = date.today()
    rows = []
    for inst in instruments:
        if str(inst.get("name") or "").upper() != symbol:
            continue
        side = str(inst.get("instrument_type") or "").upper()
        if side not in {"CE", "PE"}:
            continue
        if side_filter and side != side_filter:
            continue
        expiry = inst.get("expiry")
        if expiry is None:
            continue
        expiry_date = expiry if hasattr(expiry, "year") else date.fromisoformat(str(expiry)[:10])
        if expiry_date < today:
            continue
        rows.append({
            "tradingsymbol": inst.get("tradingsymbol"),
            "instrument_token": inst.get("instrument_token"),
            "expiry": expiry_date.isoformat(),
            "strike": float(inst.get("strike") or 0.0),
            "side": side,
            "lot_size": int(inst.get("lot_size") or 0),
            "last_price": 0.0,
        })

    rows.sort(key=lambda r: (r["expiry"], r["strike"], r["side"]))
    expiries = sorted({r["expiry"] for r in rows})
    nearest_expiry = expiries[0] if expiries else None

    # Fetch LTP for the nearest two expiries only; this keeps API payload small and
    # covers live paper entries/refreshes, which should trade near-term options.
    ltp_rows = [r for r in rows if r["expiry"] in set(expiries[:2])]
    keys = [f"NFO:{r['tradingsymbol']}" for r in ltp_rows if r.get("tradingsymbol")]
    try:
        for i in range(0, len(keys), 200):
            chunk = keys[i:i + 200]
            ltp = kite.ltp(chunk)
            for r in ltp_rows[i:i + len(chunk)]:
                item = ltp.get(f"NFO:{r['tradingsymbol']}") or {}
                r["last_price"] = float(item.get("last_price") or 0.0)
    except Exception:
        pass

    chain = ltp_rows if ltp_rows else rows
    print(json.dumps({
        "symbol": symbol,
        "nearest_expiry": nearest_expiry,
        "expiries": expiries,
        "chain_count": len(chain),
        "chain": chain,
    }, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
