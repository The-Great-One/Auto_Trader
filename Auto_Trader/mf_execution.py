from __future__ import annotations

import json
import os
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any


@dataclass
class MFOrderRequest:
    tradingsymbol: str
    transaction_type: str
    amount: float | None = None
    quantity: float | None = None
    tag: str = "mf_manual"


@dataclass
class MFExecutionConfig:
    max_order_amount: float = float(os.getenv("AT_MF_MAX_ORDER_AMOUNT", "25000"))
    max_total_amount: float = float(os.getenv("AT_MF_MAX_TOTAL_ORDER_AMOUNT", "100000"))
    min_order_amount: float = float(os.getenv("AT_MF_MIN_ORDER_AMOUNT", "100"))
    live_execution_enabled: bool = os.getenv("AT_MF_ENABLE_LIVE", "0").strip().lower() in {"1", "true", "yes"}
    require_allowlist: bool = os.getenv("AT_MF_REQUIRE_ALLOWLIST", "0").strip().lower() in {"1", "true", "yes"}
    allowlist_path: str = os.getenv(
        "AT_MF_ALLOWLIST_PATH",
        str(Path("intermediary_files") / "mf_allowlist.json"),
    )


def _safe_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except Exception:
        return None


def load_allowlist(config: MFExecutionConfig) -> set[str]:
    path = Path(config.allowlist_path)
    env_symbols = os.getenv("AT_MF_ALLOWED_SYMBOLS", "").strip()
    allowed = {
        s.strip().upper()
        for s in env_symbols.split(",")
        if s.strip()
    }
    if path.exists():
        try:
            data = json.loads(path.read_text())
            if isinstance(data, list):
                allowed.update(str(x).strip().upper() for x in data if str(x).strip())
            elif isinstance(data, dict):
                values = data.get("symbols", [])
                allowed.update(str(x).strip().upper() for x in values if str(x).strip())
        except Exception:
            pass
    return allowed


def fetch_mf_instrument_index(kite) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in kite.mf_instruments() or []:
        symbol = str(row.get("tradingsymbol") or "").strip().upper()
        if symbol and symbol not in out:
            out[symbol] = row
    return out


def search_mf_instruments(kite, query: str, limit: int = 20) -> list[dict[str, Any]]:
    query_lc = query.strip().lower()
    if not query_lc:
        return []
    matches = []
    for row in kite.mf_instruments() or []:
        text = " ".join(
            str(row.get(key) or "")
            for key in ("tradingsymbol", "name", "scheme_name", "amc")
        ).lower()
        if query_lc in text:
            matches.append(row)
        if len(matches) >= limit:
            break
    return matches


def normalize_order(raw: dict[str, Any], default_tag: str = "mf_manual") -> MFOrderRequest:
    symbol = str(raw.get("tradingsymbol") or raw.get("symbol") or "").strip().upper()
    txn = str(raw.get("transaction_type") or raw.get("side") or "").strip().upper()
    if txn in {"B", "BUY"}:
        txn = "BUY"
    elif txn in {"S", "SELL", "REDEEM"}:
        txn = "SELL"
    return MFOrderRequest(
        tradingsymbol=symbol,
        transaction_type=txn,
        amount=_safe_float(raw.get("amount")),
        quantity=_safe_float(raw.get("quantity")),
        tag=str(raw.get("tag") or default_tag)[:20],
    )


def validate_orders(kite, orders: list[MFOrderRequest], config: MFExecutionConfig | None = None) -> dict[str, Any]:
    config = config or MFExecutionConfig()
    instruments = fetch_mf_instrument_index(kite)
    allowlist = load_allowlist(config)
    validated = []
    errors = []
    total_amount = 0.0

    for idx, order in enumerate(orders, start=1):
        amount = order.amount
        quantity = order.quantity
        if not order.tradingsymbol:
            errors.append(f"order {idx}: missing tradingsymbol")
            continue
        if order.transaction_type not in {"BUY", "SELL"}:
            errors.append(f"order {idx}: invalid transaction_type {order.transaction_type!r}")
            continue
        if order.tradingsymbol not in instruments:
            errors.append(f"order {idx}: unknown MF symbol {order.tradingsymbol}")
            continue
        instrument = instruments[order.tradingsymbol]
        if order.transaction_type == "BUY" and not bool(instrument.get("purchase_allowed", True)):
            errors.append(f"order {idx}: purchases not allowed for {order.tradingsymbol}")
            continue
        if order.transaction_type == "SELL" and not bool(instrument.get("redemption_allowed", True)):
            errors.append(f"order {idx}: redemptions not allowed for {order.tradingsymbol}")
            continue
        if config.require_allowlist and order.tradingsymbol not in allowlist:
            errors.append(f"order {idx}: symbol {order.tradingsymbol} not in MF allowlist")
            continue
        if (amount is None) == (quantity is None):
            errors.append(f"order {idx}: specify exactly one of amount or quantity")
            continue
        if amount is not None:
            if amount < config.min_order_amount:
                errors.append(f"order {idx}: amount {amount} below minimum {config.min_order_amount}")
                continue
            if amount > config.max_order_amount:
                errors.append(f"order {idx}: amount {amount} exceeds per-order max {config.max_order_amount}")
                continue
            total_amount += amount
        validated.append(order)

    if total_amount > config.max_total_amount:
        errors.append(
            f"total amount {round(total_amount, 2)} exceeds run max {config.max_total_amount}"
        )

    return {
        "ok": not errors,
        "validated_orders": [asdict(o) for o in validated],
        "errors": errors,
        "total_amount": round(total_amount, 2),
        "allowlist_enabled": config.require_allowlist,
    }


def execute_orders(kite, orders: list[MFOrderRequest], dry_run: bool = True, config: MFExecutionConfig | None = None) -> dict[str, Any]:
    config = config or MFExecutionConfig()
    validation = validate_orders(kite, orders, config=config)
    if not validation["ok"] or dry_run:
        return {
            "dry_run": dry_run,
            "validation": validation,
            "live_execution_enabled": config.live_execution_enabled,
            "results": [],
        }

    if not config.live_execution_enabled:
        return {
            "dry_run": dry_run,
            "validation": validation,
            "live_execution_enabled": config.live_execution_enabled,
            "results": [],
            "errors": ["Live MF execution disabled. Set AT_MF_ENABLE_LIVE=1 to place orders."],
        }

    results = []
    for order in orders:
        response = kite.place_mf_order(
            tradingsymbol=order.tradingsymbol,
            transaction_type=order.transaction_type,
            quantity=order.quantity,
            amount=order.amount,
            tag=order.tag,
        )
        results.append(
            {
                "tradingsymbol": order.tradingsymbol,
                "transaction_type": order.transaction_type,
                "amount": order.amount,
                "quantity": order.quantity,
                "tag": order.tag,
                "response": response,
            }
        )

    return {
        "dry_run": dry_run,
        "validation": validation,
        "live_execution_enabled": config.live_execution_enabled,
        "results": results,
    }


def build_buy_orders_from_target_amounts(target_amounts: dict[str, float], tag: str = "mf_rebalance") -> list[MFOrderRequest]:
    orders: list[MFOrderRequest] = []
    for symbol, amount in target_amounts.items():
        amt = _safe_float(amount)
        if not symbol or amt is None or amt <= 0:
            continue
        orders.append(
            MFOrderRequest(
                tradingsymbol=str(symbol).strip().upper(),
                transaction_type="BUY",
                amount=amt,
                tag=tag,
            )
        )
    return orders
