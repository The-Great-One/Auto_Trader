from __future__ import annotations

import json
import math
import os
from datetime import datetime
from dataclasses import asdict, dataclass
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
class MFSIPRequest:
    tradingsymbol: str
    amount: float
    instalments: int
    frequency: str = "monthly"
    initial_amount: float | None = None
    instalment_day: int | None = None
    tag: str = "mf_sip"


@dataclass
class MFSIPModifyRequest:
    sip_id: str
    amount: float | None = None
    status: str | None = None
    instalments: int | None = None
    frequency: str | None = None
    instalment_day: int | None = None


@dataclass
class MFExecutionConfig:
    max_order_amount: float = float(os.getenv("AT_MF_MAX_ORDER_AMOUNT", "25000"))
    max_total_amount: float = float(os.getenv("AT_MF_MAX_TOTAL_ORDER_AMOUNT", "100000"))
    min_order_amount: float = float(os.getenv("AT_MF_MIN_ORDER_AMOUNT", "100"))
    max_sip_amount: float = float(os.getenv("AT_MF_MAX_SIP_AMOUNT", os.getenv("AT_MF_MAX_ORDER_AMOUNT", "25000")))
    min_sip_amount: float = float(os.getenv("AT_MF_MIN_SIP_AMOUNT", "100"))
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


def _safe_int(value: Any) -> int | None:
    try:
        if value is None or value == "":
            return None
        return int(value)
    except Exception:
        return None


def _normalize_side(value: str) -> str:
    txn = str(value or "").strip().upper()
    if txn in {"B", "BUY"}:
        return "BUY"
    if txn in {"S", "SELL", "REDEEM"}:
        return "SELL"
    return txn


def _normalize_frequency(value: str | None) -> str | None:
    if value is None:
        return None
    freq = str(value).strip().lower()
    aliases = {
        "monthly": "monthly",
        "month": "monthly",
        "weekly": "weekly",
        "week": "weekly",
        "quarterly": "quarterly",
        "quarter": "quarterly",
    }
    return aliases.get(freq, freq)


def load_allowlist(config: MFExecutionConfig) -> set[str]:
    path = Path(config.allowlist_path)
    env_symbols = os.getenv("AT_MF_ALLOWED_SYMBOLS", "").strip()
    allowed = {s.strip().upper() for s in env_symbols.split(",") if s.strip()}
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


def fetch_mf_holdings_index(kite) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in kite.mf_holdings() or []:
        symbol = str(row.get("tradingsymbol") or row.get("scheme_name") or "").strip().upper()
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
    return MFOrderRequest(
        tradingsymbol=str(raw.get("tradingsymbol") or raw.get("symbol") or "").strip().upper(),
        transaction_type=_normalize_side(raw.get("transaction_type") or raw.get("side") or ""),
        amount=_safe_float(raw.get("amount")),
        quantity=_safe_float(raw.get("quantity")),
        tag=str(raw.get("tag") or default_tag)[:20],
    )


def normalize_sip(raw: dict[str, Any], default_tag: str = "mf_sip") -> MFSIPRequest:
    return MFSIPRequest(
        tradingsymbol=str(raw.get("tradingsymbol") or raw.get("symbol") or "").strip().upper(),
        amount=float(_safe_float(raw.get("amount")) or 0.0),
        instalments=int(_safe_int(raw.get("instalments")) or 0),
        frequency=str(_normalize_frequency(raw.get("frequency")) or "monthly"),
        initial_amount=_safe_float(raw.get("initial_amount")),
        instalment_day=_safe_int(raw.get("instalment_day")),
        tag=str(raw.get("tag") or default_tag)[:20],
    )


def normalize_sip_modify(raw: dict[str, Any]) -> MFSIPModifyRequest:
    return MFSIPModifyRequest(
        sip_id=str(raw.get("sip_id") or raw.get("id") or "").strip(),
        amount=_safe_float(raw.get("amount")),
        status=str(raw.get("status")).strip().upper() if raw.get("status") is not None else None,
        instalments=_safe_int(raw.get("instalments")),
        frequency=_normalize_frequency(raw.get("frequency")),
        instalment_day=_safe_int(raw.get("instalment_day")),
    )


def _round_down_to_step(value: float, step: float) -> float:
    if step <= 0:
        return value
    return math.floor(value / step) * step


def _validate_order_quantity(order: MFOrderRequest, instrument: dict[str, Any], errors: list[str], idx: int) -> None:
    if order.quantity is None:
        return
    qty = float(order.quantity)
    if qty <= 0:
        errors.append(f"order {idx}: quantity must be positive")
        return
    if order.transaction_type == "SELL":
        min_qty = _safe_float(instrument.get("minimum_redemption_quantity")) or 0.0
        step = _safe_float(instrument.get("redemption_quantity_multiplier")) or 0.0
        if min_qty > 0 and qty < min_qty:
            errors.append(f"order {idx}: quantity {qty} below minimum redemption quantity {min_qty}")
        if step > 0:
            rounded = _round_down_to_step(qty, step)
            if abs(qty - rounded) > 1e-9:
                errors.append(f"order {idx}: quantity {qty} does not align with redemption step {step}")


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
        instrument = instruments.get(order.tradingsymbol)
        if instrument is None:
            errors.append(f"order {idx}: unknown MF symbol {order.tradingsymbol}")
            continue
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
            if order.transaction_type == "BUY":
                min_purchase = _safe_float(instrument.get("minimum_purchase_amount")) or 0.0
                if min_purchase > 0 and amount < min_purchase:
                    errors.append(f"order {idx}: amount {amount} below instrument minimum purchase amount {min_purchase}")
                    continue
            total_amount += amount
        _validate_order_quantity(order, instrument, errors, idx)
        validated.append(order)

    if total_amount > config.max_total_amount:
        errors.append(f"total amount {round(total_amount, 2)} exceeds run max {config.max_total_amount}")

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


def validate_sips(kite, sips: list[MFSIPRequest], config: MFExecutionConfig | None = None) -> dict[str, Any]:
    config = config or MFExecutionConfig()
    instruments = fetch_mf_instrument_index(kite)
    allowlist = load_allowlist(config)
    validated = []
    errors = []
    total_amount = 0.0
    valid_frequencies = {"daily", "weekly", "fortnightly", "monthly", "quarterly"}

    for idx, sip in enumerate(sips, start=1):
        if not sip.tradingsymbol:
            errors.append(f"sip {idx}: missing tradingsymbol")
            continue
        instrument = instruments.get(sip.tradingsymbol)
        if instrument is None:
            errors.append(f"sip {idx}: unknown MF symbol {sip.tradingsymbol}")
            continue
        if not bool(instrument.get("purchase_allowed", True)):
            errors.append(f"sip {idx}: purchases not allowed for {sip.tradingsymbol}")
            continue
        if config.require_allowlist and sip.tradingsymbol not in allowlist:
            errors.append(f"sip {idx}: symbol {sip.tradingsymbol} not in MF allowlist")
            continue
        if sip.amount < config.min_sip_amount:
            errors.append(f"sip {idx}: amount {sip.amount} below minimum {config.min_sip_amount}")
            continue
        if sip.amount > config.max_sip_amount:
            errors.append(f"sip {idx}: amount {sip.amount} exceeds per-SIP max {config.max_sip_amount}")
            continue
        min_purchase = _safe_float(instrument.get("minimum_purchase_amount")) or 0.0
        if min_purchase > 0 and sip.amount < min_purchase:
            errors.append(f"sip {idx}: amount {sip.amount} below instrument minimum purchase amount {min_purchase}")
            continue
        if sip.instalments <= 0:
            errors.append(f"sip {idx}: instalments must be positive")
            continue
        if sip.frequency not in valid_frequencies:
            errors.append(f"sip {idx}: unsupported frequency {sip.frequency}")
            continue
        if sip.instalment_day is not None and not (1 <= sip.instalment_day <= 31):
            errors.append(f"sip {idx}: instalment_day must be between 1 and 31")
            continue
        if sip.initial_amount is not None and sip.initial_amount <= 0:
            errors.append(f"sip {idx}: initial_amount must be positive")
            continue
        total_amount += sip.amount
        validated.append(sip)

    if total_amount > config.max_total_amount:
        errors.append(f"total SIP amount {round(total_amount, 2)} exceeds run max {config.max_total_amount}")

    return {
        "ok": not errors,
        "validated_sips": [asdict(s) for s in validated],
        "errors": errors,
        "total_amount": round(total_amount, 2),
        "allowlist_enabled": config.require_allowlist,
    }


def execute_sips(kite, sips: list[MFSIPRequest], dry_run: bool = True, config: MFExecutionConfig | None = None) -> dict[str, Any]:
    config = config or MFExecutionConfig()
    validation = validate_sips(kite, sips, config=config)
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
            "errors": ["Live MF execution disabled. Set AT_MF_ENABLE_LIVE=1 to place SIPs."],
        }

    results = []
    for sip in sips:
        response = kite.place_mf_sip(
            tradingsymbol=sip.tradingsymbol,
            amount=sip.amount,
            instalments=sip.instalments,
            frequency=sip.frequency,
            initial_amount=sip.initial_amount,
            instalment_day=sip.instalment_day,
            tag=sip.tag,
        )
        results.append({**asdict(sip), "response": response})

    return {
        "dry_run": dry_run,
        "validation": validation,
        "live_execution_enabled": config.live_execution_enabled,
        "results": results,
    }


def execute_sip_modify(kite, request: MFSIPModifyRequest, dry_run: bool = True, config: MFExecutionConfig | None = None) -> dict[str, Any]:
    config = config or MFExecutionConfig()
    payload = {k: v for k, v in asdict(request).items() if v is not None}
    errors = []
    if not request.sip_id:
        errors.append("missing sip_id")
    if len(payload) <= 1:
        errors.append("provide at least one field to modify")
    if request.amount is not None and request.amount < config.min_sip_amount:
        errors.append(f"amount {request.amount} below minimum {config.min_sip_amount}")
    if request.amount is not None and request.amount > config.max_sip_amount:
        errors.append(f"amount {request.amount} exceeds max {config.max_sip_amount}")
    if request.instalment_day is not None and not (1 <= request.instalment_day <= 31):
        errors.append("instalment_day must be between 1 and 31")

    if errors or dry_run:
        return {
            "dry_run": dry_run,
            "live_execution_enabled": config.live_execution_enabled,
            "validation": {"ok": not errors, "payload": payload, "errors": errors},
            "result": None,
        }
    if not config.live_execution_enabled:
        return {
            "dry_run": dry_run,
            "live_execution_enabled": config.live_execution_enabled,
            "validation": {"ok": True, "payload": payload, "errors": []},
            "result": None,
            "errors": ["Live MF execution disabled. Set AT_MF_ENABLE_LIVE=1 to modify SIPs."],
        }

    result = kite.modify_mf_sip(
        request.sip_id,
        amount=request.amount,
        status=request.status,
        instalments=request.instalments,
        frequency=request.frequency,
        instalment_day=request.instalment_day,
    )
    return {
        "dry_run": dry_run,
        "live_execution_enabled": config.live_execution_enabled,
        "validation": {"ok": True, "payload": payload, "errors": []},
        "result": result,
    }


def execute_sip_cancel(kite, sip_id: str, dry_run: bool = True, config: MFExecutionConfig | None = None) -> dict[str, Any]:
    config = config or MFExecutionConfig()
    if not sip_id or dry_run:
        return {
            "dry_run": dry_run,
            "live_execution_enabled": config.live_execution_enabled,
            "validation": {"ok": bool(sip_id), "sip_id": sip_id, "errors": [] if sip_id else ["missing sip_id"]},
            "result": None,
        }
    if not config.live_execution_enabled:
        return {
            "dry_run": dry_run,
            "live_execution_enabled": config.live_execution_enabled,
            "validation": {"ok": True, "sip_id": sip_id, "errors": []},
            "result": None,
            "errors": ["Live MF execution disabled. Set AT_MF_ENABLE_LIVE=1 to cancel SIPs."],
        }
    result = kite.cancel_mf_sip(sip_id)
    return {
        "dry_run": dry_run,
        "live_execution_enabled": config.live_execution_enabled,
        "validation": {"ok": True, "sip_id": sip_id, "errors": []},
        "result": result,
    }


def build_buy_orders_from_target_amounts(target_amounts: dict[str, float], tag: str = "mf_rebalance") -> list[MFOrderRequest]:
    orders: list[MFOrderRequest] = []
    for symbol, amount in target_amounts.items():
        amt = _safe_float(amount)
        if not symbol or amt is None or amt <= 0:
            continue
        orders.append(MFOrderRequest(tradingsymbol=str(symbol).strip().upper(), transaction_type="BUY", amount=amt, tag=tag))
    return orders


def _normalize_weights(symbols: list[str], weights: list[float] | None) -> dict[str, float]:
    clean_symbols = [s.strip().upper() for s in symbols if s and s.strip()]
    if not clean_symbols:
        return {}
    if not weights:
        weight = 1.0 / len(clean_symbols)
        return {symbol: weight for symbol in clean_symbols}
    raw = []
    for weight in weights[: len(clean_symbols)]:
        raw.append(max(0.0, float(weight)))
    while len(raw) < len(clean_symbols):
        raw.append(0.0)
    total = sum(raw)
    if total <= 0:
        equal = 1.0 / len(clean_symbols)
        return {symbol: equal for symbol in clean_symbols}
    return {symbol: raw[idx] / total for idx, symbol in enumerate(clean_symbols)}


def _build_redeem_orders(redeem_amount: float, holdings_index: dict[str, dict[str, Any]], instruments: dict[str, dict[str, Any]], redeem_symbols: list[str], redeem_weights: list[float] | None, tag: str) -> tuple[list[MFOrderRequest], list[str]]:
    orders: list[MFOrderRequest] = []
    notes: list[str] = []
    weights = _normalize_weights(redeem_symbols, redeem_weights)
    for symbol, weight in weights.items():
        holding = holdings_index.get(symbol)
        instrument = instruments.get(symbol)
        if holding is None or instrument is None:
            notes.append(f"missing holding/instrument for redemption symbol {symbol}")
            continue
        qty = _safe_float(holding.get("quantity")) or 0.0
        nav = _safe_float(holding.get("last_price") or holding.get("nav")) or 0.0
        if qty <= 0 or nav <= 0:
            notes.append(f"cannot compute redemption quantity for {symbol}")
            continue
        target_amount = redeem_amount * weight
        requested_qty = target_amount / nav
        raw_qty = min(qty, requested_qty)
        if requested_qty > qty:
            notes.append(f"redemption for {symbol} capped at available holding quantity {round(qty, 6)}")
        step = _safe_float(instrument.get("redemption_quantity_multiplier")) or 0.001
        min_qty = _safe_float(instrument.get("minimum_redemption_quantity")) or step
        final_qty = _round_down_to_step(raw_qty, step)
        if final_qty < min_qty:
            notes.append(f"redemption size for {symbol} below minimum quantity after rounding")
            continue
        orders.append(MFOrderRequest(tradingsymbol=symbol, transaction_type="SELL", quantity=round(final_qty, 6), tag=tag))
    return orders, notes


def build_rebalance_plan(report: dict, kite, buy_symbols: list[str] | None = None, buy_weights: list[float] | None = None, redeem_symbols: list[str] | None = None, redeem_weights: list[float] | None = None, min_ticket: float = 500.0, tag: str = "mf_rebalance") -> dict[str, Any]:
    buy_symbols = buy_symbols or []
    redeem_symbols = redeem_symbols or []
    mf_delta = float((report.get("rebalance_advice_inr") or {}).get("MF", 0.0) or 0.0)
    orders: list[MFOrderRequest] = []
    notes: list[str] = []
    instruments = fetch_mf_instrument_index(kite)
    holdings_index = fetch_mf_holdings_index(kite)

    if mf_delta > 0:
        weights = _normalize_weights(buy_symbols, buy_weights)
        if not weights:
            notes.append("MF allocation wants buys but no buy symbols were provided")
        else:
            target_amounts = {}
            for symbol, weight in weights.items():
                amount = mf_delta * weight
                if amount >= min_ticket:
                    target_amounts[symbol] = amount
                else:
                    notes.append(f"skipped {symbol} because allocated amount {round(amount, 2)} is below min ticket {min_ticket}")
            orders.extend(build_buy_orders_from_target_amounts(target_amounts, tag=tag))
    elif mf_delta < 0:
        redeem_amount = abs(mf_delta)
        if not redeem_symbols:
            notes.append("MF allocation wants redemptions but no redeem symbols were provided")
        else:
            redeem_orders, redeem_notes = _build_redeem_orders(redeem_amount, holdings_index, instruments, redeem_symbols, redeem_weights, tag)
            orders.extend(redeem_orders)
            notes.extend(redeem_notes)
    else:
        notes.append("No MF rebalance action required from current report")

    return {
        "generated_at": datetime.now().isoformat(),
        "tag": tag,
        "report_mf_delta": round(mf_delta, 2),
        "buy_symbols": [s.strip().upper() for s in buy_symbols if s and s.strip()],
        "redeem_symbols": [s.strip().upper() for s in redeem_symbols if s and s.strip()],
        "min_ticket": min_ticket,
        "orders": [asdict(o) for o in orders],
        "notes": notes,
    }
