"""Versioned contract for paper target-weight signals."""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping
from typing import Any

SCHEMA_VERSION = "paper_signal_v2_target_weights"
_NON_SEMANTIC_KEYS = {"label", "labels", "metadata", "name"}


def _semantic(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {
            str(key): _semantic(item)
            for key, item in value.items()
            if str(key).lower() not in _NON_SEMANTIC_KEYS
        }
    if isinstance(value, (list, tuple)):
        return [_semantic(item) for item in value]
    return value


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _sha256(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def params_fingerprint(params: Mapping[str, Any]) -> str:
    """Hash only canonical strategy semantics, excluding labels/metadata."""
    return _sha256(_semantic(params))


def _normalized_allocations(
    target_weights: Mapping[str, float], target_cash_weight: float
) -> tuple[dict[str, float], float]:
    weights = {str(symbol): float(weight) for symbol, weight in target_weights.items()}
    cash = float(target_cash_weight)
    values = [*weights.values(), cash]
    if any(not math.isfinite(value) or value < 0 for value in values):
        raise ValueError("target weights and cash must be finite and non-negative")
    total = sum(values)
    if total <= 0:
        raise ValueError("target allocation total must be positive")
    normalized = {symbol: round(weight / total, 12) for symbol, weight in sorted(weights.items())}
    normalized_cash = round(cash / total, 12)
    drift = round(1.0 - (sum(normalized.values()) + normalized_cash), 12)
    normalized_cash = round(normalized_cash + drift, 12)
    return normalized, normalized_cash


def build_paper_signal(
    *,
    params: Mapping[str, Any],
    signal_date: str,
    target_weights: Mapping[str, float],
    target_cash_weight: float = 0.0,
    modeled_execution_date: str | None = None,
    modeled_execution_open: Mapping[str, float] | None = None,
    metadata: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a deterministic v2 paper signal with normalized allocations."""
    weights, cash = _normalized_allocations(target_weights, target_cash_weight)
    fingerprint = params_fingerprint(params)
    identity = {
        "schema_version": SCHEMA_VERSION,
        "params_fingerprint": fingerprint,
        "signal_date": str(signal_date),
        "target_weights": weights,
    }
    opens = None
    if modeled_execution_open is not None:
        opens = {
            str(symbol): float(value)
            for symbol, value in sorted(modeled_execution_open.items())
            if math.isfinite(float(value)) and float(value) > 0
        }
    return {
        "schema_version": SCHEMA_VERSION,
        "signal_id": _sha256(identity),
        "params_fingerprint": fingerprint,
        "signal_date": str(signal_date),
        "modeled_execution_date": modeled_execution_date,
        "modeled_execution_open": opens,
        "target_weights": weights,
        "target_cash_weight": cash,
        "metadata": dict(metadata or {}),
    }
