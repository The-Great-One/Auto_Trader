#!/usr/bin/env python3
"""Build an RSI-only historical dataset from Tickertape chart data.

The broker cache is used only to discover the legacy symbol universe. Price rows
come from Tickertape's 1-year inter-day chart, so RSI research and paper signals
no longer depend on Kite authentication or potentially poisoned broker files.
Output is isolated in ``Tickertape_Hist_Data`` and swapped atomically only when
coverage and freshness gates pass. Hybrid seed history is explicit opt-in.
"""
from __future__ import annotations

import argparse
import json
import math
import shutil
import sys
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SOURCE = ROOT / "intermediary_files" / "Hist_Data"
DEFAULT_OUTPUT = ROOT / "intermediary_files" / "Tickertape_Hist_Data"
DEFAULT_SID_CACHE = ROOT / "intermediary_files" / "tickertape_sid_map.json"


class TickertapeDataError(RuntimeError):
    """Raised when Tickertape data cannot safely update the RSI dataset."""


def select_exact_stock_sid(symbol: str, payload: Any) -> str | None:
    """Return one exact stock SID; reject ambiguous search responses."""
    stocks = payload.get("stocks", []) if isinstance(payload, dict) else []
    matches = [
        str(item.get("sid", "")).strip()
        for item in stocks
        if isinstance(item, dict)
        and str(item.get("ticker", "")).strip().upper() == symbol.strip().upper()
        and str(item.get("sid", "")).strip()
    ]
    unique = list(dict.fromkeys(matches))
    if len(unique) > 1:
        raise TickertapeDataError(
            f"ambiguous exact Tickertape matches for {symbol}: {len(unique)} SIDs"
        )
    return unique[0] if unique else None


def sid_map_from_screener(payload: Any) -> dict[str, str]:
    """Build a ticker-to-SID map, omitting ambiguous screener entries."""
    results = payload.get("results", []) if isinstance(payload, dict) else []
    candidates: dict[str, set[str]] = {}
    for result in results:
        if not isinstance(result, dict):
            continue
        stock = result.get("stock", {})
        info = stock.get("info", {}) if isinstance(stock, dict) else {}
        ticker = str(info.get("ticker", "")).strip().upper() if isinstance(info, dict) else ""
        sid = str(result.get("sid", "")).strip()
        if ticker and sid:
            candidates.setdefault(ticker, set()).add(sid)

    unambiguous = {
        ticker: next(iter(sids))
        for ticker, sids in candidates.items()
        if len(sids) == 1
    }
    sid_counts: dict[str, int] = {}
    for sid in unambiguous.values():
        sid_counts[sid] = sid_counts.get(sid, 0) + 1
    mapping = {
        ticker: sid
        for ticker, sid in unambiguous.items()
        if sid_counts[sid] == 1
    }
    if not mapping:
        raise TickertapeDataError("public screener returned no unambiguous ticker/SID mappings")
    return mapping


def chart_to_frame(
    raw_chart: Any,
    *,
    expected_sid: str,
    as_of: pd.Timestamp | datetime | None = None,
    max_age_days: int = 5,
    min_points: int = 200,
) -> pd.DataFrame:
    """Validate one inter-day chart and convert it to daily OHLCV rows."""
    if not isinstance(raw_chart, list) or len(raw_chart) != 1:
        raise TickertapeDataError("Indian inter-day chart must contain one series")
    series = raw_chart[0]
    if not isinstance(series, dict):
        raise TickertapeDataError("invalid Tickertape chart series")
    actual_sid = str(series.get("sid", "")).strip()
    if actual_sid != expected_sid:
        raise TickertapeDataError(
            f"SID mismatch: expected {expected_sid}, received {actual_sid or 'missing'}"
        )
    points = series.get("points", [])
    if not isinstance(points, list):
        raise TickertapeDataError("Tickertape chart points are not a list")

    rows: list[dict[str, float | pd.Timestamp]] = []
    for point in points:
        if not isinstance(point, dict):
            continue
        try:
            date = pd.Timestamp(point["ts"])
            if date.tzinfo is not None:
                date = date.tz_convert(None)
            date = date.normalize()
            close = float(point["lp"])
            volume = float(point.get("v", 0) or 0)
        except (KeyError, TypeError, ValueError):
            continue
        if not math.isfinite(close) or close <= 0:
            continue
        if not math.isfinite(volume) or volume < 0:
            continue
        rows.append(
            {
                "Date": date,
                "Open": close,
                "High": close,
                "Low": close,
                "Close": close,
                "Volume": volume,
            }
        )

    frame = pd.DataFrame(rows)
    if frame.empty:
        raise TickertapeDataError(f"{expected_sid}: no valid chart points")
    frame = frame.sort_values("Date").drop_duplicates("Date", keep="last").reset_index(drop=True)
    if len(frame) < min_points:
        raise TickertapeDataError(
            f"{expected_sid}: only {len(frame)} points (minimum {min_points})"
        )

    check_date = pd.Timestamp.now().tz_localize(None).normalize() if as_of is None else pd.Timestamp(as_of).tz_localize(None).normalize()
    latest_date = pd.Timestamp(frame["Date"].iloc[-1]).normalize()
    age_days = int((check_date - latest_date).days)
    if age_days > max_age_days:
        raise TickertapeDataError(
            f"{expected_sid}: latest chart date {latest_date.date()} is stale by {age_days} days"
        )
    return frame[["Date", "Open", "High", "Low", "Close", "Volume"]]


def _normalise_seed(seed: pd.DataFrame) -> pd.DataFrame:
    columns = {str(col).lower(): col for col in seed.columns}
    date_col = next((columns[name] for name in ("date", "datetime") if name in columns), None)
    close_col = columns.get("close")
    if date_col is None or close_col is None:
        raise TickertapeDataError("seed file requires Date and Close columns")

    result = pd.DataFrame()
    result["Date"] = pd.to_datetime(seed[date_col], errors="coerce").dt.tz_localize(None)
    result["Close"] = pd.to_numeric(seed[close_col], errors="coerce")
    for output_name, source_name in (
        ("Open", "open"), ("High", "high"), ("Low", "low"), ("Volume", "volume")
    ):
        source_col = columns.get(source_name)
        result[output_name] = (
            pd.to_numeric(seed[source_col], errors="coerce")
            if source_col is not None
            else (result["Close"] if output_name != "Volume" else 0.0)
        )
    result = result.dropna(subset=["Date", "Close"])
    result = result[result["Close"] > 0]
    return result[["Date", "Open", "High", "Low", "Close", "Volume"]]


def merge_with_seed(seed: pd.DataFrame, tickertape: pd.DataFrame) -> pd.DataFrame:
    """Overlay Tickertape's recent daily rows on a long-history seed."""
    old = _normalise_seed(seed)
    recent = _normalise_seed(tickertape)
    # Drop the full overlap explicitly. Relying on sort/drop_duplicates can let
    # unstable sort order retain a poisoned seed row instead of Tickertape.
    old = old[old["Date"] < recent["Date"].min()]
    merged = pd.concat([old, recent], ignore_index=True)
    merged = merged.sort_values("Date").drop_duplicates("Date", keep="last").reset_index(drop=True)
    if merged["Date"].duplicated().any() or not merged["Date"].is_monotonic_increasing:
        raise TickertapeDataError("merged history has duplicate or unsorted dates")
    return merged[["Date", "Open", "High", "Low", "Close", "Volume"]]


def compose_output_history(
    seed: pd.DataFrame,
    tickertape: pd.DataFrame,
    *,
    include_seed_history: bool = False,
) -> pd.DataFrame:
    """Use pure Tickertape history by default; hybrid seeding is opt-in."""
    if include_seed_history:
        return merge_with_seed(seed, tickertape)
    return _normalise_seed(tickertape).sort_values("Date").reset_index(drop=True)


def _eligible_seed_files(source_dir: Path, min_rows: int) -> list[Path]:
    eligible: list[Path] = []
    for path in sorted(source_dir.glob("*.feather")):
        symbol = path.stem.upper()
        if any(marker in symbol for marker in ("FUT", "OPT", "-I", "-II")):
            continue
        try:
            frame = pd.read_feather(path)
        except Exception:
            continue
        if len(frame) >= min_rows:
            eligible.append(path)
    return eligible


def _load_sid_cache(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return {}
    mapping = payload.get("symbols", payload) if isinstance(payload, dict) else {}
    if not isinstance(mapping, dict):
        return {}
    return {
        str(symbol).upper(): str(sid)
        for symbol, sid in mapping.items()
        if str(symbol).strip() and str(sid).strip()
    }


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(json.dumps(payload, indent=2, sort_keys=True))
    temp.replace(path)


def required_success_count(total: int, min_coverage: float) -> int:
    if total <= 0:
        return 0
    coverage_required = math.ceil(total * min_coverage)
    breadth_floor = min(50, total)
    return max(breadth_floor, coverage_required)


def build_dataset(
    client: Any,
    *,
    source_dir: Path,
    output_dir: Path,
    sid_cache_path: Path,
    min_seed_rows: int = 700,
    min_chart_points: int = 200,
    min_coverage: float = 0.8,
    max_age_days: int = 5,
    include_seed_history: bool = False,
    limit: int = 0,
) -> dict[str, Any]:
    seed_files = _eligible_seed_files(source_dir, min_seed_rows)
    if limit > 0:
        seed_files = seed_files[:limit]
    if not seed_files:
        raise TickertapeDataError("no eligible seed files")

    sid_map = _load_sid_cache(sid_cache_path)
    try:
        bulk_map = sid_map_from_screener(
            client.screener_query({
                "match": {},
                "sortBy": "mrktCapf",
                "sortOrder": -1,
                "project": ["mrktCapf"],
                "offset": 0,
                "count": 6000,
            })
        )
        sid_map.update(bulk_map)
        _write_json_atomic(
            sid_cache_path,
            {"updated_at": datetime.now().isoformat(), "symbols": sid_map},
        )
    except Exception as exc:
        if len(sid_map) < 50:
            raise TickertapeDataError(f"unable to bootstrap SID map: {exc}") from exc

    used_sids: dict[str, str] = {}
    staging = Path(tempfile.mkdtemp(prefix="tickertape-hist-", dir=str(output_dir.parent)))
    failures: dict[str, str] = {}
    successes: list[str] = []

    try:
        for seed_path in seed_files:
            symbol = seed_path.stem.upper()
            sid = sid_map.get(symbol)
            try:
                if not sid:
                    raise TickertapeDataError("no exact Tickertape stock match in screener map")
                owner = used_sids.get(sid)
                if owner and owner != symbol:
                    raise TickertapeDataError(f"SID {sid} already mapped to {owner}")
                used_sids[sid] = symbol

                raw_chart = client.stock_inter_chart(sid, duration="1y")
                recent = chart_to_frame(
                    raw_chart,
                    expected_sid=sid,
                    max_age_days=max_age_days,
                    min_points=min_chart_points,
                )
                seed_frame = pd.read_feather(seed_path)
                merged = compose_output_history(
                    seed_frame,
                    recent,
                    include_seed_history=include_seed_history,
                )
                required_rows = min_seed_rows if include_seed_history else min_chart_points
                if len(merged) < required_rows:
                    raise TickertapeDataError(
                        f"output history has {len(merged)} rows (minimum {required_rows})"
                    )
                merged.to_feather(staging / seed_path.name)
                successes.append(symbol)
            except Exception as exc:
                failures[symbol] = str(exc)[:300]

        coverage = len(successes) / len(seed_files)
        required = required_success_count(len(seed_files), min_coverage)
        if len(successes) < required:
            raise TickertapeDataError(
                f"coverage gate failed: {len(successes)}/{len(seed_files)} "
                f"({coverage:.1%}); require {required}"
            )

        manifest = {
            "generated_at": datetime.now().isoformat(),
            "source": "tickertape_1y_plus_seed" if include_seed_history else "tickertape_1y",
            "seed_dir": str(source_dir),
            "symbols_total": len(seed_files),
            "symbols_synced": len(successes),
            "coverage": round(coverage, 4),
            "symbols": successes,
            "failures": failures,
        }
        (staging / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True))

        backup = output_dir.with_name(output_dir.name + ".previous")
        if backup.exists():
            shutil.rmtree(backup)
        if output_dir.exists():
            output_dir.replace(backup)
        staging.replace(output_dir)
        if backup.exists():
            shutil.rmtree(backup)
        return manifest
    except Exception:
        if staging.exists():
            shutil.rmtree(staging)
        raise


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-dir", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--sid-cache", type=Path, default=DEFAULT_SID_CACHE)
    parser.add_argument("--min-seed-rows", type=int, default=700)
    parser.add_argument("--min-chart-points", type=int, default=200)
    parser.add_argument("--min-coverage", type=float, default=0.8)
    parser.add_argument("--max-age-days", type=int, default=5)
    parser.add_argument(
        "--include-seed-history",
        action="store_true",
        help="Opt in to pre-Tickertape seed rows; default output is pure Tickertape 1y data",
    )
    parser.add_argument("--limit", type=int, default=0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not args.source_dir.is_dir():
        print(f"ERROR: source directory not found: {args.source_dir}", file=sys.stderr)
        return 1
    args.output_dir.parent.mkdir(parents=True, exist_ok=True)
    try:
        from tickertape_api import TickertapeClient
    except ImportError:
        print(
            "ERROR: tickertape-api-client is not installed; install the local package into this Python environment",
            file=sys.stderr,
        )
        return 1

    try:
        manifest = build_dataset(
            TickertapeClient(),
            source_dir=args.source_dir,
            output_dir=args.output_dir,
            sid_cache_path=args.sid_cache,
            min_seed_rows=args.min_seed_rows,
            min_chart_points=args.min_chart_points,
            min_coverage=args.min_coverage,
            max_age_days=args.max_age_days,
            include_seed_history=args.include_seed_history,
            limit=args.limit,
        )
    except TickertapeDataError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    print(
        f"Tickertape Hist_Data: {manifest['symbols_synced']}/{manifest['symbols_total']} "
        f"({manifest['coverage']:.1%}) -> {args.output_dir}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
