#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

try:
    from sklearn.ensemble import HistGradientBoostingClassifier  # type: ignore
except Exception:
    HistGradientBoostingClassifier = None

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from Auto_Trader import RULE_SET_7
from scripts import weekly_strategy_lab as lab

OUT_DIR = ROOT / "reports"
STATUS_PATH = OUT_DIR / "meta_label_lab_latest.json"
HISTORY_PATH = OUT_DIR / "meta_label_lab_history.jsonl"

DEFAULT_BUY = {
    "adx_min": 10,
    "volume_confirm_mult": 0.85,
    "ich_cloud_bull": 0,
    "regime_filter_enabled": 1,
    "regime_ema_fast": 50,
    "regime_ema_slow": 200,
}
DEFAULT_SELL = {"breakeven_trigger_pct": 4.0}

META_COLS = {
    "symbol",
    "date",
    "label",
    "entry_close",
    "forward_max_return_pct",
    "forward_end_return_pct",
    "forward_min_return_pct",
}


class NumpyLogisticMetaModel:
    def __init__(self, learning_rate: float = 0.05, max_iter: int = 1200, l2: float = 1e-3):
        self.learning_rate = learning_rate
        self.max_iter = max_iter
        self.l2 = l2
        self.fill_values: np.ndarray | None = None
        self.means: np.ndarray | None = None
        self.scales: np.ndarray | None = None
        self.weights: np.ndarray | None = None
        self.bias: float = 0.0
        self.constant_prob: float | None = None

    def _prepare(self, X: pd.DataFrame | np.ndarray, fit: bool = False) -> np.ndarray:
        arr = np.asarray(X, dtype=float)
        if fit:
            self.fill_values = np.nanmedian(arr, axis=0)
            self.fill_values = np.where(np.isfinite(self.fill_values), self.fill_values, 0.0)
        if self.fill_values is None:
            raise RuntimeError("Model not fitted")
        arr = np.where(np.isfinite(arr), arr, self.fill_values)
        if fit:
            self.means = arr.mean(axis=0)
            self.scales = arr.std(axis=0)
            self.scales = np.where(self.scales > 1e-9, self.scales, 1.0)
        if self.means is None or self.scales is None:
            raise RuntimeError("Model not fitted")
        return (arr - self.means) / self.scales

    @staticmethod
    def _sigmoid(z: np.ndarray) -> np.ndarray:
        return 1.0 / (1.0 + np.exp(-np.clip(z, -30.0, 30.0)))

    def fit(self, X: pd.DataFrame, y: pd.Series | np.ndarray) -> "NumpyLogisticMetaModel":
        y_arr = np.asarray(y, dtype=float)
        X_arr = self._prepare(X, fit=True)
        if len(np.unique(y_arr)) < 2:
            self.constant_prob = float(y_arr.mean()) if len(y_arr) else 0.0
            self.weights = np.zeros(X_arr.shape[1], dtype=float)
            self.bias = 0.0
            return self
        self.constant_prob = None
        self.weights = np.zeros(X_arr.shape[1], dtype=float)
        self.bias = 0.0
        n = max(1, len(y_arr))
        for _ in range(self.max_iter):
            logits = X_arr @ self.weights + self.bias
            probs = self._sigmoid(logits)
            error = probs - y_arr
            grad_w = (X_arr.T @ error) / n + self.l2 * self.weights
            grad_b = float(error.mean())
            self.weights -= self.learning_rate * grad_w
            self.bias -= self.learning_rate * grad_b
        return self

    def predict_proba(self, X: pd.DataFrame) -> np.ndarray:
        X_arr = self._prepare(X, fit=False)
        if self.constant_prob is not None:
            probs = np.full(len(X_arr), self.constant_prob, dtype=float)
        else:
            if self.weights is None:
                raise RuntimeError("Model not fitted")
            probs = self._sigmoid(X_arr @ self.weights + self.bias)
        return np.column_stack([1.0 - probs, probs])


def now_iso() -> str:
    return datetime.now().astimezone().isoformat()


def write_status(payload: dict) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    STATUS_PATH.write_text(json.dumps(payload, indent=2))
    with HISTORY_PATH.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload) + "\n")


def _latest_report(prefix: str) -> Path | None:
    files = sorted(
        [p for p in OUT_DIR.glob(f"{prefix}_*.json") if p.name != f"{prefix}_latest.json"],
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    return files[0] if files else None


def load_anchor() -> tuple[str, dict, dict, list[str], Path | None]:
    for prefix in ["regime_filter_lab", "focused_cluster_lab"]:
        report = _latest_report(prefix)
        if report is None:
            continue
        try:
            payload = json.loads(report.read_text())
            rec = payload.get("recommendation", {}) or {}
            best = rec.get("best", {}) or {}
            params = best.get("params", {}) or {}
            symbols = ((rec.get("data_context", {}) or {}).get("loaded_symbols", [])) or []
            buy = dict(params.get("buy", {}) or DEFAULT_BUY)
            sell = dict(params.get("sell", {}) or DEFAULT_SELL)
            if buy:
                return (
                    str(best.get("name") or rec.get("anchor_variant") or prefix),
                    buy,
                    sell,
                    [str(x).upper() for x in symbols if str(x).strip()],
                    report,
                )
        except Exception:
            continue
    return "regime_filter_007", dict(DEFAULT_BUY), dict(DEFAULT_SELL), [], None


def extract_signal_rows(data_map: dict[str, pd.DataFrame], buy_params: dict, horizon: int, target_return: float, max_adverse_excursion: float) -> pd.DataFrame:
    old_r7 = dict(RULE_SET_7.CONFIG)
    RULE_SET_7.CONFIG.update(buy_params)
    rows: list[dict] = []
    empty_holdings = pd.DataFrame(columns=["instrument_token", "tradingsymbol", "average_price", "quantity", "t1_quantity", "bars_in_trade"])
    warmup = max(250, int(os.getenv("AT_META_LABEL_WARMUP_BARS", "250") or 250))
    try:
        for symbol, df in data_map.items():
            use = df.sort_values("Date").reset_index(drop=True)
            for i in range(warmup, max(warmup, len(use) - horizon)):
                part = use.iloc[: i + 1].copy()
                row = part.iloc[-1].to_dict()
                row.setdefault("instrument_token", 1626369)
                decision, details = RULE_SET_7.evaluate_signal(part, row, empty_holdings)
                if str(decision).upper() != "BUY":
                    continue
                future = use.iloc[i + 1 : i + 1 + horizon].copy()
                if future.empty:
                    continue
                entry_close = float(part.iloc[-1]["Close"])
                fwd_close = pd.to_numeric(future["Close"], errors="coerce").dropna()
                if fwd_close.empty or entry_close <= 0:
                    continue
                max_ret = float((fwd_close.max() / entry_close) - 1.0)
                min_ret = float((fwd_close.min() / entry_close) - 1.0)
                end_ret = float((fwd_close.iloc[-1] / entry_close) - 1.0)
                metric_snapshot = dict((details or {}).get("metric_snapshot", {}) or {})
                gate_status = dict((details or {}).get("gate_status", {}) or {})
                feature_row = {
                    "symbol": symbol,
                    "date": pd.to_datetime(part.iloc[-1]["Date"]).isoformat(),
                    "label": int(max_ret >= target_return and min_ret > -max_adverse_excursion),
                    "entry_close": round(entry_close, 6),
                    "forward_max_return_pct": round(max_ret * 100.0, 4),
                    "forward_end_return_pct": round(end_ret * 100.0, 4),
                    "forward_min_return_pct": round(min_ret * 100.0, 4),
                }
                for key, value in metric_snapshot.items():
                    feature_row[f"metric__{key}"] = pd.to_numeric(value, errors="coerce")
                for key, value in gate_status.items():
                    feature_row[f"gate__{key}"] = 1.0 if bool(value) else 0.0
                rows.append(feature_row)
    finally:
        RULE_SET_7.CONFIG.clear()
        RULE_SET_7.CONFIG.update(old_r7)
    out = pd.DataFrame(rows)
    if out.empty:
        raise RuntimeError("No BUY signals found for meta-label dataset")
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    return out.sort_values(["date", "symbol"]).reset_index(drop=True)


def accuracy_score_binary(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    if len(y_true) == 0:
        return 0.0
    return float((y_true == y_pred).mean())


def precision_score_binary(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    tp = int(((y_true == 1) & (y_pred == 1)).sum())
    fp = int(((y_true == 0) & (y_pred == 1)).sum())
    return float(tp / (tp + fp)) if (tp + fp) > 0 else 0.0


def recall_score_binary(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    tp = int(((y_true == 1) & (y_pred == 1)).sum())
    fn = int(((y_true == 1) & (y_pred == 0)).sum())
    return float(tp / (tp + fn)) if (tp + fn) > 0 else 0.0


def roc_auc_score_binary(y_true: np.ndarray, scores: np.ndarray) -> float | None:
    pos = y_true == 1
    neg = y_true == 0
    n_pos = int(pos.sum())
    n_neg = int(neg.sum())
    if n_pos == 0 or n_neg == 0:
        return None
    ranks = pd.Series(scores).rank(method="average").to_numpy()
    auc = (ranks[pos].sum() - (n_pos * (n_pos + 1) / 2.0)) / (n_pos * n_neg)
    return float(auc)


def choose_threshold(test_df: pd.DataFrame, probabilities: np.ndarray) -> tuple[dict, list[dict]]:
    scans: list[dict] = []
    best: dict | None = None
    for threshold in [0.50, 0.55, 0.60, 0.65, 0.70]:
        mask = probabilities >= threshold
        selected = test_df.loc[mask].copy()
        support = int(mask.sum())
        entry = {
            "threshold": threshold,
            "selected_signals": support,
            "selected_pct": round((support / max(1, len(test_df))) * 100.0, 2),
            "positive_rate_pct": round(float(selected["label"].mean() * 100.0) if support else 0.0, 2),
            "avg_forward_end_return_pct": round(float(selected["forward_end_return_pct"].mean()) if support else 0.0, 4),
            "avg_forward_max_return_pct": round(float(selected["forward_max_return_pct"].mean()) if support else 0.0, 4),
            "avg_forward_min_return_pct": round(float(selected["forward_min_return_pct"].mean()) if support else 0.0, 4),
        }
        scans.append(entry)
        score = (support >= 25, entry["avg_forward_end_return_pct"], entry["positive_rate_pct"], -entry["selected_pct"])
        if best is None or score > (
            best["selected_signals"] >= 25,
            best["avg_forward_end_return_pct"],
            best["positive_rate_pct"],
            -best["selected_pct"],
        ):
            best = entry
    return best or scans[0], scans


def build_model() -> tuple[object, str]:
    if HistGradientBoostingClassifier is not None:
        return (
            HistGradientBoostingClassifier(
                learning_rate=0.05,
                max_depth=3,
                max_iter=250,
                min_samples_leaf=20,
                random_state=42,
            ),
            "HistGradientBoostingClassifier",
        )
    return NumpyLogisticMetaModel(learning_rate=0.05, max_iter=1200, l2=1e-3), "NumpyLogisticMetaModel"


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    anchor_name, anchor_buy, anchor_sell, anchor_symbols, anchor_report = load_anchor()
    if anchor_symbols and not os.getenv("AT_LAB_SYMBOLS"):
        os.environ["AT_LAB_SYMBOLS"] = ",".join(anchor_symbols)
        os.environ.setdefault("AT_LAB_USE_APPROVED_UNIVERSE", "0")
    os.environ.setdefault("AT_LAB_MATCH_LIVE", "1")
    os.environ.setdefault("AT_LAB_RNN_ENABLED", "0")

    score_context = lab.load_scorecard_context()
    trade_context = lab.load_tradebook_context()
    fundamental_context = lab.load_fundamental_context()
    data_map, data_context = lab.load_data(trade_context, fundamental_context)

    horizon = max(3, int(os.getenv("AT_META_LABEL_HORIZON_BARS", "10") or 10))
    target_return = max(0.005, float(os.getenv("AT_META_LABEL_TARGET_RETURN", "0.03") or 0.03))
    max_adverse_excursion = max(0.005, float(os.getenv("AT_META_LABEL_MAX_ADVERSE", "0.025") or 0.025))

    write_status(
        {
            "generated_at": now_iso(),
            "status": "running",
            "phase": "building_dataset",
            "message": "starting meta label lab",
            "anchor_variant": anchor_name,
            "anchor_report": str(anchor_report) if anchor_report else None,
            "symbols_loaded": len(data_context.get("loaded_symbols", [])),
            "horizon_bars": horizon,
            "target_return": target_return,
            "max_adverse_excursion": max_adverse_excursion,
        }
    )

    dataset = extract_signal_rows(data_map, anchor_buy, horizon=horizon, target_return=target_return, max_adverse_excursion=max_adverse_excursion)
    split_idx = max(1, int(len(dataset) * 0.7))
    train_df = dataset.iloc[:split_idx].copy()
    test_df = dataset.iloc[split_idx:].copy()
    if train_df.empty or test_df.empty:
        raise RuntimeError(f"Meta-label dataset too small for time split: total={len(dataset)}")

    feature_cols = [c for c in dataset.columns if c not in META_COLS]
    X_train = train_df[feature_cols]
    y_train = train_df["label"].astype(int)
    X_test = test_df[feature_cols]
    y_test = test_df["label"].astype(int)

    model, algorithm_name = build_model()
    model.fit(X_train, y_train)
    probabilities = model.predict_proba(X_test)[:, 1]
    predictions = (probabilities >= 0.5).astype(int)

    chosen_threshold, threshold_scan = choose_threshold(test_df, probabilities)
    baseline_positive_rate = float(test_df["label"].mean() * 100.0)
    baseline_avg_end = float(test_df["forward_end_return_pct"].mean())
    baseline_avg_max = float(test_df["forward_max_return_pct"].mean())
    y_test_arr = y_test.to_numpy(dtype=int)

    roc_auc = roc_auc_score_binary(y_test_arr, probabilities)
    if roc_auc is not None:
        roc_auc = round(float(roc_auc), 4)

    recommendation = {
        "generated_at": now_iso(),
        "lab_type": "meta_label_signal_quality",
        "anchor_variant": anchor_name,
        "anchor_report": str(anchor_report) if anchor_report else None,
        "anchor_buy": anchor_buy,
        "anchor_sell": anchor_sell,
        "scorecard_context": score_context,
        "tradebook_context": trade_context,
        "data_context": data_context,
        "dataset": {
            "signals_total": int(len(dataset)),
            "train_signals": int(len(train_df)),
            "test_signals": int(len(test_df)),
            "positive_rate_total_pct": round(float(dataset['label'].mean() * 100.0), 2),
            "positive_rate_test_pct": round(baseline_positive_rate, 2),
            "baseline_avg_forward_end_return_pct": round(baseline_avg_end, 4),
            "baseline_avg_forward_max_return_pct": round(baseline_avg_max, 4),
            "horizon_bars": horizon,
            "target_return_pct": round(target_return * 100.0, 2),
            "max_adverse_excursion_pct": round(max_adverse_excursion * 100.0, 2),
        },
        "model": {
            "algorithm": algorithm_name,
            "feature_count": len(feature_cols),
            "accuracy": round(accuracy_score_binary(y_test_arr, predictions), 4),
            "precision": round(precision_score_binary(y_test_arr, predictions), 4),
            "recall": round(recall_score_binary(y_test_arr, predictions), 4),
            "roc_auc": roc_auc,
        },
        "threshold_scan": threshold_scan,
        "recommended_threshold": chosen_threshold,
        "notes": [
            "This is the first ML pass, using a pooled meta-label classifier over RULE_SET_7 BUY signals.",
            "RNN remains disabled; this path is a separate signal-quality filter using a lightweight classifier.",
            "Next step if this shows lift is to wire the chosen threshold into a live-parity backtest instead of signal-quality-only evaluation.",
        ],
    }
    payload = {
        "recommendation": recommendation,
        "test_predictions": test_df.assign(predicted_probability=probabilities).head(250).to_dict(orient="records"),
    }

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = OUT_DIR / f"meta_label_lab_{ts}.json"
    csv_path = OUT_DIR / f"meta_label_lab_{ts}.csv"
    json_path.write_text(json.dumps(payload, indent=2, default=str))
    test_df.assign(predicted_probability=probabilities).to_csv(csv_path, index=False)

    write_status(
        {
            "generated_at": now_iso(),
            "status": "completed",
            "phase": "done",
            "message": "meta label lab complete",
            "anchor_variant": anchor_name,
            "signals_total": int(len(dataset)),
            "test_signals": int(len(test_df)),
            "recommended_threshold": chosen_threshold,
            "model": recommendation["model"],
            "output_json": str(json_path),
            "output_csv": str(csv_path),
        }
    )
    print(json.dumps(recommendation, indent=2, default=str))
    print(f"Saved: {json_path}")
    print(f"Saved: {csv_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
