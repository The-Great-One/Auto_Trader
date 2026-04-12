from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Dict, Optional

import numpy as np
import pandas as pd

try:
    import torch
    import torch.nn as nn
except Exception:  # pragma: no cover
    torch = None
    nn = None


FEATURE_COLUMNS = [
    "Close",
    "Volume",
    "RSI",
    "MACD_Hist",
    "ADX",
    "ATR",
    "EMA20",
    "EMA50",
    "CMF",
    "OBV_ZScore20",
    "Stochastic_%K",
]


@dataclass
class RNNOverlayConfig:
    enabled: bool = False
    seq_len: int = 20
    train_ratio: float = 0.7
    hidden_size: int = 16
    epochs: int = 8
    lr: float = 0.003
    buy_threshold: float = 0.56
    sell_threshold: float = 0.44
    seed: int = 7


class TinyGRU(nn.Module):
    def __init__(self, input_size: int, hidden_size: int = 16):
        super().__init__()
        self.gru = nn.GRU(input_size=input_size, hidden_size=hidden_size, batch_first=True)
        self.head = nn.Sequential(
            nn.Linear(hidden_size, hidden_size),
            nn.ReLU(),
            nn.Linear(hidden_size, 1),
        )

    def forward(self, x):
        out, _ = self.gru(x)
        last = out[:, -1, :]
        return self.head(last).squeeze(-1)


@dataclass
class RNNSymbolModel:
    symbol: str
    probabilities: pd.Series
    train_cutoff_idx: int
    metrics: dict

    def prob_at(self, idx: int) -> Optional[float]:
        if idx < self.train_cutoff_idx:
            return None
        try:
            val = self.probabilities.iloc[idx]
        except Exception:
            return None
        if pd.isna(val):
            return None
        return float(val)


def env_flag(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def load_config() -> RNNOverlayConfig:
    return RNNOverlayConfig(
        enabled=env_flag("AT_LAB_RNN_ENABLED", False),
        seq_len=max(10, int(os.getenv("AT_LAB_RNN_SEQ_LEN", "20"))),
        train_ratio=min(0.9, max(0.55, float(os.getenv("AT_LAB_RNN_TRAIN_RATIO", "0.7")))),
        hidden_size=max(8, int(os.getenv("AT_LAB_RNN_HIDDEN", "16"))),
        epochs=max(2, int(os.getenv("AT_LAB_RNN_EPOCHS", "8"))),
        lr=max(0.0005, float(os.getenv("AT_LAB_RNN_LR", "0.003"))),
        buy_threshold=float(os.getenv("AT_LAB_RNN_BUY_THRESHOLD", "0.56")),
        sell_threshold=float(os.getenv("AT_LAB_RNN_SELL_THRESHOLD", "0.44")),
        seed=int(os.getenv("AT_LAB_RNN_SEED", "7")),
    )


def _prepare_frame(df: pd.DataFrame) -> pd.DataFrame:
    frame = df.copy()
    for col in FEATURE_COLUMNS:
        if col not in frame.columns:
            frame[col] = np.nan
    frame = frame[FEATURE_COLUMNS].apply(pd.to_numeric, errors="coerce")
    frame = frame.replace([np.inf, -np.inf], np.nan).ffill().bfill().dropna()
    return frame


def _build_sequences(frame: pd.DataFrame, seq_len: int):
    close = pd.to_numeric(frame["Close"], errors="coerce")
    next_return = close.shift(-1) / close - 1.0
    y = (next_return > 0).astype(float)

    values = frame.values.astype(np.float32)
    means = np.nanmean(values, axis=0)
    stds = np.nanstd(values, axis=0)
    stds[stds < 1e-8] = 1.0
    scaled = (values - means) / stds

    xs = []
    ys = []
    idxs = []
    for end in range(seq_len, len(frame) - 1):
        start = end - seq_len
        target = y.iloc[end]
        if pd.isna(target):
            continue
        xs.append(scaled[start:end])
        ys.append(float(target))
        idxs.append(end)
    if not xs:
        return None, None, None
    return np.asarray(xs, dtype=np.float32), np.asarray(ys, dtype=np.float32), np.asarray(idxs, dtype=np.int64)


def train_symbol_model(symbol: str, df: pd.DataFrame, config: Optional[RNNOverlayConfig] = None) -> Optional[RNNSymbolModel]:
    config = config or load_config()
    if not config.enabled or torch is None or nn is None:
        return None

    frame = _prepare_frame(df)
    xs, ys, idxs = _build_sequences(frame, config.seq_len)
    if xs is None or len(xs) < max(40, config.seq_len * 2):
        return None

    torch.manual_seed(config.seed)
    np.random.seed(config.seed)

    train_size = int(len(xs) * config.train_ratio)
    train_size = max(20, min(len(xs) - 10, train_size))
    x_train = torch.tensor(xs[:train_size])
    y_train = torch.tensor(ys[:train_size])
    x_all = torch.tensor(xs)

    model = TinyGRU(input_size=x_train.shape[-1], hidden_size=config.hidden_size)
    optimizer = torch.optim.Adam(model.parameters(), lr=config.lr)
    criterion = nn.BCEWithLogitsLoss()

    model.train()
    for _ in range(config.epochs):
        optimizer.zero_grad()
        logits = model(x_train)
        loss = criterion(logits, y_train)
        loss.backward()
        optimizer.step()

    model.eval()
    with torch.no_grad():
        probs = torch.sigmoid(model(x_all)).detach().cpu().numpy()

    prob_series = pd.Series(np.nan, index=df.index, dtype=float)
    for pos, idx in enumerate(idxs):
        if 0 <= idx < len(prob_series):
            prob_series.iloc[int(idx)] = float(probs[pos])

    cutoff_idx = int(idxs[min(train_size, len(idxs) - 1)]) if len(idxs) else len(df)
    test_slice = slice(train_size, None)
    test_preds = (probs[test_slice] >= 0.5).astype(float)
    test_true = ys[test_slice]
    accuracy = float((test_preds == test_true).mean()) if len(test_true) else 0.0

    return RNNSymbolModel(
        symbol=symbol,
        probabilities=prob_series,
        train_cutoff_idx=cutoff_idx,
        metrics={
            "samples": int(len(xs)),
            "train_samples": int(train_size),
            "test_samples": int(max(0, len(xs) - train_size)),
            "test_accuracy": round(accuracy, 4),
            "seq_len": int(config.seq_len),
            "epochs": int(config.epochs),
            "buy_threshold": float(config.buy_threshold),
            "sell_threshold": float(config.sell_threshold),
        },
    )


def build_overlay_models(data_map: Dict[str, pd.DataFrame], config: Optional[RNNOverlayConfig] = None) -> Dict[str, RNNSymbolModel]:
    config = config or load_config()
    if not config.enabled:
        return {}
    models: Dict[str, RNNSymbolModel] = {}
    for symbol, df in data_map.items():
        model = train_symbol_model(symbol, df, config=config)
        if model is not None:
            models[symbol] = model
    return models
