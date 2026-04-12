import importlib.util
import sys
import unittest
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "Auto_Trader" / "rnn_lab.py"
SPEC = importlib.util.spec_from_file_location("rnn_lab", MODULE_PATH)
rnn_lab = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
sys.modules[SPEC.name] = rnn_lab
SPEC.loader.exec_module(rnn_lab)


class RNNLabTests(unittest.TestCase):
    def test_train_symbol_model_builds_probabilities(self):
        n = 180
        x = np.arange(n, dtype=float)
        close = 100 + 0.15 * x + 2.0 * np.sin(x / 8.0)
        df = pd.DataFrame({
            "Close": close,
            "Volume": 1000 + 20 * np.sin(x / 5.0),
            "RSI": 50 + 10 * np.sin(x / 7.0),
            "MACD_Hist": np.sin(x / 6.0),
            "ADX": 20 + 5 * np.cos(x / 9.0),
            "ATR": 1.5 + 0.1 * np.sin(x / 10.0),
            "EMA20": pd.Series(close).rolling(20, min_periods=1).mean(),
            "EMA50": pd.Series(close).rolling(50, min_periods=1).mean(),
            "CMF": 0.05 * np.sin(x / 11.0),
            "OBV_ZScore20": np.tanh((x - 90) / 20.0),
            "Stochastic_%K": 50 + 20 * np.sin(x / 4.0),
        })
        cfg = rnn_lab.RNNOverlayConfig(enabled=True, epochs=2, seq_len=15, hidden_size=8)
        model = rnn_lab.train_symbol_model("TEST", df, config=cfg)
        self.assertIsNotNone(model)
        self.assertGreater(model.metrics["samples"], 50)
        self.assertTrue(model.probabilities.notna().sum() > 20)


if __name__ == "__main__":
    unittest.main()
