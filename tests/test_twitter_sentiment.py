import importlib.util
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "Auto_Trader" / "twitter_sentiment.py"
SPEC = importlib.util.spec_from_file_location("twitter_sentiment", MODULE_PATH)
ts = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
SPEC.loader.exec_module(ts)


class TwitterSentimentTests(unittest.TestCase):
    def test_classify_tweet_detects_bearish_regulatory_flow(self):
        out = ts.classify_tweet("SEBI investigation and weak guidance make this stock a sell")
        self.assertIn("bearish", out["types"])
        self.assertIn("regulatory", out["types"])
        self.assertLess(out["sentiment"], 0)

    def test_apply_sentiment_overlay_blocks_buy_on_negative_snapshot(self):
        with tempfile.TemporaryDirectory() as td:
            state_dir = Path(td)
            path = state_dir / "ABC.json"
            path.write_text(json.dumps({
                "symbol": "ABC",
                "tweet_count": 9,
                "bullish_tweets": 1,
                "bearish_tweets": 6,
                "weighted_sentiment": -0.55,
                "dominant_types": ["bearish", "regulatory"],
                "trade_bias": {
                    "block_buy": True,
                    "force_sell": True,
                    "positive_boost": False,
                    "reason": "credible bearish twitter flow -0.55",
                },
                "generated_at": 4102444800,
                "status": "ok",
            }))

            with patch.object(ts, "STATE_DIR", state_dir), patch.dict("os.environ", {"AT_TWITTER_SENTIMENT_ENABLED": "1", "AT_TWITTER_SENTIMENT_TTL_MINUTES": "90"}):
                decision, overlay = ts.apply_sentiment_overlay("BUY", "ABC", holdings=None)

            self.assertEqual(decision, "HOLD")
            self.assertEqual(overlay["action"], "blocked_buy")


if __name__ == "__main__":
    unittest.main()
