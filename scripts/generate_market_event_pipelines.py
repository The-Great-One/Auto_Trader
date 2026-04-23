#!/usr/bin/env python3
from __future__ import annotations

import json
import re
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import yfinance as yf

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from Auto_Trader.news_sentiment import SECTOR_STOCK_MAP  # type: ignore

REPORTS = ROOT / 'reports'
ARCHIVE_DIR = ROOT / 'intermediary_files' / 'news_sentiment' / 'archive'
HIST_DIR = ROOT / 'intermediary_files' / 'Hist_Data'
NEWS_BEHAVIOR_PATH = REPORTS / 'news_topic_symbol_behavior_latest.json'
EARNINGS_PIPELINE_PATH = REPORTS / 'earnings_call_pipeline_latest.json'
ECO_PATH = REPORTS / 'economic_calendar_sector_latest.json'
NEWS_LATEST_PATH = REPORTS / 'news_sentiment_latest.json'
HOLDINGS_PATH = ROOT / 'intermediary_files' / 'Holdings.feather'
UTC = timezone.utc

EARNINGS_SYMBOLS = [
    'WIPRO', 'INFY', 'TCS', 'HDFCBANK', 'ICICIBANK', 'SBIN', 'ITC', 'RELIANCE', 'LT', 'KOTAKBANK',
    'AXISBANK', 'BAJAJ-AUTO', 'HEROMOTOCO', 'M&M', 'MARUTI', 'NTPC', 'COALINDIA', 'POWERGRID',
    'ONGC', 'ADANIPORTS', 'HINDALCO', 'CIPLA', 'DRREDDY', 'SUNPHARMA', 'BHARTIARTL', 'HCLTECH',
    'HAL', 'CANBK', 'ASHOKLEY',
]

SYMBOL_ALIAS_MAP: dict[str, list[str]] = {
    'RELIANCE': ['reliance industries', 'ril'],
    'HDFCBANK': ['hdfc bank'],
    'ICICIBANK': ['icici bank'],
    'SBIN': ['state bank of india', 'sbi'],
    'KOTAKBANK': ['kotak bank', 'kotak mahindra bank'],
    'AXISBANK': ['axis bank'],
    'BANKBARODA': ['bank of baroda'],
    'SUNPHARMA': ['sun pharma', 'sun pharmaceutical'],
    'DRREDDY': ['dr reddy', 'dr reddys', 'dr. reddy'],
    'DIVISLAB': ['divis lab', 'divis laboratories'],
    'HINDUNILVR': ['hindustan unilever', 'hul'],
    'BAJAJ-AUTO': ['bajaj auto'],
    'M&M': ['mahindra & mahindra', 'mahindra and mahindra'],
    'LT': ['larsen & toubro', 'larsen and toubro', 'l&t'],
    'BPCL': ['bharat petroleum'],
    'IOC': ['indian oil'],
    'GAIL': ['gail india'],
    'MARUTI': ['maruti suzuki'],
    'TATAMOTORS': ['tata motors'],
    'HDFCBANK': ['hdfc bank'],
    'ICICIBANK': ['icici bank'],
    'BHARTIARTL': ['bharti airtel', 'airtel'],
    'HCLTECH': ['hcl tech'],
    'TECHM': ['tech mahindra'],
    'TCS': ['tata consultancy services'],
    'INFY': ['infosys'],
    'WIPRO': ['wipro'],
    'ITC': ['itc'],
    'ONGC': ['oil and natural gas corporation'],
    'ADANIPORTS': ['adani ports'],
    'HINDALCO': ['hindalco'],
    'CIPLA': ['cipla'],
    'POWERGRID': ['power grid'],
    'COALINDIA': ['coal india'],
    'CANBK': ['canara bank'],
    'ASHOKLEY': ['ashok leyland'],
    'HEROMOTOCO': ['hero motocorp'],
    'OFSS': ['oracle financial services'],
    'INDIGO': ['interglobe aviation', 'indigo'],
    'NTPC': ['ntpc'],
    'HAL': ['hindustan aeronautics'],
    'JSWSTEEL': ['jsw steel'],
    'TATASTEEL': ['tata steel'],
    'VEDL': ['vedanta'],
    'NMDC': ['nmdc'],
    'BRITANNIA': ['britannia'],
    'DABUR': ['dabur'],
    'MARICO': ['marico'],
    'NESTLEIND': ['nestle india'],
    'EICHERMOT': ['eicher motors'],
    'HINDPETRO': ['hindustan petroleum'],
}

EARNINGS_RE = re.compile(r'\b(results?|earnings?|q[1-4]|quarter|guidance|revenue|profit|ebitda|conference call|investor call)\b', re.IGNORECASE)


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text())
    except Exception:
        return default


def normalize_ohlcv(df: pd.DataFrame) -> pd.DataFrame | None:
    if df is None or df.empty:
        return None
    df = df.copy()
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]
    if 'Date' not in df.columns:
        df = df.reset_index()
    if 'Date' not in df.columns:
        return None
    df['Date'] = pd.to_datetime(df['Date'], utc=True).dt.tz_localize(None)
    for col in ['Open', 'High', 'Low', 'Close', 'Volume']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    df = df.dropna(subset=['Close']).sort_values('Date').reset_index(drop=True)
    return df if not df.empty else None


def load_local_history(symbol: str) -> pd.DataFrame | None:
    path = HIST_DIR / f'{symbol}.feather'
    if not path.exists():
        return None
    try:
        return normalize_ohlcv(pd.read_feather(path))
    except Exception:
        return None


def download_history(symbol: str, start: datetime, end: datetime) -> pd.DataFrame | None:
    candidates = [f'{symbol}.NS', f'{symbol}.BO'] if '.' not in symbol else [symbol]
    for cand in candidates:
        try:
            df = yf.download(cand, start=start.date().isoformat(), end=(end + timedelta(days=2)).date().isoformat(), interval='1d', auto_adjust=False, progress=False)
            out = normalize_ohlcv(df)
            if out is not None and not out.empty:
                return out
        except Exception:
            continue
    return None


def get_price_frame(symbol: str, cache: dict[str, pd.DataFrame], start: datetime, end: datetime) -> pd.DataFrame | None:
    if symbol in cache:
        return cache[symbol]
    start_ts = pd.Timestamp(start)
    end_ts = pd.Timestamp(end - timedelta(days=7))
    if start_ts.tzinfo is not None:
        start_ts = start_ts.tz_localize(None)
    if end_ts.tzinfo is not None:
        end_ts = end_ts.tz_localize(None)
    df = load_local_history(symbol)
    if df is None or df['Date'].min() > start_ts or df['Date'].max() < end_ts:
        df = download_history(symbol, start - timedelta(days=5), end + timedelta(days=5))
    if df is not None:
        cache[symbol] = df
    return df


def clean_text(text: str) -> str:
    text = str(text or '').lower()
    text = text.replace('&', ' and ')
    text = re.sub(r'[^a-z0-9]+', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return f" {text} "


def build_symbol_registry() -> dict[str, list[str]]:
    symbols: set[str] = set(EARNINGS_SYMBOLS)
    for names in SECTOR_STOCK_MAP.values():
        symbols.update(str(x).upper() for x in names)
    news_latest = load_json(NEWS_LATEST_PATH, {})
    for item in news_latest.get('active') or []:
        sym = str(item.get('symbol') or '').strip().upper()
        if sym:
            symbols.add(sym)
    if HOLDINGS_PATH.exists():
        try:
            holdings = pd.read_feather(HOLDINGS_PATH)
            for sym in holdings.get('tradingsymbol', pd.Series(dtype=str)).astype(str):
                symbols.add(sym.strip().upper())
        except Exception:
            pass

    registry: dict[str, list[str]] = {}
    for sym in sorted(symbols):
        aliases = {sym.lower(), sym.lower().replace('-', ' '), sym.lower().replace('&', ' and ')}
        for alias in SYMBOL_ALIAS_MAP.get(sym, []):
            aliases.add(alias.lower())
        registry[sym] = sorted(a for a in aliases if len(a.strip()) >= 3)
    return registry


def match_symbols(text: str, registry: dict[str, list[str]]) -> list[str]:
    haystack = clean_text(text)
    found: list[str] = []
    for sym, aliases in registry.items():
        for alias in aliases:
            needle = f' {clean_text(alias).strip()} '
            if needle in haystack:
                found.append(sym)
                break
    return found[:8]


def iter_archive_rows(lookback_days: int = 45) -> list[dict[str, Any]]:
    cutoff = datetime.now(UTC) - timedelta(days=lookback_days)
    rows: list[dict[str, Any]] = []
    if not ARCHIVE_DIR.exists():
        return rows
    for path in sorted(ARCHIVE_DIR.glob('*.jsonl')):
        for line in path.read_text().splitlines():
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except Exception:
                continue
            published_at = row.get('published_at') or row.get('fetched_at')
            try:
                dt = datetime.fromtimestamp(int(published_at), tz=UTC)
            except Exception:
                continue
            if dt < cutoff:
                continue
            row['_dt'] = dt
            rows.append(row)
    return rows


def infer_direction(row: dict[str, Any]) -> int:
    cls = row.get('classification') or {}
    types = set(cls.get('types') or [])
    sentiment = float(cls.get('sentiment') or 0.0)
    if 'earnings_negative' in types or 'bearish' in types or sentiment <= -0.15:
        return -1
    if 'earnings_positive' in types or 'bullish' in types or sentiment >= 0.15:
        return 1
    return 0


def direction_label(direction: int) -> str:
    if direction > 0:
        return 'positive'
    if direction < 0:
        return 'negative'
    return 'neutral'


def compute_forward_behavior(symbol: str, event_dt: datetime, cache: dict[str, pd.DataFrame]) -> dict[str, Any] | None:
    df = get_price_frame(symbol, cache, event_dt - timedelta(days=20), event_dt + timedelta(days=20))
    if df is None or df.empty:
        return None
    forward = df[df['Date'] >= pd.Timestamp(event_dt.date())].reset_index(drop=True)
    if forward.empty:
        return None
    entry = float(forward.iloc[0]['Close'])
    if not entry:
        return None
    future = forward.iloc[1:].reset_index(drop=True)
    out: dict[str, Any] = {'entry_price': round(entry, 4), 'available_bars': int(len(future))}
    for n in [1, 3, 5, 10]:
        if len(future) >= n:
            out[f'ret_{n}d_pct'] = round((float(future.iloc[n - 1]['Close']) / entry - 1.0) * 100.0, 2)
        else:
            out[f'ret_{n}d_pct'] = None
    window = future.head(5)
    if not window.empty:
        out['max_upside_5d_pct'] = round((float(window['High'].max()) / entry - 1.0) * 100.0, 2)
        out['max_drawdown_5d_pct'] = round((float(window['Low'].min()) / entry - 1.0) * 100.0, 2)
    else:
        out['max_upside_5d_pct'] = None
        out['max_drawdown_5d_pct'] = None
    return out


def average(values: list[Any]) -> float | None:
    clean = [float(v) for v in values if isinstance(v, (int, float))]
    if not clean:
        return None
    return round(sum(clean) / len(clean), 2)


def positive_rate(values: list[Any]) -> float | None:
    clean = [float(v) for v in values if isinstance(v, (int, float))]
    if not clean:
        return None
    return round(sum(1 for v in clean if v > 0) / len(clean) * 100.0, 1)


def build_news_behavior(rows: list[dict[str, Any]], registry: dict[str, list[str]]) -> dict[str, Any]:
    cache: dict[str, pd.DataFrame] = {}
    events: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()

    for row in rows:
        if row.get('kind') != 'topic':
            continue
        text = f"{row.get('title') or ''} {row.get('summary') or ''}"
        symbols = match_symbols(text, registry)
        if not symbols:
            continue
        direction = infer_direction(row)
        base = {
            'topic': row.get('key'),
            'title': row.get('title'),
            'source': row.get('source'),
            'published_at': int(row.get('published_at') or row.get('fetched_at') or 0),
            'sentiment': float(((row.get('classification') or {}).get('sentiment')) or 0.0),
            'direction': direction,
        }
        for symbol in symbols:
            dedupe_key = (str(row.get('event_id') or row.get('link') or row.get('title')), symbol)
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            behavior = compute_forward_behavior(symbol, row['_dt'], cache)
            if not behavior:
                continue
            event = {**base, 'symbol': symbol, **behavior}
            ret_3d = event.get('ret_3d_pct')
            event['alignment_3d_pct'] = round(float(ret_3d) * direction, 2) if direction and isinstance(ret_3d, (int, float)) else None
            events.append(event)

    pair_groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    topic_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for event in events:
        pair_groups[(str(event.get('topic') or ''), str(event.get('symbol') or ''))].append(event)
        topic_groups[str(event.get('topic') or '')].append(event)

    top_pairs = []
    for (topic, symbol), bucket in pair_groups.items():
        top_pairs.append({
            'topic': topic,
            'symbol': symbol,
            'events': len(bucket),
            'avg_sentiment': average([b.get('sentiment') for b in bucket]),
            'avg_ret_1d_pct': average([b.get('ret_1d_pct') for b in bucket]),
            'avg_ret_3d_pct': average([b.get('ret_3d_pct') for b in bucket]),
            'avg_ret_5d_pct': average([b.get('ret_5d_pct') for b in bucket]),
            'avg_alignment_3d_pct': average([b.get('alignment_3d_pct') for b in bucket]),
            'positive_rate_3d': positive_rate([b.get('ret_3d_pct') for b in bucket]),
            'latest_title': bucket[-1].get('title'),
            'latest_published_at': bucket[-1].get('published_at'),
        })
    top_pairs.sort(key=lambda x: (x.get('events', 0), x.get('avg_alignment_3d_pct') or -999), reverse=True)

    topic_rollup = []
    for topic, bucket in topic_groups.items():
        topic_rollup.append({
            'topic': topic,
            'events': len(bucket),
            'symbols': len({b.get('symbol') for b in bucket}),
            'avg_sentiment': average([b.get('sentiment') for b in bucket]),
            'avg_ret_3d_pct': average([b.get('ret_3d_pct') for b in bucket]),
            'avg_alignment_3d_pct': average([b.get('alignment_3d_pct') for b in bucket]),
        })
    topic_rollup.sort(key=lambda x: (x.get('avg_alignment_3d_pct') or -999, x.get('events', 0)), reverse=True)

    recent_events = sorted(events, key=lambda x: x.get('published_at', 0), reverse=True)[:20]
    return {
        'generated_at': datetime.now(UTC).isoformat(),
        'lookback_days': 45,
        'topic_rows_scanned': sum(1 for row in rows if row.get('kind') == 'topic'),
        'matched_events': len(events),
        'top_topic_symbol_pairs': top_pairs[:20],
        'topic_rollup': topic_rollup[:20],
        'recent_events': recent_events,
    }


def is_earnings_row(row: dict[str, Any]) -> bool:
    cls = row.get('classification') or {}
    types = set(cls.get('types') or [])
    if 'earnings_positive' in types or 'earnings_negative' in types:
        return True
    text = f"{row.get('title') or ''} {row.get('summary') or ''}"
    return bool(EARNINGS_RE.search(text))


def build_earnings_pipeline(rows: list[dict[str, Any]], registry: dict[str, list[str]], eco: dict[str, Any]) -> dict[str, Any]:
    cache: dict[str, pd.DataFrame] = {}
    events: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()

    for row in rows:
        if not is_earnings_row(row):
            continue
        title = str(row.get('title') or '').strip()
        summary = str(row.get('summary') or '').strip()
        headline = title or summary or None
        text = f"{title} {summary}".strip()
        symbols = match_symbols(text, registry)
        if not symbols:
            continue
        direction = infer_direction(row)
        for symbol in symbols:
            dedupe_key = (str(row.get('link') or row.get('event_id') or headline), symbol)
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            behavior = compute_forward_behavior(symbol, row['_dt'], cache)
            if not behavior:
                continue
            ret_3d = behavior.get('ret_3d_pct')
            event_sentiment = float(((row.get('classification') or {}).get('sentiment')) or 0.0)
            events.append({
                'symbol': symbol,
                'title': headline,
                'kind': row.get('kind'),
                'source': row.get('source'),
                'published_at': int(row.get('published_at') or row.get('fetched_at') or 0),
                'sentiment': event_sentiment,
                'direction': direction,
                'direction_label': direction_label(direction),
                'alignment_3d_pct': round(float(ret_3d) * direction, 2) if direction and isinstance(ret_3d, (int, float)) else None,
                **behavior,
            })

    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for event in events:
        grouped[str(event.get('symbol') or '')].append(event)

    scoreboard = []
    latest_event_by_symbol: dict[str, dict[str, Any]] = {}
    for symbol, bucket in grouped.items():
        latest_event = sorted(bucket, key=lambda x: x.get('published_at', 0), reverse=True)[0]
        latest_event_by_symbol[symbol] = latest_event
        scoreboard.append({
            'symbol': symbol,
            'events': len(bucket),
            'avg_ret_1d_pct': average([b.get('ret_1d_pct') for b in bucket]),
            'avg_ret_3d_pct': average([b.get('ret_3d_pct') for b in bucket]),
            'avg_ret_5d_pct': average([b.get('ret_5d_pct') for b in bucket]),
            'avg_alignment_3d_pct': average([b.get('alignment_3d_pct') for b in bucket]),
            'positive_rate_3d': positive_rate([b.get('ret_3d_pct') for b in bucket]),
            'latest_title': latest_event.get('title'),
            'latest_published_at': latest_event.get('published_at'),
            'latest_signal': latest_event.get('direction_label'),
            'latest_signal_score': latest_event.get('sentiment'),
            'latest_ret_1d_pct': latest_event.get('ret_1d_pct'),
            'latest_ret_3d_pct': latest_event.get('ret_3d_pct'),
        })
    scoreboard.sort(key=lambda x: (x.get('events', 0), x.get('avg_alignment_3d_pct') or -999), reverse=True)

    upcoming = []
    for row in eco.get('earnings') or []:
        symbol = str(row.get('symbol') or '').upper()
        hist = next((item for item in scoreboard if item.get('symbol') == symbol), None) or {}
        latest_event = latest_event_by_symbol.get(symbol) or {}
        upcoming.append({
            'symbol': symbol,
            'earnings_date': row.get('earnings_date'),
            'historical_events': hist.get('events', 0),
            'avg_post_earnings_3d_pct': hist.get('avg_ret_3d_pct'),
            'avg_post_earnings_5d_pct': hist.get('avg_ret_5d_pct'),
            'avg_alignment_3d_pct': hist.get('avg_alignment_3d_pct'),
            'latest_earnings_headline': latest_event.get('title'),
            'latest_signal': latest_event.get('direction_label'),
            'latest_signal_score': latest_event.get('sentiment'),
            'latest_ret_1d_pct': latest_event.get('ret_1d_pct'),
            'latest_ret_3d_pct': latest_event.get('ret_3d_pct'),
        })
    upcoming.sort(key=lambda x: (str(x.get('earnings_date') or '9999-99-99'), str(x.get('symbol') or '')))

    recent_events = sorted(events, key=lambda x: x.get('published_at', 0), reverse=True)[:20]
    return {
        'generated_at': datetime.now(UTC).isoformat(),
        'lookback_days': 45,
        'matched_events': len(events),
        'symbol_scoreboard': scoreboard[:25],
        'upcoming_with_context': upcoming[:20],
        'recent_earnings_events': recent_events,
    }


def main() -> int:
    REPORTS.mkdir(parents=True, exist_ok=True)
    registry = build_symbol_registry()
    archive_rows = iter_archive_rows(lookback_days=45)
    eco = load_json(ECO_PATH, {})

    news_behavior = build_news_behavior(archive_rows, registry)
    earnings_pipeline = build_earnings_pipeline(archive_rows, registry, eco)

    NEWS_BEHAVIOR_PATH.write_text(json.dumps(news_behavior, indent=2, ensure_ascii=False))
    EARNINGS_PIPELINE_PATH.write_text(json.dumps(earnings_pipeline, indent=2, ensure_ascii=False))
    print(str(NEWS_BEHAVIOR_PATH))
    print(str(EARNINGS_PIPELINE_PATH))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
