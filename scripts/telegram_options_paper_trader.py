#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import os
import re
import subprocess
import sys
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
REPORTS = ROOT / 'reports'
REPORTS.mkdir(exist_ok=True)

EXPORT_DEFAULT = Path(os.getenv('AT_TELEGRAM_EXPORT', os.path.expanduser('~/.openclaw/telegram-user/exports/telegram_export_latest.json')))
SSH_KEY = os.getenv('AT_SERVER_KEY', os.path.expanduser('~/Desktop/Sahil_Oracle_Keys/ssh-key-2024-10-12.key'))
ORACLE = os.getenv('AT_SERVER_HOST', os.getenv('AT_ORACLE', ''))

OPTION_CALL_RE = re.compile(
    r'Stock\s*Name-\s*#?(?P<symbol>[A-Za-z0-9&-]+).*?'
    r'Strike-\s*(?:(?P<month>[A-Za-z]{3})\s+)?(?P<strike>\d+(?:\.\d+)?)\s*(?P<side>CE|PE)\s*(?:AT\s*)?(?P<entry1>\d+(?:\.\d+)?)(?:\s*[-–]\s*(?P<entry2>\d+(?:\.\d+)?))?.*?'
    r'Lot\s*Size-\s*(?P<lot>\d+)',
    re.IGNORECASE | re.DOTALL,
)
SL_RE = re.compile(r'SL-\s*(?P<sl>\d+(?:\.\d+)?)', re.IGNORECASE)
TARGET_RE = re.compile(r'Target-\s*(?P<targets>[0-9+/\-. ]+)', re.IGNORECASE)
MONTH_MAP = {m: i for i, m in enumerate(['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC'], start=1)}
UNDERLYING_ALIAS = {
    'SBILIFE': 'SBILIFE',
    'NIFTY': 'NIFTY',
    'BANKNIFTY': 'BANKNIFTY',
    'FINNIFTY': 'FINNIFTY',
    'MIDCPNIFTY': 'MIDCPNIFTY',
    'TATAELEXI': 'TATAELXSI',
    'ADANIPORT': 'ADANIPORTS',
    'TIIND': 'TIINDIA',
    'WAAREEENR': 'WAAREEENER',
    'TDPOWER': 'TDPOWERSYS',
}


@dataclass
class OptionCall:
    channel: str
    message_id: int
    date: str
    symbol: str
    side: str
    strike: float
    month_hint: str | None
    entry_low: float
    entry_high: float
    entry_ref: float
    lot_size: int
    stop_loss: float | None
    targets: list[float]
    text: str


@dataclass
class Position:
    call: OptionCall
    tradingsymbol: str
    expiry: str
    lot_size: int
    qty: int
    entry_time: pd.Timestamp
    entry_price: float
    invested: float
    stop_loss: float | None
    remaining_qty: int
    realized_cash: float = 0.0
    target_hits: list[int] | None = None
    exit_time: pd.Timestamp | None = None
    exit_price: float | None = None
    exit_reason: str | None = None

    def __post_init__(self):
        if self.target_hits is None:
            self.target_hits = []


def _run_oracle_python(script: str, timeout: int = 60) -> dict[str, Any]:
    cmd = [
        'ssh', '-i', SSH_KEY, '-o', 'StrictHostKeyChecking=no', ORACLE,
        '/home/ubuntu/Auto_Trader/venv/bin/python - <<\'PYEOF\'\n' + script + '\nPYEOF'
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip() or 'oracle command failed')
    text = result.stdout.strip()
    if not text:
        raise RuntimeError('oracle returned empty output')
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f'invalid oracle json: {text[:400]}') from exc


def normalize_symbol(raw: str) -> str:
    return UNDERLYING_ALIAS.get(raw.upper().strip(), raw.upper().strip())


def parse_option_calls(messages: list[dict[str, Any]], channel_name: str) -> list[OptionCall]:
    calls: list[OptionCall] = []
    for m in messages:
        text = (m.get('text') or '').strip()
        if 'stock name-' not in text.lower() or 'strike-' not in text.lower():
            continue
        match = OPTION_CALL_RE.search(text)
        if not match:
            continue
        symbol = normalize_symbol(match.group('symbol'))
        entry1 = float(match.group('entry1'))
        entry2 = float(match.group('entry2') or entry1)
        sl_match = SL_RE.search(text)
        tgt_match = TARGET_RE.search(text)
        targets = []
        if tgt_match:
            targets = [float(x) for x in re.findall(r'\d+(?:\.\d+)?', tgt_match.group('targets'))[:4]]
        calls.append(OptionCall(
            channel=channel_name,
            message_id=int(m.get('message_id') or 0),
            date=m['date'],
            symbol=symbol,
            side=match.group('side').upper(),
            strike=float(match.group('strike')),
            month_hint=(match.group('month') or '').upper() or None,
            entry_low=min(entry1, entry2),
            entry_high=max(entry1, entry2),
            entry_ref=(entry1 + entry2) / 2.0,
            lot_size=int(match.group('lot')),
            stop_loss=float(sl_match.group('sl')) if sl_match else None,
            targets=targets,
            text=text,
        ))
    return calls


def resolve_contract(call: OptionCall) -> dict[str, Any] | None:
    call_dt = datetime.fromisoformat(call.date.replace('Z', '+00:00')).astimezone(timezone.utc)
    script = f"""
import sys, json
from datetime import datetime
sys.path.insert(0, '/home/ubuntu/Auto_Trader')
from Auto_Trader.my_secrets import API_KEY
from Auto_Trader.utils import read_session_data
from kiteconnect import KiteConnect
kite = KiteConnect(api_key=API_KEY)
kite.set_access_token(read_session_data())
call_dt = datetime.fromisoformat({call_dt.isoformat()!r}.replace('Z','+00:00'))
symbol = {call.symbol!r}
side = {call.side!r}
strike = float({call.strike!r})
month_hint = {call.month_hint!r}
month_map = {MONTH_MAP!r}
rows = []
for inst in kite.instruments('NFO'):
    if inst.get('name') != symbol:
        continue
    if inst.get('instrument_type') != side:
        continue
    if float(inst.get('strike') or 0.0) != strike:
        continue
    expiry = inst.get('expiry')
    if expiry is None:
        continue
    if hasattr(expiry, 'isoformat'):
        expiry_iso = expiry.isoformat()
    else:
        expiry_iso = str(expiry)
    try:
        expiry_dt = datetime.fromisoformat(expiry_iso)
    except Exception:
        continue
    if expiry_dt.date() < call_dt.date():
        continue
    if month_hint and expiry_dt.month != month_map.get(month_hint):
        continue
    rows.append({{
        'tradingsymbol': inst.get('tradingsymbol'),
        'instrument_token': inst.get('instrument_token'),
        'exchange_token': inst.get('exchange_token'),
        'expiry': expiry_iso,
        'lot_size': int(inst.get('lot_size') or 0),
        'strike': float(inst.get('strike') or 0.0),
        'instrument_type': inst.get('instrument_type'),
        'name': inst.get('name'),
    }})
rows.sort(key=lambda x: x['expiry'])
print(json.dumps(rows[0] if rows else None, default=str))
"""
    return _run_oracle_python(script, timeout=90)


def fetch_contract_history(contract: dict[str, Any], start: datetime, end: datetime, interval: str = 'day') -> pd.DataFrame:
    script = f"""
import sys, json
from datetime import datetime, timedelta
sys.path.insert(0, '/home/ubuntu/Auto_Trader')
from Auto_Trader.my_secrets import API_KEY
from Auto_Trader.utils import read_session_data
from kiteconnect import KiteConnect
kite = KiteConnect(api_key=API_KEY)
kite.set_access_token(read_session_data())
inst_token = int({int(contract['instrument_token'])!r})
start = datetime.fromisoformat({start.isoformat()!r}.replace('Z','+00:00'))
end = datetime.fromisoformat({end.isoformat()!r}.replace('Z','+00:00'))
interval = {interval!r}
rows = kite.historical_data(inst_token, start, end, interval, oi=True)
out = []
for row in rows:
    dt = row.get('date')
    if hasattr(dt, 'isoformat'):
        dt = dt.isoformat()
    out.append({{
        'Date': dt,
        'Open': row.get('open'),
        'High': row.get('high'),
        'Low': row.get('low'),
        'Close': row.get('close'),
        'Volume': row.get('volume'),
        'OI': row.get('oi'),
    }})
print(json.dumps(out, default=str))
"""
    data = _run_oracle_python(script, timeout=120)
    df = pd.DataFrame(data)
    if df.empty:
        return df
    df['Date'] = pd.to_datetime(df['Date'], utc=True).dt.tz_localize(None)
    for c in ['Open', 'High', 'Low', 'Close', 'Volume', 'OI']:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors='coerce')
    return df.dropna(subset=['Close']).sort_values('Date').reset_index(drop=True)


def build_equity_curve(history_by_symbol: dict[str, pd.DataFrame], trades: pd.DataFrame, initial_capital: float) -> pd.DataFrame:
    all_dates = sorted({d for df in history_by_symbol.values() for d in pd.to_datetime(df['Date']).tolist()})
    if not all_dates:
        return pd.DataFrame([{'Date': pd.Timestamp.utcnow().tz_localize(None), 'equity': initial_capital}])
    trades = trades.copy()
    trades['entry_time'] = pd.to_datetime(trades['entry_time'])
    trades['exit_time'] = pd.to_datetime(trades['exit_time'])
    rows = []
    for dt in all_dates:
        closed = trades[trades['exit_time'] <= dt]
        open_trades = trades[(trades['entry_time'] <= dt) & (trades['exit_time'] > dt)]

        realized_pnl = float((closed['realized_value'] - closed['invested']).sum()) if not closed.empty else 0.0
        open_unrealized = 0.0
        for _, tr in open_trades.iterrows():
            hist = history_by_symbol.get(tr['tradingsymbol'])
            if hist is None or hist.empty:
                continue
            sub = hist[hist['Date'] <= dt]
            if sub.empty:
                continue
            px = float(sub.iloc[-1]['Close'])
            current_value = float(tr['realized_partial_cash']) + float(tr['remaining_qty']) * px
            open_unrealized += current_value - float(tr['invested'])
        equity = float(initial_capital) + realized_pnl + open_unrealized
        rows.append({'Date': dt, 'equity': equity})
    out = pd.DataFrame(rows).drop_duplicates(subset=['Date']).sort_values('Date').reset_index(drop=True)
    return out


def summarize_period_returns(equity: pd.DataFrame, freq: str) -> list[dict[str, Any]]:
    if equity.empty:
        return []
    s = equity.set_index('Date')['equity'].sort_index().resample(freq).last().dropna()
    ret = s.pct_change().dropna()
    return [
        {'period': idx.strftime('%Y-%m-%d'), 'return_pct': round(float(val * 100.0), 2), 'ending_equity': round(float(s.loc[idx]), 2)}
        for idx, val in ret.items()
    ]


def simulate_calls(
    calls: list[OptionCall],
    starting_capital: float,
    capital_per_trade_pct: float,
    interval: str,
    max_hold_bars: int,
    target_style: str,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, pd.DataFrame]]:
    cash = starting_capital
    open_positions: list[Position] = []
    closed_rows: list[dict[str, Any]] = []
    history_by_symbol: dict[str, pd.DataFrame] = {}

    # Pre-resolve and prefetch.
    resolved = []
    for call in calls:
        contract = resolve_contract(call)
        if not contract:
            continue
        start = datetime.fromisoformat(call.date.replace('Z', '+00:00')).replace(tzinfo=None)
        end_dt = datetime.utcnow() + timedelta(days=2)
        hist = fetch_contract_history(contract, start - timedelta(days=1), end_dt, interval=interval)
        if hist.empty:
            continue
        resolved.append((call, contract, hist))
        history_by_symbol[contract['tradingsymbol']] = hist

    all_dates = sorted({d for _, _, hist in resolved for d in hist['Date'].tolist()})
    call_queue = sorted(resolved, key=lambda x: pd.to_datetime(x[0].date))
    pending = list(call_queue)

    for dt in all_dates:
        # open new positions whose call has arrived
        still_pending = []
        for call, contract, hist in pending:
            call_dt = pd.to_datetime(call.date).tz_convert('UTC').tz_localize(None) if pd.to_datetime(call.date).tzinfo else pd.to_datetime(call.date)
            if call_dt > dt:
                still_pending.append((call, contract, hist))
                continue
            bar = hist[hist['Date'] >= dt]
            if bar.empty:
                still_pending.append((call, contract, hist))
                continue
            entry_bar = bar.iloc[0]
            price = float(call.entry_ref)
            lot_size = int(contract.get('lot_size') or call.lot_size or 1)
            alloc = max(0.0, cash * capital_per_trade_pct)
            cost_per_lot = lot_size * price
            lots = max(1, int(alloc // cost_per_lot)) if alloc >= cost_per_lot else 0
            if lots <= 0:
                still_pending.append((call, contract, hist))
                continue
            qty = lots * lot_size
            invested = qty * price
            if invested > cash:
                still_pending.append((call, contract, hist))
                continue
            cash -= invested
            open_positions.append(Position(
                call=call,
                tradingsymbol=contract['tradingsymbol'],
                expiry=str(contract['expiry']),
                lot_size=lot_size,
                qty=qty,
                entry_time=pd.Timestamp(dt),
                entry_price=price,
                invested=invested,
                stop_loss=call.stop_loss,
                remaining_qty=qty,
            ))
        pending = still_pending

        next_open = []
        for pos in open_positions:
            hist = history_by_symbol[pos.tradingsymbol]
            sub = hist[hist['Date'] == dt]
            if sub.empty:
                next_open.append(pos)
                continue
            row = sub.iloc[0]
            high = float(row['High'])
            low = float(row['Low'])
            close = float(row['Close'])
            bars_held = int((hist[hist['Date'] <= dt].shape[0]) - (hist[hist['Date'] < pos.entry_time].shape[0]))
            exit_now = False
            exit_reason = None
            exit_price = None

            # stop
            if pos.stop_loss is not None and low <= pos.stop_loss:
                exit_now = True
                exit_reason = 'stop_loss'
                exit_price = float(pos.stop_loss)

            # target style
            if not exit_now and pos.call.targets:
                if target_style == 't1' and high >= pos.call.targets[0]:
                    exit_now = True
                    exit_reason = 'target_1'
                    exit_price = float(pos.call.targets[0])
                elif target_style == 't2' and len(pos.call.targets) >= 2 and high >= pos.call.targets[1]:
                    exit_now = True
                    exit_reason = 'target_2'
                    exit_price = float(pos.call.targets[1])
                elif target_style == 'ladder':
                    ladder_fracs = [0.5, 0.3, 0.2]
                    for idx, tgt in enumerate(pos.call.targets[:3]):
                        if idx in pos.target_hits:
                            continue
                        if high >= tgt:
                            frac = ladder_fracs[min(idx, len(ladder_fracs)-1)]
                            sell_qty = min(pos.remaining_qty, int(pos.qty * frac))
                            if sell_qty <= 0:
                                pos.target_hits.append(idx)
                                continue
                            pos.realized_cash += sell_qty * float(tgt)
                            pos.remaining_qty -= sell_qty
                            pos.target_hits.append(idx)
                            if idx == 0 and pos.stop_loss is not None:
                                pos.stop_loss = max(pos.stop_loss, pos.entry_price)
                            if pos.remaining_qty <= 0:
                                exit_now = True
                                exit_reason = f'target_{idx+1}_full'
                                exit_price = float(tgt)
                                break

            expiry_dt = pd.to_datetime(pos.expiry)
            if not exit_now and pd.Timestamp(dt).date() >= expiry_dt.date():
                exit_now = True
                exit_reason = 'expiry'
                exit_price = close

            if not exit_now and bars_held >= max_hold_bars:
                exit_now = True
                exit_reason = 'time_stop'
                exit_price = close

            if exit_now:
                realized_value = pos.realized_cash + (pos.remaining_qty * float(exit_price))
                cash += realized_value
                pnl = realized_value - pos.invested
                closed_rows.append({
                    'channel': pos.call.channel,
                    'message_id': pos.call.message_id,
                    'signal_date': pos.call.date,
                    'symbol': pos.call.symbol,
                    'tradingsymbol': pos.tradingsymbol,
                    'side': pos.call.side,
                    'strike': pos.call.strike,
                    'entry_time': pos.entry_time.isoformat(),
                    'entry_price': round(pos.entry_price, 2),
                    'entry_ref': round(pos.call.entry_ref, 2),
                    'qty': int(pos.qty),
                    'invested': round(pos.invested, 2),
                    'realized_partial_cash': round(pos.realized_cash, 2),
                    'exit_time': pd.Timestamp(dt).isoformat(),
                    'exit_price': round(float(exit_price), 2),
                    'exit_reason': exit_reason,
                    'realized_value': round(realized_value, 2),
                    'pnl': round(pnl, 2),
                    'return_pct': round((realized_value / pos.invested - 1.0) * 100.0, 2),
                    'targets_hit': [int(x)+1 for x in pos.target_hits],
                    'stop_loss': pos.call.stop_loss,
                    'targets': pos.call.targets,
                    'text': pos.call.text[:300],
                })
            else:
                next_open.append(pos)
        open_positions = next_open

    # close leftovers at latest close
    for pos in open_positions:
        hist = history_by_symbol[pos.tradingsymbol]
        last_row = hist.iloc[-1]
        realized_value = pos.realized_cash + (pos.remaining_qty * float(last_row['Close']))
        cash += realized_value
        pnl = realized_value - pos.invested
        closed_rows.append({
            'channel': pos.call.channel,
            'message_id': pos.call.message_id,
            'signal_date': pos.call.date,
            'symbol': pos.call.symbol,
            'tradingsymbol': pos.tradingsymbol,
            'side': pos.call.side,
            'strike': pos.call.strike,
            'entry_time': pos.entry_time.isoformat(),
            'entry_price': round(pos.entry_price, 2),
            'entry_ref': round(pos.call.entry_ref, 2),
            'qty': int(pos.qty),
            'invested': round(pos.invested, 2),
            'realized_partial_cash': round(pos.realized_cash, 2),
            'exit_time': pd.Timestamp(last_row['Date']).isoformat(),
            'exit_price': round(float(last_row['Close']), 2),
            'exit_reason': 'mark_to_market',
            'realized_value': round(realized_value, 2),
            'pnl': round(pnl, 2),
            'return_pct': round((realized_value / pos.invested - 1.0) * 100.0, 2),
            'targets_hit': [int(x)+1 for x in pos.target_hits],
            'stop_loss': pos.call.stop_loss,
            'targets': pos.call.targets,
            'text': pos.call.text[:300],
        })

    trades = pd.DataFrame(closed_rows).sort_values('entry_time').reset_index(drop=True) if closed_rows else pd.DataFrame()
    equity = build_equity_curve(history_by_symbol, trades, starting_capital) if not trades.empty else pd.DataFrame([{'Date': pd.Timestamp.utcnow().tz_localize(None), 'equity': starting_capital, 'cash': starting_capital}])
    return trades, equity, history_by_symbol


def main() -> int:
    ap = argparse.ArgumentParser(description='Paper options trader for Telegram channel strategies.')
    ap.add_argument('--input', default=str(EXPORT_DEFAULT), help='Telegram export JSON path')
    ap.add_argument('--channel', default='@FinanceWithSunil')
    ap.add_argument('--starting-capital', type=float, default=100000.0)
    ap.add_argument('--capital-per-trade-pct', type=float, default=0.2)
    ap.add_argument('--interval', default=os.getenv('AT_TELEGRAM_OPTIONS_INTERVAL', 'day'))
    ap.add_argument('--max-hold-bars', type=int, default=5)
    ap.add_argument('--target-style', choices=['t1', 't2', 'ladder'], default='ladder')
    ap.add_argument('--limit-calls', type=int, default=0)
    ap.add_argument('--output-prefix', default='telegram_options_paper')
    args = ap.parse_args()

    payload = json.loads(Path(args.input).read_text())
    msgs = payload['chats'].get(args.channel, [])
    calls = parse_option_calls(msgs, args.channel)
    if args.limit_calls > 0:
        calls = calls[-args.limit_calls:]
    if not calls:
        raise SystemExit(f'No option calls found for {args.channel}')

    trades, equity, _ = simulate_calls(
        calls=calls,
        starting_capital=float(args.starting_capital),
        capital_per_trade_pct=float(args.capital_per_trade_pct),
        interval=args.interval,
        max_hold_bars=int(args.max_hold_bars),
        target_style=args.target_style,
    )

    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    out_json = REPORTS / f'{args.output_prefix}_{ts}.json'
    out_csv = REPORTS / f'{args.output_prefix}_{ts}.csv'
    out_md = REPORTS / f'{args.output_prefix}_{ts}.md'

    weekly = summarize_period_returns(equity, 'W-FRI')
    monthly = summarize_period_returns(equity, 'ME')
    total_return = 0.0
    final_equity = float(args.starting_capital)
    if not trades.empty:
        final_equity = float(args.starting_capital) + float(trades['pnl'].sum())
        total_return = (final_equity / float(args.starting_capital) - 1.0) * 100.0
    elif not equity.empty:
        final_equity = float(equity['equity'].iloc[-1])
        total_return = (final_equity / float(args.starting_capital) - 1.0) * 100.0
    win_rate = float((trades['pnl'] > 0).mean() * 100.0) if not trades.empty else 0.0
    report = {
        'generated_at': datetime.now(timezone.utc).isoformat(),
        'input': args.input,
        'channel': args.channel,
        'starting_capital': float(args.starting_capital),
        'capital_per_trade_pct': float(args.capital_per_trade_pct),
        'interval': args.interval,
        'max_hold_bars': int(args.max_hold_bars),
        'target_style': args.target_style,
        'signals_found': len(calls),
        'trades_simulated': int(len(trades)),
        'final_equity': round(final_equity, 2),
        'total_return_pct': round(total_return, 2),
        'win_rate_pct': round(win_rate, 2),
        'avg_trade_return_pct': round(float(trades['return_pct'].mean()), 2) if not trades.empty else 0.0,
        'best_trade_pct': round(float(trades['return_pct'].max()), 2) if not trades.empty else 0.0,
        'worst_trade_pct': round(float(trades['return_pct'].min()), 2) if not trades.empty else 0.0,
        'weekly_returns': weekly,
        'monthly_returns': monthly,
        'sample_trades': trades.head(25).to_dict('records') if not trades.empty else [],
    }
    out_json.write_text(json.dumps(report, indent=2))
    if not trades.empty:
        trades.to_csv(out_csv, index=False)
    else:
        pd.DataFrame(columns=['symbol']).to_csv(out_csv, index=False)
    out_md.write_text(
        '# Telegram Options Paper Trader\n\n'
        f'- Channel: `{args.channel}`\n'
        f'- Starting capital: `{args.starting_capital:.2f}`\n'
        f'- Capital per trade pct: `{args.capital_per_trade_pct:.2%}`\n'
        f'- Interval: `{args.interval}`\n'
        f'- Target style: `{args.target_style}`\n'
        f'- Max hold bars: `{args.max_hold_bars}`\n'
        f'- Signals found: `{len(calls)}`\n'
        f'- Trades simulated: `{len(trades)}`\n'
        f'- Final equity: `{final_equity:.2f}`\n'
        f'- Total return: `{total_return:.2f}%`\n'
        f'- Win rate: `{win_rate:.2f}%`\n\n'
        '## Weekly returns\n' + ('\n'.join([f"- {r['period']}: {r['return_pct']}% (equity {r['ending_equity']})" for r in weekly]) or '- none') + '\n\n'
        '## Monthly returns\n' + ('\n'.join([f"- {r['period']}: {r['return_pct']}% (equity {r['ending_equity']})" for r in monthly]) or '- none') + '\n'
    )

    print(str(out_json))
    print(str(out_csv))
    print(str(out_md))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
