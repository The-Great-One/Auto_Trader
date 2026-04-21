#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
REPORTS = ROOT / 'reports'
REPORTS.mkdir(exist_ok=True)
TRACKED_CALLS = Path('REDACTED_OPENCLAW/telegram-user/tracked_option_calls.json')
WORKSPACE_TOOLS = Path('REDACTED_OPENCLAW/workspace/tools')
if str(WORKSPACE_TOOLS) not in sys.path:
    sys.path.insert(0, str(WORKSPACE_TOOLS))

from telegram_trade_audit import fetch_option_chain_snapshot  # type: ignore

STATE_PATH = REPORTS / 'live_telegram_options_paper_state.json'
HISTORY_PATH = REPORTS / 'live_telegram_options_paper_equity_history.jsonl'
LATEST_JSON = REPORTS / 'live_telegram_options_paper_latest.json'
LATEST_MD = REPORTS / 'live_telegram_options_paper_latest.md'

TARGET_FRACTIONS = [0.5, 0.3, 0.2]


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def load_json(path: Path, default: Any):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text())
    except Exception:
        return default


def save_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False))


def append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    with path.open('a', encoding='utf-8') as fh:
        fh.write(json.dumps(payload, ensure_ascii=False) + '\n')


def tracked_calls() -> list[dict[str, Any]]:
    payload = load_json(TRACKED_CALLS, {'tracked_calls': []})
    calls = payload.get('tracked_calls') or []
    out = [c for c in calls if c.get('status', 'active') == 'active']
    out.sort(key=lambda x: x.get('captured_at') or x.get('tracking_started_at') or '')
    return out


def key_for(call: dict[str, Any]) -> str:
    return f"{call.get('source_chat')}::{call.get('source_message_id')}"


def match_contract(call: dict[str, Any], snap: dict[str, Any]) -> dict[str, Any] | None:
    rows = snap.get('chain') or []
    strike = float(call.get('option_strike') or 0)
    side = str(call.get('option_side') or '').upper()
    for row in rows:
        try:
            if float(row.get('strike') or 0) == strike and str(row.get('side') or '').upper() == side:
                return row
        except Exception:
            continue
    return None


def maybe_open_position(state: dict[str, Any], call: dict[str, Any], capital_per_trade_pct: float) -> None:
    positions = state.setdefault('positions', {})
    pos_key = key_for(call)
    if pos_key in positions:
        return

    snap = fetch_option_chain_snapshot(call['symbol'], call['option_side']) or {}
    contract = match_contract(call, snap)
    if not contract:
        positions[pos_key] = {
            'status': 'unresolved',
            'reason': 'contract_not_found',
            'call': call,
            'created_at': now_utc().isoformat(),
        }
        return

    cash = float(state.get('cash', 0.0))
    entry_price = float(call.get('entry_ref') or contract.get('last_price') or 0.0)
    lot_size = int(contract.get('lot_size') or 0)
    alloc = float(state.get('starting_capital', 0.0)) * capital_per_trade_pct
    if entry_price <= 0 or lot_size <= 0 or cash <= 0:
        positions[pos_key] = {
            'status': 'skipped',
            'reason': 'invalid_entry_or_cash',
            'call': call,
            'created_at': now_utc().isoformat(),
        }
        return
    cost_per_lot = entry_price * lot_size
    lots = int(alloc // cost_per_lot) if alloc >= cost_per_lot else 0
    if lots <= 0:
        positions[pos_key] = {
            'status': 'skipped',
            'reason': 'insufficient_cash_for_one_lot',
            'call': call,
            'created_at': now_utc().isoformat(),
            'entry_price': entry_price,
            'lot_size': lot_size,
            'required_for_one_lot': round(cost_per_lot, 2),
        }
        return

    qty = lots * lot_size
    invested = qty * entry_price
    state['cash'] = round(cash - invested, 2)
    positions[pos_key] = {
        'status': 'open',
        'created_at': now_utc().isoformat(),
        'call': {
            'source_chat': call.get('source_chat'),
            'source_message_id': call.get('source_message_id'),
            'symbol': call.get('symbol'),
            'option_side': call.get('option_side'),
            'option_strike': call.get('option_strike'),
            'entry_ref': call.get('entry_ref'),
            'underlying_entry': call.get('underlying_entry'),
            'nearest_expiry': call.get('nearest_expiry'),
            'text': ((call.get('initial_snapshot') or {}).get('text') or '')[:300],
        },
        'contract': {
            'tradingsymbol': contract.get('tradingsymbol'),
            'lot_size': lot_size,
            'expiry': contract.get('expiry') or snap.get('nearest_expiry'),
            'strike': contract.get('strike'),
            'side': contract.get('side'),
        },
        'entry_time': call.get('captured_at') or call.get('tracking_started_at') or now_utc().isoformat(),
        'entry_price': round(entry_price, 2),
        'qty': int(qty),
        'remaining_qty': int(qty),
        'invested': round(invested, 2),
        'realized_cash': 0.0,
        'stop_loss': float(_extract_stop(call) or 0.0) or None,
        'targets': _extract_targets(call),
        'target_hits': [],
        'last_price': round(float(contract.get('last_price') or 0.0), 2),
        'last_underlying_price': round(float(snap.get('underlying_price') or 0.0), 2) if snap.get('underlying_price') is not None else None,
        'last_snapshot_at': now_utc().isoformat(),
    }


def _extract_stop(call: dict[str, Any]) -> float | None:
    text = ((call.get('initial_snapshot') or {}).get('text') or '')
    import re
    m = re.search(r'SL-\s*(\d+(?:\.\d+)?)', text, re.IGNORECASE)
    return float(m.group(1)) if m else None


def _extract_targets(call: dict[str, Any]) -> list[float]:
    text = ((call.get('initial_snapshot') or {}).get('text') or '')
    import re
    m = re.search(r'Target-\s*([0-9+/\-. ]+)', text, re.IGNORECASE)
    return [float(x) for x in re.findall(r'\d+(?:\.\d+)?', m.group(1))[:4]] if m else []


def refresh_position(state: dict[str, Any], pos_key: str, pos: dict[str, Any], target_style: str) -> None:
    if pos.get('status') != 'open':
        return
    call = pos.get('call') or {}
    snap = fetch_option_chain_snapshot(call.get('symbol'), call.get('option_side')) or {}
    contract = match_contract({
        'option_strike': call.get('option_strike'),
        'option_side': call.get('option_side'),
    }, snap)
    if not contract:
        pos['last_snapshot_at'] = now_utc().isoformat()
        pos['status_note'] = 'contract_not_found_on_refresh'
        return

    price = float(contract.get('last_price') or 0.0)
    pos['last_price'] = round(price, 2)
    pos['last_underlying_price'] = round(float(snap.get('underlying_price') or 0.0), 2) if snap.get('underlying_price') is not None else None
    pos['last_snapshot_at'] = now_utc().isoformat()
    pos['contract']['tradingsymbol'] = contract.get('tradingsymbol')
    pos['contract']['expiry'] = contract.get('expiry') or pos['contract'].get('expiry') or snap.get('nearest_expiry')

    targets = pos.get('targets') or []
    hits = list(pos.get('target_hits') or [])
    remaining_qty = int(pos.get('remaining_qty') or 0)
    realized_cash = float(pos.get('realized_cash') or 0.0)
    entry_price = float(pos.get('entry_price') or 0.0)
    stop_loss = pos.get('stop_loss')
    qty = int(pos.get('qty') or 0)

    if target_style == 'ladder' and targets and remaining_qty > 0:
        for idx, tgt in enumerate(targets[: len(TARGET_FRACTIONS)]):
            if idx in hits:
                continue
            if price >= float(tgt):
                sell_qty = min(remaining_qty, int(qty * TARGET_FRACTIONS[idx]))
                if sell_qty <= 0:
                    hits.append(idx)
                    continue
                realized_cash += sell_qty * float(tgt)
                remaining_qty -= sell_qty
                hits.append(idx)
                if idx == 0 and stop_loss is not None:
                    stop_loss = max(float(stop_loss), entry_price)

    pos['target_hits'] = hits
    pos['remaining_qty'] = remaining_qty
    pos['realized_cash'] = round(realized_cash, 2)
    pos['stop_loss'] = round(float(stop_loss), 2) if stop_loss is not None else None

    expiry = pos.get('contract', {}).get('expiry')
    expired = False
    if expiry:
        try:
            expired = pd.Timestamp(expiry).tz_localize(None).date() <= pd.Timestamp.utcnow().tz_localize(None).date()
        except Exception:
            expired = False

    close_reason = None
    close_price = None
    if remaining_qty > 0 and stop_loss is not None and price <= float(stop_loss):
        close_reason = 'stop_loss'
        close_price = float(stop_loss)
    elif remaining_qty <= 0:
        close_reason = 'target_ladder_fully_exited'
        close_price = float(price)
    elif expired:
        close_reason = 'expiry'
        close_price = float(price)

    if close_reason is not None:
        realized_value = realized_cash + remaining_qty * float(close_price)
        pnl = realized_value - float(pos.get('invested') or 0.0)
        state['cash'] = round(float(state.get('cash', 0.0)) + realized_value, 2)
        pos['status'] = 'closed'
        pos['exit_reason'] = close_reason
        pos['exit_time'] = now_utc().isoformat()
        pos['exit_price'] = round(float(close_price), 2)
        pos['realized_value'] = round(realized_value, 2)
        pos['pnl'] = round(pnl, 2)
        pos['return_pct'] = round((realized_value / float(pos.get('invested') or 1.0) - 1.0) * 100.0, 2)
        pos['remaining_qty'] = 0


def mark_to_market(state: dict[str, Any]) -> dict[str, Any]:
    positions = state.get('positions') or {}
    realized = 0.0
    unrealized = 0.0
    open_rows = []
    closed_rows = []
    for pos_key, pos in positions.items():
        invested = float(pos.get('invested') or 0.0)
        realized_value = float(pos.get('realized_value') or 0.0)
        if pos.get('status') == 'closed':
            pnl = float(pos.get('pnl') or (realized_value - invested))
            realized += pnl
            closed_rows.append({
                'key': pos_key,
                'symbol': pos.get('call', {}).get('symbol'),
                'tradingsymbol': pos.get('contract', {}).get('tradingsymbol'),
                'return_pct': pos.get('return_pct'),
                'pnl': round(pnl, 2),
                'exit_reason': pos.get('exit_reason'),
            })
            continue
        if pos.get('status') != 'open':
            continue
        current_value = float(pos.get('realized_cash') or 0.0) + float(pos.get('remaining_qty') or 0) * float(pos.get('last_price') or 0.0)
        pnl = current_value - invested
        unrealized += pnl
        open_rows.append({
            'key': pos_key,
            'symbol': pos.get('call', {}).get('symbol'),
            'tradingsymbol': pos.get('contract', {}).get('tradingsymbol'),
            'entry_price': pos.get('entry_price'),
            'last_price': pos.get('last_price'),
            'qty': pos.get('qty'),
            'remaining_qty': pos.get('remaining_qty'),
            'targets_hit': [int(x) + 1 for x in pos.get('target_hits') or []],
            'stop_loss': pos.get('stop_loss'),
            'mtm_pnl': round(pnl, 2),
            'mtm_return_pct': round((current_value / invested - 1.0) * 100.0, 2) if invested > 0 else None,
        })
    equity = float(state.get('starting_capital', 0.0)) + realized + unrealized
    return {
        'cash': round(float(state.get('cash', 0.0)), 2),
        'equity': round(equity, 2),
        'realized_pnl': round(realized, 2),
        'unrealized_pnl': round(unrealized, 2),
        'open_positions': open_rows,
        'closed_positions': closed_rows,
    }


def summarize_history() -> dict[str, Any]:
    if not HISTORY_PATH.exists():
        return {'weekly_returns': [], 'monthly_returns': []}
    rows = []
    for line in HISTORY_PATH.read_text().splitlines():
        if not line.strip():
            continue
        try:
            rows.append(json.loads(line))
        except Exception:
            continue
    if not rows:
        return {'weekly_returns': [], 'monthly_returns': []}
    df = pd.DataFrame(rows)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.sort_values('timestamp').drop_duplicates(subset=['timestamp'])
    s = df.set_index('timestamp')['equity'].astype(float)
    weekly = s.resample('W-FRI').last().pct_change().dropna()
    monthly = s.resample('ME').last().pct_change().dropna()
    return {
        'weekly_returns': [
            {'period': idx.strftime('%Y-%m-%d'), 'return_pct': round(float(val * 100.0), 2), 'ending_equity': round(float(s.loc[idx]), 2)}
            for idx, val in weekly.items()
        ],
        'monthly_returns': [
            {'period': idx.strftime('%Y-%m-%d'), 'return_pct': round(float(val * 100.0), 2), 'ending_equity': round(float(s.loc[idx]), 2)}
            for idx, val in monthly.items()
        ],
    }


def render_md(summary: dict[str, Any]) -> str:
    lines = [
        '# Live Telegram Options Paper Ledger',
        '',
        f"- Updated: `{summary['updated_at']}`",
        f"- Starting capital: `{summary['starting_capital']}`",
        f"- Cash: `{summary['cash']}`",
        f"- Equity: `{summary['equity']}`",
        f"- Realized PnL: `{summary['realized_pnl']}`",
        f"- Unrealized PnL: `{summary['unrealized_pnl']}`",
        '',
        '## Open positions',
    ]
    if summary['open_positions']:
        for row in summary['open_positions']:
            lines.append(
                f"- {row['symbol']} `{row['tradingsymbol']}` entry {row['entry_price']} last {row['last_price']} mtm {row['mtm_pnl']} ({row['mtm_return_pct']}%), targets hit {row['targets_hit']}, SL {row['stop_loss']}"
            )
    else:
        lines.append('- none')
    lines += ['', '## Closed positions']
    if summary['closed_positions']:
        for row in summary['closed_positions']:
            lines.append(f"- {row['symbol']} `{row['tradingsymbol']}` pnl {row['pnl']} ({row['return_pct']}%), reason {row['exit_reason']}")
    else:
        lines.append('- none')
    lines += ['', '## Weekly returns']
    weekly = summary.get('weekly_returns') or []
    lines += [f"- {r['period']}: {r['return_pct']}% (equity {r['ending_equity']})" for r in weekly] or ['- none']
    lines += ['', '## Monthly returns']
    monthly = summary.get('monthly_returns') or []
    lines += [f"- {r['period']}: {r['return_pct']}% (equity {r['ending_equity']})" for r in monthly] or ['- none']
    return '\n'.join(lines) + '\n'


def main() -> int:
    ap = argparse.ArgumentParser(description='Live paper ledger for tracked Telegram option calls.')
    ap.add_argument('--starting-capital', type=float, default=100000.0)
    ap.add_argument('--capital-per-trade-pct', type=float, default=0.2)
    ap.add_argument('--target-style', choices=['ladder'], default='ladder')
    args = ap.parse_args()

    state = load_json(STATE_PATH, {'starting_capital': float(args.starting_capital), 'cash': float(args.starting_capital), 'positions': {}})
    state['starting_capital'] = float(state.get('starting_capital') or args.starting_capital)
    state['cash'] = float(state.get('cash', state['starting_capital']))

    for call in tracked_calls():
        maybe_open_position(state, call, float(args.capital_per_trade_pct))

    for pos_key, pos in list((state.get('positions') or {}).items()):
        refresh_position(state, pos_key, pos, args.target_style)

    summary = mark_to_market(state)
    summary.update({
        'updated_at': now_utc().isoformat(),
        'starting_capital': round(float(state['starting_capital']), 2),
        'positions_tracked': len(state.get('positions') or {}),
    })

    append_jsonl(HISTORY_PATH, {'timestamp': summary['updated_at'], 'equity': summary['equity'], 'cash': summary['cash']})
    summary.update(summarize_history())

    state['last_summary'] = summary
    save_json(STATE_PATH, state)
    save_json(LATEST_JSON, summary)
    LATEST_MD.write_text(render_md(summary))
    print(str(LATEST_JSON))
    print(str(LATEST_MD))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
