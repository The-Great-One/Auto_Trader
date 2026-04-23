#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
REPORTS = ROOT / 'reports'
REPORTS.mkdir(exist_ok=True)
TRACKED_CALLS = Path(os.getenv('AT_TRACKED_CALLS', os.path.expanduser('~/.openclaw/telegram-user/tracked_option_calls.json')))
WORKSPACE_TOOLS = Path(os.getenv('AT_WORKSPACE_TOOLS', os.path.expanduser('~/.openclaw/workspace/tools')))
if str(WORKSPACE_TOOLS) not in sys.path:
    sys.path.insert(0, str(WORKSPACE_TOOLS))

from telegram_trade_audit import fetch_option_chain_snapshot  # type: ignore

STATE_PATH = REPORTS / 'live_telegram_options_paper_state.json'
HISTORY_PATH = REPORTS / 'live_telegram_options_paper_equity_history.jsonl'
LATEST_JSON = REPORTS / 'live_telegram_options_paper_latest.json'
LATEST_MD = REPORTS / 'live_telegram_options_paper_latest.md'
WATCH_UPDATES = Path.home() / '.openclaw' / 'telegram-user' / 'watch_channel_updates.jsonl'
TELEGRAM_AUDIT_PATH = REPORTS / 'telegram_trade_audit_latest.json'
CHANNEL_LEARNING_SCRIPT = ROOT / 'scripts' / 'generate_channel_learning.py'

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


def repair_position_placeholders(state: dict[str, Any]) -> None:
    positions = state.get('positions') or {}
    to_delete = []
    for pos_key, pos in list(positions.items()):
        status = pos.get('status')
        call = pos.get('call') or {}
        if status == 'unresolved':
            if not call.get('option_side') or not call.get('option_strike'):
                pos['status'] = 'skipped'
                pos['reason'] = 'not_option_signal'
                pos['status_note'] = 'converted_from_unresolved_non_option'
                continue
            if pos.get('reason') == 'contract_not_found':
                to_delete.append(pos_key)
                continue
        if status == 'skipped' and pos.get('reason') == 'insufficient_cash_for_one_lot' and call.get('option_side') and call.get('option_strike'):
            to_delete.append(pos_key)
            continue
        if status == 'skipped' and pos.get('reason') == 'not_option_signal':
            to_delete.append(pos_key)
    for pos_key in to_delete:
        positions.pop(pos_key, None)


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


CHANNEL_LEARNING_PATH = Path(ROOT) / "reports" / "channel_learning_scores.json"


def refresh_channel_learning_if_stale() -> None:
    if not CHANNEL_LEARNING_SCRIPT.exists():
        return
    output_mtime = CHANNEL_LEARNING_PATH.stat().st_mtime if CHANNEL_LEARNING_PATH.exists() else 0.0
    input_paths = [TRACKED_CALLS, STATE_PATH, WATCH_UPDATES, TELEGRAM_AUDIT_PATH]
    newest_input = max((p.stat().st_mtime for p in input_paths if p.exists()), default=0.0)
    if CHANNEL_LEARNING_PATH.exists() and output_mtime >= newest_input - 1:
        return
    try:
        subprocess.run(
            [str(ROOT / 'venv' / 'bin' / 'python'), str(CHANNEL_LEARNING_SCRIPT)],
            cwd=str(ROOT),
            capture_output=True,
            text=True,
            timeout=90,
        )
    except Exception:
        pass


def load_channel_learning_row(chat: str) -> dict[str, Any]:
    if not CHANNEL_LEARNING_PATH.exists():
        return {}
    try:
        data = json.loads(CHANNEL_LEARNING_PATH.read_text())
    except Exception:
        return {}
    channels = data.get("channels") or {}
    return channels.get(chat) or channels.get(str(chat or '').lower()) or {}


def load_channel_sizing_mult(chat: str, default: float = 1.0) -> float:
    """Load the sizing multiplier for a channel from learning scores.
    Returns the default if no scores exist yet."""
    ch_data = load_channel_learning_row(chat)
    action = ch_data.get("action", "")
    if action == "skip_or_observe":
        return 0.0
    mult = ch_data.get("sizing_mult", default)
    return float(mult) if mult else default


def target_fractions_for(chat: str) -> list[float]:
    profile = (load_channel_learning_row(chat).get('execution_profile') or {})
    fractions = profile.get('target_fractions') or []
    cleaned = [float(x) for x in fractions if isinstance(x, (int, float)) and float(x) > 0]
    return cleaned or list(TARGET_FRACTIONS)


def stop_profile_for(chat: str) -> tuple[int, int]:
    profile = (load_channel_learning_row(chat).get('execution_profile') or {})
    to_entry = int(profile.get('move_stop_to_entry_after_hits') or 0)
    to_last_target = int(profile.get('move_stop_to_last_target_after_hits') or 0)
    return to_entry, to_last_target


def maybe_open_position(state: dict[str, Any], call: dict[str, Any], capital_per_trade_pct: float) -> None:
    positions = state.setdefault('positions', {})
    pos_key = key_for(call)
    if pos_key in positions:
        return

    if not call.get('option_side') or not call.get('option_strike'):
        positions[pos_key] = {
            'status': 'skipped',
            'reason': 'not_option_signal',
            'call': call,
            'created_at': now_utc().isoformat(),
        }
        return

    # Apply channel learning sizing multiplier
    chat = call.get('source_chat', '')
    channel_mult = load_channel_sizing_mult(chat, default=1.0)
    if channel_mult <= 0.0:
        positions[pos_key] = {
            'status': 'skipped',
            'reason': 'channel_low_confidence',
            'call': call,
            'created_at': now_utc().isoformat(),
            'channel_confidence_note': f'Channel {chat} below min confidence, skipped',
        }
        return

    adjusted_pct = capital_per_trade_pct * channel_mult
    # Cap at global max
    max_pct = 0.40
    adjusted_pct = min(adjusted_pct, max_pct)

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
    alloc = float(state.get('starting_capital', 0.0)) * adjusted_pct
    if entry_price <= 0 or lot_size <= 0 or cash <= 0:
        positions[pos_key] = {
            'status': 'skipped',
            'reason': 'invalid_entry_or_cash',
            'call': call,
            'created_at': now_utc().isoformat(),
        }
        return
    cost_per_lot = entry_price * lot_size
    max_affordable_lots = int(cash // cost_per_lot) if cost_per_lot > 0 else 0
    lots = int(alloc // cost_per_lot) if alloc >= cost_per_lot else 0
    lot_floor_applied = False
    if lots <= 0 and max_affordable_lots >= 1 and adjusted_pct > 0:
        lots = 1
        lot_floor_applied = True
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
    lots = min(lots, max_affordable_lots)
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
            'text': ((call.get('initial_snapshot') or {}).get('text') or call.get('text') or '')[:500],
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
        'stop_loss': float(call.get('stop_loss') or _extract_stop(call) or 0.0) or None,
        'targets': call.get('targets') or _extract_targets(call),
        'target_hits': [],
        'last_price': round(float(contract.get('last_price') or 0.0), 2),
        'last_underlying_price': round(float(snap.get('underlying_price') or 0.0), 2) if snap.get('underlying_price') is not None else None,
        'last_snapshot_at': now_utc().isoformat(),
        'sizing_note': 'one_lot_floor_applied' if lot_floor_applied else None,
        'channel_learning': {
            'chat': chat,
            'snapshot': load_channel_learning_row(chat),
        },
    }


def _extract_stop(call: dict[str, Any]) -> float | None:
    text = ((call.get('initial_snapshot') or {}).get('text') or call.get('text') or '')
    import re
    m = re.search(r'SL-\s*(\d+(?:\.\d+)?)', text, re.IGNORECASE)
    return float(m.group(1)) if m else None


def _extract_targets(call: dict[str, Any]) -> list[float]:
    text = ((call.get('initial_snapshot') or {}).get('text') or call.get('text') or '')
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

    chat = str(call.get('source_chat') or '')
    target_fractions = target_fractions_for(chat)
    move_stop_to_entry_after_hits, move_stop_to_last_target_after_hits = stop_profile_for(chat)
    targets = pos.get('targets') or []
    hits = list(pos.get('target_hits') or [])
    remaining_qty = int(pos.get('remaining_qty') or 0)
    realized_cash = float(pos.get('realized_cash') or 0.0)
    entry_price = float(pos.get('entry_price') or 0.0)
    stop_loss = pos.get('stop_loss')
    qty = int(pos.get('qty') or 0)

    if target_style == 'ladder' and targets and remaining_qty > 0:
        for idx, tgt in enumerate(targets[: len(target_fractions)]):
            if idx in hits:
                continue
            if price >= float(tgt):
                sell_qty = min(remaining_qty, int(qty * target_fractions[idx]))
                if sell_qty <= 0:
                    hits.append(idx)
                    continue
                realized_cash += sell_qty * float(tgt)
                remaining_qty -= sell_qty
                hits.append(idx)
                # Record the timestamp when each target is hit
                hit_times = list(pos.get('target_hit_times') or [])
                hit_times.append({'target': float(tgt), 'target_idx': idx, 'hit_at': now_utc().isoformat(), 'hit_price': round(price, 2)})
                pos['target_hit_times'] = hit_times
                hit_count = len(hits)
                if stop_loss is not None and move_stop_to_entry_after_hits and hit_count >= move_stop_to_entry_after_hits:
                    stop_loss = max(float(stop_loss), entry_price)
                if stop_loss is not None and move_stop_to_last_target_after_hits and hit_count >= move_stop_to_last_target_after_hits and idx >= 1:
                    stop_loss = max(float(stop_loss), float(targets[idx - 1]))

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
    unresolved_rows = []
    for pos_key, pos in positions.items():
        invested = float(pos.get('invested') or 0.0)
        realized_value = float(pos.get('realized_value') or 0.0)
        status = pos.get('status')
        if status == 'closed':
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
        if status != 'open':
            unresolved_rows.append({
                'key': pos_key,
                'status': status,
                'reason': pos.get('reason'),
                'symbol': pos.get('call', {}).get('symbol') or pos.get('symbol'),
                'option_side': pos.get('call', {}).get('option_side'),
                'option_strike': pos.get('call', {}).get('option_strike'),
                'entry_ref': pos.get('call', {}).get('entry_ref'),
                'source_message_id': pos.get('call', {}).get('source_message_id'),
                'created_at': pos.get('created_at'),
                'channel_update': pos.get('call', {}).get('text') or pos.get('channel_confidence_note'),
            })
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
            'target_hit_times': pos.get('target_hit_times') or [],
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
        'unresolved_positions': unresolved_rows,
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
    lines += ['', '## Unresolved / not opened']
    if summary.get('unresolved_positions'):
        for row in summary['unresolved_positions']:
            lines.append(f"- {row['symbol']} {row.get('option_side') or ''} {row.get('option_strike') or ''} status {row['status']}, reason {row.get('reason') or '-'}")
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
    repair_position_placeholders(state)
    refresh_channel_learning_if_stale()

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
