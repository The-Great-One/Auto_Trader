import logging
import traceback
import os
import time
import sys
from multiprocessing import Queue, Process
from Auto_Trader import (
    is_Market_Open,
    run_ticker,
    create_master,
    Apply_Rules,
    Updater,
)
from Auto_Trader.TelegramLink import telegram_main
import subprocess as _sp

from pathlib import Path
ROOT = Path(__file__).resolve().parent
logger = logging.getLogger("Auto_Trade_Logger")


def monitor_market():
    processes = []
    q = Queue()  # Queue for Orders Placements
    message_queue = Queue()  # Queue for Telegram Messages

    def start_processes():
        """Starts all necessary processes."""
        logger.info("Market is open. Starting processes.")
        message_queue.put("Market is open. Starting processes.")

        # Start the worker processes
        p1 = Process(target=run_ticker, args=(create_master(message_queue), q))
        p2 = Process(target=Apply_Rules, args=(q, message_queue))
        p3 = Process(target=Updater)
        p4 = Process(target=telegram_main, args=(message_queue,))
        p5 = Process(target=run_rebalancer, args=(message_queue,))

        p1.start()
        p2.start()
        p3.start()
        p4.start()
        p5.start()

        return [p1, p2, p3, p4, p5]

    def stop_processes(processes):
        """Stops all running processes."""
        logger.info("Market is closed. Stopping processes.")
        message_queue.put("Market is closed. Stopping processes.")

        for p in processes:
            p.terminate()  # Gracefully terminate the process
            p.join()  # Ensure the process has finished
        return []

    def run_rebalancer(message_queue):
        """Periodically check for new shadow signals and trigger paper rebalance."""
        import json, time, traceback as _tb
        shadow_path = 'reports/paper_shadow_rsi_momentum_latest.json'
        state_path = 'reports/paper_ledger_rsi_momentum_state.json'
        check_interval = 300  # every 5 minutes
        
        while True:
            try:
                if not Path(shadow_path).exists():
                    logger.debug('[REBALANCER] No shadow file — waiting for first signal')
                elif not Path(state_path).exists():
                    logger.debug('[REBALANCER] No state file — first run pending')
                else:
                    with open(shadow_path) as f:
                        shadow = json.load(f)
                    with open(state_path) as f:
                        state = json.load(f)
                    signal_date = shadow.get('latest_signal', {}).get('date', '')
                    last_rebalance = state.get('last_rebalance_date', '')
                    picks = shadow.get('latest_signal', {}).get('picks', [])
                    if signal_date and signal_date > last_rebalance:
                        logger.info(f'[REBALANCER] Triggering rebalance: signal {signal_date} > last {last_rebalance} | picks: {picks}')
                        try:
                            result = _sp.run(
                                ['./venv/bin/python', 'scripts/rsi_momentum_paper_ledger.py'],
                                capture_output=True, text=True, timeout=120, cwd=str(ROOT),
                                env={**os.environ, 'RSI_LEDGER_CAPITAL': '200000',
                                     'RSI_LEDGER_TELEGRAM_ALERTS': '1',
                                     'RSI_LEDGER_LIVE_MAX_AGE_SEC': '600',
                                     'RSI_LEDGER_ST_EXIT_MULT': '2.0'}
                            )
                            if result.returncode == 0:
                                logger.info('[REBALANCER] Rebalance completed OK')
                                # ── Send clean Telegram message ──
                                try:
                                    with open(state_path) as f2:
                                        new_state = json.load(f2)
                                    NL = chr(10)
                                    pos = new_state.get('positions', {})
                                    cost = new_state.get('cost_basis', {})
                                    cash_val = new_state.get('cash', 0)
                                    invested = sum(float(pos[s]) * float(cost.get(s, 0)) for s in pos if cost.get(s, 0))
                                    total_val = cash_val + invested
                                    msgs = [
                                        f'🔄 RSI Momentum Rebalance',
                                        f'Signal: {signal_date}',
                                    ]
                                    msgs.append(f'Value: ₹{total_val:,.0f}  |  Cash: ₹{cash_val:,.0f}  |  Positions: {len(pos)}')
                                    if pos:
                                        msgs.append('')
                                        for sym in sorted(pos, key=lambda s: float(pos[s]) * float(cost.get(s, 0)), reverse=True):
                                            q = float(pos[s])
                                            cp = float(cost.get(s, 0))
                                            alloc = q * cp
                                            pct = (alloc / total_val * 100) if total_val else 0
                                            msgs.append(f'  {sym}  {int(q)} sh  ₹{alloc:,.0f}  ({pct:.1f}%)')
                                    if skipped := [s for s in picks if s not in pos]:
                                        msgs.append(f'Skipped: {", ".join(skipped)}')
                                    # Show any ST exits that happened during this rebalance
                                    st_exits_log = [
                                        t for t in new_state.get('trade_log', [])
                                        if t.get('action') == 'SELL_ST' and t.get('date') == signal_date
                                    ]
                                    if st_exits_log:
                                        msgs.append(f'ST Exits: {", ".join(e["symbol"] for e in st_exits_log)}')
                                    msgs.append('Paper only — no live orders.')
                                    message_queue.put(NL.join(msgs))
                                except Exception as msg_e:
                                    logger.warning(f'[REBALANCER] Message formatting failed: {msg_e}')
                            else:
                                logger.error(f'[REBALANCER] Rebalance FAILED (rc={result.returncode})\nSTDOUT: {result.stdout[-500:]}\nSTDERR: {result.stderr[-500:]}')
                                message_queue.put(f'⚠️ RSI Momentum Rebalance FAILED\nSignal: {signal_date}\nError: {result.stderr[-300:]}')
                        except _sp.TimeoutExpired:
                            logger.error('[REBALANCER] Rebalance TIMED OUT after 120s')
                            message_queue.put(f'⚠️ RSI Momentum Rebalance TIMED OUT\nSignal: {signal_date}')
                        except Exception as e:
                            logger.error(f'[REBALANCER] Subprocess error: {e}\n{_tb.format_exc()}')
                    else:
                        logger.info(f'[REBALANCER] Heartbeat — signal {signal_date} = last {last_rebalance}, no action')
            except Exception as e:
                logger.error(f'[REBALANCER] Loop error: {e}\n{_tb.format_exc()}')
            time.sleep(check_interval)

    while True:
        try:
            market_status = is_Market_Open()  # Check market status
            if market_status and not processes:
                # Start processes if market is open and none are running
                processes = start_processes()

            elif not market_status and processes:
                # Stop processes and exit the program when the market closes
                processes = stop_processes(processes)
                sys.exit(0)  # Exit the script cleanly; systemd will restart it

            time.sleep(60)  # Sleep for 60 seconds before checking again

        except Exception as e:
            logger.error(f"Error occurred: {e}, Traceback: {traceback.format_exc()}")
            message_queue.put(
                f"Error occurred: {e}, Traceback: {traceback.format_exc()}"
            )
            if processes:
                processes = stop_processes(processes)
            sys.exit(1)  # Exit with an error code to indicate failure


if __name__ == "__main__":
    try:
        monitor_market()
    except KeyboardInterrupt:
        logger.error("Monitor stopped by user.")
        sys.exit(0)  # Exit cleanly if interrupted by the user
