#!/usr/bin/env python3
"""
SlateHub — Master Refresh Script (Agent Pipeline Edition)

Modes and when to run them:

  --quick      Every 15 min (all day)
                       Schedule + lineups + weather only. Fast (~30 sec).
                       Catches lineup confirmations and weather updates.

  --morning    9:00 AM   Full morning pull: schedule, DK slates/salaries,
                         odds, weather. Stats run in parallel.

  --postgame  11:30 PM   Post-game bullpen pitch counts + today's game logs.

  --stats      7:00 AM   Season stats refresh — now runs in parallel (~4 min).

  --splits     7:30 AM   Excel Power Query refresh + splits upload to Supabase.

  --full                 Runs everything. Use for initial setup only.

── WINDOWS TASK SCHEDULER ────────────────────────────────────────────────────
  Run schedule_tasks.bat as Administrator once to create all tasks.
──────────────────────────────────────────────────────────────────────────────
"""
import sys
sys.stdout.reconfigure(encoding='utf-8', line_buffering=True)
import time
import subprocess
import concurrent.futures
from datetime import datetime
from pathlib import Path

from agents.logger import RunLogger
from agents.agent_stats import run as run_stats
from agents.agent_lineups_dk import run as run_lineups_dk
from agents.agent_projections import run as run_projections

REPO_ROOT = Path(__file__).parent

QUICK    = '--quick'    in sys.argv
MORNING  = '--morning'  in sys.argv
POSTGAME = '--postgame' in sys.argv
STATS    = '--stats'    in sys.argv
SPLITS   = '--splits'   in sys.argv
FULL     = '--full' in sys.argv or not any([QUICK, MORNING, POSTGAME, STATS, SPLITS])

mode = ('FULL'      if FULL     else
        'QUICK'     if QUICK    else
        'MORNING'   if MORNING  else
        'POST-GAME' if POSTGAME else
        'STATS'     if STATS    else 'SPLITS')

logger = RunLogger(mode)
print(f"\nSlateHub Refresh — {datetime.now().strftime('%Y-%m-%d %H:%M')} [{mode}]")


def run_script(script: str, label: str, logger: RunLogger, continue_on_fail: bool = True) -> bool:
    """Run a single script, record result, return success bool."""
    import os as _os
    env = {**_os.environ, 'PYTHONIOENCODING': 'utf-8'}
    start = time.time()
    result = subprocess.run(
        ['py', '-3.12'] + script.split(),
        capture_output=False,
        cwd=str(REPO_ROOT),
        env=env,
    )
    elapsed = time.time() - start
    success = result.returncode == 0
    logger.record(label, success, elapsed)
    return success


def run_splits(logger: RunLogger):
    """Run Excel splits refresh sequentially."""
    print(f"\n{'='*55}")
    print(f"  Splits — Excel Power Query + Supabase Upload")
    print(f"{'='*55}")
    run_script('refresh_excel_splits.py', 'Excel Power Query Refresh', logger)
    run_script('sync_excel_splits.py',    'Splits Upload to Supabase', logger)


# ── QUICK — every 15 minutes all day ─────────────────────────────────────────
if QUICK:
    try:
        _, _, new_confirms = run_lineups_dk(logger, mode='quick', quick=True)
        # Re-run projections + ownership when new lineups are confirmed
        if new_confirms > 0:
            print(f"\n  {new_confirms} new lineup confirmation(s) — re-running projections & ownership")
            try:
                run_projections(logger)
            except Exception as e:
                print(f"\n  ERROR in Agent 3 (quick projections): {e}")
                logger.record('Agent 3 — Quick Projections', False, 0.0, str(e))
        else:
            print(f"\n  No new confirmations — projections unchanged")
    except Exception as e:
        print(f"\n  ERROR in Agent 2 (quick): {e}")
        logger.record('Agent 2 — Quick', False, 0.0, str(e))

# ── STATS (7:00 AM) ───────────────────────────────────────────────────────────
if STATS:
    try:
        run_stats(logger)
    except Exception as e:
        print(f"\n  ERROR in Agent 1 (stats): {e}")
        logger.record('Agent 1 — Stats', False, 0.0, str(e))
    run_splits(logger)

# ── MORNING (9:00 AM) ─────────────────────────────────────────────────────────
if MORNING:
    # Refresh yesterday's results before main pipeline
    run_script('load_bullpen.py',            'Bullpen — morning refresh', logger)
    run_script('load_game_logs.py --days 3', 'Game Logs — morning refresh', logger)
    try:
        # Agent 1 (stats) and Agent 2 (lineups/DK) run simultaneously
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as ex:
            stats_future   = ex.submit(run_stats, logger)
            lineups_future = ex.submit(run_lineups_dk, logger, 'morning')

        stats_future.result()  # wait for stats; results already in shared logger
        _, dk_passed = lineups_future.result()

        # Contest data (entry fees, prize pools, payout structures)
        run_script('load_contest_data.py', 'DK Contest Data', logger)

        # Agent 3 only runs if Agent 2 passed its validation gate
        if dk_passed:
            try:
                run_projections(logger)
            except Exception as e:
                print(f"\n  ERROR in Agent 3 (projections): {e}")
                logger.record('Agent 3 — Projections', False, 0.0, str(e))
        else:
            print("\n  PROJECTIONS SKIPPED — DK validation gate failed.")
            print("  Auto-fix could not resolve all mismatches — manual investigation needed, then re-run --morning.")

    except Exception as e:
        print(f"\n  ERROR in morning pipeline: {e}")
        logger.record('Morning Pipeline', False, 0.0, str(e))

# ── FULL — runs everything ────────────────────────────────────────────────────
if FULL:
    # Bullpen first — refresh reliever appearances
    run_script('load_bullpen.py', 'Bullpen — full refresh', logger)
    try:
        # Agent 1 and Agent 2 start simultaneously
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as ex:
            stats_future   = ex.submit(run_stats, logger)
            lineups_future = ex.submit(run_lineups_dk, logger, 'morning')

        stats_future.result()
        _, dk_passed = lineups_future.result()

        run_splits(logger)

        # Contest data (entry fees, prize pools, payout structures)
        run_script('load_contest_data.py', 'DK Contest Data', logger)

        if dk_passed:
            try:
                run_projections(logger)
            except Exception as e:
                print(f"\n  ERROR in Agent 3 (projections): {e}")
                logger.record('Agent 3 — Projections', False, 0.0, str(e))
        else:
            print("\n  PROJECTIONS SKIPPED — DK validation gate failed.")
            print("  Auto-fix could not resolve all mismatches — manual investigation needed, then re-run --morning or --full.")

    except Exception as e:
        print(f"\n  ERROR in full pipeline: {e}")
        logger.record('Full Pipeline', False, 0.0, str(e))

# ── POST-GAME (11:30 PM) ──────────────────────────────────────────────────────
if POSTGAME:
    print(f"\n{'='*55}")
    print(f"  Post-Game Pipeline")
    print(f"{'='*55}")
    run_script('load_bullpen.py',            'Bullpen — final pitch counts', logger)
    run_script('load_game_logs.py --days 3', "Game Logs — today's results",  logger)
    run_script('load_contest_data.py', 'DK Contest Data — final counts', logger)
    run_script('load_actual_ownership.py',  'Actual Ownership — post-lock',  logger)

# ── SPLITS (standalone, 7:30 AM) ─────────────────────────────────────────────
if SPLITS:
    run_splits(logger)

# ── SUMMARY ───────────────────────────────────────────────────────────────────
logger.print_summary()
logger.write_lessons_to_file()
