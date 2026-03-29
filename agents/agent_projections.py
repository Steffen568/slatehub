#!/usr/bin/env python3
"""
Agent 3 — Projections runner with sanity check.
Only called by the orchestrator after Agent 2 passes the validation gate.
"""
import os
import subprocess
import sys
import time
from datetime import date
from pathlib import Path

from agents.logger import RunLogger

REPO_ROOT = Path(__file__).parent.parent

PROJECTION_SCRIPTS = [
    ('sim_projections.py',     'DFS Projections (Monte Carlo Sim)'),
    ('compute_ownership.py',   'Ownership Projections'),
]


def _run_script(script: str, label: str, logger: RunLogger) -> bool:
    """Run a single script, record result, return success bool."""
    print(f"\n{'='*55}")
    print(f"  {label}")
    print(f"{'='*55}")
    start = time.time()
    env = {**os.environ, 'PYTHONIOENCODING': 'utf-8'}
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


def sanity_check(logger: RunLogger, target_date: str = None):
    """
    Query Supabase and verify projection output is sensible.
    Runs 4 checks; reports all failures (not just the first).
    """
    from dotenv import load_dotenv
    from supabase import create_client

    if target_date is None:
        target_date = str(date.today())

    print(f"\n  [Sanity Check] Verifying projections for {target_date}...")
    start = time.time()

    try:
        load_dotenv()
        sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])
    except Exception as e:
        elapsed = time.time() - start
        print(f"  ⚠ Sanity check: could not connect to Supabase: {e}")
        logger.record('Sanity Check', False, elapsed, 'Supabase connection failed')
        return

    checks_passed = []
    checks_failed = []

    try:
        # Load all projections for today
        rows = (
            sb.table('player_projections')
            .select('player_id, proj_dk_pts, is_pitcher')
            .eq('game_date', target_date)
            .limit(5000)
            .execute()
            .data or []
        )
        total = len(rows)

        # Check 1: Row count — at least 100
        if total < 100:
            msg = f"Only {total} projections for today (expected ≥100)"
            checks_failed.append(msg)
            logger.record('Sanity Check — Row Count', False, 0.0, msg)
            logger.add_lesson(
                title='Projection row count too low after compute_projections.py',
                what_happened=f"Only {total} rows in player_projections for {target_date}",
                rule=(
                    "Check that load_schedule.py ran successfully and lineups "
                    "loaded before projections."
                ),
            )
        else:
            checks_passed.append(f"{total} projections for today")

        # Check 2: Pitcher count — at least 2
        pitchers = [r for r in rows if r.get('is_pitcher')]
        pitcher_count = len(pitchers)
        if pitcher_count < 2:
            msg = f"Only {pitcher_count} pitcher(s) projected (expected ≥2)"
            checks_failed.append(msg)
            logger.record('Sanity Check — Pitcher Count', False, 0.0, msg)
            logger.add_lesson(
                title='Too few pitchers in projections',
                what_happened=(
                    f"Only {pitcher_count} pitcher(s) found in player_projections "
                    f"for {target_date}"
                ),
                rule="Check that load_schedule.py and load_stats.py ran successfully.",
            )
        else:
            checks_passed.append(f"{pitcher_count} pitchers projected")

        # Check 3: Null projection check — no more than 20% null proj_dk_pts
        if total > 0:
            null_count = sum(1 for r in rows if r.get('proj_dk_pts') is None)
            null_pct = null_count / total
            non_null_pct = int((1 - null_pct) * 100)
            if null_pct > 0.20:
                msg = f"{int(null_pct * 100)}% of rows have null proj_dk_pts (threshold: 20%)"
                checks_failed.append(msg)
                logger.record('Sanity Check — Null Projections', False, 0.0, msg)
                logger.add_lesson(
                    title='High null rate in proj_dk_pts',
                    what_happened=(
                        f"{int(null_pct * 100)}% of projections have null proj_dk_pts "
                        f"for {target_date}"
                    ),
                    rule=(
                        "Check that load_stats.py and compute_projections.py "
                        "ran without errors."
                    ),
                )
            else:
                checks_passed.append(f"{non_null_pct}% of players have proj_dk_pts")

        # Check 4: Salary match — at least 50% of batters matched to dk_salaries
        batters = [r for r in rows if not r.get('is_pitcher')]
        if batters:
            batter_ids = list({r['player_id'] for r in batters if r.get('player_id')})
            salary_ids = set()
            # Chunk into ≤150-ID batches (CLAUDE.md rule: never .in() with >150 IDs)
            for i in range(0, len(batter_ids), 150):
                chunk = batter_ids[i:i + 150]
                sal_rows = (
                    sb.table('dk_salaries')
                    .select('player_id')
                    .in_('player_id', chunk)
                    .limit(5000)
                    .execute()
                    .data or []
                )
                salary_ids.update(r['player_id'] for r in sal_rows)

            matched = sum(1 for r in batters if r.get('player_id') in salary_ids)
            match_pct = matched / len(batters)
            match_pct_int = int(match_pct * 100)
            if match_pct < 0.50:
                msg = (
                    f"Only {match_pct_int}% of batters matched to DK salaries "
                    f"(threshold: 50%)"
                )
                checks_failed.append(msg)
                logger.record('Sanity Check — Salary Match', False, 0.0, msg)
                logger.add_lesson(
                    title='Low salary match rate in sanity check',
                    what_happened=(
                        f"Only {match_pct_int}% of projected batters have a dk_salaries "
                        f"row for {target_date}"
                    ),
                    rule=(
                        "Check that load_dk_salaries.py ran successfully and the "
                        "validation gate passed."
                    ),
                )
            else:
                checks_passed.append(f"{match_pct_int}% of batters matched to DK salaries")

    except Exception as e:
        elapsed = time.time() - start
        print(f"  ⚠ Sanity check error: {e}")
        logger.record('Sanity Check', False, elapsed, str(e))
        return

    elapsed = time.time() - start

    if checks_failed:
        print(f"  ✗ Sanity check found {len(checks_failed)} issue(s):")
        for msg in checks_failed:
            print(f"    ✗ {msg}")
    else:
        print(f"  ✓ Sanity check passed:")
        for msg in checks_passed:
            print(f"    ✓ {msg}")
        logger.record('Sanity Check', True, elapsed)


def run(logger: RunLogger, target_date: str = None) -> RunLogger:
    """Run projections + ownership + sanity check. Returns logger with results."""
    print(f"\n{'='*55}")
    print(f"  Agent 3 — Projections")
    print(f"{'='*55}")

    for script, label in PROJECTION_SCRIPTS:
        _run_script(script, label, logger)

    sanity_check(logger, target_date)

    return logger
