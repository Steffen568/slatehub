#!/usr/bin/env python3
"""
Agent 2 — Lineups + DK pipeline with validation gate.
Runs sequentially. After load_dk_salaries, runs a salary validation gate.
If the gate fails, Agent 3 (projections) must not run.
"""
import json
import os
import subprocess
import time
from datetime import date
from pathlib import Path

from agents.logger import RunLogger

REPO_ROOT = Path(__file__).parent.parent
STATE_FILE = Path(__file__).parent / 'lineup_state.json'

# Full morning pipeline — validation gate runs between index 2 and 3
LINEUP_DK_SCRIPTS_PRE_GATE = [
    ('load_schedule.py',    'Schedule & Lineups'),
    ('load_dk_slates.py',   'DK Slates'),
    ('load_dk_salaries.py', 'DK Salaries'),
]
LINEUP_DK_SCRIPTS_POST_GATE = [
    ('load_odds.py',    'Odds'),
    ('load_weather.py', 'Weather'),
]

# Quick mode: only schedule + weather
QUICK_SCRIPTS = [
    ('load_schedule.py', 'Schedule & Lineups'),
    ('load_weather.py',  'Weather'),
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


def validate_dk_salaries(logger: RunLogger) -> bool:
    """
    Run diagnose_salary_mismatch.py and parse its output.
    Returns True if validation passes (0 ID mismatches).
    Returns False if new mismatches are found — logs lessons and returns failure.
    """
    print(f"\n  [Validation Gate] Running salary ID check...")
    start = time.time()
    env = {**os.environ, 'PYTHONIOENCODING': 'utf-8'}
    result = subprocess.run(
        ['py', '-3.12', str(REPO_ROOT / 'diagnose_salary_mismatch.py')],
        capture_output=True,
        text=True,
        encoding='utf-8',
        cwd=str(REPO_ROOT),
        env=env,
    )
    elapsed = time.time() - start

    mismatch_lines = [
        line for line in result.stdout.splitlines()
        if 'ID MISMATCH' in line
    ]

    if not mismatch_lines:
        print(f"  ✓ Salary Validation — 0 ID mismatches ({elapsed:.1f}s)")
        logger.record('Salary Validation', True, elapsed)
        return True

    # Mismatches found
    print(f"\n  ⚠ Salary Validation FAILED — {len(mismatch_lines)} ID mismatch(es) found:")
    for line in mismatch_lines:
        print(f"    {line.strip()}")

    logger.record('Salary Validation', False, elapsed, f"{len(mismatch_lines)} ID mismatch(es)")

    # Log a lesson for each mismatch (add_lesson deduplicates by title)
    for line in mismatch_lines:
        stripped = line.strip()
        # Line format: "  {name:<28} {id:>12}  ⚠ ID MISMATCH — DK has id(s): {dk_ids} ..."
        parts = stripped.split('⚠ ID MISMATCH')
        player_info = parts[0].strip() if parts else stripped
        player_name = ' '.join(player_info.split()) if player_info else 'unknown'
        title = f"DK ID mismatch: {player_name}"
        logger.add_lesson(
            title=title,
            what_happened=f"diagnose_salary_mismatch.py found: {stripped}",
            rule=(
                "Add the wrong_id → correct_mlbam_id mapping to PLAYER_ID_REMAP "
                "in load_dk_salaries.py, then re-run load_dk_salaries.py."
            ),
        )

    return False


def _load_lineup_state() -> dict:
    today = str(date.today())
    if STATE_FILE.exists():
        try:
            state = json.loads(STATE_FILE.read_text(encoding='utf-8'))
            if state.get('date') == today:
                return state
        except Exception:
            pass
    return {'date': today, 'confirmed_game_pks': []}


def _save_lineup_state(state: dict):
    try:
        STATE_FILE.write_text(json.dumps(state, indent=2), encoding='utf-8')
    except Exception as e:
        print(f"  WARNING: Could not write lineup_state.json: {e}")


def _update_lineup_confirmation(state: dict):
    """Query Supabase for today's confirmed lineups and update state file."""
    try:
        import os
        from dotenv import load_dotenv
        from supabase import create_client
        load_dotenv()
        sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])

        today = state['date']
        rows = (
            sb.table('lineups')
            .select('game_pk')
            .eq('game_date', today)
            .eq('status', 'confirmed')
            .limit(5000)
            .execute()
            .data or []
        )
        newly_confirmed_pks = list({r['game_pk'] for r in rows if r.get('game_pk')})

        already = set(state['confirmed_game_pks'])
        new_pks = [pk for pk in newly_confirmed_pks if pk not in already]

        if new_pks:
            print(f"  ✓ {len(new_pks)} newly confirmed game(s): {new_pks}")
        already_confirmed = [pk for pk in newly_confirmed_pks if pk in already]
        if already_confirmed:
            print(f"  — {len(already_confirmed)} game(s) already confirmed — skipped re-poll")

        state['confirmed_game_pks'] = list(already | set(newly_confirmed_pks))

        # Check whether all today's games are now confirmed
        all_rows = (
            sb.table('lineups')
            .select('game_pk')
            .eq('game_date', today)
            .limit(5000)
            .execute()
            .data or []
        )
        all_pks = list({r['game_pk'] for r in all_rows if r.get('game_pk')})
        confirmed_set = set(state['confirmed_game_pks'])
        if all_pks and all(pk in confirmed_set for pk in all_pks):
            print(f"  ✓ All lineups confirmed — quick poll skipped next run")

    except Exception as e:
        print(f"  WARNING: Lineup state check failed: {e}")


def run(logger: RunLogger, mode: str, quick: bool = False) -> tuple:
    """
    Run lineup + DK pipeline.
    Returns (logger, passed_validation_gate).
    """
    print(f"\n{'='*55}")
    print(f"  Agent 2 — Lineups & DK [{mode.upper()}]")
    print(f"{'='*55}")

    # Quick mode: only schedule + weather, no DK or validation gate
    if quick or mode == 'quick':
        state = _load_lineup_state()
        for script, label in QUICK_SCRIPTS:
            _run_script(script, label, logger)
        _update_lineup_confirmation(state)
        _save_lineup_state(state)
        return logger, True

    # Full pipeline: schedule → dk_slates → dk_salaries → [gate] → odds → weather
    dk_salary_loaded = False
    for script, label in LINEUP_DK_SCRIPTS_PRE_GATE:
        success = _run_script(script, label, logger)
        if label == 'DK Salaries':
            dk_salary_loaded = success

    # Validation gate — must pass before continuing
    if not dk_salary_loaded:
        print("\n  ⚠ DK Salaries script failed — skipping validation gate and remaining scripts.")
        logger.record('Salary Validation', False, 0.0, 'DK Salaries script failed')
        return logger, False

    gate_passed = validate_dk_salaries(logger)

    if not gate_passed:
        print("\n  ✗ Validation gate FAILED — skipping Odds and Weather.")
        print("  Fix PLAYER_ID_REMAP issues first, then re-run --morning.")
        return logger, False

    # Gate passed — run odds + weather
    for script, label in LINEUP_DK_SCRIPTS_POST_GATE:
        _run_script(script, label, logger)

    return logger, True
