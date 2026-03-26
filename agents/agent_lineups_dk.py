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


def _fix_mismatches_in_db(mismatches, sb):
    """
    Fix ID mismatches directly in Supabase dk_salaries table.
    Updates player_id on each mismatched row to match the lineups player_id.
    Returns number of rows successfully fixed.
    """
    fixed = 0
    seen = set()
    for m in mismatches:
        key = (m['lineup_id'], m['dk_id'])
        if key in seen:
            continue
        seen.add(key)
        try:
            sb.table('dk_salaries').update(
                {'player_id': m['lineup_id']}
            ).eq('player_id', m['dk_id']).eq('name', m['name']).execute()
            print(f"    ✓ Fixed {m['name']}: {m['dk_id']} → {m['lineup_id']}")
            fixed += 1
        except Exception as e:
            print(f"    ✗ Failed to fix {m['name']}: {e}")
    return fixed


def _add_to_player_id_remap(mismatches):
    """
    Append new entries to PLAYER_ID_REMAP in load_dk_salaries.py so future
    runs resolve correctly without needing another auto-fix.
    """
    remap_file = REPO_ROOT / 'load_dk_salaries.py'
    content = remap_file.read_text(encoding='utf-8')

    # Deduplicate: only add mappings not already in the file
    new_entries = {}
    for m in mismatches:
        dk_id = m['dk_id']
        lineup_id = m['lineup_id']
        # Check if this dk_id is already mapped
        if str(dk_id) in content.split('PLAYER_ID_REMAP')[1].split('}')[0]:
            continue
        new_entries[dk_id] = (lineup_id, m['name'])

    if not new_entries:
        return 0

    # Find the closing brace of PLAYER_ID_REMAP dict
    marker = 'PLAYER_ID_REMAP = {'
    start_idx = content.index(marker)
    # Find the closing } for this dict
    brace_depth = 0
    closing_idx = None
    for i in range(start_idx + len(marker) - 1, len(content)):
        if content[i] == '{':
            brace_depth += 1
        elif content[i] == '}':
            brace_depth -= 1
            if brace_depth == 0:
                closing_idx = i
                break

    if closing_idx is None:
        print("    ⚠ Could not locate PLAYER_ID_REMAP closing brace — skipping file update")
        return 0

    # Build new lines to insert before the closing brace
    new_lines = ''
    for dk_id, (lineup_id, name) in sorted(new_entries.items()):
        new_lines += f'    {dk_id:<8}: {lineup_id},   # {name} (auto-fixed)\n'

    # Insert before the closing }
    updated = content[:closing_idx] + new_lines + content[closing_idx:]
    remap_file.write_text(updated, encoding='utf-8')
    print(f"    ✓ Added {len(new_entries)} new entry/entries to PLAYER_ID_REMAP")
    return len(new_entries)


def validate_dk_salaries(logger: RunLogger) -> bool:
    """
    Check for salary ID mismatches. If found, auto-fix them in the DB
    and update PLAYER_ID_REMAP, then re-validate.
    Returns True if validation passes (0 ID mismatches after fix attempt).
    """
    # Import the refactored detection function directly (no subprocess)
    import sys
    sys.path.insert(0, str(REPO_ROOT))
    from diagnose_salary_mismatch import find_mismatches

    from dotenv import load_dotenv
    from supabase import create_client
    load_dotenv()
    sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])

    print(f"\n  [Validation Gate] Running salary ID check...")
    start = time.time()

    id_mismatches, truly_missing = find_mismatches(sb=sb)
    elapsed = time.time() - start

    if not id_mismatches:
        print(f"  ✓ Salary Validation — 0 ID mismatches ({elapsed:.1f}s)")
        logger.record('Salary Validation', True, elapsed)
        return True

    # ── Mismatches found — attempt auto-fix ──
    print(f"\n  ⚠ Found {len(id_mismatches)} ID mismatch(es) — attempting auto-fix...")
    for m in id_mismatches:
        print(f"    {m['name']:<28}  lineup_id={m['lineup_id']}  dk_id={m['dk_id']}")

    # Step 1: Fix rows in Supabase dk_salaries table
    fixed_db = _fix_mismatches_in_db(id_mismatches, sb)
    print(f"\n  DB fixes applied: {fixed_db}")

    # Step 2: Add entries to PLAYER_ID_REMAP for future runs
    added_remap = _add_to_player_id_remap(id_mismatches)

    # Step 3: Re-validate to confirm fix worked
    print(f"\n  [Validation Gate] Re-validating after auto-fix...")
    recheck_start = time.time()
    id_mismatches_2, _ = find_mismatches(sb=sb)
    recheck_elapsed = time.time() - recheck_start
    total_elapsed = time.time() - start

    if not id_mismatches_2:
        print(f"  ✓ Auto-fix successful — 0 ID mismatches remaining ({total_elapsed:.1f}s)")
        logger.record('Salary Validation (auto-fixed)', True, total_elapsed,
                       f"Fixed {fixed_db} mismatch(es), added {added_remap} REMAP entry/entries")

        # Log lesson so we have a record of what was auto-fixed
        names = ', '.join(sorted({m['name'] for m in id_mismatches}))
        logger.add_lesson(
            title=f"Auto-fixed DK ID mismatches: {names}",
            what_happened=f"Pipeline auto-fixed {fixed_db} salary ID mismatch(es) in dk_salaries and added {added_remap} PLAYER_ID_REMAP entry/entries.",
            rule="Auto-fix handled it. If the same player keeps appearing, investigate the root cause in the players table.",
        )
        return True

    # Auto-fix didn't fully resolve — log failures and block
    print(f"\n  ✗ Auto-fix incomplete — {len(id_mismatches_2)} mismatch(es) remain:")
    for m in id_mismatches_2:
        print(f"    {m['name']:<28}  lineup_id={m['lineup_id']}  dk_id={m['dk_id']}")

    logger.record('Salary Validation', False, total_elapsed,
                   f"{len(id_mismatches_2)} mismatch(es) remain after auto-fix")

    for m in id_mismatches_2:
        logger.add_lesson(
            title=f"DK ID mismatch (auto-fix failed): {m['name']}",
            what_happened=f"Auto-fix could not resolve: lineup_id={m['lineup_id']} dk_id={m['dk_id']}",
            rule="Manual investigation needed — check players table or DK API for this player.",
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
        print("\n  ✗ Validation gate FAILED — auto-fix could not resolve all mismatches.")
        print("  Manual investigation needed, then re-run --morning.")
        return logger, False

    # Gate passed — run odds + weather
    for script, label in LINEUP_DK_SCRIPTS_POST_GATE:
        _run_script(script, label, logger)

    return logger, True
