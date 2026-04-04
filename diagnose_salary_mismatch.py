#!/usr/bin/env python3
"""
diagnose_salary_mismatch.py
Finds players who appear in today's lineups but have no matching dk_salaries row.
Cross-references by name to detect ID mismatches vs genuinely missing players.

Can be run standalone or imported by the agent pipeline.

Run: py -3.12 diagnose_salary_mismatch.py
     py -3.12 diagnose_salary_mismatch.py --date 2026-03-26
"""
import sys, os, unicodedata

from datetime import date
from supabase import create_client
from dotenv import load_dotenv

# Teams that will never appear on DK (minor leagues, international leagues, etc.)
NON_MLB_TEAMS = {
    'sugar land space cowboys',
    'sultanes de monterrey',
}


def norm(name):
    if not name:
        return ''
    nfkd = unicodedata.normalize('NFKD', str(name))
    return nfkd.encode('ASCII', 'ignore').decode('ASCII').strip().lower()


def find_mismatches(target_date=None, sb=None):
    """
    Core mismatch detection logic. Returns (id_mismatches, truly_missing).

    id_mismatches: list of dicts with keys: name, team, lineup_id, dk_id, salary, dk_slate
    truly_missing: list of dicts with keys: name, team, lineup_id

    Can be called from the agent pipeline without subprocess overhead.
    """
    if sb is None:
        load_dotenv()
        sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])

    if target_date is None:
        target_date = str(date.today())

    # ── 1. Load today's lineups (confirmed batting order only)
    lineup_rows = sb.table('lineups').select(
        'player_id, player_name, team_name'
    ).eq('game_date', target_date).gte('batting_order', 1).lte('batting_order', 9).execute().data or []

    if not lineup_rows:
        return [], []

    lineup_ids   = list({r['player_id'] for r in lineup_rows if r['player_id']})
    lineup_by_id = {r['player_id']: r for r in lineup_rows}

    # ── 2. Load dk_salaries rows for today (by lineup player_ids)
    salary_by_id_rows = sb.table('dk_salaries').select(
        'player_id, name, salary, team, dk_slate'
    ).in_('player_id', lineup_ids).execute().data or []

    matched_ids = {r['player_id'] for r in salary_by_id_rows}
    missing_ids = [pid for pid in lineup_ids if pid not in matched_ids]

    if not missing_ids:
        return [], []

    # ── 3. For missing players, load ALL dk_salaries to search by name (paginated)
    all_salary_rows = []
    _offset = 0
    while True:
        _rows = sb.table('dk_salaries').select(
            'player_id, name, salary, team, dk_slate'
        ).range(_offset, _offset + 999).execute().data or []
        all_salary_rows.extend(_rows)
        if len(_rows) < 1000:
            break
        _offset += 1000

    salary_by_norm_name = {}
    for r in all_salary_rows:
        key = norm(r.get('name', ''))
        if key:
            salary_by_norm_name.setdefault(key, []).append(r)

    # ── 4. Classify each missing player
    id_mismatches = []
    truly_missing = []

    for pid in sorted(missing_ids):
        lu = lineup_by_id.get(pid, {})
        name = lu.get('player_name', 'Unknown')
        team = lu.get('team_name', '?')

        name_key = norm(name)
        salary_candidates = salary_by_norm_name.get(name_key, [])

        if salary_candidates:
            # Filter out salary rows that already match a different lineup player
            # (same name, different person — e.g. two "David Hamilton" on different teams)
            real_mismatches = [s for s in salary_candidates if s['player_id'] not in matched_ids]
            if real_mismatches:
                for s in real_mismatches:
                    id_mismatches.append({
                        'name'       : name,
                        'team'       : team,
                        'lineup_id'  : pid,
                        'dk_id'      : s['player_id'],
                        'salary'     : s['salary'],
                        'dk_slate'   : s['dk_slate'],
                    })
            else:
                truly_missing.append({'name': name, 'team': team, 'lineup_id': pid})
        else:
            truly_missing.append({'name': name, 'team': team, 'lineup_id': pid})

    return id_mismatches, truly_missing


def print_report(id_mismatches, truly_missing):
    """Pretty-print the mismatch report (used by CLI mode)."""
    print(f"\n{'Player':<28} {'Lineup ID':>12}  {'Result'}")
    print(f"{'-'*28} {'-'*12}  {'-'*40}")

    # Print mismatches
    seen_pids = set()
    for m in id_mismatches:
        if m['lineup_id'] in seen_pids:
            continue
        seen_pids.add(m['lineup_id'])
        dk_ids = ', '.join(
            str(x['dk_id']) for x in id_mismatches if x['lineup_id'] == m['lineup_id']
        )
        print(f"  {m['name']:<28} {m['lineup_id']:>12}  ⚠ ID MISMATCH — DK has id(s): {dk_ids}  salary: ${m['salary']:,}")

    for m in truly_missing:
        print(f"  {m['name']:<28} {m['lineup_id']:>12}  ✗ Not in dk_salaries at all (not on slate / not in DK)")

    # Summary
    print(f"\n{'='*60}")
    print(f"SUMMARY")
    print(f"  ID mismatches (same name, different player_id): {len(id_mismatches)}")
    print(f"  Truly missing from dk_salaries:                {len(truly_missing)}")

    if id_mismatches:
        print(f"\n── ID MISMATCHES (these need a fix) ──")
        print(f"  The player_id in `lineups` differs from `dk_salaries`.")
        print(f"  Fix: update dk_salaries.player_id to match lineups.player_id")
        print()
        for m in id_mismatches:
            print(f"  {m['name']:<28}  lineup_id={m['lineup_id']}  dk_id={m['dk_id']}  [{m['dk_slate']}  ${m['salary']:,}]")

        # Auto-generate fix SQL
        print(f"\n── AUTO-FIX SQL (run in Supabase SQL editor) ──")
        print("-- Review carefully before running!")
        seen_sql = set()
        for m in id_mismatches:
            key = (m['lineup_id'], m['dk_id'])
            if key in seen_sql:
                continue
            seen_sql.add(key)
            print(f"UPDATE dk_salaries SET player_id = {m['lineup_id']} WHERE player_id = {m['dk_id']} AND name = '{m['name']}';")

    if truly_missing:
        mlb_missing  = [m for m in truly_missing if m['team'].lower() not in NON_MLB_TEAMS]
        skip_missing = [m for m in truly_missing if m['team'].lower() in NON_MLB_TEAMS]
        if mlb_missing:
            print(f"\n── TRULY MISSING (not on this slate) ──")
            for m in mlb_missing:
                print(f"  {m['name']:<28}  lineup_id={m['lineup_id']}  ({m['team']})")
        if skip_missing:
            print(f"\n── SKIPPED (non-MLB teams — will never be on DK) ──")
            for m in skip_missing:
                print(f"  {m['name']:<28}  ({m['team']})")


# ── CLI entry point ──────────────────────────────────────────────────────────
if __name__ == '__main__':
    sys.stdout.reconfigure(encoding='utf-8', line_buffering=True)

    load_dotenv()
    sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])

    target_date = sys.argv[1] if len(sys.argv) > 1 and not sys.argv[1].startswith('--') else str(date.today())
    if '--date' in sys.argv:
        idx = sys.argv.index('--date')
        target_date = sys.argv[idx + 1]

    print(f"\nDiagnosing salary ID mismatches for {target_date}")
    print("=" * 60)

    id_mismatches, truly_missing = find_mismatches(target_date, sb)

    if not id_mismatches and not truly_missing:
        print("\n✅ All lineup players have a matching dk_salaries row by player_id — no mismatch!")
    else:
        print_report(id_mismatches, truly_missing)
