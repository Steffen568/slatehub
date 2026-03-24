#!/usr/bin/env python3
"""
diagnose_salary_mismatch.py
Finds players who appear in today's lineups but have no matching dk_salaries row.
Cross-references by name to detect ID mismatches vs genuinely missing players.

Run: py -3.12 diagnose_salary_mismatch.py
     py -3.12 diagnose_salary_mismatch.py --date 2026-03-26
"""
import sys, os, unicodedata
sys.stdout.reconfigure(encoding='utf-8', line_buffering=True)

from datetime import date
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()
sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])

target_date = sys.argv[1] if len(sys.argv) > 1 and not sys.argv[1].startswith('--') else str(date.today())
if '--date' in sys.argv:
    idx = sys.argv.index('--date')
    target_date = sys.argv[idx + 1]

print(f"\nDiagnosing salary ID mismatches for {target_date}")
print("=" * 60)

def norm(name):
    if not name:
        return ''
    nfkd = unicodedata.normalize('NFKD', str(name))
    return nfkd.encode('ASCII', 'ignore').decode('ASCII').strip().lower()

# ── 1. Load today's lineups (confirmed batting order only)
lineup_rows = sb.table('lineups').select(
    'player_id, player_name, team_name'
).eq('game_date', target_date).gte('batting_order', 1).lte('batting_order', 9).execute().data or []

if not lineup_rows:
    print("No lineup data found for this date.")
    sys.exit()

lineup_ids   = list({r['player_id'] for r in lineup_rows if r['player_id']})
lineup_by_id = {r['player_id']: r for r in lineup_rows}
print(f"Lineup players: {len(lineup_ids)}")

# ── 2. Load dk_salaries rows for today (by lineup player_ids)
salary_by_id_rows = sb.table('dk_salaries').select(
    'player_id, name, salary, team, dk_slate'
).in_('player_id', lineup_ids).execute().data or []

matched_ids = {r['player_id'] for r in salary_by_id_rows}
missing_ids = [pid for pid in lineup_ids if pid not in matched_ids]

print(f"Salary matches by player_id: {len(matched_ids)}")
print(f"Missing salary (no player_id match): {len(missing_ids)}")

if not missing_ids:
    print("\n✅ All lineup players have a matching dk_salaries row by player_id — no mismatch!")
    sys.exit()

# ── 3. For missing players, load ALL dk_salaries for today to search by name
all_salary_rows = sb.table('dk_salaries').select(
    'player_id, name, salary, team, dk_slate'
).execute().data or []

salary_by_norm_name = {}
for r in all_salary_rows:
    key = norm(r.get('name', ''))
    if key:
        salary_by_norm_name.setdefault(key, []).append(r)

# ── 4. Report each missing player
print(f"\n{'Player':<28} {'Lineup ID':>12}  {'Result'}")
print(f"{'-'*28} {'-'*12}  {'-'*40}")

id_mismatches  = []
truly_missing  = []

for pid in sorted(missing_ids):
    lu = lineup_by_id.get(pid, {})
    name = lu.get('player_name', 'Unknown')
    team = lu.get('team_name', '?')

    name_key = norm(name)
    salary_candidates = salary_by_norm_name.get(name_key, [])

    if salary_candidates:
        for s in salary_candidates:
            id_mismatches.append({
                'name'       : name,
                'team'       : team,
                'lineup_id'  : pid,
                'dk_id'      : s['player_id'],
                'salary'     : s['salary'],
                'dk_slate'   : s['dk_slate'],
            })
        dk_ids = ', '.join(str(s['player_id']) for s in salary_candidates)
        print(f"  {name:<28} {pid:>12}  ⚠ ID MISMATCH — DK has id(s): {dk_ids}  salary: ${salary_candidates[0]['salary']:,}")
    else:
        truly_missing.append({'name': name, 'team': team, 'lineup_id': pid})
        print(f"  {name:<28} {pid:>12}  ✗ Not in dk_salaries at all (not on slate / not in DK)")

# ── 5. Summary
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

    # ── Auto-generate fix SQL
    print(f"\n── AUTO-FIX SQL (run in Supabase SQL editor) ──")
    print("-- Review carefully before running!")
    for m in id_mismatches:
        print(f"UPDATE dk_salaries SET player_id = {m['lineup_id']} WHERE player_id = {m['dk_id']} AND name = '{m['name']}';")

if truly_missing:
    print(f"\n── TRULY MISSING (not on this slate) ──")
    for m in truly_missing:
        print(f"  {m['name']:<28}  lineup_id={m['lineup_id']}  ({m['team']})")
