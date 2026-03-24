#!/usr/bin/env python3
import sys
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
"""
validate_data.py — SlateHub Data Validation
Checks for player ID mismatches, missing stats, and null data
Run after any data load to catch silent failures before they reach the frontend.

Usage:
  py -3.12 validate_data.py              # validate today's lineups
  py -3.12 validate_data.py --all        # validate all recent lineups (7 days)
  py -3.12 validate_data.py --fix        # attempt auto-fix of ID mismatches
"""

import os, sys, unicodedata
from datetime import date, timedelta
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()
sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])

FIX_MODE = '--fix' in sys.argv
ALL_MODE  = '--all' in sys.argv

def normalize(name):
    if not name or not isinstance(name, str):
        return ''
    nfkd = unicodedata.normalize('NFKD', name)
    return nfkd.encode('ASCII', 'ignore').decode('ASCII').strip().lower()

def divider(title=''):
    print(f"\n{'='*60}")
    if title:
        print(f"  {title}")
        print(f"{'='*60}")

def run():
    today = date.today()
    check_dates = (
        [today + timedelta(days=i) for i in range(-7, 8)]
        if ALL_MODE else
        [today]
    )
    date_strs = [str(d) for d in check_dates]

    divider('SlateHub Data Validation')
    print(f"  Mode: {'ALL (7 days)' if ALL_MODE else 'TODAY'} | Fix: {FIX_MODE}")
    print(f"  Checking dates: {date_strs[0]} → {date_strs[-1]}")

    # ── Load reference data
    print("\nLoading reference data...")

    # All players
    all_players = []
    offset = 0
    while True:
        result = sb.table('players').select(
            'mlbam_id, fangraphs_id, name_normalized'
        ).range(offset, offset + 999).execute()
        if not result.data:
            break
        all_players.extend(result.data)
        if len(result.data) < 1000:
            break
        offset += 1000

    mlbam_id_set  = {p['mlbam_id'] for p in all_players if p['mlbam_id']}
    name_to_mlbam = {}
    for p in all_players:
        if p['name_normalized'] and p['mlbam_id']:
            if p['name_normalized'] not in name_to_mlbam:
                name_to_mlbam[p['name_normalized']] = p['mlbam_id']
    print(f"  Players loaded: {len(all_players):,}")

    # All lineups for date range — paginate to avoid 1000 row limit
    lineups = []
    offset = 0
    while True:
        res = sb.table('lineups').select(
            'player_id, player_name, team_name, game_date, batting_order, status'
        ).in_('game_date', date_strs).range(offset, offset + 999).execute()
        if not res.data:
            break
        lineups.extend(res.data)
        if len(res.data) < 1000:
            break
        offset += 1000
    print(f"  Lineup entries: {len(lineups):,}")

    if not lineups:
        print("\n  No lineup data found for these dates.")
        return

    # All player IDs in lineups
    lineup_ids = list({l['player_id'] for l in lineups if l['player_id']})

    # Batter stats for these players (all seasons)
    batter_res = sb.table('batter_stats').select(
        'player_id, full_name, wrc_plus, woba, pa, season'
    ).in_('player_id', lineup_ids).execute()
    batter_rows = batter_res.data or []

    # Build batter stats map: player_id -> {season: row}
    batter_map = {}
    for b in batter_rows:
        pid = b['player_id']
        if pid not in batter_map:
            batter_map[pid] = {}
        batter_map[pid][b['season']] = b

    # Splits check
    splits_res = sb.table('batter_splits').select(
        'player_id, season'
    ).in_('player_id', lineup_ids).execute()
    splits_ids = {s['player_id'] for s in (splits_res.data or [])}

    # ── CHECK 1: Players not in players table at all
    divider('CHECK 1 — Players not in players table')
    not_in_players = [l for l in lineups if l['player_id'] not in mlbam_id_set]
    unique_missing = {l['player_id']: l for l in not_in_players}

    if unique_missing:
        print(f"  ❌ {len(unique_missing)} lineup players NOT in players table:")
        for pid, p in sorted(unique_missing.items(), key=lambda x: x[1]['player_name']):
            norm = normalize(p['player_name'])
            suggestion = name_to_mlbam.get(norm)
            fix_str = f" → suggest ID {suggestion}" if suggestion else " → no name match found"
            print(f"     {p['player_name']:<28} ID:{pid}{fix_str}")

            if FIX_MODE and suggestion and suggestion != pid:
                print(f"       🔧 AUTO-FIX: updating lineups {pid} → {suggestion}")
                sb.table('lineups').update(
                    {'player_id': suggestion}
                ).eq('player_id', pid).execute()
    else:
        print(f"  ✅ All lineup players found in players table")

    # ── CHECK 2: Players with no batter_stats at all
    divider('CHECK 2 — Players with no batter_stats rows')
    no_stats = [
        l for l in lineups
        if l['player_id'] not in batter_map
        and l['player_id'] in mlbam_id_set  # only flag known players
    ]
    unique_no_stats = {l['player_id']: l for l in no_stats}

    if unique_no_stats:
        print(f"  ⚠️  {len(unique_no_stats)} players in lineups with NO batter_stats rows:")
        for pid, p in sorted(unique_no_stats.items(), key=lambda x: x[1]['player_name']):
            print(f"     {p['player_name']:<28} ID:{pid} ({p['team_name']})")
    else:
        print(f"  ✅ All lineup players have at least one batter_stats row")

    # ── CHECK 3: Players with null stats (placeholder rows)
    divider('CHECK 3 — Players with null/empty stats')
    null_stats = []
    for pid, seasons in batter_map.items():
        for season, row in seasons.items():
            if row['wrc_plus'] is None and row['woba'] is None:
                null_stats.append((pid, season, row.get('full_name', 'Unknown')))

    # Only show players who are in current lineups
    lineup_null = [(pid, s, name) for pid, s, name in null_stats if pid in lineup_ids]
    if lineup_null:
        print(f"  ⚠️  {len(lineup_null)} lineup players have null stats rows:")
        for pid, season, name in sorted(lineup_null, key=lambda x: (x[2] or '')):
            print(f"     {(name or 'Unknown'):<28} ID:{pid}  Season:{season}  (placeholder row — no FanGraphs match)")
    else:
        print(f"  ✅ No null stats rows for lineup players")

    # ── CHECK 4: Coverage summary per team per date
    divider('CHECK 4 — Coverage summary by team')
    from collections import defaultdict
    team_coverage = defaultdict(lambda: {'total': 0, 'has_2025': 0, 'has_splits': 0, 'null_stats': 0})

    for l in lineups:
        pid  = l['player_id']
        team = l['team_name']
        team_coverage[team]['total'] += 1

        if pid in batter_map and 2025 in batter_map[pid]:
            row = batter_map[pid][2025]
            if row['wrc_plus'] is not None:
                team_coverage[team]['has_2025'] += 1
            else:
                team_coverage[team]['null_stats'] += 1

        if pid in splits_ids:
            team_coverage[team]['has_splits'] += 1

    print(f"\n  {'Team':<30} {'Players':>8} {'2025 Stats':>12} {'Splits':>8} {'Null':>6}")
    print(f"  {'-'*30} {'-'*8} {'-'*12} {'-'*8} {'-'*6}")
    for team, c in sorted(team_coverage.items()):
        total = c['total']
        s25   = c['has_2025']
        sp    = c['has_splits']
        null  = c['null_stats']
        flag  = '⚠️ ' if s25 < total * 0.6 else '✅ '
        print(f"  {flag}{team:<28} {total:>8} {s25:>12} {sp:>8} {null:>6}")

    # ── CHECK 5: Players in lineups with null fangraphs_id
    divider('CHECK 5 — Lineup players with null FanGraphs ID')

    fg_res = sb.table('players').select(
        'mlbam_id, name_normalized, fangraphs_id'
    ).in_('mlbam_id', lineup_ids).is_('fangraphs_id', 'null').execute()

    null_fg = fg_res.data or []
    null_fg_notable = [p for p in null_fg if p['mlbam_id'] and p['mlbam_id'] < 750000]

    if null_fg_notable:
        print(f"  \u26a0\ufe0f  {len(null_fg_notable)} lineup players have null FanGraphs ID (may be missing stats):")
        for p in sorted(null_fg_notable, key=lambda x: x['name_normalized'] or ''):
            in_stats = '\u2705 has stats' if p['mlbam_id'] in batter_map else '\u274c no stats'
            print(f"     {(p['name_normalized'] or 'Unknown'):<28} MLBAM:{p['mlbam_id']}  {in_stats}")
        print(f"\n  \U0001f4a1 Fix: find FanGraphs ID at fangraphs.com/players/[name]/[ID]/stats/batting")
    else:
        print(f"  \u2705 All notable lineup players have FanGraphs IDs mapped")

    # ── SUMMARY
    divider('SUMMARY')
    total_players = len(lineup_ids)
    covered       = sum(1 for pid in lineup_ids if pid in batter_map and
                       any(r['wrc_plus'] is not None for r in batter_map[pid].values()))
    pct = (covered / total_players * 100) if total_players else 0

    print(f"  Total unique lineup players:  {total_players}")
    print(f"  Players with real stats:      {covered} ({pct:.0f}%)")
    print(f"  Players missing from table:   {len(unique_missing)}")
    print(f"  Players with no stats rows:   {len(unique_no_stats)}")
    print(f"  Players with null stats:      {len(lineup_null)}")

    if len(unique_missing) > 0 or len(unique_no_stats) > 0:
        print(f"\n  💡 Run with --fix to auto-correct ID mismatches")
        print(f"  💡 Run load_stats.py to populate missing stats")
    else:
        print(f"\n  ✅ Data looks clean — ready for Opening Day")

if __name__ == "__main__":
    run()