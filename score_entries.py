#!/usr/bin/env python3
"""Score our DK entries against actuals and contest field."""
import sys
sys.stdout.reconfigure(encoding='utf-8', line_buffering=True)
import csv, re, os
from collections import Counter, defaultdict
from dotenv import load_dotenv
load_dotenv()
from supabase import create_client
import numpy as np

sb = create_client(os.environ['SUPABASE_URL'], os.environ['SUPABASE_KEY'])

ENTRIES_CSV = r"C:\Users\Steffen's PC\Desktop\WebDev\Filtered Sim Output\DKEntries_2026-04-25.csv"
CONTEST_CSV = r"C:\Users\Steffen's PC\Desktop\WebDev\Contest_CSVs\contest-standings-189894003.csv"
GAME_DATE = '2026-04-24'

# Parse our 150 entries
entries = []
with open(ENTRIES_CSV, encoding='utf-8-sig') as f:
    reader = csv.reader(f)
    header = next(reader)
    for row in reader:
        if len(row) < 14:
            continue
        players = []
        for i in range(4, 14):
            val = row[i].strip()
            if not val:
                continue
            m = re.match(r'(.+?)\s*\((\d+)\)', val)
            if m:
                players.append({'name': m.group(1).strip(), 'dk_id': m.group(2), 'pos_idx': i})
        if len(players) == 10:
            entries.append(players)

print(f'Parsed {len(entries)} lineups')

# Player exposure
player_counts = Counter()
pitcher_counts = Counter()
for lu in entries:
    for p in lu:
        player_counts[p['name']] += 1
        if p['pos_idx'] in [4, 5]:
            pitcher_counts[p['name']] += 1

n = len(entries)
print(f'\n{"="*60}')
print(f'  PITCHER EXPOSURE')
print(f'{"="*60}')
for name, cnt in pitcher_counts.most_common():
    print(f'  {name:25s} {cnt:>3d}/{n} ({cnt/n*100:.0f}%)')

print(f'\n{"="*60}')
print(f'  TOP 20 HITTER EXPOSURE')
print(f'{"="*60}')
hitter_counts = {k: v for k, v in player_counts.items() if k not in pitcher_counts}
for name, cnt in sorted(hitter_counts.items(), key=lambda x: -x[1])[:20]:
    print(f'  {name:25s} {cnt:>3d}/{n} ({cnt/n*100:.0f}%)')

# Team stacking
sal_rows = sb.table('dk_salaries').select('name,team,player_id').eq('dk_slate', 'main').limit(5000).execute().data or []
name_to_team = {}
name_to_pid = {}
for r in sal_rows:
    name_to_team[r['name']] = r['team']
    name_to_pid[r['name']] = r['player_id']

print(f'\n{"="*60}')
print(f'  STACK ANALYSIS')
print(f'{"="*60}')
stack_sizes = Counter()
team_exposure = Counter()
for lu in entries:
    teams = Counter()
    for p in lu:
        if p['pos_idx'] not in [4, 5]:
            t = name_to_team.get(p['name'], '?')
            teams[t] += 1
    if teams:
        main = teams.most_common(1)[0]
        stack_sizes[main[1]] += 1
        team_exposure[main[0]] += 1

print(f'  Stack size distribution:')
for size in sorted(stack_sizes.keys(), reverse=True):
    print(f'    {size}-man: {stack_sizes[size]} lineups ({stack_sizes[size]/n*100:.0f}%)')

print(f'\n  Team exposure (as primary stack):')
for team, cnt in team_exposure.most_common():
    print(f'    {team:5s}: {cnt:>3d}/{n} ({cnt/n*100:.0f}%)')

# Score against actuals
print(f'\n{"="*60}')
print(f'  SCORING VS {GAME_DATE} ACTUALS')
print(f'{"="*60}')
actuals = sb.table('actual_results').select('player_id,full_name,actual_dk_pts').eq('game_date', GAME_DATE).limit(5000).execute().data or []
actual_by_name = {}
for a in actuals:
    actual_by_name[a['full_name']] = a['actual_dk_pts']
actual_by_pid = {a['player_id']: a['actual_dk_pts'] for a in actuals}

lineup_scores = []
lineup_details = []
for lu in entries:
    total = 0
    matched = 0
    details = []
    for p in lu:
        pts = actual_by_name.get(p['name'])
        if pts is None:
            pid = name_to_pid.get(p['name'])
            if pid:
                pts = actual_by_pid.get(pid)
        if pts is not None:
            total += pts
            matched += 1
            details.append((p['name'], pts))
        else:
            details.append((p['name'], None))
    if matched >= 8:
        lineup_scores.append(total)
        lineup_details.append(details)

if lineup_scores:
    scores = np.array(lineup_scores)
    print(f'  Matched lineups: {len(lineup_scores)}/{n}')
    print(f'  Avg score:   {np.mean(scores):.1f}')
    print(f'  Median:      {np.median(scores):.1f}')
    print(f'  Best:        {np.max(scores):.1f}')
    print(f'  Worst:       {np.min(scores):.1f}')
    print(f'  P75:         {np.percentile(scores, 75):.1f}')
    print(f'  P90:         {np.percentile(scores, 90):.1f}')
    print(f'  P99:         {np.percentile(scores, 99):.1f}')

    # Best lineup breakdown
    best_idx = np.argmax(scores)
    print(f'\n  Best lineup ({scores[best_idx]:.1f} pts):')
    for name, pts in lineup_details[best_idx]:
        print(f'    {name:25s} {pts if pts is not None else "N/A":>6}')

# Compare against actual contest field
print(f'\n{"="*60}')
print(f'  VS ACTUAL CONTEST FIELD')
print(f'{"="*60}')
if os.path.exists(CONTEST_CSV):
    contest_scores = []
    with open(CONTEST_CSV, encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                pts = float(row.get('Points', 0))
                contest_scores.append(pts)
            except ValueError:
                continue

    if contest_scores and lineup_scores:
        cs = np.array(contest_scores)
        print(f'  Contest entries: {len(cs):,}')
        print(f'  Contest winner: {np.max(cs):.1f}')
        print(f'  Contest top 1%: {np.percentile(cs, 99):.1f}')
        print(f'  Contest cash (~top 22%): {np.percentile(cs, 78):.1f}')
        print(f'  Contest median: {np.median(cs):.1f}')

        # Where would our lineups place?
        cs_sorted = np.sort(cs)[::-1]
        print(f'\n  Our top 10 lineups vs field:')
        sorted_idx = np.argsort(scores)[::-1]
        for rank_i, idx in enumerate(sorted_idx[:10]):
            lu_score = scores[idx]
            field_rank = np.searchsorted(-cs_sorted, -lu_score) + 1
            pct = field_rank / len(cs) * 100
            print(f'    #{rank_i+1}: {lu_score:.1f} pts -> field rank ~{field_rank:,} / {len(cs):,} (top {pct:.1f}%)')

        # Cash rate
        cash_line = np.percentile(cs, 78)
        cashed = sum(1 for s in scores if s >= cash_line)
        print(f'\n  Cash rate: {cashed}/{len(scores)} ({cashed/len(scores)*100:.0f}%) would have cashed (need >{cash_line:.1f})')

        # Top 10% rate
        top10_line = np.percentile(cs, 90)
        in_top10 = sum(1 for s in scores if s >= top10_line)
        print(f'  Top 10%: {in_top10}/{len(scores)} ({in_top10/len(scores)*100:.0f}%) (need >{top10_line:.1f})')
else:
    print(f'  Contest CSV not found at {CONTEST_CSV}')
