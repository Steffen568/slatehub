#!/usr/bin/env python3
import sys
sys.stdout.reconfigure(encoding='utf-8', line_buffering=True)
"""
load_pinch_hit_rates.py

Tracks which batters get pinch hit for mid-game and how often.

Detection: uses play-by-play offensive_substitution events (position=PH)
to find who was replaced, cross-referenced with the boxscore batters list
to separate starters from incoming PHs.

Usage:
  py -3.12 load_pinch_hit_rates.py              # last 3 days
  py -3.12 load_pinch_hit_rates.py --days 14
  py -3.12 load_pinch_hit_rates.py --season 2026
"""

import os, sys, re, time, requests, datetime
from collections import defaultdict
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()
sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])

MLB_API = "https://statsapi.mlb.com/api/v1"

# ── Arg parsing ───────────────────────────────────────────────────────────────
today = datetime.date.today()

if '--season' in sys.argv:
    season = int(sys.argv[sys.argv.index('--season') + 1])
    start  = datetime.date(season, 3, 20)
    end    = min(today - datetime.timedelta(days=1), datetime.date(season, 10, 1))
else:
    days  = int(sys.argv[sys.argv.index('--days') + 1]) if '--days' in sys.argv else 3
    end   = today - datetime.timedelta(days=1)
    start = end - datetime.timedelta(days=days - 1)
    season = today.year

print(f"\nPinch-Hit Rates — {start} → {end} (season {season})")
print("=" * 50)

# ── Step 1: Fetch completed game_pks ─────────────────────────────────────────
url = (f"{MLB_API}/schedule?sportId=1"
       f"&startDate={start}&endDate={end}"
       f"&gameType=R&hydrate=team")
try:
    data = requests.get(url, timeout=30).json()
except Exception as e:
    print(f"ERROR fetching schedule: {e}")
    sys.exit(1)

game_pks = []
for date_entry in data.get('dates', []):
    for game in date_entry.get('games', []):
        if game.get('status', {}).get('abstractGameState') == 'Final':
            game_pks.append({'game_pk': game['gamePk'], 'game_date': date_entry['date']})

print(f"Found {len(game_pks)} completed games")
if not game_pks:
    print("Nothing to process.")
    sys.exit(0)

# ── Step 2: Parse each game for PH substitution events ───────────────────────
log_rows = []
errors   = 0

for i, gm in enumerate(game_pks):
    game_pk   = gm['game_pk']
    game_date = gm['game_date']
    print(f"  [{i+1}/{len(game_pks)}] pk={game_pk} {game_date}", end=" ")

    try:
        box = requests.get(f"{MLB_API}/game/{game_pk}/boxscore", timeout=30).json()
        pbp = requests.get(f"{MLB_API}/game/{game_pk}/playByPlay", timeout=30).json()
    except Exception as e:
        print(f"ERROR: {e}")
        errors += 1
        time.sleep(0.1)
        continue

    all_plays = pbp.get('allPlays', [])

    for side in ['home', 'away']:
        team_box  = box.get('teams', {}).get(side, {})
        players   = team_box.get('players', {})
        all_batters = set(team_box.get('batters', []))

        if not all_batters:
            continue

        # Build name → player_id from boxscore (needed to resolve replaced-player descriptions)
        name_to_id = {p['person']['fullName']: p['person']['id']
                      for p in players.values() if 'person' in p}

        # Which team is this side?
        team_id = team_box.get('team', {}).get('id')

        # Find PH substitution events for this team's batters
        ph_incoming = set()   # player IDs who entered as PH (not starters)
        ph_for_set  = set()   # player IDs who were pinch hit for

        for play in all_plays:
            batter_id = play.get('matchup', {}).get('batter', {}).get('id')
            # Only consider plays where this team's player is batting
            if batter_id not in all_batters:
                continue
            for ev in play.get('playEvents', []):
                if not ev.get('isSubstitution'):
                    continue
                et  = ev.get('details', {}).get('eventType', '')
                pos = ev.get('position', {}).get('abbreviation', '')
                if et == 'offensive_substitution' and pos == 'PH':
                    ph_incoming.add(ev['player']['id'])
                    desc = ev.get('details', {}).get('description', '')
                    m = re.search(r'replaces ([^.]+)', desc)
                    if m:
                        replaced_name = m.group(1).strip()
                        replaced_id   = name_to_id.get(replaced_name)
                        if replaced_id:
                            ph_for_set.add(replaced_id)

        # Starters = everyone who batted minus incoming PHs
        starters = all_batters - ph_incoming

        for pid in starters:
            name = (players.get(f'ID{pid}', {}).get('person', {}).get('fullName') or '')
            log_rows.append({
                'player_id':   pid,
                'game_pk':     game_pk,
                'season':      season,
                'player_name': name,
                'started':     1,
                'ph_for':      1 if pid in ph_for_set else 0,
            })

    print("OK")
    time.sleep(0.05)

print(f"\nParsed {len(log_rows)} player-game records ({errors} errors)")

# ── Step 3: Upsert game log (idempotent) ──────────────────────────────────────
if log_rows:
    print("Uploading game log...")
    BATCH    = 500
    uploaded = 0
    for i in range(0, len(log_rows), BATCH):
        sb.table('pinch_hit_game_log').upsert(
            log_rows[i:i + BATCH],
            on_conflict='player_id,game_pk',
            ignore_duplicates=True
        ).execute()
        uploaded += len(log_rows[i:i + BATCH])
        print(f"  {uploaded:,} / {len(log_rows):,}")
    print(f"  Done. {uploaded:,} rows upserted.")

# ── Step 4: Recompute season rates from full game log ─────────────────────────
print(f"\nAggregating {season} rates from game log...")

all_rows, offset, PAGE = [], 0, 1000
while True:
    page = (sb.table('pinch_hit_game_log')
              .select('player_id,player_name,started,ph_for')
              .eq('season', season)
              .range(offset, offset + PAGE - 1)
              .execute().data or [])
    all_rows.extend(page)
    if len(page) < PAGE:
        break
    offset += PAGE

print(f"  {len(all_rows):,} game-log rows loaded")

agg = defaultdict(lambda: {'starts': 0, 'ph_for': 0, 'name': ''})
for r in all_rows:
    pid = r['player_id']
    agg[pid]['starts'] += r['started']
    agg[pid]['ph_for'] += r['ph_for']
    if r['player_name']:
        agg[pid]['name'] = r['player_name']

rate_rows = [{
    'player_id':     pid,
    'player_name':   d['name'],
    'season':        season,
    'games_started': d['starts'],
    'ph_for_count':  d['ph_for'],
    'ph_for_rate':   round(d['ph_for'] / d['starts'], 4) if d['starts'] > 0 else None,
} for pid, d in agg.items()]

if rate_rows:
    for i in range(0, len(rate_rows), 500):
        sb.table('pinch_hit_rates').upsert(
            rate_rows[i:i + 500],
            on_conflict='player_id,season'
        ).execute()
    print(f"  {len(rate_rows)} players upserted to pinch_hit_rates")

# ── Step 5: Summary ───────────────────────────────────────────────────────────
qualifying = sorted(
    [r for r in rate_rows if r['games_started'] >= 10 and r['ph_for_count'] > 0],
    key=lambda r: r['ph_for_rate'] or 0, reverse=True
)[:15]

print(f"\nTop pinch-hit-for rates ({season}, min 10 starts):")
for r in qualifying:
    print(f"  {r['player_name']:<25} {r['ph_for_count']:>3}/{r['games_started']:<3}  ({r['ph_for_rate']*100:.1f}%)")

print(f"\nDone. {len(game_pks)} games processed, {errors} errors.")
