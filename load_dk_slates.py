import sys
sys.stdout.reconfigure(encoding='utf-8', line_buffering=True)
print("load_dk_slates.py started", flush=True)

import urllib.request
import json
import os
import time
from datetime import datetime, timezone
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    print("ERROR: Missing credentials.")
    exit()

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
print("Connected to Supabase.")

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
}

def fetch_json(url):
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.loads(r.read())

# ── STEP 1: Get all MLB contests + draft groups
print("\nFetching MLB contests from DraftKings...")
data = fetch_json('https://www.draftkings.com/lobby/getcontests?sport=MLB')

contests  = data.get('Contests', [])
dg_list   = data.get('DraftGroups', [])

print(f"  Found {len(contests)} contests, {len(dg_list)} draft groups")

# ── STEP 2: Filter to Classic MLB draft groups only
# Classic gameTypeId = 1 or 2; exclude Showdown (single game), Tiers, etc.
# We identify Classic by looking at contests that reference each DG
classic_dg_ids = set()
for c in contests:
    if c.get('gameType') == 'Classic' and c.get('dg'):
        classic_dg_ids.add(c['dg'])

print(f"  Classic draft group IDs: {len(classic_dg_ids)}")

# Build DG metadata map
dg_meta = {}
for dg in dg_list:
    dgid = dg.get('DraftGroupId')
    if dgid in classic_dg_ids:
        dg_meta[dgid] = {
            'start_est': dg.get('StartDateEst', ''),
            'game_count': dg.get('GameCount', 0),
            'suffix': dg.get('ContestStartTimeSuffix', ''),
        }

# ── STEP 3: For each Classic DG, fetch draftables to get game matchups
# We deduplicate by competitionId to get unique games per DG
print("\nFetching draftables for each Classic draft group...")

# slate_games: maps competitionId -> { away, home, start_time, dk_slate_name, dg_id }
slate_games = {}

for dgid in sorted(classic_dg_ids):
    try:
        url = f'https://api.draftkings.com/draftgroups/v1/draftgroups/{dgid}/draftables'
        result = fetch_json(url)
        draftables = result.get('draftables', [])

        seen_comps = {}
        for p in draftables:
            comp = p.get('competition', {})
            comp_id = comp.get('competitionId')
            if not comp_id or comp_id in seen_comps:
                continue
            name = comp.get('name', '')  # e.g. "NYY @ PHI"
            start = comp.get('startTime', '')
            parts = name.split(' @ ')
            if len(parts) == 2:
                seen_comps[comp_id] = {
                    'away': parts[0].strip(),
                    'home': parts[1].strip(),
                    'start_utc': start,
                    'dg_id': dgid,
                }

        # Determine slate name from DG suffix or start time
        meta = dg_meta.get(dgid, {})
        suffix = meta.get('suffix', '').strip()
        start_est_str = meta.get('start_est', '')

        # Parse start hour ET to assign slate label
        slate_label = 'main'
        if start_est_str:
            try:
                dt = datetime.fromisoformat(start_est_str.replace('Z', ''))
                et_hour = dt.hour + dt.minute / 60
                if et_hour < 13:
                    slate_label = 'early'
                elif et_hour < 17:
                    slate_label = 'afternoon'
                elif et_hour < 19.5:
                    slate_label = 'main'
                else:
                    slate_label = 'late'
            except Exception:
                pass

        for comp_id, game in seen_comps.items():
            # Only add if not already seen from another DG (first DG wins)
            if comp_id not in slate_games:
                game['dk_slate'] = slate_label
                slate_games[comp_id] = game

        print(f"  DG {dgid} ({slate_label}): {len(seen_comps)} games — {suffix}")
        time.sleep(0.3)  # be polite

    except Exception as e:
        print(f"  ERROR fetching DG {dgid}: {e}")

print(f"\nTotal unique DK games found: {len(slate_games)}")

# ── STEP 4: Match DK games to Supabase games table and update dk_slate

# DK uses standard MLB abbreviations; normalize a few known differences
DK_TO_DB = {
    'WSH': 'WSH', 'WAS': 'WSH',
    'CWS': 'CWS', 'CHW': 'CWS',
    'CHC': 'CHC',
    'KC':  'KC',
    'SD':  'SD',
    'SF':  'SF',
    'TB':  'TB',
    'LAA': 'LAA', 'ANA': 'LAA',
    'LAD': 'LAD',
    'ATH': 'ATH', 'OAK': 'ATH',
}

def norm_abbr(a):
    return DK_TO_DB.get(a.upper(), a.upper())

def teamAbbr_from_full(full_name):
    """Map full team names to abbreviations — mirrors load_schedule.py logic"""
    MAP = {
        'Arizona Diamondbacks': 'ARI', 'Atlanta Braves': 'ATL',
        'Baltimore Orioles': 'BAL', 'Boston Red Sox': 'BOS',
        'Chicago Cubs': 'CHC', 'Chicago White Sox': 'CWS',
        'Cincinnati Reds': 'CIN', 'Cleveland Guardians': 'CLE',
        'Colorado Rockies': 'COL', 'Detroit Tigers': 'DET',
        'Houston Astros': 'HOU', 'Kansas City Royals': 'KC',
        'Los Angeles Angels': 'LAA', 'Los Angeles Dodgers': 'LAD',
        'Miami Marlins': 'MIA', 'Milwaukee Brewers': 'MIL',
        'Minnesota Twins': 'MIN', 'New York Mets': 'NYM',
        'New York Yankees': 'NYY', 'Oakland Athletics': 'ATH',
        'Athletics': 'ATH', 'Philadelphia Phillies': 'PHI',
        'Pittsburgh Pirates': 'PIT', 'San Diego Padres': 'SD',
        'San Francisco Giants': 'SF', 'Seattle Mariners': 'SEA',
        'St. Louis Cardinals': 'STL', 'Tampa Bay Rays': 'TB',
        'Texas Rangers': 'TEX', 'Toronto Blue Jays': 'TOR',
        'Washington Nationals': 'WSH',
    }
    return MAP.get(full_name, full_name[:3].upper())

print("\nLoading games from Supabase...")
games_res = supabase.table('games').select(
    'game_pk, home_team, away_team, game_time_utc, game_date'
).execute()

db_games = games_res.data or []
print(f"  Loaded {len(db_games)} games from DB")

# Build lookup: (away_abbr, home_abbr) -> game_pk
db_lookup = {}
for g in db_games:
    away = norm_abbr(teamAbbr_from_full(g['away_team']))
    home = norm_abbr(teamAbbr_from_full(g['home_team']))
    db_lookup[(away, home)] = g['game_pk']

# ── STEP 5: Update games table with dk_slate
matched   = 0
unmatched = 0
updates   = []

for comp_id, dk_game in slate_games.items():
    away = norm_abbr(dk_game['away'])
    home = norm_abbr(dk_game['home'])
    game_pk = db_lookup.get((away, home))

    if game_pk:
        updates.append({'game_pk': game_pk, 'dk_slate': dk_game['dk_slate']})
        matched += 1
    else:
        print(f"  UNMATCHED: {dk_game['away']} @ {dk_game['home']} ({dk_game['dk_slate']})")
        unmatched += 1

print(f"\nMatched: {matched} | Unmatched: {unmatched}")

if updates:
    print(f"Updating {len(updates)} games with dk_slate...")
    for u in updates:
        supabase.table('games').update(
            {'dk_slate': u['dk_slate']}
        ).eq('game_pk', u['game_pk']).execute()
    print("Done.")

print("\nload_dk_slates.py complete.")
