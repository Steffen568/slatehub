import sys
sys.stdout.reconfigure(encoding='utf-8', line_buffering=True)
print("Script started", flush=True)

import requests
import math
from supabase import create_client
from dotenv import load_dotenv
import os
import datetime

# ── Load credentials
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    print("ERROR: Missing credentials. Check your .env file.")
    exit()

print("Connecting to Supabase...")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
print("Connected.")

MLB_API = "https://statsapi.mlb.com/api/v1"

def clean(val):
    if val is None:
        return None
    if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
        return None
    return val

# ══════════════════════════════════════════════
# STEP 1 — Build date range (last 7 days)
# ══════════════════════════════════════════════
today     = datetime.date.today()
week_ago  = today - datetime.timedelta(days=7)
date_from = week_ago.isoformat()
date_to   = today.isoformat()

print(f"\nFetching games from {date_from} to {date_to}...")

# ══════════════════════════════════════════════
# STEP 2 — Get all games in the date range
# ══════════════════════════════════════════════
url = (
    f"{MLB_API}/schedule"
    f"?sportId=1&startDate={date_from}&endDate={date_to}"
    f"&gameType=R,S,F,D,L,W,C&hydrate=team"
)

try:
    response = requests.get(url, timeout=30)
    data = response.json()
except Exception as e:
    print(f"ERROR fetching schedule: {e}")
    exit()

# Collect all game PKs from the last 7 days
game_pks = []
for date_entry in data.get('dates', []):
    for game in date_entry.get('games', []):
        status = game.get('status', {}).get('abstractGameState', '')
        # Only process completed games
        if status == 'Final':
            game_pks.append({
                'game_pk':   game.get('gamePk'),
                'game_date': date_entry.get('date'),
            })

print(f"Found {len(game_pks)} completed games in the last 7 days")

# ══════════════════════════════════════════════
# STEP 3 — For each game fetch pitcher usage
# ══════════════════════════════════════════════
all_appearances = []
errors = 0

for i, game_info in enumerate(game_pks):
    game_pk   = game_info['game_pk']
    game_date = game_info['game_date']

    print(f"  Processing game {i+1}/{len(game_pks)} (pk: {game_pk})...", end=" ")

    try:
        box_url  = f"{MLB_API}/game/{game_pk}/boxscore"
        response = requests.get(box_url, timeout=30)
        boxscore = response.json()
    except Exception as e:
        print(f"ERROR: {e}")
        errors += 1
        continue

    # Process both home and away pitchers
    for side in ['home', 'away']:
        team_data = boxscore.get('teams', {}).get(side, {})
        team_info = team_data.get('team', {})
        team_id   = team_info.get('id')
        team_name = team_info.get('name', '')
        pitchers  = team_data.get('pitchers', [])
        players   = team_data.get('players', {})

        for j, pitcher_id in enumerate(pitchers):
            player_key  = f'ID{pitcher_id}'
            player_data = players.get(player_key, {})
            person      = player_data.get('person', {})
            stats       = player_data.get('stats', {})
            pitching    = stats.get('pitching', {})

            player_name = person.get('fullName', '')
            pitches     = pitching.get('numberOfPitches', 0)
            innings_str = pitching.get('inningsPitched', '0.0')
            runs        = pitching.get('runs', 0)
            earned_runs = pitching.get('earnedRuns', 0)
            strikes     = pitching.get('strikes', 0)
            note        = player_data.get('gameStatus', {}).get('note', '')

            # Convert innings string like "2.1" to float 2.333
            try:
                parts   = str(innings_str).split('.')
                full    = int(parts[0])
                partial = int(parts[1]) if len(parts) > 1 else 0
                innings = round(full + partial / 3, 3)
            except:
                innings = 0.0

            # First pitcher listed is the starter
            is_starter = (j == 0)

            if pitches and pitches > 0:
                all_appearances.append({
                    'player_id':   pitcher_id,
                    'player_name': player_name,
                    'team_id':     team_id,
                    'team_name':   team_name,
                    'game_pk':     game_pk,
                    'game_date':   game_date,
                    'pitches':     pitches,
                    'innings':     innings,
                    'runs':        clean(runs),
                    'earned_runs': clean(earned_runs),
                    'strikes':     clean(strikes),
                    'result':      note,
                    'is_starter':  is_starter,
                })

    print("OK")

print(f"\nProcessed {len(all_appearances)} pitcher appearances ({errors} errors)")

# ══════════════════════════════════════════════
# STEP 4 — Upload to Supabase in batches
# ══════════════════════════════════════════════
if all_appearances:
    print(f"Uploading {len(all_appearances)} appearances...")
    BATCH_SIZE = 500
    success = 0
    upload_errors = 0

    for i in range(0, len(all_appearances), BATCH_SIZE):
        batch = all_appearances[i:i + BATCH_SIZE]
        try:
            supabase.table('bullpen_appearances').upsert(batch, on_conflict='player_id,game_pk', ignore_duplicates=True).execute()
            success += len(batch)
            print(f"  {min(i + BATCH_SIZE, len(all_appearances)):,} / {len(all_appearances):,}")
        except Exception as e:
            upload_errors += len(batch)
            print(f"  ERROR on batch {i//BATCH_SIZE + 1}: {e}")
            break

    print(f"  Done. {success:,} uploaded, {upload_errors} errors.")
else:
    print("No appearances to upload.")

# ══════════════════════════════════════════════
# STEP 5 — Quick summary by team
# ══════════════════════════════════════════════
print("\nSample — top bullpen workload last 7 days:")
try:
    result = supabase.rpc('dummy', {}).execute()
except:
    pass

# Simple local summary
from collections import defaultdict
reliever_pitches = defaultdict(int)
for a in all_appearances:
    if not a['is_starter']:
        reliever_pitches[a['player_name']] += a['pitches']

top_10 = sorted(reliever_pitches.items(), key=lambda x: x[1], reverse=True)[:10]
for name, pitches in top_10:
    print(f"  {name}: {pitches} pitches")

print(f"\nPhase 5 complete.")
print(f"  Games processed:      {len(game_pks)}")
print(f"  Appearances uploaded: {len(all_appearances)}")