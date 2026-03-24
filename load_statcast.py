import sys
sys.stdout.reconfigure(encoding='utf-8', line_buffering=True)
print("Script started", flush=True)

import pandas as pd
import math
import unicodedata
from supabase import create_client
from dotenv import load_dotenv
import os
import pybaseball

pybaseball.cache.enable()

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

from config import SEASON

# ── Helpers
def clean(val):
    if val is None:
        return None
    if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
        return None
    return val

def clean_row(row):
    return {k: clean(v) for k, v in row.items()}

def normalize(name):
    if not name or not isinstance(name, str):
        return ''
    nfkd = unicodedata.normalize('NFKD', name)
    return nfkd.encode('ASCII', 'ignore').decode('ASCII').strip().lower()

def upload(table, rows, batch_size=500):
    print(f"  Uploading {len(rows):,} rows to {table}...")
    success = 0
    errors  = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        try:
            supabase.table(table).upsert(batch).execute()
            success += len(batch)
            print(f"    {min(i + batch_size, len(rows)):,} / {len(rows):,}")
        except Exception as e:
            errors += len(batch)
            print(f"    ERROR on batch {i//batch_size + 1}: {e}")
            break
    print(f"  Done. {success:,} uploaded, {errors} errors.")
    return errors == 0

# ══════════════════════════════════════════════
# STEP 1 — Load player ID map from Supabase
# ══════════════════════════════════════════════
print("\nLoading player ID map from database...")

all_players = []
offset = 0
while True:
    result = supabase.table('players').select(
        'mlbam_id, fangraphs_id, name_normalized'
    ).range(offset, offset + 999).execute()
    if not result.data:
        break
    all_players.extend(result.data)
    if len(result.data) < 1000:
        break
    offset += 1000

print(f"  Loaded {len(all_players):,} players")

mlbam_to_fg   = {}
fg_to_mlbam   = {}
name_to_mlbam = {}

for p in all_players:
    if p['fangraphs_id']:
        fg_to_mlbam[str(p['fangraphs_id'])] = p['mlbam_id']
        mlbam_to_fg[str(p['mlbam_id'])] = str(p['fangraphs_id'])
    if p['name_normalized']:
        name_to_mlbam[p['name_normalized']] = p['mlbam_id']

def get_mlbam_from_mlbam(mlbam_id):
    # Already an MLBAM ID — just verify it exists in our DB
    if str(mlbam_id) in mlbam_to_fg or any(
        str(p['mlbam_id']) == str(mlbam_id) for p in all_players[:100]
    ):
        return int(mlbam_id)
    return int(mlbam_id)  # Use it anyway — Savant uses MLBAM IDs natively

def get_mlbam_from_name(name):
    normalized = normalize(str(name)) if name else ''
    return name_to_mlbam.get(normalized)

# ══════════════════════════════════════════════
# STEP 2 — Statcast batting leaderboard
# (exit velo, barrel%, hard hit%, xwOBA)
# ══════════════════════════════════════════════
print("\nFetching Statcast batting leaderboard...")
print("  (this may take 30-60 seconds...)")

stat_bat = pd.DataFrame()
try:
    stat_bat = pybaseball.statcast_batter_exitvelo_barrels(SEASON, minBBE=50)
    print(f"  Got {len(stat_bat):,} batters")
    print(f"  Columns: {list(stat_bat.columns)}")
except Exception as e:
    print(f"  ERROR: {e}")

batter_updates = []
skipped = 0

if not stat_bat.empty:
    for _, row in stat_bat.iterrows():
        mlbam_id = row.get('player_id')
        if not mlbam_id:
            skipped += 1
            continue

        batter_updates.append(clean_row({
            'player_id':    int(mlbam_id),
            'season':       SEASON,
            'barrel_pct':   row.get('brl_percent'),
            'hard_hit_pct': row.get('ev95percent'),
            'avg_ev':       row.get('avg_hit_speed'),
        }))

    print(f"  Matched: {len(batter_updates):,} | Skipped: {skipped}")

    # Upsert — only updates the Statcast columns, leaves FanGraphs columns alone
    upload('batter_stats', batter_updates)

# ══════════════════════════════════════════════
# STEP 3 — Sprint speed leaderboard
# ══════════════════════════════════════════════
print("\nFetching sprint speed leaderboard...")
print("  (this may take 20-30 seconds...)")

sprint_df = pd.DataFrame()
try:
    sprint_df = pybaseball.statcast_sprint_speed(SEASON, min_opp=10)
    print(f"  Got {len(sprint_df):,} players")
    print(f"  Columns: {list(sprint_df.columns)}")
except Exception as e:
    print(f"  ERROR: {e}")

sprint_updates = []
skipped = 0

if not sprint_df.empty:
    for _, row in sprint_df.iterrows():
        # Sprint speed uses player_id (MLBAM)
        mlbam_id = row.get('player_id')
        if not mlbam_id:
            # Fall back to name
            mlbam_id = get_mlbam_from_name(row.get('last_name, first_name', ''))
        if not mlbam_id:
            skipped += 1
            continue

        sprint_updates.append(clean_row({
            'player_id':   int(mlbam_id),
            'season':      SEASON,
            'sprint_speed': row.get('hp_to_1b') or row.get('sprint_speed'),
        }))

    print(f"  Matched: {len(sprint_updates):,} | Skipped: {skipped}")
    upload('batter_stats', sprint_updates)

# ══════════════════════════════════════════════
# STEP 4 — Bat tracking (bat speed, attack angle)
# ══════════════════════════════════════════════
print("\nFetching bat tracking leaderboard...")
print("  (this may take 20-30 seconds...)")

bat_track = pd.DataFrame()
try:
    url = (
        "https://baseballsavant.mlb.com/leaderboard/bat-tracking"
        "?attackZone=&batSide=&contactType=&count=&dating=&defense=&dimension=&"
        "gameType=&isHardHit=&isSwung=&isTracked=&minSwings=50&minGroupSwings=1"
        f"&pitchHand=&pitchType=&season={SEASON}&seasonType=Regular+Season"
        "&team=&tilt=&type=batter&sportId=1&csv=true"
    )
    import requests
    response = requests.get(url, headers={
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }, timeout=30)
    from io import StringIO
    bat_track = pd.read_csv(StringIO(response.text))
    print(f"  Got {len(bat_track):,} players")
    print(f"  Columns: {list(bat_track.columns)[:15]}")
except Exception as e:
    print(f"  ERROR: {e}")

bat_track_updates = []
skipped = 0

if not bat_track.empty:
    for _, row in bat_track.iterrows():
        # This endpoint uses 'id' for MLBAM ID and 'name' for player name
        mlbam_id = row.get('id')
        if not mlbam_id:
            mlbam_id = get_mlbam_from_name(row.get('name', ''))
        if not mlbam_id:
            skipped += 1
            continue

        bat_track_updates.append(clean_row({
            'player_id':      int(mlbam_id),
            'season':         SEASON,
            'bat_speed':      row.get('avg_bat_speed'),
            'attack_angle':   None,  # not in this endpoint
            'squared_up_pct': row.get('squared_up_per_swing'),
            'blast_pct':      row.get('blast_per_swing'),
        }))

    print(f"  Matched: {len(bat_track_updates):,} | Skipped: {skipped}")
    upload('batter_stats', bat_track_updates)

# ══════════════════════════════════════════════
# STEP 5 — Pitcher arm angle + velo
# ══════════════════════════════════════════════
print("\nFetching pitcher arm angle leaderboard...")
print("  (this may take 20-30 seconds...)")

arm_df = pd.DataFrame()
try:
    arm_df = pybaseball.statcast_pitcher_exitvelo_barrels(SEASON, minBBE=25)
    print(f"  Got {len(arm_df):,} pitchers")
    print(f"  Columns: {list(arm_df.columns)}")
except Exception as e:
    print(f"  ERROR: {e}")

pitcher_updates = []
skipped = 0

if not arm_df.empty:
    for _, row in arm_df.iterrows():
        mlbam_id = row.get('player_id')
        if not mlbam_id:
            skipped += 1
            continue

        pitcher_updates.append(clean_row({
            'player_id':    int(mlbam_id),
            'season':       SEASON,
            'barrel_pct':   row.get('brl_percent'),
            'hard_hit_pct': row.get('ev95percent'),
        }))

    print(f"  Matched: {len(pitcher_updates):,} | Skipped: {skipped}")
    upload('pitcher_stats', pitcher_updates)

print("\nPhase 3 complete.")
print(f"  Batter exit velo/barrel updates: {len(batter_updates):,}")
print(f"  Sprint speed updates:            {len(sprint_updates):,}")
print(f"  Bat tracking updates:            {len(bat_track_updates):,}")
print(f"  Pitcher updates:                 {len(pitcher_updates):,}")