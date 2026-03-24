import sys
sys.stdout.reconfigure(encoding='utf-8', line_buffering=True)
print("Script started", flush=True)

import pandas as pd
from supabase import create_client
from dotenv import load_dotenv
import os
import math
import unicodedata
import pybaseball

# Disable pybaseball's progress bars for cleaner output
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

from config import SEASON as _DEFAULT_SEASON
SEASON = int(sys.argv[1]) if len(sys.argv) > 1 else _DEFAULT_SEASON

# ── Helper: clean a single value (removes nan/inf)
def clean(val):
    if val is None:
        return None
    if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
        return None
    return val

# ── Helper: clean an entire row dict
def clean_row(row):
    return {k: clean(v) for k, v in row.items()}

# ── Helper: normalize a name for matching
def normalize(name):
    if not name or not isinstance(name, str):
        return ''
    nfkd = unicodedata.normalize('NFKD', name)
    return nfkd.encode('ASCII', 'ignore').decode('ASCII').strip().lower()

# ── Helper: upload rows to a table in batches
def upload(table, rows, batch_size=500):
    print(f"  Uploading {len(rows):,} rows to {table}...")
    success = 0
    errors  = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        try:
            supabase.table(table).upsert(batch, on_conflict='player_id,season').execute()
            success += len(batch)
            print(f"    {min(i + batch_size, len(rows)):,} / {len(rows):,}")
        except Exception as e:
            errors += len(batch)
            print(f"    ERROR on batch {i//batch_size + 1}: {e}")
            break
    print(f"  Done. {success:,} uploaded, {errors} errors.")
    return errors == 0

# ══════════════════════════════════════════════
# STEP 1 — Load full player ID map from Supabase
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

print(f"  Loaded {len(all_players):,} total players from database")

# Build two lookup dicts
fg_to_mlbam   = {}
name_to_mlbam = {}

for p in all_players:
    if p['fangraphs_id']:
        fg_to_mlbam[str(p['fangraphs_id'])] = p['mlbam_id']
    if p['name_normalized']:
        name_to_mlbam[p['name_normalized']] = p['mlbam_id']

print(f"  FanGraphs ID mappings: {len(fg_to_mlbam):,}")
print(f"  Name mappings:         {len(name_to_mlbam):,}")

# ── Helper: find MLBAM ID from a FanGraphs row
def get_mlbam_id(fg_id, name):
    try:
        fg_str = str(int(float(fg_id)))
        if fg_str in fg_to_mlbam:
            return fg_to_mlbam[fg_str]
    except (ValueError, TypeError):
        pass
    normalized = normalize(str(name)) if name else ''
    if normalized in name_to_mlbam:
        return name_to_mlbam[normalized]
    return None

# ══════════════════════════════════════════════
# STEP 2 — Fetch batting stats via pybaseball
# ══════════════════════════════════════════════
print("\nFetching batting leaderboard via pybaseball...")
print("  (this may take 20-30 seconds...)")

bat_df = pd.DataFrame()
try:
    bat_df = pybaseball.batting_stats(SEASON, SEASON, qual=1)
    print(f"  Got {len(bat_df):,} batters")
    print(f"  Columns: {list(bat_df.columns)[:15]}")
except Exception as e:
    print(f"  ERROR: {e}")

batter_rows = []
skipped = 0

if not bat_df.empty:
    for _, row in bat_df.iterrows():
        mlbam_id = get_mlbam_id(row.get('IDfg'), row.get('Name'))
        if not mlbam_id:
            skipped += 1
            continue

        # Skip ghost rows with no real stats
        if row.get('PA') is None and row.get('wRC+') is None:
            skipped += 1
            continue

        batter_rows.append(clean_row({
            'player_id':    mlbam_id,
            'season':       SEASON,
            'full_name':    row.get('Name'),
            'team':         row.get('Team'),
            'pa':           row.get('PA'),
            'avg':          row.get('AVG'),
            'obp':          row.get('OBP'),
            'slg':          row.get('SLG'),
            'ops':          row.get('OPS'),
            'wrc_plus':     row.get('wRC+'),
            'woba':         row.get('wOBA'),
            'xwoba':        row.get('xwOBA'),
            'k_pct':        row.get('K%'),
            'bb_pct':       row.get('BB%'),
            'iso':          row.get('ISO'),
            'babip':        row.get('BABIP'),
            'hr':           row.get('HR'),
            'r':            row.get('R'),
            'rbi':          row.get('RBI'),
            'sb':           row.get('SB'),
            'barrel_pct':   row.get('Barrel%'),
            'hard_hit_pct': row.get('HardHit%'),
            'avg_ev':       row.get('EV') or row.get('AvgEV'),
            'swstr_pct':    row.get('SwStr%'),
            'o_swing_pct':  row.get('O-Swing%'),
            'pull_pct':     row.get('Pull%'),
            'cent_pct':     row.get('Cent%'),
            'oppo_pct':     row.get('Oppo%'),
            'fb_pct':       row.get('FB%'),
            'gb_pct':       row.get('GB%'),
            'ld_pct':       row.get('LD%'),
        }))

    print(f"  Matched: {len(batter_rows):,} | Skipped: {skipped}")
    upload('batter_stats', batter_rows)

# ══════════════════════════════════════════════
# STEP 3 — Fetch pitching stats via pybaseball
# ══════════════════════════════════════════════
print("\nFetching pitching leaderboard via pybaseball...")
print("  (this may take 20-30 seconds...)")

pit_df = pd.DataFrame()
try:
    pit_df = pybaseball.pitching_stats(SEASON, SEASON, qual=1)
    print(f"  Got {len(pit_df):,} pitchers")
except Exception as e:
    print(f"  ERROR: {e}")

pitcher_rows = []
skipped = 0

if not pit_df.empty:
    for _, row in pit_df.iterrows():
        mlbam_id = get_mlbam_id(row.get('IDfg'), row.get('Name'))
        if not mlbam_id:
            skipped += 1
            continue

        # Skip ghost rows with no real stats
        if row.get('IP') is None and row.get('ERA') is None:
            skipped += 1
            continue

        pitcher_rows.append(clean_row({
            'player_id':     mlbam_id,
            'season':        SEASON,
            'full_name':     row.get('Name'),
            'team':          row.get('Team'),
            'g':             row.get('G'),
            'gs':            row.get('GS'),
            'ip':            row.get('IP'),
            'era':           row.get('ERA'),
            'xfip':          row.get('xFIP'),
            'siera':         row.get('SIERA'),
            'fip':           row.get('FIP'),
            'k_pct':         row.get('K%'),
            'bb_pct':        row.get('BB%'),
            'k_bb_pct':      row.get('K-BB%'),
            'hr9':           row.get('HR/9'),
            'babip':         row.get('BABIP'),
            'lob_pct':       row.get('LOB%'),
            'gb_pct':        row.get('GB%'),
            'fb_pct':        row.get('FB%'),
            'ld_pct':        row.get('LD%'),
            'swstr_pct':     row.get('SwStr%'),
            'csw_pct':       row.get('CSW%'),
            'barrel_pct':    row.get('Barrel%'),
            'hard_hit_pct':  row.get('HardHit%'),
            'whip':          row.get('WHIP'),
            'k9':            row.get('K/9'),
            'bb9':           row.get('BB/9'),
            'avg':           row.get('AVG'),
            'w':             row.get('W'),
            'l':             row.get('L'),
            'stuff_plus':    row.get('Stuff+'),
            'location_plus': row.get('Location+'),
            'pitching_plus': row.get('Pitching+'),
        }))

    print(f"  Matched: {len(pitcher_rows):,} | Skipped: {skipped}")
    upload('pitcher_stats', pitcher_rows)

print("\nPhase 2 complete.")
print(f"  Batters loaded:  {len(batter_rows):,}")
print(f"  Pitchers loaded: {len(pitcher_rows):,}")