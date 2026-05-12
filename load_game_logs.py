"""
load_game_logs.py — Per-game batter stats aggregated from Savant pitch-level data
Uses pybaseball bulk statcast pull (one request per day) instead of one request per player.
Much faster: ~1-2 min for 14 days vs 5-20 min per-player approach.

Usage:
  py -3.12 load_game_logs.py              # last 14 days (default)
  py -3.12 load_game_logs.py --days 7     # last N days
  py -3.12 load_game_logs.py --days 30    # longer lookback

SQL migration (run once in Supabase):
  CREATE TABLE IF NOT EXISTS batter_game_logs (
    player_id         INTEGER NOT NULL,
    game_date         DATE    NOT NULL,
    season            INTEGER,
    pa                INTEGER,
    ab                INTEGER,
    hits              INTEGER,
    singles           INTEGER,
    doubles           INTEGER,
    triples           INTEGER,
    hr                INTEGER,
    sb                INTEGER,
    k                 INTEGER,
    bb                INTEGER,
    avg_ev            FLOAT,
    xwoba             FLOAT,
    barrel_cnt        INTEGER,
    barrel_pct        FLOAT,
    woba              FLOAT,
    opponent_team     VARCHAR(10),
    opposing_sp_name  VARCHAR(100),
    PRIMARY KEY (player_id, game_date)
  );
"""

import sys
sys.stdout.reconfigure(encoding='utf-8', line_buffering=True)

import os, math, time, requests
import pandas as pd
from datetime import datetime, date, timedelta
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

from config import SEASON

# Parse --days N from args (default 14)
DAYS = 14
for i, arg in enumerate(sys.argv[1:]):
    if arg == '--days' and i + 1 < len(sys.argv) - 1:
        try:
            DAYS = int(sys.argv[i + 2])
        except ValueError:
            pass

# ── PA-ending event sets ────────────────────────────────────────────────────────

HIT_EVENTS     = {'single', 'double', 'triple', 'home_run'}
NON_AB_EVENTS  = {'walk', 'intent_walk', 'hit_by_pitch', 'sac_fly',
                  'sac_bunt', 'sac_fly_double_play', 'catcher_interf', 'batter_interference'}
K_EVENTS       = {'strikeout', 'strikeout_double_play'}
BB_EVENTS      = {'walk', 'intent_walk'}
SB_EVENTS      = {'stolen_base_2b', 'stolen_base_3b', 'stolen_base_home'}

# wOBA weights (2024 season)
WOBA_W = {'bb': 0.690, 'single': 0.888, 'double': 1.267, 'triple': 1.603, 'home_run': 2.072}

def clean(v):
    if v is None:
        return None
    try:
        f = float(v)
        return None if (math.isnan(f) or math.isinf(f)) else f
    except (TypeError, ValueError):
        return None

# ── Pull bulk statcast data ────────────────────────────────────────────────────

end_dt   = date.today()
start_dt = end_dt - timedelta(days=DAYS)

print(f"\nload_game_logs — bulk statcast pull")
print(f"  Window : {start_dt} → {end_dt} ({DAYS} days)")
print(f"  Source : pybaseball statcast (pitch-level, all players)\n")

import pybaseball as pb
pb.cache.enable()

print("Fetching pitch data from Savant...")
t0 = time.time()
try:
    df = pb.statcast(
        start_dt=start_dt.strftime('%Y-%m-%d'),
        end_dt=end_dt.strftime('%Y-%m-%d'),
    )
except Exception as e:
    print(f"ERROR fetching statcast data: {e}")
    sys.exit(1)

print(f"  {len(df):,} pitches fetched in {time.time()-t0:.1f}s")

if df.empty:
    print("No data returned — exiting.")
    sys.exit(0)

# ── Filter to plate-ending events (one row per PA) ────────────────────────────

pa_df = df[df['events'].notna()].copy()
print(f"  {len(pa_df):,} plate appearances (includes baserunning events)")

# Ensure types
pa_df['batter']    = pd.to_numeric(pa_df['batter'],    errors='coerce')
pa_df['game_date'] = pa_df['game_date'].astype(str).str[:10]
pa_df = pa_df[pa_df['batter'].notna()]

# ── Contact-level data (for EV, xwOBA, barrels) ───────────────────────────────

contact_df = df[df['launch_speed'].notna()].copy()
contact_df['batter']    = pd.to_numeric(contact_df['batter'], errors='coerce')
contact_df['game_date'] = contact_df['game_date'].astype(str).str[:10]
contact_df = contact_df[contact_df['batter'].notna()]

# Pre-group contact by (batter, game_date) for fast lookup
contact_grp = contact_df.groupby(['batter', 'game_date'])

# ── Build stolen-base lookup: (player_id, game_date) → sb count ───────────────
# Uses on_1b/2b/3b columns to identify the actual runner, not the batter at plate.

print("Building SB lookup...")
sb_df = df[df['events'].notna() & df['events'].str.lower().isin(SB_EVENTS)].copy()
sb_lookup = {}
if not sb_df.empty:
    sb_df['game_date_str'] = sb_df['game_date'].astype(str).str[:10]
    sb_df['event_lower']   = sb_df['events'].str.lower()

    runner_ids = []
    for _, row in sb_df.iterrows():
        ev = row['event_lower']
        if ev == 'stolen_base_2b':
            runner_ids.append(row.get('on_1b'))
        elif ev == 'stolen_base_3b':
            runner_ids.append(row.get('on_2b'))
        else:  # stolen_base_home
            runner_ids.append(row.get('on_3b'))

    sb_df['runner_id'] = runner_ids
    sb_df = sb_df[sb_df['runner_id'].notna()]
    sb_df['runner_id'] = pd.to_numeric(sb_df['runner_id'], errors='coerce')
    sb_df = sb_df[sb_df['runner_id'].notna()]
    sb_df['runner_id'] = sb_df['runner_id'].astype(int)

    for _, row in sb_df.iterrows():
        key = (int(row['runner_id']), row['game_date_str'])
        sb_lookup[key] = sb_lookup.get(key, 0) + 1

    print(f"  {len(sb_df)} SB events → {len(sb_lookup)} player-game entries")
else:
    print("  No SB events in window")

# ── Build opposing-SP lookup ──────────────────────────────────────────────────
# SP = most common pitcher in inning 1 of each game half.
# 'Top' half = away team batting, home team pitching → home team's SP
# 'Bot' half = home team batting, away team pitching → away team's SP

print("Building SP lookup...")
in1_cols = ['game_date', 'home_team', 'away_team', 'inning_topbot', 'pitcher']
in1_df = df[(df['inning'] == 1) & df['pitcher'].notna()][in1_cols].copy()
in1_df['game_date']     = in1_df['game_date'].astype(str).str[:10]
in1_df['inning_topbot'] = in1_df['inning_topbot'].astype(str)
in1_df['pitcher']       = pd.to_numeric(in1_df['pitcher'], errors='coerce')
in1_df = in1_df[in1_df['pitcher'].notna()]

# (gdate, home, away, topbot) → sp_id
sp_by_half = {}
for (gdate, home, away, topbot), grp in in1_df.groupby(['game_date', 'home_team', 'away_team', 'inning_topbot']):
    mode = grp['pitcher'].mode()
    if len(mode) > 0:
        sp_by_half[(str(gdate), str(home), str(away), str(topbot))] = int(mode.iloc[0])

# Batch-resolve SP MLBAM IDs → display names (e.g. "G. Cole")
all_sp_ids = list({v for v in sp_by_half.values()})
sp_name_map = {}
if all_sp_ids:
    print(f"  Resolving {len(all_sp_ids)} SP IDs to names...")
    try:
        rev = pb.playerid_reverse_lookup(all_sp_ids, key_type='mlbam')
        for _, row in rev.iterrows():
            mid   = int(row['key_mlbam'])
            last  = str(row['name_last']).title()
            first = str(row['name_first']).title()
            sp_name_map[mid] = f"{first[0]}. {last}" if first else last
    except Exception as e:
        print(f"  WARN: SP name lookup failed: {e}")

print(f"  {len(sp_by_half)} game halves indexed, {len(sp_name_map)} SP names resolved")

# ── Aggregate per (batter, game_date) ─────────────────────────────────────────

print("\nAggregating game logs...")
t1 = time.time()

all_rows = []

for (pid, gdate), g in pa_df.groupby(['batter', 'game_date']):
    pid = int(pid)

    # Filter to real plate appearances (exclude stolen base rows etc.)
    pas = g[~g['events'].str.lower().isin(SB_EVENTS |
            {'caught_stealing_2b', 'caught_stealing_3b', 'caught_stealing_home',
             'pickoff_1b', 'pickoff_2b', 'pickoff_3b'})]

    pa    = len(pas)
    ab    = len(pas[~pas['events'].str.lower().isin(NON_AB_EVENTS)])
    hits  = len(pas[pas['events'].str.lower().isin(HIT_EVENTS)])
    hr    = len(pas[pas['events'].str.lower() == 'home_run'])
    k     = len(pas[pas['events'].str.lower().isin(K_EVENTS)])
    bb    = len(pas[pas['events'].str.lower().isin(BB_EVENTS)])

    # Contact stats from pitch-level (all pitches, not just PA-ending)
    try:
        cg = contact_grp.get_group((pid, gdate))
    except KeyError:
        cg = pd.DataFrame()

    avg_ev     = clean(cg['launch_speed'].mean())                       if not cg.empty else None
    xwoba_vals = cg['estimated_woba_using_speedangle'].dropna() if not cg.empty and 'estimated_woba_using_speedangle' in cg.columns else pd.Series(dtype=float)
    xwoba      = clean(xwoba_vals.mean())                               if len(xwoba_vals) else None
    barrel_cnt = int(cg['barrel'].sum())                                if not cg.empty and 'barrel' in cg.columns else None
    bip        = len(cg)
    barrel_pct = clean(barrel_cnt / bip)                                if barrel_cnt and bip > 0 else None

    singles = hits - (len(pas[pas['events'].str.lower() == 'double']) +
                      len(pas[pas['events'].str.lower() == 'triple']) + hr)
    doubles = len(pas[pas['events'].str.lower() == 'double'])
    triples = len(pas[pas['events'].str.lower() == 'triple'])

    denom = ab + bb
    if denom > 0 and (singles + doubles + triples + hr + bb) > 0:
        woba = round(
            (WOBA_W['bb'] * bb + WOBA_W['single'] * singles +
             WOBA_W['double'] * doubles + WOBA_W['triple'] * triples +
             WOBA_W['home_run'] * hr) / denom, 3
        )
    else:
        woba = xwoba

    # Stolen bases (attributed to correct runner via sb_lookup)
    sb = sb_lookup.get((pid, gdate), 0)

    # Opponent team and opposing SP
    first_row = g.iloc[0]
    home_team = str(first_row.get('home_team', '') or '')
    away_team = str(first_row.get('away_team', '') or '')
    topbot    = str(first_row.get('inning_topbot', '') or '')

    if topbot == 'Top':
        opponent_team = home_team
    elif topbot == 'Bot':
        opponent_team = away_team
    else:
        opponent_team = None

    # The opposing SP pitched in the batter's own half-inning of inning 1
    sp_id            = sp_by_half.get((gdate, home_team, away_team, topbot))
    opposing_sp_name = sp_name_map.get(sp_id) if sp_id else None

    all_rows.append({
        'player_id':        pid,
        'game_date':        gdate,
        'season':           SEASON,
        'pa':               pa,
        'ab':               ab,
        'hits':             hits,
        'singles':          singles,
        'doubles':          doubles,
        'triples':          triples,
        'hr':               hr,
        'sb':               sb,
        'k':                k,
        'bb':               bb,
        'r':                0,   # filled below from boxscore
        'rbi':              0,
        'hbp':              0,
        'avg_ev':           avg_ev,
        'xwoba':            xwoba,
        'barrel_cnt':       barrel_cnt,
        'barrel_pct':       barrel_pct,
        'woba':             woba,
        'opponent_team':    opponent_team,
        'opposing_sp_name': opposing_sp_name,
    })

print(f"  {len(all_rows):,} player-game rows in {time.time()-t1:.1f}s")
unique_players = len(set(r['player_id'] for r in all_rows))
print(f"  {unique_players:,} unique players")

# ── Enrich with R / RBI / HBP from MLB Stats API boxscores ────────────────────
# Statcast is pitch-level only — runs, RBI, HBP require box score data.

MLB_API = "https://statsapi.mlb.com/api/v1"

print("\nFetching boxscores for R / RBI / HBP...")
game_rows = supabase.table('games').select('game_pk,game_date') \
    .gte('game_date', str(start_dt)).lte('game_date', str(end_dt)) \
    .execute().data or []

# Build (player_id, game_date) → {r, rbi, hbp} lookup
box_lookup = {}
for gr in game_rows:
    gpk   = gr['game_pk']
    gdate = gr['game_date']
    try:
        resp = requests.get(f"{MLB_API}/game/{gpk}/boxscore", timeout=15)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"  WARN boxscore {gpk}: {e}")
        continue

    for side in ('home', 'away'):
        team_data = data.get('teams', {}).get(side, {})
        for pid_int in team_data.get('batters', []):
            pinfo   = team_data.get('players', {}).get(f'ID{pid_int}', {})
            batting = pinfo.get('stats', {}).get('batting', {})
            if not batting or batting.get('plateAppearances', 0) == 0:
                continue
            box_lookup[(int(pid_int), gdate)] = {
                'r':   batting.get('runs',        0),
                'rbi': batting.get('rbi',         0),
                'hbp': batting.get('hitByPitch',  0),
            }

# Merge into all_rows
matched = 0
for row in all_rows:
    key = (row['player_id'], row['game_date'])
    box = box_lookup.get(key)
    if box:
        row['r']   = box['r']
        row['rbi'] = box['rbi']
        row['hbp'] = box['hbp']
        matched += 1

print(f"  {len(game_rows)} games fetched, {matched}/{len(all_rows)} player-game rows enriched")

# ── Upsert ─────────────────────────────────────────────────────────────────────

BATCH = 500
print(f"\nUploading to batter_game_logs in batches of {BATCH}...")
success = 0
for i in range(0, len(all_rows), BATCH):
    batch = all_rows[i:i + BATCH]
    try:
        supabase.table('batter_game_logs').upsert(
            batch, on_conflict='player_id,game_date'
        ).execute()
        success += len(batch)
        print(f"  {min(i + BATCH, len(all_rows)):,} / {len(all_rows):,}")
    except Exception as e:
        print(f"  ERROR on batch {i // BATCH + 1}: {e}")

print(f"\nDone. {success:,} rows uploaded ({unique_players:,} players, {DAYS} days).")
