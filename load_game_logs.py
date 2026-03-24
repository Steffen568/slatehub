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
    player_id  INTEGER NOT NULL,
    game_date  DATE    NOT NULL,
    season     INTEGER,
    pa         INTEGER,
    ab         INTEGER,
    hits       INTEGER,
    hr         INTEGER,
    k          INTEGER,
    bb         INTEGER,
    avg_ev     FLOAT,
    xwoba      FLOAT,
    barrel_cnt INTEGER,
    barrel_pct FLOAT,
    woba       FLOAT,
    PRIMARY KEY (player_id, game_date)
  );
"""

import sys
sys.stdout.reconfigure(encoding='utf-8', line_buffering=True)

import os, math, time
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
print(f"  {len(pa_df):,} plate appearances")

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

# ── Aggregate per (batter, game_date) ─────────────────────────────────────────

print("\nAggregating game logs...")
t1 = time.time()

all_rows = []

for (pid, gdate), g in pa_df.groupby(['batter', 'game_date']):
    pid   = int(pid)
    evs   = set(g['events'].dropna().str.lower())

    pa    = len(g)
    ab    = len(g[~g['events'].str.lower().isin(NON_AB_EVENTS)])
    hits  = len(g[g['events'].str.lower().isin(HIT_EVENTS)])
    hr    = len(g[g['events'].str.lower() == 'home_run'])
    k     = len(g[g['events'].str.lower().isin(K_EVENTS)])
    bb    = len(g[g['events'].str.lower().isin(BB_EVENTS)])

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

    # Approximate wOBA from counting stats
    singles = hits - (len(g[g['events'].str.lower() == 'double']) +
                      len(g[g['events'].str.lower() == 'triple']) + hr)
    doubles = len(g[g['events'].str.lower() == 'double'])
    triples = len(g[g['events'].str.lower() == 'triple'])
    denom   = ab + bb
    if denom > 0 and (singles + doubles + triples + hr + bb) > 0:
        woba = round(
            (WOBA_W['bb'] * bb + WOBA_W['single'] * singles +
             WOBA_W['double'] * doubles + WOBA_W['triple'] * triples +
             WOBA_W['home_run'] * hr) / denom, 3
        )
    else:
        woba = xwoba  # fall back to xwoba if no counting data

    all_rows.append({
        'player_id':  pid,
        'game_date':  gdate,
        'season':     SEASON,
        'pa':         pa,
        'ab':         ab,
        'hits':       hits,
        'hr':         hr,
        'k':          k,
        'bb':         bb,
        'avg_ev':     avg_ev,
        'xwoba':      xwoba,
        'barrel_cnt': barrel_cnt,
        'barrel_pct': barrel_pct,
        'woba':       woba,
    })

print(f"  {len(all_rows):,} player-game rows in {time.time()-t1:.1f}s")
unique_players = len(set(r['player_id'] for r in all_rows))
print(f"  {unique_players:,} unique players")

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
