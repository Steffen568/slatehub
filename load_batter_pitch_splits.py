#!/usr/bin/env python3
"""
load_batter_pitch_splits.py — Batter performance vs specific pitch types

Fetches from Baseball Savant statcast_search, grouped by batter, filtered by pitch type.
Stores in batter_pitch_type_splits table: xwOBA, K%, whiff%, hard_hit% per pitch type.

Used by:
  - sim_projections.py: arsenal matchup quality multiplier (how well does opp lineup
    hit THIS pitcher's specific pitch mix)
  - generate_pool.py: PMS Arsenal Vulnerability component

Run frequency: weekly (rate stats stabilize over weeks, not days)
"""

import os, sys, requests, io, time
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv

sys.stdout.reconfigure(encoding='utf-8', errors='replace')
load_dotenv()

sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

from config import SEASON

PITCH_TYPES = ["FF", "SI", "FC", "SL", "ST", "CH", "CU", "FS"]
MIN_PITCHES = 20   # server-side minimum; smaller = more rows but noisier


def _safe(row, *keys):
    for k in keys:
        if k in row.index and pd.notna(row[k]):
            try:
                return float(row[k])
            except Exception:
                pass
    return None


def fetch_batter_vs_pitch(pitch_code):
    """
    Pull Savant statcast_search CSV for batters vs a specific pitch type.
    Returns DataFrame with one row per batter.
    """
    url = (
        f"https://baseballsavant.mlb.com/statcast_search/csv"
        f"?all=true"
        f"&player_type=batter"
        f"&hfSea={SEASON}|"
        f"&hfPT={pitch_code}|"
        f"&group_by=name"
        f"&sort_col=pitches&sort_order=desc"
        f"&min_pitches={MIN_PITCHES}"
    )
    print(f"  Fetching batters vs {pitch_code}...")
    r = requests.get(url, headers=HEADERS, timeout=60)
    r.raise_for_status()
    df = pd.read_csv(io.StringIO(r.text))
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    print(f"    {len(df)} batters returned")
    return df


def extract_rows(df, pitch_code):
    """Extract upsert dicts from a Savant grouped DataFrame."""
    rows = []

    # xwOBA column — Savant renames this occasionally
    xwoba_col = None
    for candidate in ['estimated_woba_using_speedangle', 'xwoba', 'est_woba', 'xwoba_value']:
        if candidate in df.columns:
            xwoba_col = candidate
            break

    for _, row in df.iterrows():
        # Player ID
        pid = None
        for id_col in ['player_id', 'batter', 'pitcher']:
            try:
                pid = int(row[id_col])
                break
            except Exception:
                pass
        if not pid:
            continue

        xwoba = _safe(row, xwoba_col) if xwoba_col else None
        woba  = _safe(row, 'woba_value', 'woba')
        pa    = _safe(row, 'pitches', 'pa', 'at_bat_number')

        # K%: Savant returns as decimal (0-1) or percent (0-100) depending on grouping
        k_raw = _safe(row, 'k_percent', 'strikeout_percent', 'k_pct')
        # whiff% and hard_hit% similarly
        wh_raw  = _safe(row, 'whiff_percent', 'whiff_pct')
        hh_raw  = _safe(row, 'hard_hit_percent', 'hard_hit_pct')

        if xwoba is None and woba is None:
            continue   # skip rows with no quality metric

        rows.append({
            'player_id':    pid,
            'season':       SEASON,
            'pitch_type':   pitch_code,
            'pa':           int(pa) if pa is not None else None,
            'xwoba':        xwoba,
            'woba':         woba,
            'k_pct':        k_raw,
            'whiff_pct':    wh_raw,
            'hard_hit_pct': hh_raw,
        })

    return rows


def upsert_batch(rows):
    """Upsert in chunks of 500 to avoid request size limits."""
    for i in range(0, len(rows), 500):
        chunk = rows[i:i + 500]
        sb.table('batter_pitch_type_splits') \
          .upsert(chunk, on_conflict='player_id,season,pitch_type') \
          .execute()
    return len(rows)


def run():
    print(f"\nLoading batter vs pitch-type splits for {SEASON}...")
    total = 0

    for pitch_code in PITCH_TYPES:
        try:
            df = fetch_batter_vs_pitch(pitch_code)
            rows = extract_rows(df, pitch_code)
            if rows:
                upserted = upsert_batch(rows)
                total += upserted
                print(f"    Upserted {upserted} rows for {pitch_code}")
            else:
                print(f"    No valid rows for {pitch_code}")
        except Exception as e:
            print(f"  WARNING {pitch_code}: {e}")
        time.sleep(0.5)

    print(f"\nDone. Total rows upserted: {total}")

    # Quick spot-check: show a sample for verification
    sample = sb.table('batter_pitch_type_splits') \
               .select('player_id,season,pitch_type,pa,xwoba,k_pct') \
               .eq('season', SEASON) \
               .eq('pitch_type', 'FF') \
               .order('pa', desc=True) \
               .limit(5).execute()
    print("\nTop 5 batters vs FF (by pitches seen):")
    for r in (sample.data or []):
        print(f"  pid={r['player_id']}  pa={r['pa']}  xwoba={r['xwoba']}  k_pct={r['k_pct']}")


if __name__ == '__main__':
    run()
