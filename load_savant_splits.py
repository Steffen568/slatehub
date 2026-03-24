#!/usr/bin/env python3
"""
load_savant_splits.py — xwOBA splits by handedness from Baseball Savant

Fetches:
  - Batter xwOBA vs LHP  → updates batter_splits.xwoba where split='L'
  - Batter xwOBA vs RHP  → updates batter_splits.xwoba where split='R'
  - Pitcher xwOBA allowed vs LHH → updates pitcher_splits.xwoba where split='L'
  - Pitcher xwOBA allowed vs RHH → updates pitcher_splits.xwoba where split='R'

URL filter used: hfOppTeamBH=L| or R|
  - With player_type=batter  → "opponent pitcher throws L/R" (batters vs LHP or RHP)
  - With player_type=pitcher → "opponent batter stands L/R" (pitchers vs LHH or RHH)

Note: pitcher_splits table needs an xwoba column added in Supabase before this
      script can write pitcher xwOBA. Run this SQL first:
        ALTER TABLE pitcher_splits ADD COLUMN IF NOT EXISTS xwoba FLOAT;
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
MIN_EVENTS = 50   # minimum batted ball events to include a split


# ── Fetch ─────────────────────────────────────────────────────────────────────

def fetch_savant(player_type, opp_hand):
    """
    Pull Savant statcast_search CSV grouped by player, filtered by opponent hand.
      player_type='batter'  + opp_hand='L' → batters vs LHP
      player_type='batter'  + opp_hand='R' → batters vs RHP
      player_type='pitcher' + opp_hand='L' → pitchers vs LHH
      player_type='pitcher' + opp_hand='R' → pitchers vs RHH
    """
    url = (
        f"https://baseballsavant.mlb.com/statcast_search/csv"
        f"?all=true"
        f"&player_type={player_type}"
        f"&hfSea={SEASON}|"
        f"&hfOppTeamBH={opp_hand}|"
        f"&group_by=name"
        f"&sort_col=pitches&sort_order=desc"
        f"&min_pitches={MIN_EVENTS}"
    )
    print(f"  Fetching {player_type} vs opp_hand={opp_hand}...")
    r = requests.get(url, headers=HEADERS, timeout=60)
    r.raise_for_status()

    df = pd.read_csv(io.StringIO(r.text))
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    print(f"  Got {len(df)} rows")
    print(f"  Columns: {list(df.columns)[:20]}")   # debug — lets us verify filter worked

    return df


# ── Extract ───────────────────────────────────────────────────────────────────

def extract_xwoba(df):
    """
    Pull {player_id: xwoba} from a Savant grouped DataFrame.
    Tries multiple column name variants in case Savant renames them.
    """
    xwoba_col = None
    for candidate in ['estimated_woba_using_speedangle', 'xwoba', 'est_woba', 'xwoba_value']:
        if candidate in df.columns:
            xwoba_col = candidate
            print(f"  xwOBA column found: '{xwoba_col}'")
            break

    if not xwoba_col:
        print(f"  WARNING: no xwOBA column found in response.")
        print(f"  All columns: {list(df.columns)}")
        return {}

    result = {}
    for _, row in df.iterrows():
        pid = row.get('player_id')
        val = row.get(xwoba_col)
        if pid is not None and pd.notna(val):
            try:
                result[int(pid)] = round(float(val), 3)
            except (ValueError, TypeError):
                pass

    print(f"  Extracted xwOBA for {len(result)} players")
    return result


# ── Update ────────────────────────────────────────────────────────────────────

def bulk_update_xwoba(table, xwoba_map, split_hand, conflict_cols='player_id,season,split'):
    """
    Fetch existing rows for this split, merge in new xwoba values, re-upsert.
    This avoids N individual HTTP calls — only a few batch requests total.
    """
    if not xwoba_map:
        print(f"  No xwOBA data — skipping {table} split={split_hand}")
        return

    # Fetch all existing rows for this season + split
    rows = []
    offset = 0
    while True:
        res = (sb.table(table)
                 .select('*')
                 .eq('season', SEASON)
                 .eq('split', split_hand)
                 .range(offset, offset + 999)
                 .execute())
        if not res.data:
            break
        rows.extend(res.data)
        if len(res.data) < 1000:
            break
        offset += 1000

    print(f"  Fetched {len(rows)} existing rows from {table} split={split_hand}")

    # Merge xwoba into matching rows
    updated = []
    unmatched = 0
    for row in rows:
        pid = row.get('player_id')
        if pid in xwoba_map:
            row['xwoba'] = xwoba_map[pid]
            updated.append(row)
        else:
            unmatched += 1

    print(f"  Matched: {len(updated)} | No Savant data: {unmatched}")

    if not updated:
        print(f"  Nothing to upsert.")
        return

    # Upsert in batches of 500
    BATCH = 500
    uploaded = 0
    for i in range(0, len(updated), BATCH):
        batch = updated[i:i + BATCH]
        try:
            sb.table(table).upsert(batch, on_conflict=conflict_cols).execute()
            uploaded += len(batch)
            print(f"  Upserted {min(i + BATCH, len(updated))}/{len(updated)}")
        except Exception as e:
            print(f"  ERROR on batch {i // BATCH + 1}: {e}")
            print(f"  (If pitcher_splits is missing xwoba column, run the SQL in the header comment)")
            break

    print(f"  Done — {uploaded} rows updated in {table} split={split_hand}")


# ── Main ──────────────────────────────────────────────────────────────────────

def run():
    print("\nload_savant_splits.py — xwOBA splits from Baseball Savant")
    print("=" * 58)

    # 1 — Batter xwOBA vs LHP (split='L')
    print("\n[1/4] Batter xwOBA vs LHP")
    try:
        df = fetch_savant('batter', 'L')
        xwoba_map = extract_xwoba(df)
        bulk_update_xwoba('batter_splits', xwoba_map, 'L')
    except Exception as e:
        print(f"  ERROR: {e}")
    time.sleep(1)

    # 2 — Batter xwOBA vs RHP (split='R')
    print("\n[2/4] Batter xwOBA vs RHP")
    try:
        df = fetch_savant('batter', 'R')
        xwoba_map = extract_xwoba(df)
        bulk_update_xwoba('batter_splits', xwoba_map, 'R')
    except Exception as e:
        print(f"  ERROR: {e}")
    time.sleep(1)

    # 3 — Pitcher xwOBA allowed vs LHH (split='L')
    print("\n[3/4] Pitcher xwOBA allowed vs LHH")
    try:
        df = fetch_savant('pitcher', 'L')
        xwoba_map = extract_xwoba(df)
        bulk_update_xwoba('pitcher_splits', xwoba_map, 'L')
    except Exception as e:
        print(f"  ERROR: {e}")
    time.sleep(1)

    # 4 — Pitcher xwOBA allowed vs RHH (split='R')
    print("\n[4/4] Pitcher xwOBA allowed vs RHH")
    try:
        df = fetch_savant('pitcher', 'R')
        xwoba_map = extract_xwoba(df)
        bulk_update_xwoba('pitcher_splits', xwoba_map, 'R')
    except Exception as e:
        print(f"  ERROR: {e}")

    print("\nDone.")
    print("Tip: check 'Columns:' output above to confirm xwOBA column was found.")
    print("     If batter count seems wrong (e.g. all RHB or all LHB), the hfOppTeamBH")
    print("     filter may need adjustment — check the column distribution.")


if __name__ == "__main__":
    run()
