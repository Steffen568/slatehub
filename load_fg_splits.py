#!/usr/bin/env python3
"""
FanGraphs Splits Loader — CSV Import
Reads CSV exports from FanGraphs (via Power Query) and uploads to Supabase.

Expected files in same directory as this script:
  splits_batter_vs_lhp.csv   — batters vs LHP (splitArr=1, statgroup=2)
  splits_batter_vs_rhp.csv   — batters vs RHP (splitArr=2, statgroup=2)
  splits_pitcher_vs_lhh_std.csv — pitchers vs LHH standard (splitArr=5, statgroup=1)
  splits_pitcher_vs_lhh_adv.csv — pitchers vs LHH advanced (splitArr=5, statgroup=2)
  splits_pitcher_vs_rhh_std.csv — pitchers vs RHH standard (splitArr=6, statgroup=1)
  splits_pitcher_vs_rhh_adv.csv — pitchers vs RHH advanced (splitArr=6, statgroup=2)

Usage:
  py -3.12 load_fg_splits.py             # all splits
  py -3.12 load_fg_splits.py --batters   # batter splits only
  py -3.12 load_fg_splits.py --pitchers  # pitcher splits only
"""

import os, sys, pandas as pd
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()
sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])

from config import SEASON

DO_BATTERS  = '--batters'  in sys.argv or len(sys.argv) == 1
DO_PITCHERS = '--pitchers' in sys.argv or len(sys.argv) == 1

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# ── File definitions
FILES = {
    'batter_vs_lhp'       : {'file': 'splits_batter_vs_lhp.csv',        'split': 'L', 'type': 'batter'},
    'batter_vs_rhp'       : {'file': 'splits_batter_vs_rhp.csv',        'split': 'R', 'type': 'batter'},
    'pitcher_vs_lhh_std'  : {'file': 'splits_pitcher_vs_lhh_std.csv',   'split': 'L', 'type': 'pitcher_std'},
    'pitcher_vs_lhh_adv'  : {'file': 'splits_pitcher_vs_lhh_adv.csv',   'split': 'L', 'type': 'pitcher_adv'},
    'pitcher_vs_rhh_std'  : {'file': 'splits_pitcher_vs_rhh_std.csv',   'split': 'R', 'type': 'pitcher_std'},
    'pitcher_vs_rhh_adv'  : {'file': 'splits_pitcher_vs_rhh_adv.csv',   'split': 'R', 'type': 'pitcher_adv'},
}

# ── Helpers
def safe_float(val):
    try:
        if pd.isna(val): return None
        return float(str(val).replace('%','').replace(',','').strip())
    except:
        return None

def pct(val):
    # FanGraphs exports pct values as decimals (0.176 = 17.6%), no conversion needed
    v = safe_float(val)
    if v is None: return None
    return round(v, 6)

def load_csv(key):
    path = os.path.join(SCRIPT_DIR, FILES[key]['file'])
    if not os.path.exists(path):
        print(f"  ⚠ File not found: {FILES[key]['file']} — skipping")
        return None
    df = pd.read_csv(path)
    df.columns = [str(c).strip() for c in df.columns]
    print(f"  Loaded {FILES[key]['file']}: {len(df)} rows, cols: {list(df.columns)[:8]}")
    return df

def build_name_map(table, name_col='full_name'):
    res = sb.table(table).select(f'player_id, {name_col}').execute()
    return {r[name_col].strip().lower(): r['player_id'] for r in (res.data or []) if r.get(name_col)}

def upsert(table, records, conflict='player_id,season,split'):
    if not records:
        print(f"  No records to upload")
        return
    # Deduplicate
    seen = {}
    for r in records:
        key = tuple(r.get(k) for k in conflict.split(','))
        seen[key] = r
    records = list(seen.values())
    for i in range(0, len(records), 500):
        sb.table(table).upsert(records[i:i+500], on_conflict=conflict).execute()
    print(f"  ✓ Uploaded {len(records)} records → {table}")

# ── Processors
def process_batter(df, split_code, name_map):
    records, unmatched = [], []
    for _, row in df.iterrows():
        name = str(row.get('Name', '')).strip()
        if not name or name == 'nan': continue
        pid = name_map.get(name.lower())
        if not pid:
            unmatched.append(name)
            continue
        records.append({
            'player_id'   : pid,
            'player_name' : name,
            'season'      : SEASON,
            'split'       : split_code,
            'pa'          : safe_float(row.get('PA')),
            'avg'         : safe_float(row.get('AVG')),
            'obp'         : safe_float(row.get('OBP')),
            'slg'         : safe_float(row.get('SLG')),
            'ops'         : safe_float(row.get('OPS')),
            'woba'        : safe_float(row.get('wOBA')),
            'wrc_plus'    : safe_float(row.get('wRC+')),
            'wraa'        : safe_float(row.get('wRAA')),
            'babip'       : safe_float(row.get('BABIP')),
            'iso'         : safe_float(row.get('ISO')),
            'k_pct'       : safe_float(row.get('K%')),
            'bb_pct'      : safe_float(row.get('BB%')),
            'bb_k'        : safe_float(row.get('BB/K')),
            'xwoba'       : None,
            'hard_hit_pct': None,
            'barrel_pct'  : None,
        })
    print(f"  Matched: {len(records)} | Unmatched: {len(unmatched)}")
    if unmatched[:3]: print(f"  Sample unmatched: {unmatched[:3]}")
    return records

def process_pitcher_std(df, split_code, name_map):
    records, unmatched = [], []
    for _, row in df.iterrows():
        name = str(row.get('Name', '')).strip()
        if not name or name == 'nan': continue
        pid = name_map.get(name.lower())
        if not pid:
            unmatched.append(name)
            continue
        records.append({
            'player_id'  : pid,
            'player_name': name,
            'season'     : SEASON,
            'split'      : split_code,
            'pa'         : safe_float(row.get('TBF')),
            'era'        : safe_float(row.get('ERA')),
            'avg'        : safe_float(row.get('AVG')),
            'obp'        : safe_float(row.get('OBP')),
            'slg'        : safe_float(row.get('SLG')),
            'woba'       : safe_float(row.get('wOBA')),
        })
    print(f"  Matched: {len(records)} | Unmatched: {len(unmatched)}")
    return records

def process_pitcher_adv(df, split_code, existing, name_map):
    for _, row in df.iterrows():
        name = str(row.get('Name', '')).strip()
        if not name or name == 'nan': continue
        pid = name_map.get(name.lower())
        if not pid: continue
        rec = next((r for r in existing if r['player_id'] == pid and r['split'] == split_code), None)
        if not rec:
            rec = {'player_id': pid, 'player_name': name, 'season': SEASON, 'split': split_code}
            existing.append(rec)
        rec.update({
            'fip'      : safe_float(row.get('FIP')),
            'xfip'     : safe_float(row.get('xFIP')),
            'k9'       : safe_float(row.get('K/9')),
            'bb9'      : safe_float(row.get('BB/9')),
            'hr9'      : safe_float(row.get('HR/9')),
            'k_pct'    : safe_float(row.get('K%')),
            'bb_pct'   : safe_float(row.get('BB%')),
            'k_bb_pct' : safe_float(row.get('K-BB%')),
            'whip'     : safe_float(row.get('WHIP')),
            'babip'    : safe_float(row.get('BABIP')),
            'lob_pct'  : safe_float(row.get('LOB%')),
        })
    return existing

# ── Main
def run():
    print("\nFanGraphs Splits Loader — CSV")
    print("=" * 40)

    if DO_BATTERS:
        print("\n── Batter Splits")
        name_map = build_name_map('batter_stats')
        print(f"  Name map: {len(name_map)} batters")
        records = []
        for key in ['batter_vs_lhp', 'batter_vs_rhp']:
            df = load_csv(key)
            if df is not None:
                records += process_batter(df, FILES[key]['split'], name_map)
        upsert('batter_splits', records)

    if DO_PITCHERS:
        print("\n── Pitcher Splits")
        name_map = build_name_map('pitcher_stats')
        print(f"  Name map: {len(name_map)} pitchers")
        records = []
        for hand in ['L', 'R']:
            h = hand.lower()
            df_std = load_csv(f'pitcher_vs_{h}hh_std')
            if df_std is not None:
                records += process_pitcher_std(df_std, hand, name_map)
            df_adv = load_csv(f'pitcher_vs_{h}hh_adv')
            if df_adv is not None:
                records = process_pitcher_adv(df_adv, hand, records, name_map)
        upsert('pitcher_splits', records)

    print("\nDone!")

if __name__ == '__main__':
    run()
    