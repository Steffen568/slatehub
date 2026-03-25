#!/usr/bin/env python3
"""
sync_excel_splits.py — Excel Power Query Sync + Missing SP Placeholder

Does three things in one run:
  1. Reads your FanGraphs Power Query Excel file and exports all 6 split
     sheets as season-stamped CSVs to the MLB CSVs folder
  2. Runs the full splits upload to Supabase (season-stamped rows — prior
     seasons are never overwritten)
  3. Checks today's probable SPs against pitcher_stats and inserts minimal
     placeholder rows for any missing pitchers so the UI doesn't break

CSVs are named splits_batter_vs_lhp_{SEASON}.csv etc. so 2025 and 2026
files coexist safely in the same folder.

Usage:
  py -3.12 sync_excel_splits.py                    # full run
  py -3.12 sync_excel_splits.py --excel-only       # just export CSVs, no upload
  py -3.12 sync_excel_splits.py --upload-only      # skip Excel, just upload existing CSVs
  py -3.12 sync_excel_splits.py --fix-missing-sps  # just fix missing SP placeholders
"""
import unicodedata
import os, sys
from datetime import date
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()
sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])

from config import SEASON

# ── Paths ─────────────────────────────────────────────────────────────────────
EXCEL_PATH = r"C:\Users\Steffen's PC\Desktop\WebDev\MLB_PQs\MLB_PQs.xlsx"
CSV_DIR    = r"C:\Users\Steffen's PC\Desktop\WebDev\MLB CSVs"

DO_EXCEL  = '--upload-only'     not in sys.argv
DO_UPLOAD = '--excel-only'      not in sys.argv
DO_FIX_SP = '--fix-missing-sps' in sys.argv or (len(sys.argv) == 1)

# ── Sheet name → season-stamped CSV filename ──────────────────────────────────
SHEET_MAP = {
    'vsLHP'              : f'splits_batter_vs_lhp_{SEASON}.csv',
    'vsRHP'              : f'splits_batter_vs_rhp_{SEASON}.csv',
    'Pitcher vs LHH Std' : f'splits_pitcher_vs_lhh_std_{SEASON}.csv',
    'Pitcher vs LHH Adv' : f'splits_pitcher_vs_lhh_adv_{SEASON}.csv',
    'Pitcher vs RHH Std' : f'splits_pitcher_vs_rhh_std_{SEASON}.csv',
    'Pitcher vs RHH Adv' : f'splits_pitcher_vs_rhh_adv_{SEASON}.csv',
}

# ── File definitions ──────────────────────────────────────────────────────────
FILES = {
    'batter_vs_lhp'      : {'file': f'splits_batter_vs_lhp_{SEASON}.csv',        'split': 'L', 'type': 'batter'},
    'batter_vs_rhp'      : {'file': f'splits_batter_vs_rhp_{SEASON}.csv',        'split': 'R', 'type': 'batter'},
    'pitcher_vs_lhh_std' : {'file': f'splits_pitcher_vs_lhh_std_{SEASON}.csv',   'split': 'L', 'type': 'pitcher_std'},
    'pitcher_vs_lhh_adv' : {'file': f'splits_pitcher_vs_lhh_adv_{SEASON}.csv',   'split': 'L', 'type': 'pitcher_adv'},
    'pitcher_vs_rhh_std' : {'file': f'splits_pitcher_vs_rhh_std_{SEASON}.csv',   'split': 'R', 'type': 'pitcher_std'},
    'pitcher_vs_rhh_adv' : {'file': f'splits_pitcher_vs_rhh_adv_{SEASON}.csv',   'split': 'R', 'type': 'pitcher_adv'},
}

# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

def safe_float(val):
    try:
        if pd.isna(val): return None
        return float(str(val).replace('%','').replace(',','').strip())
    except:
        return None

def load_csv(key):
    path = os.path.join(CSV_DIR, FILES[key]['file'])
    if not os.path.exists(path):
        print(f"  ⚠ File not found: {FILES[key]['file']}")
        print(f"    Expected at: {path}")
        return None
    df = pd.read_csv(path)
    df.columns = [str(c).strip() for c in df.columns]
    print(f"  Loaded {FILES[key]['file']}: {len(df)} rows")
    return df

def build_name_map(table, name_col='full_name'):
    def normalize(name):
        nfkd = unicodedata.normalize('NFKD', name)
        return nfkd.encode('ASCII', 'ignore').decode('ASCII').strip().lower()
    res = sb.table(table).select(f'player_id, {name_col}').execute()
    return {normalize(r[name_col]): r['player_id'] for r in (res.data or []) if r.get(name_col)}

def upsert(table, records, conflict='player_id,season,split'):
    if not records:
        print(f"  No records to upload")
        return
    seen = {}
    for r in records:
        key = tuple(r.get(k) for k in conflict.split(','))
        seen[key] = r
    records = list(seen.values())
    for i in range(0, len(records), 500):
        sb.table(table).upsert(records[i:i+500], on_conflict=conflict).execute()
    print(f"  ✓ Uploaded {len(records)} records → {table} (season={SEASON})")

# ─────────────────────────────────────────────
# STEP 1 — Export Excel sheets to season-stamped CSVs
# ─────────────────────────────────────────────

def export_excel_to_csvs():
    print(f"\n── Step 1: Export Excel → CSVs (season={SEASON})")
    print(f"  Source: {EXCEL_PATH}")
    print(f"  Output: {CSV_DIR}")

    if not os.path.exists(EXCEL_PATH):
        print(f"  ✗ ERROR: Excel file not found.")
        return False

    os.makedirs(CSV_DIR, exist_ok=True)

    try:
        xl = pd.ExcelFile(EXCEL_PATH)
        print(f"  Found sheets: {xl.sheet_names}")
    except Exception as e:
        print(f"  ✗ ERROR reading Excel file: {e}")
        return False

    exported = 0
    for sheet_name, csv_name in SHEET_MAP.items():
        matched_sheet = None
        for s in xl.sheet_names:
            if s == sheet_name or s.lower() == sheet_name.lower():
                matched_sheet = s
                break

        if not matched_sheet:
            print(f"  ⚠ Sheet not found: '{sheet_name}' — skipping")
            print(f"    Available sheets: {xl.sheet_names}")
            continue

        df = xl.parse(matched_sheet)
        df.columns = [str(c).strip() for c in df.columns]
        df = df.dropna(how='all')

        out_path = os.path.join(CSV_DIR, csv_name)
        df.to_csv(out_path, index=False)
        print(f"  ✓ {matched_sheet} → {csv_name} ({len(df)} rows)")
        exported += 1

    print(f"\n  Exported {exported}/{len(SHEET_MAP)} sheets")
    return exported > 0

# ─────────────────────────────────────────────
# STEP 2 — Upload splits to Supabase
# ─────────────────────────────────────────────

def process_batter(df, split_code, name_map):
    records, unmatched = [], []
    for _, row in df.iterrows():
        name = str(row.get('Name', '')).strip()
        if not name or name == 'nan': continue
        pid = name_map.get(
            unicodedata.normalize('NFKD', name).encode('ASCII', 'ignore').decode('ASCII').strip().lower()
        )
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
    if unmatched[:5]: print(f"  Sample unmatched: {unmatched[:5]}")
    return records

def process_pitcher_std(df, split_code, name_map):
    records, unmatched = [], []
    for _, row in df.iterrows():
        name = str(row.get('Name', '')).strip()
        if not name or name == 'nan': continue
        pid = name_map.get(
            unicodedata.normalize('NFKD', name).encode('ASCII', 'ignore').decode('ASCII').strip().lower()
        )
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
    if unmatched[:5]: print(f"  Sample unmatched: {unmatched[:5]}")
    return records

def process_pitcher_adv(df, split_code, existing, name_map):
    for _, row in df.iterrows():
        name = str(row.get('Name', '')).strip()
        if not name or name == 'nan': continue
        pid = name_map.get(
            unicodedata.normalize('NFKD', name).encode('ASCII', 'ignore').decode('ASCII').strip().lower()
        )
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

def upload_splits():
    print(f"\n── Step 2: Upload Splits to Supabase (season={SEASON})")

    print("\n  Batter Splits")
    name_map = build_name_map('batter_stats')
    print(f"  Name map: {len(name_map)} batters")
    records = []
    for key in ['batter_vs_lhp', 'batter_vs_rhp']:
        df = load_csv(key)
        if df is not None:
            records += process_batter(df, FILES[key]['split'], name_map)
    upsert('batter_splits', records)

    print("\n  Pitcher Splits")
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

# ─────────────────────────────────────────────
# STEP 3 — Fix missing SP placeholders
# ─────────────────────────────────────────────

def fix_missing_sps():
    print("\n── Step 3: Check for Missing Probable SPs")
    today = str(date.today())

    res = sb.table('games').select(
        'home_sp_id,home_sp_name,away_sp_id,away_sp_name'
    ).eq('game_date', today).execute()

    if not res.data:
        print(f"  No games found for {today}")
        return

    sps = {}
    for g in res.data:
        if g.get('home_sp_id') and g.get('home_sp_name'):
            sps[g['home_sp_id']] = g['home_sp_name']
        if g.get('away_sp_id') and g.get('away_sp_name'):
            sps[g['away_sp_id']] = g['away_sp_name']

    print(f"  Found {len(sps)} probable SPs for {today}")
    if not sps:
        return

    sp_ids = list(sps.keys())
    existing = sb.table('pitcher_stats').select('player_id').in_(
        'player_id', sp_ids
    ).execute()
    existing_ids = {r['player_id'] for r in (existing.data or [])}

    missing = {pid: name for pid, name in sps.items() if pid not in existing_ids}

    if not missing:
        print(f"  ✓ All {len(sps)} SPs found in pitcher_stats — nothing to fix")
        return

    print(f"  ⚠ {len(missing)} SPs missing from pitcher_stats:")
    for pid, name in missing.items():
        print(f"    - {name} (ID: {pid})")

    placeholders = [
        {
            'player_id': pid,
            'full_name': name,
            'season'   : SEASON,
            'team'     : 'UNK',
        }
        for pid, name in missing.items()
    ]

    try:
        sb.table('pitcher_stats').upsert(
            placeholders, on_conflict='player_id,season', ignore_duplicates=True
        ).execute()
        print(f"  ✓ Inserted {len(placeholders)} placeholder rows")
    except Exception as e:
        print(f"  ✗ Error inserting placeholders: {e}")

# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def run():
    print(f"\nSlateHub — Excel Sync + SP Fix (season={SEASON})")
    print("=" * 40)

    if DO_EXCEL:
        success = export_excel_to_csvs()
        if not success and DO_UPLOAD:
            print(f"\n  ⚠ Excel export failed — uploading from existing CSVs instead")

    if DO_UPLOAD:
        upload_splits()

    if DO_FIX_SP:
        fix_missing_sps()

    print("\nDone!")

if __name__ == '__main__':
    run()
