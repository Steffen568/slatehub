#!/usr/bin/env python3
import sys
sys.stdout.reconfigure(encoding='utf-8', line_buffering=True)
"""
load_fangraphs_excel.py — Load FanGraphs stats from Excel Power Queries

Reads MLB_PQs_ALL.xlsx and enriches:
  1. pitcher_stats — xFIP, FIP, LOB%, GB%, FB%, LD%, Stuff+, Location+, Pitching+
  2. batter_stats  — wRC+, wOBA, xwOBA, Pull%/Cent%/Oppo%, FB%/GB%/LD%, bat tracking
  3. batter_splits  — vRHP / vLHP (wOBA, wRC+, K%, BB%, ISO, OBP, SLG)
  4. pitcher_splits — vLHH / vRHH (K%, BB%, FIP, xFIP, wOBA, ERA, OBP, SLG)

Run AFTER load_stats.py (which loads MLB API + Savant as primary data).
This script only fills nulls or overwrites with FanGraphs authoritative values.

Usage:
  py -3.12 load_fangraphs_excel.py
  py -3.12 load_fangraphs_excel.py --dry-run   # preview without uploading
"""

import os, math, unicodedata
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv
from config import SEASON

load_dotenv()
sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])

EXCEL_PATH = os.path.join(os.path.expanduser('~'), 'Desktop', 'WebDev', 'MLB_PQs', 'MLB_PQs_ALL.xlsx')
DRY_RUN = '--dry-run' in sys.argv

# ── Helpers ──────────────────────────────────────────────────────────────────

def normalize(name):
    if not name or not isinstance(name, str):
        return ''
    nfkd = unicodedata.normalize('NFKD', name)
    return nfkd.encode('ASCII', 'ignore').decode('ASCII').strip().lower()

def sfloat(val):
    """Safe float conversion — handles %, NaN, None."""
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return None
    try:
        s = str(val).replace('%', '').replace(',', '').strip()
        if s == '' or s == '-' or s == '--':
            return None
        v = float(s)
        return None if math.isnan(v) or math.isinf(v) else v
    except (ValueError, TypeError):
        return None

def col(df, keyword):
    """Find a column by keyword prefix (before the description text).
    FanGraphs Excel columns look like: 'K%K% - Strikeout Percentage...'
    We match on the short prefix before the first description."""
    keyword_lower = keyword.lower().strip()
    for c in df.columns:
        c_str = str(c).strip()
        # Exact match
        if c_str.lower() == keyword_lower:
            return c_str
        # Match prefix: FG doubles the short name, e.g. 'K%K% - ...'
        doubled = keyword + keyword
        if c_str.lower().startswith(doubled.lower()):
            return c_str
        # Column starts with keyword (handles vFA (pi)vFA, BlastCon%BlastContact%, etc.)
        if c_str.lower().startswith(keyword_lower):
            return c_str
    return None

def get(row, df, keyword, as_float=True):
    """Get a value from a row by keyword column match."""
    c = col(df, keyword)
    if c is None:
        return None
    val = row.get(c)
    return sfloat(val) if as_float else val


# ── Load Player ID Map ───────────────────────────────────────────────────────

print("Loading player ID map from database...")
all_players = []
offset = 0
while True:
    result = sb.table('players').select(
        'mlbam_id, fangraphs_id, name_normalized'
    ).range(offset, offset + 999).execute()
    if not result.data:
        break
    all_players.extend(result.data)
    if len(result.data) < 1000:
        break
    offset += 1000

name_to_mlbam = {}
for p in all_players:
    if p['name_normalized']:
        name_to_mlbam[p['name_normalized']] = p['mlbam_id']

# Also build from pitcher_stats and batter_stats for players not in Chadwick
# AND build name+team lookup to disambiguate common names (Luis Garcia, Eduardo Rodriguez, etc.)
name_team_to_mlbam = {}  # (norm_name, team_abbr) → mlbam_id

# FanGraphs team abbreviations → our DB team names (for matching)
FG_TEAM_MAP = {
    'ARI': 'Arizona Diamondbacks', 'ATL': 'Atlanta Braves', 'BAL': 'Baltimore Orioles',
    'BOS': 'Boston Red Sox', 'CHC': 'Chicago Cubs', 'CHW': 'Chicago White Sox',
    'CWS': 'Chicago White Sox', 'CIN': 'Cincinnati Reds', 'CLE': 'Cleveland Guardians',
    'COL': 'Colorado Rockies', 'DET': 'Detroit Tigers', 'HOU': 'Houston Astros',
    'KC': 'Kansas City Royals', 'KCR': 'Kansas City Royals',
    'LAA': 'Los Angeles Angels', 'LAD': 'Los Angeles Dodgers',
    'MIA': 'Miami Marlins', 'MIL': 'Milwaukee Brewers', 'MIN': 'Minnesota Twins',
    'NYM': 'New York Mets', 'NYY': 'New York Yankees',
    'OAK': 'Athletics', 'ATH': 'Athletics',
    'PHI': 'Philadelphia Phillies', 'PIT': 'Pittsburgh Pirates',
    'SD': 'San Diego Padres', 'SDP': 'San Diego Padres',
    'SF': 'San Francisco Giants', 'SFG': 'San Francisco Giants',
    'SEA': 'Seattle Mariners', 'STL': 'St. Louis Cardinals',
    'TB': 'Tampa Bay Rays', 'TBR': 'Tampa Bay Rays',
    'TEX': 'Texas Rangers', 'TOR': 'Toronto Blue Jays',
    'WSH': 'Washington Nationals', 'WSN': 'Washington Nationals',
}

for tbl in ['pitcher_stats', 'batter_stats']:
    rows = []
    off = 0
    while True:
        r = sb.table(tbl).select('player_id,full_name,team').eq('season', SEASON).range(off, off + 999).execute()
        if not r.data:
            break
        rows.extend(r.data)
        if len(r.data) < 1000:
            break
        off += 1000
    for r in rows:
        n = normalize(r.get('full_name', ''))
        team = r.get('team', '')
        if n and n not in name_to_mlbam:
            name_to_mlbam[n] = r['player_id']
        # Build name+team lookup (team from DB is full name like "New York Yankees")
        if n and team:
            name_team_to_mlbam[(n, team)] = r['player_id']

print(f"  Name mappings: {len(name_to_mlbam):,}")
print(f"  Name+team mappings: {len(name_team_to_mlbam):,}")

def resolve_id(name, team_abbr=None):
    """Resolve a player name to MLBAM ID. Uses team to disambiguate common names."""
    n = normalize(name)
    # Try name+team first (handles Luis Garcia, Eduardo Rodriguez, etc.)
    if team_abbr and n:
        full_team = FG_TEAM_MAP.get(team_abbr.upper(), team_abbr)
        key = (n, full_team)
        if key in name_team_to_mlbam:
            return name_team_to_mlbam[key]
    if n in name_to_mlbam:
        return name_to_mlbam[n]
    # Try last name, first initial match (handles "J. Smith" style)
    return None


# ── Read Excel ───────────────────────────────────────────────────────────────

print(f"\nReading {EXCEL_PATH}...")
xls = pd.ExcelFile(EXCEL_PATH)
print(f"  Sheets: {xls.sheet_names}")

def read_sheet(name):
    if name not in xls.sheet_names:
        print(f"  WARNING: Sheet '{name}' not found")
        return pd.DataFrame()
    df = xls.parse(name)
    # Drop separator columns
    df = df[[c for c in df.columns if 'Line Break' not in str(c)]]
    return df


# ══════════════════════════════════════════════
# STEP 1 — Enrich pitcher_stats
# ══════════════════════════════════════════════

print("\n═══ PITCHER STATS ENRICHMENT ═══")

df_dash = read_sheet('Dash')
df_bb = read_sheet('BattedBall')
df_pit_plus = read_sheet('Pitching+')
df_stuff = read_sheet('Stuff+')
df_loc = read_sheet('Location+')
df_adv = read_sheet('Pitcehr Advanced')
df_disc = read_sheet('Plate Discipline')

# Merge pitching sheets by Name+Team
pitcher_data = {}  # mlbam_id -> dict of updates

for sheet_name, df in [('Dash', df_dash), ('BattedBall', df_bb),
                        ('Pitching+', df_pit_plus), ('Stuff+', df_stuff),
                        ('Location+', df_loc), ('Advanced', df_adv),
                        ('PlateDiscipline', df_disc)]:
    if df.empty:
        continue
    matched = 0
    for _, row in df.iterrows():
        name = row.get('Name')
        if not name or not isinstance(name, str):
            continue
        pid = resolve_id(name, row.get('Team'))
        if not pid:
            continue
        matched += 1

        if pid not in pitcher_data:
            pitcher_data[pid] = {}

        d = pitcher_data[pid]

        if sheet_name == 'Dash':
            d['xfip'] = get(row, df, 'xFIP')
            d['fip'] = get(row, df, 'FIP')
            d['era'] = get(row, df, 'ERA')
            d['lob_pct'] = get(row, df, 'LOB%')
            d['gb_pct'] = get(row, df, 'GB%')
            hr_fb = get(row, df, 'HR/FB')
            d['k9'] = get(row, df, 'K/9')
            d['bb9'] = get(row, df, 'BB/9')
            d['hr9'] = get(row, df, 'HR/9')
            d['babip'] = get(row, df, 'BABIP')
            vfa = get(row, df, 'vFA (pi)')
            if vfa:
                d['velo'] = vfa

        elif sheet_name == 'BattedBall':
            d['ld_pct'] = get(row, df, 'LD%')
            d['fb_pct'] = get(row, df, 'FB%')
            d['gb_pct_bb'] = get(row, df, 'GB%')  # may override Dash GB%
            # Pull/Cent/Oppo — these aren't in pitcher_stats schema yet, skip

        elif sheet_name == 'Pitching+':
            d['stuff_plus'] = get(row, df, 'Stuff+')
            d['location_plus'] = get(row, df, 'Location+')
            d['pitching_plus'] = get(row, df, 'Pitching+')

        elif sheet_name == 'Advanced':
            d['siera'] = get(row, df, 'SIERA')
            d['k_pct'] = get(row, df, 'K%')
            d['bb_pct'] = get(row, df, 'BB%')
            d['whip'] = get(row, df, 'WHIP')

        elif sheet_name == 'PlateDiscipline':
            # Derive SwStr% from Swing% and Contact% — SwStr% = Swing% * (1 - Contact%)
            swing = get(row, df, 'Swing%')
            contact = get(row, df, 'Contact%')
            if swing is not None and contact is not None:
                d['swstr_pct'] = round(swing * (1.0 - contact), 4)

    print(f"  {sheet_name}: matched {matched} pitchers")

# Merge gb_pct from BattedBall if Dash didn't have it
for pid, d in pitcher_data.items():
    if d.get('gb_pct') is None and d.get('gb_pct_bb') is not None:
        d['gb_pct'] = d.pop('gb_pct_bb')
    elif 'gb_pct_bb' in d:
        del d['gb_pct_bb']

# Build upsert rows
pitcher_rows = []
for pid, d in pitcher_data.items():
    row = {'player_id': pid, 'season': SEASON}
    for k, v in d.items():
        if v is not None:
            row[k] = round(v, 4) if isinstance(v, float) else v
    if len(row) > 2:  # has data beyond player_id + season
        pitcher_rows.append(row)

print(f"\n  Pitcher updates: {len(pitcher_rows)} rows")

if not DRY_RUN and pitcher_rows:
    BATCH = 500
    for i in range(0, len(pitcher_rows), BATCH):
        batch = pitcher_rows[i:i+BATCH]
        sb.table('pitcher_stats').upsert(batch, on_conflict='player_id,season').execute()
        print(f"    Uploaded {min(i+BATCH, len(pitcher_rows)):,} / {len(pitcher_rows):,}")
    print(f"  Done — {len(pitcher_rows)} pitcher_stats rows enriched")


# ══════════════════════════════════════════════
# STEP 2 — Enrich batter_stats
# ══════════════════════════════════════════════

print("\n═══ BATTER STATS ENRICHMENT ═══")

df_hdash = read_sheet('HitterDash')
df_hsc = read_sheet('HitterStatcas')
df_bt = read_sheet('BatTracking')

batter_data = {}  # mlbam_id -> dict of updates

for sheet_name, df in [('HitterDash', df_hdash), ('HitterStatcas', df_hsc),
                        ('BatTracking', df_bt)]:
    if df.empty:
        continue
    matched = 0
    for _, row in df.iterrows():
        name = row.get('Name')
        if not name or not isinstance(name, str):
            continue
        pid = resolve_id(name, row.get('Team'))
        if not pid:
            continue
        matched += 1

        if pid not in batter_data:
            batter_data[pid] = {}

        d = batter_data[pid]

        if sheet_name == 'HitterDash':
            d['wrc_plus'] = get(row, df, 'wRC+')
            d['woba'] = get(row, df, 'wOBA')
            d['xwoba'] = get(row, df, 'xwOBA')
            d['k_pct'] = get(row, df, 'K%')
            d['bb_pct'] = get(row, df, 'BB%')
            d['iso'] = get(row, df, 'ISO')
            d['babip'] = get(row, df, 'BABIP')
            d['avg'] = get(row, df, 'AVG')
            d['obp'] = get(row, df, 'OBP')
            d['slg'] = get(row, df, 'SLG')

        elif sheet_name == 'HitterStatcas':
            d['barrel_pct'] = get(row, df, 'Barrel%')
            d['hard_hit_pct'] = get(row, df, 'HardHit%')
            d['avg_ev'] = get(row, df, 'EVEV')

        elif sheet_name == 'BatTracking':
            d['bat_speed'] = get(row, df, 'BatSpd')
            d['attack_angle'] = get(row, df, 'AtkAng')
            d['squared_up_pct'] = get(row, df, 'SqUpCon%')
            d['blast_pct'] = get(row, df, 'BlastCon%')

    print(f"  {sheet_name}: matched {matched} batters")

# Build upsert rows
batter_rows = []
for pid, d in batter_data.items():
    row = {'player_id': pid, 'season': SEASON}
    for k, v in d.items():
        if v is not None:
            row[k] = round(v, 4) if isinstance(v, float) else v
    if len(row) > 2:
        batter_rows.append(row)

print(f"\n  Batter updates: {len(batter_rows)} rows")

if not DRY_RUN and batter_rows:
    BATCH = 500
    for i in range(0, len(batter_rows), BATCH):
        batch = batter_rows[i:i+BATCH]
        sb.table('batter_stats').upsert(batch, on_conflict='player_id,season').execute()
        print(f"    Uploaded {min(i+BATCH, len(batter_rows)):,} / {len(batter_rows):,}")
    print(f"  Done — {len(batter_rows)} batter_stats rows enriched")


# ══════════════════════════════════════════════
# STEP 3 — Batter splits (vRHP / vLHP)
# ══════════════════════════════════════════════

print("\n═══ BATTER SPLITS ═══")

df_vrhp = read_sheet('vRHP')
df_vlhp = read_sheet('vLHP')

batter_split_rows = []

for split_label, df in [('R', df_vrhp), ('L', df_vlhp)]:
    if df.empty:
        continue
    matched = 0
    for _, row in df.iterrows():
        name = row.get('Name')
        if not name or not isinstance(name, str):
            continue
        pid = resolve_id(name, row.get('Team'))
        if not pid:
            continue

        pa = get(row, df, 'PA')
        if not pa or pa < 1:
            continue
        matched += 1

        batter_split_rows.append({
            'player_id':   pid,
            'player_name': str(name).strip(),
            'season':      SEASON,
            'split':       split_label,
            'pa':          int(pa),
            'woba':        get(row, df, 'wOBA'),
            'wrc_plus':    get(row, df, 'wRC+'),
            'k_pct':       get(row, df, 'K%'),
            'bb_pct':      get(row, df, 'BB%'),
            'iso':         get(row, df, 'ISO'),
            'obp':         get(row, df, 'OBP'),
            'slg':         get(row, df, 'SLG'),
            'babip':       get(row, df, 'BABIP'),
            'bb_k':        get(row, df, 'BB/K'),
        })

    print(f"  vs {'RHP' if split_label == 'R' else 'LHP'}: matched {matched} batters")

# Clean None values and dedup (same player_id+split → keep last)
for r in batter_split_rows:
    for k in list(r.keys()):
        if r[k] is None:
            del r[k]

dedup = {}
for r in batter_split_rows:
    dedup[(r['player_id'], r['split'])] = r
batter_split_rows = list(dedup.values())

print(f"\n  Batter split rows: {len(batter_split_rows)}")

if not DRY_RUN and batter_split_rows:
    BATCH = 500
    for i in range(0, len(batter_split_rows), BATCH):
        batch = batter_split_rows[i:i+BATCH]
        sb.table('batter_splits').upsert(batch, on_conflict='player_id,season,split').execute()
        print(f"    Uploaded {min(i+BATCH, len(batter_split_rows)):,} / {len(batter_split_rows):,}")
    print(f"  Done — {len(batter_split_rows)} batter_splits rows upserted")


# ══════════════════════════════════════════════
# STEP 4 — Pitcher splits (vLHH / vRHH)
# ══════════════════════════════════════════════

print("\n═══ PITCHER SPLITS ═══")

df_vlhh_adv = read_sheet('vLHH Adv')
df_vrhh_adv = read_sheet('vRHH Adv')
df_vlhh_std = read_sheet('vLHH Stand')
df_vrhh_std = read_sheet('vRHH Stand')

pitcher_split_data = {}  # (pid, split) -> dict

# Standard sheets first (counting stats + wOBA)
for split_label, df in [('L', df_vlhh_std), ('R', df_vrhh_std)]:
    if df.empty:
        continue
    matched = 0
    for _, row in df.iterrows():
        name = row.get('Name')
        if not name or not isinstance(name, str):
            continue
        pid = resolve_id(name, row.get('Team'))
        if not pid:
            continue
        matched += 1

        key = (pid, split_label)
        if key not in pitcher_split_data:
            pitcher_split_data[key] = {'player_name': str(name).strip()}

        d = pitcher_split_data[key]
        tbf = get(row, df, 'TBF')
        d['pa'] = int(tbf) if tbf else None
        d['woba'] = get(row, df, 'wOBA')
        d['avg'] = get(row, df, 'AVG')
        d['obp'] = get(row, df, 'OBP')
        d['slg'] = get(row, df, 'SLG')
        d['era'] = get(row, df, 'ERA')

    print(f"  vs {'LHH' if split_label == 'L' else 'RHH'} Standard: matched {matched}")

# Advanced sheets (rates + FIP/xFIP)
for split_label, df in [('L', df_vlhh_adv), ('R', df_vrhh_adv)]:
    if df.empty:
        continue
    matched = 0
    for _, row in df.iterrows():
        name = row.get('Name')
        if not name or not isinstance(name, str):
            continue
        pid = resolve_id(name, row.get('Team'))
        if not pid:
            continue
        matched += 1

        key = (pid, split_label)
        if key not in pitcher_split_data:
            pitcher_split_data[key] = {'player_name': str(name).strip()}

        d = pitcher_split_data[key]
        d['k_pct'] = get(row, df, 'K%')
        d['bb_pct'] = get(row, df, 'BB%')
        k_pct = d.get('k_pct')
        bb_pct = d.get('bb_pct')
        if k_pct is not None and bb_pct is not None:
            d['k_bb_pct'] = round(k_pct - bb_pct, 4)
        d['fip'] = get(row, df, 'FIP')
        d['xfip'] = get(row, df, 'xFIP')
        d['babip'] = get(row, df, 'BABIP')
        d['lob_pct'] = get(row, df, 'LOB%')
        d['whip'] = get(row, df, 'WHIP')
        d['k9'] = get(row, df, 'K/9')
        d['bb9'] = get(row, df, 'BB/9')
        d['hr9'] = get(row, df, 'HR/9')

    print(f"  vs {'LHH' if split_label == 'L' else 'RHH'} Advanced: matched {matched}")

# Build upsert rows
pitcher_split_rows = []
for (pid, split_label), d in pitcher_split_data.items():
    row = {
        'player_id':   pid,
        'player_name': d.pop('player_name', ''),
        'season':      SEASON,
        'split':       split_label,
    }
    for k, v in d.items():
        if v is not None:
            row[k] = round(v, 4) if isinstance(v, float) else v
    if len(row) > 4:  # has data beyond the keys
        pitcher_split_rows.append(row)

# Clean None values
for r in pitcher_split_rows:
    for k in list(r.keys()):
        if r[k] is None:
            del r[k]

print(f"\n  Pitcher split rows: {len(pitcher_split_rows)}")

if not DRY_RUN and pitcher_split_rows:
    BATCH = 500
    for i in range(0, len(pitcher_split_rows), BATCH):
        batch = pitcher_split_rows[i:i+BATCH]
        sb.table('pitcher_splits').upsert(batch, on_conflict='player_id,season,split').execute()
        print(f"    Uploaded {min(i+BATCH, len(pitcher_split_rows)):,} / {len(pitcher_split_rows):,}")
    print(f"  Done — {len(pitcher_split_rows)} pitcher_splits rows upserted")


# ══════════════════════════════════════════════
# STEP 5 — Per-pitch Stuff+/Location+/Pitching+ into pitch_arsenal
# ═══════════════════════════════════════════���══

print("\n═══ PITCH ARSENAL ENRICHMENT (per-pitch plus grades) ═══")

# Build name -> pid from pitch_arsenal (names stored as "Last, First")
arsenal_name_to_pid = {}
off = 0
while True:
    r = sb.table('pitch_arsenal').select('player_id,player_name').eq('season', SEASON).range(off, off + 999).execute()
    if not r.data:
        break
    for row in r.data:
        pn = row.get('player_name', '')
        pid = row.get('player_id')
        if pn and pid:
            parts = pn.split(',', 1)
            if len(parts) == 2:
                norm = normalize(f"{parts[1].strip()} {parts[0].strip()}")
            else:
                norm = normalize(pn)
            arsenal_name_to_pid[norm] = pid
    if len(r.data) < 1000:
        break
    off += 1000

# Also use the general name_to_mlbam for broader coverage
for n, pid in name_to_mlbam.items():
    if n not in arsenal_name_to_pid:
        arsenal_name_to_pid[n] = pid

print(f"  Arsenal name lookup: {len(arsenal_name_to_pid):,} pitchers")

PITCH_MAP = {'FA': 'FF', 'SI': 'SI', 'FC': 'FC', 'FS': 'FS',
             'SL': 'SL', 'CU': 'CU', 'CH': 'CH', 'KC': 'KC', 'FO': 'FO'}

# FanGraphs and Savant classify some pitches differently:
#   FG "SL" = Savant "ST" (Sweeper) for 117 pitchers
#   FG "KC" = Savant "CU" (Curveball) for 31 pitchers
# Build per-pitcher pitch type set from Savant (rows with usage_pct),
# then remap FG types to match Savant's classification.
FG_TO_SAVANT_ALIASES = {'SL': 'ST', 'KC': 'CU'}  # FG type -> possible Savant equivalent

savant_pitches = {}  # pid -> set of pitch_types that have usage data
off = 0
while True:
    r = sb.table('pitch_arsenal').select('player_id,pitch_type,usage_pct').eq('season', SEASON).not_.is_('usage_pct', 'null').range(off, off + 999).execute()
    if not r.data:
        break
    for row in r.data:
        savant_pitches.setdefault(row['player_id'], set()).add(row['pitch_type'])
    if len(r.data) < 1000:
        break
    off += 1000

def remap_pitch(pid, fg_type):
    """Remap FG pitch type to match Savant's classification for this pitcher."""
    player_types = savant_pitches.get(pid, set())
    if fg_type in player_types:
        return fg_type  # exact match exists in Savant
    alias = FG_TO_SAVANT_ALIASES.get(fg_type)
    if alias and alias in player_types:
        return alias  # pitcher has the Savant equivalent
    return fg_type  # no alias found, keep original

# Build all plus-grade updates into a single dict keyed by (player_id, pitch_type)
# then batch-upsert once — avoids thousands of individual API calls.
arsenal_pending = {}   # (pid, pt) -> {stuff_plus: int, location_plus: int, pitching_plus: int}
sheet_counts = {}
remap_count = 0

for sheet_name, df, prefix, db_col in [
    ('Stuff+', df_stuff, 'Stf+', 'stuff_plus'),
    ('Location+', df_loc, 'Loc+', 'location_plus'),
    ('Pitching+', df_pit_plus, 'Pit+', 'pitching_plus'),
]:
    if df.empty:
        continue
    count = 0
    for _, row in df.iterrows():
        name = row.get('Name')
        if not name or not isinstance(name, str):
            continue
        norm = normalize(name)
        pid = arsenal_name_to_pid.get(norm)
        if not pid:
            pid = resolve_id(name, row.get('Team'))
        if not pid:
            continue

        for csv_suffix, pt in PITCH_MAP.items():
            col_name = f"{prefix} {csv_suffix}"
            val = sfloat(row.get(col_name))
            if val is None:
                continue
            mapped_pt = remap_pitch(pid, pt)
            if mapped_pt != pt:
                remap_count += 1
            key = (pid, mapped_pt)
            if key not in arsenal_pending:
                arsenal_pending[key] = {}
            arsenal_pending[key][db_col] = int(round(val))
            count += 1

    sheet_counts[sheet_name] = count
    print(f"  {sheet_name} -> pitch_arsenal.{db_col}: {count} values collected")

# Batch upsert all collected updates
arsenal_updates = 0
if arsenal_pending and not DRY_RUN:
    rows = []
    for (pid, pt), cols in arsenal_pending.items():
        row = {'player_id': pid, 'pitch_type': pt, 'season': SEASON}
        row.update(cols)
        rows.append(row)

    BATCH = 500
    for i in range(0, len(rows), BATCH):
        batch = rows[i:i + BATCH]
        sb.table('pitch_arsenal').upsert(
            batch, on_conflict='player_id,pitch_type,season', ignore_duplicates=False
        ).execute()
        print(f"    OK {min(i + BATCH, len(rows))}/{len(rows)}")
    arsenal_updates = len(rows)

if remap_count:
    print(f"  Pitch type remaps (FG->Savant): {remap_count} (SL->ST, KC->CU)")

# Clean up orphan rows created by prior runs (FG-only rows with no usage data)
# These are KC/SL rows where the stuff+ now lives on the CU/ST row instead
if not DRY_RUN:
    orphan_types = list(FG_TO_SAVANT_ALIASES.keys())  # ['SL', 'KC']
    cleaned = 0
    for fg_type, savant_type in FG_TO_SAVANT_ALIASES.items():
        # Find rows for this FG type that have stuff+ but no usage (orphans)
        orphans = sb.table('pitch_arsenal').select('player_id').eq('season', SEASON).eq('pitch_type', fg_type).is_('usage_pct', 'null').not_.is_('stuff_plus', 'null').limit(500).execute()
        if orphans.data:
            pids = [r['player_id'] for r in orphans.data]
            # Only delete if the pitcher has the Savant equivalent with usage
            for pid in pids:
                if savant_type in savant_pitches.get(pid, set()):
                    sb.table('pitch_arsenal').delete().eq('player_id', pid).eq('pitch_type', fg_type).eq('season', SEASON).execute()
                    cleaned += 1
    if cleaned:
        print(f"  Cleaned {cleaned} orphan FG-only pitch rows (merged into Savant types)")

print(f"  Total pitch_arsenal updates: {arsenal_updates} rows")


# ══════════════════════════════════════════════
# SUMMARY
# ══════════════════════════════════════════════

print(f"\n{'='*55}")
print(f"  FanGraphs Excel enrichment complete (season {SEASON})")
print(f"  Pitcher stats:  {len(pitcher_rows):,} rows")
print(f"  Batter stats:   {len(batter_rows):,} rows")
print(f"  Batter splits:  {len(batter_split_rows):,} rows")
print(f"  Pitcher splits: {len(pitcher_split_rows):,} rows")
if DRY_RUN:
    print(f"  ** DRY RUN — nothing uploaded **")
print(f"{'='*55}")
