import sys
sys.stdout.reconfigure(encoding='utf-8', line_buffering=True)
print("Script started", flush=True)

import pandas as pd
from supabase import create_client
from dotenv import load_dotenv
import os
import math
import unicodedata
import requests
import io
import csv

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

# ── Helper: safe float
def sfloat(val, default=None):
    try:
        v = float(val)
        return v if not (math.isnan(v) or math.isinf(v)) else default
    except (ValueError, TypeError):
        return default

# ── Helper: safe int
def sint(val, default=None):
    try:
        return int(val)
    except (ValueError, TypeError):
        return default

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
ambiguous_names = set()

for p in all_players:
    if p['fangraphs_id']:
        fg_to_mlbam[str(p['fangraphs_id'])] = p['mlbam_id']
    if p['name_normalized']:
        nn = p['name_normalized']
        mlbam = p['mlbam_id']
        if nn in name_to_mlbam and name_to_mlbam[nn] != mlbam:
            ambiguous_names.add(nn)
        else:
            name_to_mlbam[nn] = mlbam

# Remove ambiguous names — FG ID or roster API must resolve these
for nn in ambiguous_names:
    name_to_mlbam.pop(nn, None)

print(f"  FanGraphs ID mappings: {len(fg_to_mlbam):,}")
print(f"  Name mappings:         {len(name_to_mlbam):,}")
if ambiguous_names:
    print(f"  Ambiguous names excluded: {len(ambiguous_names):,}")

# Build roster-based name lookup as authoritative fallback for ambiguous names
MLB_TEAMS = [108,109,110,111,112,113,114,115,116,117,118,119,120,121,
             133,134,135,136,137,138,139,140,141,142,143,144,145,146,147,158]
roster_name_to_mlbam = {}
print("  Loading MLB active rosters for name disambiguation...")
for tid in MLB_TEAMS:
    try:
        url = f"https://statsapi.mlb.com/api/v1/teams/{tid}/roster?rosterType=active"
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        for p in r.json().get('roster', []):
            person = p.get('person', {})
            pid = person.get('id')
            pname = person.get('fullName', '')
            if pid and pname:
                roster_name_to_mlbam[normalize(pname)] = pid
    except Exception:
        pass
print(f"  Roster mappings: {len(roster_name_to_mlbam):,}")

# Build SP name→MLBAM fallback from games table (catches new players not in Chadwick register)
sp_name_to_mlbam = {}
try:
    sp_rows = supabase.table('games').select(
        'home_sp_id, home_sp_name, away_sp_id, away_sp_name'
    ).not_.is_('home_sp_id', 'null').limit(5000).execute().data or []
    for r in sp_rows:
        if r.get('home_sp_name') and r.get('home_sp_id'):
            sp_name_to_mlbam[normalize(r['home_sp_name'])] = r['home_sp_id']
        if r.get('away_sp_name') and r.get('away_sp_id'):
            sp_name_to_mlbam[normalize(r['away_sp_name'])] = r['away_sp_id']
    print(f"  SP name fallback mappings: {len(sp_name_to_mlbam):,}")
except Exception as e:
    print(f"  WARNING: Could not load SP fallback: {e}")

# ── Helper: find MLBAM ID from a FanGraphs row
def get_mlbam_id(fg_id, name):
    try:
        fg_str = str(int(float(fg_id)))
        if fg_str in fg_to_mlbam:
            return fg_to_mlbam[fg_str]
    except (ValueError, TypeError):
        pass
    normalized = normalize(str(name)) if name else ''
    # Priority 2: Unambiguous name (only names that map to exactly one player)
    if normalized in name_to_mlbam:
        return name_to_mlbam[normalized]
    # Priority 3: MLB active roster lookup (authoritative for ambiguous names)
    if normalized in roster_name_to_mlbam:
        return roster_name_to_mlbam[normalized]
    # Priority 4: Games table SP names (catches new players not in Chadwick register)
    if normalized in sp_name_to_mlbam:
        mlbam_id = sp_name_to_mlbam[normalized]
        # Auto-insert into players table so future runs resolve instantly
        try:
            parts = name.strip().split(' ', 1) if name else ['', '']
            first = parts[0] if len(parts) > 0 else ''
            last = parts[1] if len(parts) > 1 else ''
            supabase.table('players').upsert({
                'mlbam_id': mlbam_id,
                'fangraphs_id': int(float(fg_id)) if fg_id else None,
                'name_normalized': normalized,
                'full_name': name.strip(),
                'first_name': first,
                'last_name': last,
            }, on_conflict='mlbam_id').execute()
            # Update local caches
            if fg_id:
                fg_to_mlbam[str(int(float(fg_id)))] = mlbam_id
            name_to_mlbam[normalized] = mlbam_id
            print(f"    + Auto-registered '{name}' (MLBAM {mlbam_id}, FG {fg_id})")
        except Exception as e:
            print(f"    WARNING: Could not auto-register '{name}': {e}")
        return mlbam_id
    return None

# ── Helper: find MLBAM ID from MLB Stats API player id (they use the same IDs)
def get_mlbam_from_api(player_id, full_name):
    """MLB Stats API player IDs ARE MLBAM IDs — just verify it's in our DB."""
    pid = sint(player_id)
    if not pid:
        return None
    # Check if this player exists in our name map (fast path)
    normalized = normalize(full_name) if full_name else ''
    if normalized in name_to_mlbam and name_to_mlbam[normalized] == pid:
        return pid
    # The MLB API ID is the MLBAM ID, trust it directly
    return pid


# ══════════════════════════════════════════════
# STEP 2 — Fetch from MLB Stats API (primary)
# ══════════════════════════════════════════════

def fetch_mlb_api_pitching(season):
    """Fetch season pitching stats from the official MLB Stats API."""
    print("\nFetching pitching stats from MLB Stats API...")
    url = (f'https://statsapi.mlb.com/api/v1/stats?stats=season&group=pitching'
           f'&season={season}&gameType=R&limit=1000&offset=0'
           f'&sortStat=inningsPitched&order=desc')
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    splits = r.json().get('stats', [{}])[0].get('splits', [])
    print(f"  Got {len(splits)} pitchers from MLB API")
    return splits

def fetch_mlb_api_batting(season):
    """Fetch season batting stats from the official MLB Stats API."""
    print("\nFetching batting stats from MLB Stats API...")
    url = (f'https://statsapi.mlb.com/api/v1/stats?stats=season&group=hitting'
           f'&season={season}&gameType=R&limit=1000&offset=0'
           f'&sortStat=plateAppearances&order=desc')
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    splits = r.json().get('stats', [{}])[0].get('splits', [])
    print(f"  Got {len(splits)} batters from MLB API")
    return splits


# ══════════════════════════════════════════════
# STEP 3 — Fetch from Baseball Savant (expected stats + statcast)
# ══════════════════════════════════════════════

def fetch_savant_expected(player_type, season):
    """Fetch xwOBA/xBA/xSLG/xERA from Savant expected stats leaderboard."""
    print(f"  Fetching Savant expected stats ({player_type})...")
    url = (f'https://baseballsavant.mlb.com/leaderboard/expected_statistics'
           f'?type={player_type}&year={season}&position=&team=&min=1&csv=true')
    r = requests.get(url, timeout=30)
    if r.status_code != 200 or len(r.text) < 100:
        print(f"    WARNING: Savant expected stats returned {r.status_code}")
        return {}
    # Fix BOM + quoting issues in Savant CSV
    text = r.text.replace('\ufeff', '').replace('"', '')
    reader = csv.DictReader(io.StringIO(text))
    lookup = {}
    for row in reader:
        pid = sint(row.get('player_id'))
        if pid:
            lookup[pid] = row
    print(f"    Got {len(lookup)} players")
    return lookup

def fetch_savant_statcast(player_type, season):
    """Fetch barrel%, hard_hit%, avg EV from Savant statcast leaderboard."""
    print(f"  Fetching Savant statcast metrics ({player_type})...")
    url = (f'https://baseballsavant.mlb.com/leaderboard/statcast'
           f'?type={player_type}&year={season}&position=&team=&min=1&csv=true')
    r = requests.get(url, timeout=30)
    if r.status_code != 200 or len(r.text) < 100:
        print(f"    WARNING: Savant statcast returned {r.status_code}")
        return {}
    text = r.text.replace('\ufeff', '').replace('"', '')
    reader = csv.DictReader(io.StringIO(text))
    lookup = {}
    for row in reader:
        pid = sint(row.get('player_id'))
        if pid:
            lookup[pid] = row
    print(f"    Got {len(lookup)} players")
    return lookup


# ══════════════════════════════════════════════
# STEP 4 — Try FanGraphs as secondary enrichment
# ══════════════════════════════════════════════

def try_fangraphs_pitching(season):
    """Try to fetch FanGraphs advanced pitching metrics. Returns empty dict on failure."""
    print("\n  Trying FanGraphs for advanced pitching metrics...")
    try:
        import pybaseball
        pybaseball.cache.disable()
        df = pybaseball.pitching_stats(season, season, qual=1)
        print(f"    Got {len(df)} pitchers from FanGraphs")
        lookup = {}
        for _, row in df.iterrows():
            mlbam_id = get_mlbam_id(row.get('IDfg'), row.get('Name'))
            if mlbam_id:
                lookup[mlbam_id] = row
        return lookup
    except Exception as e:
        print(f"    FanGraphs unavailable: {e}")
        return {}

def try_fangraphs_batting(season):
    """Try to fetch FanGraphs advanced batting metrics. Returns empty dict on failure."""
    print("\n  Trying FanGraphs for advanced batting metrics...")
    try:
        import pybaseball
        pybaseball.cache.disable()
        df = pybaseball.batting_stats(season, season, qual=1)
        print(f"    Got {len(df)} batters from FanGraphs")
        lookup = {}
        for _, row in df.iterrows():
            mlbam_id = get_mlbam_id(row.get('IDfg'), row.get('Name'))
            if mlbam_id:
                lookup[mlbam_id] = row
        return lookup
    except Exception as e:
        print(f"    FanGraphs unavailable: {e}")
        return {}


# ══════════════════════════════════════════════
# STEP 5 — Calculated metrics
# ══════════════════════════════════════════════

FIP_CONSTANT = 3.10  # approximate; varies by year (~3.05-3.15)
LG_HR_FB_PCT = 0.115  # league average HR/FB rate

def calc_fip(hr, bb, hbp, k, ip):
    """Calculate FIP from counting stats."""
    if not ip or ip <= 0:
        return None
    return ((13 * hr + 3 * (bb + hbp) - 2 * k) / ip) + FIP_CONSTANT

def calc_xfip(air_outs, bb, hbp, k, ip):
    """Calculate approximate xFIP using air outs as FB proxy."""
    if not ip or ip <= 0 or not air_outs:
        return None
    # air_outs ≈ fly balls + pop ups; expected HR = air_outs * lgHR/FB%
    expected_hr = air_outs * LG_HR_FB_PCT
    return ((13 * expected_hr + 3 * (bb + hbp) - 2 * k) / ip) + FIP_CONSTANT


# ══════════════════════════════════════════════
# MAIN — Fetch all sources and merge
# ══════════════════════════════════════════════

# ── Pitching ────────────────────────────────────────────────────────────────

mlb_pitching = fetch_mlb_api_pitching(SEASON)
savant_p_exp = fetch_savant_expected('pitcher', SEASON)
savant_p_sc  = fetch_savant_statcast('pitcher', SEASON)
fg_pitching  = try_fangraphs_pitching(SEASON)

pitcher_rows = []
skipped = 0
fg_enriched = 0

for split in mlb_pitching:
    player = split.get('player', {})
    stat   = split.get('stat', {})
    pid    = sint(player.get('id'))
    name   = player.get('fullName', '')

    if not pid:
        skipped += 1
        continue

    ip  = sfloat(stat.get('inningsPitched'), 0)
    # Skip pitchers with essentially no innings
    if ip < 0.1:
        skipped += 1
        continue

    bf  = sint(stat.get('battersFaced'), 0)
    k   = sint(stat.get('strikeOuts'), 0)
    bb  = sint(stat.get('baseOnBalls'), 0)
    hbp = sint(stat.get('hitBatsmen'), 0) or sint(stat.get('hitByPitch'), 0) or 0
    hr  = sint(stat.get('homeRuns'), 0)
    h   = sint(stat.get('hits'), 0)
    air = sint(stat.get('airOuts'), 0)
    go  = sint(stat.get('groundOuts'), 0)

    # Calculate rates from counting stats
    k_pct  = round(k / bf, 4)  if bf > 0 else None
    bb_pct = round(bb / bf, 4) if bf > 0 else None
    hr9    = round(hr * 9 / ip, 3) if ip > 0 else None
    k9     = sfloat(stat.get('strikeoutsPer9Inn'))
    bb9    = sfloat(stat.get('walksPer9Inn'))

    # Batted ball proxy: GB% and FB% from ground outs / air outs
    total_bip = go + air
    gb_pct = round(go / total_bip, 4) if total_bip > 0 else None
    # air outs includes fly balls + pop ups, so this is an approximation
    fb_pct = round(air / total_bip, 4) if total_bip > 0 else None

    # Calculate FIP and approximate xFIP
    fip  = calc_fip(hr, bb, hbp, k, ip)
    xfip = calc_xfip(air, bb, hbp, k, ip)

    # BABIP: (H - HR) / (BF - K - HR - HBP + SF)
    sf = sint(stat.get('sacFlies'), 0)
    babip_denom = bf - k - hr - hbp + sf
    babip = round((h - hr) / babip_denom, 4) if babip_denom > 0 and h >= hr else None

    row = {
        'player_id':     pid,
        'season':        SEASON,
        'full_name':     name,
        'team':          split.get('team', {}).get('name'),
        'g':             sint(stat.get('gamesPitched')),
        'gs':            sint(stat.get('gamesStarted')),
        'ip':            ip,
        'era':           sfloat(stat.get('era')),
        'fip':           round(fip, 3) if fip is not None else None,
        'xfip':          round(xfip, 3) if xfip is not None else None,
        'k_pct':         k_pct,
        'bb_pct':        bb_pct,
        'k_bb_pct':      round(k_pct - bb_pct, 4) if k_pct is not None and bb_pct is not None else None,
        'hr9':           hr9,
        'babip':         babip,
        'gb_pct':        gb_pct,
        'fb_pct':        fb_pct,
        'whip':          sfloat(stat.get('whip')),
        'k9':            k9,
        'bb9':           bb9,
        'avg':           sfloat(stat.get('avg')),
        'w':             sint(stat.get('wins')),
        'l':             sint(stat.get('losses')),
        # These need FanGraphs or Savant — omitted (not set to None)
        # so upsert won't overwrite values loaded by load_fangraphs_excel.py
    }

    # Enrich with Savant statcast
    sc = savant_p_sc.get(pid)
    if sc:
        row['barrel_pct']  = sfloat(sc.get('brl_percent'))
        row['hard_hit_pct'] = sfloat(sc.get('ev95percent'))

    # Enrich with FanGraphs advanced metrics (overwrite calculated with authoritative)
    fg = fg_pitching.get(pid)
    if fg is not None:
        fg_enriched += 1
        # Only overwrite if FanGraphs has the value
        if fg.get('xFIP') is not None:   row['xfip']          = sfloat(fg.get('xFIP'))
        if fg.get('SIERA') is not None:  row['siera']         = sfloat(fg.get('SIERA'))
        if fg.get('FIP') is not None:    row['fip']           = sfloat(fg.get('FIP'))
        if fg.get('LOB%') is not None:   row['lob_pct']       = sfloat(fg.get('LOB%'))
        if fg.get('GB%') is not None:    row['gb_pct']        = sfloat(fg.get('GB%'))
        if fg.get('FB%') is not None:    row['fb_pct']        = sfloat(fg.get('FB%'))
        if fg.get('LD%') is not None:    row['ld_pct']        = sfloat(fg.get('LD%'))
        if fg.get('SwStr%') is not None: row['swstr_pct']     = sfloat(fg.get('SwStr%'))
        if fg.get('CSW%') is not None:   row['csw_pct']       = sfloat(fg.get('CSW%'))
        if fg.get('Stuff+') is not None: row['stuff_plus']    = sfloat(fg.get('Stuff+'))
        if fg.get('Location+') is not None: row['location_plus'] = sfloat(fg.get('Location+'))
        if fg.get('Pitching+') is not None: row['pitching_plus'] = sfloat(fg.get('Pitching+'))

    pitcher_rows.append(clean_row(row))

print(f"\n  Pitchers matched: {len(pitcher_rows):,} | Skipped: {skipped}")
if fg_pitching:
    print(f"  FanGraphs enriched: {fg_enriched} pitchers")
else:
    print(f"  FanGraphs unavailable — using calculated xFIP/FIP + Savant metrics")

if pitcher_rows:
    upload('pitcher_stats', pitcher_rows)


# ── Batting ─────────────────────────────────────────────────────────────────

mlb_batting  = fetch_mlb_api_batting(SEASON)
savant_b_exp = fetch_savant_expected('batter', SEASON)
savant_b_sc  = fetch_savant_statcast('batter', SEASON)
fg_batting   = try_fangraphs_batting(SEASON)

batter_rows = []
skipped = 0
fg_enriched = 0

for split in mlb_batting:
    player = split.get('player', {})
    stat   = split.get('stat', {})
    pid    = sint(player.get('id'))
    name   = player.get('fullName', '')

    if not pid:
        skipped += 1
        continue

    pa = sint(stat.get('plateAppearances'), 0)
    if pa < 1:
        skipped += 1
        continue

    ab = sint(stat.get('atBats'), 0)
    h  = sint(stat.get('hits'), 0)
    k  = sint(stat.get('strikeOuts'), 0)
    bb = sint(stat.get('baseOnBalls'), 0)

    k_pct  = round(k / pa, 4)  if pa > 0 else None
    bb_pct = round(bb / pa, 4) if pa > 0 else None
    avg    = sfloat(stat.get('avg'))
    slg    = sfloat(stat.get('slg'))
    iso    = round(slg - avg, 4) if slg is not None and avg is not None else None

    row = {
        'player_id':    pid,
        'season':       SEASON,
        'full_name':    name,
        'team':         split.get('team', {}).get('name'),
        'pa':           pa,
        'avg':          avg,
        'obp':          sfloat(stat.get('obp')),
        'slg':          slg,
        'ops':          sfloat(stat.get('ops')),
        'k_pct':        k_pct,
        'bb_pct':       bb_pct,
        'iso':          iso,
        'babip':        sfloat(stat.get('babip')),
        'hr':           sint(stat.get('homeRuns')),
        'r':            sint(stat.get('runs')),
        'rbi':          sint(stat.get('rbi')),
        'sb':           sint(stat.get('stolenBases')),
        # FanGraphs/Savant fields omitted here (not set to None)
        # so upsert won't overwrite values loaded by load_fangraphs_excel.py
    }

    # Enrich with Savant expected stats
    sav = savant_b_exp.get(pid)
    if sav:
        row['xwoba'] = sfloat(sav.get('est_woba'))
        row['woba']  = sfloat(sav.get('woba'))

    # Enrich with Savant statcast
    sc = savant_b_sc.get(pid)
    if sc:
        row['barrel_pct']  = sfloat(sc.get('brl_percent'))
        row['hard_hit_pct'] = sfloat(sc.get('ev95percent'))
        row['avg_ev']       = sfloat(sc.get('avg_hit_speed'))

    # Enrich with FanGraphs advanced metrics
    fg = fg_batting.get(pid)
    if fg is not None:
        fg_enriched += 1
        if fg.get('wRC+') is not None:    row['wrc_plus']    = sfloat(fg.get('wRC+'))
        if fg.get('wOBA') is not None:    row['woba']        = sfloat(fg.get('wOBA'))
        if fg.get('xwOBA') is not None:   row['xwoba']       = sfloat(fg.get('xwOBA'))
        if fg.get('SwStr%') is not None:  row['swstr_pct']   = sfloat(fg.get('SwStr%'))
        if fg.get('O-Swing%') is not None:row['o_swing_pct'] = sfloat(fg.get('O-Swing%'))
        if fg.get('Pull%') is not None:   row['pull_pct']    = sfloat(fg.get('Pull%'))
        if fg.get('Cent%') is not None:   row['cent_pct']    = sfloat(fg.get('Cent%'))
        if fg.get('Oppo%') is not None:   row['oppo_pct']    = sfloat(fg.get('Oppo%'))
        if fg.get('FB%') is not None:     row['fb_pct']      = sfloat(fg.get('FB%'))
        if fg.get('GB%') is not None:     row['gb_pct']      = sfloat(fg.get('GB%'))
        if fg.get('LD%') is not None:     row['ld_pct']      = sfloat(fg.get('LD%'))

    batter_rows.append(clean_row(row))

print(f"\n  Batters matched: {len(batter_rows):,} | Skipped: {skipped}")
if fg_batting:
    print(f"  FanGraphs enriched: {fg_enriched} batters")
else:
    print(f"  FanGraphs unavailable — using Savant xwOBA + calculated rates")

if batter_rows:
    upload('batter_stats', batter_rows)


# ── Summary ─────────────────────────────────────────────────────────────────

print("\nPhase 2 complete.")
print(f"  Batters loaded:  {len(batter_rows):,}")
print(f"  Pitchers loaded: {len(pitcher_rows):,}")
if not fg_pitching and not fg_batting:
    print("  NOTE: FanGraphs was unavailable. Advanced metrics (SIERA, SwStr%, Stuff+, wRC+)")
    print("        will use prior-season values in projections until FG comes back online.")
