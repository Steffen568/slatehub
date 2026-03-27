import sys
sys.stdout.reconfigure(encoding='utf-8', line_buffering=True)
print("load_dk_salaries.py started", flush=True)

import urllib.request
import json
import os
import io
import csv
import time
from datetime import datetime, timezone
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    print("ERROR: Missing credentials.")
    exit()

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
print("Connected to Supabase.")

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
}

from config import SEASON

def fetch_json(url):
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.loads(r.read())

def fetch_dk_csv_positions(dgid):
    """Fetch DK salary CSV for a draft group and return {dk_player_id: position} map.
    The CSV 'Position' column has multi-position eligibility like '2B/SS', '1B/3B'."""
    url = f'https://www.draftkings.com/lineup/getavailableplayerscsv?draftGroupId={dgid}'
    try:
        req = urllib.request.Request(url, headers=HEADERS)
        with urllib.request.urlopen(req, timeout=15) as r:
            content = r.read().decode('utf-8-sig')
        pos_map = {}
        reader = csv.DictReader(io.StringIO(content))
        for row in reader:
            raw_id  = row.get('ID', '').strip()
            raw_pos = row.get('Position', '').strip()
            if raw_id and raw_pos:
                try:
                    pos_map[int(raw_id)] = raw_pos
                except ValueError:
                    pass
        print(f"  CSV positions fetched: {len(pos_map)} players")
        return pos_map
    except Exception as e:
        print(f"  WARNING: Could not fetch CSV positions for DG {dgid}: {e}")
        return {}

# ── STEP 1: Get Classic + Showdown MLB draft groups
print("\nFetching MLB contests from DraftKings...")
data = fetch_json('https://www.draftkings.com/lobby/getcontests?sport=MLB')

contests = data.get('Contests', [])
dg_list  = data.get('DraftGroups', [])

classic_dg_ids  = set()
showdown_dg_ids = set()
for c in contests:
    gt = c.get('gameType', '')
    dg = c.get('dg')
    if not dg:
        continue
    if gt == 'Classic':
        classic_dg_ids.add(dg)
    elif gt == 'Showdown Captain Mode':
        showdown_dg_ids.add(dg)

print(f"  Classic draft group IDs:  {sorted(classic_dg_ids)}")
print(f"  Showdown draft group IDs: {sorted(showdown_dg_ids)}")

# Build DG metadata (classic slate labels + showdown game labels)
dg_meta = {}
for dg in dg_list:
    dgid      = dg.get('DraftGroupId')
    start_est = dg.get('StartDateEst', '')
    game_date = ''
    if start_est:
        try:
            game_date = datetime.fromisoformat(start_est.replace('Z','')).strftime('%Y-%m-%d')
        except Exception:
            pass

    if dgid in classic_dg_ids:
        slate_label = 'main'
        if start_est:
            try:
                dt = datetime.fromisoformat(start_est.replace('Z',''))
                et_hour = dt.hour + dt.minute / 60
                if et_hour < 13:     slate_label = 'early'
                elif et_hour < 17:   slate_label = 'afternoon'
                elif et_hour < 19.5: slate_label = 'main'
                else:                slate_label = 'late'
            except Exception:
                pass
        dg_meta[dgid] = {'slate_label': slate_label, 'contest_type': 'classic'}

    elif dgid in showdown_dg_ids:
        # Team names aren't in DG metadata (Games array is always empty from the lobby API)
        # Slate label will be resolved from draftables in Step 3
        dg_meta[dgid] = {'slate_label': f'sd_{dgid}', 'contest_type': 'showdown', 'game_date': game_date}

# ── STEP 2: Load MLBAM player ID map from Supabase
print("\nLoading player ID map from Supabase...")
all_players = []
offset = 0
while True:
    res = supabase.table('players').select(
        'mlbam_id, name_normalized'
    ).range(offset, offset + 999).execute()
    if not res.data:
        break
    all_players.extend(res.data)
    if len(res.data) < 1000:
        break
    offset += 1000

# ── DK proprietary ID → correct MLBAM ID overrides ──────────────────────────
# DK uses its own internal player IDs for many players — these don't match the
# MLBAM IDs stored in `lineups` (from MLB API). Add any newly discovered
# mismatches here (run diagnose_salary_mismatch.py to find them).
DK_TO_MLBAM = {
    295203 : 467793,   # Carlos Santana
    455056 : 543877,   # Christian Vazquez
    500816 : 571448,   # Nolan Arenado
    503161 : 573262,   # Mike Yastrzemski
    597768 : 605137,   # Josh Bell
    597026 : 606466,   # Ketel Marte
    738605 : 641555,   # J.C. Escarra
    737633 : 641933,   # Tyler O'Neill
    828153 : 650333,   # Luis Arraez
    830274 : 650489,   # Willi Castro
    828304 : 650559,   # Bryan De La Cruz
    830359 : 655316,   # Andruw Monasterio
    874181 : 664040,   # Brandon Lowe
    920245 : 665489,   # Vladimir Guerrero Jr.
    906282 : 665742,   # Juan Soto
    917114 : 665926,   # Andres Gimenez
    665532 : 666126,   # Carlos Cortes
    918435 : 666126,   # Carlos Cortes (alternate slate)
    824481 : 666152,   # David Hamilton
    1118787: 666310,   # Bo Naylor
    1056129: 668885,   # Austin Martin
    1054992: 671218,   # Heliot Ramos
    1071614: 672386,   # Alejandro Kirk
    1053474: 672695,   # Geraldo Perdomo
    915732 : 676391,   # Ernie Clement
    1055204: 676609,   # Jose Caballero
    692025 : 677578,   # Carlos Rodriguez
    543713 : 677594,   # Julio Rodriguez
    1118204: 677594,   # Julio Rodriguez (alternate slate)
    1118821: 677950,   # Alek Thomas
    830104 : 702284,   # Cole Young
    # Added Session 23 — confirmed mismatches from diagnose_salary_mismatch.py
    455591 : 514888,   # Jose Altuve
    598784 : 608070,   # Jose Ramirez
    657963 : 608348,   # Carson Kelly
    830219 : 645277,   # Ozzie Albies
    877503 : 660670,   # Ronald Acuna Jr.
    917940 : 666185,   # Dylan Carlson
    873265 : 669257,   # Will Smith (C)
    1072529: 672356,   # Gabriel Arias
    1169451: 677951,   # Bobby Witt Jr.
    1115762: 686469,   # Vinnie Pasquantino
    1453726: 692216,   # CJ Kayfus
    1396848: 701358,   # Cam Smith (HOU) — was stored as raw DK id
}

# Final remap applied AFTER all ID resolution (name lookup, DK_TO_MLBAM, fallback).
# Catches cases where the players table itself has a wrong/DK-internal ID that the
# name lookup returns, and the lineups table uses the correct MLBAM ID.
# Key = wrong ID that ends up resolved; Value = correct MLBAM ID matching lineups.
PLAYER_ID_REMAP = {
    392995 : 521692,   # Salvador Perez
    455627 : 500743,   # Miguel Rojas
    830219 : 645277,   # Ozzie Albies
    877503 : 660670,   # Ronald Acuna Jr.
    918766 : 666152,   # David Hamilton
    918999 : 669701,   # Josh Smith (TEX)
    1053355: 672580,   # Maikel Garcia
    1169451: 677951,   # Bobby Witt Jr.
    1115762: 686469,   # Vinnie Pasquantino
    548255  : 592663,   # J.T. Realmuto (auto-fixed)
    828445  : 650968,   # Yohel Pozo (auto-fixed)
    919910  : 665487,   # Fernando Tatis Jr. (auto-fixed)
    962605  : 671277,   # Luis Garcia Jr. (auto-fixed)
    1115760 : 686780,   # Pedro Pages (auto-fixed)
    1118063 : 677588,   # Jose Tena (auto-fixed)
    1118963 : 681807,   # David Fry (auto-fixed)
    1120962 : 678246,   # Miguel Vargas (auto-fixed)
    1284664 : 682657,   # Angel Martinez (auto-fixed)
    1316803 : 695578,   # James Wood (auto-fixed)
    1318244 : 696285,   # Jacob Young (auto-fixed)
    1053621 : 671056,   # Ivan Herrera (auto-fixed)
    467793  : 808652,   # Carlos Santana (auto-fixed)
    503373  : 691777,   # Max Muncy (auto-fixed)
    608070  : 681459,   # Jose Ramirez (auto-fixed)
    657863  : 621566,   # Matt Olson (auto-fixed)
    665489  : 115223,   # Vladimir Guerrero Jr. (auto-fixed)
    669257  : 446920,   # Will Smith (auto-fixed)
    672356  : 699087,   # Gabriel Arias (auto-fixed)
    676609  : 691606,   # Jose Caballero (auto-fixed)
    677594  : 451219,   # Julio Rodriguez (auto-fixed)
    828962  : 700951,   # Lane Thomas (auto-fixed)
    872787  : 660821,   # Jesus Sanchez (auto-fixed)
    1316799 : 691777,   # Max Muncy (auto-fixed)
    1396147 : 673784,   # Cole Young (auto-fixed)
    1452073 : 814526,   # Jacob Wilson (auto-fixed)
}

# Build name → mlbam_id lookup AND a set of valid mlbam_ids
import unicodedata, re as _re

def normalize(name):
    if not name:
        return ''
    nfkd = unicodedata.normalize('NFKD', str(name))
    n = nfkd.encode('ASCII', 'ignore').decode('ASCII').strip().lower()
    n = n.replace('.', '').replace("'", '')          # strip dots (J.T. → jt) and apostrophes
    n = _re.sub(r'\s+(jr|sr|ii|iii|iv)\s*$', '', n) # strip suffixes
    return _re.sub(r'\s+', ' ', n).strip()

def drop_middle_initials(name):
    """'bryan r de la cruz' → 'bryan de la cruz'  (removes single-letter middle words only).
    Chadwick stores legal names with middle initials; DK shows display names without them."""
    parts = name.split()
    if len(parts) <= 2:
        return name
    filtered = [parts[0]] + [w for w in parts[1:-1] if len(w) > 1] + [parts[-1]]
    return ' '.join(filtered)

valid_mlbam_ids = set()  # all mlbam_ids in our players table
name_to_mlbam   = {}
ambiguous_names  = set()  # normalized names that map to 2+ different players

for p in all_players:
    if p.get('mlbam_id'):
        valid_mlbam_ids.add(p['mlbam_id'])
    if p.get('name_normalized') and p.get('mlbam_id'):
        mlbam = p['mlbam_id']
        nn          = p['name_normalized']   # e.g. "bryan r. de la cruz"
        nn_stripped = normalize(nn)          # e.g. "bryan r de la cruz"
        nn_no_mi    = drop_middle_initials(nn_stripped)  # e.g. "bryan de la cruz"

        for key in {nn, nn_stripped, nn_no_mi}:
            if not key:
                continue
            if key in name_to_mlbam and name_to_mlbam[key] != mlbam:
                ambiguous_names.add(key)     # collision — two players share this key
            else:
                name_to_mlbam[key] = mlbam

# Remove ambiguous entries — Priority 1 (DK playerId) must resolve these
for key in ambiguous_names:
    name_to_mlbam.pop(key, None)

print(f"  Loaded {len(name_to_mlbam):,} player name mappings ({len(valid_mlbam_ids):,} unique IDs)")
if ambiguous_names:
    print(f"  ⚠ {len(ambiguous_names)} ambiguous name(s) excluded (duplicate players — DK playerId required): {sorted(ambiguous_names)[:8]}")

# ── STEP 3: Fetch draftables for Classic + Showdown DGs
print("\nFetching salaries from DraftKings...")

all_salary_rows = []
seen_player_dg  = {}   # (dk_player_id, dgid) → row dict (merge positions from duplicate entries)

# DK position sort order (matches DK convention)
DK_POS_ORDER = {'C':0, '1B':1, '2B':2, '3B':3, 'SS':4, 'OF':5, 'SP':6, 'RP':7, 'UTIL':8}
def merge_positions(existing_pos, new_pos):
    """Merge two position strings (e.g. '3B' + '2B' → '2B/3B') preserving DK order."""
    parts = set(existing_pos.split('/')) | set(new_pos.split('/'))
    parts.discard('')
    return '/'.join(sorted(parts, key=lambda p: DK_POS_ORDER.get(p, 99))) or 'UTIL'

all_dg_ids = sorted(classic_dg_ids | showdown_dg_ids)
all_slate_game_rows = []   # for dk_slate_games table
dg_earliest_start = {}     # dgid → earliest competition startTime (UTC datetime)

for dgid in all_dg_ids:
    meta         = dg_meta.get(dgid, {})
    slate_label  = meta.get('slate_label', 'main')
    contest_type = meta.get('contest_type', 'classic')
    is_showdown  = contest_type == 'showdown'

    try:
        url = f'https://api.draftkings.com/draftgroups/v1/draftgroups/{dgid}/draftables'
        result = fetch_json(url)
        draftables = result.get('draftables', [])

        # Extract unique competitions (games) for dk_slate_games table
        seen_comps = {}
        for p in draftables:
            comp = p.get('competition', {})
            comp_id = comp.get('competitionId')
            if not comp_id or comp_id in seen_comps:
                continue
            comp_name = comp.get('name', '')  # e.g. "DET @ SD"
            parts = comp_name.split(' @ ')
            if len(parts) == 2:
                seen_comps[comp_id] = {
                    'dg_id':          dgid,
                    'competition_id': comp_id,
                    'dk_slate':       None,  # filled after slate_label resolved
                    'away_team':      parts[0].strip(),
                    'home_team':      parts[1].strip(),
                    'start_time':     comp.get('startTime', ''),
                    'season':         SEASON,
                }

        # Fetch CSV for this DG to get position info
        # For Showdown: CSV position column is 'CPT' or the real position ('SP','OF', etc.)
        csv_pos_map = fetch_dk_csv_positions(dgid)

        # Games array is always empty from DG metadata — infer team names from draftables instead
        if is_showdown:
            teams = sorted({p.get('teamAbbreviation','') for p in draftables if p.get('teamAbbreviation')})
            game_date_str = dg_meta[dgid].get('game_date', '')
            if len(teams) >= 2:
                slate_label = f'sd_{teams[0]}@{teams[1]}_{game_date_str}' if game_date_str else f'sd_{teams[0]}@{teams[1]}'
            else:
                slate_label = f'sd_{dgid}_{game_date_str}' if game_date_str else f'sd_{dgid}'
            dg_meta[dgid]['slate_label'] = slate_label

            # Showdown: each player appears TWICE (CPT at 1.5× salary, FLEX at base salary).
            # position='SP'/'OF'/etc for BOTH — not 'CPT'. Distinguish by rosterSlotId or salary.
            # Keep only FLEX (lower salary) entry per playerDkId.
            flex_draftable_ids = set()
            by_player = {}
            for p in draftables:
                pid  = p.get('playerDkId')
                sal  = p.get('salary', 0)
                did  = p.get('draftableId')
                if pid not in by_player or sal < by_player[pid][0]:
                    by_player[pid] = (sal, did)
            flex_draftable_ids = {did for (sal, did) in by_player.values()}
            cpt_skipped = len(draftables) - len(flex_draftable_ids)
        else:
            flex_draftable_ids = None
            cpt_skipped = 0

        count = 0
        name_hit = 0
        dk_fallback = 0
        no_match = 0

        for p in draftables:
            dk_player_id = p.get('playerDkId')
            mlbam_id     = p.get('playerId')
            name         = p.get('displayName', '')
            draftable_id = p.get('draftableId')
            csv_pos      = csv_pos_map.get(draftable_id, '')
            api_pos      = p.get('position', '')
            salary       = p.get('salary', 0)
            team         = p.get('teamAbbreviation', '')

            # Showdown: skip CPT entries — only keep the FLEX (lower salary) draftable per player
            if is_showdown and draftable_id not in flex_draftable_ids:
                continue

            position = csv_pos or api_pos

            # Multi-position merge: DK lists multi-eligible players as separate draftable
            # entries (same playerDkId, different positions). Merge instead of skipping.
            key = (dk_player_id, dgid)
            if key in seen_player_dg:
                if position:
                    existing_row = seen_player_dg[key]
                    existing_row['position'] = merge_positions(existing_row['position'], position)
                continue

            # Resolve to MLBAM ID — priority order:
            # 1. Hardcoded DK→MLBAM override
            # 2. Unambiguous name match → MLBAM from Chadwick
            # 3. Middle-initial-stripped name match
            # 4. DK playerId confirmed in our players table
            # 5. Raw DK playerId fallback
            norm       = normalize(name)
            norm_no_mi = drop_middle_initials(norm)

            if mlbam_id and mlbam_id in DK_TO_MLBAM:
                player_id = DK_TO_MLBAM[mlbam_id]
                dk_fallback += 1
            elif name_to_mlbam.get(norm):
                player_id = name_to_mlbam[norm]
                name_hit += 1
            elif norm_no_mi != norm and name_to_mlbam.get(norm_no_mi):
                player_id = name_to_mlbam[norm_no_mi]
                name_hit += 1
            elif mlbam_id and mlbam_id in valid_mlbam_ids:
                player_id = mlbam_id
                dk_fallback += 1
            elif mlbam_id:
                player_id = mlbam_id
                dk_fallback += 1
                print(f"    ⚠ No match for '{name}' (DK id {mlbam_id}) — stored raw DK id, add to DK_TO_MLBAM if broken")
            else:
                no_match += 1
                print(f"    ✗ No match at all for '{name}' — skipped")
                continue

            # Final remap: catches wrong IDs sourced from our players table or DK fallback
            player_id = PLAYER_ID_REMAP.get(player_id, player_id)

            row = {
                'player_id':    player_id,
                'season':       SEASON,
                'dk_player_id': dk_player_id,
                'name':         name,
                'position':     position,
                'salary':       salary,
                'team':         team,
                'dk_slate':     slate_label,
                'contest_type': contest_type,
                'dg_id':        dgid,
            }
            all_salary_rows.append(row)
            seen_player_dg[key] = row   # store reference for position merging
            count += 1

        # Track earliest competition start time per DG (for in-progress guard)
        for comp_id, game_info in seen_comps.items():
            st = game_info.get('start_time', '')
            if st:
                try:
                    raw = st.replace('Z', '+00:00') if st.endswith('Z') else st
                    if '+' not in raw and not raw.endswith('+00:00'):
                        raw = raw + '+00:00'
                    t = datetime.fromisoformat(raw)
                    if dgid not in dg_earliest_start or t < dg_earliest_start[dgid]:
                        dg_earliest_start[dgid] = t
                except Exception:
                    pass

        # Attach slate label to competition rows and collect
        for comp_id, game_row in seen_comps.items():
            game_row['dk_slate'] = slate_label
            all_slate_game_rows.append(game_row)

        extra = f' | cpt_skipped: {cpt_skipped}' if is_showdown else ''
        print(f"  DG {dgid} [{contest_type}] ({slate_label}): {count} players, {len(seen_comps)} games | name_matched: {name_hit} | dk_id_fallback: {dk_fallback} | no_match: {no_match}{extra}")
        time.sleep(0.3)

    except Exception as e:
        print(f"  ERROR fetching DG {dgid}: {e}")

print(f"\nTotal salary rows (pre-dedup): {len(all_salary_rows)}")

# Deduplicate by (player_id, dg_id) — merge positions from duplicate entries.
# A player can appear twice in the same DG when DK lists them under multiple positions.
seen = {}
for r in all_salary_rows:
    key = (r['player_id'], r['dg_id'])
    if key in seen:
        seen[key]['position'] = merge_positions(seen[key]['position'], r['position'])
    else:
        seen[key] = r
all_salary_rows = list(seen.values())
print(f"Total salary rows (post-dedup): {len(all_salary_rows)}")

# ── STEP 4: Upload to Supabase
if all_salary_rows:
    # Only delete rows for DG IDs that appeared in the current API response.
    # Locked DGs (no longer in the API) keep their existing data untouched.
    # Guard: skip delete for DGs whose earliest game has already started — upsert only.
    season = all_salary_rows[0]['season']
    api_dg_ids = sorted(all_dg_ids)
    now_utc = datetime.now(timezone.utc)
    started_dg_ids = {dgid for dgid, t in dg_earliest_start.items() if t <= now_utc}
    safe_dg_ids = [dgid for dgid in api_dg_ids if dgid not in started_dg_ids]

    if started_dg_ids:
        print(f"  ⚠ {len(started_dg_ids)} DG(s) have in-progress games — skipping delete (upsert only): {sorted(started_dg_ids)}")

    print(f"Clearing dk_salaries for {len(safe_dg_ids)} of {len(api_dg_ids)} active DG IDs (season {season})...")
    for dgid in safe_dg_ids:
        supabase.table('dk_salaries').delete().eq('dg_id', dgid).eq('season', season).execute()
    print("Cleared. Uploading fresh data...")
    batch_size = 500
    uploaded = 0
    for i in range(0, len(all_salary_rows), batch_size):
        batch = all_salary_rows[i:i+batch_size]
        supabase.table('dk_salaries').upsert(batch, on_conflict='player_id,dg_id').execute()
        uploaded += len(batch)
        print(f"  {uploaded}/{len(all_salary_rows)}")
    print("Done.")

    # Upsert dk_slate_games (game-to-slate mapping)
    if all_slate_game_rows:
        print(f"\nUpserting {len(all_slate_game_rows)} slate-game mappings...")
        for dgid in safe_dg_ids:
            supabase.table('dk_slate_games').delete().eq('dg_id', dgid).eq('season', season).execute()
        for i in range(0, len(all_slate_game_rows), 500):
            batch = all_slate_game_rows[i:i+500]
            supabase.table('dk_slate_games').upsert(batch, on_conflict='dg_id,competition_id').execute()
        print(f"  Uploaded {len(all_slate_game_rows)} slate-game rows.")

    # Sanity check
    print("\nSanity check — top 5 salaries:")
    top = sorted(all_salary_rows, key=lambda x: x['salary'], reverse=True)[:5]
    for p in top:
        print(f"  {p['name']} ({p['team']}) {p['position']}: ${p['salary']:,} [{p['dk_slate']}]")
    multi = [p for p in all_salary_rows if '/' in p['position']]
    print(f"\nMulti-position players uploaded: {len(multi)}")
    for p in multi[:5]:
        print(f"  {p['name']} ({p['team']}) {p['position']}: ${p['salary']:,}")

print("\nload_dk_salaries.py complete.")