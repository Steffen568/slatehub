import sys
sys.stdout.reconfigure(encoding='utf-8', line_buffering=True)
print("load_dk_salaries.py started", flush=True)

import urllib.request
import json
import os
import io
import csv
import time
from datetime import datetime
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

# ── STEP 1: Get Classic MLB draft groups
print("\nFetching MLB contests from DraftKings...")
data = fetch_json('https://www.draftkings.com/lobby/getcontests?sport=MLB')

contests = data.get('Contests', [])
dg_list  = data.get('DraftGroups', [])

classic_dg_ids = set()
for c in contests:
    if c.get('gameType') == 'Classic' and c.get('dg'):
        classic_dg_ids.add(c['dg'])

print(f"  Classic draft group IDs: {sorted(classic_dg_ids)}")

# Build DG metadata
dg_meta = {}
for dg in dg_list:
    dgid = dg.get('DraftGroupId')
    if dgid in classic_dg_ids:
        start_est = dg.get('StartDateEst', '')
        slate_label = 'main'
        if start_est:
            try:
                dt = datetime.fromisoformat(start_est.replace('Z',''))
                et_hour = dt.hour + dt.minute / 60
                if et_hour < 13:    slate_label = 'early'
                elif et_hour < 17:  slate_label = 'afternoon'
                elif et_hour < 19.5: slate_label = 'main'
                else:               slate_label = 'late'
            except Exception:
                pass
        dg_meta[dgid] = {'slate_label': slate_label}

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

# ── STEP 3: Fetch draftables for each Classic DG and collect salaries
print("\nFetching salaries from DraftKings...")

all_salary_rows = []
seen_player_dg  = set()

for dgid in sorted(classic_dg_ids):
    slate_label = dg_meta.get(dgid, {}).get('slate_label', 'main')
    try:
        url = f'https://api.draftkings.com/draftgroups/v1/draftgroups/{dgid}/draftables'
        result = fetch_json(url)
        draftables = result.get('draftables', [])

        # Fetch CSV for this DG to get multi-position eligibility (e.g. "2B/SS", "1B/3B")
        csv_pos_map = fetch_dk_csv_positions(dgid)

        count = 0
        name_hit = 0
        dk_fallback = 0
        no_match = 0
        for p in draftables:
            dk_player_id = p.get('playerDkId')
            mlbam_id     = p.get('playerId')
            name         = p.get('displayName', '')
            draftable_id = p.get('draftableId')
            position     = csv_pos_map.get(draftable_id) or p.get('position', '')
            salary       = p.get('salary', 0)
            team         = p.get('teamAbbreviation', '')

            # Skip if already seen this player in this DG
            key = (dk_player_id, dgid)
            if key in seen_player_dg:
                continue
            seen_player_dg.add(key)

            # Resolve to MLBAM ID — priority order:
            # 1. Hardcoded DK→MLBAM override (catches known DK proprietary IDs)
            # 2. Unambiguous name match → MLBAM from Chadwick (same source as lineups)
            # 3. Middle-initial-stripped name match (Chadwick "Bryan R. De La Cruz" → DK "Bryan De La Cruz")
            # 4. DK playerId confirmed in our players table (DK ID happens to equal MLBAM)
            # 5. Raw DK playerId fallback (stored with warning — add to DK_TO_MLBAM when found)
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
                player_id = mlbam_id  # raw DK id — add to DK_TO_MLBAM if salary doesn't show
                dk_fallback += 1
                print(f"    ⚠ No match for '{name}' (DK id {mlbam_id}) — stored raw DK id, add to DK_TO_MLBAM if broken")
            else:
                no_match += 1
                print(f"    ✗ No match at all for '{name}' — skipped")
                continue

            all_salary_rows.append({
                'player_id':    player_id,
                'season':       SEASON,
                'dk_player_id': dk_player_id,
                'name':         name,
                'position':     position,
                'salary':       salary,
                'team':         team,
                'dk_slate':     slate_label,
                'dg_id':        dgid,
            })
            count += 1

        print(f"  DG {dgid} ({slate_label}): {count} players | name_matched: {name_hit} | dk_id_fallback: {dk_fallback} | no_match: {no_match}")
        time.sleep(0.3)

    except Exception as e:
        print(f"  ERROR fetching DG {dgid}: {e}")

print(f"\nTotal salary rows (pre-dedup): {len(all_salary_rows)}")

# Deduplicate by (player_id, dg_id) — keep last occurrence.
# A player can appear twice in the same DG when DK lists them under multiple positions.
seen = {}
for r in all_salary_rows:
    seen[(r['player_id'], r['dg_id'])] = r
all_salary_rows = list(seen.values())
print(f"Total salary rows (post-dedup): {len(all_salary_rows)}")

# ── STEP 4: Upload to Supabase
if all_salary_rows:
    # Delete existing rows for these slate labels + season before inserting fresh data
    # This removes stale rows with wrong player_ids from previous loads
    slate_labels = list({r['dk_slate'] for r in all_salary_rows})
    season = all_salary_rows[0]['season']
    print(f"Clearing old dk_salaries rows for slates {slate_labels} season {season}...")
    for sl in slate_labels:
        supabase.table('dk_salaries').delete().eq('dk_slate', sl).eq('season', season).execute()
    print("Cleared. Uploading fresh data...")
    batch_size = 500
    uploaded = 0
    for i in range(0, len(all_salary_rows), batch_size):
        batch = all_salary_rows[i:i+batch_size]
        supabase.table('dk_salaries').upsert(batch, on_conflict='player_id,dg_id').execute()
        uploaded += len(batch)
        print(f"  {uploaded}/{len(all_salary_rows)}")
    print("Done.")

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