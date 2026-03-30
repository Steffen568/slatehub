"""Generate a mock DraftKings entries CSV for testing late-swap import.

Creates a realistic DK-format CSV with:
- 20 entries with valid 10-player classic lineups
- Full player pool section with all players from the main slate
- Real player names, salaries, positions, teams from dk_salaries

Usage:  py -3.12 generate_mock_dk_csv.py
Output: mock_dk_entries.csv
"""
import sys
sys.stdout.reconfigure(encoding='utf-8', line_buffering=True)

import os
import random
from datetime import datetime
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()
sb = create_client(os.getenv('SUPABASE_URL'), os.getenv('SUPABASE_KEY'))

# ── Load main slate salary data ──────────────────────────────────────────────
print("Loading main slate salaries...")
all_sals = []
offset = 0
while True:
    res = sb.table('dk_salaries').select(
        'name,team,position,salary,player_id,dk_player_id,dg_id'
    ).eq('dk_slate', 'main').eq('contest_type', 'classic').range(offset, offset + 999).execute()
    if not res.data:
        break
    all_sals.extend(res.data)
    if len(res.data) < 1000:
        break
    offset += 1000

print(f"  Loaded {len(all_sals)} players")

# ── Load games for today ─────────────────────────────────────────────────────
print("Loading games...")
games = sb.table('games').select(
    'game_pk,home_team,away_team,home_team_id,away_team_id,game_time_utc'
).eq('game_date', '2026-03-27').execute()
game_data = games.data or []

# Build team → game info string (e.g. "NYY@SF 03:35PM ET")
team_game_info = {}
for g in game_data:
    from config import SEASON  # noqa
    # Parse UTC time → ET display
    try:
        utc = datetime.fromisoformat(g['game_time_utc'].replace('Z', '+00:00'))
        # Approximate ET = UTC - 4 (EDT)
        et_hour = (utc.hour - 4) % 24
        et_min = utc.minute
        ampm = 'AM' if et_hour < 12 else 'PM'
        display_hour = et_hour % 12 or 12
        time_str = f"{display_hour:02d}:{et_min:02d}{ampm} ET"
    except Exception:
        time_str = ""

    # Map full team names to abbreviations
    NAME_TO_ABBR = {
        'New York Yankees':'NYY','Boston Red Sox':'BOS','Tampa Bay Rays':'TB',
        'Toronto Blue Jays':'TOR','Baltimore Orioles':'BAL','Chicago White Sox':'CWS',
        'Cleveland Guardians':'CLE','Detroit Tigers':'DET','Kansas City Royals':'KC',
        'Minnesota Twins':'MIN','Houston Astros':'HOU','Los Angeles Angels':'LAA',
        'Oakland Athletics':'ATH','Athletics':'ATH','Seattle Mariners':'SEA',
        'Texas Rangers':'TEX','Atlanta Braves':'ATL','Miami Marlins':'MIA',
        'New York Mets':'NYM','Philadelphia Phillies':'PHI','Washington Nationals':'WSH',
        'Chicago Cubs':'CHC','Cincinnati Reds':'CIN','Milwaukee Brewers':'MIL',
        'Pittsburgh Pirates':'PIT','St. Louis Cardinals':'STL',
        'Arizona Diamondbacks':'ARI','Colorado Rockies':'COL',
        'Los Angeles Dodgers':'LAD','San Diego Padres':'SD','San Francisco Giants':'SF',
    }
    away_ab = NAME_TO_ABBR.get(g['away_team'], g['away_team'][:3].upper())
    home_ab = NAME_TO_ABBR.get(g['home_team'], g['home_team'][:3].upper())
    game_info = f"{away_ab}@{home_ab} {time_str}"
    team_game_info[away_ab] = game_info
    team_game_info[home_ab] = game_info

# ── Categorize players by position ───────────────────────────────────────────
# DK slots: SP, SP, C, 1B, 2B, 3B, SS, OF, OF, OF
sp_pool = []
c_pool = []
b1_pool = []
b2_pool = []
b3_pool = []
ss_pool = []
of_pool = []

for s in all_sals:
    pos = (s.get('position') or '').upper()
    positions = [p.strip() for p in pos.split('/')]

    if 'SP' in positions:
        sp_pool.append(s)
    if 'C' in positions:
        c_pool.append(s)
    if '1B' in positions:
        b1_pool.append(s)
    if '2B' in positions:
        b2_pool.append(s)
    if '3B' in positions:
        b3_pool.append(s)
    if 'SS' in positions:
        ss_pool.append(s)
    if 'OF' in positions:
        of_pool.append(s)

print(f"  SP:{len(sp_pool)} C:{len(c_pool)} 1B:{len(b1_pool)} 2B:{len(b2_pool)} "
      f"3B:{len(b3_pool)} SS:{len(ss_pool)} OF:{len(of_pool)}")

# Filter to only starters (top salary SPs are likely starters)
sp_pool.sort(key=lambda x: x['salary'], reverse=True)
sp_starters = sp_pool[:16]  # top 16 SPs by salary ≈ starters

# ── Generate draftable IDs (DK uses unique IDs per player) ───────────────────
# Use dk_player_id as base, add offset for uniqueness
draftable_map = {}  # player name → draftable ID
next_id = 20000001
for s in all_sals:
    name = s['name']
    if name not in draftable_map:
        draftable_map[name] = str(next_id)
        next_id += 1

# ── Build valid lineups ──────────────────────────────────────────────────────
def build_lineup():
    """Build a random valid classic lineup under $50,000 salary cap."""
    for _ in range(200):  # retry up to 200 times
        try:
            picks = {
                'sp1': random.choice(sp_starters),
                'sp2': random.choice(sp_starters),
                'c':   random.choice(c_pool),
                '1b':  random.choice(b1_pool),
                '2b':  random.choice(b2_pool),
                '3b':  random.choice(b3_pool),
                'ss':  random.choice(ss_pool),
                'of1': random.choice(of_pool),
                'of2': random.choice(of_pool),
                'of3': random.choice(of_pool),
            }

            # No duplicate players
            names = [p['name'] for p in picks.values()]
            if len(set(names)) != 10:
                continue

            # Under salary cap
            total = sum(p['salary'] for p in picks.values())
            if total > 50000:
                continue

            return picks
        except Exception:
            continue
    return None

print("\nGenerating 20 lineups...")
lineups = []
for i in range(20):
    lu = build_lineup()
    if lu:
        lineups.append(lu)
    else:
        print(f"  WARNING: Could not build lineup {i+1}")

print(f"  Built {len(lineups)} valid lineups")

# ── Write CSV ─────────────────────────────────────────────────────────────────
# DK CSV format:
# Header: Entry ID,Contest Name,Contest ID,Entry Fee,SP,SP,C,1B,2B,3B,SS,OF,OF,OF,,Roster Position,Name + ID,Name,ID,Position,Salary,Game Info,TeamAbbrev,AvgPointsPerGame
# Entry rows: entryId,"contestName",contestId,$fee,Player1 (id1),...Player10 (id10),,pos,"Name (id)",Name,id,Pos,Salary,GameInfo,Team,AvgPts

HEADER = 'Entry ID,Contest Name,Contest ID,Entry Fee,SP,SP,C,1B,2B,3B,SS,OF,OF,OF,,Roster Position,Name + ID,Name,ID,Position,Salary,Game Info,TeamAbbrev,AvgPointsPerGame'
SLOT_ORDER = ['sp1', 'sp2', 'c', '1b', '2b', '3b', 'ss', 'of1', 'of2', 'of3']

# Build pool rows (one per player, all players in the slate)
pool_rows = []
for s in all_sals:
    name = s['name']
    did = draftable_map.get(name, '0')
    pos = s.get('position', 'UTIL')
    salary = s.get('salary', 0)
    team = s.get('team', '')
    game_info = team_game_info.get(team, '')
    avg_pts = round(random.uniform(3.0, 12.0), 1)  # mock avg points

    # Roster position = first position
    roster_pos = pos.split('/')[0] if pos else 'UTIL'

    pool_rows.append({
        'roster_pos': roster_pos,
        'name_id': f"{name} ({did})",
        'name': name,
        'id': did,
        'position': pos,
        'salary': str(salary),
        'game_info': game_info,
        'team': team,
        'avg_pts': str(avg_pts),
    })

print(f"\nPool section: {len(pool_rows)} players")

# Merge entry rows and pool rows side by side
lines = [HEADER]

contest_id = '987654321'
contest_name = 'MLB $500K Moonshot'
entry_fee = '$20.00'

max_rows = max(len(lineups), len(pool_rows))

for i in range(max_rows):
    # Entry section (cols 0-13)
    if i < len(lineups):
        entry_id = str(10000000 + i)
        lu = lineups[i]
        player_cells = []
        for slot in SLOT_ORDER:
            p = lu[slot]
            did = draftable_map.get(p['name'], '0')
            player_cells.append(f"{p['name']} ({did})")
        entry_part = f'{entry_id},"{contest_name}",{contest_id},{entry_fee},' + ','.join(player_cells)
    else:
        # Empty entry section (14 empty cols: id, name, contestid, fee, 10 slots)
        entry_part = ',,,,,,,,,,,,,'

    # Separator column (col 14)
    sep = ''

    # Pool section (cols 15-23)
    if i < len(pool_rows):
        pr = pool_rows[i]
        pool_part = f'{pr["roster_pos"]},{pr["name_id"]},{pr["name"]},{pr["id"]},{pr["position"]},{pr["salary"]},{pr["game_info"]},{pr["team"]},{pr["avg_pts"]}'
    else:
        pool_part = ',,,,,,,,,'

    lines.append(f'{entry_part},{sep},{pool_part}')

output_path = 'mock_dk_entries.csv'
with open(output_path, 'w', encoding='utf-8') as f:
    f.write('\n'.join(lines))

print(f"\n✓ Written {output_path}")
print(f"  {len(lineups)} entries × 10 players each")
print(f"  {len(pool_rows)} players in pool section")
print(f"\nSample entry lineup:")
if lineups:
    lu = lineups[0]
    total_sal = 0
    for slot in SLOT_ORDER:
        p = lu[slot]
        total_sal += p['salary']
        print(f"  {slot:4s}: {p['name']:25s} ({p['team']}) ${p['salary']:,}  [{p['position']}]")
    print(f"  Total salary: ${total_sal:,}")

# Show which teams are in early vs late games (for lock testing)
print("\nGame times (for testing locks):")
for g in sorted(game_data, key=lambda x: x['game_time_utc']):
    away_ab = NAME_TO_ABBR.get(g['away_team'], g['away_team'][:3].upper())
    home_ab = NAME_TO_ABBR.get(g['home_team'], g['home_team'][:3].upper())
    print(f"  {away_ab}@{home_ab} — {g['game_time_utc']}")
print("\nTo test locks: temporarily set the earliest game's game_time_utc to a past time in Supabase,")
print("then import the CSV and verify those players show as locked.")
