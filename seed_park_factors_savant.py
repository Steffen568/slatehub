"""
seed_park_factors_savant.py
Seeds park factors from Baseball Savant 3-year (2023-2025) park factor leaderboard.

Source: baseballsavant.mlb.com/leaderboard/statcast-parks (3-yr, all batters, R games)
Column order from Savant: Park Factor, wOBAcon, xwOBAcon, BACON, xBACON, HardHit, R, OBP, H, 1B, 2B, 3B, HR, BB, SO

SQL — run once in Supabase before executing this script:
    ALTER TABLE park_factors ADD COLUMN IF NOT EXISTS woba_con        INTEGER;
    ALTER TABLE park_factors ADD COLUMN IF NOT EXISTS xwoba_con       INTEGER;
    ALTER TABLE park_factors ADD COLUMN IF NOT EXISTS bacon           INTEGER;
    ALTER TABLE park_factors ADD COLUMN IF NOT EXISTS xbacon          INTEGER;
    ALTER TABLE park_factors ADD COLUMN IF NOT EXISTS hard_hit_factor INTEGER;
    ALTER TABLE park_factors ADD COLUMN IF NOT EXISTS obp_factor      INTEGER;
    ALTER TABLE park_factors ADD COLUMN IF NOT EXISTS h_factor        INTEGER;
    ALTER TABLE park_factors ADD COLUMN IF NOT EXISTS bb_factor       INTEGER;
    ALTER TABLE park_factors ADD COLUMN IF NOT EXISTS k_factor        INTEGER;

Note: Athletics (venue_id=3) and Rays (venue_id=22) not in Savant dataset — not updated.
"""

import sys, os
sys.stdout.reconfigure(encoding='utf-8', line_buffering=True)
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()
sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])

SEASON = 2026  # Labels the 2024-2026 3-year dataset

# Column order: venue_id, venue_name, team_name,
#   basic, woba_con, xwoba_con, bacon, xbacon, hard_hit,
#   runs, obp, h, singles, doubles, triples, hr, bb, k
# Source: baseballsavant.mlb.com/leaderboard/statcast-park-factors
#         year=2026, rolling=3yr, condition=All, batSide=All
# Note: woba_con/xwoba_con/bacon/xbacon/hard_hit/obp carried from 2025 (not in 2026 fetch).
#       Key production factors (basic/runs/h/1B/2B/3B/hr/bb/k) updated to 2026 values.
SAVANT_PARK_FACTORS = [
    (25, 'Coors Field',                 'Colorado Rockies',      112, 112, 101, 113, 102, 101, 125, 111, 117, 117, 120, 194, 107,  99,  91),
    (23, 'Fenway Park',                 'Boston Red Sox',        102, 105, 100, 106, 100, 101, 104, 105, 104, 105, 119,  84,  84,  98,  98),
    ( 9, 'Chase Field',                 'Arizona Diamondbacks',  105, 102, 100, 102, 101, 102, 110, 103, 106, 103, 119, 207,  93,  99,  91),
    (24, 'Great American Ball Park',    'Cincinnati Reds',       103, 103,  98, 101,  99,  94, 106, 101, 100,  96, 103,  68, 122, 107, 102),
    (28, 'Target Field',                'Minnesota Twins',       104, 104, 101, 104, 100, 101, 108, 102, 104, 104, 112,  81,  97,  99,  97),
    (14, 'loanDepot park',              'Miami Marlins',         100, 100, 101, 102, 101, 100, 100, 101, 101, 102, 106, 132,  88,  99,  97),
    (26, 'Kauffman Stadium',            'Kansas City Royals',    100,  98, 102,  99, 102, 103, 100, 102, 103, 100, 117, 183,  83,  99,  91),
    (16, 'Nationals Park',              'Washington Nationals',  101,  99, 101, 100, 101, 101, 102, 102, 103, 106,  98,  99,  97,  95,  94),
    (19, 'Citizens Bank Park',          'Philadelphia Phillies', 102, 103,  99, 101,  99, 100, 104,  99, 102, 102,  95,  98, 114,  96, 103),
    (10, 'Dodger Stadium',              'Los Angeles Dodgers',   102, 101, 102,  98, 101, 101, 104,  99,  98,  93,  94,  69, 129, 104, 100),
    ( 1, 'Angel Stadium',               'Los Angeles Angels',    100, 102, 100, 101,  99,  99, 100, 100,  98,  98,  92,  91, 109, 100, 105),
    ( 5, 'Truist Park',                 'Atlanta Braves',        100, 103, 103, 103, 103, 103, 100, 100, 102, 106,  95,  94,  95, 100, 105),
    ( 7, 'Busch Stadium',               'St. Louis Cardinals',    98,  97, 101,  99, 101, 102,  96, 101, 103, 107, 107,  77,  80,  93,  90),
    (17, 'Oriole Park at Camden Yards', 'Baltimore Orioles',     104, 101, 102, 101, 101, 105, 108,  99, 106, 105, 102, 128, 113,  92,  99),
    (27, 'Comerica Park',               'Detroit Tigers',        101, 100, 100, 100, 100,  99, 102, 100, 101, 101,  93, 151, 103,  98,  98),
    ( 2, 'Daikin Park',                 'Houston Astros',        101, 101,  98, 100,  99,  98, 102, 100,  99,  98,  96,  69, 115, 101, 106),
    ( 4, 'Rogers Centre',               'Toronto Blue Jays',     101,  99,  99,  98,  99, 100, 102, 100, 101,  99, 105,  74, 110,  98,  97),
    (30, 'Yankee Stadium',              'New York Yankees',      102,  99, 102,  97, 100, 104, 104,  99,  95,  91,  92,  72, 118, 119, 101),
    (20, 'PNC Park',                    'Pittsburgh Pirates',    100,  97, 101, 100, 101, 102, 100, 101, 102, 102, 118,  78,  79, 100,  97),
    (29, 'Guaranteed Rate Field',       'Chicago White Sox',      98,  98,  98,  99,  99,  97,  96, 100,  97,  99,  93,  76,  94, 106,  97),
    (15, 'Citi Field',                  'New York Mets',          99,  97, 100,  96,  99, 101,  98,  99,  95,  95,  93,  81, 102, 108, 103),
    ( 6, 'American Family Field',       'Milwaukee Brewers',      97,  99,  98,  98,  99,  96,  94,  97,  95,  95,  86,  93, 106, 104, 109),
    (12, 'Progressive Field',           'Cleveland Guardians',    98,  96,  98,  98, 100,  96,  96,  98,  97,  96, 103,  51,  95, 102, 105),
    (11, 'Oracle Park',                 'San Francisco Giants',   98,  96,  97,  99,  98,  99,  96,  97, 101, 103, 108, 139,  78,  93,  97),
    (18, 'Petco Park',                  'San Diego Padres',       97,  97,  99,  97,  99,  98,  94,  98,  96,  97,  89,  70, 108, 100, 102),
    ( 8, 'Wrigley Field',               'Chicago Cubs',           95,  97, 100,  97, 100, 100,  90,  97,  94,  96,  80, 119,  98, 101, 103),
    (21, 'Globe Life Field',            'Texas Rangers',           92,  97, 100,  96,  98, 102,  85,  97,  93,  95,  89,  79,  89,  95, 103),
    (13, 'T-Mobile Park',               'Seattle Mariners',        92,  94,  99,  94,  99,  98,  85,  92,  90,  90,  91,  41,  96,  96, 117),
    (22, 'Tropicana Field',             'Tampa Bay Rays',          95, 100, 100, 100, 100, 100,  90,  95,  96,  98,  85, 124,  96,  95, 104),
]

rows = []
for (vid, vname, tname,
     basic, woba_con, xwoba_con, bacon, xbacon, hard_hit,
     runs, obp, h, singles, doubles, triples, hr, bb, k) in SAVANT_PARK_FACTORS:
    rows.append({
        'venue_id':         vid,
        'venue_name':       vname,
        'team_name':        tname,
        'season':           SEASON,
        'data_type':        'blend',
        'basic_factor':     basic,
        'runs_factor':      runs,
        'singles_factor':   singles,
        'doubles_factor':   doubles,
        'triples_factor':   triples,
        'hr_factor':        hr,
        'woba_con':         woba_con,
        'xwoba_con':        xwoba_con,
        'bacon':            bacon,
        'xbacon':           xbacon,
        'hard_hit_factor':  hard_hit,
        'obp_factor':       obp,
        'h_factor':         h,
        'bb_factor':        bb,
        'k_factor':         k,
    })

print(f"Upserting {len(rows)} Savant park factor rows (season={SEASON}, source: 2024-2026 3-yr)...")
try:
    sb.table('park_factors').upsert(rows, on_conflict='venue_id,season,data_type').execute()
    print(f"  ✓ {len(rows)} rows upserted")
except Exception as e:
    print(f"  ERROR: {e}")

print("\nVerification:")
checks = [
    (30, 'Yankees  — HR=118, BB=119, basic=102'),
    (25, 'Coors    — basic=112, R=125, 3B=194'),
    (13, 'T-Mobile — basic=92, K=117'),
    (21, 'Globe    — basic=92, HR=89 (was 97/104)'),
    (22, 'Tropicana — HR=96, basic=95 (NEW)'),
]
for vid, label in checks:
    res = sb.table('park_factors').select(
        'venue_name,basic_factor,hr_factor,runs_factor,triples_factor,bb_factor,k_factor,woba_con,bacon'
    ).eq('venue_id', vid).eq('season', SEASON).maybe_single().execute()
    if res.data:
        d = res.data
        print(f"  {label}")
        print(f"    → basic={d['basic_factor']} HR={d['hr_factor']} R={d['runs_factor']} 3B={d['triples_factor']} BB={d['bb_factor']} K={d['k_factor']} wOBAcon={d['woba_con']} BACON={d['bacon']}")

print("\nDone. Athletics (venue_id=3) not updated — not in 2026 Savant dataset.")
print("Note: Rays (venue_id=22) now included — previously missing from 2025 dataset.")
