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

SEASON = 2025  # Labels the 2023-2025 3-year dataset

# Column order: venue_id, venue_name, team_name,
#   basic, woba_con, xwoba_con, bacon, xbacon, hard_hit,
#   runs, obp, h, singles, doubles, triples, hr, bb, k
SAVANT_PARK_FACTORS = [
    (25, 'Coors Field',                 'Colorado Rockies',      113, 112, 101, 113, 102, 101, 128, 111, 117, 116, 119, 202, 106, 101,  90),
    (23, 'Fenway Park',                 'Boston Red Sox',        104, 105, 100, 106, 100, 101, 108, 105, 107, 106, 122,  94,  89,  97,  97),
    ( 9, 'Chase Field',                 'Arizona Diamondbacks',  103, 102, 100, 102, 101, 102, 106, 103, 105, 103, 115, 204,  88,  99,  94),
    (24, 'Great American Ball Park',    'Cincinnati Reds',       103, 103,  98, 101,  99,  94, 106, 101, 100,  96,  99,  76, 123, 103, 102),
    (28, 'Target Field',                'Minnesota Twins',       102, 104, 101, 104, 100, 101, 104, 102, 102, 100, 111,  98, 102, 100, 103),
    (14, 'loanDepot park',              'Miami Marlins',         101, 100, 101, 102, 101, 100, 102, 101, 103, 104, 107, 118,  90,  97,  98),
    (26, 'Kauffman Stadium',            'Kansas City Royals',    101,  98, 102,  99, 102, 103, 102, 102, 104, 103, 113, 183,  85, 100,  89),
    (16, 'Nationals Park',              'Washington Nationals',  101,  99, 101, 100, 101, 101, 102, 102, 104, 108,  98,  99,  94,  94,  90),
    (19, 'Citizens Bank Park',          'Philadelphia Phillies', 101, 103,  99, 101,  99, 100, 102,  99, 100,  99,  96, 100, 114,  96, 104),
    (10, 'Dodger Stadium',              'Los Angeles Dodgers',   101, 101, 102,  98, 101, 101, 102,  99,  97,  92,  96,  64, 127, 101, 100),
    ( 1, 'Angel Stadium',               'Los Angeles Angels',    101, 102, 100, 101,  99,  99, 102, 100,  98,  97,  90,  98, 113, 103, 105),
    ( 5, 'Truist Park',                 'Atlanta Braves',        101, 103, 103, 103, 103, 103, 102, 100, 102, 103,  94,  91, 104,  99, 106),
    ( 7, 'Busch Stadium',               'St. Louis Cardinals',   100,  97, 101,  99, 101, 102, 100, 101, 103, 107, 105,  81,  87,  96,  91),
    (17, 'Oriole Park at Camden Yards', 'Baltimore Orioles',     100, 101, 102, 101, 101, 105, 100,  99, 103, 103,  97, 120, 105,  91,  99),
    (27, 'Comerica Park',               'Detroit Tigers',        100, 100, 100, 100, 100,  99, 100, 100, 100, 100,  95, 144,  99, 100,  98),
    ( 2, 'Daikin Park',                 'Houston Astros',        100, 101,  98, 100,  99,  98, 100, 100, 100, 100,  96,  89, 105, 100, 102),
    ( 4, 'Rogers Centre',               'Toronto Blue Jays',     100,  99,  99,  98,  99, 100, 100, 100,  99,  97, 104,  70, 104, 100,  98),
    (30, 'Yankee Stadium',              'New York Yankees',      100,  99, 102,  97, 100, 104, 100,  99,  94,  91,  90,  63, 119, 112, 102),
    (20, 'PNC Park',                    'Pittsburgh Pirates',     99,  97, 101, 100, 101, 102,  98, 101, 101, 102, 115,  84,  76, 100,  96),
    (29, 'Guaranteed Rate Field',       'Chicago White Sox',      99,  98,  98,  99,  99,  97,  98, 100,  98, 101,  92,  70,  96, 103, 100),
    (15, 'Citi Field',                  'New York Mets',          98,  97, 100,  96,  99, 101,  96,  99,  94,  93,  89,  72, 104, 110, 103),
    ( 6, 'American Family Field',       'Milwaukee Brewers',      97,  99,  98,  98,  99,  96,  94,  97,  94,  94,  87,  89, 106, 106, 109),
    (12, 'Progressive Field',           'Cleveland Guardians',    97,  96,  98,  98, 100,  96,  94,  98,  97,  98, 106,  72,  85, 100, 102),
    (11, 'Oracle Park',                 'San Francisco Giants',   97,  96,  97,  99,  98,  99,  94,  97, 101, 104, 102, 122,  82,  90,  98),
    (18, 'Petco Park',                  'San Diego Padres',       97,  97,  99,  97,  99,  98,  94,  98,  96,  97,  92,  64, 102, 103, 102),
    ( 8, 'Wrigley Field',               'Chicago Cubs',           97,  97, 100,  97, 100, 100,  94,  97,  96,  98,  86, 116,  99, 100, 103),
    (21, 'Globe Life Field',            'Texas Rangers',           97,  97, 100,  96,  98, 102,  94,  97,  97,  97,  95,  74, 104, 100, 101),
    (13, 'T-Mobile Park',               'Seattle Mariners',        91,  94,  99,  94,  99,  98,  83,  92,  89,  89,  89,  52,  93,  97, 117),
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

print(f"Upserting {len(rows)} Savant park factor rows (season={SEASON}, source: 2023-2025 3-yr)...")
try:
    sb.table('park_factors').upsert(rows, on_conflict='venue_id,season').execute()
    print(f"  ✓ {len(rows)} rows upserted")
except Exception as e:
    print(f"  ERROR: {e}")

print("\nVerification:")
checks = [
    (30, 'Yankees  — HR=119, BB=112, 3B=63'),
    (25, 'Coors    — basic=113, R=128, 3B=202'),
    (13, 'T-Mobile — basic=91, K=117'),
    ( 9, 'Chase    — 3B=204, HR=88'),
]
for vid, label in checks:
    res = sb.table('park_factors').select(
        'venue_name,basic_factor,hr_factor,runs_factor,triples_factor,bb_factor,k_factor,woba_con,bacon'
    ).eq('venue_id', vid).eq('season', SEASON).maybe_single().execute()
    if res.data:
        d = res.data
        print(f"  {label}")
        print(f"    → basic={d['basic_factor']} HR={d['hr_factor']} R={d['runs_factor']} 3B={d['triples_factor']} BB={d['bb_factor']} K={d['k_factor']} wOBAcon={d['woba_con']} BACON={d['bacon']}")

print("\nDone. Athletics (venue_id=3) and Rays (venue_id=22) not updated — not in Savant dataset.")
print("Note: Astros venue updated to 'Daikin Park'.")
