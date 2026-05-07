"""
seed_park_factors_singleyear.py
Seeds single-season park factors for 2024, 2025, 2026 from Baseball Savant.
Source: baseballsavant.mlb.com/leaderboard/statcast-park-factors (rolling=1)

These complement the 3-yr blend rows (data_type='blend') and let users compare
year-over-year trends for parks that changed (humidors, dimension changes, etc.).

Note: 2026 data is partial-season (~30 games as of May 2026) — noisy for triples
and other low-frequency events. Use blend for projections; single-year for context.

Run migrate_park_factors_data_type.sql in Supabase BEFORE this script.
"""
import sys, os
sys.stdout.reconfigure(encoding='utf-8', line_buffering=True)
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()
sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])

# Column order: venue_id, venue_name, team_name,
#   basic(wOBA), runs, h, singles, doubles, triples, hr, bb, k
# woba_con/xwoba_con/bacon/xbacon/hard_hit/obp not available from single-year fetch

PARK_FACTORS_2026 = [
    (29, 'Guaranteed Rate Field',      'Chicago White Sox',       94,  88,  89,  94,  83,  91,  73, 111,  99),
    ( 9, 'Chase Field',                'Arizona Diamondbacks',   105, 110, 108, 102, 115, 263, 102,  96,  82),
    (25, 'Coors Field',                'Colorado Rockies',       102, 104, 107, 100, 122, 270, 109,  81, 100),
    (17, 'Oriole Park at Camden Yards','Baltimore Orioles',      109, 119, 118, 116, 113, 176, 126,  85,  97),
    (16, 'Nationals Park',             'Washington Nationals',   106, 112, 106, 103, 105,  37, 127, 102,  93),
    (13, 'T-Mobile Park',              'Seattle Mariners',       101, 102, 102,  99, 116,  36, 110,  95, 110),
    (11, 'Oracle Park',                'San Francisco Giants',    97,  94, 102, 106,  95, 237,  77,  94,  93),
    (28, 'Target Field',               'Minnesota Twins',        101, 102, 100, 105,  94, 107,  85, 106,  92),
    ( 3, 'Sutter Health Park',         'Oakland Athletics',      118, 139, 110, 102, 131,  88, 122, 142,  87),
    ( 8, 'Wrigley Field',              'Chicago Cubs',            98,  96,  96, 102,  69, 116, 104, 108,  97),
    (10, 'Dodger Stadium',             'Los Angeles Dodgers',     98,  96,  94,  90,  95,  20, 117, 110, 111),
    ( 7, 'Busch Stadium',              'St. Louis Cardinals',    100, 100, 107, 112, 110,  37,  85,  86,  82),
    (21, 'Globe Life Field',           'Texas Rangers',           84,  71,  90,  96,  78, 145,  72,  68, 103),
    (24, 'Great American Ball Park',   'Cincinnati Reds',        112, 125,  99,  91,  85,  76, 179, 132, 101),
    (18, 'Petco Park',                 'San Diego Padres',        96,  92,  92,  95,  85,  38, 100, 113,  97),
    ( 6, 'American Family Field',      'Milwaukee Brewers',       97,  94,  95,  94,  89,  21, 127,  97, 112),
    (19, 'Citizens Bank Park',         'Philadelphia Phillies',  103, 106, 104, 106, 101,  76,  99,  99, 109),
    (26, 'Kauffman Stadium',           'Kansas City Royals',     105, 110, 104,  95, 128,  93, 114, 105,  96),
    (12, 'Progressive Field',          'Cleveland Guardians',     89,  79,  84,  87,  78,  36,  79, 106, 111),
    ( 1, 'Angel Stadium',              'Los Angeles Angels',      89,  79,  93, 102,  86,  50,  64,  84, 119),
    (27, 'Comerica Park',              'Detroit Tigers',          99,  98, 104, 112,  72, 285,  99,  84,  93),
    ( 2, 'Daikin Park',                'Houston Astros',         102, 104,  91,  78, 105,  34, 144, 112, 115),
    (20, 'PNC Park',                   'Pittsburgh Pirates',     105, 110, 109, 106, 140,  78,  87,  94,  99),
    (23, 'Fenway Park',                'Boston Red Sox',          94,  88, 100, 102, 133, 106,  48, 104,  97),
    ( 5, 'Truist Park',                'Atlanta Braves',          99,  98, 108, 117,  99, 145,  78,  91,  93),
    (14, 'loanDepot park',             'Miami Marlins',           94,  88,  94,  97, 103, 114,  66,  87, 102),
    ( 4, 'Rogers Centre',              'Toronto Blue Jays',       98,  96,  98,  96,  99,  50, 112,  93, 110),
    (30, 'Yankee Stadium',             'New York Yankees',       109, 119,  96,  86, 103,  66, 147, 137, 100),
    (15, 'Citi Field',                 'New York Mets',           95,  90,  96, 105,  80,  88,  83, 104, 110),
    (22, 'Tropicana Field',            'Tampa Bay Rays',          96,  92, 104, 107,  99, 121,  93,  82,  92),
]

PARK_FACTORS_2025 = [
    (21, 'Globe Life Field',           'Texas Rangers',           91,  83,  92,  94,  98,  53,  80,  98, 102),
    (15, 'Citi Field',                 'New York Mets',           99,  98,  99, 102,  89,  81, 101, 101, 100),
    (17, 'Oriole Park at Camden Yards','Baltimore Orioles',      103, 106, 103,  99, 103, 106, 121,  93, 102),
    (24, 'Great American Ball Park',   'Cincinnati Reds',         99,  98,  97,  95,  99,  64, 111, 103, 104),
    (20, 'PNC Park',                   'Pittsburgh Pirates',      96,  92, 100, 101, 117, 108,  66,  95, 100),
    (18, 'Petco Park',                 'San Diego Padres',        95,  90,  92,  96,  82,  83,  91, 110, 104),
    (12, 'Progressive Field',          'Cleveland Guardians',     95,  90,  95,  98,  91,  63,  88, 103, 106),
    (29, 'Guaranteed Rate Field',      'Chicago White Sox',       98,  96,  96,  98,  91,  65,  96, 106,  96),
    (30, 'Yankee Stadium',             'New York Yankees',        99,  98,  92,  90,  86,  91, 111, 115, 101),
    (11, 'Oracle Park',                'San Francisco Giants',    99,  98, 103, 107, 102, 111,  84,  96,  97),
    ( 9, 'Chase Field',                'Arizona Diamondbacks',   102, 104, 102,  98, 113, 218,  90, 100,  92),
    (25, 'Coors Field',                'Colorado Rockies',       115, 132, 120, 118, 126, 200, 110,  99,  89),
    (28, 'Target Field',               'Minnesota Twins',        101, 102, 103, 106, 112,  67,  87,  96,  98),
    ( 6, 'American Family Field',      'Milwaukee Brewers',       98,  96,  95,  95,  94,  90,  98, 107, 106),
    (23, 'Fenway Park',                'Boston Red Sox',         103, 106, 105, 107, 114,  94,  84, 101,  93),
    (27, 'Comerica Park',              'Detroit Tigers',         105, 110, 104, 101, 101, 161, 114, 102,  99),
    (10, 'Dodger Stadium',             'Los Angeles Dodgers',    104, 108,  99,  92,  98,  79, 137, 107, 102),
    ( 2, 'Daikin Park',                'Houston Astros',          97,  94,  97,  97,  92,  80, 106, 100, 110),
    ( 3, 'Sutter Health Park',         'Oakland Athletics',      108, 117, 107, 101, 122,  82, 112, 107,  97),
    ( 8, 'Wrigley Field',              'Chicago Cubs',            98,  96,  96,  94,  89, 132, 111,  98, 103),
    (16, 'Nationals Park',             'Washington Nationals',   101, 102, 104, 108,  98, 125,  93,  93,  96),
    (13, 'T-Mobile Park',              'Seattle Mariners',        91,  83,  89,  87,  95,  31,  98,  95, 113),
    ( 1, 'Angel Stadium',              'Los Angeles Angels',     101, 102,  99,  99,  93,  96, 111,  99, 106),
    (19, 'Citizens Bank Park',         'Philadelphia Phillies',  102, 104, 104, 102,  99, 109, 117,  92, 107),
    (22, 'Tropicana Field',            'Tampa Bay Rays',         102, 104, 104, 109,  85,  59, 112,  96, 101),
    ( 7, 'Busch Stadium',              'St. Louis Cardinals',     97,  94, 103, 108, 109,  56,  77,  90,  90),
    (14, 'loanDepot park',             'Miami Marlins',           97,  94,  98,  99, 101, 148,  84,  97,  96),
    (26, 'Kauffman Stadium',           'Kansas City Royals',      97,  94, 100,  99, 106, 212,  83,  98,  91),
    ( 4, 'Rogers Centre',              'Toronto Blue Jays',      103, 106, 102, 100, 100,  69, 118, 101,  94),
    ( 5, 'Truist Park',                'Atlanta Braves',         101, 102, 101, 103,  94,  83, 105, 100, 104),
]

PARK_FACTORS_2024 = [
    (21, 'Globe Life Field',           'Texas Rangers',           95,  90,  95,  95,  86, 105, 105,  98, 103),
    (28, 'Target Field',               'Minnesota Twins',        105, 110, 105, 102, 112,  92, 113, 100, 101),
    ( 6, 'American Family Field',      'Milwaukee Brewers',       96,  92,  94,  94,  78, 114, 111, 102, 113),
    (26, 'Kauffman Stadium',           'Kansas City Royals',     102, 104, 105, 103, 124, 191,  82,  98,  90),
    (24, 'Great American Ball Park',   'Cincinnati Reds',        105, 110, 103,  98, 112,  59, 124, 103, 102),
    (12, 'Progressive Field',          'Cleveland Guardians',    102, 104, 101,  96, 121,  44, 106, 101, 101),
    (10, 'Dodger Stadium',             'Los Angeles Dodgers',    100, 100,  97,  94,  90,  65, 121, 101,  95),
    (19, 'Citizens Bank Park',         'Philadelphia Phillies',  100, 100, 100, 100,  90,  94, 114,  97,  98),
    ( 8, 'Wrigley Field',              'Chicago Cubs',            91,  83,  91,  96,  78, 107,  85, 100, 106),
    (22, 'Tropicana Field',            'Tampa Bay Rays',          96,  92,  95,  97,  83, 132,  99,  98, 106),
    (16, 'Nationals Park',             'Washington Nationals',    99,  98, 100, 104,  92,  79,  93,  96,  93),
    (15, 'Citi Field',                 'New York Mets',           99,  98,  92,  87,  96,  81, 108, 116, 105),
    (13, 'T-Mobile Park',              'Seattle Mariners',        89,  79,  88,  90,  80,  58,  89,  97, 122),
    (11, 'Oracle Park',                'San Francisco Giants',    96,  92, 100, 100, 117, 156,  72,  89,  97),
    (27, 'Comerica Park',              'Detroit Tigers',          97,  94,  97,  99,  89, 141,  92,  93,  98),
    ( 2, 'Daikin Park',                'Houston Astros',         103, 106, 102, 100,  99,  64, 116, 101, 100),
    ( 3, 'Oakland Coliseum',           'Oakland Athletics',       98,  96,  99, 101, 104,  89,  82, 105,  99),
    ( 1, 'Angel Stadium',              'Los Angeles Angels',     101, 102,  97,  97,  92,  89, 107, 106, 102),
    ( 9, 'Chase Field',                'Arizona Diamondbacks',   106, 112, 109, 106, 121, 194,  92,  98,  92),
    (18, 'Petco Park',                 'San Diego Padres',        99,  98,  99,  97,  95,  69, 123,  87, 102),
    (17, 'Oriole Park at Camden Yards','Baltimore Orioles',      102, 104, 104, 105,  97, 136, 104,  90,  98),
    (20, 'PNC Park',                   'Pittsburgh Pirates',     102, 104, 103, 103, 115,  62,  90, 102,  95),
    (29, 'Guaranteed Rate Field',      'Chicago White Sox',       99,  98,  99, 102,  96,  72,  93, 105,  97),
    (25, 'Coors Field',                'Colorado Rockies',       110, 121, 115, 117, 110, 176, 105, 103,  92),
    (30, 'Yankee Stadium',             'New York Yankees',       103, 106,  97,  94,  93,  50, 122, 118, 101),
    ( 4, 'Rogers Centre',              'Toronto Blue Jays',      100, 100, 102, 100, 114,  90, 100,  94,  95),
    ( 5, 'Truist Park',                'Atlanta Braves',          99,  98, 101, 105,  97, 100,  88, 100, 108),
    (23, 'Fenway Park',                'Boston Red Sox',         102, 104, 104, 104, 117,  81,  90,  94, 103),
    ( 7, 'Busch Stadium',              'St. Louis Cardinals',     98,  96, 101, 105, 102, 117,  83,  97,  91),
    (14, 'loanDepot park',             'Miami Marlins',          104, 108, 105, 106, 107, 126,  95, 103,  97),
]

def build_rows(factor_list, season):
    rows = []
    for (vid, vname, tname, basic, runs, h, singles, doubles, triples, hr, bb, k) in factor_list:
        rows.append({
            'venue_id':       vid,
            'venue_name':     vname,
            'team_name':      tname,
            'season':         season,
            'data_type':      'single_year',
            'basic_factor':   basic,
            'runs_factor':    runs,
            'h_factor':       h,
            'singles_factor': singles,
            'doubles_factor': doubles,
            'triples_factor': triples,
            'hr_factor':      hr,
            'bb_factor':      bb,
            'k_factor':       k,
        })
    return rows

for season, data in [(2026, PARK_FACTORS_2026), (2025, PARK_FACTORS_2025), (2024, PARK_FACTORS_2024)]:
    rows = build_rows(data, season)
    print(f"Upserting {len(rows)} single-year rows for {season}...")
    try:
        sb.table('park_factors').upsert(rows, on_conflict='venue_id,season,data_type').execute()
        print(f"  ✓ {len(rows)} rows upserted")
    except Exception as e:
        print(f"  ERROR: {e}")

print("\nVerification (2026 single-year):")
checks = [
    (21, 'Globe Life — HR should be 72 (humidor effect)'),
    (30, 'Yankees   — HR should be 147 (small sample 2026)'),
    (25, 'Coors     — basic should be 102 (2026)'),
]
for vid, label in checks:
    res = sb.table('park_factors').select('venue_name,basic_factor,hr_factor,runs_factor,data_type,season') \
        .eq('venue_id', vid).eq('data_type', 'single_year').eq('season', 2026).maybe_single().execute()
    if res.data:
        d = res.data
        print(f"  {label}")
        print(f"    → basic={d['basic_factor']} HR={d['hr_factor']} R={d['runs_factor']}")

print("\nDone.")
