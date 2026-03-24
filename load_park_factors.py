import sys
sys.stdout.reconfigure(encoding='utf-8', line_buffering=True)
print("Script started", flush=True)

import requests
import math
from bs4 import BeautifulSoup
from supabase import create_client
from dotenv import load_dotenv
import os

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

from config import SEASON

# ── Map FanGraphs team abbreviations to venue IDs
TEAM_TO_VENUE = {
    'Angels':       (1,  'Angel Stadium',               'Los Angeles Angels'),
    'Astros':       (2,  'Minute Maid Park',             'Houston Astros'),
    'Athletics':    (3,  'Sacramento Ballpark',          'Athletics'),
    'Blue Jays':    (4,  'Rogers Centre',                'Toronto Blue Jays'),
    'Braves':       (5,  'Truist Park',                  'Atlanta Braves'),
    'Brewers':      (6,  'American Family Field',        'Milwaukee Brewers'),
    'Cardinals':    (7,  'Busch Stadium',                'St. Louis Cardinals'),
    'Cubs':         (8,  'Wrigley Field',                'Chicago Cubs'),
    'Diamondbacks': (9,  'Chase Field',                  'Arizona Diamondbacks'),
    'Dodgers':      (10, 'Dodger Stadium',               'Los Angeles Dodgers'),
    'Giants':       (11, 'Oracle Park',                  'San Francisco Giants'),
    'Guardians':    (12, 'Progressive Field',            'Cleveland Guardians'),
    'Mariners':     (13, 'T-Mobile Park',                'Seattle Mariners'),
    'Marlins':      (14, 'loanDepot park',               'Miami Marlins'),
    'Mets':         (15, 'Citi Field',                   'New York Mets'),
    'Nationals':    (16, 'Nationals Park',               'Washington Nationals'),
    'Orioles':      (17, 'Oriole Park at Camden Yards',  'Baltimore Orioles'),
    'Padres':       (18, 'Petco Park',                   'San Diego Padres'),
    'Phillies':     (19, 'Citizens Bank Park',           'Philadelphia Phillies'),
    'Pirates':      (20, 'PNC Park',                     'Pittsburgh Pirates'),
    'Rangers':      (21, 'Globe Life Field',             'Texas Rangers'),
    'Rays':         (22, 'Steinbrenner Field',            'Tampa Bay Rays'),
    'Red Sox':      (23, 'Fenway Park',                  'Boston Red Sox'),
    'Reds':         (24, 'Great American Ball Park',     'Cincinnati Reds'),
    'Rockies':      (25, 'Coors Field',                  'Colorado Rockies'),
    'Royals':       (26, 'Kauffman Stadium',             'Kansas City Royals'),
    'Tigers':       (27, 'Comerica Park',                'Detroit Tigers'),
    'Twins':        (28, 'Target Field',                 'Minnesota Twins'),
    'White Sox':    (29, 'Guaranteed Rate Field',        'Chicago White Sox'),
    'Yankees':      (30, 'Yankee Stadium',               'New York Yankees'),
}

def clean(val):
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None

# ══════════════════════════════════════════════
# STEP 1 — Fetch park factors from FanGraphs
# ══════════════════════════════════════════════
print(f"\nFetching park factors from FanGraphs ({SEASON})...")

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Referer': 'https://www.fangraphs.com/',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
}

url = f"https://www.fangraphs.com/guts.aspx?type=pf&teamid=0&season={SEASON}"

try:
    response = requests.get(url, headers=HEADERS, timeout=30)
    print(f"  Response status: {response.status_code}")
    print(f"  Response length: {len(response.text):,} characters")
except Exception as e:
    print(f"  ERROR fetching page: {e}")
    exit()

# ── Parse the HTML table
soup   = BeautifulSoup(response.text, 'lxml')
table  = None
tables = soup.find_all('table')
print(f"  Found {len(tables)} tables on page, searching for park factors...")

for t in tables:
    headers_row = t.find('tr')
    if headers_row and 'Basic' in headers_row.get_text():
        table = t
        break

if not table:
    print("  ERROR: Could not find park factors table on FanGraphs page.")
    print("  Page preview:", response.text[:500])
    exit()

print("  Found park factors table")

# ── Extract rows
# Column order: [season, team, basic, 1B, 2B, 3B, HR, R, ...]
rows      = table.find_all('tr')
park_rows = []
skipped   = 0

for row in rows[1:]:  # Skip header row
    cols = row.find_all('td')
    if len(cols) < 7:
        continue

    # Col 1 = team name, Col 0 = season year
    team_name = cols[1].get_text(strip=True)
    if not team_name:
        continue

    # Match to our venue mapping
    matched = None
    for key, val in TEAM_TO_VENUE.items():
        if key.lower() in team_name.lower() or team_name.lower() in key.lower():
            matched = (key, val)
            break

    if not matched:
        print(f"  Could not match team: {team_name}")
        skipped += 1
        continue

    key, (venue_id, venue_name, full_team_name) = matched

    # Col 2=basic, 3=1B, 4=2B, 5=3B, 6=HR, 7=R
    try:
        basic_factor   = clean(cols[2].get_text(strip=True))
        singles_factor = clean(cols[3].get_text(strip=True))
        doubles_factor = clean(cols[4].get_text(strip=True))
        triples_factor = clean(cols[5].get_text(strip=True))
        hr_factor      = clean(cols[6].get_text(strip=True))
        runs_factor    = clean(cols[7].get_text(strip=True))
    except Exception as e:
        print(f"  Error parsing row for {team_name}: {e}")
        skipped += 1
        continue

    park_rows.append({
        'venue_id':       venue_id,
        'venue_name':     venue_name,
        'team_name':      full_team_name,
        'season':         SEASON,
        'basic_factor':   basic_factor,
        'singles_factor': singles_factor,
        'doubles_factor': doubles_factor,
        'triples_factor': triples_factor,
        'hr_factor':      hr_factor,
        'runs_factor':    runs_factor,
    })

    print(f"  {full_team_name}: Basic={basic_factor}, HR={hr_factor}, R={runs_factor}")

print(f"\nParsed {len(park_rows)} teams, skipped {skipped}")

# ══════════════════════════════════════════════
# STEP 2 — Upload to Supabase
# ══════════════════════════════════════════════
if park_rows:
    print(f"\nUploading {len(park_rows)} park factor records...")
    try:
        supabase.table('park_factors').upsert(park_rows, on_conflict='venue_id,season').execute()
        print(f"  ✓ {len(park_rows)} records uploaded")
    except Exception as e:
        print(f"  ERROR: {e}")
else:
    print("\nNo park factor records to upload.")

print(f"\nPhase 7 complete.")
print(f"  Park factors uploaded: {len(park_rows)}")