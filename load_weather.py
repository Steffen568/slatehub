#!/usr/bin/env python3
"""Phase 6 — Weather data from PropFinder (Kevin Roth's baseball-specific forecasts)"""
import sys
sys.stdout.reconfigure(encoding='utf-8', line_buffering=True)

import os, re, requests
from datetime import date
from supabase import create_client
from dotenv import load_dotenv
from bs4 import BeautifulSoup

load_dotenv()
sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])

PROPFINDER_URL = "https://propfinder.app/weather"

def deg_to_compass(deg):
    dirs = ["N","NNE","NE","ENE","E","ESE","SE","SSE",
            "S","SSW","SW","WSW","W","WNW","NW","NNW"]
    return dirs[round(deg / 22.5) % 16]

def parse_number(text):
    """Extract first number from text like '57°F' or '20%' or '8 Mph'."""
    if not text:
        return None
    m = re.search(r'([\d.]+)', text.replace(',', ''))
    return float(m.group(1)) if m else None

def parse_rotation(svg_el):
    """Extract rotation degrees from SVG arrow path transform='rotate(X 12 12)'."""
    if not svg_el:
        return None
    for path in svg_el.find_all('path'):
        t = path.get('transform', '')
        m = re.search(r'rotate\(([\d.]+)', t)
        if m:
            return float(m.group(1))
    return None

def scrape_propfinder():
    """Scrape propfinder.app/weather and return per-game weather data keyed by (away_abbr, home_abbr)."""
    resp = requests.get(PROPFINDER_URL, timeout=20, headers={
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    })
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, 'html.parser')

    games = {}

    # Find expanded game cards (MuiCard with hourly tables)
    cards = soup.find_all('div', class_=re.compile(r'MuiCard-root'))
    print(f"  Found {len(cards)} game cards on PropFinder")

    for card in cards:

        # ── Teams ──
        team_imgs = card.find_all('img', src=re.compile(r'/teams/\w+\.png'))
        teams = []
        for img in team_imgs:
            m = re.search(r'/teams/(\w+)\.png', img.get('src', ''))
            if m:
                teams.append(m.group(1))
        away_team = teams[0] if len(teams) >= 1 else ''
        home_team = teams[1] if len(teams) >= 2 else ''

        # ── Venue name ──
        venue_el = card.find('p', string=re.compile(r'(Park|Field|Stadium|Coliseum|Centre|Ballpark|Depot|Fenway|Wrigley|Kauffman|Oracle|Petco|Coors|Truist|Busch|Citi|Comerica|PNC|Tropicana|Target)', re.I))
        venue_name = venue_el.get_text(strip=True) if venue_el else ''

        # ── Hourly table — get game-time data (first pitch hour column) ──
        table = card.find('table')
        temp_f = None
        precip_pct = None
        humidity = None
        wind_speed = None
        wind_deg = None
        conditions = None

        if table:
            rows = table.find_all('tr')
            for row in rows:
                cells = row.find_all(['td', 'th'])
                if not cells:
                    continue
                label = cells[0].get_text(strip=True).lower()

                # Use 2nd column (first data column = ~game time hour)
                # But prefer 3rd column if available (often the actual first-pitch hour)
                data_idx = 2 if len(cells) > 2 else (1 if len(cells) > 1 else None)
                if data_idx is None:
                    continue
                cell = cells[data_idx]

                if label == 'conditions':
                    # Conditions text from <p> or alt text
                    p = cell.find('p')
                    if p:
                        conditions = p.get_text(strip=True)
                    else:
                        img = cell.find('img')
                        if img:
                            conditions = img.get('alt', '')
                elif label == 'temperature':
                    temp_f = parse_number(cell.get_text())
                elif label == 'precipitation':
                    precip_pct = parse_number(cell.get_text())
                elif label == 'humidity':
                    humidity = parse_number(cell.get_text())
                elif label == 'wind speed':
                    wind_speed = parse_number(cell.get_text())
                elif label == 'wind dir':
                    svg = cell.find('svg')
                    wind_deg = parse_rotation(svg)

        # ── Delay risk chip ──
        delay_risk = None
        chip = card.find('span', class_=re.compile(r'MuiChip-label'))
        if chip:
            chip_text = chip.get_text(strip=True)
            # Only capture delay/postponement chips, not "Clear"
            if 'delay' in chip_text.lower() or 'postpone' in chip_text.lower():
                delay_risk = chip_text

        # ── Forecaster notes (per-card) ──
        forecaster_notes = None
        notes_header = card.find('h3', string=re.compile(r'Forecaster Notes', re.I))
        if notes_header:
            notes_container = notes_header.find_parent('div')
            if notes_container:
                # Get all <p> tags after the header that contain note content
                parent_box = notes_container.find_parent('div')
                if parent_box:
                    note_paras = []
                    for p in parent_box.find_all('p'):
                        txt = p.get_text(strip=True)
                        if txt and 'no notes posted' not in txt.lower():
                            note_paras.append(txt)
                    if note_paras:
                        forecaster_notes = ' '.join(note_paras)

        if not away_team or not home_team:
            continue

        key = (away_team.upper(), home_team.upper())
        games[key] = {
            'away_team': away_team,
            'home_team': home_team,
            'venue_name': venue_name,
            'temp_f': temp_f,
            'precip_pct': precip_pct,
            'humidity': humidity,
            'wind_speed': wind_speed,
            'wind_deg': wind_deg,
            'wind_dir': deg_to_compass(wind_deg) if wind_deg is not None else None,
            'conditions': conditions,
            'delay_risk': delay_risk,
            'forecaster_notes': forecaster_notes,
        }

    return games


def run():
    today = str(date.today())
    print(f"\nFetching weather from PropFinder for {today}...")

    # Get today's games from our DB
    db_games = sb.table("games").select(
        "game_pk,venue_name,game_date,home_team,away_team"
    ).eq("game_date", today).execute().data

    if not db_games:
        print("No games today.")
        return

    # Scrape PropFinder
    pf_data = scrape_propfinder()
    print(f"  PropFinder games: {len(pf_data)}")

    # Build team abbreviation lookup from DB team names
    # DB uses full names like "Baltimore Orioles" — PropFinder uses "BAL"
    TEAM_ABBR = {
        'Arizona Diamondbacks': 'ARI', 'Atlanta Braves': 'ATL', 'Baltimore Orioles': 'BAL',
        'Boston Red Sox': 'BOS', 'Chicago Cubs': 'CHC', 'Chicago White Sox': 'CWS',
        'Cincinnati Reds': 'CIN', 'Cleveland Guardians': 'CLE', 'Colorado Rockies': 'COL',
        'Detroit Tigers': 'DET', 'Houston Astros': 'HOU', 'Kansas City Royals': 'KC',
        'Los Angeles Angels': 'LAA', 'Los Angeles Dodgers': 'LAD', 'Miami Marlins': 'MIA',
        'Milwaukee Brewers': 'MIL', 'Minnesota Twins': 'MIN', 'New York Mets': 'NYM',
        'New York Yankees': 'NYY', 'Athletics': 'ATH', 'Oakland Athletics': 'ATH',
        'Philadelphia Phillies': 'PHI', 'Pittsburgh Pirates': 'PIT',
        'San Diego Padres': 'SD', 'San Francisco Giants': 'SF',
        'Seattle Mariners': 'SEA', 'St. Louis Cardinals': 'STL',
        'Tampa Bay Rays': 'TB', 'Texas Rangers': 'TEX', 'Toronto Blue Jays': 'TOR',
        'Washington Nationals': 'WSH',
    }

    loaded, skipped = 0, 0
    for g in db_games:
        gpk = g["game_pk"]
        away_abbr = TEAM_ABBR.get(g['away_team'], '').upper()
        home_abbr = TEAM_ABBR.get(g['home_team'], '').upper()
        wx = pf_data.get((away_abbr, home_abbr))

        if not wx:
            print(f"  SKIP (not on PropFinder): {g['venue_name']} — {away_abbr}@{home_abbr} (game_pk={gpk})")
            skipped += 1
            continue

        if wx['temp_f'] is None:
            print(f"  SKIP (no temp data): {g['venue_name']} (game_pk={gpk})")
            skipped += 1
            continue

        record = {
            "game_pk"          : gpk,
            "venue_name"       : g["venue_name"],
            "game_date"        : g["game_date"],
            "temp_f"           : wx['temp_f'],
            "feels_like_f"     : None,
            "humidity"         : wx['humidity'],
            "wind_speed"       : wx['wind_speed'],
            "wind_dir"         : wx['wind_dir'],
            "wind_deg"         : wx['wind_deg'],
            "conditions"       : wx['conditions'],
            "precip_pct"       : wx['precip_pct'],
            "visibility"       : None,
            "dew_point"        : None,
            "is_outdoor"       : True,
            "delay_risk"       : wx['delay_risk'],
            "forecaster_notes" : wx['forecaster_notes'],
        }

        try:
            sb.table("weather").upsert(
                record, on_conflict="game_pk", ignore_duplicates=False
            ).execute()
            notes_flag = " [NOTES]" if wx['forecaster_notes'] else ""
            delay_flag = f" [{wx['delay_risk']}]" if wx['delay_risk'] else ""
            print(f"  OK {g['venue_name']} — {wx['temp_f']}F, {wx['conditions']}, "
                  f"wind {wx['wind_speed']}mph {wx['wind_dir']}, precip {wx['precip_pct']}%"
                  f"{delay_flag}{notes_flag}")
            loaded += 1
        except Exception as e:
            print(f"  ERROR {g['venue_name']}: {e}")
            skipped += 1

    print(f"\nWeather complete. Loaded: {loaded}  Skipped: {skipped}")


if __name__ == "__main__":
    run()
