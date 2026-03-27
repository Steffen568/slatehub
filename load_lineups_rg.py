#!/usr/bin/env python3
import sys
sys.stdout.reconfigure(encoding='utf-8', line_buffering=True)
"""
Load projected lineups from RotoGrinders.
Only writes status='projected' rows — never overwrites confirmed lineups.
"""

import os, requests, unicodedata
from datetime import date
from bs4 import BeautifulSoup
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()
sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])

RG_URL = "https://rotogrinders.com/lineups/mlb"

# RotoGrinders team abbreviation → MLB Stats API team ID
# Keep this map up-to-date if RG changes abbreviations
TEAM_ABBR_TO_ID = {
    "ARI": 109, "ATL": 144, "BAL": 110, "BOS": 111, "CHC": 112,
    "CWS": 145, "CIN": 113, "CLE": 114, "COL": 115, "DET": 116,
    "HOU": 117, "KC":  118, "LAA": 108, "LAD": 119, "MIA": 146,
    "MIL": 158, "MIN": 142, "NYM": 121, "NYY": 147, "OAK": 133,
    "PHI": 143, "PIT": 134, "SD":  135, "SF":  137, "SEA": 136,
    "STL": 138, "TB":  139, "TEX": 140, "TOR": 141, "WSH": 120,
    # Alternate abbreviations RG might use
    "CHW": 145, "SDP": 135, "SFG": 137, "TBR": 139, "WSN": 120,
    "KCR": 118, "AZ": 109, "ATH": 133,
}


def ascii_name(name):
    if not name:
        return name
    nfkd = unicodedata.normalize('NFKD', str(name))
    return nfkd.encode('ASCII', 'ignore').decode('ASCII').strip()


def normalize(name):
    if not name or not isinstance(name, str):
        return ''
    nfkd = unicodedata.normalize('NFKD', name)
    result = nfkd.encode('ASCII', 'ignore').decode('ASCII').strip().lower()
    # Strip periods (T.J. → TJ, A.J. → AJ) so initials match DB format
    result = result.replace('.', '')
    # Strip common suffixes that DB may not include
    for suffix in [' jr.', ' jr', ' sr.', ' sr', ' iii', ' ii', ' iv']:
        if result.endswith(suffix):
            result = result[:-len(suffix)].strip()
            break
    return result


def load_player_name_map():
    """Load normalized name → (mlbam_id, display_name) from players table."""
    print("Loading player name map...")
    all_players = []
    offset = 0
    while True:
        res = sb.table('players').select(
            'mlbam_id, name_normalized, first_name, last_name'
        ).range(offset, offset + 999).execute()
        if not res.data:
            break
        all_players.extend(res.data)
        if len(res.data) < 1000:
            break
        offset += 1000

    name_map = {}
    for p in all_players:
        if p['name_normalized'] and p['mlbam_id']:
            full = ' '.join(x for x in [p.get('first_name'), p.get('last_name')] if x)
            name_map[p['name_normalized']] = (p['mlbam_id'], full)
    print(f"  {len(name_map):,} player name mappings loaded")
    return name_map


def load_todays_games():
    """Load today's games from our games table to get game_pk and team IDs."""
    today_str = str(date.today())
    res = (sb.table('games')
             .select('game_pk, game_date, home_team_id, away_team_id, home_team, away_team')
             .eq('game_date', today_str)
             .execute())
    games = res.data or []
    print(f"  {len(games)} games found for {today_str}")
    return games


def load_confirmed_lineups(game_pks):
    """Load lineups already marked confirmed so we don't overwrite them."""
    if not game_pks:
        return set()
    confirmed = set()
    # Batch to avoid URL-too-long
    for i in range(0, len(game_pks), 100):
        batch = game_pks[i:i+100]
        res = (sb.table('lineups')
                 .select('game_pk, team_id')
                 .in_('game_pk', batch)
                 .eq('status', 'confirmed')
                 .limit(5000)
                 .execute())
        for row in (res.data or []):
            confirmed.add((row['game_pk'], row['team_id']))
    return confirmed


def fetch_rg_page():
    """Fetch and parse the RotoGrinders MLB lineups page."""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                       '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    r = requests.get(RG_URL, headers=headers, timeout=30)
    r.raise_for_status()
    return BeautifulSoup(r.text, 'html.parser')


def parse_lineups(soup):
    """
    Parse RotoGrinders page into list of:
      { 'away_abbr': str, 'home_abbr': str,
        'away_players': [name1..name9], 'home_players': [name1..name9] }

    HTML structure (as of 2026-03):
      .game-card
        .game-card-header
          .team-nameplate-title[data-abbr="NYY"]  (away first, home second)
        .game-card-lineups
          .lineup-card  (away first, home second)
            .lineup-card-player
              .player-nameplate-name  → player name text
    """
    games = []
    game_cards = soup.select('.game-card')
    print(f"  Found {len(game_cards)} game cards on page")

    for card in game_cards:
        # Team abbreviations from header — away is first, home is second
        team_els = card.select('.team-nameplate-title[data-abbr]')
        if len(team_els) < 2:
            continue
        away_abbr = team_els[0]['data-abbr']
        home_abbr = team_els[1]['data-abbr']

        # Lineup cards — away first, home second
        lineup_cards = card.select('.lineup-card')
        if len(lineup_cards) < 2:
            continue

        def extract_players(lineup_card):
            players = []
            for el in lineup_card.select('.lineup-card-player'):
                name_el = el.select_one('.player-nameplate-name')
                if name_el:
                    name = ascii_name(name_el.get_text(strip=True))
                    if name:
                        players.append(name)
            return players[:9]

        games.append({
            'away_abbr': away_abbr,
            'home_abbr': home_abbr,
            'away_players': extract_players(lineup_cards[0]),
            'home_players': extract_players(lineup_cards[1]),
        })

    return games


def match_rg_to_db(rg_games, db_games):
    """Match RotoGrinders games to our database games by team IDs."""
    # Build lookup: (away_team_id, home_team_id) → game record
    db_lookup = {}
    for g in db_games:
        key = (g['away_team_id'], g['home_team_id'])
        db_lookup[key] = g

    matched = []
    for rg in rg_games:
        away_id = TEAM_ABBR_TO_ID.get(rg['away_abbr'])
        home_id = TEAM_ABBR_TO_ID.get(rg['home_abbr'])
        if not away_id or not home_id:
            print(f"  ⚠ Unknown team abbreviation: {rg['away_abbr']} or {rg['home_abbr']}")
            continue
        db_game = db_lookup.get((away_id, home_id))
        if not db_game:
            print(f"  ⚠ No DB game found for {rg['away_abbr']}@{rg['home_abbr']}")
            continue
        matched.append((rg, db_game))
    return matched


def run():
    print("=== RotoGrinders Projected Lineups Loader ===\n")

    # 1. Load our games for today
    db_games = load_todays_games()
    if not db_games:
        print("No games in database for today. Run load_schedule.py first.")
        return

    # 2. Load player name map
    name_map = load_player_name_map()

    # 3. Check which lineups are already confirmed
    game_pks = [g['game_pk'] for g in db_games]
    confirmed = load_confirmed_lineups(game_pks)
    print(f"  {len(confirmed)} team lineups already confirmed — will skip those\n")

    # 4. Fetch and parse RotoGrinders
    print("Fetching RotoGrinders page...")
    soup = fetch_rg_page()
    rg_games = parse_lineups(soup)
    print(f"  Parsed {len(rg_games)} games from RotoGrinders\n")

    if not rg_games:
        print("No games parsed from RotoGrinders. HTML structure may have changed.")
        print("Check the page manually and update parse_lineups().")
        return

    # 5. Match RG games to our DB
    matched = match_rg_to_db(rg_games, db_games)
    print(f"  {len(matched)} games matched to database\n")

    # 6. Build lineup rows
    lineups_to_insert = []
    skipped_confirmed = 0
    unresolved_names = []

    for rg, db_game in matched:
        gm = db_game['game_pk']
        game_date = db_game['game_date']

        for side in ['away', 'home']:
            team_id = db_game[f'{side}_team_id']
            team_name = db_game[f'{side}_team']
            is_home = (side == 'home')
            players = rg[f'{side}_players']

            # Skip if this team already has confirmed lineup
            if (gm, team_id) in confirmed:
                skipped_confirmed += 1
                continue

            if not players:
                continue

            for i, player_name in enumerate(players):
                norm = normalize(player_name)
                match = name_map.get(norm)

                if match:
                    player_id, db_name = match
                else:
                    unresolved_names.append(player_name)
                    player_id = None

                if not player_id:
                    continue  # Can't insert without a valid player_id

                lineups_to_insert.append({
                    "game_pk":       gm,
                    "game_date":     game_date,
                    "team_id":       team_id,
                    "team_name":     team_name,
                    "player_id":     player_id,
                    "player_name":   ascii_name(player_name),
                    "batting_order": i + 1,
                    "is_home":       is_home,
                    "status":        "projected",
                })

    print(f"Projected lineup rows to upsert: {len(lineups_to_insert)}")
    print(f"Skipped (already confirmed): {skipped_confirmed} teams")

    if unresolved_names:
        print(f"\n⚠ {len(unresolved_names)} players not found in DB:")
        for n in sorted(set(unresolved_names)):
            print(f"   {n}")

    # 7. Upsert projected lineups
    if lineups_to_insert:
        # Deduplicate by (game_pk, team_id, batting_order)
        seen = {}
        for row in lineups_to_insert:
            key = (row["game_pk"], row["team_id"], row["batting_order"])
            seen[key] = row
        lineups_to_insert = list(seen.values())

        res = (sb.table("lineups")
                 .upsert(lineups_to_insert,
                         on_conflict="game_pk,team_id,batting_order",
                         ignore_duplicates=False)
                 .execute())
        if hasattr(res, "error") and res.error:
            print(f"  ERROR uploading lineups: {res.error}")
        else:
            print(f"  ✓ {len(lineups_to_insert)} projected lineup entries upserted")
    else:
        print("  No projected lineups to insert.")

    print("\nDone.")


if __name__ == "__main__":
    run()
