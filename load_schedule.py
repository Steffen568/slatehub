#!/usr/bin/env python3
import sys
sys.stdout.reconfigure(encoding='utf-8', line_buffering=True)
"""
Phase 4 — Schedule, Games & Lineups
Loads games for today + next 7 days from MLB API
Uses previous game batting order as projected lineup when official not available

v2: Added player ID resolution — cross-references MLB API IDs against
    Chadwick Register (players table) by name when direct ID lookup fails.
    This prevents silent '--' in the frontend for players like Carlos Correa
    where the MLB API player_id differs from our MLBAM ID.
"""

import os, requests, unicodedata
from datetime import date, timedelta
from supabase import create_client
from dotenv import load_dotenv

def ascii_name(name):
    """Normalize player name to ASCII — strips accents (José → Jose) so names match
    across sources (MLB API uses accented chars, DK uses ASCII).
    Prevents replacement-character corruption (Jos\ufffd) when terminals/DB mishandle UTF-8."""
    if not name:
        return name
    nfkd = unicodedata.normalize('NFKD', str(name))
    return nfkd.encode('ASCII', 'ignore').decode('ASCII').strip()

load_dotenv()
sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])

MLB_BASE = "https://statsapi.mlb.com/api/v1"

def get_schedule(game_date, sport_id=1):
    url = f"{MLB_BASE}/schedule?sportId={sport_id}&date={game_date}&hydrate=probablePitcher,lineups,team,venue,weather"
    r = requests.get(url, timeout=15)
    r.raise_for_status()
    return r.json()

# Sport IDs to pull — MLB (1) + WBC / World Baseball Classic (51)
SPORT_IDS = [1, 51]

def parse_roof_status(condition):
    """Extract roof status from MLB weather condition string. Returns 'Closed', 'Open', or None."""
    if not condition:
        return None
    c = condition.lower()
    if 'roof closed' in c or 'dome' in c:
        return 'Closed'
    if 'roof open' in c:
        return 'Open'
    return None

def get_prev_lineup(team_id, before_date):
    """Get the most recent confirmed lineup for a team before a given date"""
    res = (sb.table("lineups")
             .select("*")
             .eq("team_id", team_id)
             .lt("game_date", str(before_date))
             .eq("status", "confirmed")
             .order("game_date", desc=True)
             .limit(9)
             .execute())
    return res.data or []

def load_pitcher_hands():
    """Load pitcher throwing hand from rosters table"""
    res = sb.table("rosters").select("player_id, throws").not_.is_("throws", "null").execute()
    return {r["player_id"]: r["throws"] for r in (res.data or [])}

def normalize(name):
    """Normalize a name for fuzzy matching"""
    if not name or not isinstance(name, str):
        return ''
    nfkd = unicodedata.normalize('NFKD', name)
    return nfkd.encode('ASCII', 'ignore').decode('ASCII').strip().lower()

def load_player_id_maps():
    """
    Load lookup maps from the players table:
    1. mlbam_id_set   — set of all known MLBAM IDs (for fast existence check)
    2. name_to_mlbam  — normalized name -> mlbam_id (fallback when ID doesn't match)
    3. mlbam_to_name  — mlbam_id -> 'First Last' (fallback when API returns no name)
    """
    print("Loading player ID maps from database...")
    all_players = []
    offset = 0
    while True:
        result = sb.table('players').select(
            'mlbam_id, name_normalized, first_name, last_name'
        ).range(offset, offset + 999).execute()
        if not result.data:
            break
        all_players.extend(result.data)
        if len(result.data) < 1000:
            break
        offset += 1000

    mlbam_id_set  = set()
    name_to_mlbam = {}
    mlbam_to_name = {}

    for p in all_players:
        if p['mlbam_id']:
            mlbam_id_set.add(p['mlbam_id'])
            parts = [p.get('first_name'), p.get('last_name')]
            full  = ' '.join(x for x in parts if x)
            if full:
                mlbam_to_name[p['mlbam_id']] = full
        if p['name_normalized'] and p['mlbam_id']:
            if p['name_normalized'] not in name_to_mlbam:
                name_to_mlbam[p['name_normalized']] = p['mlbam_id']

    print(f"  {len(mlbam_id_set):,} known MLBAM IDs")
    print(f"  {len(name_to_mlbam):,} name mappings")
    print(f"  {len(mlbam_to_name):,} ID-to-name mappings")
    return mlbam_id_set, name_to_mlbam, mlbam_to_name

def resolve_player_id(mlb_api_id, player_name, mlbam_id_set, name_to_mlbam, mismatches):
    """
    Resolve the correct MLBAM ID for a player.
    1. If MLB API ID exists in our players table → use it directly
    2. If not → look up by normalized name
    3. If still not found → use MLB API ID as fallback (better than nothing)

    Logs mismatches for the validation report.
    """
    if mlb_api_id in mlbam_id_set:
        return mlb_api_id  # Direct match — happy path

    # Fallback: name-based lookup
    norm_name = normalize(player_name)
    if norm_name in name_to_mlbam:
        resolved_id = name_to_mlbam[norm_name]
        mismatches.append({
            'player_name' : player_name,
            'mlb_api_id'  : mlb_api_id,
            'resolved_id' : resolved_id,
            'method'      : 'name_lookup'
        })
        return resolved_id

    # No match found — use MLB API ID as last resort
    mismatches.append({
        'player_name' : player_name,
        'mlb_api_id'  : mlb_api_id,
        'resolved_id' : mlb_api_id,
        'method'      : 'unresolved'
    })
    return mlb_api_id

def run():
    today = date.today()
    pitcher_hands = load_pitcher_hands()
    print(f"Loaded {len(pitcher_hands)} pitcher hand records from rosters")

    # Load player ID maps for resolution
    mlbam_id_set, name_to_mlbam, mlbam_to_name = load_player_id_maps()

    # --today flag: only load today's games (used by --quick pipeline)
    today_only = '--today' in sys.argv
    if today_only:
        dates_to_load = [today]
        print("  Mode: --today (single day)")
    else:
        dates_to_load = [today + timedelta(days=i) for i in range(8)]

    total_games   = 0
    total_lineups = 0
    all_mismatches = []  # Collect all ID mismatches for report

    for load_date in dates_to_load:
        date_str = str(load_date)
        print(f"\nLoading {date_str}...")

        games_data = []
        for sport_id in SPORT_IDS:
            try:
                data = get_schedule(date_str, sport_id)
                sport_games = data.get("dates", [{}])[0].get("games", []) if data.get("dates") else []
                if sport_games:
                    label = "MLB" if sport_id == 1 else f"sportId={sport_id}"
                    print(f"  {label}: {len(sport_games)} games")
                games_data.extend(sport_games)
            except Exception as e:
                print(f"  ERROR fetching sportId={sport_id}: {e}")
        if not games_data:
            print(f"  No games found")
            continue
        print(f"  Total: {len(games_data)} games")

        games_to_insert   = []
        lineups_to_insert = []

        for g in games_data:
            # Skip doubleheader Game 2 with TBD start time (not on any DK slate)
            if g.get("status", {}).get("startTimeTBD", False):
                away_name = g.get("teams", {}).get("away", {}).get("team", {}).get("name", "?")
                home_name = g.get("teams", {}).get("home", {}).get("team", {}).get("name", "?")
                print(f"  SKIP TBD game: {away_name} @ {home_name} (gamePk={g.get('gamePk')})")
                continue

            gm   = g.get("gamePk")
            home = g.get("teams", {}).get("home", {})
            away = g.get("teams", {}).get("away", {})
            venue = g.get("venue", {})
            status = g.get("status", {}).get("detailedState", "Scheduled")

            home_sp = home.get("probablePitcher", {})
            away_sp = away.get("probablePitcher", {})

            # Try MLB API pitchHand first (works for spring training), fall back to rosters table
            home_sp_hand = home_sp.get('pitchHand', {}).get('code') or pitcher_hands.get(home_sp.get("id"))
            away_sp_hand = away_sp.get('pitchHand', {}).get('code') or pitcher_hands.get(away_sp.get("id"))

            mlb_condition = g.get("weather", {}).get("condition", "")
            roof_status   = parse_roof_status(mlb_condition)

            game_record = {
                "game_pk"       : gm,
                "game_date"     : date_str,
                "game_time_utc" : g.get("gameDate"),
                "status"        : status,
                "home_team"     : home.get("team", {}).get("name"),
                "home_team_id"  : home.get("team", {}).get("id"),
                "away_team"     : away.get("team", {}).get("name"),
                "away_team_id"  : away.get("team", {}).get("id"),
                "venue_id"      : venue.get("id"),
                "venue_name"    : venue.get("name"),
                "home_sp_id"    : home_sp.get("id"),
                "home_sp_name"  : home_sp.get("fullName"),
                "home_sp_hand"  : home_sp_hand,
                "away_sp_id"    : away_sp.get("id"),
                "away_sp_name"  : away_sp.get("fullName"),
                "away_sp_hand"  : away_sp_hand,
                "roof_status"   : roof_status,
            }
            games_to_insert.append(game_record)

            # ── Lineups
            lineups = g.get("lineups", {})
            home_batters = lineups.get("homePlayers", [])
            away_batters = lineups.get("awayPlayers", [])

            def process_lineup(batters, team_id, team_name, is_home):
                entries = []
                for i, p in enumerate(batters):
                    raw_id = p.get("id")
                    name   = p.get("fullName", "") or p.get("name", "")

                    # ── Resolve to correct MLBAM ID
                    resolved_id = resolve_player_id(
                        raw_id, name, mlbam_id_set, name_to_mlbam, all_mismatches
                    )

                    # ── Fill name from our DB if API didn't provide one
                    if not name and resolved_id:
                        name = mlbam_to_name.get(resolved_id, "")

                    entries.append({
                        "game_pk"      : gm,
                        "game_date"    : date_str,
                        "team_id"      : team_id,
                        "team_name"    : team_name,
                        "player_id"    : resolved_id,
                        "player_name"  : ascii_name(name) or None,
                        "batting_order": i + 1,
                        "position"     : p.get("primaryPosition", {}).get("abbreviation"),
                        "is_home"      : is_home,
                        "status"       : "confirmed",
                    })
                return entries

            if home_batters:
                lineups_to_insert += process_lineup(
                    home_batters,
                    home.get("team",{}).get("id"),
                    home.get("team",{}).get("name"),
                    True
                )
            if away_batters:
                lineups_to_insert += process_lineup(
                    away_batters,
                    away.get("team",{}).get("id"),
                    away.get("team",{}).get("name"),
                    False
                )

            if not home_batters:
                prev = get_prev_lineup(home.get("team",{}).get("id"), load_date)
                for p in prev:
                    p2 = dict(p)
                    p2["game_pk"]   = gm
                    p2["game_date"] = date_str
                    p2["status"]    = "projected"
                    p2.pop("id", None)
                    lineups_to_insert.append(p2)

            if not away_batters:
                prev = get_prev_lineup(away.get("team",{}).get("id"), load_date)
                for p in prev:
                    p2 = dict(p)
                    p2["game_pk"]   = gm
                    p2["game_date"] = date_str
                    p2["status"]    = "projected"
                    p2.pop("id", None)
                    lineups_to_insert.append(p2)

        # Upload games — ignore_duplicates=False so roof_status + SP hands update on re-runs
        if games_to_insert:
            res = (sb.table("games")
                     .upsert(games_to_insert, on_conflict="game_pk", ignore_duplicates=False)
                     .execute())
            if hasattr(res, "error") and res.error:
                print(f"  ERROR uploading games: {res.error}")
            else:
                print(f"  ✓ {len(games_to_insert)} games uploaded")
                total_games += len(games_to_insert)

        # Deduplicate lineups by (game_pk, team_id, batting_order) — handles API returning same game twice
        seen = {}
        for row in lineups_to_insert:
            key = (row["game_pk"], row["team_id"], row["batting_order"])
            seen[key] = row
        lineups_to_insert = list(seen.values())

        # Upload lineups
        if lineups_to_insert:
            res = (sb.table("lineups")
                     .upsert(lineups_to_insert, on_conflict="game_pk,team_id,batting_order", ignore_duplicates=False)
                     .execute())
            if hasattr(res, "error") and res.error:
                print(f"  ERROR uploading lineups: {res.error}")
            else:
                print(f"  ✓ {len(lineups_to_insert)} lineup entries uploaded")
                total_lineups += len(lineups_to_insert)

    # ── Print mismatch report
    print(f"\n{'='*60}")
    print(f"PLAYER ID RESOLUTION REPORT")
    print(f"{'='*60}")

    name_lookups  = [m for m in all_mismatches if m['method'] == 'name_lookup']
    unresolved    = [m for m in all_mismatches if m['method'] == 'unresolved']

    if name_lookups:
        print(f"\n⚠️  {len(name_lookups)} players resolved by name (MLB API ID didn't match):")
        for m in name_lookups:
            print(f"   {m['player_name']:<25} MLB API:{m['mlb_api_id']} → Resolved:{m['resolved_id']}")
    else:
        print(f"\n✅ All player IDs matched directly — no name lookups needed")

    if unresolved:
        print(f"\n❌ {len(unresolved)} players UNRESOLVED (using MLB API ID as fallback):")
        for m in unresolved:
            print(f"   {m['player_name']:<25} MLB API ID: {m['mlb_api_id']} — NOT in players table")
    else:
        print(f"✅ No unresolved players")

    print(f"\nPhase 4 complete.")
    print(f"  Total games loaded:   {total_games}")
    print(f"  Total lineup entries: {total_lineups}")
    print(f"  ID mismatches fixed:  {len(name_lookups)}")
    print(f"  Unresolved players:   {len(unresolved)}")

if __name__ == "__main__":
    run()
    