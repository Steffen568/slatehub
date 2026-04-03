#!/usr/bin/env python3
import sys
sys.stdout.reconfigure(encoding='utf-8', line_buffering=True)
"""
Load Pitcher Props — The Odds API
Fetches pitcher_outs and pitcher_strikeouts props for today's games.
Converts outs to implied IP, matches to MLBAM player IDs via games table.
Upserts into pitcher_props table.

Free tier: 500 requests/month. 1 call per event + 1 for event list.
~16 calls/day for a full 15-game slate.

Run AFTER load_odds.py (so game_odds exist) and load_lineups.py (so games have SP IDs).
"""

import os, requests, unicodedata
from datetime import date, timedelta
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()
sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])
API_KEY = os.environ.get("ODDS_API_KEY", "")

ODDS_API_BASE = "https://api.the-odds-api.com/v4/sports/baseball_mlb"

# Map Odds API full team names -> DB team names (same as load_odds.py)
TEAM_NAME_MAP = {
    "Arizona Diamondbacks"     : "D-backs",
    "Atlanta Braves"           : "Braves",
    "Baltimore Orioles"        : "Orioles",
    "Boston Red Sox"           : "Red Sox",
    "Chicago Cubs"             : "Cubs",
    "Chicago White Sox"        : "White Sox",
    "Cincinnati Reds"          : "Reds",
    "Cleveland Guardians"      : "Guardians",
    "Colorado Rockies"         : "Rockies",
    "Detroit Tigers"           : "Tigers",
    "Houston Astros"           : "Astros",
    "Kansas City Royals"       : "Royals",
    "Los Angeles Angels"       : "Angels",
    "Los Angeles Dodgers"      : "Dodgers",
    "Miami Marlins"            : "Marlins",
    "Milwaukee Brewers"        : "Brewers",
    "Minnesota Twins"          : "Twins",
    "New York Mets"            : "Mets",
    "New York Yankees"         : "Yankees",
    "Oakland Athletics"        : "Athletics",
    "Philadelphia Phillies"    : "Phillies",
    "Pittsburgh Pirates"       : "Pirates",
    "San Diego Padres"         : "Padres",
    "San Francisco Giants"     : "Giants",
    "Seattle Mariners"         : "Mariners",
    "St. Louis Cardinals"      : "Cardinals",
    "Tampa Bay Rays"           : "Rays",
    "Texas Rangers"            : "Rangers",
    "Toronto Blue Jays"        : "Blue Jays",
    "Washington Nationals"     : "Nationals",
}

# Preferred books — first match wins
BOOK_PRIORITY = [
    "draftkings", "fanduel", "betmgm", "caesars",
    "pointsbetus", "williamhill_us", "bovada", "betonlineag",
]


def best_line(bookmakers, market_key):
    """Return the best available line for a market from preferred bookmaker."""
    book_map = {b["key"]: b for b in bookmakers}
    for pref in BOOK_PRIORITY:
        if pref in book_map:
            for mkt in book_map[pref].get("markets", []):
                if mkt["key"] == market_key:
                    return mkt
    # Fallback: first available
    for b in bookmakers:
        for mkt in b.get("markets", []):
            if mkt["key"] == market_key:
                return mkt
    return None


def extract_prop_line(market):
    """Extract the O/U point from a prop market. Returns float or None."""
    if not market:
        return None
    for outcome in market.get("outcomes", []):
        if outcome.get("name") == "Over" and outcome.get("point") is not None:
            return float(outcome["point"])
    return None


def extract_prop_players(market):
    """Extract all player lines from a prop market. Returns {description: point}."""
    if not market:
        return {}
    players = {}
    for outcome in market.get("outcomes", []):
        if outcome.get("name") == "Over" and outcome.get("description"):
            players[outcome["description"]] = float(outcome.get("point", 0))
    return players


def norm_name(name):
    """Normalize a player name for matching: strip accents, lowercase, collapse spaces."""
    if not name:
        return ""
    # Strip accents (é → e, etc.)
    nfkd = unicodedata.normalize('NFKD', name)
    ascii_only = ''.join(c for c in nfkd if not unicodedata.combining(c))
    return ascii_only.lower().strip()


def fetch_prizepicks():
    """Fetch pitcher props from PrizePicks. Returns {norm_name: {ks, outs, team}}."""
    try:
        from curl_cffi import requests as cffi
    except ImportError:
        print("  PrizePicks: curl_cffi not installed — skipping")
        return {}

    try:
        r = cffi.get("https://api.prizepicks.com/projections?league_id=2",
                      impersonate="chrome", timeout=15)
        if r.status_code != 200:
            print(f"  PrizePicks: HTTP {r.status_code}")
            return {}
    except Exception as e:
        print(f"  PrizePicks: ERROR {e}")
        return {}

    data = r.json()

    # Build player lookup from included
    players = {}
    for item in data.get("included", []):
        if item["type"] == "new_player":
            attrs = item.get("attributes", {})
            if not attrs.get("combo"):  # skip combo players
                players[item["id"]] = attrs

    # Extract pitcher props
    pp = {}  # norm_name → {ks, outs, team, display_name}
    for proj in data.get("data", []):
        attrs = proj.get("attributes", {})
        st = attrs.get("stat_type", "")
        if st not in ("Pitcher Strikeouts", "Pitching Outs"):
            continue

        player_ref = proj.get("relationships", {}).get("new_player", {}).get("data", {})
        pid = player_ref.get("id")
        player = players.get(pid)
        if not player:
            continue

        name = norm_name(player.get("display_name", ""))
        if not name:
            continue

        if name not in pp:
            pp[name] = {"ks": None, "outs": None, "team": player.get("team", ""),
                        "display_name": player.get("display_name", "")}

        line = attrs.get("line_score")
        if line is not None:
            line = float(line)
            if st == "Pitcher Strikeouts" and (pp[name]["ks"] is None or line < pp[name]["ks"]):
                # Take the lowest main line (avoid alternate lines)
                pp[name]["ks"] = line
            elif st == "Pitching Outs" and (pp[name]["outs"] is None or line < pp[name]["outs"]):
                pp[name]["outs"] = line

    return pp


# DK team abbreviation map for matching PrizePicks team codes to DB teams
PP_TEAM_MAP = {
    "ARI": "ARI", "ATL": "ATL", "BAL": "BAL", "BOS": "BOS",
    "CHC": "CHC", "CWS": "CWS", "CIN": "CIN", "CLE": "CLE",
    "COL": "COL", "DET": "DET", "HOU": "HOU", "KC": "KC",
    "LAA": "LAA", "LAD": "LAD", "MIA": "MIA", "MIL": "MIL",
    "MIN": "MIN", "NYM": "NYM", "NYY": "NYY", "OAK": "ATH",
    "PHI": "PHI", "PIT": "PIT", "SD": "SD", "SF": "SF",
    "SEA": "SEA", "STL": "STL", "TB": "TB", "TEX": "TEX",
    "TOR": "TOR", "WSH": "WSH",
}


def run():
    print("\nLoad Pitcher Props — The Odds API + PrizePicks")
    print("=" * 50)

    today = str(date.today())
    tomorrow = str(date.today() + timedelta(days=1))
    print(f"  Date: {today}")

    # Load games from DB to get SP IDs and game_pks
    db_games = (sb.table("games")
                  .select("game_pk, home_team, away_team, game_date, "
                          "home_sp_id, away_sp_id, home_sp_name, away_sp_name")
                  .in_("game_date", [today, tomorrow])
                  .execute().data)
    if not db_games:
        print("  No games in DB — skipping")
        return
    print(f"  Games in DB: {len(db_games)}")

    # Build SP lookup: player_id → {game_pk, name, team, game_date}
    sp_lookup = {}  # norm_name → {player_id, game_pk, name, team, game_date}
    for g in db_games:
        for sp_id, sp_name, team in [
            (g.get("home_sp_id"), g.get("home_sp_name"), g.get("home_team")),
            (g.get("away_sp_id"), g.get("away_sp_name"), g.get("away_team")),
        ]:
            if sp_id and sp_name:
                sp_lookup[norm_name(sp_name)] = {
                    "player_id": sp_id, "game_pk": g["game_pk"],
                    "name": sp_name, "team": team, "game_date": g["game_date"],
                }

    # Build lookup: Odds API team names -> game rows
    full_to_short = TEAM_NAME_MAP
    short_to_full = {v: k for k, v in TEAM_NAME_MAP.items()}
    db_lookup = {}
    for g in db_games:
        home = g["home_team"]
        away = g["away_team"]
        home_variants = {home, full_to_short.get(home, home), short_to_full.get(home, home)}
        away_variants = {away, full_to_short.get(away, away), short_to_full.get(away, away)}
        for h in home_variants:
            for a in away_variants:
                db_lookup[f"{h}|{a}"] = g

    records = {}  # player_id → record (dedup key)

    # ── Source 1: The Odds API ──────────────────────────────────────────────
    if API_KEY:
        print("\n  [Odds API]")
        try:
            r = requests.get(f"{ODDS_API_BASE}/events",
                             params={"apiKey": API_KEY}, timeout=15)
            r.raise_for_status()
            events = r.json()
            remaining = r.headers.get("x-requests-remaining", "?")
            print(f"  Events: {len(events)}, API remaining: {remaining}")
            api_calls = 1

            for ev in events:
                home_api = ev["home_team"]
                away_api = ev["away_team"]
                eid = ev["id"]

                key = f"{home_api}|{away_api}"
                game = db_lookup.get(key)
                if not game:
                    key = f"{away_api}|{home_api}"
                    game = db_lookup.get(key)
                if not game:
                    continue

                try:
                    r2 = requests.get(f"{ODDS_API_BASE}/events/{eid}/odds",
                                      params={
                                          "apiKey": API_KEY,
                                          "regions": "us",
                                          "markets": "pitcher_outs,pitcher_strikeouts",
                                          "oddsFormat": "american",
                                      }, timeout=15)
                    api_calls += 1
                    if r2.status_code != 200:
                        continue
                except Exception:
                    continue

                data = r2.json()
                bookmakers = data.get("bookmakers", [])
                if not bookmakers:
                    continue

                outs_market = best_line(bookmakers, "pitcher_outs")
                ks_market = best_line(bookmakers, "pitcher_strikeouts")
                outs_players = extract_prop_players(outs_market)
                ks_players = extract_prop_players(ks_market)
                all_prop_names = set(outs_players.keys()) | set(ks_players.keys())

                for role, sp_id, sp_name, team in [
                    ("home", game.get("home_sp_id"), game.get("home_sp_name"), game.get("home_team")),
                    ("away", game.get("away_sp_id"), game.get("away_sp_name"), game.get("away_team")),
                ]:
                    if not sp_id or not sp_name:
                        continue
                    matched_prop = None
                    for prop_name in all_prop_names:
                        if prop_name.lower() == sp_name.lower():
                            matched_prop = prop_name
                            break
                        prop_last = prop_name.split()[-1].lower() if prop_name else ""
                        db_last = sp_name.split()[-1].lower() if sp_name else ""
                        if prop_last == db_last and prop_last:
                            matched_prop = prop_name
                            break
                    if not matched_prop:
                        continue

                    outs_line = outs_players.get(matched_prop)
                    ks_line = ks_players.get(matched_prop)
                    if outs_line is None and ks_line is None:
                        continue

                    records[sp_id] = {
                        "game_pk": game["game_pk"], "game_date": game["game_date"],
                        "player_id": sp_id, "player_name": sp_name, "team": team,
                        "outs_line": outs_line, "strikeouts_line": ks_line,
                        "implied_ip": round(outs_line / 3, 2) if outs_line else None,
                        "implied_ks": ks_line,
                    }
                    parts = []
                    if outs_line: parts.append(f"IP={round(outs_line/3, 2)}")
                    if ks_line: parts.append(f"Ks={ks_line}")
                    print(f"    {sp_name:25s} {' | '.join(parts)}")

            remaining_final = r2.headers.get("x-requests-remaining", "?") if events else remaining
            print(f"  Odds API: {len(records)} lines, {api_calls} calls, {remaining_final} remaining")
        except Exception as e:
            print(f"  Odds API error: {e}")
    else:
        print("  ODDS_API_KEY not set — skipping Odds API")

    # ── Source 2: PrizePicks (fill gaps) ────────────────────────────────────
    print("\n  [PrizePicks]")
    pp_data = fetch_prizepicks()
    print(f"  PrizePicks pitchers found: {len(pp_data)}")

    pp_filled = 0
    pp_updated = 0
    for pp_name, pp_info in pp_data.items():
        # Match to DB SP by normalized name
        sp = sp_lookup.get(pp_name)
        if not sp:
            # Try last-name match
            pp_last = pp_name.split()[-1] if pp_name else ""
            for db_norm, db_sp in sp_lookup.items():
                if db_norm.split()[-1] == pp_last and pp_last:
                    sp = db_sp
                    break
        if not sp:
            continue

        pid = sp["player_id"]
        outs = pp_info.get("outs")
        ks = pp_info.get("ks")

        if pid not in records:
            # New pitcher not in Odds API
            records[pid] = {
                "game_pk": sp["game_pk"], "game_date": sp["game_date"],
                "player_id": pid, "player_name": sp["name"], "team": sp["team"],
                "outs_line": outs, "strikeouts_line": ks,
                "implied_ip": round(outs / 3, 2) if outs else None,
                "implied_ks": ks,
            }
            parts = []
            if outs: parts.append(f"IP={round(outs/3, 2)}")
            if ks: parts.append(f"Ks={ks}")
            print(f"    {sp['name']:25s} {' | '.join(parts)}  (new)")
            pp_filled += 1
        else:
            # Fill in gaps where Odds API had partial data
            existing = records[pid]
            updated = False
            if existing["outs_line"] is None and outs is not None:
                existing["outs_line"] = outs
                existing["implied_ip"] = round(outs / 3, 2)
                updated = True
            if existing["strikeouts_line"] is None and ks is not None:
                existing["strikeouts_line"] = ks
                existing["implied_ks"] = ks
                updated = True
            if updated:
                print(f"    {sp['name']:25s} gap-filled")
                pp_updated += 1

    print(f"  PrizePicks: {pp_filled} new + {pp_updated} gap-filled")

    # ── Upsert ──────────────────────────────────────────────────────────────
    all_records = list(records.values())
    if not all_records:
        print("\n  No pitcher props found — nothing to upsert")
        return

    sb.table("pitcher_props").upsert(
        all_records, on_conflict="game_pk,player_id"
    ).execute()
    print(f"\n  Total: {len(all_records)} pitcher prop lines upserted")


if __name__ == "__main__":
    run()
