#!/usr/bin/env python3
import sys
sys.stdout.reconfigure(encoding='utf-8', line_buffering=True)
"""
Load Odds — The Odds API
Fetches MLB moneylines + totals for today's slate.
Derives implied team totals (home_implied, away_implied) from:
  - Game total (Over/Under line)
  - Moneyline win probabilities (vig-removed)

Upserts into game_odds table, matched to games.game_pk by team name + date.

Free tier: 500 requests/month. One call fetches all games for the day.
Run once per morning in refresh_all.py --morning.

SETUP:
  Add ODDS_API_KEY=<your_key> to your .env file
  Get a free key at https://the-odds-api.com
"""

import os, requests
from datetime import date, datetime, timedelta, timezone
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()
sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])
API_KEY = os.environ.get("ODDS_API_KEY", "")

ODDS_API_BASE = "https://api.the-odds-api.com/v4/sports/baseball_mlb/odds"

# Map The Odds API full team names -> our games table home_team / away_team values
# The Odds API uses "City Team" format; adjust right side to match what's in your DB
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

# Preferred bookmaker order — first match wins
BOOK_PRIORITY = [
    "draftkings", "fanduel", "betmgm", "caesars",
    "pointsbetus", "williamhill_us", "bovada",
]


def american_to_implied_prob(american_odds: int) -> float:
    """Convert American odds to implied probability (raw, not vig-removed)."""
    if american_odds > 0:
        return 100 / (american_odds + 100)
    else:
        return abs(american_odds) / (abs(american_odds) + 100)


def remove_vig(prob_a: float, prob_b: float):
    """Remove vig from two-outcome market. Returns (clean_a, clean_b)."""
    total = prob_a + prob_b
    return prob_a / total, prob_b / total


def best_bookmaker(bookmakers: list, market_key: str) -> dict | None:
    """Return the first bookmaker (by priority) that has the requested market."""
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


def extract_h2h(market: dict) -> tuple[int | None, int | None]:
    """Return (home_ml, away_ml) in American odds from an h2h market."""
    home_ml = away_ml = None
    for outcome in market.get("outcomes", []):
        price = outcome.get("price")
        # The Odds API marks one outcome as home team by name matching
        # We'll return in order: [home, away] by position 0/1 which matches game order
        if outcome.get("name") == market.get("_home_team"):
            home_ml = int(price)
        else:
            away_ml = int(price)
    return home_ml, away_ml


def extract_total(market: dict) -> float | None:
    """Return the Over/Under total line from a totals market."""
    for outcome in market.get("outcomes", []):
        if outcome.get("name") == "Over":
            return float(outcome.get("point", 0))
    return None


def fetch_odds() -> list:
    """Fetch all MLB odds from The Odds API. Returns list of game dicts."""
    params = {
        "apiKey"    : API_KEY,
        "regions"   : "us",
        "markets"   : "h2h,totals",
        "oddsFormat": "american",
        "dateFormat": "iso",
    }
    r = requests.get(ODDS_API_BASE, params=params, timeout=15)

    remaining = r.headers.get("x-requests-remaining", "?")
    used      = r.headers.get("x-requests-used", "?")
    print(f"  Odds API usage: {used} used / {remaining} remaining this month")

    r.raise_for_status()
    return r.json()


def run():
    print("\nLoad Odds — The Odds API")
    print("=" * 40)

    if not API_KEY:
        print("  ERROR: ODDS_API_KEY not set in .env — skipping")
        return

    today = str(date.today())
    tomorrow = str(date.today() + timedelta(days=1))
    print(f"  Date: {today} (also checking {tomorrow} for UTC-shifted games)")

    # Fetch games for today AND tomorrow — late-night games (US time) have
    # UTC commence dates one day ahead, so we need both.
    db_games = (sb.table("games")
                  .select("game_pk, home_team, away_team, game_date")
                  .in_("game_date", [today, tomorrow])
                  .execute().data)

    if not db_games:
        print("  No games in DB for today/tomorrow — skipping")
        return

    print(f"  Games in DB: {len(db_games)} ({today} + {tomorrow})")

    # Build lookup: match by team names.
    # DB uses full names ("New York Yankees"); Odds API also uses full names.
    # TEAM_NAME_MAP maps Odds API names → short DB names, but our DB actually
    # stores full names. Build a reverse map: short → full for flexible matching.
    short_to_full = {v: k for k, v in TEAM_NAME_MAP.items()}

    db_lookup = {}
    for g in db_games:
        # Store both the raw DB name AND the full Odds API name as keys
        home = g["home_team"]
        away = g["away_team"]
        db_lookup[(home, away)] = g
        # Also index by Odds API full name → game, using reverse map
        home_full = short_to_full.get(home, home)
        away_full = short_to_full.get(away, away)
        db_lookup[(home_full, away_full)] = g

    # Fetch odds
    print("  Fetching from The Odds API...")
    try:
        odds_games = fetch_odds()
    except Exception as e:
        print(f"  ERROR fetching odds: {e}")
        return

    print(f"  Games returned by Odds API: {len(odds_games)}")

    records = []
    unmatched = []

    for og in odds_games:
        raw_home = og.get("home_team", "")
        raw_away = og.get("away_team", "")
        mapped_home = TEAM_NAME_MAP.get(raw_home, raw_home)
        mapped_away = TEAM_NAME_MAP.get(raw_away, raw_away)

        # Try both raw Odds API names and mapped short names
        db_game = (db_lookup.get((raw_home, raw_away))
                   or db_lookup.get((mapped_home, mapped_away)))
        if not db_game:
            unmatched.append(f"{raw_away} @ {raw_home}")
            continue
        game_pk = db_game["game_pk"]

        bookmakers = og.get("bookmakers", [])

        # Extract totals
        totals_mkt = best_bookmaker(bookmakers, "totals")
        game_total = extract_total(totals_mkt) if totals_mkt else None

        # Extract moneyline
        h2h_mkt = best_bookmaker(bookmakers, "h2h")
        home_ml = away_ml = None
        home_implied = away_implied = None

        if h2h_mkt:
            # Tag which outcome is home vs away
            h2h_mkt["_home_team"] = raw_home
            outcomes = h2h_mkt.get("outcomes", [])
            home_odds_raw = next((o["price"] for o in outcomes if o["name"] == raw_home), None)
            away_odds_raw = next((o["price"] for o in outcomes if o["name"] == raw_away), None)

            if home_odds_raw is not None and away_odds_raw is not None:
                home_ml = int(home_odds_raw)
                away_ml = int(away_odds_raw)

                # Derive vig-free implied probs
                home_prob_raw = american_to_implied_prob(home_ml)
                away_prob_raw = american_to_implied_prob(away_ml)
                home_prob, away_prob = remove_vig(home_prob_raw, away_prob_raw)

                # Implied team totals = game_total * team win prob
                if game_total:
                    home_implied = round(game_total * home_prob, 2)
                    away_implied = round(game_total * away_prob, 2)

        record = {
            "game_pk"      : game_pk,
            "game_date"    : db_game["game_date"],
            "home_team"    : db_game["home_team"],
            "away_team"    : db_game["away_team"],
            "game_total"   : game_total,
            "home_implied" : home_implied,
            "away_implied" : away_implied,
            "home_ml"      : home_ml,
            "away_ml"      : away_ml,
            "fetched_at"   : datetime.now(timezone.utc).isoformat(),
        }
        records.append(record)

        total_str  = f"O/U {game_total}" if game_total else "no total"
        implied_str = (f"  home={home_implied} / away={away_implied}"
                       if home_implied else "  (no ML for implied totals)")
        print(f"  ✓ {mapped_away} @ {mapped_home} — {total_str}{implied_str}")

    if unmatched:
        print(f"\n  Unmatched games (not in DB): {', '.join(unmatched)}")

    if not records:
        print("  No records to upsert.")
        return

    # Upsert
    (sb.table("game_odds")
       .upsert(records, on_conflict="game_pk", ignore_duplicates=False)
       .execute())

    print(f"\nOdds complete. {len(records)} games uploaded.")


if __name__ == "__main__":
    run()
