#!/usr/bin/env python3
import sys
sys.stdout.reconfigure(encoding='utf-8', line_buffering=True)
"""
Load Odds — ESPN Scoreboard API (powered by DraftKings lines)
Fetches MLB moneylines + totals for today's slate.
Derives implied team totals (home_implied, away_implied) from:
  - Game total (Over/Under line)
  - Moneyline win probabilities (vig-removed)

No API key needed. Free, unlimited calls.
"""

import os, requests
from datetime import date, datetime, timedelta, timezone
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()
sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])

ESPN_SCOREBOARD = "https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/scoreboard"

# Map ESPN team displayName -> our games table team names
# ESPN uses "Chicago Cubs", our DB uses "Chicago Cubs" (same format, mostly)
# Only map the ones that differ
TEAM_NAME_MAP = {
    "Oakland Athletics": "Athletics",
}


def american_to_implied_prob(odds_str: str) -> float | None:
    """Convert American odds string ('+108', '-130') to implied probability."""
    if not odds_str:
        return None
    try:
        odds = int(odds_str.replace("+", ""))
    except ValueError:
        return None
    if odds > 0:
        return 100 / (odds + 100)
    else:
        return abs(odds) / (abs(odds) + 100)


def remove_vig(prob_a: float, prob_b: float):
    """Remove vig from two-outcome market. Returns (clean_a, clean_b)."""
    total = prob_a + prob_b
    if total == 0:
        return 0.5, 0.5
    return prob_a / total, prob_b / total


def fetch_espn_odds(game_date: str) -> list:
    """Fetch MLB scoreboard with odds from ESPN for a given date."""
    date_fmt = game_date.replace("-", "")  # ESPN wants YYYYMMDD
    resp = requests.get(ESPN_SCOREBOARD, params={"dates": date_fmt}, timeout=15)
    resp.raise_for_status()
    return resp.json().get("events", [])


def run():
    print("\nLoad Odds — ESPN (DraftKings lines)")
    print("=" * 45)

    today = str(date.today())
    tomorrow = str(date.today() + timedelta(days=1))
    print(f"  Date: {today}")

    # Fetch games from our DB
    db_games = (sb.table("games")
                  .select("game_pk, home_team, away_team, game_date")
                  .in_("game_date", [today, tomorrow])
                  .execute().data)

    if not db_games:
        print("  No games in DB for today/tomorrow — skipping")
        return

    print(f"  Games in DB: {len(db_games)}")

    # Build lookup by (away, home, game_date) to avoid collisions when the
    # same teams play on consecutive days (e.g. series games).
    db_lookup = {}
    for g in db_games:
        home = g["home_team"]
        away = g["away_team"]
        gd   = g["game_date"]
        db_lookup[(away, home, gd)] = g
        for k, v in TEAM_NAME_MAP.items():
            if home == v:
                db_lookup[(away, k, gd)] = g
            if away == v:
                db_lookup[(k, home, gd)] = g

    # Fetch ESPN scoreboard for today (and tomorrow for late UTC games)
    # Tag each event with the request date so we can match to the right DB game
    all_events = []
    for d in [today, tomorrow]:
        try:
            events = fetch_espn_odds(d)
            for ev in events:
                ev["_request_date"] = d
            all_events.extend(events)
        except Exception as e:
            print(f"  ERROR fetching ESPN for {d}: {e}")

    print(f"  ESPN events: {len(all_events)}")

    records = []
    unmatched = []
    seen_pks = set()

    for ev in all_events:
        comp = ev.get("competitions", [{}])[0]
        competitors = comp.get("competitors", [])
        if len(competitors) < 2:
            continue

        home = next((t for t in competitors if t.get("homeAway") == "home"), {})
        away = next((t for t in competitors if t.get("homeAway") == "away"), {})
        home_name = home.get("team", {}).get("displayName", "")
        away_name = away.get("team", {}).get("displayName", "")
        req_date = ev.get("_request_date", today)

        # Match to our DB game using team names + date
        db_game = db_lookup.get((away_name, home_name, req_date))
        if not db_game:
            mapped_home = TEAM_NAME_MAP.get(home_name, home_name)
            mapped_away = TEAM_NAME_MAP.get(away_name, away_name)
            db_game = db_lookup.get((mapped_away, mapped_home, req_date))
        if not db_game:
            unmatched.append(f"{away_name} @ {home_name} ({req_date})")
            continue

        game_pk = db_game["game_pk"]
        if game_pk in seen_pks:
            continue  # Deduplicate (same game from today + tomorrow fetch)
        seen_pks.add(game_pk)

        # Extract odds
        odds_list = comp.get("odds", [])
        if not odds_list:
            continue
        odds = odds_list[0]

        game_total = odds.get("overUnder")
        ml_data = odds.get("moneyline", {})
        home_ml_str = ml_data.get("home", {}).get("close", {}).get("odds")
        away_ml_str = ml_data.get("away", {}).get("close", {}).get("odds")

        home_ml = int(home_ml_str.replace("+", "")) if home_ml_str else None
        away_ml = int(away_ml_str.replace("+", "")) if away_ml_str else None

        home_implied = away_implied = None
        if game_total and home_ml_str and away_ml_str:
            home_prob_raw = american_to_implied_prob(home_ml_str)
            away_prob_raw = american_to_implied_prob(away_ml_str)
            if home_prob_raw and away_prob_raw:
                home_prob, away_prob = remove_vig(home_prob_raw, away_prob_raw)
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

        total_str = f"O/U {game_total}" if game_total else "no total"
        implied_str = (f"  home={home_implied} / away={away_implied}"
                       if home_implied else "  (no ML for implied)")
        print(f"  OK {away_name:24s} @ {home_name:24s} — {total_str}{implied_str}")

    if unmatched:
        print(f"\n  Unmatched: {', '.join(set(unmatched))}")

    if not records:
        print("  No records to upsert.")
        return

    # Upsert
    sb.table("game_odds").upsert(
        records, on_conflict="game_pk", ignore_duplicates=False
    ).execute()

    print(f"\nOdds complete. {len(records)} games uploaded.")


if __name__ == "__main__":
    run()
