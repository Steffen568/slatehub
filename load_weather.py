#!/usr/bin/env python3
"""Phase 6 — Weather data from PropFinder API (Kevin Roth's baseball-specific forecasts)"""
import sys
sys.stdout.reconfigure(encoding='utf-8', line_buffering=True)

import os, requests
from datetime import date, datetime, timezone
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()
sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])

PROPFINDER_API = "https://api.propfinder.app"

# weatherIndicator values from PropFinder → standardized delay_risk strings
INDICATOR_MAP = {
    'Green':  'Clear',
    'Yellow': 'Chance For Delay',
    'Orange': 'Delay Likely',
    'Red':    'Postponement Likely',
}

def deg_to_compass(deg):
    dirs = ["N","NNE","NE","ENE","E","ESE","SE","SSE",
            "S","SSW","SW","WSW","W","WNW","NW","NNW"]
    return dirs[round(deg / 22.5) % 16]


def fetch_propfinder(target_date):
    """Fetch weather data from PropFinder API. Returns (games_list, notes_dict)."""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/json',
        'Referer': 'https://propfinder.app/weather',
    }

    # Game weather data
    resp = requests.get(f"{PROPFINDER_API}/mlb/weather-games",
                        params={'date': target_date}, headers=headers, timeout=20)
    resp.raise_for_status()
    games = resp.json() or []

    # Forecaster notes (keyed by gameId)
    notes_resp = requests.get(f"{PROPFINDER_API}/mlb/weather-notes",
                              headers=headers, timeout=20)
    notes_by_game = {}
    if notes_resp.status_code == 200:
        for note in (notes_resp.json() or []):
            gid = note.get('gameId')
            if gid:
                # Keep latest note per game (list is newest-first)
                if gid not in notes_by_game:
                    notes_by_game[gid] = note.get('content', '')

    return games, notes_by_game


def find_gametime_weather(weather_data, game_date_iso):
    """Find the hourly weather entry closest to game time.
    game_date_iso is like '2026-04-07T17:10:00Z'."""
    if not weather_data or not game_date_iso:
        return None

    game_dt = datetime.fromisoformat(game_date_iso.replace('Z', '+00:00'))
    game_hour = game_dt.hour

    # Find the entry matching the game hour
    for wx in weather_data:
        dt_str = wx.get('dateTime', '')
        try:
            hour = int(dt_str.split(':')[0])
        except (ValueError, IndexError):
            continue
        if hour == game_hour:
            return wx

    # Fallback: return the entry closest to game hour
    best = None
    best_diff = 999
    for wx in weather_data:
        try:
            hour = int(wx.get('dateTime', '').split(':')[0])
            diff = abs(hour - game_hour)
            if diff < best_diff:
                best_diff = diff
                best = wx
        except (ValueError, IndexError):
            continue
    return best


def run():
    today = str(date.today())
    print(f"\nFetching weather from PropFinder API for {today}...")

    # Get today's games from our DB
    db_games = sb.table("games").select(
        "game_pk,venue_name,game_date,home_team,away_team"
    ).eq("game_date", today).execute().data

    if not db_games:
        print("No games today.")
        return

    # Fetch from PropFinder API
    pf_games, pf_notes = fetch_propfinder(today)
    print(f"  PropFinder API returned {len(pf_games)} games")

    # Index PropFinder games by game ID (same as MLB game_pk)
    pf_by_id = {g['id']: g for g in pf_games}

    loaded, skipped = 0, 0
    for g in db_games:
        gpk = g["game_pk"]
        pf = pf_by_id.get(gpk)

        if not pf:
            print(f"  SKIP (not in PropFinder API): {g['venue_name']} (game_pk={gpk})")
            skipped += 1
            continue

        # Find game-time weather from hourly data
        wx = find_gametime_weather(pf.get('weatherData'), pf.get('gameDate'))
        if not wx:
            print(f"  SKIP (no weather data): {g['venue_name']} (game_pk={gpk})")
            skipped += 1
            continue

        temp_f = wx.get('temp')
        if temp_f is None:
            print(f"  SKIP (no temp): {g['venue_name']} (game_pk={gpk})")
            skipped += 1
            continue

        wind_deg = wx.get('windDir')
        wind_dir = deg_to_compass(wind_deg) if wind_deg is not None else None
        precip_pct = wx.get('precipProb', 0)
        humidity = wx.get('humidity')
        wind_speed = wx.get('windSpeed')
        conditions = wx.get('conditions')
        feels_like = wx.get('feelsLike')

        # Delay risk from weatherIndicator field
        indicator = pf.get('weatherIndicator')
        delay_risk = INDICATOR_MAP.get(indicator) if indicator else None

        # Forecaster notes
        forecaster_notes = pf_notes.get(gpk)

        # Determine if outdoor (indoor venues have no weatherIndicator)
        is_outdoor = indicator is not None

        record = {
            "game_pk"          : gpk,
            "venue_name"       : g["venue_name"],
            "game_date"        : g["game_date"],
            "temp_f"           : temp_f,
            "feels_like_f"     : feels_like,
            "humidity"         : humidity,
            "wind_speed"       : wind_speed,
            "wind_dir"         : wind_dir,
            "wind_deg"         : wind_deg,
            "conditions"       : conditions,
            "precip_pct"       : precip_pct,
            "visibility"       : wx.get('visibility'),
            "dew_point"        : wx.get('dew'),
            "is_outdoor"       : is_outdoor,
            "delay_risk"       : delay_risk,
            "forecaster_notes" : forecaster_notes,
        }

        try:
            sb.table("weather").upsert(
                record, on_conflict="game_pk", ignore_duplicates=False
            ).execute()
            notes_flag = " [NOTES]" if forecaster_notes else ""
            delay_flag = f" [{delay_risk}]" if delay_risk else ""
            print(f"  OK {g['venue_name']} — {temp_f}F, {conditions}, "
                  f"wind {wind_speed}mph {wind_dir}, precip {precip_pct}%"
                  f"{delay_flag}{notes_flag}")
            loaded += 1
        except Exception as e:
            print(f"  ERROR {g['venue_name']}: {e}")
            skipped += 1

    print(f"\nWeather complete. Loaded: {loaded}  Skipped: {skipped}")


if __name__ == "__main__":
    run()
