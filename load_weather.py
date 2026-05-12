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
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/json',
        'Referer': 'https://propfinder.app/weather',
    }

    resp = requests.get(f"{PROPFINDER_API}/mlb/weather-games",
                        params={'date': target_date}, headers=headers, timeout=20)
    resp.raise_for_status()
    games = resp.json() or []

    notes_resp = requests.get(f"{PROPFINDER_API}/mlb/weather-notes",
                              headers=headers, timeout=20)
    notes_by_game = {}
    if notes_resp.status_code == 200:
        for note in (notes_resp.json() or []):
            gid = note.get('gameId')
            if gid and gid not in notes_by_game:
                notes_by_game[gid] = note.get('content', '')

    return games, notes_by_game


def find_gametime_weather(weather_data, game_date_iso):
    """Find the hourly entry closest to game time using epoch timestamps."""
    if not weather_data or not game_date_iso:
        return None

    try:
        game_epoch = datetime.fromisoformat(game_date_iso.replace('Z', '+00:00')).timestamp()
    except (ValueError, AttributeError):
        return None

    best, best_diff = None, float('inf')
    for wx in weather_data:
        epoch = wx.get('dateTimeEpoch')
        if epoch is not None:
            diff = abs(epoch - game_epoch)
            if diff < best_diff:
                best_diff = diff
                best = wx
    return best


def run():
    today = str(date.today())
    print(f"\nFetching weather from PropFinder API for {today}...")

    db_games = sb.table("games").select(
        "game_pk,venue_name,game_date,home_team,away_team"
    ).eq("game_date", today).execute().data

    if not db_games:
        print("No games today.")
        return

    pf_games, pf_notes = fetch_propfinder(today)
    print(f"  PropFinder API returned {len(pf_games)} games")

    pf_by_id = {g['id']: g for g in pf_games}

    loaded, skipped = 0, 0
    for g in db_games:
        gpk = g["game_pk"]
        pf = pf_by_id.get(gpk)

        if not pf:
            print(f"  SKIP (not in PropFinder API): {g['venue_name']} (game_pk={gpk})")
            skipped += 1
            continue

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
        indicator = pf.get('weatherIndicator')

        record = {
            "game_pk"          : gpk,
            "venue_name"       : g["venue_name"],
            "game_date"        : g["game_date"],
            "temp_f"           : temp_f,
            "feels_like_f"     : wx.get('feelsLike'),
            "humidity"         : wx.get('humidity'),
            "wind_speed"       : wx.get('windSpeed'),
            "wind_dir"         : wind_dir,
            "wind_deg"         : wind_deg,
            "conditions"       : wx.get('conditions'),
            "precip_pct"       : wx.get('precipProb', 0),
            "visibility"       : wx.get('visibility'),
            "dew_point"        : wx.get('dew'),
            "is_outdoor"       : indicator is not None,
            "delay_risk"       : INDICATOR_MAP.get(indicator) if indicator else None,
            "forecaster_notes" : pf_notes.get(gpk),
            "cf_bearing"       : (pf.get('ballpark') or {}).get('azimuthAngle'),
        }

        try:
            sb.table("weather").upsert(
                record, on_conflict="game_pk", ignore_duplicates=False
            ).execute()
            notes_flag = " [NOTES]" if record["forecaster_notes"] else ""
            delay_flag = f" [{record['delay_risk']}]" if record["delay_risk"] else ""
            print(f"  OK {g['venue_name']} — {temp_f}F, {wx.get('conditions')}, "
                  f"wind {wx.get('windSpeed')}mph {wind_dir}, precip {wx.get('precipProb',0)}%"
                  f"{delay_flag}{notes_flag}")
            loaded += 1
        except Exception as e:
            print(f"  ERROR {g['venue_name']}: {e}")
            skipped += 1

    print(f"\nWeather complete. Loaded: {loaded}  Skipped: {skipped}")


if __name__ == "__main__":
    run()
