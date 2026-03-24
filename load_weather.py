#!/usr/bin/env python3
"""Phase 6 — Weather data for all games (MLB + Spring Training + WBC)"""

import os, requests
from datetime import date, timedelta
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()
sb  = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])
WX  = os.environ["VISUAL_CROSSING_KEY"]

# ── ALL venues (MLB regular season + Spring Training + WBC sites)
# venue_id : "City, State" for weather API lookup
OUTDOOR_VENUES = {
    # ── MLB Regular Season ──────────────────────────────────────────
    5    : "Anaheim, CA",          # Angel Stadium
    4    : "Oakland, CA",          # Oakland Coliseum
    2    : "Phoenix, AZ",          # Chase Field (retractable - skip if needed)
    17   : "Atlanta, GA",          # Truist Park
    2392 : "Cumberland, GA",       # Truist Park alt
    3    : "Baltimore, MD",        # Camden Yards
    11   : "Boston, MA",           # Fenway Park
    19   : "Chicago, IL",          # Wrigley Field
    18   : "Chicago, IL",          # Guaranteed Rate Field
    12   : "Cincinnati, OH",       # Great American Ball Park
    5150 : "Cleveland, OH",        # Progressive Field
    22   : "Cleveland, OH",        # Progressive Field alt
    20   : "Denver, CO",           # Coors Field
    23   : "Detroit, MI",          # Comerica Park
    24   : "Houston, TX",          # Minute Maid Park
    7    : "Kansas City, MO",      # Kauffman Stadium
    13   : "Los Angeles, CA",      # Dodger Stadium
    1    : "New York, NY",         # Yankee Stadium
    3289 : "New York, NY",         # Citi Field
    14   : "Oakland, CA",          # Oakland Coliseum alt
    15   : "Philadelphia, PA",     # Citizens Bank Park
    16   : "Pittsburgh, PA",       # PNC Park
    2889 : "Arlington, TX",        # Globe Life Field
    2680 : "San Diego, CA",        # Petco Park
    26   : "San Francisco, CA",    # Oracle Park
    2681 : "Seattle, WA",          # T-Mobile Park
    29   : "St. Louis, MO",        # Busch Stadium
    2756 : "St. Petersburg, FL",   # Tropicana Field
    30   : "Toronto, ON",          # Rogers Centre
    3313 : "Washington, DC",       # Nationals Park
    32   : "Miami, FL",            # loanDepot park
    4169 : "Milwaukee, WI",        # American Family Field
    5325 : "Minneapolis, MN",      # Target Field
    2395 : "New York, NY",         # Citi Field alt
    4705 : "Sacramento, CA",       # Sutter Health Park (Athletics)

    # ── Spring Training — Grapefruit League (Florida) ────────────────
    2520 : "Jupiter, FL",           # Roger Dean Chevrolet Stadium (Marlins/Cardinals)
    2523 : "Tampa, FL",             # George M. Steinbrenner Field (Yankees)
    2526 : "Bradenton, FL",         # LECOM Park (Pirates)
    2534 : "Port Charlotte, FL",    # Charlotte Sports Park (Rays)
    2700 : "Clearwater, FL",        # BayCare Ballpark (Phillies)
    5000 : "West Palm Beach, FL",   # CACTI Park of the Palm Beaches (Nationals/Astros)
    5380 : "North Port, FL",        # CoolToday Park (Braves)
    2489 : "Fort Myers, FL",        # JetBlue Park (Red Sox)
    2490 : "Fort Myers, FL",        # Hammond Stadium (Twins)
    2502 : "Sarasota, FL",          # Ed Smith Stadium (Orioles)
    2503 : "Port St. Lucie, FL",    # Clover Park (Mets)
    2505 : "Lakeland, FL",          # Publix Field at Joker Marchant (Tigers)
    2507 : "Dunedin, FL",           # TD Ballpark (Blue Jays)
    2508 : "Clearwater, FL",        # Spectrum Field alt
    2516 : "Kissimmee, FL",         # Osceola County Stadium (Astros alt)

    # ── Spring Training — Cactus League (Arizona) ────────────────────
    2500 : "Tempe, AZ",             # Tempe Diablo Stadium (Angels)
    2530 : "Peoria, AZ",            # Peoria Stadium (Mariners/Padres)
    2603 : "Surprise, AZ",          # Surprise Stadium (Royals/Rangers)
    3809 : "Glendale, AZ",          # Camelback Ranch (Dodgers/White Sox)
    3834 : "Goodyear, AZ",          # Goodyear Ballpark (Reds/Guardians)
    4249 : "Scottsdale, AZ",        # Salt River Fields at Talking Stick (Rockies/DBacks)
    4629 : "Mesa, AZ",              # Sloan Park (Cubs)
    2509 : "Scottsdale, AZ",        # Scottsdale Stadium (Giants)
    2510 : "Phoenix, AZ",           # American Family Fields (Brewers)
    2511 : "Mesa, AZ",              # Hohokam Stadium (Athletics)
    2513 : "Tempe, AZ",             # Diablo Stadium alt
    4914 : "Mesa, AZ",              # Mesa Solar Sox / Cubs alt

    # ── Spring Training — missing / alternate IDs ───────────────────
    2518 : "Phoenix, AZ",           # American Family Fields of Phoenix (Brewers)
    2856 : "Port St. Lucie, FL",    # Clover Park (Mets) alt ID
    4309 : "Fort Myers, FL",        # JetBlue Park (Red Sox) alt ID
    5355 : "Las Vegas, NV",         # Las Vegas Ballpark (Athletics)
    2862 : "Fort Myers, FL",        # Lee Health Sports Complex (Twins)
    2532 : "Scottsdale, AZ",        # Scottsdale Stadium (Giants) alt ID
    2536 : "Dunedin, FL",           # TD Ballpark (Blue Jays) alt ID

    # ── WBC / International sites ────────────────────────────────────
    5103 : "Miami, FL",             # loanDepot park (WBC)
    4962 : "Phoenix, AZ",           # Chase Field (WBC)
    2680 : "San Diego, CA",         # Petco Park (WBC)
    5325 : "Minneapolis, MN",       # Target Field (WBC)
}

def get_weather(city, game_date):
    url = (f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services"
           f"/timeline/{requests.utils.quote(city)}/{game_date}"
           f"?unitGroup=us&key={WX}&include=hours&contentType=json")
    r = requests.get(url, timeout=10)
    r.raise_for_status()
    return r.json()

def deg_to_compass(deg):
    dirs = ["N","NNE","NE","ENE","E","ESE","SE","SSE",
            "S","SSW","SW","WSW","W","WNW","NW","NNW"]
    return dirs[round(deg / 22.5) % 16]

def run():
    today = str(date.today())
    print(f"\nFetching weather for {today}...")

    games = sb.table("games").select(
        "game_pk,venue_id,venue_name,game_date,game_time_utc"
    ).eq("game_date", today).execute().data

    if not games:
        print("No games today.")
        return

    loaded, skipped = 0, 0
    for g in games:
        vid  = g["venue_id"]
        city = OUTDOOR_VENUES.get(vid)
        if not city:
            print(f"  SKIP (no city mapping): {g['venue_name']} (venue_id={vid})")
            skipped += 1
            continue

        try:
            wx   = get_weather(city, g["game_date"])
            day  = wx["days"][0]

            # Find the hour closest to game time
            hour_data = day
            if g.get("game_time_utc") and "hours" in day:
                from datetime import datetime
                gt = datetime.fromisoformat(g["game_time_utc"].replace("Z","+00:00"))
                local_hour = gt.hour  # approximate
                closest = min(day["hours"], key=lambda h: abs(int(h["datetime"][:2]) - local_hour))
                hour_data = closest

            record = {
                "game_pk"     : g["game_pk"],
                "venue_name"  : g["venue_name"],
                "game_date"   : g["game_date"],
                "temp_f"      : hour_data.get("temp", day.get("temp")),
                "feels_like_f": hour_data.get("feelslike", day.get("feelslike")),
                "humidity"    : hour_data.get("humidity", day.get("humidity")),
                "wind_speed"  : hour_data.get("windspeed", day.get("windspeed")),
                "wind_dir"    : deg_to_compass(hour_data.get("winddir", day.get("winddir", 0))),
                "wind_deg"    : hour_data.get("winddir", day.get("winddir")),
                "conditions"  : hour_data.get("conditions", day.get("conditions","")),
                "precip_pct"  : hour_data.get("precipprob", day.get("precipprob", 0)),
                "visibility"  : hour_data.get("visibility", day.get("visibility")),
                "dew_point"   : hour_data.get("dew", day.get("dew")),
                "is_outdoor"  : True,
            }

            (sb.table("weather")
               .upsert(record, on_conflict="game_pk", ignore_duplicates=False)
               .execute())
            print(f"  ✓ {g['venue_name']} — {record['temp_f']}°F, {record['conditions']}, wind {record['wind_speed']} mph {record['wind_dir']}")
            loaded += 1

        except Exception as e:
            print(f"  ERROR {g['venue_name']}: {e}")
            skipped += 1

    print(f"\nWeather complete. Loaded: {loaded}  Skipped: {skipped}")

if __name__ == "__main__":
    run()
    