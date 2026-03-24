#!/usr/bin/env python3
"""
seed_park_dimensions.py — One-time static seed for park dimension fields

Adds to the park_factors table (matching on venue_id, season-agnostic):
  lf_dist        — left field wall distance (ft)
  cf_dist        — center field wall distance (ft)
  rf_dist        — right field wall distance (ft)
  lf_wall_height — left field wall height (ft)
  rf_wall_height — right field wall height (ft)
  altitude       — park altitude above sea level (ft)
  roof_type      — 'open', 'dome', or 'retractable'

These fields are static and do not change season to season.
Run once, then only re-run if a park is renovated or a team moves.

Venue IDs match the TEAM_TO_VENUE mapping in load_park_factors.py.

SQL to add columns before running (run in Supabase SQL editor):
  ALTER TABLE park_factors ADD COLUMN IF NOT EXISTS lf_dist INTEGER;
  ALTER TABLE park_factors ADD COLUMN IF NOT EXISTS cf_dist INTEGER;
  ALTER TABLE park_factors ADD COLUMN IF NOT EXISTS rf_dist INTEGER;
  ALTER TABLE park_factors ADD COLUMN IF NOT EXISTS lf_wall_height FLOAT;
  ALTER TABLE park_factors ADD COLUMN IF NOT EXISTS rf_wall_height FLOAT;
  ALTER TABLE park_factors ADD COLUMN IF NOT EXISTS altitude INTEGER;
  ALTER TABLE park_factors ADD COLUMN IF NOT EXISTS roof_type TEXT;
"""

import os, sys
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()
sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])

# ── Park dimensions
# venue_id matches load_park_factors.py TEAM_TO_VENUE mapping
# Sources: Baseball Reference park factors, ESPN park info, Andrew Clem park diagrams
#
# lf_wall_height / rf_wall_height: height of the foul-side power alley wall
#   (the wall that matters most for pull-side HR — not scoreboard or batter's eye)
# altitude: feet above sea level (Coors is 5183, Arizona ~1100, Atlanta ~1050, etc.)

PARK_DIMENSIONS = [
    # venue_id, lf_dist, cf_dist, rf_dist, lf_wall_ht, rf_wall_ht, altitude, roof_type
    # Angels — Angel Stadium, Anaheim CA
    { 'venue_id': 1,  'lf_dist': 347, 'cf_dist': 396, 'rf_dist': 350, 'lf_wall_height': 8.0,  'rf_wall_height': 8.0,  'altitude': 160,  'roof_type': 'open' },
    # Astros — Minute Maid Park, Houston TX (retractable)
    { 'venue_id': 2,  'lf_dist': 315, 'cf_dist': 435, 'rf_dist': 326, 'lf_wall_height': 19.0, 'rf_wall_height': 7.0,  'altitude': 45,   'roof_type': 'retractable' },
    # Athletics — Sutter Health Park, Sacramento CA (2025 temp venue)
    { 'venue_id': 3,  'lf_dist': 330, 'cf_dist': 400, 'rf_dist': 325, 'lf_wall_height': 12.0, 'rf_wall_height': 8.0,  'altitude': 25,   'roof_type': 'open' },
    # Blue Jays — Rogers Centre, Toronto ON (dome)
    { 'venue_id': 4,  'lf_dist': 328, 'cf_dist': 400, 'rf_dist': 328, 'lf_wall_height': 12.0, 'rf_wall_height': 12.0, 'altitude': 287,  'roof_type': 'dome' },
    # Braves — Truist Park, Cumberland GA
    { 'venue_id': 5,  'lf_dist': 335, 'cf_dist': 400, 'rf_dist': 325, 'lf_wall_height': 14.0, 'rf_wall_height': 8.0,  'altitude': 1050, 'roof_type': 'open' },
    # Brewers — American Family Field, Milwaukee WI (retractable)
    { 'venue_id': 6,  'lf_dist': 344, 'cf_dist': 400, 'rf_dist': 345, 'lf_wall_height': 8.0,  'rf_wall_height': 8.0,  'altitude': 635,  'roof_type': 'retractable' },
    # Cardinals — Busch Stadium, St. Louis MO
    { 'venue_id': 7,  'lf_dist': 336, 'cf_dist': 400, 'rf_dist': 335, 'lf_wall_height': 8.0,  'rf_wall_height': 8.0,  'altitude': 465,  'roof_type': 'open' },
    # Cubs — Wrigley Field, Chicago IL
    { 'venue_id': 8,  'lf_dist': 355, 'cf_dist': 400, 'rf_dist': 353, 'lf_wall_height': 11.5, 'rf_wall_height': 11.5, 'altitude': 595,  'roof_type': 'open' },
    # Diamondbacks — Chase Field, Phoenix AZ (retractable)
    { 'venue_id': 9,  'lf_dist': 330, 'cf_dist': 407, 'rf_dist': 334, 'lf_wall_height': 25.0, 'rf_wall_height': 25.0, 'altitude': 1100, 'roof_type': 'retractable' },
    # Dodgers — Dodger Stadium, Los Angeles CA
    { 'venue_id': 10, 'lf_dist': 330, 'cf_dist': 395, 'rf_dist': 330, 'lf_wall_height': 13.5, 'rf_wall_height': 13.5, 'altitude': 515,  'roof_type': 'open' },
    # Giants — Oracle Park, San Francisco CA
    { 'venue_id': 11, 'lf_dist': 339, 'cf_dist': 399, 'rf_dist': 309, 'lf_wall_height': 25.0, 'rf_wall_height': 24.0, 'altitude': 20,   'roof_type': 'open' },
    # Guardians — Progressive Field, Cleveland OH
    { 'venue_id': 12, 'lf_dist': 325, 'cf_dist': 405, 'rf_dist': 325, 'lf_wall_height': 19.0, 'rf_wall_height': 19.0, 'altitude': 653,  'roof_type': 'open' },
    # Mariners — T-Mobile Park, Seattle WA (retractable)
    { 'venue_id': 13, 'lf_dist': 331, 'cf_dist': 401, 'rf_dist': 326, 'lf_wall_height': 8.0,  'rf_wall_height': 8.0,  'altitude': 30,   'roof_type': 'retractable' },
    # Marlins — loanDepot park, Miami FL (retractable)
    { 'venue_id': 14, 'lf_dist': 340, 'cf_dist': 416, 'rf_dist': 335, 'lf_wall_height': 18.0, 'rf_wall_height': 18.0, 'altitude': 6,    'roof_type': 'retractable' },
    # Mets — Citi Field, New York NY
    { 'venue_id': 15, 'lf_dist': 335, 'cf_dist': 408, 'rf_dist': 330, 'lf_wall_height': 16.0, 'rf_wall_height': 15.0, 'altitude': 30,   'roof_type': 'open' },
    # Nationals — Nationals Park, Washington DC
    { 'venue_id': 16, 'lf_dist': 336, 'cf_dist': 402, 'rf_dist': 335, 'lf_wall_height': 8.0,  'rf_wall_height': 8.0,  'altitude': 25,   'roof_type': 'open' },
    # Orioles — Oriole Park at Camden Yards, Baltimore MD
    { 'venue_id': 17, 'lf_dist': 333, 'cf_dist': 410, 'rf_dist': 318, 'lf_wall_height': 25.0, 'rf_wall_height': 7.0,  'altitude': 40,   'roof_type': 'open' },
    # Padres — Petco Park, San Diego CA
    { 'venue_id': 18, 'lf_dist': 336, 'cf_dist': 396, 'rf_dist': 322, 'lf_wall_height': 8.0,  'rf_wall_height': 8.0,  'altitude': 20,   'roof_type': 'open' },
    # Phillies — Citizens Bank Park, Philadelphia PA
    { 'venue_id': 19, 'lf_dist': 329, 'cf_dist': 401, 'rf_dist': 330, 'lf_wall_height': 13.0, 'rf_wall_height': 8.0,  'altitude': 20,   'roof_type': 'open' },
    # Pirates — PNC Park, Pittsburgh PA
    { 'venue_id': 20, 'lf_dist': 325, 'cf_dist': 399, 'rf_dist': 320, 'lf_wall_height': 21.0, 'rf_wall_height': 6.0,  'altitude': 730,  'roof_type': 'open' },
    # Rangers — Globe Life Field, Arlington TX (retractable)
    { 'venue_id': 21, 'lf_dist': 329, 'cf_dist': 407, 'rf_dist': 326, 'lf_wall_height': 14.0, 'rf_wall_height': 14.0, 'altitude': 551,  'roof_type': 'retractable' },
    # Rays — Steinbrenner Field, Tampa FL (open, Yankees spring training park, 2025 temp venue)
    { 'venue_id': 22, 'lf_dist': 318, 'cf_dist': 408, 'rf_dist': 314, 'lf_wall_height': 12.0, 'rf_wall_height': 10.0, 'altitude': 15,   'roof_type': 'open' },
    # Red Sox — Fenway Park, Boston MA
    { 'venue_id': 23, 'lf_dist': 310, 'cf_dist': 420, 'rf_dist': 302, 'lf_wall_height': 37.0, 'rf_wall_height': 3.0,  'altitude': 20,   'roof_type': 'open' },
    # Reds — Great American Ball Park, Cincinnati OH
    { 'venue_id': 24, 'lf_dist': 328, 'cf_dist': 404, 'rf_dist': 325, 'lf_wall_height': 12.0, 'rf_wall_height': 12.0, 'altitude': 490,  'roof_type': 'open' },
    # Rockies — Coors Field, Denver CO
    { 'venue_id': 25, 'lf_dist': 347, 'cf_dist': 415, 'rf_dist': 350, 'lf_wall_height': 14.0, 'rf_wall_height': 14.0, 'altitude': 5183, 'roof_type': 'open' },
    # Royals — Kauffman Stadium, Kansas City MO
    { 'venue_id': 26, 'lf_dist': 330, 'cf_dist': 410, 'rf_dist': 330, 'lf_wall_height': 9.0,  'rf_wall_height': 9.0,  'altitude': 910,  'roof_type': 'open' },
    # Tigers — Comerica Park, Detroit MI
    { 'venue_id': 27, 'lf_dist': 345, 'cf_dist': 420, 'rf_dist': 330, 'lf_wall_height': 14.0, 'rf_wall_height': 7.0,  'altitude': 585,  'roof_type': 'open' },
    # Twins — Target Field, Minneapolis MN
    { 'venue_id': 28, 'lf_dist': 339, 'cf_dist': 404, 'rf_dist': 328, 'lf_wall_height': 8.0,  'rf_wall_height': 23.0, 'altitude': 838,  'roof_type': 'open' },
    # White Sox — Guaranteed Rate Field, Chicago IL
    { 'venue_id': 29, 'lf_dist': 330, 'cf_dist': 400, 'rf_dist': 335, 'lf_wall_height': 8.0,  'rf_wall_height': 8.0,  'altitude': 595,  'roof_type': 'open' },
    # Yankees — Yankee Stadium, New York NY
    { 'venue_id': 30, 'lf_dist': 318, 'cf_dist': 408, 'rf_dist': 314, 'lf_wall_height': 8.0,  'rf_wall_height': 8.0,  'altitude': 55,   'roof_type': 'open' },
]


def run():
    print("\nseed_park_dimensions.py — Static park dimension seed")
    print("=" * 52)
    print(f"  Parks to update: {len(PARK_DIMENSIONS)}")
    print()

    updated = 0
    errors  = 0

    for park in PARK_DIMENSIONS:
        vid = park['venue_id']
        dims = {k: v for k, v in park.items() if k != 'venue_id'}

        try:
            # Update all rows for this venue_id (covers multiple seasons)
            res = (sb.table('park_factors')
                     .update(dims)
                     .eq('venue_id', vid)
                     .execute())
            rows_touched = len(res.data) if res.data else 0
            print(f"  venue_id={vid:2d}  LF={park['lf_dist']}  CF={park['cf_dist']}  "
                  f"RF={park['rf_dist']}  LF_ht={park['lf_wall_height']}  "
                  f"alt={park['altitude']}  roof={park['roof_type']}  "
                  f"→ {rows_touched} row(s) updated")
            updated += 1
        except Exception as e:
            print(f"  ERROR venue_id={vid}: {e}")
            errors += 1

    print(f"\nDone. {updated} parks updated, {errors} errors.")

    if errors:
        print("\nIf you see column-not-found errors, run this SQL in Supabase first:")
        print("  ALTER TABLE park_factors ADD COLUMN IF NOT EXISTS lf_dist INTEGER;")
        print("  ALTER TABLE park_factors ADD COLUMN IF NOT EXISTS cf_dist INTEGER;")
        print("  ALTER TABLE park_factors ADD COLUMN IF NOT EXISTS rf_dist INTEGER;")
        print("  ALTER TABLE park_factors ADD COLUMN IF NOT EXISTS lf_wall_height FLOAT;")
        print("  ALTER TABLE park_factors ADD COLUMN IF NOT EXISTS rf_wall_height FLOAT;")
        print("  ALTER TABLE park_factors ADD COLUMN IF NOT EXISTS altitude INTEGER;")
        print("  ALTER TABLE park_factors ADD COLUMN IF NOT EXISTS roof_type TEXT;")


if __name__ == "__main__":
    run()
