"""
Backfill projection_history for all historical game dates.
Uses the current Bayesian model to project every past slate.
Run this once to build the full history for postgame analysis.
"""
import sys
sys.stdout.reconfigure(encoding='utf-8', line_buffering=True)

from sim_projections import run
from datetime import date, timedelta
from dotenv import load_dotenv
load_dotenv()

import os
from supabase import create_client

sb = create_client(os.environ['SUPABASE_URL'], os.environ['SUPABASE_KEY'])

# Get all game dates
games = sb.table('games').select('game_date').gte('game_date', '2026-03-26').lte('game_date', '2026-04-22').order('game_date').execute().data
dates = sorted(set(g['game_date'] for g in games))

print(f"Backfilling {len(dates)} dates: {dates[0]} to {dates[-1]}")
print(f"Using 500 sims per date for speed (production uses 2000+)")
print()

for i, d in enumerate(dates):
    print(f"\n{'='*60}")
    print(f"  [{i+1}/{len(dates)}] Backfilling {d}")
    print(f"{'='*60}")
    try:
        sys.argv = ['sim_projections.py', '--date', d, '--sims', '500']
        run()
    except Exception as e:
        print(f"  ERROR on {d}: {e}")
        continue

print(f"\n\nBackfill complete. {len(dates)} dates processed.")
