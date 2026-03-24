#!/usr/bin/env python3
"""
SlateHub — Master Refresh Script

Modes and when to run them:

  --quick      Every 15 min (all day)
                         Schedule + lineups + weather only. Fast (~30 sec).
                         Catches lineup confirmations and weather updates
                         across all game windows throughout the day.

  --morning    9:00 AM   Full morning pull: last night's bullpen/logs,
                         today's schedule, weather, DK slates/salaries.

  --postgame  11:30 PM   Post-game bullpen pitch counts + today's game logs.

  --stats      7:00 AM daily
                         Season stats, arsenal, park factors. Slow (~15 min).
                         Runs after FanGraphs/Savant process last night's games.

  --splits     7:30 AM daily (after --stats)
                         Excel Power Query refresh + splits upload to Supabase.

  --full                 Runs everything. Use for initial setup only.

Note: DK slates lock at game time — no need to poll DK for lock status.
      DK salaries/slates are pulled once in --morning when they go live.

── WINDOWS TASK SCHEDULER ─────────────────────────────────────────────────────
  Run schedule_tasks.bat as Administrator once to create all tasks.
───────────────────────────────────────────────────────────────────────────────
"""

import sys, subprocess, time
from datetime import datetime

QUICK    = '--quick'    in sys.argv
MORNING  = '--morning'  in sys.argv
POSTGAME = '--postgame' in sys.argv
STATS    = '--stats'    in sys.argv
SPLITS   = '--splits'   in sys.argv
FULL     = '--full'     in sys.argv or not any([QUICK, MORNING, POSTGAME, STATS, SPLITS])

mode = ('FULL'      if FULL     else
        'QUICK'     if QUICK    else
        'MORNING'   if MORNING  else
        'POST-GAME' if POSTGAME else
        'STATS'     if STATS    else 'SPLITS')

def run(script, label, continue_on_fail=False):
    print(f"\n{'='*55}")
    print(f"  {label}")
    print(f"{'='*55}")
    start = time.time()
    result = subprocess.run(['py', '-3.12'] + script.split(), capture_output=False)
    elapsed = time.time() - start
    status = '✓ Done' if result.returncode == 0 else '✗ FAILED'
    print(f"\n{status} — {label} ({elapsed:.1f}s)")
    return result.returncode == 0

print(f"\nSlateHub Refresh — {datetime.now().strftime('%Y-%m-%d %H:%M')}  [{mode}]")

results = {}

# ── QUICK — every 15 minutes all day ─────────────────────────────────────────
# Lightweight: just schedule + weather. Catches lineup confirmations as they
# drop across all game windows (day, afternoon, evening, west coast).
# Visual Crossing free tier handles this easily (~14 calls/run × 96 runs = fine).
if QUICK:
    results['Schedule'] = run('load_schedule.py', 'Schedule & Lineups — lineup confirmation pass')
    results['Weather']  = run('load_weather.py',  'Weather — updated forecast')

# ── MORNING (9:00 AM) ─────────────────────────────────────────────────────────
# Full morning pull: last night's bullpen/logs + DK slates/salaries once live.
# The --quick task handles schedule/weather from here on during the day.
if FULL or MORNING:
    results['Bullpen Usage']   = run('load_bullpen.py',            'Bullpen Appearances — last 14 days (last night results)')
    results['Game Logs']       = run('load_game_logs.py --days 7', 'Batter Game Logs — last 7 days')
    results['Schedule']        = run('load_schedule.py',           'Schedule & Lineups — today + 7 days')
    results['Weather']         = run('load_weather.py',            'Weather — all venues')
    results['DK Slates']       = run('load_dk_slates.py',          'DraftKings — Slate Data')
    results['DK Salaries']     = run('load_dk_salaries.py',        'DraftKings — Salaries')
    results['Odds']            = run('load_odds.py',               'Odds — Implied Totals (The Odds API)')
    results['Projections']     = run('compute_projections.py',     'DFS Projections — Three-Tier Engine')
    results['Ownership']       = run('compute_ownership.py',       'Ownership Projections — Formula Engine')

# ── POST-GAME (11:30 PM) ──────────────────────────────────────────────────────
# After west coast games finish. Captures final pitch counts for all relievers
# and logs today's hitting results. Sets up clean data for tomorrow morning.
if FULL or POSTGAME:
    results['Bullpen (Post-Game)'] = run('load_bullpen.py',            'Bullpen — final pitch counts')
    results['Game Logs (Today)']   = run('load_game_logs.py --days 3', "Game Logs — today's results")

# ── DAILY STATS (7:00 AM) ─────────────────────────────────────────────────────
# Full season stats refresh. FanGraphs and Savant process last night's games
# overnight, so by 7am the previous day's stats are available.
# Takes ~15 minutes. Runs daily so stats are never more than 24 hours stale.
if FULL or STATS:
    results['Rosters']         = run('load_rosters.py',        'Rosters — 40-man roster update')
    results['Player Stats']    = run('load_stats.py',          'Batter & Pitcher Stats — FanGraphs')
    results['Reliever Stats']  = run('load_reliever_stats.py', 'Reliever Stats — FanGraphs')
    results['Pitch Arsenal']   = run('load_arsenal.py',        'Pitch Arsenal — Savant')
    results['Bat Tracking']    = run('load_bat_tracking.py',   'Bat Tracking — Savant swing metrics')
    results['Savant Splits']   = run('load_savant_splits.py',  'Savant xwOBA Splits — by hand')
    # Park factors — seeded once from Savant (seed_park_factors_savant.py), not refreshed daily
    # results['Park Factors']    = run('load_park_factors.py',   'Park Factors — FanGraphs')
    results['League Averages'] = run('seed_league_averages.py','League Averages — reference table')

# ── DAILY SPLITS (7:30 AM, after --stats) ────────────────────────────────────
if FULL or SPLITS:
    results['Excel PQ Refresh'] = run('refresh_excel_splits.py', 'Excel — Power Query Refresh')
    results['Splits Upload']    = run('sync_excel_splits.py',    'Splits — Upload to Supabase')

# ── SUMMARY ───────────────────────────────────────────────────────────────────
print(f"\n{'='*55}")
print(f"  DONE — {mode}  [{datetime.now().strftime('%H:%M:%S')}]")
print(f"{'='*55}")
all_ok = True
for label, ok in results.items():
    print(f"  {'✓' if ok else '✗'} {label}")
    if not ok:
        all_ok = False
print(f"\n{'All phases OK.' if all_ok else 'Some phases FAILED — check output above.'}\n")
