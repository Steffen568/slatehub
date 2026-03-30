"""Temporarily backdate one game for testing late-swap lock detection.

Usage:
  py -3.12 test_backdate_game.py backdate   ← sets NYY@SF game_time_utc to 2 hours ago
  py -3.12 test_backdate_game.py restore    ← restores the original time

The game_pk for NYY@SF on 2026-03-27 is 823243.
Original game_time_utc: 2026-03-27T20:35:00+00:00
"""
import sys
sys.stdout.reconfigure(encoding='utf-8', line_buffering=True)

import os
from datetime import datetime, timezone, timedelta
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()
sb = create_client(os.getenv('SUPABASE_URL'), os.getenv('SUPABASE_KEY'))

GAME_PK = 823243
ORIGINAL_TIME = '2026-03-27T20:35:00+00:00'

action = sys.argv[1] if len(sys.argv) > 1 else ''

if action == 'backdate':
    past_time = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
    print(f"Setting game {GAME_PK} (NYY@SF) game_time_utc to {past_time}")
    sb.table('games').update({'game_time_utc': past_time}).eq('game_pk', GAME_PK).execute()
    print("Done. NYY and SF players should now show as LOCKED in the lineup builder.")
    print(f"\nRun 'py -3.12 test_backdate_game.py restore' when done testing.")

elif action == 'restore':
    print(f"Restoring game {GAME_PK} (NYY@SF) game_time_utc to {ORIGINAL_TIME}")
    sb.table('games').update({'game_time_utc': ORIGINAL_TIME}).eq('game_pk', GAME_PK).execute()
    print("Done. Original time restored.")

else:
    # Show current state
    res = sb.table('games').select('game_pk,away_team,home_team,game_time_utc').eq('game_pk', GAME_PK).execute()
    if res.data:
        g = res.data[0]
        print(f"Game {GAME_PK}: {g['away_team']} @ {g['home_team']}")
        print(f"  Current game_time_utc: {g['game_time_utc']}")
        print(f"  Original:              {ORIGINAL_TIME}")
    print(f"\nUsage:")
    print(f"  py -3.12 test_backdate_game.py backdate   ← set to 2 hours ago")
    print(f"  py -3.12 test_backdate_game.py restore    ← restore original time")
