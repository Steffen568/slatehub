#!/usr/bin/env python3
"""
load_sb_data.py — Load stolen base data for the SB probability model.

Two data sources:
1. Catcher pop time (Baseball Savant via pybaseball) → catcher_poptime table
2. Pitcher SB allowed (MLB Stats API) → pitcher_stats columns (sb_allowed, cs_allowed, sb_per_9, etc.)

Usage:
  py -3.12 load_sb_data.py              # Load current + prior season
  py -3.12 load_sb_data.py --season 2025  # Specific season only
"""
import os, sys, requests, time
from datetime import datetime
from supabase import create_client
from dotenv import load_dotenv
from pybaseball import statcast_catcher_poptime

load_dotenv()
sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])

CURRENT_SEASON = datetime.now().year if datetime.now().month >= 3 else datetime.now().year - 1


def load_catcher_poptime(season):
    """Pull catcher pop time from Baseball Savant and upsert to Supabase."""
    print(f"\n  Catcher Pop Time — {season}")
    try:
        df = statcast_catcher_poptime(season, min_2b_att=1)
    except Exception as e:
        print(f"    ERROR fetching pop time: {e}")
        return 0

    if df is None or df.empty:
        print("    No data returned")
        return 0

    import math
    def clean(v):
        """Convert pandas value to JSON-safe Python type. NaN → None."""
        if v is None: return None
        try:
            f = float(v)
            return None if math.isnan(f) else f
        except (ValueError, TypeError):
            return None

    rows = []
    for _, r in df.iterrows():
        pid = r.get('entity_id')
        if not pid:
            continue
        rows.append({
            'player_id': int(pid),
            'season': season,
            'full_name': str(r.get('entity_name', '')),
            'team_id': int(r['team_id']) if clean(r.get('team_id')) else None,
            'pop_2b': clean(r.get('pop_2b_sba')),
            'pop_2b_cs': clean(r.get('pop_2b_cs')),
            'pop_2b_sb': clean(r.get('pop_2b_sb')),
            'pop_2b_attempts': int(r['pop_2b_sba_count']) if clean(r.get('pop_2b_sba_count')) else None,
            'exchange': clean(r.get('exchange_2b_3b_sba')),
            'arm_strength': clean(r.get('maxeff_arm_2b_3b_sba')),
        })

    if not rows:
        print("    No valid rows")
        return 0

    # Upsert in batches
    for i in range(0, len(rows), 50):
        batch = rows[i:i+50]
        sb.table('catcher_poptime').upsert(batch, on_conflict='player_id,season').execute()

    print(f"    Upserted {len(rows)} catchers")
    return len(rows)


def load_pitcher_sb(season):
    """Pull pitcher SB allowed from MLB Stats API and update pitcher_stats."""
    print(f"\n  Pitcher SB Allowed — {season}")

    # Get all pitcher IDs from pitcher_stats for this season
    pitcher_ids = []
    for i in range(0, 2000, 1000):
        rows = sb.table('pitcher_stats').select('player_id').eq('season', season).range(i, i+999).execute().data or []
        pitcher_ids.extend([r['player_id'] for r in rows])
        if len(rows) < 1000:
            break

    if not pitcher_ids:
        print("    No pitchers found in pitcher_stats")
        return 0

    print(f"    Fetching SB data for {len(pitcher_ids)} pitchers...")

    updated = 0
    # Batch by 50 to avoid rate limits
    for batch_start in range(0, len(pitcher_ids), 50):
        batch = pitcher_ids[batch_start:batch_start+50]
        ids_param = ','.join(str(pid) for pid in batch)

        # MLB Stats API: hydrate with season stats for multiple players
        for pid in batch:
            try:
                url = f'https://statsapi.mlb.com/api/v1/people/{pid}/stats?stats=season&season={season}&group=pitching'
                resp = requests.get(url, timeout=10)
                if resp.status_code != 200:
                    continue
                data = resp.json()
                splits = data.get('stats', [{}])[0].get('splits', [])
                if not splits:
                    continue
                stat = splits[0].get('stat', {})

                sb_allowed = stat.get('stolenBases')
                cs_allowed = stat.get('caughtStealing')
                wild_pitches = stat.get('wildPitches')
                pickoffs = stat.get('pickoffs')
                ip_str = stat.get('inningsPitched', '0')

                # Parse IP (e.g., "150.1" → 150.333)
                try:
                    parts = str(ip_str).split('.')
                    ip = int(parts[0]) + (int(parts[1]) / 3.0 if len(parts) > 1 else 0)
                except:
                    ip = 0

                sb_per_9 = round((sb_allowed or 0) / ip * 9, 2) if ip > 0 else None
                sb_pct_val = None
                if sb_allowed is not None and cs_allowed is not None and (sb_allowed + cs_allowed) > 0:
                    sb_pct_val = round(sb_allowed / (sb_allowed + cs_allowed), 3)

                update = {}
                if sb_allowed is not None: update['sb_allowed'] = sb_allowed
                if cs_allowed is not None: update['cs_allowed'] = cs_allowed
                if sb_pct_val is not None: update['sb_pct'] = sb_pct_val
                if wild_pitches is not None: update['wild_pitches'] = wild_pitches
                if pickoffs is not None: update['pickoffs'] = pickoffs
                if sb_per_9 is not None: update['sb_per_9'] = sb_per_9

                if update:
                    sb.table('pitcher_stats').update(update).eq('player_id', pid).eq('season', season).execute()
                    updated += 1

            except Exception as e:
                continue  # skip individual failures

        # Brief pause between batches to be polite to the API
        if batch_start + 50 < len(pitcher_ids):
            time.sleep(0.5)

    print(f"    Updated {updated} pitchers with SB data")
    return updated


if __name__ == '__main__':
    season_arg = None
    if '--season' in sys.argv:
        idx = sys.argv.index('--season')
        if idx + 1 < len(sys.argv):
            season_arg = int(sys.argv[idx + 1])

    seasons = [season_arg] if season_arg else [CURRENT_SEASON, CURRENT_SEASON - 1]

    print(f"SlateHub SB Data Loader — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"Seasons: {seasons}")

    for season in seasons:
        load_catcher_poptime(season)
        load_pitcher_sb(season)

    print("\nDone.")
