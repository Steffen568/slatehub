#!/usr/bin/env python3
import sys
sys.stdout.reconfigure(encoding='utf-8', line_buffering=True)
"""
load_actuals.py — Load actual DK fantasy points from MLB boxscores

Pulls completed game boxscores from the MLB Stats API, computes actual
DraftKings fantasy points for every batter and pitcher, stores in
actual_results table for backtesting projections.

Usage:
  py -3.12 load_actuals.py                    # today
  py -3.12 load_actuals.py --date 2026-03-28  # specific date
  py -3.12 load_actuals.py --days 7           # last N days

SQL migration (run once in Supabase):
  CREATE TABLE IF NOT EXISTS actual_results (
    player_id   INTEGER NOT NULL,
    game_pk     INTEGER NOT NULL,
    game_date   DATE    NOT NULL,
    full_name   TEXT,
    team        TEXT,
    is_pitcher  BOOLEAN DEFAULT FALSE,
    -- Batter stats
    pa  INTEGER, ab INTEGER, h INTEGER,
    singles INTEGER, doubles INTEGER, triples INTEGER, hr INTEGER,
    r INTEGER, rbi INTEGER, bb INTEGER, hbp INTEGER, sb INTEGER, cs INTEGER, k INTEGER,
    -- Pitcher stats
    ip FLOAT, p_k INTEGER, p_er INTEGER, p_h INTEGER, p_bb INTEGER,
    p_hr INTEGER, win BOOLEAN, loss BOOLEAN, cg BOOLEAN, sho BOOLEAN,
    -- DK scoring
    actual_dk_pts FLOAT,
    PRIMARY KEY (player_id, game_pk)
  );
"""

import os, requests
from datetime import date, datetime, timedelta
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()
sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])

MLB_API = "https://statsapi.mlb.com/api/v1"


def compute_batter_dk_pts(stats: dict) -> float:
    """Compute DraftKings fantasy points for a batter's game."""
    singles = stats.get('singles', 0)
    doubles = stats.get('doubles', 0)
    triples = stats.get('triples', 0)
    hr      = stats.get('hr', 0)
    r       = stats.get('r', 0)
    rbi     = stats.get('rbi', 0)
    bb      = stats.get('bb', 0)
    hbp     = stats.get('hbp', 0)
    sb      = stats.get('sb', 0)

    return (
        singles * 3 +
        doubles * 5 +
        triples * 8 +
        hr      * 10 +
        r       * 2 +
        rbi     * 2 +
        bb      * 2 +
        hbp     * 2 +
        sb      * 5
    )


def compute_pitcher_dk_pts(stats: dict) -> float:
    """Compute DraftKings fantasy points for a pitcher's game."""
    ip  = stats.get('ip', 0.0)
    k   = stats.get('p_k', 0)
    er  = stats.get('p_er', 0)
    h   = stats.get('p_h', 0)
    bb  = stats.get('p_bb', 0)
    win = stats.get('win', False)
    cg  = stats.get('cg', False)
    sho = stats.get('sho', False)
    # No-hitter: CG with 0 hits
    nh  = cg and h == 0

    pts = (
        ip  * 2.25 +
        k   * 2.0 +
        (4.0 if win else 0) -
        er  * 2.0 -
        h   * 0.6 -
        bb  * 0.6 +
        (3.0 if cg else 0) +
        (3.0 if sho else 0) +
        (5.0 if nh else 0)
    )
    return round(pts, 1)


def parse_ip(ip_str) -> float:
    """Convert MLB API innings pitched string (e.g. '6.1') to actual IP.
    MLB notation: 6.1 = 6 1/3 IP, 6.2 = 6 2/3 IP."""
    if ip_str is None:
        return 0.0
    ip = float(ip_str)
    whole = int(ip)
    frac = ip - whole
    # .1 = 1/3, .2 = 2/3
    if abs(frac - 0.1) < 0.05:
        return whole + 1/3
    elif abs(frac - 0.2) < 0.05:
        return whole + 2/3
    return ip


def load_boxscore(game_pk: int) -> list:
    """Fetch boxscore for one game, return list of player result records."""
    url = f"{MLB_API}/game/{game_pk}/boxscore"
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"    [WARN] Failed to fetch boxscore for {game_pk}: {e}")
        return []

    records = []
    for side in ['home', 'away']:
        team_data = data.get('teams', {}).get(side, {})
        team_info = team_data.get('team', {})
        team_abbr = team_info.get('abbreviation', '?')

        # Batters
        for player_data in team_data.get('batters', []):
            pid = player_data
            player_info = team_data.get('players', {}).get(f'ID{pid}', {})
            if not player_info:
                continue

            person = player_info.get('person', {})
            batting = player_info.get('stats', {}).get('batting', {})

            if not batting or batting.get('plateAppearances', 0) == 0:
                continue

            h  = batting.get('hits', 0)
            hr = batting.get('homeRuns', 0)
            doubles = batting.get('doubles', 0)
            triples = batting.get('triples', 0)
            singles = h - doubles - triples - hr

            stats = {
                'pa': batting.get('plateAppearances', 0),
                'ab': batting.get('atBats', 0),
                'h': h, 'singles': max(0, singles),
                'doubles': doubles, 'triples': triples, 'hr': hr,
                'r': batting.get('runs', 0),
                'rbi': batting.get('rbi', 0),
                'bb': batting.get('baseOnBalls', 0),
                'hbp': batting.get('hitByPitch', 0),
                'sb': batting.get('stolenBases', 0),
                'cs': batting.get('caughtStealing', 0),
                'k': batting.get('strikeOuts', 0),
            }

            records.append({
                'player_id': pid,
                'game_pk': game_pk,
                'full_name': person.get('fullName', '?'),
                'team': team_abbr,
                'is_pitcher': False,
                **stats,
                'ip': None, 'p_k': None, 'p_er': None, 'p_h': None,
                'p_bb': None, 'p_hr': None,
                'win': None, 'loss': None, 'cg': None, 'sho': None,
                'actual_dk_pts': compute_batter_dk_pts(stats),
            })

        # Pitchers
        for player_data in team_data.get('pitchers', []):
            pid = player_data
            player_info = team_data.get('players', {}).get(f'ID{pid}', {})
            if not player_info:
                continue

            person = player_info.get('person', {})
            pitching = player_info.get('stats', {}).get('pitching', {})

            if not pitching:
                continue

            ip = parse_ip(pitching.get('inningsPitched'))
            if ip == 0:
                continue

            # Win/loss from game decisions — note format: "(W, 1-0)" or "(L, 0-1)"
            note = pitching.get('note', '')
            win  = '(W,' in note or '(W)' in note
            loss = '(L,' in note or '(L)' in note

            # CG/SHO detection
            game_ip = 9.0  # standard game
            cg = ip >= game_ip
            er = pitching.get('earnedRuns', 0)
            sho = cg and er == 0

            stats = {
                'ip': round(ip, 2),
                'p_k': pitching.get('strikeOuts', 0),
                'p_er': er,
                'p_h': pitching.get('hits', 0),
                'p_bb': pitching.get('baseOnBalls', 0),
                'p_hr': pitching.get('homeRuns', 0),
                'win': win, 'loss': loss, 'cg': cg, 'sho': sho,
            }

            records.append({
                'player_id': pid,
                'game_pk': game_pk,
                'full_name': person.get('fullName', '?'),
                'team': team_abbr,
                'is_pitcher': True,
                'pa': None, 'ab': None, 'h': None,
                'singles': None, 'doubles': None, 'triples': None, 'hr': None,
                'r': None, 'rbi': None, 'bb': None, 'hbp': None,
                'sb': None, 'cs': None, 'k': None,
                **stats,
                'actual_dk_pts': compute_pitcher_dk_pts(stats),
            })

    return records


def run():
    # Parse args
    target_date = None
    days = 1
    args = sys.argv[1:]
    i = 0
    while i < len(args):
        if args[i] == '--date' and i+1 < len(args):
            target_date = args[i+1]; i += 2
        elif args[i] == '--days' and i+1 < len(args):
            days = int(args[i+1]); i += 2
        else:
            target_date = target_date or args[i]; i += 1

    if target_date:
        dates = [target_date]
    else:
        dates = [(date.today() - timedelta(days=d)).isoformat() for d in range(days)]

    print(f"\nLoad Actuals — {len(dates)} date(s)")
    print("=" * 50)

    all_records = []

    for game_date in dates:
        # Get completed games from our games table
        games = sb.table('games').select('game_pk').eq('game_date', game_date).execute().data or []
        print(f"\n  {game_date}: {len(games)} games")

        for g in games:
            gpk = g['game_pk']
            records = load_boxscore(gpk)
            batters = [r for r in records if not r['is_pitcher']]
            pitchers = [r for r in records if r['is_pitcher']]
            if records:
                print(f"    Game {gpk}: {len(batters)} batters, {len(pitchers)} pitchers")
            all_records.extend(records)

    if not all_records:
        print("\n  No results to upload.")
        return

    # Add game_date to records
    # (We need to look it up from games table since boxscore doesn't include it directly)
    game_dates = {}
    for game_date in dates:
        games = sb.table('games').select('game_pk,game_date').eq('game_date', game_date).execute().data or []
        for g in games:
            game_dates[g['game_pk']] = g['game_date']
    for r in all_records:
        r['game_date'] = game_dates.get(r['game_pk'], dates[0])

    # Deduplicate by (player_id, game_pk)
    seen = {}
    for r in all_records:
        seen[(r['player_id'], r['game_pk'])] = r
    all_records = list(seen.values())

    # Upsert
    BATCH = 500
    uploaded = 0
    for i in range(0, len(all_records), BATCH):
        batch = all_records[i:i+BATCH]
        sb.table('actual_results').upsert(
            batch, on_conflict='player_id,game_pk', ignore_duplicates=False
        ).execute()
        uploaded += len(batch)
        print(f"  Uploaded {uploaded}/{len(all_records)}")

    # Summary
    batters = [r for r in all_records if not r['is_pitcher'] and r['actual_dk_pts'] > 0]
    pitchers = [r for r in all_records if r['is_pitcher']]
    if batters:
        avg_bat = sum(r['actual_dk_pts'] for r in batters) / len(batters)
        top_bat = sorted(batters, key=lambda r: r['actual_dk_pts'], reverse=True)[:5]
        print(f"\n  Batters with pts: {len(batters)} (avg {avg_bat:.1f} DK pts)")
        for r in top_bat:
            print(f"    {r['full_name']:25s}  {r['actual_dk_pts']:5.1f} pts  "
                  f"({r.get('h',0)}-for-{r.get('ab',0)}, {r.get('hr',0)} HR, "
                  f"{r.get('r',0)} R, {r.get('rbi',0)} RBI, {r.get('sb',0)} SB)")

    if pitchers:
        top_pit = sorted(pitchers, key=lambda r: r['actual_dk_pts'], reverse=True)[:5]
        print(f"\n  Top 5 pitcher actuals:")
        for r in top_pit:
            w = 'W' if r.get('win') else ('L' if r.get('loss') else '-')
            print(f"    {r['full_name']:25s}  {r['actual_dk_pts']:5.1f} pts  "
                  f"(IP={r.get('ip',0):.1f}  K={r.get('p_k',0)}  "
                  f"ER={r.get('p_er',0)}  {w})")

    print(f"\nActuals complete. {uploaded} records upserted.")


if __name__ == '__main__':
    run()
