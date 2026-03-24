"""
load_milb_stats.py
==================
Fetches minor league pitching stats from the MLB Stats API for pitchers who
have little or no MLB data, applies level translation factors to convert MiLB
rates to MLB-equivalent projections, then upserts into the pitcher_stats table.

Usage:
    py -3.12 load_milb_stats.py [--date YYYY-MM-DD]

    --date  If provided, only processes pitchers scheduled that date (via the
            games table and dk_salaries). Without --date, all DK SP-eligible
            pitchers are checked.
"""

import sys
sys.stdout.reconfigure(encoding='utf-8', line_buffering=True)

import os
import json
import time
import argparse
import urllib.request
from datetime import date

from supabase import create_client
from dotenv import load_dotenv

# ── Credentials ────────────────────────────────────────────────────────────────
load_dotenv()
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')

if not SUPABASE_URL or not SUPABASE_KEY:
    print('ERROR: Missing SUPABASE_URL or SUPABASE_KEY in .env file.')
    sys.exit(1)

sb = create_client(SUPABASE_URL, SUPABASE_KEY)

# ── MiLB sport IDs and translation factors ─────────────────────────────────────
# sport_id 11 = AAA (Triple-A), 12 = AA (Double-A), 13 = A+ (High-A)
# k_mult:   discount on strikeout rate (MiLB K% overstates MLB ability)
# bb_mult:  penalty on walk rate (MiLB BB% understates MLB walk rate)
# proj_mult: not used in per-stat formulas but retained for reference
LEVEL_FACTORS = {
    11: {'abbrev': 'AAA', 'k_mult': 0.90, 'bb_mult': 1.05, 'proj_mult': 0.92},
    12: {'abbrev': 'AA',  'k_mult': 0.82, 'bb_mult': 1.10, 'proj_mult': 0.85},
    13: {'abbrev': 'A+',  'k_mult': 0.74, 'bb_mult': 1.15, 'proj_mult': 0.78},
}

# Minimum MLB IP before we consider a pitcher's MLB data sufficient
MLB_IP_THRESHOLD = 50

# MLB Stats API base URL
MLB_API_BASE = 'https://statsapi.mlb.com/api/v1'

# Request headers to avoid 403 errors from the MLB API
HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/120.0.0.0 Safari/537.36'
    )
}

# Rate-limit pause between MLB API calls (seconds)
API_SLEEP = 0.1


# ── Helpers ────────────────────────────────────────────────────────────────────

def parse_ip(s):
    """
    Convert an MLB API inningsPitched string like '79.2' to a float.
    The decimal part represents outs, not tenths: '79.2' = 79 + 2/3 = 79.667.
    """
    try:
        parts = str(s).split('.')
        full = int(parts[0])
        outs = int(parts[1]) if len(parts) > 1 else 0
        return full + outs / 3.0
    except Exception:
        return 0.0


def fetch_json(url):
    """Fetch JSON from a URL and return the parsed dict, or None on error."""
    try:
        req = urllib.request.Request(url, headers=HEADERS)
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read())
    except Exception as e:
        print(f'    [WARN] API fetch failed for {url}: {e}')
        return None


def fetch_milb_splits(player_id, sport_id):
    """
    Fetch year-by-year pitching splits for one player at one MiLB level.
    Returns a list of split dicts, or [] on failure.
    """
    url = (
        f'{MLB_API_BASE}/people/{player_id}/stats'
        f'?stats=yearByYear&group=pitching&sportId={sport_id}'
    )
    data = fetch_json(url)
    if not data:
        return []

    splits = []
    for stat_block in data.get('stats', []):
        splits.extend(stat_block.get('splits', []))
    return splits


def pick_best_split(splits):
    """
    From a list of splits (all at the same level), pick the best single season.
    Criteria: most recent season where gs >= 5 OR ip >= 20.
    Returns the chosen split dict, or None if none qualify.
    """
    candidates = []
    for split in splits:
        stat = split.get('stat', {})
        ip = parse_ip(stat.get('inningsPitched', '0'))
        gs = int(stat.get('gamesStarted', 0) or 0)
        season = int(split.get('season', 0))
        if gs >= 5 or ip >= 20:
            candidates.append((season, split))

    if not candidates:
        return None

    # Return the split from the most recent qualifying season
    candidates.sort(key=lambda x: x[0], reverse=True)
    return candidates[0][1]


def compute_translated_stats(split, sport_id):
    """
    Given a split dict and sport_id, compute MLB-translated rate stats.
    Returns a dict of computed fields ready to merge into an upsert row.
    """
    factors = LEVEL_FACTORS[sport_id]
    k_mult  = factors['k_mult']
    bb_mult = factors['bb_mult']

    stat = split.get('stat', {})

    ip_float = parse_ip(stat.get('inningsPitched', '0'))
    if ip_float <= 0:
        ip_float = 0.001  # guard against division by zero

    so = int(stat.get('strikeOuts', 0) or 0)
    bb = int(stat.get('baseOnBalls', 0) or 0)
    hr = int(stat.get('homeRuns', 0) or 0)
    bf = int(stat.get('battersFaced') or 0) or max(1, int(ip_float * 4.3))

    go = int(stat.get('groundOuts', 0) or 0)
    ao = int(stat.get('airOuts', 0) or 0)

    # Translated rate stats (MLB-equivalent)
    k9  = (so / ip_float) * 9 * k_mult
    bb9 = (bb / ip_float) * 9 * bb_mult
    hr9 = (hr / ip_float) * 9

    k_pct  = (so / bf) * k_mult
    bb_pct = (bb / bf) * bb_mult

    k_bb_pct = k_pct - bb_pct

    # Pseudo-xFIP from translated K/BB/HR rates
    pseudo_xfip = (13 * hr9 + 3 * bb9 - 2 * k9) / 9 + 3.17
    pseudo_xfip = max(2.0, min(7.0, pseudo_xfip))  # sanity clip

    # Ground-ball pct (raw, no translation factor available)
    gb_pct = None
    if go > 0 and ao > 0:
        gb_pct = go / (go + ao)

    return {
        'ip_float'    : ip_float,
        'k9'          : round(k9, 2),
        'bb9'         : round(bb9, 2),
        'hr9'         : round(hr9, 3),
        'k_pct'       : round(k_pct, 4),
        'bb_pct'      : round(bb_pct, 4),
        'k_bb_pct'    : round(k_bb_pct, 4),
        'pseudo_xfip' : round(pseudo_xfip, 2),
        'gb_pct'      : round(gb_pct, 3) if gb_pct is not None else None,
    }


def build_upsert_row(player_id, full_name, split, sport_id, computed):
    """
    Assemble the full pitcher_stats row dict for upserting.
    Fields not available from MiLB data are set to None so they do not
    overwrite existing values on conflict (Supabase upsert merges on conflict).
    """
    stat         = split.get('stat', {})
    level_abbrev = LEVEL_FACTORS[sport_id]['abbrev']
    season       = int(split.get('season', 0))
    team_name    = split.get('team', {}).get('name', '')

    # ERA: use raw MiLB ERA (not translated); mark as-is
    raw_era = stat.get('era')
    era = None
    if raw_era and raw_era not in ('-.--', '', None):
        try:
            era = float(raw_era)
        except (ValueError, TypeError):
            era = None

    # WHIP and AVG: pass through as-is from MiLB data
    raw_whip = stat.get('whip')
    whip = None
    if raw_whip:
        try:
            whip = float(raw_whip)
        except (ValueError, TypeError):
            whip = None

    row = {
        'player_id'      : player_id,
        'season'         : season,
        'full_name'      : full_name,
        'team'           : team_name,
        'g'              : stat.get('gamesPlayed'),
        'gs'             : stat.get('gamesStarted'),
        'ip'             : round(computed['ip_float'], 1),
        'era'            : era,
        'xfip'           : computed['pseudo_xfip'],
        'siera'          : None,     # not derivable from MiLB data
        'fip'            : None,     # not derivable from MiLB data
        'k_pct'          : computed['k_pct'],
        'bb_pct'         : computed['bb_pct'],
        'k_bb_pct'       : computed['k_bb_pct'],
        'hr9'            : computed['hr9'],
        'k9'             : computed['k9'],
        'bb9'            : computed['bb9'],
        'whip'           : whip,
        'avg'            : stat.get('avg'),
        'w'              : stat.get('wins'),
        'l'              : stat.get('losses'),
        'gb_pct'         : computed['gb_pct'],
        'stats_level'    : level_abbrev,
        # Advanced metrics not available from the MiLB API
        'babip'          : None,
        'lob_pct'        : None,
        'fb_pct'         : None,
        'ld_pct'         : None,
        'swstr_pct'      : None,
        'csw_pct'        : None,
        'stuff_plus'     : None,
        'location_plus'  : None,
        'pitching_plus'  : None,
        'barrel_pct'     : None,
        'hard_hit_pct'   : None,
        'arm_angle'      : None,
        'velo'           : None,
        'sample_size'    : None,
        'age'            : None,
        'sv'             : None,
        'hld'            : None,
    }
    return row


# ── Data-fetching helpers ──────────────────────────────────────────────────────

def get_mlb_ip_by_player():
    """
    Query pitcher_stats for rows where stats_level='MLB' (or NULL, treated as
    MLB). Returns a dict of {player_id: total_ip}.
    """
    print('  Fetching MLB pitcher_stats IP totals...')
    ip_map = {}

    # Fetch in pages to handle large tables
    page_size = 1000
    offset = 0
    while True:
        resp = (
            sb.table('pitcher_stats')
            .select('player_id, ip, stats_level')
            .or_('stats_level.eq.MLB,stats_level.is.null')
            .range(offset, offset + page_size - 1)
            .execute()
        )
        rows = resp.data or []
        for row in rows:
            pid = row.get('player_id')
            ip  = float(row.get('ip') or 0)
            if pid:
                ip_map[pid] = ip_map.get(pid, 0.0) + ip
        if len(rows) < page_size:
            break
        offset += page_size

    print(f'    Found {len(ip_map)} pitchers with MLB data.')
    return ip_map


def get_dk_pitchers(filter_date=None):
    """
    Return a list of dicts with {player_id, name} for all DK pitchers
    (position in SP, RP, P, or any value containing SP).

    If filter_date is provided (YYYY-MM-DD string), restrict to pitchers
    scheduled to play that date via the games table.
    """
    print('  Fetching DK pitcher list...')

    query = (
        sb.table('dk_salaries')
        .select('player_id, name, position')
        .or_('position.eq.SP,position.eq.RP,position.eq.P')
    )

    if filter_date:
        # Fetch game_ids for that date
        games_resp = (
            sb.table('games')
            .select('game_id')
            .eq('game_date', filter_date)
            .execute()
        )
        game_ids = [g['game_id'] for g in (games_resp.data or [])]
        if not game_ids:
            print(f'    No games found for {filter_date}.')
            return []

        # Get player_ids in those games from dk_salaries
        id_list = ','.join(str(g) for g in game_ids)
        query = (
            sb.table('dk_salaries')
            .select('player_id, name, position')
            .in_('game_id', game_ids)
            .or_('position.eq.SP,position.eq.RP,position.eq.P')
        )

    resp = query.execute()
    pitchers = resp.data or []

    # Deduplicate by player_id (keep first occurrence)
    seen = {}
    for p in pitchers:
        pid = p.get('player_id')
        if pid and pid not in seen:
            seen[pid] = {'player_id': pid, 'name': p.get('name', '')}

    result = list(seen.values())
    print(f'    Found {len(result)} unique DK pitchers.')
    return result


# ── Main logic ─────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description='Load MiLB stats into pitcher_stats.')
    parser.add_argument('--date', metavar='YYYY-MM-DD', help='Only process pitchers scheduled this date.')
    args = parser.parse_args()

    print('MiLB Stats Loader')
    print('=================')

    # Step 1: Get existing MLB IP totals to identify thin/no-data pitchers
    mlb_ip_map = get_mlb_ip_by_player()

    # Step 2: Get DK pitcher list (all, or filtered by date)
    dk_pitchers = get_dk_pitchers(filter_date=args.date)
    if not dk_pitchers:
        print('No DK pitchers to process. Exiting.')
        return

    # Counters for summary
    total_checked   = len(dk_pitchers)
    skipped_mlb     = 0
    fetched_milb    = 0
    no_milb_found   = 0
    level_counts    = {'AAA': 0, 'AA': 0, 'A+': 0}
    sample_rows     = []   # for display at the end

    upsert_rows = []

    # Step 3: For each DK pitcher, decide whether to fetch MiLB stats
    print(f'\n  Processing {total_checked} pitchers...')
    for pitcher in dk_pitchers:
        player_id = pitcher['player_id']
        full_name = pitcher['name']

        total_mlb_ip = mlb_ip_map.get(player_id, 0.0)

        # Skip if the pitcher has sufficient MLB data
        if total_mlb_ip >= MLB_IP_THRESHOLD:
            skipped_mlb += 1
            continue

        # Step 4: Try MiLB levels in order AAA → AA → A+
        best_split    = None
        best_sport_id = None

        for sport_id in [11, 12, 13]:
            splits = fetch_milb_splits(player_id, sport_id)
            time.sleep(API_SLEEP)  # be polite to the API

            if not splits:
                continue

            candidate = pick_best_split(splits)
            if candidate:
                # Take the highest level (first qualifying) and stop
                best_split    = candidate
                best_sport_id = sport_id
                break

        if best_split is None:
            no_milb_found += 1
            continue

        # Step 5: Compute translated stats and build the upsert row
        computed = compute_translated_stats(best_split, best_sport_id)
        row      = build_upsert_row(player_id, full_name, best_split, best_sport_id, computed)

        upsert_rows.append(row)
        fetched_milb += 1

        level_abbrev = LEVEL_FACTORS[best_sport_id]['abbrev']
        level_counts[level_abbrev] = level_counts.get(level_abbrev, 0) + 1

        # Collect up to 10 rows for the summary display
        if len(sample_rows) < 10:
            sample_rows.append({
                'name'      : full_name,
                'level'     : level_abbrev,
                'season'    : row['season'],
                'gs'        : row['gs'] or 0,
                'ip'        : row['ip'],
                'k_pct'     : row['k_pct'],
                'bb_pct'    : row['bb_pct'],
                'pseudo_xfip': row['xfip'],
            })

    # Step 6: Upsert all collected rows into pitcher_stats
    if upsert_rows:
        print(f'\n  Upserting {len(upsert_rows)} MiLB rows into pitcher_stats...')
        success = 0
        errors  = 0
        for row in upsert_rows:
            try:
                sb.table('pitcher_stats').upsert(
                    row, on_conflict='player_id,season'
                ).execute()
                success += 1
            except Exception as e:
                # Fall back to insert with ignore_duplicates if upsert fails
                try:
                    sb.table('pitcher_stats').insert(
                        row, count=None
                    ).execute()
                    success += 1
                except Exception as e2:
                    errors += 1
                    print(f'    [ERROR] Failed to upsert {row.get("full_name")} '
                          f'({row.get("player_id")}): {e2}')

        print(f'    Upserted {success} rows, {errors} errors.')
    else:
        print('\n  No MiLB rows to upsert.')

    # ── Summary ────────────────────────────────────────────────────────────────
    print()
    print('Summary')
    print('-------')
    print(f'  DK pitchers checked:      {total_checked}')
    print(f'  Skipped (MLB >= 50 IP):   {skipped_mlb}')
    print(f'  Fetched MiLB stats:       {fetched_milb}')
    print(f'    AAA: {level_counts.get("AAA", 0)} pitchers')
    print(f'    AA:  {level_counts.get("AA",  0)} pitchers')
    print(f'    A+:  {level_counts.get("A+",  0)} pitchers')
    print(f'    No MiLB found:           {no_milb_found}')

    if sample_rows:
        print()
        print('  Sample:')
        for s in sample_rows:
            k_pct_pct  = (s['k_pct']  or 0) * 100
            bb_pct_pct = (s['bb_pct'] or 0) * 100
            print(
                f"    {s['name']:<22} {s['level']:<4} {s['season']}  "
                f"GS={s['gs']:<3}  IP={s['ip']:<7.1f}  "
                f"xK%={k_pct_pct:.1f}%  xBB%={bb_pct_pct:.1f}%  "
                f"pseudo-xFIP={s['pseudo_xfip']:.2f}"
            )

    print()
    print('Done.')


if __name__ == '__main__':
    main()
