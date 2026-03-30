#!/usr/bin/env python3
import sys
sys.stdout.reconfigure(encoding='utf-8', line_buffering=True)
"""
compute_ownership.py — SlateHub DFS Ownership Projection Engine

Formula-based (no historical training data).
Weighted signal score per player, normalized per position group per slate.

Ownership Drivers:
  35% — Value score (proj_dk_pts / salary * 1000)
  25% — Salary rank within position group (inverted percentile)
  15% — Batting order position (spots 1–4 highest)
  10% — Vegas implied runs (via vegas_mult)
  8%  — Park factor (via park_mult)
  7%  — Weather suppression (rain/headwind reduces score)

Modifiers applied before normalization:
  Confirmed lineup  → ×1.2
  Unconfirmed       → ×0.55

Cap logic:
  Large slate → POSITION_MAX_OWN base caps (SP=40%, OF=25%, etc.)
  Small slate → dynamic cap = max(base, min(2× avg_target, 90%))
  3 SPs / 2-game slate → avg=66.7% → cap raises to 90% so softmax can express concentration

Output: proj_ownership (0–100 float), ownership_slate_size ('small'/'medium'/'large')
Upserts into player_projections on (player_id, game_pk).

Run:
  py -3.12 compute_ownership.py
  py -3.12 compute_ownership.py --date 2026-03-26
"""

import os, math, re
from datetime import date, datetime, timezone
from collections import defaultdict
from supabase import create_client
from dotenv import load_dotenv
from config import SEASON

load_dotenv()
sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])


# ── Signal weights ─────────────────────────────────────────────────────────────
W_VALUE        = 0.35   # proj_dk_pts per $1000 salary
W_SALARY_RANK  = 0.20   # salary rank within position group
W_PROJ_PTS     = 0.12   # raw projection magnitude (star power)
W_BAT_ORDER    = 0.13   # batting order position
W_VEGAS        = 0.10   # team implied runs
W_PARK         = 0.05   # park factor
W_WEATHER      = 0.05   # weather suppression

LEAGUE_AVG_IMPLIED = 4.5  # baseline implied runs, same as compute_projections.py

# Confirmation modifiers (applied to raw score before normalization)
# Note: when ALL players in a group are unconfirmed (spring training),
# softmax is scale-invariant so the multiplier has no relative effect.
# The steeper UNCONF_MULT matters in-season when confirmed players co-exist.
CONF_MULT   = 1.40
UNCONF_MULT = 0.40

# Batting order score — spots 1–4 premium, 5–9 haircut
BAT_ORDER_SCORE = {1: 1.0, 2: 0.97, 3: 0.94, 4: 0.90,
                   5: 0.80, 6: 0.70, 7: 0.60, 8: 0.50, 9: 0.40}

# Slate size thresholds (by number of games) — used only for labeling
# Concentration effect is natural: fewer players per position = higher per-player %
SLATE_SMALL_MAX  = 4   # ≤ 4 games → small
SLATE_MEDIUM_MAX = 7   # ≤ 7 games → medium; > 7 → large

# Softmax temperature — controls distribution sharpness
# Lower = more concentrated at top; higher = more uniform
# Position-specific temps used in normalize_position_group()
SOFTMAX_TEMP_SP     = 0.8    # SPs: very sharp — top SP dominates (40-50%)
SOFTMAX_TEMP_HITTER = 1.4    # Hitters: moderately sharp

# DK lineup slot counts per position.
# Ownership across all players in a group must sum to (slots × 100%).
# avg_target is computed dynamically as (slots × 100) / n_players.
POSITION_SLOTS = {
    'SP': 2,
    'C':  1,
    '1B': 1,
    '2B': 1,
    '3B': 1,
    'SS': 1,
    'OF': 3,
}

# Hard cap: no single player realistically exceeds this % on a LARGE slate.
# On small slates the cap is computed dynamically in normalize_position_group
# (2× the avg_target, floored at this value) so small pools don't get cut off.
POSITION_MAX_OWN = {
    'SP': 55.0,
    'C':  30.0,
    '1B': 30.0,
    '2B': 30.0,
    '3B': 30.0,
    'SS': 30.0,
    'OF': 25.0,
}

# Canonical position priority for multi-position players (scarcer = higher priority)
POSITION_PRIORITY = ['C', 'SS', '2B', '3B', '1B', 'OF', 'SP']

# Wind directions that suppress offense (blowing into park / crosswind)
WIND_IN_DIRS = {'N', 'NNW', 'NW', 'NNE', 'NE', 'WNW'}

BATCH_SIZE = 500


# ── Utility helpers ────────────────────────────────────────────────────────────

def norm_name(name):
    """Normalize a player name for fuzzy matching across data sources.
    Removes periods/apostrophes, strips common suffixes (Jr., III, etc.),
    and lowercases. Used as fallback when player_id joins fail.
    """
    if not name:
        return ''
    n = name.lower()
    n = n.replace('.', '').replace("'", '').replace('\u2019', '')  # periods, apostrophes
    n = re.sub(r'\s+(jr|sr|ii|iii|iv)\s*$', '', n.strip())
    return re.sub(r'\s+', ' ', n).strip()


def safe(val, default=None):
    """Return val as float, or default if None/NaN/inf."""
    if val is None:
        return default
    try:
        f = float(val)
        return None if (math.isnan(f) or math.isinf(f)) else f
    except (TypeError, ValueError):
        return default


def clip(val, lo, hi):
    if val is None:
        return None
    return max(lo, min(hi, val))


def softmax(scores, temperature=1.0):
    """
    Softmax with temperature scaling.
    Higher temperature → flatter distribution.
    Returns list of fractional shares summing to 1.0.
    """
    if not scores:
        return []
    scaled  = [s / temperature for s in scores]
    max_s   = max(scaled)   # numerical stability
    exps    = [math.exp(s - max_s) for s in scaled]
    total   = sum(exps)
    return [e / total for e in exps] if total > 0 else [1.0 / len(scores)] * len(scores)


def canonical_pos(dk_position_str):
    """
    Map DK multi-position string to single canonical position.
    '2B/SS' → 'SS' (scarcer), '1B/3B' → '3B', etc.
    Returns None if unrecognized.
    """
    if not dk_position_str:
        return None
    parts = [p.strip() for p in str(dk_position_str).split('/')]
    for p in POSITION_PRIORITY:
        if p in parts:
            return p
    return parts[0] if parts else None


# ── Signal component functions ─────────────────────────────────────────────────

def compute_value_score(proj_dk_pts, salary):
    """Value = projected DK points per $1,000 salary. Clipped to [0, 15]."""
    pts = safe(proj_dk_pts, 0)
    sal = safe(salary, 0)
    if sal <= 0 or pts <= 0:
        return 0.0
    return clip((pts / sal) * 1000, 0.0, 15.0)


def compute_salary_rank_score(salary, all_salaries_in_group):
    """
    Inverted percentile rank within position group.
    Highest salary → 1.0 (chalk). Lowest → ~0.0.
    """
    if not all_salaries_in_group or salary is None:
        return 0.5
    sorted_sals = sorted(set(s for s in all_salaries_in_group if s))
    if len(sorted_sals) <= 1:
        return 0.5
    sal = safe(salary, 0)
    # Find the rank of this salary (0 = lowest, n-1 = highest)
    rank = 0
    for s in sorted_sals:
        if s <= sal:
            rank = sorted_sals.index(s) + 1
    rank = min(rank, len(sorted_sals))
    return rank / len(sorted_sals)   # higher salary → higher score


def compute_proj_pts_score(proj_dk_pts, all_proj_pts_in_group):
    """
    Raw projection magnitude — captures star power / name recognition.
    Highest-projected player in the group → 1.0, lowest → ~0.0.
    Players with high projections get outsized ownership regardless of value.
    """
    pts = safe(proj_dk_pts, 0)
    if not all_proj_pts_in_group or pts <= 0:
        return 0.0
    max_pts = max(all_proj_pts_in_group)
    min_pts = min(all_proj_pts_in_group)
    if max_pts <= min_pts:
        return 0.5
    return clip((pts - min_pts) / (max_pts - min_pts), 0.0, 1.0)


def compute_bat_order_score(batting_order, is_pitcher):
    """Batting order signal. Pitchers always return 1.0 (not applicable)."""
    if is_pitcher:
        return 1.0
    order = int(batting_order) if batting_order else 5
    return BAT_ORDER_SCORE.get(order, 0.50)


def compute_vegas_score(vegas_mult):
    """
    Convert vegas_mult (team_implied / 4.5) into 0–1 score.
    0.5 → 0.0  |  1.0 → 0.5  |  1.5 → 1.0
    """
    v = safe(vegas_mult, 1.0)
    return clip((v - 0.5) / 1.0, 0.0, 1.0)


def compute_park_score(park_mult):
    """
    Convert park_mult (basic_factor / 100) into 0–1 score.
    0.70 → 0.0  |  1.00 → 0.5  |  1.30 → 1.0
    """
    p = safe(park_mult, 1.0)
    return clip((p - 0.70) / 0.60, 0.0, 1.0)


def compute_weather_score(weather_mult, precip_pct, wind_dir, is_outdoor):
    """
    Weather suppression: rain, headwind, and cold reduce ownership interest.
    Returns 0.0 (heavy suppression) to 1.0 (no suppression).
    """
    if not is_outdoor:
        # Indoor park: neutral game environment, slight public suppression
        return 0.75

    # weather_mult from projections engine: ~[0.80, 1.10]
    # Normalize: 0.80 → 0.0, 1.10 → 1.0
    w    = safe(weather_mult, 1.0)
    base = clip((w - 0.80) / 0.30, 0.0, 1.0)

    # Additional rain penalty (above 30% chance)
    rain_penalty = 0.0
    pct = safe(precip_pct, 0)
    if pct > 30:
        rain_penalty = (pct - 30) / 100   # 30% → 0.0, 80% → 0.50

    # Headwind penalty
    wind_penalty = 0.15 if (wind_dir or '').upper() in WIND_IN_DIRS else 0.0

    return clip(base - rain_penalty - wind_penalty, 0.0, 1.0)


# ── Core scoring ───────────────────────────────────────────────────────────────

def compute_raw_score(row, salary_group_sals, proj_pts_group):
    """
    Weighted sum of all signals + confirmation modifier.
    All component scores are in [0, 1] before weighting.
    """
    v_val   = compute_value_score(row.get('proj_dk_pts'), row.get('salary'))
    v_sal   = compute_salary_rank_score(row.get('salary'), salary_group_sals)
    v_proj  = compute_proj_pts_score(row.get('proj_dk_pts'), proj_pts_group)
    v_order = compute_bat_order_score(row.get('batting_order'), row.get('is_pitcher'))
    v_vegas = compute_vegas_score(row.get('vegas_mult'))
    v_park  = compute_park_score(row.get('park_mult'))
    v_wx    = compute_weather_score(
                  row.get('weather_mult'),
                  row.get('precip_pct'),
                  row.get('wind_dir'),
                  row.get('is_outdoor', True))

    raw = (W_VALUE       * v_val   +
           W_SALARY_RANK * v_sal   +
           W_PROJ_PTS    * v_proj  +
           W_BAT_ORDER   * v_order +
           W_VEGAS        * v_vegas +
           W_PARK         * v_park  +
           W_WEATHER      * v_wx)

    # Lineup confirmation modifier
    confirmed = row.get('lineup_confirmed')
    if confirmed is True:
        raw *= CONF_MULT
    elif confirmed is False:
        raw *= UNCONF_MULT
    # None = unknown → no modifier

    return max(raw, 0.0)


def normalize_position_group(raw_scores, pos_key):
    """
    Convert raw scores → realistic ownership percentages.

    Mathematically anchored: ownership across all players in a position group
    sums to (slots × 100%). e.g. 2 SP slots → SP group sums to 200%.

    Natural concentration: fewer players in the softmax pool already produces
    higher per-player % — no extra multiplier needed. In a 4-game slate with
    5 SP options, each SP averages 40% (200 / 5). In a 15-game slate with
    25 SP options, each averages 8% (200 / 25). The concentration is baked in.

    Steps:
    1. Softmax → fractional shares summing to 1.0
    2. Each player's ownership = share × (slots × 100)
    3. Cap at max_own and clip to [0, 100]
    """
    if not raw_scores:
        return []

    slots    = POSITION_SLOTS.get(pos_key, 1)
    pool     = slots * 100   # sum across group always = slots × 100
    n        = len(raw_scores)

    # Dynamic cap: on small slates (few players) the base cap is too tight.
    # Allow up to 2× the average expected ownership, but never above 90%.
    # Example: 3 SPs, pool=200 → avg=66.7% → dynamic cap = min(133, 90) = 90%
    #          8 SPs, pool=200 → avg=25%   → dynamic cap = min(50, 90)  = 50%
    #         25 SPs, pool=200 → avg=8%    → dynamic cap = min(16, 90)  = 16% → base 40% wins
    base_max    = POSITION_MAX_OWN.get(pos_key, 30.0)
    avg_target  = pool / n if n > 0 else pool
    dynamic_max = min(avg_target * 2.0, 90.0)
    max_own     = max(base_max, dynamic_max)

    # Position-specific temperature: SPs need very sharp distribution (top SP dominates),
    # hitters need moderately sharp. Scale down for small pools.
    base_temp = SOFTMAX_TEMP_SP if pos_key == 'SP' else SOFTMAX_TEMP_HITTER
    temp = min(base_temp, max(0.5, base_temp * math.sqrt(n / 10.0)))

    shares     = softmax(raw_scores, temperature=temp)
    ownerships = [s * pool for s in shares]
    ownerships = [min(o, max_own) for o in ownerships]
    ownerships = [clip(o, 0.0, 100.0) for o in ownerships]
    return ownerships


# ── Data fetch ────────────────────────────────────────────────────────────────

def batch_in(table_query, column, ids, chunk=150):
    """Splits large .in() queries into chunks to avoid Supabase 150-id limit."""
    results = []
    for i in range(0, len(ids), chunk):
        results.extend(table_query.in_(column, ids[i:i+chunk]).execute().data or [])
    return results


# games.home_team / away_team stores full MLB API names ("Houston Astros", etc.)
# Map standard DK abbreviations → substring to match against the full name
TEAM_ABBR_MAP = {
    'ARI': 'arizona',     'ATL': 'atlanta',      'BAL': 'baltimore',
    'BOS': 'boston',      'CHC': 'cubs',          'CHW': 'white sox',
    'CIN': 'cincinnati',  'CLE': 'cleveland',     'COL': 'colorado',
    'DET': 'detroit',     'HOU': 'houston',       'KC':  'kansas',
    'KCR': 'kansas',      'LAA': 'angels',        'LAD': 'dodger',
    'MIA': 'miami',       'MIL': 'milwaukee',     'MIN': 'minnesota',
    'NYM': 'mets',        'NYY': 'yankee',        'OAK': 'athletics',
    'PHI': 'philadelphia','PIT': 'pittsburgh',    'SD':  'san diego',
    'SDP': 'san diego',   'SEA': 'seattle',       'SF':  'francisco',
    'SFG': 'francisco',   'STL': 'louis',         'TB':  'tampa',
    'TBR': 'tampa',       'TEX': 'texas',         'TOR': 'toronto',
    'WAS': 'washington',  'WSH': 'washington',
}


def resolve_teams_to_game_pks(target_date, teams):
    """
    Given a list of team abbreviations, return the set of game_pks on that date
    where either the home or away team matches. Used for --teams arg.
    games.home_team stores full names ("Houston Astros") so we match by substring.
    """
    rows = (sb.table('games')
              .select('game_pk, home_team, away_team')
              .eq('game_date', target_date)
              .execute().data or [])

    # Build set of name fragments to search for
    fragments = set()
    for t in teams:
        abbr = t.strip().upper()
        frag = TEAM_ABBR_MAP.get(abbr)
        if frag:
            fragments.add(frag.lower())
        else:
            # Unknown abbreviation — try raw match too
            fragments.add(abbr.lower())

    matched = set()
    for r in rows:
        home = (r.get('home_team') or '').lower()
        away = (r.get('away_team') or '').lower()
        for frag in fragments:
            if frag in home or frag in away:
                matched.add(r['game_pk'])
                break
    return matched


def fetch_ownership_data(target_date, game_pk_filter=None, slate_filter=None):
    """
    Fetches all signals needed for ownership computation.
    Requires compute_projections.py to have already run for target_date.

    Args:
        game_pk_filter : set of game_pks to include (from --games or --teams).
                         None = include all games on the date.
        slate_filter   : dk_slate string ('main', 'early', etc.) to restrict
                         salary lookup. None = prefer 'main', fall back to any.
    """
    print(f"  Fetching projections for {target_date}...")
    q = (sb.table('player_projections')
           .select('player_id, game_pk, game_date, full_name, team, '
                   'is_pitcher, batting_order, '
                   'proj_dk_pts, vegas_mult, park_mult, weather_mult')
           .eq('game_date', target_date))

    all_proj = q.execute().data or []

    # Apply game filter if provided (--games or --teams)
    if game_pk_filter:
        proj_rows = [r for r in all_proj if r.get('game_pk') in game_pk_filter]
        print(f"  Filtered to {len(proj_rows)}/{len(all_proj)} players "
              f"({len(game_pk_filter)} game(s))")
    else:
        proj_rows = all_proj

    if not proj_rows:
        return {}

    player_ids = list({r['player_id'] for r in proj_rows if r.get('player_id')})
    game_pks   = list({r['game_pk']   for r in proj_rows if r.get('game_pk')})

    # DK salaries — fetch all for the season (small table, ~750 rows).
    # player_id in dk_salaries often differs from MLB Stats API ids used in
    # player_projections, so we build both an id-index and a name-index and
    # fall back to name matching when the id join misses.
    print(f"  Fetching DK salaries (all, season {SEASON})...")
    sal_rows = (sb.table('dk_salaries')
                  .select('player_id, name, salary, position, dk_slate')
                  .eq('season', SEASON)
                  .execute().data or [])

    def _best_sal(existing, candidate, slate_filter):
        """Return whichever salary row is preferred (main slate > others)."""
        if existing is None:
            return candidate
        slt_e = existing.get('dk_slate', '')
        slt_c = candidate.get('dk_slate', '')
        if slate_filter:
            return candidate if slt_c == slate_filter else existing
        return candidate if slt_c == 'main' else existing

    # Build salary_map by player_id AND by normalized name for fallback
    salary_map      = {}   # player_id → sal_row
    salary_name_map = {}   # norm_name → sal_row

    for r in sal_rows:
        slt = r.get('dk_slate', '')
        if slate_filter and slt != slate_filter:
            continue   # skip players not on the requested slate

        pid  = r['player_id']
        nnam = norm_name(r.get('name', ''))

        salary_map[pid]       = _best_sal(salary_map.get(pid),       r, slate_filter)
        if nnam:
            salary_name_map[nnam] = _best_sal(salary_name_map.get(nnam), r, slate_filter)

    # If slate_filter was given, re-restrict proj_rows to players actually in
    # that slate (catches the case where game_pk_filter wasn't set but --slate was)
    if slate_filter and not game_pk_filter:
        slate_player_ids = set(salary_map.keys())
        proj_rows  = [r for r in proj_rows if r['player_id'] in slate_player_ids]
        game_pks   = list({r['game_pk'] for r in proj_rows if r.get('game_pk')})
        print(f"  Slate '{slate_filter}': {len(proj_rows)} players across {len(game_pks)} game(s)")

    # Lineup confirmation status
    lineup_map = {}
    if game_pks:
        print(f"  Fetching lineups ({len(game_pks)} games)...")
        lu_query = sb.table('lineups').select('player_id, batting_order, status')
        lu_rows  = batch_in(lu_query, 'game_pk', game_pks)
        for r in lu_rows:
            lineup_map[r['player_id']] = r

    # Weather (precip_pct + wind_dir beyond weather_mult)
    weather_map = {}
    if game_pks:
        wx_query = sb.table('weather').select('game_pk, precip_pct, wind_dir, is_outdoor')
        wx_rows  = batch_in(wx_query, 'game_pk', game_pks)
        weather_map = {r['game_pk']: r for r in wx_rows}

    # n_games reflects only the filtered slate, not the full day
    n_games = len(game_pks)

    return {
        'proj_rows'      : proj_rows,
        'salary_map'     : salary_map,
        'salary_name_map': salary_name_map,
        'lineup_map'     : lineup_map,
        'weather_map'    : weather_map,
        'n_games'        : n_games,
    }


# ── Score + upload for a single slate ─────────────────────────────────────────

def run_single_slate(target_date, slate_filter=None, game_pk_filter=None):
    """
    Compute and upload ownership for one slate (or one explicit set of games).
    Returns number of records written, or 0 if no data.
    """
    data = fetch_ownership_data(target_date,
                                game_pk_filter=game_pk_filter,
                                slate_filter=slate_filter)
    if not data or not data.get('proj_rows'):
        print("  No projections found for this slate.")
        return 0

    proj_rows       = data['proj_rows']
    salary_map      = data['salary_map']
    salary_name_map = data['salary_name_map']
    lineup_map      = data['lineup_map']
    weather_map     = data['weather_map']
    n_games         = data['n_games']

    # Slate size label (informational only — no multiplier applied to pool)
    if n_games <= SLATE_SMALL_MAX:
        slate_size = 'small'
    elif n_games <= SLATE_MEDIUM_MAX:
        slate_size = 'medium'
    else:
        slate_size = 'large'

    print(f"  Games on slate: {n_games}  → {slate_size} slate")

    # Enrich each projection row with salary, lineup, and weather data
    enriched    = []
    skipped     = 0
    name_hits   = 0   # count of name-fallback matches
    for row in proj_rows:
        pid     = row['player_id']
        lu_row  = lineup_map.get(pid, {})
        wx_row  = weather_map.get(row.get('game_pk'), {})

        # Salary lookup: try player_id first, then normalized-name fallback
        sal_row = salary_map.get(pid)
        if sal_row is None:
            nnam    = norm_name(row.get('full_name', ''))
            sal_row = salary_name_map.get(nnam)
            if sal_row:
                name_hits += 1
        sal_row = sal_row or {}

        salary = safe(sal_row.get('salary'), 0)
        if salary <= 0 and not row.get('is_pitcher'):
            skipped += 1
            continue   # no salary data → skip (can't compute value score)

        # Lineup confirmation (True/False/None)
        status    = lu_row.get('status')
        confirmed = True if status == 'confirmed' else (False if status == 'projected' else None)

        # Canonical DK position
        if row.get('is_pitcher'):
            canon_pos = 'SP'
        else:
            canon_pos = canonical_pos(sal_row.get('position', ''))

        enriched.append({
            **row,
            'salary'           : salary,
            'dk_pos'           : canon_pos,
            'lineup_confirmed' : confirmed,
            'precip_pct'       : safe(wx_row.get('precip_pct'), 0),
            'wind_dir'         : wx_row.get('wind_dir'),
            'is_outdoor'       : wx_row.get('is_outdoor', True),
        })

    print(f"  Players to score: {len(enriched)}  (skipped {skipped} with no salary, {name_hits} matched by name fallback)")

    # Group by canonical DK position
    pos_groups = defaultdict(list)
    for row in enriched:
        pos = row.get('dk_pos') or 'UTIL'
        pos_groups[pos].append(row)

    # Compute raw scores + normalize per position group
    computed_at = datetime.now(timezone.utc).isoformat()
    records     = []
    debug_rows  = []

    for pos_key, group in pos_groups.items():
        if pos_key == 'UTIL':
            # No position match → assign low baseline ownership
            for row in group:
                records.append({
                    'player_id'            : row['player_id'],
                    'game_pk'              : row['game_pk'],
                    'proj_ownership'       : 1.0,
                    'ownership_slate_size' : slate_size,
                    'computed_at'          : computed_at,
                })
            continue

        group_sals = [safe(r.get('salary'), 0) for r in group]
        group_proj = [safe(r.get('proj_dk_pts'), 0) for r in group]
        raw_scores = [compute_raw_score(r, group_sals, group_proj) for r in group]
        ownerships = normalize_position_group(raw_scores, pos_key)

        for row, raw, own in zip(group, raw_scores, ownerships):
            records.append({
                'player_id'            : row['player_id'],
                'game_pk'              : row['game_pk'],
                'proj_ownership'       : round(own, 1),
                'ownership_slate_size' : slate_size,
                'computed_at'          : computed_at,
            })
            debug_rows.append({
                'name'    : row.get('full_name', '?'),
                'pos'     : pos_key,
                'salary'  : row.get('salary', 0),
                'proj_pts': row.get('proj_dk_pts', 0),
                'raw'     : round(raw, 4),
                'own_pct' : round(own, 1),
            })

    # Batch upsert to player_projections
    print(f"\n  Upserting {len(records)} records...")
    uploaded = 0
    for i in range(0, len(records), BATCH_SIZE):
        chunk = records[i:i+BATCH_SIZE]
        sb.table('player_projections').upsert(
            chunk, on_conflict='player_id,game_pk', ignore_duplicates=False
        ).execute()
        uploaded += len(chunk)
        print(f"  Uploaded {uploaded}/{len(records)}")

    # Sample output per position group
    print(f"\n  {'─'*51}")
    print(f"  Ownership projections — {slate_size} slate ({n_games} games)")
    print(f"  {'─'*51}")
    for pos_key in POSITION_PRIORITY:
        group_results = sorted(
            [r for r in debug_rows if r.get('pos') == pos_key],
            key=lambda r: r.get('own_pct', 0), reverse=True
        )[:3]
        if group_results:
            print(f"\n  Top {pos_key}:")
            for r in group_results:
                print(f"    {r['name']:27s}  {r['own_pct']:5.1f}%  "
                      f"${r['salary']:,}  {r['proj_pts'] or 0:.1f} pts")

    print(f"\n  Done. {len(records)} ownership projections written.")
    return len(records)


# ── Main ──────────────────────────────────────────────────────────────────────

def run():
    """
    Usage:
      py -3.12 compute_ownership.py
      py -3.12 compute_ownership.py --date 2026-03-26
      py -3.12 compute_ownership.py --slate early
      py -3.12 compute_ownership.py --games 748230,748231
      py -3.12 compute_ownership.py --teams NYY,BOS,HOU,LAA
      py -3.12 compute_ownership.py --date 2026-03-19 --teams NYY,BOS

    --slate  [main|early|afternoon|late]  Filter to players on this DK slate
    --games  748230,748231                Explicit game_pks (spring training / custom)
    --teams  NYY,BOS                      Team abbreviations → auto-resolve game_pks

    Priority: --games > --teams > --slate > auto-detect all slates
    When no filter is given, auto-detects all classic slates from dk_salaries
    and runs ownership independently for each slate.
    """
    def arg_val(flag):
        """Return the value after a flag, or None."""
        if flag in sys.argv:
            idx = sys.argv.index(flag)
            return sys.argv[idx + 1] if idx + 1 < len(sys.argv) else None
        return None

    target_date  = arg_val('--date') or str(date.today())
    slate_filter = arg_val('--slate')   # e.g. 'main', 'early'
    games_arg    = arg_val('--games')   # e.g. '748230,748231'
    teams_arg    = arg_val('--teams')   # e.g. 'NYY,BOS,HOU,LAA'

    # Build game_pk_filter from explicit args
    game_pk_filter = None
    if games_arg:
        game_pk_filter = {int(g.strip()) for g in games_arg.split(',') if g.strip()}
        print(f"  Filter: --games ({len(game_pk_filter)} game_pks)")
    elif teams_arg:
        teams = [t.strip() for t in teams_arg.split(',') if t.strip()]
        game_pk_filter = resolve_teams_to_game_pks(target_date, teams)
        if not game_pk_filter:
            print(f"  WARNING: No games found on {target_date} for teams: {teams}")
        else:
            print(f"  Filter: --teams {teams} → {len(game_pk_filter)} game(s)")

    # ── Explicit filter: run once for that filter ─────────────────────────────
    if slate_filter or game_pk_filter:
        label = f" [{slate_filter} slate]" if slate_filter else \
                f" [{len(game_pk_filter)} games]"
        print(f"\n{'='*55}")
        print(f"  Ownership Engine — {target_date}{label}")
        print(f"{'='*55}")
        n = run_single_slate(target_date,
                             slate_filter=slate_filter,
                             game_pk_filter=game_pk_filter)
        if n == 0:
            print("  No projections found — run compute_projections.py first.")
        return

    # ── No filter: auto-detect all classic slates and run each ────────────────
    print(f"\n{'='*55}")
    print(f"  Ownership Engine — {target_date} (auto-detect slates)")
    print(f"{'='*55}")

    # Query distinct classic slates from dk_salaries for this season
    sal_rows = (sb.table('dk_salaries')
                  .select('dk_slate')
                  .eq('season', SEASON)
                  .eq('contest_type', 'classic')
                  .limit(5000)
                  .execute().data or [])
    slates = sorted({r['dk_slate'] for r in sal_rows if r.get('dk_slate')})

    if not slates:
        print("  No classic slates found in dk_salaries. Running for all games...")
        run_single_slate(target_date)
        return

    print(f"  Found {len(slates)} classic slate(s): {slates}\n")

    total_records = 0
    for slate in slates:
        print(f"\n{'─'*55}")
        print(f"  ▸ Slate: {slate}")
        print(f"{'─'*55}")
        n = run_single_slate(target_date, slate_filter=slate)
        total_records += n

    print(f"\n{'='*55}")
    print(f"  All slates complete. {total_records} total records written.")
    print(f"{'='*55}\n")


if __name__ == '__main__':
    run()
