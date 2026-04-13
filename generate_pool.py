#!/usr/bin/env python3
import sys
sys.stdout.reconfigure(encoding='utf-8', line_buffering=True)
"""
generate_pool.py — Lineup pool generation for contest simulation

Generates TWO pools per slate:
  1. User pool: optimized lineups using real projections + greedy randomized builder
  2. Contest pool: ownership-weighted lineups representing the DK field

Modes:
  py -3.12 generate_pool.py                    # Auto-generate for all slates (legacy)
  py -3.12 generate_pool.py --slate main       # Single slate
  py -3.12 generate_pool.py --watch            # Poll for frontend requests via Supabase
"""

import os, math, random, json, time, copy
import numpy as np
from datetime import date, datetime, timezone
from collections import defaultdict
from supabase import create_client
from dotenv import load_dotenv
from config import SEASON

load_dotenv()
sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])

# DK Classic position slots
POS_SLOTS = {'SP': 2, 'C': 1, '1B': 1, '2B': 1, '3B': 1, 'SS': 1, 'OF': 3}
POS_PRIORITY = ['C', 'SS', '2B', '3B', '1B', 'OF', 'SP']
SALARY_CAP = 50000
SALARY_FLOOR = 48500
UPSIDE_BLEND = 0.15    # user pool scoring: 15% weight toward ceiling (P90)

# ── Daily overrides (used by legacy run(), overridden by --watch requests) ──
USER_EXCLUDE_TEAMS = set()
CONTEST_DISCOUNT_TEAMS = {}

# Contest type profiles for contest pool generation
CONTEST_PROFILES = {
    'gpp':   {'noise_pit': 0.50, 'noise_hit': 0.30, 'pitcher_mult': 5.0, 'unconf_pen': 0.4},
    'small': {'noise_pit': 0.25, 'noise_hit': 0.15, 'pitcher_mult': 6.0, 'unconf_pen': 0.2},
}

# Normalize projection team abbreviations to DK abbreviations
TEAM_ABBR_MAP = {'CHW': 'CWS', 'KCR': 'KC', 'SDP': 'SD', 'TBR': 'TB', 'WSN': 'WSH'}

def safe(val, default=None):
    if val is None: return default
    try:
        f = float(val)
        return None if math.isnan(f) or math.isinf(f) else f
    except: return default

def clip(val, lo, hi):
    return max(lo, min(hi, val)) if val is not None else lo


# ── HES / SP Grade / PMS ────────────────────────────────────────────────────
# Ported from frontend (hand-builders-hub.html) computeHES / computeSpGrade / computeSimplePMS

def compute_hes(pf, wx):
    """Hitter Environment Score (1-10). Park + weather."""
    s = 5.0
    if pf:
        basic = safe(pf.get('basic_factor'), 100)
        hr    = safe(pf.get('hr_factor'), 100)
        wc    = safe(pf.get('woba_con'), 100)
        xwc   = safe(pf.get('xwoba_con'), 100)
        bb    = safe(pf.get('bb_factor'), 100)
        k     = safe(pf.get('k_factor'), 100)
        s += (xwc * 0.55 + wc * 0.45 - 100) / 18
        s += (hr - 100) / 20
        s += (bb - k) / 80
        s += (basic - 100) / 50
    if wx:
        temp = safe(wx.get('temp_f'), 72)
        wind = safe(wx.get('wind_speed'), 0)
        wind_dir = (wx.get('wind_dir') or '').upper()
        if temp < 45:   s -= 1.2
        elif temp < 55: s -= 0.6
        elif temp > 80: s += 0.3
        out = 'OUT' in wind_dir or 'OUTWARD' in wind_dir
        inw = 'IN' in wind_dir and not out
        if wind > 10:
            s += wind / 14 if out else (-wind / 14 if inw else 0)
        if safe(wx.get('precip_pct'), 0) > 70:
            s -= 0.5
    return clip(s, 1, 10)

def compute_sp_grade(ps):
    """SP grade from pitcher stats. Returns grade letter or None."""
    if not ps:
        return None
    score, w = 0.0, 0.0
    def add(val, hi, mid, lo, wt, up):
        nonlocal score, w
        if val is None: return
        n = float(val)
        pts = (10 if n >= hi else 7 if n >= mid else 4 if n >= lo else 1) if up \
              else (10 if n <= hi else 7 if n <= mid else 4 if n <= lo else 1)
        score += pts * wt; w += wt
    def to_d(v):
        return None if v is None else (v / 100.0 if v > 1 else v)
    add(safe(ps.get('xfip')),       2.80, 3.50, 4.50, 3, False)
    add(safe(ps.get('siera')),      3.00, 3.80, 4.80, 2, False)
    add(to_d(safe(ps.get('k_pct'))),  0.280, 0.220, 0.170, 2, True)
    add(to_d(safe(ps.get('bb_pct'))), 0.060, 0.090, 0.120, 2, False)
    add(to_d(safe(ps.get('swstr_pct'))), 0.135, 0.110, 0.085, 1, True)
    add(safe(ps.get('stuff_plus')),  110, 100, 90, 1, True)
    if w == 0: return None
    a = score / w
    for thresh, letter in [(9.2,'A+'),(8.0,'A'),(6.8,'B+'),(5.6,'B'),(4.4,'C+'),(3.2,'C'),(2.0,'D')]:
        if a >= thresh: return letter
    return 'F'

def compute_pms(grade, hes):
    """Pitcher Matchup Score (1-10). Higher = better for hitter."""
    if not grade:
        return 5.0
    base = {'A+':1,'A':2,'B+':3,'B':4,'C+':6,'C':7,'D':8,'F':9}.get(grade, 5)
    hes_adj = (hes - 5) * 0.2 if hes else 0
    return clip(round(base + hes_adj), 1, 10)


# ── Data Fetching ────────────────────────────────────────────────────────────

def fetch_data(target_date, slate_filter=None):
    """Fetch all data needed for pool generation."""
    print(f"  Fetching data for {target_date}...")

    # Games
    games = sb.table('games').select(
        'game_pk,game_date,home_team,away_team,home_team_id,away_team_id,home_sp_id,away_sp_id,venue_id'
    ).eq('game_date', target_date).execute().data or []
    print(f"  Games: {len(games)}")
    if not games: return None

    game_pks = [g['game_pk'] for g in games]

    # Projections
    projs = []
    for i in range(0, 5000, 1000):
        rows = sb.table('player_projections').select(
            'player_id,game_pk,full_name,team,proj_dk_pts,proj_floor,proj_ceiling,'
            'is_pitcher,batting_order'
        ).eq('game_date', target_date).range(i, i + 999).execute().data or []
        projs.extend(rows)
        if len(rows) < 1000: break
    print(f"  Projections: {len(projs)}")

    # DK Salaries for this slate
    contest_type = 'showdown' if (slate_filter or '').startswith('sd_') else 'classic'
    sal_q = sb.table('dk_salaries').select(
        'player_id,name,position,salary,team,dk_slate'
    ).eq('season', SEASON).eq('contest_type', contest_type)
    if slate_filter:
        sal_q = sal_q.eq('dk_slate', slate_filter)
    sals = []
    for i in range(0, 5000, 1000):
        rows = sal_q.range(i, i + 999).execute().data or []
        sals.extend(rows)
        if len(rows) < 1000: break

    sal_map = {}
    for s in sals:
        if s.get('player_id') and s.get('salary'):
            sal_map[s['player_id']] = s
    # Name fallback
    sal_name_map = {}
    for s in sals:
        if s.get('name') and s.get('salary'):
            norm = s['name'].lower().replace('.', '').replace("'", '').strip()
            sal_name_map[norm] = s
    print(f"  Salaries [{slate_filter or 'all'}]: {len(sal_map)}")

    # Lineups
    lineups = sb.table('lineups').select(
        'player_id,game_pk,batting_order,status'
    ).in_('game_pk', game_pks).execute().data or []
    lineup_map = {lu['player_id']: lu for lu in lineups}

    # Odds
    odds = {}
    if game_pks:
        rows = sb.table('game_odds').select(
            'game_pk,game_total,home_implied,away_implied'
        ).in_('game_pk', game_pks).execute().data or []
        odds = {r['game_pk']: r for r in rows}

    # Ownership (per-slate if available)
    ownership = {}
    if slate_filter:
        own_rows = sb.table('slate_ownership').select(
            'player_id,proj_ownership'
        ).eq('game_date', target_date).eq('dk_slate', slate_filter).limit(5000).execute().data or []
        for r in own_rows:
            ownership[r['player_id']] = r.get('proj_ownership', 0)
    if not ownership:
        own_rows = sb.table('player_projections').select(
            'player_id,proj_ownership'
        ).eq('game_date', target_date).limit(5000).execute().data or []
        for r in own_rows:
            ownership[r['player_id']] = r.get('proj_ownership', 0)

    # Pitcher stats for SP grade → PMS computation (current season first)
    sp_ids = list(set(g.get('home_sp_id') for g in games if g.get('home_sp_id'))
                | set(g.get('away_sp_id') for g in games if g.get('away_sp_id')))
    pitcher_stats = {}
    if sp_ids:
        for i in range(0, len(sp_ids), 150):
            batch = sp_ids[i:i+150]
            rows = sb.table('pitcher_stats').select(
                'player_id,season,xfip,siera,k_pct,bb_pct,swstr_pct,stuff_plus'
            ).in_('player_id', batch).order('season', desc=True).execute().data or []
            for r in rows:
                if r['player_id'] not in pitcher_stats:
                    pitcher_stats[r['player_id']] = r

    # Park factors
    venue_ids = list(set(g.get('venue_id') for g in games if g.get('venue_id')))
    park_factors = {}
    if venue_ids:
        rows = sb.table('park_factors').select(
            'venue_id,basic_factor,hr_factor,k_factor,bb_factor,woba_con,xwoba_con'
        ).in_('venue_id', venue_ids).execute().data or []
        park_factors = {r['venue_id']: r for r in rows}

    # Weather
    weather = {}
    if game_pks:
        rows = sb.table('weather').select(
            'game_pk,temp_f,wind_speed,wind_dir,precip_pct,is_outdoor'
        ).in_('game_pk', game_pks).execute().data or []
        weather = {r['game_pk']: r for r in rows}

    return {
        'games': games, 'projs': projs, 'sal_map': sal_map,
        'sal_name_map': sal_name_map, 'lineup_map': lineup_map,
        'odds': odds, 'ownership': ownership,
        'pitcher_stats': pitcher_stats, 'park_factors': park_factors,
        'weather': weather,
    }


# ── Build Player Pool ────────────────────────────────────────────────────────

def build_player_pool(data):
    """Build enriched player pool from projections + salaries."""
    pool = []
    for p in data['projs']:
        pid = p['player_id']
        sal_row = data['sal_map'].get(pid)
        if not sal_row:
            norm = (p.get('full_name') or '').lower().replace('.', '').replace("'", '').strip()
            sal_row = data['sal_name_map'].get(norm)
        if not sal_row or not sal_row.get('salary'): continue
        # Always use the salary player_id — it matches what the frontend loads from dk_salaries
        pid = sal_row.get('player_id', pid)

        proj = safe(p.get('proj_dk_pts'), 0)
        if proj <= 0: continue

        salary = sal_row['salary']
        is_pitcher = p.get('is_pitcher', False)

        # Canonical position
        if is_pitcher:
            pos = 'SP'
        else:
            dk_positions = sal_row.get('position', '').split('/')
            pos = 'OF'
            for pp in POS_PRIORITY:
                if pp in dk_positions:
                    pos = pp
                    break

        # All eligible positions (for multi-pos)
        all_positions = sal_row.get('position', pos).split('/')
        if is_pitcher:
            all_positions = ['SP']

        lu = data['lineup_map'].get(pid)
        batting_order = lu.get('batting_order') if lu else None
        confirmed = lu and lu.get('batting_order') and lu['batting_order'] >= 1

        odds_row = data['odds'].get(p.get('game_pk'))
        game_total = safe(odds_row.get('game_total'), 8.5) if odds_row else 8.5

        raw_team = sal_row.get('team', '') or p.get('team', '')
        team = TEAM_ABBR_MAP.get(raw_team, raw_team)
        own = data['ownership'].get(pid, 5.0)

        pool.append({
            'player_id': pid,
            'name': p.get('full_name') or sal_row.get('name', '?'),
            'team': team,
            'pos': pos,
            'all_positions': all_positions,
            'salary': salary,
            'proj': proj,
            'floor': safe(p.get('proj_floor'), proj * 0.5),
            'ceiling': safe(p.get('proj_ceiling'), proj * 1.5),
            'is_pitcher': is_pitcher,
            'game_pk': p.get('game_pk'),
            'batting_order': batting_order,
            'confirmed': confirmed,
            'game_total': game_total,
            'ownership': own,
        })

    # Dedup by name (keep best projection)
    by_name = defaultdict(list)
    for p in pool:
        by_name[p['name'].lower().strip()].append(p)
    deduped = []
    for group in by_name.values():
        best = max(group, key=lambda p: p['proj'])
        # Inherit batting order
        bo = next((p['batting_order'] for p in group if p['batting_order']), None)
        best['batting_order'] = bo
        deduped.append(best)

    # ── Attach PMS / HES to each player ────────────────────────────────────
    # Build game → opposing SP grade + HES lookup
    game_hes_map = {}   # game_pk → HES score
    game_sp_grade = {}  # team_id → opposing SP grade letter
    for g in data['games']:
        gpk = g['game_pk']
        pf = data.get('park_factors', {}).get(g.get('venue_id'))
        wx = data.get('weather', {}).get(gpk)
        hes = compute_hes(pf, wx)
        game_hes_map[gpk] = hes

        home_sp = data.get('pitcher_stats', {}).get(g.get('home_sp_id'))
        away_sp = data.get('pitcher_stats', {}).get(g.get('away_sp_id'))
        # Away team faces home SP; home team faces away SP
        game_sp_grade[g.get('away_team_id')] = compute_sp_grade(home_sp)
        game_sp_grade[g.get('home_team_id')] = compute_sp_grade(away_sp)

    # Map DK abbreviation → team_id via dk_salaries team + game_pk
    # Each pool player has game_pk and team (DK abbr). Games have team_ids.
    # Bridge: for each game, find which DK team abbr maps to home vs away team_id
    # by checking dk_salaries: the SP player_id for home_sp must match a sal row
    # with a specific team abbreviation.
    gpk_abbr_to_id = {}  # (game_pk, dk_team_abbr) → team_id
    for g in data['games']:
        gpk = g['game_pk']
        home_id, away_id = g.get('home_team_id'), g.get('away_team_id')
        home_sp, away_sp = g.get('home_sp_id'), g.get('away_sp_id')
        # Look up SP in salary map to find DK team abbr
        home_sal = data['sal_map'].get(home_sp)
        away_sal = data['sal_map'].get(away_sp)
        if home_sal and home_sal.get('team'):
            ht = TEAM_ABBR_MAP.get(home_sal['team'], home_sal['team'])
            gpk_abbr_to_id[(gpk, ht)] = home_id
        if away_sal and away_sal.get('team'):
            at = TEAM_ABBR_MAP.get(away_sal['team'], away_sal['team'])
            gpk_abbr_to_id[(gpk, at)] = away_id
        # Also assign the other team by exclusion if one is known
        known = set()
        if (gpk, ht if home_sal else '') in gpk_abbr_to_id:
            known.add(gpk_abbr_to_id.get((gpk, ht)))
        if (gpk, at if away_sal else '') in gpk_abbr_to_id:
            known.add(gpk_abbr_to_id.get((gpk, at)))

    # Fallback: collect all DK team abbrs per game_pk from pool, assign by elimination
    gpk_teams = defaultdict(set)
    for p in deduped:
        if p.get('game_pk'):
            gpk_teams[p['game_pk']].add(p['team'])
    for gpk, teams in gpk_teams.items():
        g = next((g for g in data['games'] if g['game_pk'] == gpk), None)
        if not g: continue
        home_id, away_id = g.get('home_team_id'), g.get('away_team_id')
        assigned = {v for (gp, _), v in gpk_abbr_to_id.items() if gp == gpk}
        unassigned = [t for t in teams if (gpk, t) not in gpk_abbr_to_id]
        for t in unassigned:
            if home_id not in assigned:
                gpk_abbr_to_id[(gpk, t)] = home_id
                assigned.add(home_id)
            elif away_id not in assigned:
                gpk_abbr_to_id[(gpk, t)] = away_id
                assigned.add(away_id)

    for p in deduped:
        gpk = p.get('game_pk')
        hes = game_hes_map.get(gpk, 5.0)
        p['hes'] = hes
        tid = gpk_abbr_to_id.get((gpk, p['team']))
        grade = game_sp_grade.get(tid)
        p['pms'] = compute_pms(grade, hes)

    # Log PMS/HES per team
    team_pms = defaultdict(list)
    for p in deduped:
        if not p['is_pitcher']:
            team_pms[p['team']].append(p.get('pms', 5))
    pms_summary = sorted(
        [(t, sum(v)/len(v)) for t, v in team_pms.items()],
        key=lambda x: x[1], reverse=True
    )
    print(f"  PMS by team: {', '.join(f'{t}={avg:.1f}' for t, avg in pms_summary[:10])}")

    return deduped


# ── Greedy Lineup Builder ────────────────────────────────────────────────────

STACK_CONFIGS = [
    {'name': '5-3',       'main': 5, 'subs': [3]},    # 20% — 5+3 is the GPP winner
    {'name': '5-3',       'main': 5, 'subs': [3]},
    {'name': '5-2',       'main': 5, 'subs': [2]},    # 10%
    {'name': '4-3',       'main': 4, 'subs': [3]},    # 30% — diversified correlation
    {'name': '4-3',       'main': 4, 'subs': [3]},
    {'name': '4-3',       'main': 4, 'subs': [3]},
    {'name': '4-3-2',     'main': 4, 'subs': [3, 2]}, # 20% — multi-game correlation
    {'name': '4-3-2',     'main': 4, 'subs': [3, 2]},
    {'name': '3-3-2',     'main': 3, 'subs': [3, 2]}, # 10% — max diversification
    {'name': '5-naked',   'main': 5, 'subs': []},     # 10% — naked 5-stack upside
]

def build_lineup_greedy(pool, scores, main_team=None, main_size=4,
                         sub_teams=None, sub_sizes=None, rng=None,
                         pvh_off=False, game_teams=None, pvh_stack_only=False,
                         salary_floor=None):
    """
    Build one DK Classic lineup using greedy randomized selection.
    Supports multiple sub-stacks and bring-backs.
    pvh_off: if True, hitters cannot face a pitcher in the same lineup.
    pvh_stack_only: if True, PvH only blocks stacks (main/sub), not individual fills.
    game_teams: dict mapping (game_pk, team_abbr) → opposing_team_abbr.
    """
    if salary_floor is None:
        salary_floor = SALARY_FLOOR
    if rng is None: rng = np.random.default_rng()

    # Group by position (multi-pos: player appears in all eligible buckets)
    pos_players = defaultdict(list)
    for i, p in enumerate(pool):
        for pp in p['all_positions']:
            if pp in POS_SLOTS:
                pos_players[pp].append((i, scores[i], p))

    # Sort each position by score descending
    for pos in pos_players:
        pos_players[pos].sort(key=lambda x: x[1], reverse=True)

    # Cheapest per position
    cheapest = {}
    for pos, players in pos_players.items():
        cheapest[pos] = min(p[2]['salary'] for p in players) if players else 3000

    remaining = dict(POS_SLOTS)
    sal_left = SALARY_CAP
    selected = []
    used_pids = set()
    # Track position assignment for each player so we can output in DK slot order
    pid_to_pos = {}  # player_id → assigned position (e.g. 'SP', 'C', '1B', ...)
    pvh_excluded_teams = set()  # teams whose hitters face a selected SP

    # PvH: fill SPs FIRST so we know which teams to exclude from stack/hitter selection
    if pvh_off and game_teams and remaining.get('SP', 0) > 0:
        sp_candidates = [(i, s, p) for i, s, p in pos_players.get('SP', [])
                         if p['player_id'] not in used_pids]
        sp_slots = remaining['SP']
        for _ in range(sp_slots):
            if not sp_candidates:
                break
            top_k = 2
            top = sp_candidates[:top_k]
            weights = np.array([max(s, 0.1) for _, s, _ in top])
            weights /= weights.sum()
            pick_idx = rng.choice(len(top), p=weights)
            pick = top[pick_idx]
            selected.append(pick[2]['player_id'])
            used_pids.add(pick[2]['player_id'])
            sal_left -= pick[2]['salary']
            remaining['SP'] -= 1
            pid_to_pos[pick[2]['player_id']] = 'SP'
            # Track opposing team
            opp = game_teams.get((pick[2].get('game_pk'), pick[2]['team']))
            if opp:
                pvh_excluded_teams.add(opp)
            sp_candidates = [(i, s, p) for i, s, p in sp_candidates
                             if p['player_id'] not in used_pids]

    # Force main stack players first (skip if main_team is PvH-excluded)
    if main_team and main_size > 0 and main_team in pvh_excluded_teams:
        return None  # can't stack a team facing our pitcher
    if main_team and main_size > 0:
        team_hitters = [(i, s, p) for i, s, p in pos_players.get('OF', []) + pos_players.get('1B', []) +
                        pos_players.get('2B', []) + pos_players.get('3B', []) + pos_players.get('SS', []) +
                        pos_players.get('C', []) if p['team'] == main_team and p['player_id'] not in used_pids]
        team_hitters.sort(key=lambda x: x[1], reverse=True)
        # Remove duplicates (same player in multiple pos buckets)
        seen = set()
        unique_team = []
        for item in team_hitters:
            if item[0] not in seen:
                seen.add(item[0])
                unique_team.append(item)

        picked_main = 0
        for idx, score, p in unique_team:
            if picked_main >= main_size: break
            if p['is_pitcher']: continue
            if p['player_id'] in used_pids: continue
            # Find which position to assign
            assigned = False
            for pp in p['all_positions']:
                if pp in remaining and remaining[pp] > 0 and pp != 'SP':
                    selected.append(p['player_id'])
                    used_pids.add(p['player_id'])
                    sal_left -= p['salary']
                    remaining[pp] -= 1
                    pid_to_pos[p['player_id']] = pp
                    picked_main += 1
                    assigned = True
                    break
            # If no eligible slot available, skip this player (don't force into wrong position)

        if picked_main < main_size:
            return None  # couldn't fill main stack

    # Force sub stacks (can be multiple: e.g., [3] or [2, 2])
    if sub_teams is None: sub_teams = []
    if sub_sizes is None: sub_sizes = []
    for st, ss in zip(sub_teams, sub_sizes):
        if not st or ss <= 0: continue
        if st in pvh_excluded_teams: continue  # PvH: skip sub stacks facing our pitcher
        sub_hitters = [(i, s, p) for pos in ['OF','1B','2B','3B','SS','C'] for i, s, p in pos_players.get(pos, [])
                       if p['team'] == st and p['player_id'] not in used_pids and not p['is_pitcher']]
        sub_hitters.sort(key=lambda x: x[1], reverse=True)
        seen_sub = set()
        unique_sub = [item for item in sub_hitters if item[0] not in seen_sub and not seen_sub.add(item[0])]

        picked_sub = 0
        for idx, score, p in unique_sub:
            if picked_sub >= ss: break
            for pp in p['all_positions']:
                if pp in remaining and remaining[pp] > 0 and pp != 'SP':
                    selected.append(p['player_id'])
                    used_pids.add(p['player_id'])
                    sal_left -= p['salary']
                    remaining[pp] -= 1
                    pid_to_pos[p['player_id']] = pp
                    picked_sub += 1
                    break

    # Fill remaining slots greedily with randomization
    # Track per-team hitter counts to cap at 5 (DK Classic max practical stacking)
    team_hitter_count = defaultdict(int)
    for pid in selected:
        for p in pool:
            if p['player_id'] == pid and not p['is_pitcher']:
                team_hitter_count[p['team']] += 1
                break
    MAX_HITTERS_PER_TEAM = 5

    # PvH: fill SP first so we know which teams to exclude from hitter candidates
    pos_order = list(remaining.keys())
    if pvh_off and game_teams:
        sp_positions = [p for p in pos_order if p == 'SP']
        non_sp = [p for p in pos_order if p != 'SP']
        rng.shuffle(non_sp)
        pos_order = sp_positions + non_sp
    else:
        rng.shuffle(pos_order)

    # Don't reset pvh_excluded_teams — keep teams excluded by SP-first picks above

    for pos in pos_order:
        slots_needed = remaining[pos]
        while slots_needed > 0:
            # Reserve salary for remaining positions
            reserve = sum(cheapest.get(rp, 3000) * (remaining[rp] if rp != pos else max(0, slots_needed - 1))
                          for rp in remaining if remaining[rp] > 0)
            budget = sal_left - reserve

            # pvh_stack_only: PvH already enforced on stacks; individual fills ignore PvH
            pvh_check = pvh_excluded_teams if not pvh_stack_only else set()
            candidates = [(i, s, p) for i, s, p in pos_players.get(pos, [])
                          if p['player_id'] not in used_pids and p['salary'] <= budget
                          and (p['is_pitcher'] or team_hitter_count[p['team']] < MAX_HITTERS_PER_TEAM)
                          and (p['is_pitcher'] or p['team'] not in pvh_check)]

            if not candidates:
                # Fallback: cheapest available (still respect team cap)
                fallback = [(i, s, p) for i, s, p in pos_players.get(pos, [])
                            if p['player_id'] not in used_pids and p['salary'] <= sal_left
                            and (p['is_pitcher'] or team_hitter_count[p['team']] < MAX_HITTERS_PER_TEAM)
                            and (p['is_pitcher'] or p['team'] not in pvh_check)]
                if not fallback:
                    # Can't fill this position — reject lineup
                    return None
                pick = fallback[-1]  # cheapest
            else:
                # Tighter selection for SP (quality matters more), wider for hitters
                top_k = 2 if pos == 'SP' else 3
                top = candidates[:top_k]
                weights = np.array([max(s, 0.1) for _, s, _ in top])
                weights /= weights.sum()
                pick_idx = rng.choice(len(top), p=weights)
                pick = top[pick_idx]

            selected.append(pick[2]['player_id'])
            used_pids.add(pick[2]['player_id'])
            sal_left -= pick[2]['salary']
            pid_to_pos[pick[2]['player_id']] = pos
            if not pick[2]['is_pitcher']:
                team_hitter_count[pick[2]['team']] += 1
            # PvH: when an SP is picked, exclude the opposing team's hitters
            if pvh_off and game_teams and pick[2]['is_pitcher']:
                opp = game_teams.get((pick[2].get('game_pk'), pick[2]['team']))
                if opp:
                    pvh_excluded_teams.add(opp)
            remaining[pos] -= 1
            slots_needed -= 1

    if len(selected) != 10:
        return None
    total_sal = SALARY_CAP - sal_left
    if total_sal < salary_floor or total_sal > SALARY_CAP:
        return None

    # Output in DK slot order: SP, SP, C, 1B, 2B, 3B, SS, OF, OF, OF
    DK_SLOT_ORDER = ['SP', 'SP', 'C', '1B', '2B', '3B', 'SS', 'OF', 'OF', 'OF']
    pos_buckets = defaultdict(list)
    for pid in selected:
        pos_buckets[pid_to_pos[pid]].append(pid)
    ordered = []
    for pos in DK_SLOT_ORDER:
        if pos_buckets[pos]:
            ordered.append(pos_buckets[pos].pop(0))
        else:
            return None  # position couldn't be filled — shouldn't happen
    return ordered


# ── Noise Sampling ───────────────────────────────────────────────────────────

def sample_noisy_scores(pool, rng, mode='user', contest_type='gpp', contest_discounts=None):
    """
    Sample noisy projection scores for one sim.
    mode='user': real projections with game/team/individual correlation
    mode='contest': ownership-weighted public bias scoring
    """
    scores = np.zeros(len(pool))
    if contest_discounts is None:
        contest_discounts = CONTEST_DISCOUNT_TEAMS

    if mode == 'contest':
        profile = CONTEST_PROFILES.get(contest_type, CONTEST_PROFILES['gpp'])
        # Public bias scoring (same as sim_ownership.py)
        for i, p in enumerate(pool):
            proj_score = p['proj'] ** 1.5
            sal_score = (p['salary'] / 3500) ** 1.3
            bo = p['batting_order']
            if not p['is_pitcher'] and bo:
                bo_score = {1:1.30,2:1.25,3:1.20,4:1.15,5:1.05,
                            6:0.90,7:0.80,8:0.70,9:0.60}.get(bo, 0.75)
            else:
                bo_score = 1.0
            env_score = (p['game_total'] / 8.5) ** 2.0
            value = (p['proj'] / p['salary'] * 1000) if p['salary'] > 0 else 0
            value_score = value ** 0.6
            base = proj_score * 0.35 + sal_score * 0.25 + value_score * 0.05 + env_score * 0.20 + bo_score * 0.15
            if p['is_pitcher']:
                base *= profile['pitcher_mult']
            if not p['is_pitcher'] and not p['confirmed']:
                base *= profile['unconf_pen']
            # Discount teams with weather/PPD risk
            discount = contest_discounts.get(p.get('team'), 1.0)
            if discount < 1.0:
                base *= discount
            # Multiplicative noise
            noise_sd = profile['noise_pit'] if p['is_pitcher'] else profile['noise_hit']
            mult = 1.0 + rng.normal(0, noise_sd)
            scores[i] = base * max(mult, 0.05)
    else:
        # User mode: real projection + correlated noise
        game_z = {}
        team_z = {}
        for p in pool:
            gk = p.get('game_pk')
            if gk and gk not in game_z:
                game_z[gk] = rng.normal(0, 1)
            tk = p.get('team')
            if tk and tk not in team_z:
                team_z[tk] = rng.normal(0, 1)

        H_W_GAME, H_W_TEAM, H_W_INDIV = 0.387, 0.447, 0.806
        P_W_GAME, P_W_TEAM, P_W_INDIV = -0.30, 0.15, 0.94

        for i, p in enumerate(pool):
            # Blend toward ceiling for GPP upside
            mean = p['proj'] * (1 - UPSIDE_BLEND) + p['ceiling'] * UPSIDE_BLEND
            sd = (p['ceiling'] - p['floor']) / 3.3 if p['ceiling'] > p['floor'] else max(mean * 0.20, 0.5)
            sd = max(sd, 0.5)

            zg = game_z.get(p.get('game_pk'), 0)
            zt = team_z.get(p.get('team'), 0)
            zi = rng.normal(0, 1)

            if p['is_pitcher']:
                noise = P_W_GAME * zg + P_W_TEAM * zt + P_W_INDIV * zi
            else:
                noise = H_W_GAME * zg + H_W_TEAM * zt + H_W_INDIV * zi

            # PMS/HES edge boost: scale mean for high-edge matchups
            # PMS 7+ (weak pitcher) = +8%, PMS 5 = neutral, PMS 3 = -4%
            # HES 7+ (great env) = +5%, HES 5 = neutral, HES 3 = -3%
            if not p['is_pitcher']:
                pms = p.get('pms', 5)
                hes = p.get('hes', 5)
                edge_mult = 1.0 + (pms - 5) * 0.04 + (hes - 5) * 0.025
                edge_mult = clip(edge_mult, 0.90, 1.15)
                mean *= edge_mult

            scores[i] = max(0, mean + sd * noise)

    return scores


# ── Pool Generation ──────────────────────────────────────────────────────────

def generate_lineups(pool, n_lineups, mode='user', rng=None, game_count=0,
                     contest_type='gpp', contest_discounts=None,
                     exclude_teams=None, exposure_caps=None,
                     hitter_exp_max=100, pitcher_exp_max=100):
    """Generate n_lineups unique lineups using greedy randomized builder."""
    if rng is None: rng = np.random.default_rng()

    # PvH exclusion: don't pair hitters facing a lineup's SP
    # Full PvH (all positions): slates with 5+ games — enough teams to fill around exclusions
    # Stack-only PvH: 3-4 game slates — only block the main/sub stack from facing the SP,
    #   but allow individual hitter fills from any team (otherwise pool is too constrained)
    pvh_off = True
    pvh_stack_only = game_count < 5  # on short slates, PvH only blocks stacks
    game_teams = {}
    if pvh_off:
        # Build (game_pk, team_abbr) → opposing_team_abbr from pool data
        games_teams_map = defaultdict(set)  # game_pk → set of team abbreviations
        for p in pool:
            gpk = p.get('game_pk')
            if gpk and p.get('team'):
                games_teams_map[gpk].add(p['team'])
        for gpk, teams in games_teams_map.items():
            teams_list = list(teams)
            if len(teams_list) == 2:
                game_teams[(gpk, teams_list[0])] = teams_list[1]
                game_teams[(gpk, teams_list[1])] = teams_list[0]
        mode = "stack-only" if pvh_stack_only else "full"
        print(f"    PvH exclusion: ON ({mode}, {game_count} games, {len(game_teams)//2} matchups)")

    # Dynamic salary floor: on short slates with cheap players, lower the floor
    # so the builder can actually produce lineups
    avg_sal = np.mean([p['salary'] for p in pool]) if pool else 5000
    dynamic_floor = max(44000, min(SALARY_FLOOR, int(avg_sal * 10 * 0.92)))
    if dynamic_floor < SALARY_FLOOR:
        print(f"    Salary floor: ${dynamic_floor:,} (lowered from ${SALARY_FLOOR:,}, avg player ${avg_sal:,.0f})")

    # Get viable main teams (4+ hitters)
    team_hitters = defaultdict(list)
    for p in pool:
        if not p['is_pitcher']:
            team_hitters[p['team']].append(p)
    viable_teams = [t for t, hs in team_hitters.items() if len(hs) >= 4]

    # User pool: exclude teams (weather/PPD, or from request)
    _exclude = exclude_teams or (USER_EXCLUDE_TEAMS if mode == 'user' else set())
    if _exclude:
        excluded = [t for t in viable_teams if t in _exclude]
        viable_teams = [t for t in viable_teams if t not in _exclude]
        if excluded:
            print(f"    Excluding teams: {excluded}")

    print(f"    Viable main teams: {len(viable_teams)} — {viable_teams}")

    if not viable_teams:
        print("    No viable teams — skipping")
        return []

    # Teams that can support 5-man stacks
    viable_5 = [t for t, hs in team_hitters.items() if len(hs) >= 5]
    # Teams that can support 3-man sub stacks
    viable_3sub = [t for t, hs in team_hitters.items() if len(hs) >= 3]

    # Team weighting: blend of leverage (ceiling/ownership) and game environment.
    # Leverage alone over-indexes on low-owned teams regardless of game quality.
    # Environment must be a strong independent factor so high-total games
    # (e.g. Wrigley 12.5) aren't starved of exposure.
    team_leverage = {}
    for t in viable_teams:
        hitters = team_hitters[t]
        ceiling_sum = sum(h.get('ceiling', h['proj'] * 1.5) for h in hitters)
        own_sum = sum(h.get('ownership', 5.0) for h in hitters)
        lev = ceiling_sum / max(own_sum, 5.0)
        gt = hitters[0].get('game_total', 8.5) if hitters else 8.5
        env = (gt / 8.5) ** 2.5  # 12.5 → 2.83x, 9.0 → 1.18x, 7.0 → 0.58x
        # PMS factor: teams facing weak pitchers get a matchup boost
        pms_vals = [h.get('pms', 5) for h in hitters if h.get('pms')]
        avg_pms = sum(pms_vals) / len(pms_vals) if pms_vals else 5.0
        pms_mult = (avg_pms / 5.0) ** 1.5  # PMS 8 → 1.86x, PMS 5 → 1.0x, PMS 3 → 0.46x
        team_leverage[t] = {'lev': lev, 'env': env * pms_mult, 'gt': gt}
    # Normalize each component independently, then blend
    levs = np.array([team_leverage[t]['lev'] for t in viable_teams])
    envs = np.array([team_leverage[t]['env'] for t in viable_teams])
    norm_lev = levs / levs.sum() if levs.sum() > 0 else np.ones(len(levs)) / len(levs)
    norm_env = envs / envs.sum() if envs.sum() > 0 else np.ones(len(envs)) / len(envs)
    blended = norm_lev * 0.50 + norm_env * 0.50
    team_weights = blended / blended.sum()
    if mode == 'user':
        min_weight = max(0.025, 1.0 / len(viable_teams))
        team_weights = np.maximum(team_weights, min_weight)
        team_weights /= team_weights.sum()
    # Also compute for viable_5
    if viable_5:
        v5_idx = [viable_teams.index(t) for t in viable_5 if t in viable_teams]
        lev_5_arr = blended[v5_idx] if v5_idx else None
        team_5_weights = lev_5_arr / lev_5_arr.sum() if lev_5_arr is not None and len(lev_5_arr) else None
    else:
        team_5_weights = None

    # Log team weights
    tw_sorted = sorted(zip(viable_teams, team_weights), key=lambda x: x[1], reverse=True)
    print(f"    Team weights (lev+env): {', '.join(f'{t}={w*100:.1f}%' for t, w in tw_sorted[:10])}")

    lineups = []
    seen = set()
    attempts = 0
    max_attempts = n_lineups * 4
    config_idx = 0

    # User pool: cap any single team at 15% of lineups (20% for high-total games)
    team_stack_counts = defaultdict(int)
    team_game_totals = {}
    for p in pool:
        t = p.get('team')
        if t and t not in team_game_totals:
            team_game_totals[t] = p.get('game_total', 8.5)
    base_cap_pct = 0.15
    team_cap_map = {}
    if mode == 'user':
        for t in viable_teams:
            gt = team_game_totals.get(t, 8.5)
            # High-total games (10+) get up to 20% cap
            cap_pct = base_cap_pct + clip((gt - 9.0) * 0.015, 0, 0.05)
            team_cap_map[t] = int(n_lineups * cap_pct)
    team_cap = int(n_lineups * base_cap_pct) if mode == 'user' else None

    # Per-player exposure tracking
    player_appear = defaultdict(int)
    _exp_caps = exposure_caps or {}
    _exp_caps = {int(k): v for k, v in _exp_caps.items() if str(k).isdigit()}  # skip cpt_/flex_ SD keys
    _pool_lookup = {p['player_id']: p for p in pool}  # fast lookup for cap checks

    while len(lineups) < n_lineups and attempts < max_attempts:
        # First cycle: round-robin for coverage; then leverage-weighted
        if attempts < len(viable_teams):
            main_team = viable_teams[attempts % len(viable_teams)]
        else:
            main_team = rng.choice(viable_teams, p=team_weights)

        # Skip teams that hit the cap (user pool only, per-team cap for high-total games)
        effective_cap = team_cap_map.get(main_team, team_cap) if team_cap_map else team_cap
        if effective_cap and team_stack_counts[main_team] >= effective_cap:
            attempts += 1
            continue

        config = STACK_CONFIGS[config_idx % len(STACK_CONFIGS)]
        config_idx += 1
        attempts += 1

        main_size = config['main']

        # Skip 5-man configs for teams without enough hitters
        if main_size >= 5 and main_team not in viable_5:
            continue

        # Determine sub teams
        sub_candidates = [t for t in (viable_3sub if any(s >= 3 for s in config['subs']) else viable_teams)
                          if t != main_team]
        sub_teams = []
        sub_sizes = config['subs']
        used_sub = set()
        for ss in sub_sizes:
            cands = [t for t in sub_candidates if t not in used_sub and len(team_hitters.get(t, [])) >= ss]
            if cands:
                # Weight sub-stack selection toward high-environment teams
                sub_wts = np.array([team_leverage.get(t, {}).get('env', 1.0) for t in cands])
                sub_wts = sub_wts / sub_wts.sum()
                st = rng.choice(cands, p=sub_wts)
                sub_teams.append(st)
                used_sub.add(st)
            else:
                sub_teams.append(None)

        # Filter out capped players before building
        # Use absolute max appearances (% of target pool size) — not rolling ratio
        build_pool = pool
        if _exp_caps or hitter_exp_max < 100 or pitcher_exp_max < 100:
            capped_pids = set()
            for pid, cnt in player_appear.items():
                if pid in _exp_caps:
                    max_appearances = int(n_lineups * _exp_caps[pid] / 100.0)
                    if cnt >= max_appearances:
                        capped_pids.add(pid)
                # Check global position cap
                p_obj = _pool_lookup.get(pid)
                if p_obj:
                    global_cap = pitcher_exp_max if p_obj['is_pitcher'] else hitter_exp_max
                    if global_cap < 100:
                        max_app = int(n_lineups * global_cap / 100.0)
                        if cnt >= max_app:
                            capped_pids.add(pid)
            if capped_pids:
                build_pool = [p for p in pool if p['player_id'] not in capped_pids]

        # Sample noisy scores
        scores = sample_noisy_scores(build_pool, rng, mode=mode,
                                      contest_type=contest_type,
                                      contest_discounts=contest_discounts)

        # Build lineup
        lu = build_lineup_greedy(build_pool, scores, main_team=main_team, main_size=main_size,
                                  sub_teams=sub_teams, sub_sizes=sub_sizes, rng=rng,
                                  pvh_off=pvh_off, game_teams=game_teams,
                                  pvh_stack_only=pvh_stack_only, salary_floor=dynamic_floor)
        if lu is None:
            continue

        # Dedup
        key = tuple(sorted(lu))
        if key in seen:
            continue
        seen.add(key)

        # Compute metadata
        salary = sum(p['salary'] for p in pool if p['player_id'] in set(lu))
        proj = sum(p['proj'] for p in pool if p['player_id'] in set(lu))

        # Find main stack
        pid_set = set(lu)
        team_counts = defaultdict(int)
        for p in pool:
            if p['player_id'] in pid_set and not p['is_pitcher']:
                team_counts[p['team']] += 1
        stack_team = max(team_counts, key=team_counts.get) if team_counts else ''
        stack_size = team_counts.get(stack_team, 0)

        # Sub stack
        sub_counts = {t: c for t, c in team_counts.items() if t != stack_team and c >= 2}
        sub_t = max(sub_counts, key=sub_counts.get) if sub_counts else None
        sub_s = sub_counts.get(sub_t, 0) if sub_t else 0

        lineups.append({
            'player_ids': list(lu),
            'salary': salary,
            'proj': round(proj, 2),
            'stack_team': stack_team,
            'stack_size': stack_size,
            'sub_team': sub_t,
            'sub_size': sub_s,
        })
        team_stack_counts[stack_team] += 1
        for pid in lu:
            player_appear[pid] += 1

        if len(lineups) % 1000 == 0:
            print(f"    Generated {len(lineups):,} / {n_lineups:,} ({attempts:,} attempts)")

    print(f"    Final: {len(lineups):,} unique lineups from {attempts:,} attempts")

    # ── User pool top-up: ensure every viable team hits 2.5% floor ──────────
    if mode == 'user' and lineups:
        floor_count = max(1, int(len(lineups) * 0.025))
        teams_count = defaultdict(int)
        for lu in lineups:
            teams_count[lu['stack_team']] += 1

        for t in viable_teams:
            deficit = floor_count - teams_count.get(t, 0)
            if deficit <= 0:
                continue
            print(f"    Top-up: {t} needs {deficit} more lineups to reach 2.5% floor")
            added = 0
            topup_attempts = 0
            max_topup = deficit * 25  # high multiplier for thin rosters
            n_hitters = len(team_hitters.get(t, []))
            while added < deficit and topup_attempts < max_topup:
                topup_attempts += 1
                config = STACK_CONFIGS[rng.integers(len(STACK_CONFIGS))]
                main_size = config['main']
                if main_size >= 5 and t not in viable_5:
                    main_size = 4
                # For thin rosters (< 6 hitters), try 3-man stacks too
                if n_hitters < 6 and main_size > 3:
                    main_size = rng.choice([3, 4])
                sub_cands = [st for st in viable_3sub if st != t]
                sub_teams_tu = []
                for ss in config['subs']:
                    cs = [st for st in sub_cands if st not in sub_teams_tu and len(team_hitters.get(st, [])) >= ss]
                    sub_teams_tu.append(rng.choice(cs) if cs else None)
                scores = sample_noisy_scores(pool, rng, mode='user')
                lu = build_lineup_greedy(pool, scores, main_team=t, main_size=main_size,
                                          sub_teams=sub_teams_tu, sub_sizes=config['subs'], rng=rng,
                                          pvh_off=pvh_off, game_teams=game_teams,
                                          pvh_stack_only=pvh_stack_only, salary_floor=dynamic_floor)
                if lu is None:
                    continue
                key = tuple(sorted(lu))
                if key in seen:
                    continue
                seen.add(key)
                salary = sum(p['salary'] for p in pool if p['player_id'] in set(lu))
                proj = sum(p['proj'] for p in pool if p['player_id'] in set(lu))
                pid_set = set(lu)
                tc = defaultdict(int)
                for p in pool:
                    if p['player_id'] in pid_set and not p['is_pitcher']:
                        tc[p['team']] += 1
                st_team = max(tc, key=tc.get) if tc else ''
                st_size = tc.get(st_team, 0)
                sub_c = {st: c for st, c in tc.items() if st != st_team and c >= 2}
                sub_t = max(sub_c, key=sub_c.get) if sub_c else None
                sub_s = sub_c.get(sub_t, 0) if sub_t else 0
                lineups.append({
                    'player_ids': list(lu), 'salary': salary, 'proj': round(proj, 2),
                    'stack_team': st_team, 'stack_size': st_size,
                    'sub_team': sub_t, 'sub_size': sub_s,
                })
                added += 1
            if added > 0:
                print(f"    Top-up: added {added} lineups for {t}")

    return lineups


# ── Showdown Pool Generation ────────────────────────────────────────────────

SD_SALARY_CAP = 50000

def build_lineup_sd(pool, scores, rng, salary_cap=SD_SALARY_CAP,
                    cpt_pool=None, flex_pool=None):
    """
    Build one Showdown lineup: 1 CPT (1.5x sal/proj) + 5 FLEX.
    cpt_pool/flex_pool: index lists into `pool` for eligible CPT/FLEX candidates.
    Returns (cpt_pid, [flex_pids]) or None.
    """
    if cpt_pool is None:
        cpt_pool = list(range(len(pool)))
    if flex_pool is None:
        flex_pool = list(range(len(pool)))

    # Rank CPT candidates by score (already 1.5x in scores if desired)
    cpt_ranked = sorted(cpt_pool, key=lambda i: scores[i], reverse=True)

    # Try top-K CPT candidates with weighted random
    top_k = min(5, len(cpt_ranked))
    if top_k == 0:
        return None
    top_cpt = cpt_ranked[:top_k]
    weights = np.array([max(scores[i], 0.1) for i in top_cpt])
    weights /= weights.sum()
    cpt_idx = top_cpt[rng.choice(len(top_cpt), p=weights)]
    cpt = pool[cpt_idx]
    cpt_pid = cpt['player_id']
    cpt_sal = cpt['salary'] * 1.5
    sal_left = salary_cap - cpt_sal

    # Fill 5 FLEX with randomized top-K selection — skip CPT player
    flex_ranked = sorted(flex_pool, key=lambda i: scores[i], reverse=True)
    flex_candidates = [i for i in flex_ranked if pool[i]['player_id'] != cpt_pid]
    flex_pids = []
    used_flex = set()
    for _ in range(5):
        # Filter to affordable candidates not yet picked
        affordable = [i for i in flex_candidates
                      if i not in used_flex and pool[i]['salary'] <= sal_left]
        if not affordable:
            break
        top_k = min(3, len(affordable))
        top = affordable[:top_k]
        wts = np.array([max(scores[i], 0.1) for i in top])
        wts /= wts.sum()
        pick = top[rng.choice(len(top), p=wts)]
        flex_pids.append(pool[pick]['player_id'])
        used_flex.add(pick)
        sal_left -= pool[pick]['salary']

    if len(flex_pids) < 5:
        return None

    return (cpt_pid, flex_pids)


def generate_sd_lineups(pool, n_lineups, mode='user', rng=None,
                        contest_type='gpp', excluded_cpt=None, excluded_flex=None,
                        exposure_caps=None, cpt_exp_max=35,
                        hitter_exp_max=100, pitcher_exp_max=100,
                        contest_discounts=None, salary_cap=SD_SALARY_CAP):
    """Generate N Showdown lineups with separate CPT/FLEX exposure tracking."""
    if rng is None:
        rng = np.random.default_rng()

    excluded_cpt = set(excluded_cpt or [])
    excluded_flex = set(excluded_flex or [])
    _exp_caps = exposure_caps or {}
    _exp_caps = {k: v for k, v in _exp_caps.items()}  # keep string keys for cpt_/flex_ prefix

    # Build index lists for CPT and FLEX eligible players
    cpt_indices = [i for i, p in enumerate(pool) if p['player_id'] not in excluded_cpt]
    flex_indices = [i for i, p in enumerate(pool) if p['player_id'] not in excluded_flex]

    print(f"    SD pool: {len(pool)} players, {len(cpt_indices)} CPT eligible, {len(flex_indices)} FLEX eligible")

    lineups = []
    seen = set()
    attempts = 0
    max_attempts = n_lineups * 6

    # Separate CPT/FLEX appearance tracking
    cpt_appear = defaultdict(int)
    flex_appear = defaultdict(int)

    while len(lineups) < n_lineups and attempts < max_attempts:
        attempts += 1
        # Filter capped players from this iteration's eligible lists
        # Use absolute max appearances (% of target pool size)
        iter_cpt = cpt_indices
        iter_flex = flex_indices

        if _exp_caps or cpt_exp_max < 100 or hitter_exp_max < 100 or pitcher_exp_max < 100:
            capped_cpt = set()
            capped_flex = set()
            for i in cpt_indices:
                pid = pool[i]['player_id']
                # CPT-specific cap
                cpt_cap_key = f'cpt_{pid}'
                cap_val = _exp_caps.get(cpt_cap_key, _exp_caps.get(str(pid)))
                effective_cap = min(cpt_exp_max, cap_val) if cap_val is not None else cpt_exp_max
                max_app = int(n_lineups * effective_cap / 100.0)
                if effective_cap < 100 and cpt_appear[pid] >= max_app:
                    capped_cpt.add(i)
            for i in flex_indices:
                pid = pool[i]['player_id']
                is_pit = pool[i]['is_pitcher']
                global_cap = pitcher_exp_max if is_pit else hitter_exp_max
                flex_cap_key = f'flex_{pid}'
                cap_val = _exp_caps.get(flex_cap_key, _exp_caps.get(str(pid)))
                effective_cap = min(global_cap, cap_val) if cap_val is not None else global_cap
                max_app = int(n_lineups * effective_cap / 100.0)
                if effective_cap < 100 and flex_appear[pid] >= max_app:
                    capped_flex.add(i)
            if capped_cpt:
                iter_cpt = [i for i in cpt_indices if i not in capped_cpt]
            if capped_flex:
                iter_flex = [i for i in flex_indices if i not in capped_flex]

        if not iter_cpt or len(iter_flex) < 5:
            continue

        # Sample noisy scores
        scores = sample_noisy_scores(pool, rng, mode=mode,
                                      contest_type=contest_type,
                                      contest_discounts=contest_discounts)
        # For CPT scoring, boost by 1.5x (CPT gets 1.5x projection)
        cpt_scores = scores.copy()
        for i in iter_cpt:
            cpt_scores[i] *= 1.5

        result = build_lineup_sd(pool, cpt_scores, rng, salary_cap=salary_cap,
                                  cpt_pool=iter_cpt, flex_pool=iter_flex)
        if result is None:
            continue

        cpt_pid, flex_pids = result

        # Dedup
        key = (cpt_pid, tuple(sorted(flex_pids)))
        if key in seen:
            continue
        seen.add(key)

        # Compute metadata
        cpt_obj = next(p for p in pool if p['player_id'] == cpt_pid)
        flex_objs = [next(p for p in pool if p['player_id'] == fid) for fid in flex_pids]
        salary = int(cpt_obj['salary'] * 1.5) + sum(f['salary'] for f in flex_objs)
        proj = round(cpt_obj['proj'] * 1.5 + sum(f['proj'] for f in flex_objs), 2)

        lineups.append({
            'player_ids': [cpt_pid] + flex_pids,  # CPT first, then 5 FLEX
            'salary': salary,
            'proj': proj,
            'stack_team': cpt_obj['team'],
            'stack_size': 1,
            'sub_team': None,
            'sub_size': 0,
        })

        # Track appearances
        cpt_appear[cpt_pid] += 1
        for fid in flex_pids:
            flex_appear[fid] += 1

        if len(lineups) % 1000 == 0:
            print(f"    Generated {len(lineups):,} / {n_lineups:,} ({attempts:,} attempts)")

    print(f"    Final: {len(lineups):,} unique SD lineups from {attempts:,} attempts")
    return lineups


def process_sd_request(req):
    """Process a Showdown pool generation request."""
    req_id = req['id']
    print(f"\n{'='*55}")
    print(f"  Processing SD request #{req_id}")
    print(f"{'='*55}")

    sb.table('pool_requests').update({'status': 'processing'}).eq('id', req_id).execute()

    try:
        target_date = str(req['game_date'])
        slate = req['dk_slate']
        contest_type = req.get('contest_type', 'gpp')
        u_size = req.get('user_pool_size') or 5000
        c_size = req.get('contest_pool_size') or 8000
        salary_cap = req.get('salary_cap', SD_SALARY_CAP)

        # SD-specific settings
        excluded_cpt = set(req.get('excluded_cpt') or [])
        excluded_flex = set(req.get('excluded_flex') or [])
        exp_caps = req.get('exposure_caps') or {}
        cpt_exp = req.get('cpt_exp_max', 35)
        hitter_exp = req.get('hitter_exp_max', 100)
        pitcher_exp = req.get('pitcher_exp_max', 100)
        contest_discounts = req.get('contest_discount_teams') or {}
        proj_overrides = req.get('proj_overrides') or {}

        print(f"  Date: {target_date}  Slate: {slate}  Contest: {contest_type}")
        print(f"  User pool: {u_size:,}  Contest pool: {c_size:,}  CPT max: {cpt_exp}%")

        # Fetch data — SD slate is scoped to one game
        data = fetch_data(target_date, slate_filter=slate)
        if not data:
            raise ValueError("No data found for SD slate")

        raw_pool = build_player_pool(data)
        print(f"  SD player pool: {len(raw_pool)} players")

        if len(raw_pool) < 6:
            raise ValueError(f"Pool too small for SD ({len(raw_pool)} players)")

        # Build user pool with proj overrides applied
        user_pool = copy.deepcopy(raw_pool)
        # Apply projection overrides only (exclusions handled by SD builder)
        for p in user_pool:
            pid_str = str(p['player_id'])
            if pid_str in proj_overrides:
                new_proj = float(proj_overrides[pid_str])
                p['proj'] = new_proj
                p['ceiling'] = new_proj * 1.5
                p['floor'] = new_proj * 0.5

        rng = np.random.default_rng(seed=42)

        # Generate user SD pool
        print(f"\n  Generating SD USER pool ({u_size:,} target)...")
        user_lineups = generate_sd_lineups(
            user_pool, u_size, mode='user', rng=rng,
            excluded_cpt=excluded_cpt, excluded_flex=excluded_flex,
            exposure_caps=exp_caps, cpt_exp_max=cpt_exp,
            hitter_exp_max=hitter_exp, pitcher_exp_max=pitcher_exp,
            salary_cap=salary_cap,
        )

        # Generate contest SD pool (raw pool, no user overrides)
        print(f"\n  Generating SD CONTEST pool ({c_size:,} target)...")
        contest_lineups = generate_sd_lineups(
            raw_pool, c_size, mode='contest', rng=rng,
            contest_type=contest_type, contest_discounts=contest_discounts,
            salary_cap=salary_cap,
        )

        # Clear and upload
        print(f"\n  Clearing existing pools for {target_date}/{slate}...")
        sb.table('sim_pool').delete().eq('game_date', target_date).eq('dk_slate', slate).execute()

        computed_at = datetime.now(timezone.utc).isoformat()
        BATCH = 500

        for pool_type, lineups in [('user', user_lineups), ('contest', contest_lineups)]:
            records = [{
                'game_date': target_date,
                'dk_slate': slate,
                'pool_type': pool_type,
                'player_ids': lu['player_ids'],
                'salary': lu['salary'],
                'proj': lu['proj'],
                'stack_team': lu.get('stack_team', ''),
                'stack_size': lu.get('stack_size', 0),
                'sub_team': lu.get('sub_team'),
                'sub_size': lu.get('sub_size', 0),
                'computed_at': computed_at,
            } for lu in lineups]

            uploaded = 0
            for j in range(0, len(records), BATCH):
                batch = records[j:j+BATCH]
                sb.table('sim_pool').upsert(batch, on_conflict='pool_id').execute()
                uploaded += len(batch)
            print(f"  Uploaded {uploaded} {pool_type} SD lineups [{slate}]")

        # TODO: SD ownership upsert — re-enable once slate_ownership schema cache refreshes
        # Requires cpt_ownership and flex_ownership columns (migrate_sd_ownership.sql)

        # Mark complete
        sb.table('pool_requests').update({
            'status': 'complete',
            'user_pool_count': len(user_lineups),
            'contest_pool_count': len(contest_lineups),
            'completed_at': datetime.now(timezone.utc).isoformat(),
        }).eq('id', req_id).execute()

        # CPT exposure summary
        cpt_counts = defaultdict(int)
        for lu in user_lineups:
            cpt_counts[lu['player_ids'][0]] += 1
        print(f"\n  Top CPT exposures:")
        pid_name = {p['player_id']: p['name'] for p in raw_pool}
        for pid, cnt in sorted(cpt_counts.items(), key=lambda x: -x[1])[:8]:
            pct = cnt / len(user_lineups) * 100 if user_lineups else 0
            print(f"    {pid_name.get(pid, pid):25s}: {cnt:>5,} ({pct:.1f}%)")

        print(f"\n  SD Request #{req_id} complete: {len(user_lineups)} user + {len(contest_lineups)} contest")

    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"  ERROR: {e}")
        sb.table('pool_requests').update({
            'status': 'error',
            'error_message': str(e)[:500],
        }).eq('id', req_id).execute()


# ── User Settings (for --watch requests) ────────────────────────────────────

def apply_user_settings(pool, req):
    """Apply frontend user customizations to the player pool."""
    excluded = set(req.get('excluded_players') or [])
    locked = set(req.get('locked_players') or [])
    proj_overrides = req.get('proj_overrides') or {}
    exclude_teams = set(req.get('exclude_teams') or [])

    # Remove excluded players and teams
    pool = [p for p in pool if p['player_id'] not in excluded
            and p['team'] not in exclude_teams]

    # Apply projection overrides
    for p in pool:
        pid_str = str(p['player_id'])
        if pid_str in proj_overrides:
            new_proj = float(proj_overrides[pid_str])
            p['proj'] = new_proj
            p['ceiling'] = new_proj * 1.5
            p['floor'] = new_proj * 0.5

    # Mark locked players (used by build_lineup_greedy if we add lock support)
    for p in pool:
        p['locked'] = p['player_id'] in locked

    return pool


def process_request(req):
    """Process a single pool generation request from the frontend."""
    if req.get('is_showdown'):
        process_sd_request(req)
        return

    req_id = req['id']
    print(f"\n{'='*55}")
    print(f"  Processing request #{req_id}")
    print(f"{'='*55}")

    sb.table('pool_requests').update({'status': 'processing'}).eq('id', req_id).execute()

    try:
        target_date = str(req['game_date'])
        slate = req['dk_slate']
        contest_type = req.get('contest_type', 'gpp')
        u_size = req.get('user_pool_size') or 10000
        c_size = req.get('contest_pool_size') or 15000

        # Read user customizations
        exclude_teams = set(req.get('exclude_teams') or [])
        contest_discounts = req.get('contest_discount_teams') or {}
        exp_caps = req.get('exposure_caps') or {}
        hitter_exp = req.get('hitter_exp_max', 100)
        pitcher_exp = req.get('pitcher_exp_max', 100)

        salary_cap_override = req.get('salary_cap', SALARY_CAP)
        min_salary_override = req.get('min_salary', SALARY_FLOOR)

        print(f"  Date: {target_date}  Slate: {slate}  Contest: {contest_type}")
        print(f"  User pool: {u_size:,}  Contest pool: {c_size:,}")

        excluded_count = len(req.get('excluded_players') or [])
        locked_count = len(req.get('locked_players') or [])
        override_count = len(req.get('proj_overrides') or {})
        if excluded_count or locked_count or override_count:
            print(f"  Customizations: {excluded_count} excluded, {locked_count} locked, "
                  f"{override_count} proj overrides")

        # Fetch data
        data = fetch_data(target_date, slate_filter=slate)
        if not data:
            raise ValueError("No data found for date/slate")

        # Build raw pool (for contest — no user overrides)
        raw_pool = build_player_pool(data)
        print(f"  Raw player pool: {len(raw_pool)} players")

        if len(raw_pool) < 15:
            raise ValueError(f"Pool too small ({len(raw_pool)} players)")

        # Build user pool (with overrides applied)
        user_pool = copy.deepcopy(raw_pool)
        user_pool = apply_user_settings(user_pool, req)
        print(f"  User player pool (after settings): {len(user_pool)} players")

        game_count = len({p['game_pk'] for p in raw_pool if p.get('game_pk')})
        rng = np.random.default_rng(seed=42)

        # Generate user pool
        print(f"\n  Generating USER pool ({u_size:,} target)...")
        user_lineups = generate_lineups(
            user_pool, u_size, mode='user', rng=rng, game_count=game_count,
            exclude_teams=exclude_teams,
            exposure_caps=exp_caps, hitter_exp_max=hitter_exp, pitcher_exp_max=pitcher_exp,
        )

        # Generate contest pool (raw pool, ownership-weighted, no user overrides)
        print(f"\n  Generating CONTEST pool ({c_size:,} target)...")
        contest_lineups = generate_lineups(
            raw_pool, c_size, mode='contest', rng=rng, game_count=game_count,
            contest_type=contest_type, contest_discounts=contest_discounts,
        )

        # Clear existing pools for this date/slate and upload
        print(f"\n  Clearing existing pools for {target_date}/{slate}...")
        sb.table('sim_pool').delete().eq('game_date', target_date).eq('dk_slate', slate).execute()

        computed_at = datetime.now(timezone.utc).isoformat()
        BATCH = 500

        for pool_type, lineups in [('user', user_lineups), ('contest', contest_lineups)]:
            records = [{
                'game_date': target_date,
                'dk_slate': slate,
                'pool_type': pool_type,
                'player_ids': lu['player_ids'],
                'salary': lu['salary'],
                'proj': lu['proj'],
                'stack_team': lu['stack_team'],
                'stack_size': lu['stack_size'],
                'sub_team': lu.get('sub_team'),
                'sub_size': lu.get('sub_size', 0),
                'computed_at': computed_at,
            } for lu in lineups]

            uploaded = 0
            for j in range(0, len(records), BATCH):
                batch = records[j:j+BATCH]
                sb.table('sim_pool').upsert(batch, on_conflict='pool_id').execute()
                uploaded += len(batch)
            print(f"  Uploaded {uploaded} {pool_type} lineups [{slate}]")

        # Mark complete
        sb.table('pool_requests').update({
            'status': 'complete',
            'user_pool_count': len(user_lineups),
            'contest_pool_count': len(contest_lineups),
            'completed_at': datetime.now(timezone.utc).isoformat(),
        }).eq('id', req_id).execute()

        # Summary
        teams_user = defaultdict(int)
        for lu in user_lineups:
            teams_user[lu['stack_team']] += 1
        print(f"\n  User pool stack distribution:")
        for t, cnt in sorted(teams_user.items(), key=lambda x: -x[1]):
            pct = cnt / len(user_lineups) * 100 if user_lineups else 0
            print(f"    {t:5s}: {cnt:>5,} ({pct:.1f}%)")

        print(f"\n  Request #{req_id} complete: {len(user_lineups)} user + {len(contest_lineups)} contest")

    except Exception as e:
        print(f"  ERROR: {e}")
        sb.table('pool_requests').update({
            'status': 'error',
            'error_message': str(e)[:500],
        }).eq('id', req_id).execute()


def watch():
    """Poll Supabase for pending pool generation requests."""
    print("\n" + "=" * 55)
    print("  Pool Generator — Watch Mode")
    print("  Polling for frontend requests... (Ctrl+C to stop)")
    print("=" * 55)

    while True:
        try:
            rows = (sb.table('pool_requests')
                    .select('*')
                    .eq('status', 'pending')
                    .order('created_at')
                    .limit(1)
                    .execute().data)
            if rows:
                process_request(rows[0])
            else:
                time.sleep(3)
        except KeyboardInterrupt:
            print("\n  Watch mode stopped.")
            break
        except Exception as e:
            print(f"  Poll error: {e}")
            time.sleep(5)


# ── Main ─────────────────────────────────────────────────────────────────────

def run():
    args = sys.argv[1:]
    target_date = None
    slate_arg = None
    user_size = None
    contest_size = None

    i = 0
    while i < len(args):
        if args[i] == '--date' and i+1 < len(args):
            target_date = args[i+1]; i += 2
        elif args[i] == '--slate' and i+1 < len(args):
            slate_arg = args[i+1]; i += 2
        elif args[i] == '--user-size' and i+1 < len(args):
            user_size = int(args[i+1]); i += 2
        elif args[i] == '--contest-size' and i+1 < len(args):
            contest_size = int(args[i+1]); i += 2
        else:
            i += 1
    if not target_date:
        target_date = str(date.today())

    print(f"\nPool Generator — {target_date}")
    print("=" * 55)

    # Detect slates (paginate to avoid 1000-row default limit)
    sal_rows = []
    for i in range(0, 5000, 1000):
        rows = sb.table('dk_salaries').select('dk_slate').eq('season', SEASON).eq('contest_type', 'classic').range(i, i + 999).execute().data or []
        sal_rows.extend(rows)
        if len(rows) < 1000: break
    all_slates = sorted({r['dk_slate'] for r in sal_rows if r.get('dk_slate')})
    if slate_arg:
        all_slates = [slate_arg]
    print(f"  Slates: {all_slates}")

    # Count games per slate for auto-sizing
    for slate in all_slates:
        print(f"\n{'─'*55}")
        print(f"  Slate: {slate}")
        print(f"{'─'*55}")

        data = fetch_data(target_date, slate_filter=slate)
        if not data:
            print("  No data — skipping")
            continue

        pool = build_player_pool(data)
        n_pit = sum(1 for p in pool if p['is_pitcher'])
        n_hit = sum(1 for p in pool if not p['is_pitcher'])
        print(f"  Player pool: {len(pool)} ({n_pit} SP, {n_hit} hitters)")

        if len(pool) < 15:
            print("  Pool too small — skipping")
            continue

        # Count games actually in the pool (not all games on the date)
        game_count = len({p['game_pk'] for p in pool if p.get('game_pk')})
        u_size = user_size or min(game_count * 1500, 15000)
        c_size = contest_size or min(int(game_count * 1500 * 1.5), 25000)

        rng = np.random.default_rng(seed=42)

        # Generate user pool
        print(f"\n  Generating USER pool ({u_size:,} target)...")
        user_lineups = generate_lineups(pool, u_size, mode='user', rng=rng, game_count=game_count)

        # Generate contest pool
        print(f"\n  Generating CONTEST pool ({c_size:,} target)...")
        contest_lineups = generate_lineups(pool, c_size, mode='contest', rng=rng, game_count=game_count)

        # Clear existing pools for this date/slate
        print(f"\n  Clearing existing pools for {target_date}/{slate}...")
        sb.table('sim_pool').delete().eq('game_date', target_date).eq('dk_slate', slate).execute()

        # Upload
        computed_at = datetime.now(timezone.utc).isoformat()
        BATCH = 500

        for pool_type, lineups in [('user', user_lineups), ('contest', contest_lineups)]:
            records = [{
                'game_date': target_date,
                'dk_slate': slate,
                'pool_type': pool_type,
                'player_ids': lu['player_ids'],
                'salary': lu['salary'],
                'proj': lu['proj'],
                'stack_team': lu['stack_team'],
                'stack_size': lu['stack_size'],
                'sub_team': lu.get('sub_team'),
                'sub_size': lu.get('sub_size', 0),
                'computed_at': computed_at,
            } for lu in lineups]

            uploaded = 0
            for j in range(0, len(records), BATCH):
                batch = records[j:j+BATCH]
                sb.table('sim_pool').upsert(batch, on_conflict='pool_id').execute()
                uploaded += len(batch)
            print(f"  Uploaded {uploaded} {pool_type} lineups [{slate}]")

        # Summary
        teams_user = defaultdict(int)
        for lu in user_lineups:
            teams_user[lu['stack_team']] += 1
        print(f"\n  User pool stack distribution:")
        for t, cnt in sorted(teams_user.items(), key=lambda x: -x[1]):
            print(f"    {t:5s}: {cnt:>5,} ({cnt/len(user_lineups)*100:.1f}%)")

    print(f"\n{'='*55}")
    print(f"  Pool generation complete.")
    print(f"{'='*55}\n")


if __name__ == '__main__':
    if '--watch' in sys.argv:
        watch()
    else:
        run()
