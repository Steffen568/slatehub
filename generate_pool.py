#!/usr/bin/env python3
import sys
sys.stdout.reconfigure(encoding='utf-8', line_buffering=True)
"""
generate_pool.py — Pre-compute lineup pools for contest simulation

Generates TWO pools per slate:
  1. User pool: optimized lineups using real projections + greedy randomized builder
  2. Contest pool: ownership-weighted lineups representing the DK field

Uses a greedy randomized builder (not LP solver) for massive diversity.
Round-robin team selection ensures every viable team gets coverage.

Run:
  py -3.12 generate_pool.py
  py -3.12 generate_pool.py --slate main
  py -3.12 generate_pool.py --user-size 15000 --contest-size 20000
"""

import os, math, random, json
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

def safe(val, default=None):
    if val is None: return default
    try:
        f = float(val)
        return None if math.isnan(f) or math.isinf(f) else f
    except: return default

def clip(val, lo, hi):
    return max(lo, min(hi, val)) if val is not None else lo


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
    sal_q = sb.table('dk_salaries').select(
        'player_id,name,position,salary,team,dk_slate'
    ).eq('season', SEASON).eq('contest_type', 'classic')
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

    return {
        'games': games, 'projs': projs, 'sal_map': sal_map,
        'sal_name_map': sal_name_map, 'lineup_map': lineup_map,
        'odds': odds, 'ownership': ownership,
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

        team = sal_row.get('team', '') or p.get('team', '')
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

    return deduped


# ── Greedy Lineup Builder ────────────────────────────────────────────────────

STACK_CONFIGS = [
    {'name': '5-3',       'main': 5, 'subs': [3]},    # best correlated config
    {'name': '5-3',       'main': 5, 'subs': [3]},    # double-weighted (top performer)
    {'name': '5-2',       'main': 5, 'subs': [2]},
    {'name': '5-naked',   'main': 5, 'subs': []},
    {'name': '4-3',       'main': 4, 'subs': [3]},
    {'name': '4-3',       'main': 4, 'subs': [3]},    # double-weighted (2nd best)
    {'name': '4-2-2',     'main': 4, 'subs': [2, 2]},
]

def build_lineup_greedy(pool, scores, main_team=None, main_size=4,
                         sub_teams=None, sub_sizes=None, rng=None):
    """
    Build one DK Classic lineup using greedy randomized selection.
    Supports multiple sub-stacks and bring-backs.
    """
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

    # Force main stack players first
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

    pos_order = list(remaining.keys())
    rng.shuffle(pos_order)

    for pos in pos_order:
        slots_needed = remaining[pos]
        while slots_needed > 0:
            # Reserve salary for remaining positions
            reserve = sum(cheapest.get(rp, 3000) * (remaining[rp] if rp != pos else max(0, slots_needed - 1))
                          for rp in remaining if remaining[rp] > 0)
            budget = sal_left - reserve

            candidates = [(i, s, p) for i, s, p in pos_players.get(pos, [])
                          if p['player_id'] not in used_pids and p['salary'] <= budget
                          and (p['is_pitcher'] or team_hitter_count[p['team']] < MAX_HITTERS_PER_TEAM)]

            if not candidates:
                # Fallback: cheapest available (still respect team cap)
                fallback = [(i, s, p) for i, s, p in pos_players.get(pos, [])
                            if p['player_id'] not in used_pids and p['salary'] <= sal_left
                            and (p['is_pitcher'] or team_hitter_count[p['team']] < MAX_HITTERS_PER_TEAM)]
                if not fallback:
                    # Last resort: ignore team cap to avoid null lineup
                    fallback = [(i, s, p) for i, s, p in pos_players.get(pos, [])
                                if p['player_id'] not in used_pids and p['salary'] <= sal_left]
                    if not fallback:
                        return None
                pick = fallback[-1]  # cheapest
            else:
                # Weighted random from top 3
                top = candidates[:3]
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
            remaining[pos] -= 1
            slots_needed -= 1

    if len(selected) != 10:
        return None
    total_sal = SALARY_CAP - sal_left
    if total_sal < SALARY_FLOOR:
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

def sample_noisy_scores(pool, rng, mode='user'):
    """
    Sample noisy projection scores for one sim.
    mode='user': real projections with game/team/individual correlation
    mode='contest': ownership-weighted public bias scoring
    """
    scores = np.zeros(len(pool))

    if mode == 'contest':
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
            env_score = (p['game_total'] / 8.5) ** 1.5
            base = proj_score * 0.40 + sal_score * 0.30 + env_score * 0.10 + bo_score * 0.15
            if p['is_pitcher']:
                base *= 5.0
            if not p['is_pitcher'] and not p['confirmed']:
                base *= 0.4
            # Multiplicative noise
            noise_sd = 0.50 if p['is_pitcher'] else 0.30
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
            mean = p['proj']
            sd = (p['ceiling'] - p['floor']) / 3.3 if p['ceiling'] > p['floor'] else max(mean * 0.20, 0.5)
            sd = max(sd, 0.5)

            zg = game_z.get(p.get('game_pk'), 0)
            zt = team_z.get(p.get('team'), 0)
            zi = rng.normal(0, 1)

            if p['is_pitcher']:
                noise = P_W_GAME * zg + P_W_TEAM * zt + P_W_INDIV * zi
            else:
                noise = H_W_GAME * zg + H_W_TEAM * zt + H_W_INDIV * zi

            scores[i] = max(0, mean + sd * noise)

    return scores


# ── Pool Generation ──────────────────────────────────────────────────────────

def generate_lineups(pool, n_lineups, mode='user', rng=None):
    """Generate n_lineups unique lineups using greedy randomized builder."""
    if rng is None: rng = np.random.default_rng()

    # Get viable main teams (4+ hitters)
    team_hitters = defaultdict(list)
    for p in pool:
        if not p['is_pitcher']:
            team_hitters[p['team']].append(p)
    viable_teams = [t for t, hs in team_hitters.items() if len(hs) >= 4]
    print(f"    Viable main teams: {len(viable_teams)} — {viable_teams}")

    if not viable_teams:
        print("    No viable teams — skipping")
        return []

    # Teams that can support 5-man stacks
    viable_5 = [t for t, hs in team_hitters.items() if len(hs) >= 5]
    # Teams that can support 3-man sub stacks
    viable_3sub = [t for t, hs in team_hitters.items() if len(hs) >= 3]

    lineups = []
    seen = set()
    attempts = 0
    max_attempts = n_lineups * 4
    config_idx = 0

    while len(lineups) < n_lineups and attempts < max_attempts:
        # Round-robin main team + stack config
        main_team = viable_teams[attempts % len(viable_teams)]
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
                st = rng.choice(cands)
                sub_teams.append(st)
                used_sub.add(st)
            else:
                sub_teams.append(None)

        # Sample noisy scores
        scores = sample_noisy_scores(pool, rng, mode=mode)

        # Build lineup
        lu = build_lineup_greedy(pool, scores, main_team=main_team, main_size=main_size,
                                  sub_teams=sub_teams, sub_sizes=sub_sizes, rng=rng)
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

        if len(lineups) % 1000 == 0:
            print(f"    Generated {len(lineups):,} / {n_lineups:,} ({attempts:,} attempts)")

    print(f"    Final: {len(lineups):,} unique lineups from {attempts:,} attempts")
    return lineups


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

    # Detect slates
    sal_rows = sb.table('dk_salaries').select('dk_slate').eq('season', SEASON).eq('contest_type', 'classic').limit(5000).execute().data or []
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

        game_count = len(data['games'])
        u_size = user_size or min(game_count * 1500, 15000)
        c_size = contest_size or min(int(game_count * 1500 * 1.5), 25000)

        rng = np.random.default_rng(seed=42)

        # Generate user pool
        print(f"\n  Generating USER pool ({u_size:,} target)...")
        user_lineups = generate_lineups(pool, u_size, mode='user', rng=rng)

        # Generate contest pool
        print(f"\n  Generating CONTEST pool ({c_size:,} target)...")
        contest_lineups = generate_lineups(pool, c_size, mode='contest', rng=rng)

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
    run()
