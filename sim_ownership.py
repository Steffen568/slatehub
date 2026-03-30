#!/usr/bin/env python3
import sys
sys.stdout.reconfigure(encoding='utf-8', line_buffering=True)
"""
sim_ownership.py — Sim-Based DFS Ownership Projection Engine

Estimates ownership by simulating what the DFS field would roster.
Runs thousands of optimized lineups with "public-facing" projections
(boosted for salary efficiency, name value, confirmed lineups) and
counts appearance rates. Calibrates the raw rates against actual
DK contest ownership data.

Key insight: ownership ≈ "how often does a player appear in optimized
lineups across many projection variants?" This naturally captures the
correlation between projection quality, salary, game environment, and
position scarcity that drives real ownership.

Run:
  py -3.12 sim_ownership.py
  py -3.12 sim_ownership.py --date 2026-03-28
  py -3.12 sim_ownership.py --sims 2000
"""

import os, math, random
import numpy as np
from datetime import date, datetime, timezone
from collections import defaultdict
from supabase import create_client
from dotenv import load_dotenv
from config import SEASON

load_dotenv()
sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])

NUM_SIMS = 3000  # default simulations

# Position slot counts (DK Classic)
POS_SLOTS = {'SP': 2, 'C': 1, '1B': 1, '2B': 1, '3B': 1, 'SS': 1, 'OF': 3}

# Canonical position priority for multi-pos players
POS_PRIORITY = ['C', 'SS', '2B', '3B', '1B', 'OF', 'SP']

# Public bias adjustments — the field overweights these factors
SALARY_CURVE_EXP = 1.1    # public loves value but not as extremely as pure optimizers
PITCHER_BOOST = 2.5        # pitchers get concentrated ownership (field gravitates to top 3-4 SPs)
CONFIRMED_BOOST = 1.5      # confirmed lineup players get rostered more
UNCONFIRMED_PENALTY = 0.4  # unconfirmed players get heavily discounted

# ── Helpers ───────────────────────────────────────────────────────────────────

def safe(val, default=None):
    if val is None:
        return default
    try:
        f = float(val)
        return None if math.isnan(f) or math.isinf(f) else f
    except (TypeError, ValueError):
        return default


def clip(val, lo, hi):
    return max(lo, min(hi, val)) if val is not None else lo


# ── Data Fetching ─────────────────────────────────────────────────────────────

def fetch_data(target_date):
    """Fetch projections, salaries, lineups, and odds for ownership sim."""
    print(f"  Fetching data for {target_date}...")

    # Games
    games = sb.table('games').select(
        'game_pk,game_date,home_team,away_team,home_team_id,away_team_id,venue_id'
    ).eq('game_date', target_date).execute().data or []
    print(f"  Games: {len(games)}")
    if not games:
        return None

    game_pks = [g['game_pk'] for g in games]

    # Projections (from sim_projections or compute_projections — whatever ran last)
    projs = []
    for i in range(0, 5000, 1000):
        rows = sb.table('player_projections').select(
            'player_id,game_pk,full_name,team,proj_dk_pts,proj_floor,proj_ceiling,'
            'is_pitcher,batting_order'
        ).eq('game_date', target_date).range(i, i + 999).execute().data or []
        projs.extend(rows)
        if len(rows) < 1000:
            break
    print(f"  Projections: {len(projs)}")

    # DK Salaries
    sals = []
    for i in range(0, 5000, 1000):
        rows = sb.table('dk_salaries').select(
            'player_id,name,position,salary,team'
        ).eq('season', SEASON).eq('contest_type', 'classic').range(i, i + 999).execute().data or []
        sals.extend(rows)
        if len(rows) < 1000:
            break
    sal_map = {}
    for s in sals:
        if s.get('player_id') and s.get('salary'):
            sal_map[s['player_id']] = s
    print(f"  Salaries: {len(sal_map)}")

    # Lineups (for confirmed status)
    lineups = sb.table('lineups').select(
        'player_id,game_pk,batting_order,status'
    ).in_('game_pk', game_pks).execute().data or []
    lineup_map = {lu['player_id']: lu for lu in lineups}
    print(f"  Lineups: {len(lineups)}")

    # Odds (for game total / implied runs)
    odds = {}
    if game_pks:
        rows = sb.table('game_odds').select(
            'game_pk,game_total,home_implied,away_implied'
        ).in_('game_pk', game_pks).execute().data or []
        odds = {r['game_pk']: r for r in rows}

    # Pitcher stats (for K% excitement factor)
    sp_ids = list({p['player_id'] for p in projs if p.get('is_pitcher')})
    pitcher_k = {}
    if sp_ids:
        for i in range(0, len(sp_ids), 150):
            chunk = sp_ids[i:i+150]
            rows = sb.table('pitcher_stats').select('player_id,k_pct,stuff_plus').in_(
                'player_id', chunk).eq('season', SEASON).execute().data or []
            for r in rows:
                pitcher_k[r['player_id']] = {
                    'k_pct': safe(r.get('k_pct'), 0.22),
                    'stuff_plus': safe(r.get('stuff_plus'), 100)
                }
        # Fallback to prior season
        missing = [pid for pid in sp_ids if pid not in pitcher_k]
        if missing:
            for i in range(0, len(missing), 150):
                chunk = missing[i:i+150]
                rows = sb.table('pitcher_stats').select('player_id,k_pct,stuff_plus').in_(
                    'player_id', chunk).eq('season', SEASON-1).execute().data or []
                for r in rows:
                    if r['player_id'] not in pitcher_k:
                        pitcher_k[r['player_id']] = {
                            'k_pct': safe(r.get('k_pct'), 0.22),
                            'stuff_plus': safe(r.get('stuff_plus'), 100)
                        }

    return {
        'games': games, 'projs': projs, 'sal_map': sal_map,
        'lineup_map': lineup_map, 'odds': odds, 'pitcher_k': pitcher_k,
    }


# ── Build Player Pool ────────────────────────────────────────────────────────

def build_pool(data):
    """Build the player pool with public-biased scores for ownership sim."""
    pool = []

    for p in data['projs']:
        pid = p['player_id']
        sal_row = data['sal_map'].get(pid)
        if not sal_row or not sal_row.get('salary'):
            continue

        proj = safe(p.get('proj_dk_pts'), 0)
        if proj <= 0:
            continue

        salary = sal_row['salary']
        position = sal_row.get('position', '').split('/')[0]
        is_pitcher = p.get('is_pitcher', False)

        # Assign canonical position
        if is_pitcher:
            pos = 'SP'
        else:
            dk_positions = sal_row.get('position', '').split('/')
            pos = 'OF'  # default
            for pp in POS_PRIORITY:
                if pp in dk_positions:
                    pos = pp
                    break

        # Lineup status
        lu = data['lineup_map'].get(pid)
        confirmed = lu and lu.get('batting_order') and lu['batting_order'] >= 1
        batting_order = lu.get('batting_order') if lu else None

        # Game environment
        odds_row = data['odds'].get(p.get('game_pk'))
        game_total = safe(odds_row.get('game_total'), 8.5) if odds_row else 8.5

        # ── Public-biased score ───────────────────────────────────────────
        # Value score: proj per $1000 salary (public loves value)
        value = (proj / salary * 1000) if salary > 0 else 0
        value_score = value ** SALARY_CURVE_EXP  # amplify value advantage

        # Raw projection (star power — public recognizes big names via high proj)
        proj_score = proj ** 1.3  # slight amplification

        # Game environment boost (public loves high-total games)
        env_score = (game_total / 8.5) ** 1.5

        # Batting order premium (public heavily weights top of order)
        if not is_pitcher and batting_order:
            bo_score = {1: 1.4, 2: 1.35, 3: 1.3, 4: 1.25, 5: 1.1,
                        6: 0.95, 7: 0.85, 8: 0.75, 9: 0.65}.get(batting_order, 0.8)
        else:
            bo_score = 1.0

        # Combine
        base_score = (value_score * 0.35 + proj_score * 0.40 + env_score * 0.15 + bo_score * 0.10)

        # Pitcher concentration boost (public heavily rosters top SPs)
        # Three factors: salary (expensive = popular), K rate (exciting), Stuff+ (hype)
        if is_pitcher:
            salary_tier = clip((salary - 5000) / 3000, 0.2, 2.0) ** 1.5
            pk = data.get('pitcher_k', {}).get(pid, {})
            k_excitement = clip((pk.get('k_pct', 0.22) - 0.18) / 0.10, 0.5, 2.0)  # 28% K = 1.5x, 18% = 0.5x
            stuff_hype = clip((pk.get('stuff_plus', 100) - 90) / 20, 0.5, 1.5)  # 110 Stuff+ = 1.5x
            base_score *= PITCHER_BOOST * (0.3 + salary_tier) * k_excitement * stuff_hype

        # Confirmed/unconfirmed modifier
        if confirmed:
            base_score *= CONFIRMED_BOOST
        elif not is_pitcher and not confirmed:
            base_score *= UNCONFIRMED_PENALTY

        pool.append({
            'player_id': pid,
            'name': p.get('full_name') or sal_row.get('name', '?'),
            'team': p.get('team') or sal_row.get('team', ''),
            'pos': pos,
            'salary': salary,
            'proj': proj,
            'floor': safe(p.get('proj_floor'), proj * 0.5),
            'ceiling': safe(p.get('proj_ceiling'), proj * 1.5),
            'is_pitcher': is_pitcher,
            'game_pk': p.get('game_pk'),
            'batting_order': batting_order,
            'base_score': base_score,
        })

    return pool


# ── Greedy Lineup Builder ────────────────────────────────────────────────────

def build_lineup_greedy(pool, scores, salary_cap=50000):
    """
    Build one valid DK Classic lineup using greedy selection by score,
    with salary feasibility awareness.

    Two-pass approach:
    1. Fill each position with the highest-scoring player that fits the budget
    2. Budget check: reserve enough salary for remaining cheapest players per position

    Returns set of player_ids or None if infeasible.
    """
    # Group players by position with their scores
    pos_players = defaultdict(list)
    for i, p in enumerate(pool):
        pos_players[p['pos']].append((i, scores[i], p))

    # Sort each position group by score descending
    for pos in pos_players:
        pos_players[pos].sort(key=lambda x: x[1], reverse=True)

    # Pre-compute cheapest player per position (for salary reservation)
    cheapest = {}
    for pos, players in pos_players.items():
        if players:
            cheapest[pos] = min(p[2]['salary'] for p in players)
        else:
            cheapest[pos] = 3000  # fallback

    # Randomize position fill order — prevents always picking SPs first
    # which would lock in the same 2 pitchers every time
    pos_list = list(POS_SLOTS.keys())
    random.shuffle(pos_list)
    pos_order = pos_list

    remaining = dict(POS_SLOTS)
    sal_left = salary_cap
    selected = []
    used_pids = set()

    for pos in pos_order:
        slots_needed = remaining[pos]
        for _ in range(slots_needed):
            # Calculate salary we need to reserve for unfilled positions
            reserve = 0
            for rpos, rslots in remaining.items():
                if rpos == pos:
                    reserve += cheapest.get(rpos, 3000) * max(0, rslots - 1)
                else:
                    reserve += cheapest.get(rpos, 3000) * rslots

            budget = sal_left - reserve

            picked = False
            for idx, score, p in pos_players[pos]:
                if p['player_id'] in used_pids:
                    continue
                if p['salary'] <= budget:
                    selected.append(p['player_id'])
                    used_pids.add(p['player_id'])
                    sal_left -= p['salary']
                    remaining[pos] -= 1
                    picked = True
                    break

            if not picked:
                # Can't fill this slot — try cheapest available
                for idx, score, p in reversed(pos_players[pos]):
                    if p['player_id'] not in used_pids and p['salary'] <= sal_left:
                        selected.append(p['player_id'])
                        used_pids.add(p['player_id'])
                        sal_left -= p['salary']
                        remaining[pos] -= 1
                        picked = True
                        break
                if not picked:
                    return None  # truly infeasible

    return set(selected) if len(selected) == 10 else None


# ── Ownership Simulation ─────────────────────────────────────────────────────

def simulate_ownership(pool, n_sims, rng):
    """
    Run n_sims lineup builds with randomized public-biased scores.
    Count appearance rates per player = raw ownership signal.
    """
    appear = defaultdict(int)
    feasible = 0

    for sim in range(n_sims):
        # Perturb scores: each player gets noise based on their projection SD
        scores = np.zeros(len(pool))
        for i, p in enumerate(pool):
            sd = (p['ceiling'] - p['floor']) / 3.3 if p['ceiling'] > p['floor'] else p['proj'] * 0.15
            sd = max(sd, 0.5)
            noise = rng.normal(0, sd)
            # Public-biased score + noise. Pitchers get more noise to spread ownership
            # across multiple SPs (public field is split, not unanimous)
            noise_scale = 0.8 if p['is_pitcher'] else 0.5
            scores[i] = p['base_score'] + noise * noise_scale

        lineup = build_lineup_greedy(pool, scores)
        if lineup:
            feasible += 1
            for pid in lineup:
                appear[pid] += 1

    return appear, feasible


# ── Calibration ──────────────────────────────────────────────────────────────

def calibrate_ownership(pool, appear, feasible, target_date):
    """
    Convert raw appearance rates to calibrated ownership percentages.

    Calibration approach:
    1. Compute raw rate per player: appear_count / feasible_sims
    2. Normalize within position groups so total = slots × 100%
    3. Apply pitcher concentration (top SPs get 30-50% in real contests)
    4. If actual ownership data exists, fit a scaling curve
    """
    if feasible == 0:
        return {}

    # Raw rates
    raw_rates = {}
    for p in pool:
        pid = p['player_id']
        raw_rates[pid] = appear.get(pid, 0) / feasible * 100  # as percentage

    # Group by position
    pos_groups = defaultdict(list)
    for p in pool:
        pos_groups[p['pos']].append(p)

    # Normalize each position group so total = slots × 100%
    calibrated = {}
    for pos, players in pos_groups.items():
        slots = POS_SLOTS.get(pos, 1)
        target_sum = slots * 100.0

        raw_sum = sum(raw_rates.get(p['player_id'], 0) for p in players)
        if raw_sum <= 0:
            for p in players:
                calibrated[p['player_id']] = 0.0
            continue

        scale = target_sum / raw_sum
        for p in players:
            calibrated[p['player_id']] = raw_rates.get(p['player_id'], 0) * scale

    # ── Actual ownership calibration (if available) ──────────────────────
    actual_rows = sb.table('actual_ownership').select(
        'player_id,ownership_pct'
    ).eq('game_date', target_date).execute().data or []

    if len(actual_rows) >= 20:
        actual_map = {r['player_id']: r['ownership_pct'] for r in actual_rows}
        # Fit a simple linear correction: actual = a * predicted + b
        matched_pred = []
        matched_act = []
        for pid, pred in calibrated.items():
            if pid in actual_map:
                matched_pred.append(pred)
                matched_act.append(actual_map[pid])

        if len(matched_pred) >= 10:
            pred_arr = np.array(matched_pred)
            act_arr = np.array(matched_act)
            # Linear regression
            if np.std(pred_arr) > 0:
                slope = np.sum((pred_arr - pred_arr.mean()) * (act_arr - act_arr.mean())) / np.sum((pred_arr - pred_arr.mean())**2)
                intercept = act_arr.mean() - slope * pred_arr.mean()
                print(f"  Calibration fit: actual = {slope:.2f} × predicted + {intercept:.2f}")
                print(f"  Calibration based on {len(matched_pred)} matched players from {target_date}")
                # Apply correction
                for pid in calibrated:
                    calibrated[pid] = max(0.1, calibrated[pid] * slope + intercept)
            else:
                print("  Calibration: insufficient variance in predictions")
        else:
            print(f"  Calibration: only {len(matched_pred)} matched players — using uncalibrated")
    else:
        print(f"  No actual ownership data for {target_date} — using uncalibrated rates")

    # Final clamp
    for pid in calibrated:
        calibrated[pid] = clip(calibrated[pid], 0.1, 75.0)

    return calibrated


# ── Main ──────────────────────────────────────────────────────────────────────

def run():
    target_date = None
    n_sims = NUM_SIMS
    args = sys.argv[1:]
    i = 0
    while i < len(args):
        if args[i] == '--date' and i+1 < len(args):
            target_date = args[i+1]; i += 2
        elif args[i] == '--sims' and i+1 < len(args):
            n_sims = int(args[i+1]); i += 2
        else:
            target_date = target_date or args[i]; i += 1
    if not target_date:
        target_date = str(date.today())

    print(f"\nSim Ownership Engine — {target_date} ({n_sims:,} sims)")
    print("=" * 55)

    data = fetch_data(target_date)
    if not data:
        print("  No games found — exiting")
        return

    pool = build_pool(data)
    print(f"  Player pool: {len(pool)} ({sum(1 for p in pool if p['is_pitcher'])} pitchers, {sum(1 for p in pool if not p['is_pitcher'])} hitters)")

    if len(pool) < 10:
        print("  Pool too small — exiting")
        return

    rng = np.random.default_rng(seed=123)
    print(f"\n  Running {n_sims:,} ownership simulations...")
    appear, feasible = simulate_ownership(pool, n_sims, rng)
    print(f"  Feasible lineups: {feasible}/{n_sims} ({100*feasible/n_sims:.0f}%)")

    calibrated = calibrate_ownership(pool, appear, feasible, target_date)

    # Upsert to player_projections
    computed_at = datetime.now(timezone.utc).isoformat()
    records = []
    for p in pool:
        own = calibrated.get(p['player_id'], 0)
        records.append({
            'player_id': p['player_id'],
            'game_pk': p['game_pk'],
            'game_date': target_date,
            'proj_ownership': round(own, 1),
            'computed_at': computed_at,
        })

    # Deduplicate
    seen = {}
    for r in records:
        seen[(r['player_id'], r['game_pk'])] = r
    records = list(seen.values())

    BATCH = 500
    uploaded = 0
    for i in range(0, len(records), BATCH):
        batch = records[i:i+BATCH]
        sb.table('player_projections').upsert(
            batch, on_conflict='player_id,game_pk', ignore_duplicates=False
        ).execute()
        uploaded += len(batch)
    print(f"  Uploaded {uploaded} ownership projections")

    # ── Validation against actuals ────────────────────────────────────────
    actual_rows = sb.table('actual_ownership').select(
        'player_id,dk_name,ownership_pct'
    ).eq('game_date', target_date).execute().data or []

    if actual_rows:
        actual_map = {r['player_id']: r for r in actual_rows}
        matched = []
        for p in pool:
            a = actual_map.get(p['player_id'])
            if a:
                pred = calibrated.get(p['player_id'], 0)
                matched.append({
                    'name': p['name'], 'pred': pred,
                    'actual': a['ownership_pct'], 'pos': p['pos']
                })

        if matched:
            preds = np.array([m['pred'] for m in matched])
            acts = np.array([m['actual'] for m in matched])
            errors = preds - acts
            corr = np.corrcoef(preds, acts)[0, 1] if len(matched) > 2 else 0

            print(f"\n  Validation ({len(matched)} matched):")
            print(f"    Correlation:  {corr:.3f}")
            print(f"    MAE:          {np.mean(np.abs(errors)):.1f}%")
            print(f"    Mean error:   {np.mean(errors):+.1f}%")

    # ── Sample output ─────────────────────────────────────────────────────
    pool_sorted = sorted(pool, key=lambda p: calibrated.get(p['player_id'], 0), reverse=True)
    pitchers = [p for p in pool_sorted if p['is_pitcher']][:10]
    hitters = [p for p in pool_sorted if not p['is_pitcher']][:15]

    print(f"\n  Top pitcher ownership:")
    for p in pitchers:
        own = calibrated.get(p['player_id'], 0)
        actual_row = actual_map.get(p['player_id']) if actual_rows else None
        act = f"  actual={actual_row['ownership_pct']:.1f}%" if actual_row else ""
        print(f"    {p['name']:25s}  {own:5.1f}%  (${p['salary']:,}  proj={p['proj']:.1f}){act}")

    print(f"\n  Top hitter ownership:")
    for p in hitters:
        own = calibrated.get(p['player_id'], 0)
        actual_row = actual_map.get(p['player_id']) if actual_rows else None
        act = f"  actual={actual_row['ownership_pct']:.1f}%" if actual_row else ""
        print(f"    {p['name']:25s}  {own:5.1f}%  (${p['salary']:,}  proj={p['proj']:.1f}  {p['pos']}){act}")

    print(f"\nOwnership projection complete.")


if __name__ == '__main__':
    run()
