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

# Public bias adjustments — calibrated from actual DK ownership data (2026-03-28, 2026-03-30)
# Key finding: projection (r=0.557) and salary (r=0.536) drive ownership.
# Value (pts/$1k) has near-zero correlation (r=-0.055) — public rosters stars, not value.
SALARY_CURVE_EXP = 0.6     # value matters much less than raw projection/salary
PITCHER_BOOST = 5.0         # top SPs get 30-47% actual ownership — need heavy concentration
CONFIRMED_BOOST = 1.5       # confirmed lineup players get rostered more
UNCONFIRMED_PENALTY = 0.4   # unconfirmed players get heavily discounted

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

    # DK Salaries — fetch ALL classic salaries with dk_slate info
    sals = []
    for i in range(0, 5000, 1000):
        rows = sb.table('dk_salaries').select(
            'player_id,name,position,salary,team,dk_slate'
        ).eq('season', SEASON).eq('contest_type', 'classic').range(i, i + 999).execute().data or []
        sals.extend(rows)
        if len(rows) < 1000:
            break

    # Detect all distinct slates
    all_slates = sorted({s['dk_slate'] for s in sals if s.get('dk_slate')})

    # Build per-slate salary maps: {slate: {player_id: sal_row}}
    # Also build per-slate name maps for fallback matching
    slate_sal_maps = {}
    slate_sal_name_maps = {}
    for slate in all_slates:
        sal_map = {}
        sal_name_map = {}
        for s in sals:
            if s.get('dk_slate') != slate:
                continue
            if s.get('player_id') and s.get('salary'):
                sal_map[s['player_id']] = s
            if s.get('name') and s.get('salary'):
                norm = s['name'].lower().replace('.', '').replace("'", '').strip()
                for suffix in [' jr', ' sr', ' ii', ' iii', ' iv']:
                    if norm.endswith(suffix):
                        norm = norm[:-len(suffix)].strip()
                sal_name_map[norm] = s
        slate_sal_maps[slate] = sal_map
        slate_sal_name_maps[slate] = sal_name_map
        print(f"  Salaries [{slate}]: {len(sal_map)} by ID, {len(sal_name_map)} by name")
    print(f"  Slates detected: {all_slates}")

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
        'games': games, 'projs': projs,
        'all_slates': all_slates,
        'slate_sal_maps': slate_sal_maps,
        'slate_sal_name_maps': slate_sal_name_maps,
        'lineup_map': lineup_map,
        'odds': odds, 'pitcher_k': pitcher_k,
    }


# ── Build Player Pool ────────────────────────────────────────────────────────

def build_pool(data, slate=None):
    """Build the player pool with public-biased scores for ownership sim.
    If slate is given, only include players on that slate."""
    # Select the right salary maps
    if slate:
        sal_map = data['slate_sal_maps'].get(slate, {})
        sal_name_map = data['slate_sal_name_maps'].get(slate, {})
    else:
        # Fallback: merge all slates (prefer 'main' if exists)
        sal_map = {}
        sal_name_map = {}
        for s in data['all_slates']:
            for pid, row in data['slate_sal_maps'].get(s, {}).items():
                if pid not in sal_map or s == 'main':
                    sal_map[pid] = row
            for nm, row in data['slate_sal_name_maps'].get(s, {}).items():
                if nm not in sal_name_map or s == 'main':
                    sal_name_map[nm] = row

    pool = []

    name_fallback_count = 0
    for p in data['projs']:
        pid = p['player_id']
        sal_row = sal_map.get(pid)
        # Fallback: match by normalized name if ID doesn't match
        if (not sal_row or not sal_row.get('salary')) and p.get('full_name'):
            norm = p['full_name'].lower().replace('.', '').replace("'", '').strip()
            for suffix in [' jr', ' sr', ' ii', ' iii', ' iv']:
                if norm.endswith(suffix):
                    norm = norm[:-len(suffix)].strip()
            sal_row = sal_name_map.get(norm)
            if sal_row:
                name_fallback_count += 1
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
        # Weights calibrated from actual DK ownership data:
        #   Projection r=0.557, Salary r=0.536, Value r=-0.055, BatOrder r=0.203

        # Raw projection magnitude — strongest driver (public rosters high-proj stars)
        proj_score = proj ** 1.5

        # Salary / star power — public gravitates to expensive, recognizable names
        salary_score = (salary / 3500) ** 1.3  # $3500 baseline → 1.0

        # Value score — near-zero actual correlation, minimal weight
        value = (proj / salary * 1000) if salary > 0 else 0
        value_score = value ** SALARY_CURVE_EXP

        # Game environment boost (public loves high-total games)
        env_score = (game_total / 8.5) ** 1.5

        # Batting order premium (r=0.203 — matters but less than proj/salary)
        # Softened: bo=1 was 1.5x causing 40%+ concentration on leadoff hitters.
        # Real data shows top-of-order premium is moderate, not extreme.
        if not is_pitcher and batting_order:
            bo_score = {1: 1.30, 2: 1.25, 3: 1.20, 4: 1.15, 5: 1.05,
                        6: 0.90, 7: 0.80, 8: 0.70, 9: 0.60}.get(batting_order, 0.75)
        else:
            bo_score = 1.0

        # Combine — projection + salary dominate (match actual correlations)
        base_score = (proj_score * 0.40 + salary_score * 0.30 + value_score * 0.05 +
                      env_score * 0.10 + bo_score * 0.15)

        # Pitcher concentration boost (public heavily rosters top SPs)
        # The base_score already captures projection quality (which includes K%,
        # Stuff+, matchup, etc.). Don't re-multiply by those signals — it
        # double-counts and creates extreme concentration on one SP.
        # Instead: flat boost to separate pitchers from hitters in the greedy
        # builder, then let projection differences + noise create the spread.
        if is_pitcher:
            base_score *= PITCHER_BOOST

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

    if name_fallback_count:
        print(f"  Name-matched {name_fallback_count} players with ID mismatches")

    # Deduplicate by name: same player can have multiple IDs (MLBAM vs DK).
    # Keep the entry with the highest base_score (best combination of proj + batting order + salary).
    # But first inherit batting_order across duplicates so the best-projected
    # row isn't penalized for missing batting_order.
    from collections import defaultdict as _dd
    name_groups = _dd(list)
    for p in pool:
        name_groups[p['name'].lower().strip()].append(p)

    deduped = []
    merges = 0
    for key, group in name_groups.items():
        if len(group) == 1:
            deduped.append(group[0])
            continue
        # Merge batting_order: use the first non-None
        bo = next((p['batting_order'] for p in group if p['batting_order']), None)
        # Find which row has lineup data (for confirmed status lookup)
        lineup_pid = next((p['player_id'] for p in group if data['lineup_map'].get(p['player_id'])), None)
        for p in group:
            p['batting_order'] = bo
        # Pick the row with the highest projection (it has the best data)
        best = max(group, key=lambda p: p['proj'])
        best['batting_order'] = bo
        # Use the player_id that has lineup data (so confirmed/unconfirmed lookup works)
        if lineup_pid:
            best['player_id'] = lineup_pid
        # Recalculate base_score with merged batting_order
        if not best['is_pitcher'] and bo:
            bo_score = {1: 1.30, 2: 1.25, 3: 1.20, 4: 1.15, 5: 1.05,
                        6: 0.90, 7: 0.80, 8: 0.70, 9: 0.60}.get(bo, 0.75)
        else:
            bo_score = 1.0
        proj_score = best['proj'] ** 1.5
        salary_score = (best['salary'] / 3500) ** 1.3
        value = (best['proj'] / best['salary'] * 1000) if best['salary'] > 0 else 0
        value_score = value ** SALARY_CURVE_EXP
        odds_row = data['odds'].get(best.get('game_pk'))
        game_total = safe(odds_row.get('game_total'), 8.5) if odds_row else 8.5
        env_score = (game_total / 8.5) ** 1.5
        best['base_score'] = (proj_score * 0.40 + salary_score * 0.30 + value_score * 0.05 +
                              env_score * 0.10 + bo_score * 0.15)
        if best['is_pitcher']:
            best['base_score'] *= PITCHER_BOOST
        lu = data['lineup_map'].get(best['player_id'])
        confirmed = lu and lu.get('batting_order') and lu['batting_order'] >= 1
        if confirmed:
            best['base_score'] *= CONFIRMED_BOOST
        elif not best['is_pitcher'] and not confirmed:
            best['base_score'] *= UNCONFIRMED_PENALTY
        deduped.append(best)
        merges += len(group) - 1

    if merges:
        print(f"  Deduped: {len(pool)} → {len(deduped)} (merged {merges} duplicate players)")
    return deduped


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
        # Perturb scores with MULTIPLICATIVE noise so all players have a
        # realistic chance of being selected. Additive noise can't overcome
        # large base_score gaps between top pitchers.
        scores = np.zeros(len(pool))
        for i, p in enumerate(pool):
            # Multiplicative noise: score * (1 + noise)
            # Pitchers: SD=0.50 (±50%) so top 3-4 SPs compete for 2 slots
            # Hitters: SD=0.30 (±30%) — less volatile, more projection-driven
            noise_sd = 0.50 if p['is_pitcher'] else 0.30
            mult = 1.0 + rng.normal(0, noise_sd)
            scores[i] = p['base_score'] * max(mult, 0.05)  # floor at 5% to avoid negatives

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

    # Raw appearance rates: appear_count / feasible_sims * 100 → percentage
    # Each sim picks exactly 2 SP + 8 hitters, so across all sims the
    # raw rates within each position group naturally sum to (slots × 100%).
    # No need for additional normalization — the sim structure handles it.
    calibrated = {}
    for p in pool:
        pid = p['player_id']
        calibrated[pid] = appear.get(pid, 0) / feasible * 100

    # ── Actual ownership calibration (if available) ──────────────────────
    # Use multiplicative scaling (no intercept) to preserve the shape of the
    # distribution. Linear regression with an intercept lifts the floor
    # artificially, compressing the tails.
    actual_rows = sb.table('actual_ownership').select(
        'player_id,ownership_pct'
    ).eq('game_date', target_date).execute().data or []

    if len(actual_rows) >= 20:
        actual_map = {r['player_id']: r['ownership_pct'] for r in actual_rows}
        matched_pred = []
        matched_act = []
        for pid, pred in calibrated.items():
            if pid in actual_map and pred > 0:
                matched_pred.append(pred)
                matched_act.append(actual_map[pid])

        if len(matched_pred) >= 10:
            pred_arr = np.array(matched_pred)
            act_arr = np.array(matched_act)
            # Multiplicative scale: find ratio that minimizes squared error
            # actual ≈ scale * predicted (no intercept)
            scale = np.sum(pred_arr * act_arr) / np.sum(pred_arr ** 2)
            scale = clip(scale, 0.3, 3.0)  # sanity bounds
            print(f"  Calibration: scale = {scale:.2f}× (from {len(matched_pred)} matched players)")
            for pid in calibrated:
                calibrated[pid] = calibrated[pid] * scale
        else:
            print(f"  Calibration: only {len(matched_pred)} matched players — using uncalibrated")
    else:
        print(f"  No actual ownership data for {target_date} — using uncalibrated rates")

    # Re-normalize per position group so totals = slots × 100%.
    # The calibration step can distort totals — this restores the constraint
    # while preserving the relative ordering within each position.
    pos_groups = defaultdict(list)
    for p in pool:
        pos_groups[p['pos']].append(p['player_id'])

    for pos, pids in pos_groups.items():
        slots = POS_SLOTS.get(pos, 1)
        target_sum = slots * 100.0
        current_sum = sum(calibrated.get(pid, 0) for pid in pids)
        if current_sum > 0:
            scale = target_sum / current_sum
            for pid in pids:
                calibrated[pid] = calibrated[pid] * scale

    # Clamp to valid range [0, 100]
    for pid in calibrated:
        calibrated[pid] = clip(calibrated[pid], 0.0, 100.0)

    return calibrated


# ── Main ──────────────────────────────────────────────────────────────────────

def run_slate(data, slate, target_date, n_sims, rng):
    """Run ownership sim for a single slate. Returns (pool, calibrated) or None."""
    print(f"\n{'─'*55}")
    print(f"  Slate: {slate}")
    print(f"{'─'*55}")

    pool = build_pool(data, slate=slate)
    n_pit = sum(1 for p in pool if p['is_pitcher'])
    n_hit = sum(1 for p in pool if not p['is_pitcher'])
    print(f"  Pool: {len(pool)} ({n_pit} SP, {n_hit} hitters)")

    if len(pool) < 10:
        print("  Pool too small — skipping")
        return None

    print(f"  Running {n_sims:,} sims...")
    appear, feasible = simulate_ownership(pool, n_sims, rng)
    print(f"  Feasible: {feasible}/{n_sims} ({100*feasible/n_sims:.0f}%)")

    calibrated = calibrate_ownership(pool, appear, feasible, target_date)

    # ── Upsert to slate_ownership table ──────────────────────────────────
    computed_at = datetime.now(timezone.utc).isoformat()
    records = []
    for p in pool:
        own = calibrated.get(p['player_id'], 0)
        records.append({
            'player_id': p['player_id'],
            'game_pk': p['game_pk'],
            'game_date': target_date,
            'dk_slate': slate,
            'proj_ownership': round(own, 1),
            'computed_at': computed_at,
        })

    # Deduplicate (player can appear in multiple game_pks on same slate)
    seen = {}
    for r in records:
        seen[(r['player_id'], r['game_pk'], r['dk_slate'])] = r
    records = list(seen.values())

    BATCH = 500
    uploaded = 0
    for i in range(0, len(records), BATCH):
        batch = records[i:i+BATCH]
        sb.table('slate_ownership').upsert(
            batch, on_conflict='player_id,game_pk,dk_slate', ignore_duplicates=False
        ).execute()
        uploaded += len(batch)
    print(f"  Uploaded {uploaded} to slate_ownership [{slate}]")

    # ── Also update player_projections.proj_ownership for 'main' slate (backward compat)
    if slate == 'main':
        proj_records = []
        for p in pool:
            own = calibrated.get(p['player_id'], 0)
            proj_records.append({
                'player_id': p['player_id'],
                'game_pk': p['game_pk'],
                'game_date': target_date,
                'proj_ownership': round(own, 1),
                'computed_at': computed_at,
            })
        seen_proj = {}
        for r in proj_records:
            seen_proj[(r['player_id'], r['game_pk'])] = r
        proj_records = list(seen_proj.values())
        for i in range(0, len(proj_records), BATCH):
            batch = proj_records[i:i+BATCH]
            sb.table('player_projections').upsert(
                batch, on_conflict='player_id,game_pk', ignore_duplicates=False
            ).execute()
        print(f"  Also updated {len(proj_records)} in player_projections (main compat)")

    # ── Validation against actuals ────────────────────────────────────────
    actual_rows = sb.table('actual_ownership').select(
        'player_id,dk_name,ownership_pct'
    ).eq('game_date', target_date).eq('dk_slate', slate).execute().data or []

    actual_map = {}
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
    pitchers = [p for p in pool_sorted if p['is_pitcher']][:8]
    hitters = [p for p in pool_sorted if not p['is_pitcher']][:10]

    print(f"\n  Top SP ownership [{slate}]:")
    for p in pitchers:
        own = calibrated.get(p['player_id'], 0)
        actual_row = actual_map.get(p['player_id'])
        act = f"  actual={actual_row['ownership_pct']:.1f}%" if actual_row else ""
        print(f"    {p['name']:25s}  {own:5.1f}%  (${p['salary']:,}  proj={p['proj']:.1f}){act}")

    print(f"\n  Top hitter ownership [{slate}]:")
    for p in hitters:
        own = calibrated.get(p['player_id'], 0)
        actual_row = actual_map.get(p['player_id'])
        act = f"  actual={actual_row['ownership_pct']:.1f}%" if actual_row else ""
        print(f"    {p['name']:25s}  {own:5.1f}%  (${p['salary']:,}  proj={p['proj']:.1f}  {p['pos']}){act}")

    return pool, calibrated


def run():
    target_date = None
    n_sims = NUM_SIMS
    slate_arg = None
    args = sys.argv[1:]
    i = 0
    while i < len(args):
        if args[i] == '--date' and i+1 < len(args):
            target_date = args[i+1]; i += 2
        elif args[i] == '--sims' and i+1 < len(args):
            n_sims = int(args[i+1]); i += 2
        elif args[i] == '--slate' and i+1 < len(args):
            slate_arg = args[i+1]; i += 2
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

    slates = data['all_slates']
    if slate_arg:
        if slate_arg not in slates:
            print(f"  Slate '{slate_arg}' not found. Available: {slates}")
            return
        slates = [slate_arg]

    if not slates:
        print("  No slates found in dk_salaries — exiting")
        return

    print(f"\n  Running per-slate ownership for: {slates}")

    rng = np.random.default_rng(seed=123)
    total_uploaded = 0

    for slate in slates:
        result = run_slate(data, slate, target_date, n_sims, rng)
        if result:
            pool, calibrated = result
            total_uploaded += len(pool)

    print(f"\n{'='*55}")
    print(f"  All slates complete. {total_uploaded} total ownership records.")
    print(f"{'='*55}\n")


if __name__ == '__main__':
    run()
