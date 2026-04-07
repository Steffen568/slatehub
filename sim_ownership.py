#!/usr/bin/env python3
import sys
sys.stdout.reconfigure(encoding='utf-8', line_buffering=True)
"""
sim_ownership.py — Score-Based DFS Ownership Projection Engine

Estimates ownership using a weighted score model + softmax normalization.
Each player gets a score from known ownership drivers (projection, salary,
batting order, game environment), then softmax converts scores to ownership
percentages within each position group.

Weights derived from actual DK ownership data:
  Projection r=0.557, Salary r=0.536, BatOrder r=0.203, Value r=-0.055

Run:
  py -3.12 sim_ownership.py
  py -3.12 sim_ownership.py --date 2026-03-28
"""

import os, math
import numpy as np
from datetime import date, datetime, timezone
from collections import defaultdict
from supabase import create_client
from dotenv import load_dotenv
from config import SEASON

load_dotenv()
sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])

# Position slot counts (DK Classic)
POS_SLOTS = {'SP': 2, 'C': 1, '1B': 1, '2B': 1, '3B': 1, 'SS': 1, 'OF': 3}

# Canonical position priority for multi-pos players
POS_PRIORITY = ['C', 'SS', '2B', '3B', '1B', 'OF', 'SP']

# ── Scoring weights (from research correlation analysis) ────────────────────
# Projection r=0.557, Salary r=0.536, Value r=-0.055, BatOrder r=0.203
W_PROJ      = 0.40   # projection magnitude — strongest driver
W_SALARY    = 0.30   # salary / star power
W_BAT_ORDER = 0.15   # lineup position premium
W_ENV       = 0.10   # game environment (Vegas total)
W_VALUE     = 0.05   # pts per $1k — near-zero correlation but kept

# Pitcher gets a multiplicative boost — top SPs get 30-47% actual ownership
PITCHER_BOOST = 5.0

# Confirmed/unconfirmed modifiers
CONFIRMED_BOOST = 1.5
UNCONFIRMED_PENALTY = 0.4

# Softmax temperature per position — lower = sharper (more concentrated)
SOFTMAX_TEMP = {'SP': 0.8, 'C': 1.4, '1B': 1.4, '2B': 1.4,
                '3B': 1.4, 'SS': 1.4, 'OF': 1.4}

# Per-position ownership cap (large slate)
POSITION_MAX_OWN = {'SP': 55.0, 'C': 30.0, '1B': 30.0, '2B': 30.0,
                    '3B': 30.0, 'SS': 30.0, 'OF': 25.0}


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


def softmax(scores, temperature=1.0):
    """Softmax with temperature scaling. Returns list of shares summing to 1.0."""
    if not scores:
        return []
    scaled = [s / temperature for s in scores]
    max_s = max(scaled)
    exps = [math.exp(s - max_s) for s in scaled]
    total = sum(exps)
    return [e / total for e in exps] if total > 0 else [1.0 / len(scores)] * len(scores)


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

    # Projections
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
            'player_id,name,position,salary,team,dk_slate'
        ).eq('season', SEASON).eq('contest_type', 'classic').range(i, i + 999).execute().data or []
        sals.extend(rows)
        if len(rows) < 1000:
            break

    all_slates = sorted({s['dk_slate'] for s in sals if s.get('dk_slate')})

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

    # Odds
    odds = {}
    if game_pks:
        rows = sb.table('game_odds').select(
            'game_pk,game_total,home_implied,away_implied'
        ).in_('game_pk', game_pks).execute().data or []
        odds = {r['game_pk']: r for r in rows}

    return {
        'games': games, 'projs': projs,
        'all_slates': all_slates,
        'slate_sal_maps': slate_sal_maps,
        'slate_sal_name_maps': slate_sal_name_maps,
        'lineup_map': lineup_map,
        'odds': odds,
    }


# ── Build Player Pool ────────────────────────────────────────────────────────

def build_pool(data, slate=None):
    """Build the player pool with public-biased scores for ownership."""
    if slate:
        sal_map = data['slate_sal_maps'].get(slate, {})
        sal_name_map = data['slate_sal_name_maps'].get(slate, {})
    else:
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
        is_pitcher = p.get('is_pitcher', False)

        # Assign canonical position
        if is_pitcher:
            pos = 'SP'
        else:
            dk_positions = sal_row.get('position', '').split('/')
            pos = 'OF'
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
        proj_score = proj ** 1.5
        salary_score = (salary / 3500) ** 1.3
        value = (proj / salary * 1000) if salary > 0 else 0
        value_score = value ** 0.6
        env_score = (game_total / 8.5) ** 1.5

        if not is_pitcher and batting_order:
            bo_score = {1: 1.30, 2: 1.25, 3: 1.20, 4: 1.15, 5: 1.05,
                        6: 0.90, 7: 0.80, 8: 0.70, 9: 0.60}.get(batting_order, 0.75)
        else:
            bo_score = 1.0

        base_score = (proj_score * W_PROJ + salary_score * W_SALARY +
                      value_score * W_VALUE + env_score * W_ENV + bo_score * W_BAT_ORDER)

        if is_pitcher:
            base_score *= PITCHER_BOOST
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

    # Deduplicate by name
    name_groups = defaultdict(list)
    for p in pool:
        name_groups[p['name'].lower().strip()].append(p)

    deduped = []
    merges = 0
    for key, group in name_groups.items():
        if len(group) == 1:
            deduped.append(group[0])
            continue
        bo = next((p['batting_order'] for p in group if p['batting_order']), None)
        lineup_pid = next((p['player_id'] for p in group if data['lineup_map'].get(p['player_id'])), None)
        for p in group:
            p['batting_order'] = bo
        best = max(group, key=lambda p: p['proj'])
        best['batting_order'] = bo
        if lineup_pid:
            best['player_id'] = lineup_pid
        # Recalculate base_score with merged batting_order
        if not best['is_pitcher'] and bo:
            bo_s = {1: 1.30, 2: 1.25, 3: 1.20, 4: 1.15, 5: 1.05,
                    6: 0.90, 7: 0.80, 8: 0.70, 9: 0.60}.get(bo, 0.75)
        else:
            bo_s = 1.0
        ps = best['proj'] ** 1.5
        ss = (best['salary'] / 3500) ** 1.3
        v = (best['proj'] / best['salary'] * 1000) if best['salary'] > 0 else 0
        vs = v ** 0.6
        odds_row = data['odds'].get(best.get('game_pk'))
        gt = safe(odds_row.get('game_total'), 8.5) if odds_row else 8.5
        es = (gt / 8.5) ** 1.5
        best['base_score'] = (ps * W_PROJ + ss * W_SALARY + vs * W_VALUE +
                              es * W_ENV + bo_s * W_BAT_ORDER)
        if best['is_pitcher']:
            best['base_score'] *= PITCHER_BOOST
        lu = data['lineup_map'].get(best['player_id'])
        conf = lu and lu.get('batting_order') and lu['batting_order'] >= 1
        if conf:
            best['base_score'] *= CONFIRMED_BOOST
        elif not best['is_pitcher'] and not conf:
            best['base_score'] *= UNCONFIRMED_PENALTY
        deduped.append(best)
        merges += len(group) - 1

    if merges:
        print(f"  Deduped: {len(pool)} → {len(deduped)} (merged {merges} duplicate players)")
    return deduped


# ── Score-Based Ownership ────────────────────────────────────────────────────

def compute_ownership_scores(pool):
    """
    Convert player base_scores to ownership percentages via softmax per position.

    For each position group:
    1. Collect base_scores
    2. Apply softmax with position-specific temperature
    3. Scale to (slots × 100%) so total ownership per position is correct
    4. Cap individual players at position max
    5. Redistribute excess from capped players to uncapped ones
    """
    # Group by position
    pos_groups = defaultdict(list)
    for p in pool:
        pos_groups[p['pos']].append(p)

    ownership = {}

    for pos, players in pos_groups.items():
        if not players:
            continue

        slots = POS_SLOTS.get(pos, 1)
        target_sum = slots * 100.0
        base_temp = SOFTMAX_TEMP.get(pos, 1.4)
        # Scale temperature with pool size — more players = softer distribution
        # Prevents 100%/0% binary splits on large slates
        temp = base_temp * max(1.0, len(players) / (slots * 4))
        cap = POSITION_MAX_OWN.get(pos, 30.0)

        # Dynamic cap for small pools: if few players, allow higher concentration
        avg_target = target_sum / len(players) if players else 10.0
        if avg_target > cap * 0.5:
            cap = min(max(cap, avg_target * 2), 90.0)

        raw_scores = [p['base_score'] for p in players]
        # Normalize to z-scores so softmax operates on relative differences,
        # not raw magnitudes (which can have huge gaps from PITCHER_BOOST etc.)
        mean_s = sum(raw_scores) / len(raw_scores)
        std_s = (sum((s - mean_s) ** 2 for s in raw_scores) / len(raw_scores)) ** 0.5
        if std_s > 0:
            scores = [(s - mean_s) / std_s for s in raw_scores]
        else:
            scores = [0.0] * len(raw_scores)
        shares = softmax(scores, temperature=temp)

        # Scale to target sum
        raw_own = [s * target_sum for s in shares]

        # Cap and redistribute
        for _ in range(5):  # iterate to converge
            excess = 0.0
            uncapped_sum = 0.0
            for i, own in enumerate(raw_own):
                if own > cap:
                    excess += own - cap
                    raw_own[i] = cap
                else:
                    uncapped_sum += own
            if excess <= 0.01:
                break
            if uncapped_sum > 0:
                scale = (uncapped_sum + excess) / uncapped_sum
                for i in range(len(raw_own)):
                    if raw_own[i] < cap:
                        raw_own[i] *= scale

        for i, p in enumerate(players):
            ownership[p['player_id']] = clip(raw_own[i], 0.0, 100.0)

    return ownership


# ── Calibration ──────────────────────────────────────────────────────────────

def calibrate_ownership(pool, raw_ownership, target_date):
    """
    Calibrate raw ownership scores against actual DK ownership data.
    Uses multiplicative scaling to preserve distribution shape.
    """
    calibrated = dict(raw_ownership)

    # ── Actual ownership calibration (if available) ──────────────────────
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
            scale = np.sum(pred_arr * act_arr) / np.sum(pred_arr ** 2)
            scale = clip(scale, 0.3, 3.0)
            print(f"  Calibration: scale = {scale:.2f}× (from {len(matched_pred)} matched players)")
            for pid in calibrated:
                calibrated[pid] = calibrated[pid] * scale
        else:
            print(f"  Calibration: only {len(matched_pred)} matched players — using uncalibrated")
    else:
        print(f"  No actual ownership data for {target_date} — using uncalibrated rates")

    # Re-normalize per position group
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

    for pid in calibrated:
        calibrated[pid] = clip(calibrated[pid], 0.0, 100.0)

    return calibrated


# ── Main ──────────────────────────────────────────────────────────────────────

def run_slate(data, slate, target_date):
    """Run ownership projection for a single slate. Returns (pool, calibrated) or None."""
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

    # Score-based ownership (replaces 3,000-sim lineup builder)
    raw_ownership = compute_ownership_scores(pool)
    calibrated = calibrate_ownership(pool, raw_ownership, target_date)

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

    # Also update player_projections.proj_ownership for 'main' slate (backward compat)
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
    slate_arg = None
    args = sys.argv[1:]
    i = 0
    while i < len(args):
        if args[i] == '--date' and i+1 < len(args):
            target_date = args[i+1]; i += 2
        elif args[i] == '--slate' and i+1 < len(args):
            slate_arg = args[i+1]; i += 2
        else:
            target_date = target_date or args[i]; i += 1
    if not target_date:
        target_date = str(date.today())

    print(f"\nOwnership Engine (Score-Based) — {target_date}")
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

    total_uploaded = 0
    for slate in slates:
        result = run_slate(data, slate, target_date)
        if result:
            pool, calibrated = result
            total_uploaded += len(pool)

    print(f"\n{'='*55}")
    print(f"  All slates complete. {total_uploaded} total ownership records.")
    print(f"{'='*55}\n")


if __name__ == '__main__':
    run()
