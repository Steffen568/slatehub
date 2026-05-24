#!/usr/bin/env python3
"""
optimize_portfolio.py — DFS Portfolio Optimizer

Three selection strategies:

1. Leverage-based (leverage_portfolio) [DEFAULT]: sorts pool by gpp_score,
   applies per-player caps derived from leverage = proj × (1 − ownership).
   K-independent — same proportions at K=20 and K=150.
   Backed by industry-standard leverage theory (4for4, FantasyLabs, SaberSim).

2. Coverage-based (greedy_portfolio): rewards lineups that cover new high-upside
   players not yet represented. Known issue: K-dependent flip (chalk at 100% for
   K=20, near 0% for K=150). Kept for comparison.

3. Simulation-based (sim_greedy_portfolio): generates N game-outcome scenarios
   from player distributions, then greedily maximizes E[max score across portfolio].
   Backed by Hunter/Vielma/Zaman (2016).

Usage:
    py -3.12 optimize_portfolio.py --date 2026-05-01 --slate main --k 20
    py -3.12 optimize_portfolio.py --date 2026-05-01 --slate main --k 150
    py -3.12 optimize_portfolio.py --date 2026-05-01 --slate main --k 20 --mode sim

Outputs: selected lineup IDs + diversity stats (avg pairwise overlap %).
"""
import os
import sys
import math
import argparse
import numpy as np
from datetime import date
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()
sb = create_client(os.environ['SUPABASE_URL'], os.environ['SUPABASE_KEY'])


# ── Coverage reward weight by contest type (coverage mode only) ──────────────
COVERAGE_ALPHA = {
    'large':  0.10,
    'mid':    0.07,
    'small':  0.04,
    'single': 0.00,
}

# ── Exposure target range by contest type (leverage mode) ─────────────────────
# (min_exp, max_exp) — leverage-proportional target percentage per player.
# min_exp = low-leverage players (chalk); max_exp = high-leverage contrarians.
# Larger field → lower caps needed (must be unique); smaller field → higher caps OK.
EXP_RANGES = {
    'gpp_large': (0.12, 0.35),   # 15k+ entries — diversify hard
    'gpp_mid':   (0.15, 0.42),   # 10k–15k entries
    'gpp_small': (0.18, 0.50),   # 5k–10k entries
    'se_large':  (0.25, 0.60),   # single-entry, 1.5k+ field
    'se_mid':    (0.30, 0.70),   # single-entry, 500–1k field
    'se_small':  (0.35, 0.80),   # single-entry, <500 field — just play your best stuff
}


def load_pool(game_date: str, slate: str) -> list[dict]:
    """Load user pool lineups from Supabase sim_pool table."""
    rows = []
    offset = 0
    while True:
        q = (sb.table('sim_pool')
               .select('pool_id,player_ids,proj,gpp_score,avg_pms,stack_team,salary')
               .eq('game_date', game_date)
               .eq('pool_type', 'user'))
        if slate != 'all':
            q = q.eq('dk_slate', slate)
        chunk = q.range(offset, offset + 999).execute()
        rows += chunk.data
        if len(chunk.data) < 1000:
            break
        offset += 1000
    return rows


def load_player_quality(game_date: str, player_ids: list[int]) -> dict[int, float]:
    """Load per-player leverage-weighted quality = sqrt(ceiling × context_mult / ownership).
    Rewards high-ceiling plays that are underowned relative to their upside (Haugh/Singal 2021).
    sqrt dampens extreme outliers; falls back to ceiling × ctx when ownership is missing."""
    quality = {}
    for i in range(0, len(player_ids), 500):
        chunk_ids = player_ids[i:i + 500]
        rows = (sb.table('player_projections')
                  .select('player_id,proj_ceiling,context_mult,proj_ownership')
                  .eq('game_date', game_date)
                  .in_('player_id', chunk_ids)
                  .execute())
        for r in rows.data:
            ceil_ = r.get('proj_ceiling') or 0.0
            ctx   = r.get('context_mult') or 1.0
            own_  = r.get('proj_ownership') or 10.0
            quality[r['player_id']] = max(ceil_ * ctx / max(own_, 1.0), 0.01) ** 0.5
    return quality


def load_player_leverage(game_date: str, player_ids: list[int]) -> dict[int, float]:
    """Load leverage = proj_dk_pts × (1 − ownership_fraction) for each player.
    High leverage = well-projected AND underowned. Caps derived from these values
    are proportional to leverage, giving K-independent portfolio exposure."""
    leverage = {}
    for i in range(0, len(player_ids), 500):
        chunk_ids = player_ids[i:i + 500]
        rows = (sb.table('player_projections')
                  .select('player_id,proj_dk_pts,proj_ownership')
                  .eq('game_date', game_date)
                  .in_('player_id', chunk_ids)
                  .execute())
        for r in rows.data:
            proj = r.get('proj_dk_pts') or 0.0
            own  = (r.get('proj_ownership') or 10.0) / 100.0
            leverage[r['player_id']] = max(proj * (1.0 - own), 0.01)
    return leverage


def leverage_portfolio(pool: list[dict], leverage: dict[int, float],
                       k: int, contest: str = 'gpp_mid') -> list[dict]:
    """Rank pool lineups by gpp_score with K-independent percentage-based exposure caps.

    Each player gets a target exposure percentage derived from their leverage score
    and the contest type (EXP_RANGES). A lineup is blocked if adding it would push
    any player's running percentage above their target.

    This check uses counts[pid] / selected_so_far — no K in the formula — so the
    ranking order is identical regardless of K. Top 20 of a K=150 run == K=20 run.
    """
    for lu in pool:
        lu['_pids'] = set(lu['player_ids'])

    min_exp, max_exp = EXP_RANGES.get(contest, EXP_RANGES['gpp_mid'])
    fallback_pct     = (min_exp + max_exp) / 2

    levs    = list(leverage.values())
    min_lev = min(levs) if levs else 0.01
    max_lev = max(levs) if levs else 1.0
    lev_rng = max_lev - min_lev or 1.0

    target_pct: dict[int, float] = {}
    for pid, lev in leverage.items():
        norm = (lev - min_lev) / lev_rng
        target_pct[pid] = min_exp + (max_exp - min_exp) * norm

    sorted_pool   = sorted(pool, key=lambda x: x.get('gpp_score') or 0.0, reverse=True)
    player_counts: dict[int, int] = {}
    selected      = []

    print(f"  Running leverage portfolio (contest={contest}, "
          f"exp={min_exp:.0%}–{max_exp:.0%}) over {len(sorted_pool):,} candidates...")

    for lu in sorted_pool:
        if len(selected) >= k:
            break
        n = len(selected)
        if n > 0 and any(
            player_counts.get(pid, 0) / n > target_pct.get(pid, fallback_pct)
            for pid in lu['_pids']
        ):
            continue
        for pid in lu['_pids']:
            player_counts[pid] = player_counts.get(pid, 0) + 1
        lu['_rank'] = n + 1
        selected.append(lu)

    return selected


def lineup_corr(pids_a: set, pids_b: set, player_quality: dict) -> float:
    """Quality-weighted Jaccard overlap between two lineups for diversity reporting.
    Returns 0 (nothing shared) to 1 (identical lineups).
    High-quality boom players count more when shared."""
    shared = pids_a & pids_b
    all_p  = pids_a | pids_b
    shared_q = sum(player_quality.get(p, 1.0) for p in shared)
    total_q  = sum(player_quality.get(p, 1.0) for p in all_p)
    return shared_q / total_q if total_q > 0 else 0.0


def marginal_gain(candidate: dict, selected: list[dict],
                  player_quality: dict, alpha: float) -> float:
    """Coverage-based marginal gain from adding candidate to selected set.
    Rewards covering high-quality players not yet represented in the portfolio.
    Signal: gpp_score (ceiling-weighted). Diversity: coverage of boom opportunities.
    alpha=0.30 → strong coverage reward; alpha=0.0 → pure gpp_score sort."""
    ev = candidate.get('gpp_score') or candidate.get('proj') or 0.0
    if not selected or alpha == 0.0:
        return ev
    already_covered = {pid for lu in selected for pid in lu['_pids']}
    new_quality = sum(player_quality.get(pid, 0.0)
                      for pid in candidate['_pids']
                      if pid not in already_covered)
    # Normalize: 10 players × avg quality ~18 pts ≈ 180 per full lineup
    return ev + alpha * (new_quality / 180.0)


def greedy_portfolio(pool: list[dict], player_quality: dict,
                     k: int, alpha: float) -> list[dict]:
    """Greedy coverage-maximizing selection. O(K × N) time.
    Returns list of K selected lineup dicts, ordered by selection round."""
    selected = []
    remaining = list(pool)

    # Pre-compute pid sets for speed
    for lu in remaining:
        lu['_pids'] = set(lu['player_ids'])

    print(f"  Running greedy portfolio selection (K={k}, alpha={alpha:.2f}) "
          f"over {len(remaining):,} candidates...")

    while len(selected) < k and remaining:
        best_idx, best_gain = 0, -1.0
        for i, cand in enumerate(remaining):
            g = marginal_gain(cand, selected, player_quality, alpha)
            if g > best_gain:
                best_gain = g
                best_idx = i
        chosen = remaining.pop(best_idx)
        chosen['_rank'] = len(selected) + 1
        selected.append(chosen)
        if len(selected) % 10 == 0:
            print(f"    Selected {len(selected)}/{k}...")

    return selected


# ── Simulation-based portfolio selection ──────────────────────────────────────
# Implements Max-E[max(V_1,...,V_K)] via scenario simulation.
# No alpha/disc parameters — lineup diversity emerges from the math:
# a lineup correlated with already-selected ones adds near-zero expected improvement.

N_SIMS       = 1000   # scenarios per slate (increase for less variance, decrease for speed)
TEAM_VAR_SHARE = 0.30  # 30% of player score variance is shared within a team
TEAM_SD_FACTOR = 0.15  # team boost sd = 15% of avg slate projection


def load_player_sim_data(game_date: str, player_ids: list[int]) -> dict[int, dict]:
    """Load per-player simulation parameters: proj, sd, team, game_pk, ceiling, ctx.
    SD is derived from (proj_ceiling - proj_floor) / 4 — treating ceiling/floor as ~2-sigma bounds.
    Falls back to proj_dk_pts * 0.45 if ceiling/floor are missing.
    """
    data = {}
    for i in range(0, len(player_ids), 500):
        chunk = player_ids[i:i + 500]
        rows = (sb.table('player_projections')
                  .select('player_id,proj_dk_pts,proj_floor,proj_ceiling,team,game_pk,context_mult')
                  .eq('game_date', game_date)
                  .in_('player_id', chunk)
                  .execute())
        for r in rows.data:
            proj  = r.get('proj_dk_pts') or 0.0
            ceil_ = r.get('proj_ceiling') or 0.0
            floor_ = r.get('proj_floor') or 0.0
            # Derive SD from ceiling-floor spread; fallback to 45% of projection
            if ceil_ > floor_:
                sd = (ceil_ - floor_) / 4.0
            else:
                sd = proj * 0.45
            data[r['player_id']] = {
                'proj':  proj,
                'sd':    sd,
                'team':  r.get('team') or '',
                'ceil':  ceil_,
                'ctx':   r.get('context_mult') or 1.0,
            }
    return data


def simulate_scenarios(player_sim_data: dict[int, dict],
                       n_sims: int = N_SIMS,
                       seed: int = 42) -> dict[int, np.ndarray]:
    """
    Generate n_sims game-outcome scenarios using stored player distributions.
    Models team-level correlation: 30% of variance is shared within a team
    (when the Cubs score 8 runs, all Cubs batters benefit).

    Returns: {player_id: np.array(n_sims,)} — one score per scenario.
    """
    rng = np.random.default_rng(seed)

    # Avg proj across all players for team boost scaling
    projs = [d['proj'] for d in player_sim_data.values() if d['proj'] > 0]
    avg_proj = float(np.mean(projs)) if projs else 8.0

    # Draw one team-level boost per team per scenario
    teams = list({d['team'] for d in player_sim_data.values() if d['team']})
    team_boosts = {t: rng.normal(0, TEAM_SD_FACTOR * avg_proj, n_sims) for t in teams}

    scenario_scores = {}
    for pid, d in player_sim_data.items():
        proj = d['proj']
        sd   = d['sd'] if d['sd'] > 0 else proj * 0.5
        team = d['team']

        # Individual variance (after removing team-level share)
        ind_sd = sd * np.sqrt(max(1 - TEAM_VAR_SHARE, 0.0))
        individual = rng.normal(proj, ind_sd, n_sims)
        boost = team_boosts.get(team, np.zeros(n_sims))
        scenario_scores[pid] = np.maximum(0.0, individual + boost)

    return scenario_scores


def score_lineups_scenarios(pool: list[dict],
                            scenario_scores: dict[int, np.ndarray],
                            n_sims: int = N_SIMS) -> None:
    """
    Adds '_sim_scores' (np.array, shape n_sims) to each lineup dict in-place.
    Vectorized across players for speed.
    """
    # Build score matrix (n_players, n_sims) for fast indexing
    all_pids   = list(scenario_scores.keys())
    pid_to_idx = {pid: i for i, pid in enumerate(all_pids)}
    score_mat  = np.stack([scenario_scores[pid] for pid in all_pids])  # (n_players, n_sims)
    zeros      = np.zeros(n_sims)

    for lu in pool:
        pids    = lu.get('player_ids') or []
        indices = [pid_to_idx[pid] for pid in pids if pid in pid_to_idx]
        lu['_sim_scores'] = score_mat[indices].sum(axis=0) if indices else zeros.copy()


def sim_greedy_portfolio(pool: list[dict], k: int,
                         verbose: bool = True) -> list[dict]:
    """
    Greedy Max-E[max(V_1,...,V_K)] using simulated scenarios.
    Fully vectorized — fast even for 25k lineups × 1k sims.

    At each round: picks the lineup with highest Expected Improvement
    over the current portfolio ceiling across all scenarios:
        E[max(0, candidate_score[s] - current_max[s])]

    Diversity is automatic: a lineup correlated with already-selected ones
    wins in the same scenarios → near-zero marginal improvement → not picked.
    """
    n_sims = len(pool[0]['_sim_scores']) if pool else N_SIMS

    # Build score matrix (n_pool, n_sims) for vectorized ops
    sim_matrix   = np.stack([lu['_sim_scores'] for lu in pool])  # (n_pool, n_sims)
    remaining_idx = list(range(len(pool)))
    current_max  = np.zeros(n_sims)
    selected     = []

    if verbose:
        print(f"  Running sim-greedy portfolio (K={k}, n_sims={n_sims:,}) "
              f"over {len(pool):,} candidates...")

    while len(selected) < k and remaining_idx:
        # Vectorized expected improvement for all remaining candidates
        rem_sims     = sim_matrix[remaining_idx]          # (n_rem, n_sims)
        improvements = np.maximum(0.0, rem_sims - current_max)  # broadcast
        exp_imp      = improvements.mean(axis=1)           # (n_rem,)

        best_local  = int(exp_imp.argmax())
        best_global = remaining_idx[best_local]

        chosen = pool[best_global]
        chosen['_rank'] = len(selected) + 1
        current_max = np.maximum(current_max, sim_matrix[best_global])
        remaining_idx.pop(best_local)
        selected.append(chosen)

        if verbose and len(selected) % 10 == 0:
            print(f"    Selected {len(selected)}/{k}...")

    return selected


def diversity_stats(selected: list[dict], player_quality: dict) -> dict:
    """Compute avg and max pairwise quality-weighted player overlap for selected portfolio."""
    n = len(selected)
    if n < 2:
        return {'avg_overlap_pct': 0.0, 'max_overlap_pct': 0.0, 'pairs': 0}
    overlaps = []
    for i in range(n):
        for j in range(i + 1, n):
            overlaps.append(lineup_corr(selected[i]['_pids'], selected[j]['_pids'], player_quality))
    return {
        'avg_overlap_pct': round(np.mean(overlaps) * 100, 1),
        'max_overlap_pct': round(np.max(overlaps) * 100, 1),
        'pairs': len(overlaps),
    }


def main():
    parser = argparse.ArgumentParser(description='DFS Portfolio Optimizer')
    parser.add_argument('--date',   default=str(date.today()), help='Game date (YYYY-MM-DD)')
    parser.add_argument('--slate',  default='main', help='Slate: main/early/turbo/night/all')
    parser.add_argument('--k',      type=int, default=20, help='Number of lineups to select')
    parser.add_argument('--contest', default='gpp_mid',
                        choices=list(EXP_RANGES.keys()) + ['large', 'mid', 'small', 'single'],
                        help='Contest type: gpp_large/gpp_mid/gpp_small/se_large/se_mid/se_small')
    parser.add_argument('--mode', default='leverage',
                        choices=['leverage', 'coverage', 'sim'],
                        help='Selection mode: leverage (default, K-independent) / coverage / sim')
    parser.add_argument('--sims', type=int, default=N_SIMS,
                        help=f'Number of scenarios for sim mode (default: {N_SIMS})')
    args = parser.parse_args()

    # Map legacy contest names and derive coverage alpha
    _contest_map = {'large': 'gpp_large', 'mid': 'gpp_mid', 'small': 'gpp_small', 'single': 'se_small'}
    contest = _contest_map.get(args.contest, args.contest)
    _alpha_map = {'gpp_large': 0.10, 'gpp_mid': 0.07, 'gpp_small': 0.04,
                  'se_large': 0.02, 'se_mid': 0.01, 'se_small': 0.00}
    alpha = _alpha_map.get(contest, 0.07)
    print(f"\nPortfolio Optimizer — {args.date} / {args.slate} / K={args.k} / "
          f"mode={args.mode} / contest={contest}")
    print('=' * 65)

    # Load pool
    print(f"  Loading user pool...")
    pool = load_pool(args.date, args.slate)
    if not pool:
        print("  ERROR: No user pool found. Run generate_pool.py first.")
        sys.exit(1)
    print(f"  Loaded {len(pool):,} user lineups")

    # Collect all unique player IDs
    all_pids = list({p for lu in pool for p in (lu['player_ids'] or [])})

    if args.mode == 'leverage':
        print(f"  Loading player leverage for {len(all_pids):,} players...")
        leverage = load_player_leverage(args.date, all_pids)
        print(f"  Leverage loaded for {len(leverage):,} players")
        selected = leverage_portfolio(pool, leverage, args.k, contest)
        # Use leverage as quality proxy for diversity reporting
        player_quality = leverage

    elif args.mode == 'sim':
        print(f"  Loading player sim data for {len(all_pids):,} players...")
        player_sim_data = load_player_sim_data(args.date, all_pids)
        print(f"  Simulating {args.sims:,} game scenarios...")
        scenario_scores = simulate_scenarios(player_sim_data, n_sims=args.sims)
        score_lineups_scenarios(pool, scenario_scores, n_sims=args.sims)
        selected = sim_greedy_portfolio(pool, args.k)
        player_quality = {pid: d['ceil'] * d['ctx']
                          for pid, d in player_sim_data.items()}

    else:  # coverage
        print(f"  Loading player quality for {len(all_pids):,} players...")
        player_quality = load_player_quality(args.date, all_pids)
        print(f"  Quality loaded for {len(player_quality):,} players")
        selected = greedy_portfolio(pool, player_quality, args.k, alpha)

    # Diversity stats
    stats = diversity_stats(selected, player_quality)

    # Report
    print(f"\n{'='*65}")
    print(f"  Portfolio: {len(selected)} lineups selected")
    print(f"  Avg pairwise player overlap: {stats['avg_overlap_pct']}%  "
          f"(target: 20-35% for large GPP)")
    print(f"  Max pairwise player overlap: {stats['max_overlap_pct']}%")
    print(f"\n  {'Rank':<5} {'Stack':<6} {'Proj':>6}  {'GPP':>6}  {'PMS':>5}  {'Lineup IDs'}")
    print(f"  {'-'*60}")
    for lu in selected:
        print(f"  {lu['_rank']:<5} {lu.get('stack_team','??'):<6} "
              f"{lu.get('proj',0):>6.1f}  {lu.get('gpp_score',0):>6.3f}  "
              f"{lu.get('avg_pms',0):>5.1f}  "
              f"[{','.join(str(p) for p in lu['player_ids'][:5])}...]")

    avg_proj = np.mean([lu.get('proj', 0) for lu in selected])
    avg_gpp  = np.mean([lu.get('gpp_score', 0) for lu in selected])
    print(f"\n  Avg proj: {avg_proj:.1f}  |  Avg GPP score: {avg_gpp:.3f}")

    # Player exposure summary (top 12 by appearance count)
    from collections import Counter
    pid_counts = Counter(pid for lu in selected for pid in (lu['player_ids'] or []))
    print(f"\n  Top player exposure (appearances / {len(selected)} lineups):")
    # Load names for display from dk_salaries (no date filter — names don't change)
    pid_names: dict[int, str] = {}
    name_rows = (sb.table('dk_salaries')
                   .select('player_id,name,position')
                   .in_('player_id', list(pid_counts.keys()))
                   .limit(500)
                   .execute().data)
    for r in name_rows:
        if r['player_id'] not in pid_names:
            pid_names[r['player_id']] = f"{r.get('name','?')[:18]:18s} ({r.get('position','?')[:3]})"
    for pid, cnt in pid_counts.most_common(12):
        pct = cnt / len(selected) * 100
        label = pid_names.get(pid, f"pid={pid}")
        bar = '#' * int(pct / 5)
        print(f"    {label}  {cnt:3d} / {len(selected)}  {pct:4.0f}%  {bar}")

    # Stack distribution
    from collections import Counter as Ctr
    team_counts = Ctr(lu.get('stack_team', '??') for lu in selected)
    print(f"\n  Stack distribution:")
    for team, cnt in team_counts.most_common():
        pct = cnt / len(selected) * 100
        bar = '#' * int(pct / 3)
        print(f"    {team:<6}  {cnt:3d} / {len(selected)}  {pct:4.0f}%  {bar}")

    print(f"\n  Pool IDs (for DB lookup): "
          f"{[lu.get('pool_id') for lu in selected[:10]]}...")


if __name__ == '__main__':
    main()
