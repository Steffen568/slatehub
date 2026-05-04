#!/usr/bin/env python3
"""
optimize_portfolio.py — DFS Portfolio Optimizer

Selects the optimal K lineups from the generated user pool using a
greedy Max-E[max(V_1,...,V_K)] algorithm. No contest simulation or
ownership data required.

Mathematical basis:
  E[max(V_1,...,V_K)] is maximized by selecting lineups with high
  individual EV AND low correlation with already-selected lineups.
  Correlation is measured as variance-weighted player overlap (Jaccard).
  High-variance (boom-or-bust) players count more when shared — sharing
  them tightly couples lineup fates, reducing portfolio ceiling.

Usage:
    py -3.12 optimize_portfolio.py --date 2026-05-01 --slate main --k 20
    py -3.12 optimize_portfolio.py --date 2026-05-01 --slate main --k 150
    py -3.12 optimize_portfolio.py --date 2026-05-01 --slate all --k 20

Outputs: selected lineup IDs + diversity stats (avg pairwise overlap %).
"""
import os
import sys
import argparse
import numpy as np
from datetime import date
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()
sb = create_client(os.environ['SUPABASE_URL'], os.environ['SUPABASE_KEY'])


# ── Coverage reward weight by contest type ───────────────────────────────────
# How aggressively to reward covering new high-upside players not yet in the portfolio.
# Large GPP (100k+ field): maximize coverage of all boom opportunities.
# Single entry: no coverage incentive — just pick the highest gpp_score lineup.
COVERAGE_ALPHA = {
    'large':  0.30,   # large GPP — strong coverage incentive
    'mid':    0.20,   # mid-size GPP (1k–10k entries)
    'small':  0.10,   # small field (20–100 players)
    'single': 0.00,   # single-entry — pure gpp_score, no diversification
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
    """Load per-player quality score = proj_ceiling × context_mult (game-adjusted boom potential).
    High quality = high ceiling AND favorable game environment (Vegas/park/weather).
    Used as both coverage reward weights in selection and overlap weights in diversity reporting."""
    quality = {}
    for i in range(0, len(player_ids), 500):
        chunk_ids = player_ids[i:i + 500]
        rows = (sb.table('player_projections')
                  .select('player_id,proj_ceiling,context_mult')
                  .eq('game_date', game_date)
                  .in_('player_id', chunk_ids)
                  .execute())
        for r in rows.data:
            ceil_ = r.get('proj_ceiling') or 0.0
            ctx   = r.get('context_mult') or 1.0
            quality[r['player_id']] = max(ceil_ * ctx, 0.01)
    return quality


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
    parser.add_argument('--contest', default='large',
                        choices=['large', 'mid', 'small', 'single'],
                        help='Contest type: large/mid/small/single (controls coverage reward strength)')
    args = parser.parse_args()

    alpha = COVERAGE_ALPHA[args.contest]
    print(f"\nPortfolio Optimizer — {args.date} / {args.slate} / K={args.k} / "
          f"contest={args.contest} (alpha={alpha:.2f})")
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
    print(f"  Loading player quality for {len(all_pids):,} players...")
    player_quality = load_player_quality(args.date, all_pids)
    print(f"  Quality loaded for {len(player_quality):,} players")

    # Run optimizer
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
    print(f"  Pool IDs (for DB lookup): "
          f"{[lu.get('pool_id') for lu in selected[:10]]}...")


if __name__ == '__main__':
    main()
