#!/usr/bin/env python3
"""
backtest_portfolio.py — Portfolio Optimizer Backtest

Compares four lineup-selection strategies across historical slate data:
  1. Portfolio  — greedy Max-E[max] (our optimizer)
  2. GPP Score  — top-K by gpp_score
  3. Projection — top-K by proj
  4. Random     — baseline

For each strategy, measures:
  max_actual  — best lineup actual score (the GPP metric — one lineup winning is all that matters)
  avg_actual  — average across K lineups
  gpp_count   — how many lineups exceeded the GPP line
  cash_count  — how many lineups exceeded the cash line
  avg_overlap — avg pairwise player overlap (diversity check)

Usage:
    py -3.12 backtest_portfolio.py
    py -3.12 backtest_portfolio.py --start 2026-04-25 --end 2026-05-03
    py -3.12 backtest_portfolio.py --slate main --k 20
    py -3.12 backtest_portfolio.py --k 5 --contest small
"""
import os
import sys
import argparse
import random
import numpy as np
from datetime import date, timedelta
from dotenv import load_dotenv
from supabase import create_client

# Reuse portfolio core functions
from optimize_portfolio import (
    lineup_corr, greedy_portfolio, diversity_stats, COVERAGE_ALPHA
)

load_dotenv()
sb = create_client(os.environ['SUPABASE_URL'], os.environ['SUPABASE_KEY'])

GPP_LINE  = 147.0   # avg top-1% threshold from research_findings
CASH_LINE = 94.0    # avg cash line from research_findings


# ── Data loading ──────────────────────────────────────────────────────────────

def load_pool_for_date(game_date: str, slate: str) -> list[dict]:
    """Load user pool from sim_pool for a given date/slate."""
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


def load_actuals_for_date(game_date: str) -> dict[int, float]:
    """Load actual DK points keyed by player_id. One game per player per slate."""
    rows = (sb.table('actual_results')
              .select('player_id,actual_dk_pts')
              .eq('game_date', game_date)
              .execute())
    # If a player somehow appears twice (doubleheader), take the higher score
    result = {}
    for r in rows.data:
        pid = r['player_id']
        pts = r.get('actual_dk_pts') or 0.0
        result[pid] = max(result.get(pid, 0.0), pts)
    return result


def load_player_quality_for_date(game_date: str, player_ids: list[int]) -> dict[int, float]:
    """Load per-player quality = proj_ceiling × context_mult for a given date."""
    quality = {}
    for i in range(0, len(player_ids), 500):
        chunk = player_ids[i:i + 500]
        rows = (sb.table('player_projections')
                  .select('player_id,proj_ceiling,context_mult')
                  .eq('game_date', game_date)
                  .in_('player_id', chunk)
                  .execute())
        for r in rows.data:
            ceil_ = r.get('proj_ceiling') or 0.0
            ctx   = r.get('context_mult') or 1.0
            quality[r['player_id']] = max(ceil_ * ctx, 0.01)
    return quality


# ── Scoring ───────────────────────────────────────────────────────────────────

def score_lineup(lineup: dict, actual_by_pid: dict[int, float]) -> float:
    """Sum actual DK points for all player_ids in a lineup."""
    return sum(actual_by_pid.get(pid, 0.0) for pid in (lineup.get('player_ids') or []))


def score_strategy(picks: list[dict], actual_by_pid: dict[int, float],
                   player_quality: dict, pool: list[dict]) -> dict:
    """Compute all metrics for a set of K selected lineups."""
    actuals = [score_lineup(lu, actual_by_pid) for lu in picks]
    if not actuals:
        return {}
    # Pairwise overlap for diversity measurement
    for lu in picks:
        lu['_pids'] = set(lu.get('player_ids') or [])
    div = diversity_stats(picks, player_quality)
    # Where does the best lineup rank in the full pool?
    all_actuals = sorted([score_lineup(lu, actual_by_pid) for lu in pool], reverse=True)
    best = max(actuals)
    best_rank = next((i + 1 for i, v in enumerate(all_actuals) if v <= best), len(all_actuals))
    return {
        'max_actual':   round(best, 1),
        'avg_actual':   round(np.mean(actuals), 1),
        'gpp_count':    sum(a >= GPP_LINE for a in actuals),
        'cash_count':   sum(a >= CASH_LINE for a in actuals),
        'best_rank':    best_rank,
        'avg_overlap':  div['avg_overlap_pct'],
    }


# ── Strategy selectors ────────────────────────────────────────────────────────

def top_k_by(pool: list[dict], key: str, k: int, reverse: bool = True) -> list[dict]:
    return sorted(pool, key=lambda x: x.get(key) or 0.0, reverse=reverse)[:k]


def random_k(pool: list[dict], k: int, seed: int = 42) -> list[dict]:
    rng = random.Random(seed)
    return rng.sample(pool, min(k, len(pool)))


# ── Per-date backtest ─────────────────────────────────────────────────────────

def backtest_date(game_date: str, slate: str, k: int, alpha: float) -> dict | None:
    pool = load_pool_for_date(game_date, slate)
    if not pool:
        return None

    actual_by_pid = load_actuals_for_date(game_date)
    if not actual_by_pid:
        return None

    # Check that at least some pool lineups can be scored
    scored_count = sum(1 for lu in pool[:100]
                       if any(actual_by_pid.get(pid) for pid in (lu.get('player_ids') or [])))
    if scored_count < 5:
        return None

    all_pids = list({p for lu in pool for p in (lu.get('player_ids') or [])})
    player_quality = load_player_quality_for_date(game_date, all_pids)

    # Pool oracle: best possible lineup score
    pool_best = max(score_lineup(lu, actual_by_pid) for lu in pool)
    pool_avg  = np.mean([score_lineup(lu, actual_by_pid) for lu in pool])

    # Run all strategies
    port_picks = greedy_portfolio(pool, player_quality, k, alpha)
    gpp_picks  = top_k_by(pool, 'gpp_score', k)
    proj_picks = top_k_by(pool, 'proj', k)
    rand_picks = random_k(pool, k)

    results = {
        'date':       game_date,
        'slate':      slate,
        'pool_size':  len(pool),
        'pool_best':  round(pool_best, 1),
        'pool_avg':   round(pool_avg, 1),
        'portfolio':  score_strategy(port_picks, actual_by_pid, player_quality, pool),
        'gpp_score':  score_strategy(gpp_picks,  actual_by_pid, player_quality, pool),
        'projection': score_strategy(proj_picks, actual_by_pid, player_quality, pool),
        'random':     score_strategy(rand_picks, actual_by_pid, player_quality, pool),
    }
    return results


# ── Reporting ─────────────────────────────────────────────────────────────────

def print_summary(all_results: list[dict], k: int) -> None:
    strategies = ['portfolio', 'gpp_score', 'projection', 'random']
    labels     = ['Portfolio', 'GPP Score', 'Projection', 'Random  ']

    print(f"\n{'='*72}")
    print(f"  Portfolio Backtest — {len(all_results)} slate-days, K={k}")
    print(f"{'='*72}")
    print(f"  {'Strategy':<12} {'MaxActual':>10} {'AvgActual':>10} "
          f"{'GPP_hits':>9} {'Cash_hits':>10} {'AvgOverlap':>11} {'BestRank':>9}")
    print(f"  {'-'*70}")

    for strat, label in zip(strategies, labels):
        vals = [r[strat] for r in all_results if r.get(strat)]
        if not vals:
            continue
        avg_max   = np.mean([v['max_actual'] for v in vals])
        avg_avg   = np.mean([v['avg_actual'] for v in vals])
        avg_gpp   = np.mean([v['gpp_count'] for v in vals])
        avg_cash  = np.mean([v['cash_count'] for v in vals])
        avg_ovlp  = np.mean([v['avg_overlap'] for v in vals])
        avg_rank  = np.mean([v['best_rank'] for v in vals])
        print(f"  {label:<12} {avg_max:>10.1f} {avg_avg:>10.1f} "
              f"{avg_gpp:>9.2f} {avg_cash:>10.2f} {avg_ovlp:>10.1f}% {avg_rank:>9.1f}")

    print(f"\n  {'Pool oracle (best possible):':<30} "
          f"avg max={np.mean([r['pool_best'] for r in all_results]):.1f}  "
          f"avg pool_avg={np.mean([r['pool_avg'] for r in all_results]):.1f}")

    # Per-date breakdown
    print(f"\n  Per-date breakdown (max_actual per strategy):")
    print(f"  {'Date':<12} {'Slate':<8} {'Port':>6} {'GPP':>6} {'Proj':>6} "
          f"{'Rand':>6} {'Oracle':>8} {'PoolSz':>7}")
    print(f"  {'-'*65}")
    for r in sorted(all_results, key=lambda x: x['date']):
        port = r['portfolio'].get('max_actual', 0) if r.get('portfolio') else 0
        gpp  = r['gpp_score'].get('max_actual', 0) if r.get('gpp_score') else 0
        proj = r['projection'].get('max_actual', 0) if r.get('projection') else 0
        rand = r['random'].get('max_actual', 0) if r.get('random') else 0
        # Highlight winner per row
        best_s = max(port, gpp, proj)
        pf = f'*{port:.0f}' if port == best_s else f' {port:.0f}'
        gf = f'*{gpp:.0f}' if gpp == best_s else f' {gpp:.0f}'
        prf = f'*{proj:.0f}' if proj == best_s else f' {proj:.0f}'
        print(f"  {r['date']:<12} {r['slate']:<8} {pf:>6} {gf:>6} {prf:>6} "
              f"{rand:>6.0f} {r['pool_best']:>8.0f} {r['pool_size']:>7,}")

    # Win count per strategy (how many dates each strategy had the highest max_actual)
    wins = {s: 0 for s in strategies[:-1]}  # exclude random
    for r in all_results:
        vals_s = {s: r[s].get('max_actual', 0) if r.get(s) else 0 for s in strategies[:-1]}
        best_val = max(vals_s.values())
        for s, v in vals_s.items():
            if v == best_val:
                wins[s] += 1
    total = len(all_results)
    print(f"\n  Wins (highest max_actual): "
          + ' | '.join(f'{labels[i].strip()}={wins[s]}/{total}' for i, s in enumerate(strategies[:-1])))


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description='Portfolio Optimizer Backtest')
    parser.add_argument('--start',   default='2026-04-13', help='Start date YYYY-MM-DD')
    parser.add_argument('--end',     default=str(date.today() - timedelta(days=1)),
                        help='End date YYYY-MM-DD (default: yesterday)')
    parser.add_argument('--slate',   default='main', help='Slate to test (main/early/turbo/all)')
    parser.add_argument('--k',       type=int, default=20, help='Portfolio size K')
    parser.add_argument('--contest', default='large',
                        choices=['large', 'mid', 'small', 'single'],
                        help='Contest type for coverage reward strength')
    args = parser.parse_args()

    alpha = COVERAGE_ALPHA[args.contest]
    print(f"\nPortfolio Backtest")
    print(f"  Range: {args.start} to {args.end}  |  Slate: {args.slate}  |  "
          f"K={args.k}  |  contest={args.contest} (alpha={alpha:.2f})")
    print(f"  GPP line: {GPP_LINE}  |  Cash line: {CASH_LINE}")

    # Build list of dates
    start = date.fromisoformat(args.start)
    end   = date.fromisoformat(args.end)
    dates = []
    d = start
    while d <= end:
        dates.append(str(d))
        d += timedelta(days=1)

    all_results = []
    for game_date in dates:
        slates = ['main', 'early', 'turbo', 'night'] if args.slate == 'all' else [args.slate]
        for slate in slates:
            print(f"  {game_date} / {slate} ...", end=' ', flush=True)
            result = backtest_date(game_date, slate, args.k, alpha)
            if result is None:
                print("no data")
                continue
            all_results.append(result)
            port_max = result['portfolio'].get('max_actual', 0) if result.get('portfolio') else 0
            gpp_max  = result['gpp_score'].get('max_actual', 0) if result.get('gpp_score') else 0
            print(f"pool={result['pool_size']:,}  port={port_max:.0f}  gpp={gpp_max:.0f}  oracle={result['pool_best']:.0f}")

    if not all_results:
        print("\nNo data found for the specified range. Check that actuals are loaded.")
        sys.exit(1)

    print_summary(all_results, args.k)


if __name__ == '__main__':
    main()
