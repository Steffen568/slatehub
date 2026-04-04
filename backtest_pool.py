#!/usr/bin/env python3
import sys
sys.stdout.reconfigure(encoding='utf-8', line_buffering=True)
"""
Backtest Pool — Reverse-engineer optimal pool filters

For each day with both sim_pool and actual_results data:
1. Score every pool lineup using actual DK points
2. Identify top-performing lineups (top 1%, top 5%, cash line, winner)
3. Compare characteristics of top lineups vs the full pool
4. Output optimal filter thresholds

Run: py -3.12 backtest_pool.py
"""

import os
import numpy as np
from collections import Counter
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()
sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])


def paginate(table, select, filters=None, limit=5000):
    """Paginate a Supabase query to get all rows."""
    all_rows = []
    offset = 0
    while True:
        q = sb.table(table).select(select).range(offset, offset + 999)
        if filters:
            for col, val in filters:
                q = q.eq(col, val)
        res = q.execute()
        if not res.data:
            break
        all_rows.extend(res.data)
        if len(res.data) < 1000:
            break
        offset += 1000
    return all_rows


def run():
    print("Backtest Pool — Optimal Filter Analysis")
    print("=" * 60)

    # Find overlapping dates
    pool_dates_raw = paginate('sim_pool', 'game_date, dk_slate, pool_type')
    pool_dates = {}
    for r in pool_dates_raw:
        key = (r['game_date'], r['dk_slate'])
        pool_dates.setdefault(key, set()).add(r['pool_type'])

    actual_dates_raw = paginate('actual_results', 'game_date')
    actual_dates = set(r['game_date'] for r in actual_dates_raw)

    # Only main slates with user pools that have actuals
    testable = []
    for (d, s), types in sorted(pool_dates.items()):
        if d in actual_dates and 'user' in types and s == 'main':
            testable.append((d, s))

    print(f"  Pool dates: {len(pool_dates)} slate-days")
    print(f"  Actual dates: {sorted(actual_dates)}")
    print(f"  Testable (main + user + actuals): {testable}")

    if not testable:
        print("\n  No overlapping data to backtest — need more days.")
        return

    # Aggregate across all testable days
    all_scored = []
    all_top1 = []
    all_top5 = []
    all_cash = []

    for game_date, slate in testable:
        print(f"\n{'─'*60}")
        print(f"  {game_date} / {slate}")
        print(f"{'─'*60}")

        # Load actuals for this date
        actuals_raw = paginate('actual_results', 'player_id, actual_dk_pts',
                               [('game_date', game_date)])
        pts_map = {}
        for r in actuals_raw:
            pts_map[r['player_id']] = r['actual_dk_pts'] or 0
        print(f"  Actuals: {len(pts_map)} players")

        # Load user pool for this date/slate
        pool_raw = paginate('sim_pool', 'player_ids, salary, proj, stack_team, stack_size, sub_team, sub_size',
                            [('game_date', game_date), ('dk_slate', slate), ('pool_type', 'user')])
        print(f"  User pool: {len(pool_raw)} lineups")

        if not pool_raw or not pts_map:
            print("  Skipping — no data")
            continue

        # Score each lineup
        scored = []
        for lu in pool_raw:
            pids = lu['player_ids']
            if not pids:
                continue
            actual_total = sum(pts_map.get(pid, 0) for pid in pids)
            scored.append({
                'actual': actual_total,
                'proj': lu['proj'] or 0,
                'salary': lu['salary'] or 0,
                'stack_team': lu['stack_team'],
                'stack_size': lu['stack_size'] or 0,
                'sub_team': lu['sub_team'],
                'sub_size': lu['sub_size'] or 0,
                'player_ids': pids,
            })

        scored.sort(key=lambda x: x['actual'], reverse=True)
        n = len(scored)
        actuals_arr = [s['actual'] for s in scored]

        # Thresholds
        top1_cutoff = np.percentile(actuals_arr, 99)
        top5_cutoff = np.percentile(actuals_arr, 95)
        cash_cutoff = np.percentile(actuals_arr, 50)

        top1 = [s for s in scored if s['actual'] >= top1_cutoff]
        top5 = [s for s in scored if s['actual'] >= top5_cutoff]
        cash = [s for s in scored if s['actual'] >= cash_cutoff]

        print(f"\n  Score distribution:")
        print(f"    Mean: {np.mean(actuals_arr):.1f}")
        print(f"    P50 (cash): {cash_cutoff:.1f}")
        print(f"    P95 (top 5%): {top5_cutoff:.1f}")
        print(f"    P99 (top 1%): {top1_cutoff:.1f}")
        print(f"    Max (winner): {max(actuals_arr):.1f}")

        all_scored.extend(scored)
        all_top1.extend(top1)
        all_top5.extend(top5)
        all_cash.extend(cash)

    if not all_scored:
        print("\n  No scored lineups — need more data.")
        return

    # ── Aggregate Analysis ──────────────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"  AGGREGATE ANALYSIS ({len(testable)} days, {len(all_scored)} lineups)")
    print(f"{'='*60}")

    def analyze_group(label, group, full):
        """Compare a group of lineups to the full pool."""
        if not group:
            print(f"\n  {label}: no lineups")
            return

        print(f"\n  {label} ({len(group)} lineups):")
        print(f"    Actual pts:  {np.mean([s['actual'] for s in group]):.1f} avg  "
              f"(pool avg: {np.mean([s['actual'] for s in full]):.1f})")

        # Projection
        g_proj = [s['proj'] for s in group]
        f_proj = [s['proj'] for s in full]
        print(f"    Projection:  {np.mean(g_proj):.1f} avg  (pool: {np.mean(f_proj):.1f})  "
              f"min={np.min(g_proj):.1f}  P25={np.percentile(g_proj,25):.1f}  P50={np.median(g_proj):.1f}")

        # Salary
        g_sal = [s['salary'] for s in group]
        f_sal = [s['salary'] for s in full]
        print(f"    Salary:      ${np.mean(g_sal):,.0f} avg  (pool: ${np.mean(f_sal):,.0f})  "
              f"min=${np.min(g_sal):,}  P25=${np.percentile(g_sal,25):,.0f}")

        # Stack size
        g_stack = [s['stack_size'] for s in group]
        f_stack = [s['stack_size'] for s in full]
        stack_dist_g = Counter(g_stack)
        stack_dist_f = Counter(f_stack)
        print(f"    Stack size:  {np.mean(g_stack):.1f} avg  (pool: {np.mean(f_stack):.1f})")
        print(f"      Winners:   {dict(sorted(stack_dist_g.items()))}")
        print(f"      Pool:      {dict(sorted(stack_dist_f.items()))}")

        # Sub stack
        g_sub = [s['sub_size'] for s in group]
        f_sub = [s['sub_size'] for s in full]
        sub_dist_g = Counter(g_sub)
        print(f"    Sub size:    {np.mean(g_sub):.1f} avg  (pool: {np.mean(f_sub):.1f})")
        print(f"      Winners:   {dict(sorted(sub_dist_g.items()))}")

        # Stack config (main-sub)
        g_configs = Counter(f"{s['stack_size']}-{s['sub_size']}" for s in group)
        f_configs = Counter(f"{s['stack_size']}-{s['sub_size']}" for s in full)
        print(f"    Stack configs (winners):")
        for cfg, cnt in g_configs.most_common(5):
            pct_g = cnt / len(group) * 100
            pct_f = f_configs.get(cfg, 0) / len(full) * 100
            print(f"      {cfg:6s}  {pct_g:5.1f}% of winners  (vs {pct_f:.1f}% of pool)")

        # Top stack teams
        g_teams = Counter(s['stack_team'] for s in group)
        print(f"    Top stack teams:")
        for team, cnt in g_teams.most_common(5):
            pct = cnt / len(group) * 100
            print(f"      {team:5s}  {pct:.1f}%")

    analyze_group("TOP 1% LINEUPS", all_top1, all_scored)
    analyze_group("TOP 5% LINEUPS", all_top5, all_scored)
    analyze_group("CASH LINEUPS (top 50%)", all_cash, all_scored)

    # ── Optimal Filter Recommendations ──────────────────────────────────
    print(f"\n{'='*60}")
    print(f"  RECOMMENDED FILTERS")
    print(f"{'='*60}")

    if all_top1:
        t1_proj = [s['proj'] for s in all_top1]
        t1_sal = [s['salary'] for s in all_top1]
        t1_stack = Counter(f"{s['stack_size']}-{s['sub_size']}" for s in all_top1)
        best_config = t1_stack.most_common(1)[0][0] if t1_stack else "?"

        print(f"\n  For GPP (top 1% targeting):")
        print(f"    Min projection:  {np.percentile(t1_proj, 10):.1f} pts  (P10 of winners)")
        print(f"    Min salary:      ${np.percentile(t1_sal, 10):,.0f}  (P10 of winners)")
        print(f"    Best stack cfg:  {best_config}")
        print(f"    Avg stack size:  {np.mean([s['stack_size'] for s in all_top1]):.1f}")

    if all_top5:
        t5_proj = [s['proj'] for s in all_top5]
        t5_sal = [s['salary'] for s in all_top5]
        print(f"\n  For top 5%:")
        print(f"    Min projection:  {np.percentile(t5_proj, 10):.1f} pts")
        print(f"    Min salary:      ${np.percentile(t5_sal, 10):,.0f}")

    # ── Projection accuracy at lineup level ─────────────────────────────
    projs = np.array([s['proj'] for s in all_scored])
    acts = np.array([s['actual'] for s in all_scored])
    corr = np.corrcoef(projs, acts)[0, 1]
    print(f"\n  Lineup-level projection correlation: r={corr:.3f}")
    print(f"  Projection bias: {np.mean(projs - acts):+.1f} pts (positive = over-projected)")

    print(f"\n  NOTE: Only {len(testable)} days of data. Run daily — patterns stabilize after 2+ weeks.")


if __name__ == "__main__":
    run()
