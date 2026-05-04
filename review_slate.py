#!/usr/bin/env python3
import sys
sys.stdout.reconfigure(encoding='utf-8', line_buffering=True)
"""
Slate Review — Post-game diagnostic for lineup pool performance

Scores every sim_pool lineup against actual DK points and produces a
6-section report diagnosing what went right, wrong, and whether
different sort strategies would have helped.

Section 6 (Ownership Accuracy) runs automatically if actual ownership
has been loaded. To include it, run after games lock:

  py -3.12 load_actual_ownership.py --csv contest-standings-XXXXXX.csv --date YYYY-MM-DD
  py -3.12 review_slate.py --date YYYY-MM-DD

Run:
  py -3.12 review_slate.py                           # latest date with actuals
  py -3.12 review_slate.py --date 2026-04-13          # specific date
  py -3.12 review_slate.py --range 2026-04-12 2026-04-13
  py -3.12 review_slate.py --top 20 --slate main
"""

import os
import argparse
import numpy as np
from collections import Counter, defaultdict
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()
sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
FINDINGS_PATH = os.path.join(SCRIPT_DIR, 'tasks', 'research_findings.md')


# ── Helpers ───────────────────────────────────────────────────────────────────

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


def pct(arr, p):
    return float(np.percentile(arr, p)) if len(arr) > 0 else 0.0


def safe(v, default=0):
    return v if v is not None else default


def cfg_label(stack_size, sub_size):
    return f"{stack_size}-{sub_size}" if sub_size else f"{stack_size}-naked"


# ── Main ──────────────────────────────────────────────────────────────────────

def review(dates, slate_filter=None, top_n=20):
    print("\n  Slate Review — Post-Game Lineup Diagnostic")
    print(f"  {'='*56}")

    for game_date in dates:
        # ── Load data ─────────────────────────────────────────────────
        actuals_raw = paginate('actual_results',
            'player_id, full_name, team, actual_dk_pts, is_pitcher',
            [('game_date', game_date)])
        pts_map = {r['player_id']: safe(r['actual_dk_pts']) for r in actuals_raw}
        name_map = {r['player_id']: r['full_name'] for r in actuals_raw}
        team_map = {r['player_id']: r['team'] for r in actuals_raw}
        pitcher_set = {r['player_id'] for r in actuals_raw if r.get('is_pitcher')}

        if not pts_map:
            print(f"\n  {game_date}: No actual results found — skipping")
            continue

        # Load projections for player-level analysis
        proj_raw = paginate('player_projections',
            'player_id, full_name, proj_dk_pts, batting_order, is_pitcher',
            [('game_date', game_date)])
        proj_map = {r['player_id']: r for r in proj_raw}
        for r in proj_raw:
            if r['player_id'] not in name_map and r.get('full_name'):
                name_map[r['player_id']] = r['full_name']

        # Load salary data for position info
        sal_raw = paginate('dk_salaries', 'player_id, salary, position, name')
        sal_map = {r['player_id']: r for r in sal_raw}

        # Find available slates for this date
        slate_meta = paginate('sim_pool', 'dk_slate, pool_type',
                              [('game_date', game_date)])
        available_slates = set()
        for r in slate_meta:
            if r['pool_type'] == 'user':
                available_slates.add(r['dk_slate'])

        if slate_filter:
            slates = [s for s in available_slates if s == slate_filter]
        else:
            slates = sorted(available_slates)

        if not slates:
            print(f"\n  {game_date}: No user pool found — skipping")
            continue

        for slate in slates:
            try:
                pool_raw = paginate('sim_pool',
                    'player_ids, salary, proj, stack_team, stack_size, sub_team, sub_size, avg_pms, avg_hes',
                    [('game_date', game_date), ('dk_slate', slate), ('pool_type', 'user')])
            except Exception:
                # avg_pms/avg_hes columns may not exist yet
                pool_raw = paginate('sim_pool',
                    'player_ids, salary, proj, stack_team, stack_size, sub_team, sub_size',
                    [('game_date', game_date), ('dk_slate', slate), ('pool_type', 'user')])

            if not pool_raw:
                print(f"\n  {game_date} / {slate}: Empty pool — skipping")
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
                    'proj': safe(lu['proj']),
                    'salary': safe(lu['salary']),
                    'stack_team': lu['stack_team'] or '',
                    'stack_size': safe(lu['stack_size']),
                    'sub_team': lu['sub_team'] or '',
                    'sub_size': safe(lu['sub_size']),
                    'avg_pms': lu.get('avg_pms'),
                    'avg_hes': lu.get('avg_hes'),
                    'player_ids': pids,
                })

            if not scored:
                continue

            print(f"\n\n  {'─'*56}")
            print(f"  {game_date} / {slate}  ({len(scored)} lineups)")
            print(f"  {'─'*56}")

            # ══════════════════════════════════════════════════════════
            # SECTION 1: Pool Scorecard
            # ══════════════════════════════════════════════════════════
            scored.sort(key=lambda x: x['actual'], reverse=True)
            actuals_arr = np.array([s['actual'] for s in scored])
            projs_arr = np.array([s['proj'] for s in scored])

            print(f"\n  SECTION 1: Pool Scorecard")
            print(f"  {'═'*52}")
            print(f"    Lineups scored: {len(scored)}")
            print(f"    Players with actuals: {len(pts_map)}")
            print()
            print(f"    Actual Score Distribution:")
            print(f"      Mean:   {np.mean(actuals_arr):6.1f}")
            print(f"      P25:    {pct(actuals_arr, 25):6.1f}")
            print(f"      P50:    {pct(actuals_arr, 50):6.1f}  (cash line)")
            print(f"      P75:    {pct(actuals_arr, 75):6.1f}")
            print(f"      P90:    {pct(actuals_arr, 90):6.1f}")
            print(f"      P99:    {pct(actuals_arr, 99):6.1f}  (GPP line)")
            print(f"      Max:    {np.max(actuals_arr):6.1f}  (best lineup)")
            print(f"      Min:    {np.min(actuals_arr):6.1f}  (worst lineup)")

            # Projection accuracy
            corr = np.corrcoef(projs_arr, actuals_arr)[0, 1] if len(scored) > 2 else 0
            bias = float(np.mean(projs_arr - actuals_arr))
            mae = float(np.mean(np.abs(projs_arr - actuals_arr)))
            print(f"\n    Projection Accuracy (lineup-level):")
            print(f"      Correlation (r):  {corr:+.3f}  {'(signal)' if corr > 0.15 else '(weak/none)' if corr > 0 else '(NEGATIVE — projections hurt)'}")
            print(f"      MAE:              {mae:.1f} pts")
            print(f"      Bias:             {bias:+.1f} pts  {'(over-projecting)' if bias > 0 else '(under-projecting)'}")
            print(f"      Avg projected:    {np.mean(projs_arr):.1f}")
            print(f"      Avg actual:       {np.mean(actuals_arr):.1f}")

            # ══════════════════════════════════════════════════════════
            # SECTION 2: Top N vs Bottom N
            # ══════════════════════════════════════════════════════════
            print(f"\n  SECTION 2: Top {top_n} vs Bottom {top_n}")
            print(f"  {'═'*52}")

            top = scored[:top_n]
            bot = scored[-top_n:]

            def group_stats(group, label):
                if not group:
                    return
                acts = [s['actual'] for s in group]
                prjs = [s['proj'] for s in group]
                sals = [s['salary'] for s in group]
                cfgs = Counter(cfg_label(s['stack_size'], s['sub_size']) for s in group)
                teams = Counter(s['stack_team'] for s in group)
                print(f"\n    {label} ({len(group)} lineups):")
                print(f"      Avg actual:     {np.mean(acts):6.1f}")
                print(f"      Avg projected:  {np.mean(prjs):6.1f}")
                print(f"      Avg salary:     ${np.mean(sals):,.0f}")
                print(f"      Stack configs:  {dict(cfgs.most_common(5))}")
                print(f"      Top teams:      {dict(teams.most_common(5))}")

            group_stats(top, f"WINNERS (top {top_n} by actual)")
            group_stats(bot, f"LOSERS (bottom {top_n} by actual)")

            # Overlap: top N by projection vs top N by actual
            proj_sorted = sorted(scored, key=lambda x: x['proj'], reverse=True)
            top_by_proj_ids = set(id(s) for s in proj_sorted[:top_n])
            top_by_actual_ids = set(id(s) for s in top[:top_n])
            overlap = len(top_by_proj_ids & top_by_actual_ids)
            print(f"\n    Projection rank overlap:")
            print(f"      Top {top_n} by PROJ vs top {top_n} by ACTUAL: {overlap}/{top_n} overlap")
            print(f"      {'Projections are helping pick winners' if overlap >= top_n * 0.3 else 'Projections are NOT selecting winners — sort strategy may need change'}")

            # ══════════════════════════════════════════════════════════
            # SECTION 3: Player-Level Hits & Misses
            # ══════════════════════════════════════════════════════════
            print(f"\n  SECTION 3: Player-Level Hits & Misses")
            print(f"  {'═'*52}")

            # Count exposure per player and compute deltas
            player_exposure = Counter()
            for s in scored:
                for pid in s['player_ids']:
                    player_exposure[pid] += 1

            total_lineups = len(scored)
            player_data = []
            for pid, count in player_exposure.items():
                proj_info = proj_map.get(pid, {})
                proj_pts = safe(proj_info.get('proj_dk_pts'))
                actual_pts = pts_map.get(pid, 0)
                delta = actual_pts - proj_pts if proj_pts else 0
                exposure_pct = count / total_lineups * 100
                sal_info = sal_map.get(pid, {})
                player_data.append({
                    'pid': pid,
                    'name': name_map.get(pid, f'ID:{pid}'),
                    'team': team_map.get(pid, ''),
                    'pos': sal_info.get('position', ''),
                    'proj': proj_pts,
                    'actual': actual_pts,
                    'delta': delta,
                    'exposure': exposure_pct,
                    'count': count,
                    'is_pitcher': pid in pitcher_set or proj_info.get('is_pitcher', False),
                    # Impact: how much this player's delta cost/helped the portfolio
                    # exposure * delta = total point impact across all lineups using them
                    'impact': delta * count,
                })

            # Biggest misses: high exposure + big negative delta
            misses = sorted([p for p in player_data if p['exposure'] >= 5 and p['delta'] < -2],
                            key=lambda p: p['impact'])
            print(f"\n    BIGGEST MISSES (high exposure, under-performed):")
            if misses:
                for p in misses[:8]:
                    tag = 'SP' if p['is_pitcher'] else p['pos']
                    print(f"      {p['name']:22s} {tag:4s} {p['team']:4s}  "
                          f"proj={p['proj']:5.1f}  actual={p['actual']:5.1f}  "
                          f"delta={p['delta']:+5.1f}  exp={p['exposure']:4.1f}%  "
                          f"impact={p['impact']:+.0f} pts")
            else:
                print(f"      (none with >5% exposure and >2pt miss)")

            # Missed opportunities: low exposure + big positive delta
            opps = sorted([p for p in player_data if p['exposure'] < 15 and p['actual'] >= 15],
                          key=lambda p: -p['actual'])
            print(f"\n    MISSED OPPORTUNITIES (low exposure, crushed it):")
            if opps:
                for p in opps[:8]:
                    tag = 'SP' if p['is_pitcher'] else p['pos']
                    print(f"      {p['name']:22s} {tag:4s} {p['team']:4s}  "
                          f"proj={p['proj']:5.1f}  actual={p['actual']:5.1f}  "
                          f"delta={p['delta']:+5.1f}  exp={p['exposure']:4.1f}%")
            else:
                print(f"      (no low-exposure players scored 15+)")

            # Best calls: high exposure + positive delta
            calls = sorted([p for p in player_data if p['exposure'] >= 15 and p['delta'] > 2],
                           key=lambda p: -p['impact'])
            print(f"\n    BEST CALLS (high exposure, delivered):")
            if calls:
                for p in calls[:8]:
                    tag = 'SP' if p['is_pitcher'] else p['pos']
                    print(f"      {p['name']:22s} {tag:4s} {p['team']:4s}  "
                          f"proj={p['proj']:5.1f}  actual={p['actual']:5.1f}  "
                          f"delta={p['delta']:+5.1f}  exp={p['exposure']:4.1f}%  "
                          f"impact={p['impact']:+.0f} pts")
            else:
                print(f"      (no high-exposure players beat projections by >2)")

            # ══════════════════════════════════════════════════════════
            # SECTION 4: Stack Autopsy
            # ══════════════════════════════════════════════════════════
            print(f"\n  SECTION 4: Stack Autopsy")
            print(f"  {'═'*52}")

            # Group lineups by stack team
            stack_groups = defaultdict(list)
            for s in scored:
                if s['stack_team']:
                    stack_groups[s['stack_team']].append(s)

            stack_summary = []
            for team, lineups in stack_groups.items():
                acts = [l['actual'] for l in lineups]
                prjs = [l['proj'] for l in lineups]
                stack_summary.append({
                    'team': team,
                    'count': len(lineups),
                    'pct': len(lineups) / total_lineups * 100,
                    'avg_actual': np.mean(acts),
                    'avg_proj': np.mean(prjs),
                    'best': max(acts),
                    'worst': min(acts),
                })

            stack_summary.sort(key=lambda x: -x['avg_actual'])
            print(f"\n    Stack Performance (sorted by avg actual):")
            print(f"    {'Team':5s} {'Count':>6s} {'Exp%':>6s} {'AvgProj':>8s} {'AvgAct':>8s} {'Best':>6s} {'Worst':>6s}")
            print(f"    {'─'*48}")
            for st in stack_summary:
                marker = ' <<' if st == stack_summary[0] else ''
                print(f"    {st['team']:5s} {st['count']:6d} {st['pct']:5.1f}% "
                      f"{st['avg_proj']:8.1f} {st['avg_actual']:8.1f} "
                      f"{st['best']:6.1f} {st['worst']:6.1f}{marker}")

            # Best actual team — scoped to teams that appear in this slate's pool
            slate_teams = set()
            for s in scored:
                for pid in s['player_ids']:
                    t = team_map.get(pid, '')
                    if t:
                        slate_teams.add(t)

            team_actuals = defaultdict(list)
            for pid, pts in pts_map.items():
                team = team_map.get(pid, '')
                if team and team in slate_teams and pid not in pitcher_set:
                    team_actuals[team].append(pts)

            print(f"\n    Actual Team Performance (hitter totals, top 5 on this slate):")
            team_totals = [(t, sum(pts), np.mean(pts), len(pts))
                           for t, pts in team_actuals.items() if len(pts) >= 4]
            team_totals.sort(key=lambda x: -x[1])
            for t, total, avg, ct in team_totals[:5]:
                in_pool = t in stack_groups
                pool_exp = sum(1 for s in scored if s['stack_team'] == t) / total_lineups * 100
                print(f"      {t:5s}  total={total:5.1f}  avg={avg:4.1f}  hitters={ct}  "
                      f"pool exp={pool_exp:4.1f}%  {'IN POOL' if in_pool else 'NOT STACKED'}")

            # ══════════════════════════════════════════════════════════
            # SECTION 5: Sort Strategy What-If
            # ══════════════════════════════════════════════════════════
            print(f"\n  SECTION 5: Sort Strategy What-If")
            print(f"  {'═'*52}")
            print(f"    'If I had picked my top {top_n} lineups by [X], what would they have scored?'\n")

            strategies = {
                'Projection (highest proj)': lambda s: -s['proj'],
                'Salary (highest salary)': lambda s: -s['salary'],
                'Value (proj/salary)': lambda s: -(s['proj'] / max(s['salary'], 1) * 1000),
                'Stack size (5-stacks first)': lambda s: (-s['stack_size'], -s['proj']),
            }

            # Add PMS/HES/Blended strategies if data available
            has_pms = any(s.get('avg_pms') is not None for s in scored)
            if has_pms:
                strategies['PMS (highest avg_pms)'] = lambda s: -(s.get('avg_pms') or 5)
                strategies['HES (highest avg_hes)'] = lambda s: -(s.get('avg_hes') or 5)
                strategies['Blended (70% proj + 30% PMS)'] = lambda s: -(
                    0.70 * (s['proj'] / max(1, np.mean(projs_arr))) +
                    0.30 * ((s.get('avg_pms') or 5) / 10)
                )

            # Random baseline (avg of 50 random samples)
            rng = np.random.default_rng(42)
            random_avgs = []
            for _ in range(50):
                sample = rng.choice(len(scored), size=min(top_n, len(scored)), replace=False)
                random_avgs.append(np.mean([scored[i]['actual'] for i in sample]))
            random_baseline = np.mean(random_avgs)

            print(f"    {'Strategy':35s} {'Top-N Avg Actual':>16s} {'vs Random':>10s} {'vs Pool Avg':>12s}")
            print(f"    {'─'*76}")

            pool_avg = float(np.mean(actuals_arr))
            best_strategy = None
            best_avg = -999

            for name, key_fn in strategies.items():
                sorted_by = sorted(scored, key=key_fn)
                top_group = sorted_by[:top_n]
                avg_actual = np.mean([s['actual'] for s in top_group])
                vs_random = avg_actual - random_baseline
                vs_pool = avg_actual - pool_avg
                print(f"    {name:35s} {avg_actual:16.1f} {vs_random:+10.1f} {vs_pool:+12.1f}")
                if avg_actual > best_avg:
                    best_avg = avg_actual
                    best_strategy = name

            print(f"    {'Random baseline':35s} {random_baseline:16.1f} {'---':>10s} {random_baseline - pool_avg:+12.1f}")
            print(f"    {'Pool average':35s} {pool_avg:16.1f} {'---':>10s} {'---':>12s}")

            # Rank correlation: does higher proj = higher actual?
            proj_ranks = np.argsort(np.argsort(-projs_arr))  # rank by proj (0 = best)
            actual_ranks = np.argsort(np.argsort(-actuals_arr))
            rank_corr = np.corrcoef(proj_ranks, actual_ranks)[0, 1] if len(scored) > 2 else 0

            print(f"\n    Rank correlation (proj rank vs actual rank): r={rank_corr:.3f}")
            if rank_corr > 0.20:
                print(f"    >> Projections have decent signal — proj-first sort is working")
            elif rank_corr > 0.05:
                print(f"    >> Weak signal — projections help slightly but aren't decisive")
            else:
                print(f"    >> NO signal — projection rank doesn't predict actual rank")
                print(f"    >> Consider: the blended sort may be adding noise rather than signal")

            print(f"\n    Best strategy for this slate: {best_strategy}")

            # ══════════════════════════════════════════════════════════
            # CONCLUSIONS
            # ══════════════════════════════════════════════════════════
            print(f"\n  CONCLUSIONS")
            print(f"  {'═'*52}")

            # Was this a good or bad slate?
            cash_line = pct(actuals_arr, 50)
            gpp_line = pct(actuals_arr, 99)
            print(f"    >> Pool avg actual: {pool_avg:.1f} pts")
            print(f"    >> Pool cash line (P50): {cash_line:.1f} pts — "
                  f"{'about half your lineups cashed' if pool_avg >= cash_line * 0.95 else 'most lineups missed cash'}")
            print(f"    >> Pool GPP line (P99): {gpp_line:.1f} pts — "
                  f"best lineup scored {np.max(actuals_arr):.1f}")

            # Was the sort helping?
            if overlap >= top_n * 0.3:
                print(f"    >> Projection ranking picked {overlap}/{top_n} actual winners — sort is working")
            elif overlap >= top_n * 0.15:
                print(f"    >> Projection ranking picked {overlap}/{top_n} actual winners — marginal signal")
            else:
                print(f"    >> Projection ranking picked {overlap}/{top_n} actual winners — SORT IS NOT HELPING")
                print(f"    >> Your top picks by projection were NOT the best actual lineups")

            # Impact of biggest misses
            if misses:
                total_miss_impact = sum(p['impact'] for p in misses[:5])
                worst_miss = misses[0]
                print(f"    >> Biggest bust: {worst_miss['name']} (proj={worst_miss['proj']:.1f}, "
                      f"actual={worst_miss['actual']:.1f}, {worst_miss['exposure']:.0f}% exposure)")
                print(f"    >> Top 5 busts cost {total_miss_impact:+.0f} points across portfolio")

            # Did we miss the winning stack?
            if team_totals and stack_summary:
                actual_best_team = team_totals[0][0]
                pool_best_team = stack_summary[0]['team']
                if actual_best_team != pool_best_team:
                    pool_exp_of_winner = sum(1 for s in scored if s['stack_team'] == actual_best_team) / total_lineups * 100
                    print(f"    >> Winning stack was {actual_best_team} but pool was heaviest on "
                          f"{pool_best_team} — {actual_best_team} had only {pool_exp_of_winner:.1f}% exposure")
                else:
                    print(f"    >> Correctly identified {actual_best_team} as the best stack")

            # ══════════════════════════════════════════════════════════
            # SECTION 6: Ownership Accuracy (if actual ownership loaded)
            # ══════════════════════════════════════════════════════════
            own_stats = None
            try:
                # Load actual ownership for this date/slate from actual_ownership table
                # (populated by: py -3.12 load_actual_ownership.py --csv <file> --date <date>)
                actual_own_rows = sb.table('actual_ownership').select(
                    'player_id, dk_name, ownership_pct, position'
                ).eq('game_date', game_date).eq('contest_type', 'classic').execute().data or []

                if actual_own_rows:
                    # Deduplicate — keep highest ownership_pct per player_id
                    actual_own_map = {}
                    for r in actual_own_rows:
                        pid = r['player_id']
                        pct_val = r.get('ownership_pct') or 0.0
                        if pid and (pid not in actual_own_map or pct_val > actual_own_map[pid]['pct']):
                            actual_own_map[pid] = {
                                'pct': pct_val,
                                'name': r.get('dk_name', ''),
                                'pos': r.get('position', ''),
                            }

                    # Load projected ownership for this date/slate from slate_ownership
                    proj_own_rows = sb.table('slate_ownership').select(
                        'player_id, proj_ownership'
                    ).eq('game_date', game_date).eq('dk_slate', slate).execute().data or []
                    proj_own_map = {r['player_id']: r['proj_ownership'] for r in proj_own_rows
                                    if r.get('proj_ownership') is not None}

                    # Match on player_id
                    own_matched = []
                    for pid, arow in actual_own_map.items():
                        if pid in proj_own_map:
                            actual_pct = arow['pct']
                            proj_pct = proj_own_map[pid]
                            own_matched.append({
                                'pid': pid,
                                'name': arow['name'],
                                'pos': arow['pos'],
                                'actual': actual_pct,
                                'proj': proj_pct,
                                'delta': proj_pct - actual_pct,
                            })

                    if len(own_matched) >= 5:
                        a_vals = [m['actual'] for m in own_matched]
                        p_vals = [m['proj'] for m in own_matched]
                        own_bias = float(np.mean([m['delta'] for m in own_matched]))
                        own_mae = float(np.mean([abs(m['delta']) for m in own_matched]))
                        own_r = float(np.corrcoef(p_vals, a_vals)[0, 1]) if len(own_matched) > 2 else 0.0

                        # Tier breakdown
                        chalk = [m for m in own_matched if m['actual'] > 20]
                        mid = [m for m in own_matched if 5 <= m['actual'] <= 20]
                        low = [m for m in own_matched if m['actual'] < 5 and m['actual'] > 0]

                        print(f"\n  SECTION 6: Ownership Accuracy ({len(own_matched)} matched players)")
                        print(f"  {'═'*52}")
                        print(f"    Overall: r={own_r:.3f}  bias={own_bias:+.2f}%  MAE={own_mae:.2f}%")
                        for label, grp in [('Chalk >20%', chalk), ('Mid 5-20%', mid), ('Low <5%', low)]:
                            if grp:
                                gb = float(np.mean([m['delta'] for m in grp]))
                                gm = float(np.mean([abs(m['delta']) for m in grp]))
                                print(f"    {label:<12}: n={len(grp):3d}  bias={gb:+.2f}%  MAE={gm:.2f}%")

                        over = sorted(own_matched, key=lambda m: m['delta'], reverse=True)[:5]
                        under = sorted(own_matched, key=lambda m: m['delta'])[:5]
                        print(f"\n    Over-projected (we too high):")
                        for m in over:
                            if m['delta'] > 1:
                                print(f"      {m['name']:<25} proj={m['proj']:5.1f}%  actual={m['actual']:5.1f}%  delta={m['delta']:+.1f}%")
                        print(f"    Under-projected (we too low):")
                        for m in under:
                            if m['delta'] < -1:
                                print(f"      {m['name']:<25} proj={m['proj']:5.1f}%  actual={m['actual']:5.1f}%  delta={m['delta']:+.1f}%")

                        own_stats = {'r': own_r, 'bias': own_bias, 'mae': own_mae, 'n': len(own_matched)}
                    else:
                        print(f"\n  SECTION 6: Ownership — insufficient matched players ({len(own_matched)})")
                        print(f"    Run: py -3.12 load_actual_ownership.py --csv <contest-standings-*.csv> --date {game_date}")
                else:
                    print(f"\n  SECTION 6: Ownership — no actual ownership data for {game_date}")
                    print(f"    Run: py -3.12 load_actual_ownership.py --csv <contest-standings-*.csv> --date {game_date}")
            except Exception as e:
                print(f"\n  SECTION 6: Ownership — error: {e}")

            # ── Write to findings MD ──────────────────────────────────
            lines = [f"\n## Slate Review — {game_date} / {slate}\n"]
            lines.append(f"- **Pool**: {len(scored)} lineups, avg actual={pool_avg:.1f}, "
                         f"cash line={cash_line:.1f}, GPP line={gpp_line:.1f}, best={np.max(actuals_arr):.1f}")
            lines.append(f"- **Proj accuracy**: r={corr:.3f}, MAE={mae:.1f}, bias={bias:+.1f}")
            lines.append(f"- **Overlap**: {overlap}/{top_n} top-by-proj were actual winners")
            lines.append(f"- **Best strategy**: {best_strategy}")
            if stack_summary:
                top_stack = stack_summary[0]
                lines.append(f"- **Top stack**: {top_stack['team']} (avg actual={top_stack['avg_actual']:.1f}, {top_stack['pct']:.1f}% exposure)")
            if misses:
                w = misses[0]
                lines.append(f"- **Biggest bust**: {w['name']} (proj={w['proj']:.1f}, actual={w['actual']:.1f}, {w['exposure']:.0f}% exp)")
            if opps:
                o = opps[0]
                lines.append(f"- **Biggest missed opp**: {o['name']} (actual={o['actual']:.1f}, {o['exposure']:.1f}% exp)")
            if own_stats:
                lines.append(f"- **Ownership accuracy**: r={own_stats['r']:.3f}, MAE={own_stats['mae']:.2f}%, bias={own_stats['bias']:+.2f}% (n={own_stats['n']})")
            lines.append("")

            with open(FINDINGS_PATH, 'a', encoding='utf-8') as f:
                f.write('\n'.join(lines))


def main():
    parser = argparse.ArgumentParser(description='Slate Review — Post-game lineup diagnostic')
    parser.add_argument('--date', type=str, help='Specific date (YYYY-MM-DD)')
    parser.add_argument('--range', type=str, nargs=2, metavar=('START', 'END'),
                        help='Date range (START END)')
    parser.add_argument('--top', type=int, default=20, help='Number of top/bottom lineups to analyze')
    parser.add_argument('--slate', type=str, default=None, help='Specific slate (main, early, etc.)')
    args = parser.parse_args()

    if args.range:
        # Generate date list
        from datetime import date, timedelta
        start = date.fromisoformat(args.range[0])
        end = date.fromisoformat(args.range[1])
        dates = []
        d = start
        while d <= end:
            dates.append(d.isoformat())
            d += timedelta(days=1)
    elif args.date:
        dates = [args.date]
    else:
        # Auto-detect latest date with actual results
        res = sb.table('actual_results').select('game_date').order('game_date', desc=True).limit(1).execute()
        if res.data:
            dates = [res.data[0]['game_date']]
            print(f"  Auto-detected latest date: {dates[0]}")
        else:
            print("  No actual results found. Run load_actuals.py first.")
            return

    review(dates, slate_filter=args.slate, top_n=args.top)


if __name__ == '__main__':
    main()
