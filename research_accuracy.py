#!/usr/bin/env python3
import sys
sys.stdout.reconfigure(encoding='utf-8', line_buffering=True)
"""
research_accuracy.py — Post-Contest Research & Analysis

Compares projections, ownership, and sim outputs against actual results.
Draws conclusions and surfaces actionable improvements.

Run:
  py -3.12 research_accuracy.py                           # latest completed date
  py -3.12 research_accuracy.py --date 2026-03-28         # specific date
  py -3.12 research_accuracy.py --range 2026-03-27 2026-03-29
  py -3.12 research_accuracy.py --date 2026-03-28 --csv "C:\\path\\to\\contest.csv"
"""

import os, math, csv, glob
from datetime import date, timedelta
from collections import defaultdict
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()
sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])

CONTEST_CSV_DIR = os.path.join(os.path.expanduser("~"), "Desktop", "WebDev", "Contest_CSVs")
FINDINGS_PATH = os.path.join(os.path.dirname(__file__), "tasks", "research_findings.md")


def safe(val, default=0):
    if val is None: return default
    try:
        f = float(val)
        return default if math.isnan(f) or math.isinf(f) else f
    except: return default


def paginate(query, page_size=1000):
    rows = []
    for offset in range(0, 50000, page_size):
        page = query.range(offset, offset + page_size - 1).execute().data or []
        rows.extend(page)
        if len(page) < page_size: break
    return rows


# ── Section A: Projection Accuracy ──────────────────────────────────────────

def analyze_projections(dates):
    print(f"\n{'='*60}")
    print(f"  SECTION A: Projection Accuracy")
    print(f"{'='*60}")

    # Load projections and actuals
    projs = paginate(sb.table('player_projections')
        .select('player_id,game_pk,game_date,full_name,team,is_pitcher,batting_order,'
                'proj_dk_pts,proj_floor,proj_ceiling,'
                'pitcher_mult,platoon_mult,context_mult,vegas_mult,park_mult,weather_mult')
        .in_('game_date', dates))

    actuals = paginate(sb.table('actual_results')
        .select('player_id,game_pk,game_date,actual_dk_pts,is_pitcher')
        .in_('game_date', dates))

    # Load salaries for salary bucket analysis
    sals = paginate(sb.table('dk_salaries')
        .select('player_id,salary,dk_slate')
        .eq('contest_type', 'classic'))
    sal_map = {s['player_id']: s.get('salary', 0) for s in sals}

    # Join on (player_id, game_pk)
    actual_map = {(r['player_id'], r['game_pk']): r for r in actuals}
    matched = []
    for p in projs:
        a = actual_map.get((p['player_id'], p['game_pk']))
        if a and p.get('proj_dk_pts') is not None and a.get('actual_dk_pts') is not None:
            matched.append({**p, 'actual': safe(a['actual_dk_pts']), 'salary': sal_map.get(p['player_id'], 0)})

    if not matched:
        print("  No matched projection/actual data found.")
        return {}

    # Overall metrics
    errors = [m['actual'] - safe(m['proj_dk_pts']) for m in matched]
    abs_errors = [abs(e) for e in errors]
    mae = sum(abs_errors) / len(abs_errors)
    rmse = math.sqrt(sum(e**2 for e in errors) / len(errors))
    mean_proj = sum(safe(m['proj_dk_pts']) for m in matched) / len(matched)
    mean_actual = sum(m['actual'] for m in matched) / len(matched)
    bias = mean_proj - mean_actual

    print(f"\n  Overall ({len(matched)} players across {len(dates)} date(s)):")
    print(f"    MAE:  {mae:.2f} DK pts")
    print(f"    RMSE: {rmse:.2f} DK pts")
    print(f"    Bias: {bias:+.2f} pts ({'over-projecting' if bias > 0 else 'under-projecting'})")

    # Hitter vs Pitcher split
    findings = {'overall_mae': mae, 'overall_rmse': rmse, 'overall_bias': bias}
    for label, filt in [('Hitters', lambda m: not m.get('is_pitcher')), ('Pitchers', lambda m: m.get('is_pitcher'))]:
        subset = [m for m in matched if filt(m)]
        if not subset: continue
        errs = [m['actual'] - safe(m['proj_dk_pts']) for m in subset]
        sub_mae = sum(abs(e) for e in errs) / len(errs)
        sub_bias = sum(errs) / len(errs)
        print(f"\n  {label} ({len(subset)}):")
        print(f"    MAE: {sub_mae:.2f}  |  Bias: {sub_bias:+.2f} pts")
        findings[f'{label.lower()}_mae'] = sub_mae
        findings[f'{label.lower()}_bias'] = sub_bias

    # By salary bucket
    print(f"\n  By Salary Bucket:")
    buckets = [(3000, 4000), (4000, 5000), (5000, 6000), (6000, 8000), (8000, 15000)]
    for lo, hi in buckets:
        subset = [m for m in matched if not m.get('is_pitcher') and lo <= m['salary'] < hi]
        if len(subset) < 5: continue
        errs = [m['actual'] - safe(m['proj_dk_pts']) for m in subset]
        sub_mae = sum(abs(e) for e in errs) / len(errs)
        sub_bias = sum(errs) / len(errs)
        print(f"    ${lo/1000:.0f}k-${hi/1000:.0f}k ({len(subset):3d}): MAE={sub_mae:.2f}  Bias={sub_bias:+.2f}")

    # By batting order
    print(f"\n  By Batting Order:")
    for bo_range, label in [((1,2,3), 'BO 1-3'), ((4,5,6), 'BO 4-6'), ((7,8,9), 'BO 7-9')]:
        subset = [m for m in matched if not m.get('is_pitcher') and m.get('batting_order') in bo_range]
        if len(subset) < 5: continue
        errs = [m['actual'] - safe(m['proj_dk_pts']) for m in subset]
        sub_mae = sum(abs(e) for e in errs) / len(errs)
        sub_bias = sum(errs) / len(errs)
        print(f"    {label} ({len(subset):3d}): MAE={sub_mae:.2f}  Bias={sub_bias:+.2f}")

    # By multiplier — which tier adds most error?
    print(f"\n  Tier Multiplier Analysis (correlation with actual over/under):")
    for mult_name in ['pitcher_mult', 'platoon_mult', 'context_mult', 'vegas_mult', 'park_mult', 'weather_mult']:
        subset = [(safe(m.get(mult_name), 1.0), m['actual'] - safe(m['proj_dk_pts'])) for m in matched
                  if m.get(mult_name) is not None and not m.get('is_pitcher')]
        if len(subset) < 10: continue
        # Simple correlation
        mx = sum(x for x, _ in subset) / len(subset)
        my = sum(y for _, y in subset) / len(subset)
        num = sum((x - mx) * (y - my) for x, y in subset)
        dx = math.sqrt(sum((x - mx)**2 for x, _ in subset))
        dy = math.sqrt(sum((y - my)**2 for _, y in subset))
        corr = num / (dx * dy) if dx > 0 and dy > 0 else 0
        direction = "boosts help" if corr > 0.05 else "boosts hurt" if corr < -0.05 else "neutral"
        print(f"    {mult_name:16s}: r={corr:+.3f}  ({direction})")
        findings.setdefault('tier_correlations', {})[mult_name] = corr

    # Top misses
    sorted_by_error = sorted(matched, key=lambda m: m['actual'] - safe(m['proj_dk_pts']), reverse=True)
    print(f"\n  Top 5 Under-Projections (actual >> projected):")
    for m in sorted_by_error[:5]:
        diff = m['actual'] - safe(m['proj_dk_pts'])
        print(f"    {m['full_name']:22s} proj={safe(m['proj_dk_pts']):5.1f}  actual={m['actual']:5.1f}  diff={diff:+.1f}")
    print(f"\n  Top 5 Over-Projections (projected >> actual):")
    for m in sorted_by_error[-5:]:
        diff = m['actual'] - safe(m['proj_dk_pts'])
        print(f"    {m['full_name']:22s} proj={safe(m['proj_dk_pts']):5.1f}  actual={m['actual']:5.1f}  diff={diff:+.1f}")

    # Conclusions
    conclusions = []
    if findings.get('pitchers_bias', 0) > 1.5:
        conclusions.append(f"Pitchers over-projected by {findings['pitchers_bias']:.1f} pts — consider reducing SP_CALIBRATION")
    elif findings.get('pitchers_bias', 0) < -1.5:
        conclusions.append(f"Pitchers under-projected by {abs(findings['pitchers_bias']):.1f} pts — consider increasing SP_CALIBRATION")
    if findings.get('hitters_bias', 0) > 0.5:
        conclusions.append(f"Hitters over-projected by {findings['hitters_bias']:.1f} pts — check context_mult scaling")
    conclusions.append(f"Overall MAE: {mae:.2f} (target < 5.0 for hitters, < 7.0 for pitchers)")

    print(f"\n  CONCLUSIONS:")
    for c in conclusions:
        print(f"    >> {c}")

    findings['_matched'] = matched  # pass matched data to diagnostics
    return findings


# ── Helpers for diagnostics ─────────────────────────────────────────────────

def pearson_r(xs, ys):
    """Pearson correlation between two lists. Returns 0 if not computable."""
    n = len(xs)
    if n < 5: return 0
    mx = sum(xs) / n
    my = sum(ys) / n
    num = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    dx = math.sqrt(sum((x - mx)**2 for x in xs))
    dy = math.sqrt(sum((y - my)**2 for y in ys))
    return num / (dx * dy) if dx > 0 and dy > 0 else 0


# ── Section F: Predictive Diagnostics ───────────────────────────────────────

def analyze_diagnostics(dates, matched):
    """
    Reverse-engineer projection errors using ALL available stats.
    Scans every numeric column in batter_stats, pitcher_stats, and game context
    for correlation with over/under-performance. Surfaces missing predictors
    and runs backtesting weight sweeps.
    """
    print(f"\n{'='*60}")
    print(f"  SECTION F: Predictive Diagnostics")
    print(f"{'='*60}")

    if len(matched) < 20:
        print("  Not enough matched data for diagnostics (need 20+)")
        return {}

    findings = {}

    # Split hitters and pitchers
    hitters = [m for m in matched if not m.get('is_pitcher')]
    pitchers = [m for m in matched if m.get('is_pitcher')]
    hitter_ids = list({m['player_id'] for m in hitters})
    pitcher_ids = list({m['player_id'] for m in pitchers})

    # Compute errors
    for m in matched:
        m['error'] = m['actual'] - safe(m['proj_dk_pts'])

    # ── Feature Scan: Load ALL stats ────────────────────────────────────────

    # Batter stats
    batter_stats_map = {}
    if hitter_ids:
        print(f"\n  Loading batter stats for {len(hitter_ids)} hitters...")
        for yr in [2025, 2024]:
            rows = paginate(sb.table('batter_stats')
                .select('*')
                .in_('player_id', hitter_ids[:150])
                .eq('season', yr))
            if len(hitter_ids) > 150:
                rows += paginate(sb.table('batter_stats')
                    .select('*')
                    .in_('player_id', hitter_ids[150:300])
                    .eq('season', yr))
            for r in rows:
                if r['player_id'] not in batter_stats_map:
                    batter_stats_map[r['player_id']] = r
            if len(batter_stats_map) > len(hitter_ids) * 0.5:
                break

    # Pitcher stats
    pitcher_stats_map = {}
    if pitcher_ids:
        print(f"  Loading pitcher stats for {len(pitcher_ids)} pitchers...")
        for yr in [2025, 2024]:
            rows = paginate(sb.table('pitcher_stats')
                .select('*')
                .in_('player_id', pitcher_ids[:150])
                .eq('season', yr))
            for r in rows:
                if r['player_id'] not in pitcher_stats_map:
                    pitcher_stats_map[r['player_id']] = r
            if len(pitcher_stats_map) > len(pitcher_ids) * 0.5:
                break

    # Game context: load games for venue/park info
    game_pks = list({m['game_pk'] for m in matched if m.get('game_pk')})
    games_map = {}
    if game_pks:
        rows = paginate(sb.table('games')
            .select('game_pk,venue_id,home_team,away_team,home_team_id,away_team_id')
            .in_('game_pk', game_pks[:150]))
        for r in rows:
            games_map[r['game_pk']] = r

    # ── ANALYSIS: Full Feature Scan (Hitters) ───────────────────────────────

    SKIP_COLS = {'player_id', 'season', 'team', 'full_name', 'name_display', 'key_mlbam',
                 'key_fangraphs', 'key_bbref', 'stats_level', 'team_id', 'age'}

    if hitters and batter_stats_map:
        print(f"\n  FEATURE SCAN — Hitters ({len(hitters)} matched):")
        # Get all numeric columns from a sample row
        sample = next(iter(batter_stats_map.values()))
        numeric_cols = [k for k, v in sample.items()
                       if k not in SKIP_COLS and v is not None
                       and isinstance(v, (int, float))]

        USED_BATTER = {'woba', 'xwoba', 'k_pct', 'bb_pct', 'iso', 'avg', 'wrc_plus'}
        scan_results = []
        for col in numeric_cols:
            pairs = []
            for m in hitters:
                st = batter_stats_map.get(m['player_id'])
                if not st: continue
                val = st.get(col)
                if val is not None:
                    try:
                        pairs.append((float(val), m['error']))
                    except (TypeError, ValueError):
                        pass
            if len(pairs) < 20: continue
            r = pearson_r([p[0] for p in pairs], [p[1] for p in pairs])
            used = 'Yes' if col in USED_BATTER else 'NO'
            scan_results.append((col, r, used, len(pairs)))

        scan_results.sort(key=lambda x: abs(x[1]), reverse=True)
        findings['hitter_feature_scan'] = scan_results[:25]
        missing = [s for s in scan_results if s[2] == 'NO' and abs(s[1]) > 0.08][:10]
        findings['hitter_missing_predictors'] = missing

        for col, r, used, n in scan_results[:15]:
            flag = ' <-- MISSING PREDICTOR' if used == 'NO' and abs(r) > 0.08 else ''
            print(f"    {col:22s}: r={r:+.3f}  (n={n:3d})  Used={used}{flag}")

    # ── ANALYSIS: Full Feature Scan (Pitchers) ──────────────────────────────

    if pitchers and pitcher_stats_map:
        print(f"\n  FEATURE SCAN — Pitchers ({len(pitchers)} matched):")
        sample = next(iter(pitcher_stats_map.values()))
        numeric_cols = [k for k, v in sample.items()
                       if k not in SKIP_COLS and v is not None
                       and isinstance(v, (int, float))]

        USED_PITCHER = {'xfip', 'siera', 'k_pct', 'bb_pct', 'stuff_plus', 'hr9', 'babip'}
        scan_results = []
        for col in numeric_cols:
            pairs = []
            for m in pitchers:
                st = pitcher_stats_map.get(m['player_id'])
                if not st: continue
                val = st.get(col)
                if val is not None:
                    try:
                        pairs.append((float(val), m['error']))
                    except (TypeError, ValueError):
                        pass
            if len(pairs) < 10: continue
            r = pearson_r([p[0] for p in pairs], [p[1] for p in pairs])
            used = 'Yes' if col in USED_PITCHER else 'NO'
            scan_results.append((col, r, used, len(pairs)))

        scan_results.sort(key=lambda x: abs(x[1]), reverse=True)
        findings['pitcher_feature_scan'] = scan_results[:25]
        missing = [s for s in scan_results if s[2] == 'NO' and abs(s[1]) > 0.10][:10]
        findings['pitcher_missing_predictors'] = missing

        for col, r, used, n in scan_results[:15]:
            flag = ' <-- MISSING PREDICTOR' if used == 'NO' and abs(r) > 0.10 else ''
            print(f"    {col:22s}: r={r:+.3f}  (n={n:3d})  Used={used}{flag}")

    # ── ANALYSIS: Opposing Lineup Quality for Pitchers ──────────────────────

    if pitchers and batter_stats_map:
        print(f"\n  OPPOSING LINEUP SCAN — Pitchers ({len(pitchers)} matched):")
        # For each pitcher, compute aggregate stats of the opposing lineup
        opp_agg_cols = ['k_pct', 'bb_pct', 'iso', 'woba', 'xwoba', 'barrel_pct',
                        'hard_hit_pct', 'wrc_plus', 'avg_ev', 'o_swing_pct', 'gb_pct']
        opp_scan = []
        for col in opp_agg_cols:
            pairs = []
            for m in pitchers:
                game = games_map.get(m['game_pk'])
                if not game: continue
                # Determine opposing team
                opp_team = game['away_team'] if m.get('team') == game.get('home_team') else game['home_team']
                # Get all hitters from the opposing team in this game
                opp_hitters = [h for h in hitters if h.get('team') == opp_team
                               and h.get('game_pk') == m.get('game_pk')]
                if not opp_hitters: continue
                vals = []
                for h in opp_hitters:
                    st = batter_stats_map.get(h['player_id'])
                    if st and st.get(col) is not None:
                        try:
                            vals.append(float(st[col]))
                        except (TypeError, ValueError):
                            pass
                if len(vals) < 3: continue
                avg_val = sum(vals) / len(vals)
                pairs.append((avg_val, m['error']))

            if len(pairs) < 8: continue
            r = pearson_r([p[0] for p in pairs], [p[1] for p in pairs])
            opp_scan.append((f'opp_{col}', r, len(pairs)))

        opp_scan.sort(key=lambda x: abs(x[1]), reverse=True)
        findings['opp_lineup_scan'] = opp_scan

        for col, r, n in opp_scan:
            flag = ' <-- ADD TO MODEL' if abs(r) > 0.12 else ''
            print(f"    {col:22s}: r={r:+.3f}  (n={n:3d}){flag}")

    # ── ANALYSIS: Park Factor Breakdown ─────────────────────────────────────

    if matched and games_map:
        print(f"\n  PARK FACTOR BREAKDOWN:")
        park_rows = paginate(sb.table('park_factors').select('*'))
        park_map = {r.get('venue_id') or r.get('park'): r for r in park_rows}

        venue_errors = defaultdict(list)
        for m in matched:
            game = games_map.get(m['game_pk'])
            if game and game.get('venue_id'):
                venue_errors[game['venue_id']].append(m['error'])

        park_results = []
        for vid, errors in venue_errors.items():
            if len(errors) < 5: continue
            avg_err = sum(errors) / len(errors)
            mae = sum(abs(e) for e in errors) / len(errors)
            pf = park_map.get(vid, {})
            basic = safe(pf.get('basic_factor'), 100)
            park_results.append((vid, basic, avg_err, mae, len(errors)))

        park_results.sort(key=lambda x: abs(x[2]), reverse=True)
        findings['park_breakdown'] = park_results

        for vid, basic, avg_err, mae, n in park_results[:10]:
            direction = 'over-proj' if avg_err < -1 else 'under-proj' if avg_err > 1 else 'accurate'
            print(f"    Venue {vid}: PF={basic:3.0f}  avg_err={avg_err:+.1f}  MAE={mae:.1f}  n={n}  ({direction})")

    # ── ANALYSIS: Batter Archetype Breakdown ────────────────────────────────

    if hitters and batter_stats_map:
        print(f"\n  BATTER ARCHETYPE ANALYSIS:")
        archetypes = {
            'Power (ISO>.200)': lambda st: safe(st.get('iso')) > 0.200,
            'Contact (K%<15%)': lambda st: safe(st.get('k_pct')) < 0.15,
            'Strikeout (K%>28%)': lambda st: safe(st.get('k_pct')) > 0.28,
            'High barrel (>10%)': lambda st: safe(st.get('barrel_pct')) > 10,
            'Speed (SB pace>15)': lambda st: safe(st.get('sb')) > 10,
            'Ground ball (GB%>50%)': lambda st: safe(st.get('gb_pct')) > 50,
            'Fly ball (FB%>40%)': lambda st: safe(st.get('fb_pct')) > 40,
        }
        arch_results = {}
        for arch_name, filt in archetypes.items():
            subset = [m for m in hitters if filt(batter_stats_map.get(m['player_id'], {}))]
            if len(subset) < 10: continue
            errs = [m['error'] for m in subset]
            avg_err = sum(errs) / len(errs)
            mae = sum(abs(e) for e in errs) / len(errs)
            arch_results[arch_name] = {'avg_err': avg_err, 'mae': mae, 'n': len(subset)}
            flag = ' <-- SYSTEMATIC BIAS' if abs(avg_err) > 1.0 else ''
            print(f"    {arch_name:25s}: n={len(subset):3d}  avg_err={avg_err:+.2f}  MAE={mae:.2f}{flag}")

        findings['archetype_analysis'] = arch_results

    # ── ANALYSIS: Platoon Split by Hand ─────────────────────────────────────

    if hitters:
        print(f"\n  PLATOON EFFECTIVENESS BY PITCHER HAND:")
        # Use pitcher_mult proxy: high pitcher_mult = weak pitcher = better for hitters
        for hand_label, hand_filt in [('All', lambda m: True)]:
            subset = [m for m in hitters if hand_filt(m) and m.get('platoon_mult') is not None]
            if len(subset) < 20: continue
            # Split by platoon_mult magnitude
            high_plat = [m for m in subset if safe(m.get('platoon_mult')) > 1.1]
            low_plat = [m for m in subset if safe(m.get('platoon_mult')) < 0.9]
            if high_plat:
                avg_err_hi = sum(m['error'] for m in high_plat) / len(high_plat)
                print(f"    High platoon (>1.10): n={len(high_plat):3d}  avg_err={avg_err_hi:+.2f}  "
                      f"({'accurate' if abs(avg_err_hi) < 0.5 else 'biased'})")
            if low_plat:
                avg_err_lo = sum(m['error'] for m in low_plat) / len(low_plat)
                print(f"    Low platoon  (<0.90): n={len(low_plat):3d}  avg_err={avg_err_lo:+.2f}  "
                      f"({'accurate' if abs(avg_err_lo) < 0.5 else 'biased'})")

    # ── ANALYSIS: Backtesting Weight Sweep ──────────────────────────────────

    if hitters and len(hitters) > 30:
        print(f"\n  BACKTESTING WEIGHT SWEEP — Hitters ({len(hitters)} matched):")

        # Current: context weights = vegas 58%, park 26%, weather 16%
        # Rebuild context_mult from stored sub-mults and test different weights
        test_hitters = [m for m in hitters
                       if m.get('vegas_mult') is not None
                       and m.get('park_mult') is not None
                       and m.get('weather_mult') is not None]

        if len(test_hitters) > 20:
            # Current formula: context_mult = 1.0 + (vegas-1)*0.58 + (park-1)*0.26 + (weather-1)*0.16
            # Test grid of context weights
            best_mae = 999
            best_weights = None
            current_mae = sum(abs(m['error']) for m in test_hitters) / len(test_hitters)

            for vw in range(40, 81, 5):
                for pw in range(5, 31, 5):
                    ww = 100 - vw - pw
                    if ww < 5 or ww > 30: continue
                    total_ae = 0
                    for m in test_hitters:
                        vm = safe(m['vegas_mult'], 1.0)
                        pm = safe(m['park_mult'], 1.0)
                        wm = safe(m['weather_mult'], 1.0)
                        # Recompute context_mult with test weights
                        new_ctx = 1.0 + (vm - 1.0) * vw/100 + (pm - 1.0) * pw/100 + (wm - 1.0) * ww/100
                        old_ctx = safe(m.get('context_mult'), 1.0)
                        if old_ctx == 0: old_ctx = 1.0
                        # Scale the projection by the ratio of new/old context
                        ratio = new_ctx / old_ctx if old_ctx != 0 else 1.0
                        new_proj = safe(m['proj_dk_pts']) * ratio
                        total_ae += abs(m['actual'] - new_proj)
                    test_mae = total_ae / len(test_hitters)
                    if test_mae < best_mae:
                        best_mae = test_mae
                        best_weights = (vw, pw, ww)

            if best_weights:
                improvement = current_mae - best_mae
                pct_improve = improvement / current_mae * 100 if current_mae > 0 else 0
                print(f"    Current context weights: Vegas=58%, Park=26%, Weather=16%  →  MAE={current_mae:.2f}")
                print(f"    Best context weights:    Vegas={best_weights[0]}%, Park={best_weights[1]}%, Weather={best_weights[2]}%  →  MAE={best_mae:.2f}")
                print(f"    Improvement: {improvement:.2f} pts ({pct_improve:.1f}%)")
                findings['best_context_weights'] = best_weights
                findings['context_mae_improvement'] = improvement

    # ── SUMMARY ─────────────────────────────────────────────────────────────

    print(f"\n  DIAGNOSTIC RECOMMENDATIONS:")
    rec_count = 0

    # Surface missing predictors
    for label, key in [('Hitter', 'hitter_missing_predictors'), ('Pitcher', 'pitcher_missing_predictors')]:
        missing = findings.get(key, [])
        for col, r, _, n in missing[:3]:
            rec_count += 1
            direction = 'positively' if r > 0 else 'negatively'
            print(f"    {rec_count}. ADD {col} to {label.lower()} projections (r={r:+.3f}, {direction} correlated with under-projection, n={n})")

    # Opp lineup recommendations
    for col, r, n in findings.get('opp_lineup_scan', [])[:2]:
        if abs(r) > 0.12:
            rec_count += 1
            print(f"    {rec_count}. ADD {col} to pitcher matchup model (r={r:+.3f}, n={n})")

    # Context weight recommendation
    if findings.get('best_context_weights'):
        bw = findings['best_context_weights']
        imp = findings.get('context_mae_improvement', 0)
        if imp > 0.05:
            rec_count += 1
            print(f"    {rec_count}. CHANGE context weights to Vegas={bw[0]}%, Park={bw[1]}%, Weather={bw[2]}% (saves {imp:.2f} MAE)")

    # Archetype recommendations
    for arch, data in findings.get('archetype_analysis', {}).items():
        if abs(data['avg_err']) > 1.0:
            rec_count += 1
            direction = 'under-projected' if data['avg_err'] > 0 else 'over-projected'
            print(f"    {rec_count}. FIX {arch} batters {direction} by {abs(data['avg_err']):.1f} pts (n={data['n']})")

    if rec_count == 0:
        print("    No actionable recommendations (sample may be too small)")

    return findings


# ── Section B: Ownership Accuracy ───────────────────────────────────────────

def analyze_ownership(dates):
    print(f"\n{'='*60}")
    print(f"  SECTION B: Ownership Accuracy")
    print(f"{'='*60}")

    proj_own = paginate(sb.table('slate_ownership')
        .select('player_id,game_date,dk_slate,proj_ownership')
        .in_('game_date', dates))

    actual_own = paginate(sb.table('actual_ownership')
        .select('player_id,game_date,dk_slate,ownership_pct,salary,position')
        .in_('game_date', dates))

    # Join on (player_id, game_date) — actual_ownership often has dk_slate='unknown'
    actual_map = {}
    for r in actual_own:
        actual_map[(r['player_id'], r['game_date'])] = r
    matched = []
    seen = set()
    for p in proj_own:
        key = (p['player_id'], p['game_date'])
        if key in seen: continue
        seen.add(key)
        a = actual_map.get(key)
        if a and p.get('proj_ownership') is not None and a.get('ownership_pct') is not None:
            matched.append({
                'player_id': p['player_id'],
                'proj': safe(p['proj_ownership']),
                'actual': safe(a['ownership_pct']),
                'salary': a.get('salary', 0),
                'position': a.get('position', ''),
            })

    if not matched:
        print("  No matched ownership data found.")
        return {}

    errors = [m['actual'] - m['proj'] for m in matched]
    abs_errors = [abs(e) for e in errors]
    mae = sum(abs_errors) / len(abs_errors)
    bias = sum(errors) / len(errors)

    print(f"\n  Overall ({len(matched)} players):")
    print(f"    MAE:  {mae:.2f}%")
    print(f"    Bias: {bias:+.2f}% ({'we underestimate ownership' if bias > 0 else 'we overestimate ownership'})")

    # By position
    print(f"\n  By Position:")
    pos_groups = defaultdict(list)
    for m in matched:
        pos = m['position'].split('/')[0] if m['position'] else '?'
        pos_groups[pos].append(m)
    for pos in sorted(pos_groups, key=lambda p: -len(pos_groups[p])):
        subset = pos_groups[pos]
        if len(subset) < 3: continue
        errs = [m['actual'] - m['proj'] for m in subset]
        sub_mae = sum(abs(e) for e in errs) / len(errs)
        sub_bias = sum(errs) / len(errs)
        print(f"    {pos:4s} ({len(subset):3d}): MAE={sub_mae:.2f}%  Bias={sub_bias:+.2f}%")

    # Biggest misses
    sorted_by_miss = sorted(matched, key=lambda m: abs(m['actual'] - m['proj']), reverse=True)
    print(f"\n  Top 10 Ownership Misses:")
    for m in sorted_by_miss[:10]:
        diff = m['actual'] - m['proj']
        print(f"    ID {m['player_id']:>8d}: proj={m['proj']:5.1f}%  actual={m['actual']:5.1f}%  miss={diff:+.1f}%")

    conclusions = []
    if bias > 2:
        conclusions.append(f"We underestimate ownership by {bias:.1f}% on average — public is chalkier than we model")
    elif bias < -2:
        conclusions.append(f"We overestimate ownership by {abs(bias):.1f}% — public is more contrarian than we model")
    conclusions.append(f"Ownership MAE: {mae:.2f}% (target < 3.0%)")

    print(f"\n  CONCLUSIONS:")
    for c in conclusions:
        print(f"    >> {c}")

    return {'own_mae': mae, 'own_bias': bias}


# ── Section C: Contest Lineup Analysis ──────────────────────────────────────

def analyze_contest_csv(csv_path, target_date):
    print(f"\n{'='*60}")
    print(f"  SECTION C: Contest Lineup Analysis")
    print(f"  CSV: {os.path.basename(csv_path)}")
    print(f"{'='*60}")

    # Parse DK contest standings CSV
    entries = []
    with open(csv_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            rank = int(row.get('Rank', 0) or 0)
            pts = float(row.get('Points', 0) or 0)
            lineup_str = row.get('Lineup', '')
            if rank <= 0 or not lineup_str: continue
            # Parse lineup string: "1B Pete Alonso 2B Josh Smith ..."
            players = {}
            parts = lineup_str.split()
            i = 0
            while i < len(parts):
                if parts[i] in ('P', 'C', '1B', '2B', '3B', 'SS', 'OF'):
                    pos = parts[i]
                    name_parts = []
                    i += 1
                    while i < len(parts) and parts[i] not in ('P', 'C', '1B', '2B', '3B', 'SS', 'OF'):
                        name_parts.append(parts[i])
                        i += 1
                    name = ' '.join(name_parts)
                    if pos not in players: players[pos] = []
                    players[pos].append(name)
                else:
                    i += 1
            entries.append({'rank': rank, 'pts': pts, 'players': players, 'lineup_str': lineup_str})

    if not entries:
        print("  No valid entries parsed from CSV.")
        return {}

    total = len(entries)
    top1_cutoff = max(1, int(total * 0.01))
    cash_cutoff = max(1, int(total * 0.22))  # ~22% cash rate typical

    winner = entries[0]
    top1 = entries[:top1_cutoff]
    cashers = entries[:cash_cutoff]

    print(f"\n  Contest: {total} entries")
    print(f"  Winner: {winner['pts']} pts — {winner['lineup_str'][:80]}")
    print(f"  Top 1% cutoff ({top1_cutoff}): {top1[-1]['pts']:.2f} pts")
    print(f"  Cash line (~{cash_cutoff}): {cashers[-1]['pts']:.2f} pts")

    # Analyze stack patterns in top 1%
    print(f"\n  Stack Patterns in Top 1% ({len(top1)} lineups):")
    stack_counts = defaultdict(int)
    for e in top1:
        team_counts = defaultdict(int)
        # Count team appearances (rough — using player names, not IDs)
        # This is approximate since we don't have team mappings for CSV names
        for pos, names in e['players'].items():
            if pos == 'P': continue
            for name in names:
                team_counts[name] = 1  # placeholder — real analysis needs name→team mapping
        # Count by position diversity
        hitter_count = sum(len(names) for pos, names in e['players'].items() if pos != 'P')
        pitcher_count = len(e['players'].get('P', []))
        stack_counts[f'{hitter_count}H_{pitcher_count}P'] += 1

    # Salary analysis from top entries
    print(f"\n  Scoring Distribution:")
    print(f"    Winner:      {winner['pts']:.2f} pts")
    print(f"    Top 1% avg:  {sum(e['pts'] for e in top1) / len(top1):.2f} pts")
    print(f"    Cash avg:    {sum(e['pts'] for e in cashers) / len(cashers):.2f} pts")
    print(f"    Field avg:   {sum(e['pts'] for e in entries) / len(entries):.2f} pts")
    print(f"    Field median: {sorted(e['pts'] for e in entries)[len(entries)//2]:.2f} pts")

    # Compare our pool against actuals
    our_projs = paginate(sb.table('player_projections')
        .select('player_id,full_name,proj_dk_pts')
        .eq('game_date', target_date))
    proj_by_name = {p['full_name'].lower(): safe(p['proj_dk_pts']) for p in our_projs if p.get('full_name')}

    # How well did our projections predict the winning lineup?
    if winner['players']:
        winner_names = []
        for names in winner['players'].values():
            winner_names.extend(names)
        winner_proj_total = sum(proj_by_name.get(n.lower(), 0) for n in winner_names)
        print(f"\n  Our Projection of Winner's Lineup:")
        print(f"    Projected: {winner_proj_total:.1f} pts  |  Actual: {winner['pts']:.1f} pts  |  Miss: {winner['pts'] - winner_proj_total:+.1f}")

    conclusions = [
        f"Top 1% threshold: {top1[-1]['pts']:.1f} pts (our pool needs to consistently reach this)",
        f"Cash line: {cashers[-1]['pts']:.1f} pts",
    ]
    print(f"\n  CONCLUSIONS:")
    for c in conclusions:
        print(f"    >> {c}")

    return {'winner_pts': winner['pts'], 'top1_threshold': top1[-1]['pts'], 'cash_line': cashers[-1]['pts']}


# ── Section D: Sim Accuracy (requires sim data in pool) ────────────────────

def analyze_sim_accuracy(dates):
    print(f"\n{'='*60}")
    print(f"  SECTION D: Sim Pool Quality Analysis")
    print(f"{'='*60}")

    # Load our pool lineups
    pool_rows = paginate(sb.table('sim_pool')
        .select('player_ids,proj,stack_team,stack_size,sub_team,sub_size,dk_slate')
        .in_('game_date', dates)
        .eq('pool_type', 'user'))

    if not pool_rows:
        print("  No sim pool data found for these dates.")
        return {}

    # Load actuals to score our pool lineups
    actuals = paginate(sb.table('actual_results')
        .select('player_id,game_pk,actual_dk_pts')
        .in_('game_date', dates))
    actual_by_pid = {}
    for a in actuals:
        pid = a['player_id']
        pts = safe(a.get('actual_dk_pts'))
        if pid not in actual_by_pid or pts > actual_by_pid[pid]:
            actual_by_pid[pid] = pts

    # Score each pool lineup with actuals
    scored = []
    for row in pool_rows:
        pids = row.get('player_ids', [])
        actual_pts = sum(actual_by_pid.get(pid, 0) for pid in pids)
        proj_pts = safe(row.get('proj'))
        scored.append({
            'actual': actual_pts,
            'proj': proj_pts,
            'stack_team': row.get('stack_team', ''),
            'stack_size': row.get('stack_size', 0),
            'sub_team': row.get('sub_team'),
            'sub_size': row.get('sub_size', 0),
            'slate': row.get('dk_slate', 'main'),
        })

    if not scored:
        print("  Could not score any pool lineups.")
        return {}

    # Overall pool accuracy
    scored.sort(key=lambda x: x['actual'], reverse=True)
    proj_errors = [s['actual'] - s['proj'] for s in scored]
    pool_mae = sum(abs(e) for e in proj_errors) / len(proj_errors)
    pool_bias = sum(proj_errors) / len(proj_errors)

    print(f"\n  Pool Lineups Scored: {len(scored)}")
    print(f"    Projection MAE (lineup-level): {pool_mae:.2f} pts")
    print(f"    Projection Bias: {pool_bias:+.2f} pts")
    print(f"    Best actual lineup:  {scored[0]['actual']:.1f} pts (projected {scored[0]['proj']:.1f})")
    print(f"    Worst actual lineup: {scored[-1]['actual']:.1f} pts")
    print(f"    Pool avg actual:     {sum(s['actual'] for s in scored) / len(scored):.1f} pts")
    print(f"    Pool avg projected:  {sum(s['proj'] for s in scored) / len(scored):.1f} pts")

    # Did high-projected lineups actually score higher?
    top_proj = sorted(scored, key=lambda x: x['proj'], reverse=True)[:len(scored)//10]
    bot_proj = sorted(scored, key=lambda x: x['proj'])[:len(scored)//10]
    if top_proj and bot_proj:
        top_avg = sum(s['actual'] for s in top_proj) / len(top_proj)
        bot_avg = sum(s['actual'] for s in bot_proj) / len(bot_proj)
        print(f"\n  Top 10% by projection: avg actual = {top_avg:.1f} pts")
        print(f"  Bottom 10% by projection: avg actual = {bot_avg:.1f} pts")
        print(f"  Spread: {top_avg - bot_avg:+.1f} pts ({'projections have signal' if top_avg > bot_avg else 'projections NOT predictive'})")

    # Stack config analysis
    print(f"\n  Stack Config Performance:")
    config_perf = defaultdict(list)
    for s in scored:
        key = f"{s['stack_size']}-{s.get('sub_size', 0) or 0}"
        config_perf[key].append(s['actual'])
    for config in sorted(config_perf, key=lambda k: -sum(config_perf[k])/len(config_perf[k])):
        vals = config_perf[config]
        avg = sum(vals) / len(vals)
        best = max(vals)
        print(f"    {config:6s} ({len(vals):5d} lineups): avg={avg:.1f}  best={best:.1f}")

    # Stack team analysis — which teams' stacks performed best?
    print(f"\n  Best Stack Teams (by avg actual pts):")
    team_perf = defaultdict(list)
    for s in scored:
        if s['stack_team']:
            team_perf[s['stack_team']].append(s['actual'])
    team_ranked = sorted(team_perf.items(), key=lambda x: -sum(x[1])/len(x[1]))
    for team, vals in team_ranked[:10]:
        avg = sum(vals) / len(vals)
        print(f"    {team:5s} ({len(vals):4d} lineups): avg={avg:.1f} pts")

    conclusions = []
    if pool_bias > 3:
        conclusions.append(f"Pool lineups over-projected by {pool_bias:.1f} pts on average — projections too optimistic")
    elif pool_bias < -3:
        conclusions.append(f"Pool lineups under-projected by {abs(pool_bias):.1f} pts — projections too conservative")
    if top_proj and bot_proj:
        spread = top_avg - bot_avg
        if spread > 5:
            conclusions.append(f"Projections show strong signal ({spread:.1f} pt spread between top/bottom deciles)")
        elif spread < 2:
            conclusions.append(f"Projections show WEAK signal ({spread:.1f} pt spread) — model needs improvement")

    print(f"\n  CONCLUSIONS:")
    for c in conclusions:
        print(f"    >> {c}")

    # Salary analysis
    sal_map_local = {}
    sal_rows = paginate(sb.table('dk_salaries').select('player_id,salary').eq('contest_type', 'classic'))
    for s in sal_rows:
        sal_map_local[s['player_id']] = s.get('salary', 0)

    print(f"\n  Salary Allocation in Top vs Bottom Lineups:")
    for label, subset in [('Top 10%', scored[:len(scored)//10]), ('Bottom 10%', scored[-(len(scored)//10):])]:
        total_sals = []
        sp_sals = []
        for s in subset:
            pids = [row['player_ids'] for row in pool_rows if abs(row.get('proj', 0) - s['proj']) < 0.01]
            if pids:
                lineup_pids = pids[0]
                total_sal = sum(sal_map_local.get(pid, 0) for pid in lineup_pids)
                total_sals.append(total_sal)
        if total_sals:
            print(f"    {label}: avg salary ${sum(total_sals)/len(total_sals):,.0f}")

    # Ceiling/floor analysis
    print(f"\n  Actual Score Distribution of Pool:")
    actual_pts = sorted([s['actual'] for s in scored], reverse=True)
    pcts = [1, 5, 10, 25, 50, 75, 90]
    for p in pcts:
        idx = min(int(len(actual_pts) * p / 100), len(actual_pts) - 1)
        print(f"    P{p:2d}: {actual_pts[idx]:.1f} pts")

    return {'pool_mae': pool_mae, 'pool_bias': pool_bias,
            'best_config': max(config_perf, key=lambda k: sum(config_perf[k])/len(config_perf[k])) if config_perf else None,
            'top_decile_avg': top_avg if top_proj else None,
            'bot_decile_avg': bot_avg if bot_proj else None}


# ── Section E: Actionable Recommendations ───────────────────────────────────

def generate_recommendations(proj_findings, own_findings, sim_findings, contest_findings):
    print(f"\n{'='*60}")
    print(f"  SECTION E: Actionable Recommendations")
    print(f"{'='*60}")

    recs = []

    # Projection recommendations
    p_bias = proj_findings.get('pitchers_bias', 0)
    h_bias = proj_findings.get('hitters_bias', 0)
    if abs(p_bias) > 1.5:
        direction = "decrease" if p_bias > 0 else "increase"
        recs.append(f"PROJECTION: {direction} SP_CALIBRATION by ~{abs(p_bias)/15:.2f} (current 0.90)")
    if abs(h_bias) > 0.5:
        direction = "reduce" if h_bias > 0 else "increase"
        recs.append(f"PROJECTION: {direction} context multiplier weights — hitter bias is {h_bias:+.2f} pts")

    # Ownership recommendations
    own_bias = own_findings.get('own_bias', 0)
    if abs(own_bias) > 2:
        direction = "increase" if own_bias > 0 else "decrease"
        recs.append(f"OWNERSHIP: {direction} baseline ownership estimates — bias is {own_bias:+.1f}%")

    # Sim pool recommendations
    best_config = sim_findings.get('best_config')
    if best_config:
        recs.append(f"POOL: Best performing stack config is {best_config} — increase its weight in STACK_CONFIGS")
    top_dec = sim_findings.get('top_decile_avg')
    bot_dec = sim_findings.get('bot_decile_avg')
    if top_dec and bot_dec:
        spread = top_dec - bot_dec
        if spread > 5:
            recs.append(f"POOL: Projections have {spread:.1f} pt spread — use projection rank as primary sort for portfolio selection")
        else:
            recs.append(f"POOL: Projection spread only {spread:.1f} pts — diversify selection, don't over-rely on projection ranking")

    # Contest recommendations
    if contest_findings.get('top1_threshold'):
        recs.append(f"CONTEST: Avg Top 1% threshold is {contest_findings['top1_threshold']:.1f} pts across {contest_findings.get('n_contests', 1)} contests")
    if contest_findings.get('cash_line'):
        recs.append(f"CONTEST: Avg cash line is {contest_findings['cash_line']:.1f} pts — pool floor should exceed this")
    if contest_findings.get('winner_pts'):
        recs.append(f"CONTEST: Avg winner scores {contest_findings['winner_pts']:.1f} pts — need high-ceiling correlated stacks")

    # Ownership recommendations
    own_bias = own_findings.get('own_bias', 0)
    own_mae = own_findings.get('own_mae', 0)
    if abs(own_bias) > 2:
        direction = "increase" if own_bias > 0 else "decrease"
        recs.append(f"OWNERSHIP: {direction} baseline estimates — bias is {own_bias:+.1f}%")
    if own_mae > 5:
        recs.append(f"OWNERSHIP: MAE is {own_mae:.1f}% — needs significant model improvement")
    elif own_mae > 0:
        recs.append(f"OWNERSHIP: MAE is {own_mae:.1f}% ({'good' if own_mae < 3 else 'needs work'})")

    # Tier-specific recommendations from projection analysis
    tier_data = proj_findings.get('tier_correlations', {})
    for mult, corr in tier_data.items():
        if corr < -0.10:
            recs.append(f"PROJECTION: {mult} is hurting accuracy (r={corr:+.3f}) — reduce its weight or cap its range")

    recs.append("TRACKING: Run this analysis daily to build sample size — patterns stabilize after 2+ weeks")

    for i, r in enumerate(recs, 1):
        print(f"  {i}. {r}")

    return recs


# ── Write Findings to File ──────────────────────────────────────────────────

def write_findings(dates, proj_findings, own_findings, sim_findings, contest_findings, recs, diag_findings=None):
    header = f"\n## Research Findings — {', '.join(dates)}\n"
    lines = [header]

    if proj_findings:
        lines.append(f"**Projection**: MAE={proj_findings.get('overall_mae', '?'):.2f}, "
                     f"Bias={proj_findings.get('overall_bias', '?'):+.2f}, "
                     f"Hitter MAE={proj_findings.get('hitters_mae', '?'):.2f}, "
                     f"Pitcher MAE={proj_findings.get('pitchers_mae', '?'):.2f}")
    if own_findings:
        lines.append(f"**Ownership**: MAE={own_findings.get('own_mae', '?'):.2f}%, "
                     f"Bias={own_findings.get('own_bias', '?'):+.2f}%")
    if sim_findings:
        lines.append(f"**Pool**: MAE={sim_findings.get('pool_mae', '?'):.2f}, "
                     f"Bias={sim_findings.get('pool_bias', '?'):+.2f}")
    if contest_findings:
        lines.append(f"**Contest**: Winner={contest_findings.get('winner_pts', '?')}, "
                     f"Top1%={contest_findings.get('top1_threshold', '?')}")

    # Diagnostic findings
    if diag_findings:
        lines.append("\n### Predictive Diagnostics")
        # Missing predictors
        for label, key in [('Hitter', 'hitter_missing_predictors'), ('Pitcher', 'pitcher_missing_predictors')]:
            missing = diag_findings.get(key, [])
            if missing:
                lines.append(f"\n**{label} Missing Predictors** (correlated with error but not in model):")
                for col, r, _, n in missing[:5]:
                    lines.append(f"- `{col}` r={r:+.3f} (n={n})")

        # Opp lineup scan
        opp = diag_findings.get('opp_lineup_scan', [])
        strong_opp = [(c, r, n) for c, r, n in opp if abs(r) > 0.10]
        if strong_opp:
            lines.append(f"\n**Opposing Lineup Factors** (for pitcher projections):")
            for col, r, n in strong_opp[:5]:
                lines.append(f"- `{col}` r={r:+.3f} (n={n})")

        # Context weight optimization
        if diag_findings.get('best_context_weights'):
            bw = diag_findings['best_context_weights']
            imp = diag_findings.get('context_mae_improvement', 0)
            lines.append(f"\n**Optimal Context Weights**: Vegas={bw[0]}% Park={bw[1]}% Weather={bw[2]}% (saves {imp:.2f} MAE)")

        # Archetype biases
        arch = diag_findings.get('archetype_analysis', {})
        biased = {k: v for k, v in arch.items() if abs(v['avg_err']) > 0.8}
        if biased:
            lines.append(f"\n**Archetype Biases:**")
            for name, data in biased.items():
                direction = 'under-projected' if data['avg_err'] > 0 else 'over-projected'
                lines.append(f"- {name}: {direction} by {abs(data['avg_err']):.1f} pts (n={data['n']})")

    lines.append("\n**Recommendations:**")
    for r in recs:
        lines.append(f"- {r}")
    lines.append("")

    with open(FINDINGS_PATH, 'a', encoding='utf-8') as f:
        f.write('\n'.join(lines))
    print(f"\n  Findings appended to {FINDINGS_PATH}")


# ── Main ────────────────────────────────────────────────────────────────────

def run():
    args = sys.argv[1:]
    target_dates = []
    csv_path = None

    i = 0
    while i < len(args):
        if args[i] == '--date' and i+1 < len(args):
            target_dates = [args[i+1]]; i += 2
        elif args[i] == '--range' and i+2 < len(args):
            start = date.fromisoformat(args[i+1])
            end = date.fromisoformat(args[i+2])
            d = start
            while d <= end:
                target_dates.append(str(d))
                d += timedelta(days=1)
            i += 3
        elif args[i] == '--csv' and i+1 < len(args):
            csv_path = args[i+1]; i += 2
        else:
            i += 1

    if not target_dates:
        # Default: latest date with actual results
        rows = sb.table('actual_results').select('game_date').order('game_date', desc=True).limit(1).execute().data
        if rows:
            target_dates = [rows[0]['game_date']]
        else:
            print("No actual results found. Run load_actuals.py first.")
            return

    print(f"\nSlateHub Research — {', '.join(target_dates)}")
    print("=" * 60)

    proj_findings = analyze_projections(target_dates)
    own_findings = analyze_ownership(target_dates)
    sim_findings = analyze_sim_accuracy(target_dates)

    contest_findings = {}
    if csv_path:
        contest_findings = analyze_contest_csv(csv_path, target_dates[0])
    else:
        # Auto-discover and analyze ALL CSVs in Contest_CSVs directory
        csvs = sorted(glob.glob(os.path.join(CONTEST_CSV_DIR, 'contest-standings-*.csv')))
        if csvs:
            print(f"\n  Found {len(csvs)} contest CSVs in {CONTEST_CSV_DIR}")
            all_contest = []
            for c in csvs:
                cf = analyze_contest_csv(c, target_dates[0])
                if cf: all_contest.append(cf)
            if all_contest:
                contest_findings = {
                    'winner_pts': sum(c.get('winner_pts', 0) for c in all_contest) / len(all_contest),
                    'top1_threshold': sum(c.get('top1_threshold', 0) for c in all_contest) / len(all_contest),
                    'cash_line': sum(c.get('cash_line', 0) for c in all_contest) / len(all_contest),
                    'n_contests': len(all_contest),
                }
                print(f"\n  AGGREGATE ({len(all_contest)} contests):")
                print(f"    Avg Winner: {contest_findings['winner_pts']:.1f} pts")
                print(f"    Avg Top 1%: {contest_findings['top1_threshold']:.1f} pts")
                print(f"    Avg Cash:   {contest_findings['cash_line']:.1f} pts")

    # Predictive diagnostics: full feature scan + backtesting
    diag_findings = {}
    matched_data = proj_findings.get('_matched', [])
    if matched_data:
        diag_findings = analyze_diagnostics(target_dates, matched_data)

    recs = generate_recommendations(proj_findings, own_findings, sim_findings, contest_findings)
    write_findings(target_dates, proj_findings, own_findings, sim_findings, contest_findings, recs, diag_findings)

    print(f"\n{'='*60}")
    print(f"  Research complete.")
    print(f"{'='*60}\n")


if __name__ == '__main__':
    run()
