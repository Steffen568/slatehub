#!/usr/bin/env python3
import sys
sys.stdout.reconfigure(encoding='utf-8', line_buffering=True)
"""
Simulation Validation — Post-game diagnostic for sim engine accuracy

Tests the simulation pipeline against actual results across 6 dimensions:
A. Distribution calibration (are P10/P90 bands correct?)
B. Projection accuracy by segment (position, salary, batting order)
C. Pitcher component accuracy (IP, Ks, ER individually)
D. Multiplier effectiveness (do adjustments help or hurt?)
E. Ownership accuracy (projected vs actual DK ownership)
F. Accumulation (track metrics over time)

Run:
  py -3.12 validate_sim.py                              # latest date
  py -3.12 validate_sim.py --date 2026-04-13
  py -3.12 validate_sim.py --range 2026-04-04 2026-04-13
"""

import os
import math
import csv
import argparse
import numpy as np
from collections import defaultdict
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()
sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
HISTORY_PATH = os.path.join(SCRIPT_DIR, 'sim_validation_history.csv')
FINDINGS_PATH = os.path.join(SCRIPT_DIR, 'tasks', 'research_findings.md')


# ── Helpers ───────────────────────────────────────────────────────────────────

def safe(val, default=0):
    if val is None: return default
    try:
        f = float(val)
        return default if math.isnan(f) or math.isinf(f) else f
    except Exception:
        return default


def paginate(query, page_size=1000):
    rows = []
    for offset in range(0, 50000, page_size):
        page = query.range(offset, offset + page_size - 1).execute().data or []
        rows.extend(page)
        if len(page) < page_size:
            break
    return rows


def pearson_r(xs, ys):
    n = len(xs)
    if n < 5:
        return 0
    mx = sum(xs) / n
    my = sum(ys) / n
    num = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    dx = math.sqrt(sum((x - mx) ** 2 for x in xs))
    dy = math.sqrt(sum((y - my) ** 2 for y in ys))
    return num / (dx * dy) if dx > 0 and dy > 0 else 0


def verdict(value, pass_lo, pass_hi, warn_lo, warn_hi):
    if pass_lo <= value <= pass_hi:
        return 'PASS'
    elif warn_lo <= value <= warn_hi:
        return 'WARN'
    return 'FAIL'


def verdict_below(value, pass_thresh, warn_thresh):
    if value <= pass_thresh:
        return 'PASS'
    elif value <= warn_thresh:
        return 'WARN'
    return 'FAIL'


def verdict_above(value, pass_thresh, warn_thresh):
    if value >= pass_thresh:
        return 'PASS'
    elif value >= warn_thresh:
        return 'WARN'
    return 'FAIL'


# ── Data Loading ──────────────────────────────────────────────────────────────

def load_data(dates):
    print(f"  Loading data for {len(dates)} date(s)...")

    # Try projection_history first (preserved archive), fall back to player_projections (live)
    hist_cols = ('player_id,game_pk,game_date,full_name,team,is_pitcher,batting_order,'
                 'proj_dk_pts,proj_floor,proj_ceiling,proj_ip,proj_ks,proj_er,win_prob')
    proj_cols = ('player_id,game_pk,game_date,full_name,team,is_pitcher,batting_order,'
                 'proj_dk_pts,proj_floor,proj_ceiling,'
                 'pitcher_mult,platoon_mult,context_mult,vegas_mult,park_mult,weather_mult,'
                 'proj_ip,proj_ks,proj_er,proj_h_allowed,proj_bb_allowed,win_prob,proj_ownership')
    try:
        projs = paginate(sb.table('projection_history')
            .select(hist_cols)
            .in_('game_date', dates))
        if projs:
            print(f"  Projections: {len(projs)} from projection_history (archived)")
        else:
            raise Exception("No history data")
    except Exception:
        try:
            projs = paginate(sb.table('player_projections')
                .select(proj_cols + ',sim_sd,sim_median,sim_p25,sim_p75')
                .in_('game_date', dates))
        except Exception:
            projs = paginate(sb.table('player_projections')
                .select(proj_cols)
                .in_('game_date', dates))

    actuals = paginate(sb.table('actual_results')
        .select('player_id,game_pk,game_date,actual_dk_pts,is_pitcher,'
                'ip,p_k,p_er,p_h,p_bb,p_hr,win,loss')
        .in_('game_date', dates))

    sals = paginate(sb.table('dk_salaries')
        .select('player_id,salary,position'))

    # Build lookup maps
    actual_map = {}
    for a in actuals:
        actual_map[(a['player_id'], a['game_pk'])] = a

    sal_map = {r['player_id']: r for r in sals}

    # Join projections with actuals
    matched = []
    for p in projs:
        key = (p['player_id'], p['game_pk'])
        a = actual_map.get(key)
        if not a:
            continue
        sal_info = sal_map.get(p['player_id'], {})
        pos = sal_info.get('position', 'SP' if p.get('is_pitcher') else '??')
        matched.append({
            **p,
            'actual_dk_pts': safe(a['actual_dk_pts']),
            'actual_ip': safe(a.get('ip')),
            'actual_ks': safe(a.get('p_k')),
            'actual_er': safe(a.get('p_er')),
            'actual_h': safe(a.get('p_h')),
            'actual_bb': safe(a.get('p_bb')),
            'actual_win': bool(a.get('win')),
            'salary': safe(sal_info.get('salary')),
            'position': pos.split('/')[0] if pos else '??',
        })

    # Ownership data
    own_proj = paginate(sb.table('player_projections')
        .select('player_id,game_date,proj_ownership')
        .in_('game_date', dates))

    try:
        own_actual = paginate(sb.table('actual_ownership')
            .select('player_id,game_date,ownership_pct')
            .in_('game_date', dates))
    except Exception:
        own_actual = []

    own_actual_map = {}
    for o in own_actual:
        own_actual_map.setdefault(o['player_id'], {})[o['game_date']] = safe(o['ownership_pct'])

    own_matched = []
    for p in own_proj:
        proj_own = safe(p.get('proj_ownership'))
        if proj_own <= 0:
            continue
        actual_own = own_actual_map.get(p['player_id'], {}).get(p['game_date'])
        if actual_own is None:
            continue
        own_matched.append({
            'player_id': p['player_id'],
            'game_date': p['game_date'],
            'proj_own': proj_own,
            'actual_own': actual_own,
            'salary': safe(sal_map.get(p['player_id'], {}).get('salary')),
            'position': (sal_map.get(p['player_id'], {}).get('position', '??') or '??').split('/')[0],
        })

    hitters = [m for m in matched if not m.get('is_pitcher')]
    pitchers = [m for m in matched if m.get('is_pitcher')]

    print(f"  Matched: {len(matched)} players ({len(hitters)} hitters, {len(pitchers)} pitchers)")
    print(f"  Ownership pairs: {len(own_matched)}")

    return {
        'matched': matched,
        'hitters': hitters,
        'pitchers': pitchers,
        'own_matched': own_matched,
    }


# ── Section A: Distribution Calibration ───────────────────────────────────────

def section_a(data):
    print(f"\n{'='*60}")
    print(f"  SECTION A: Distribution Calibration")
    print(f"{'='*60}")

    metrics = {}
    for label, group in [('Hitters', data['hitters']), ('Pitchers', data['pitchers'])]:
        # Filter to rows with sim distribution data
        rows = [m for m in group if m.get('proj_floor') is not None and m.get('proj_ceiling') is not None]
        if not rows:
            print(f"\n  {label}: No distribution data available")
            continue

        below_floor = sum(1 for m in rows if m['actual_dk_pts'] < safe(m['proj_floor']))
        above_ceil = sum(1 for m in rows if m['actual_dk_pts'] > safe(m['proj_ceiling']))

        rows_iqr = [m for m in rows if m.get('sim_p25') is not None and m.get('sim_p75') is not None]
        in_iqr = sum(1 for m in rows_iqr if safe(m['sim_p25']) <= m['actual_dk_pts'] <= safe(m['sim_p75']))
        in_80 = sum(1 for m in rows if safe(m['proj_floor']) <= m['actual_dk_pts'] <= safe(m['proj_ceiling']))

        n = len(rows)
        n_iqr = len(rows_iqr)
        pct_below = below_floor / n * 100
        pct_above = above_ceil / n * 100
        pct_in_80 = in_80 / n * 100
        pct_in_iqr = in_iqr / n_iqr * 100 if n_iqr > 0 else 0

        tag = label.lower()
        metrics[f'pct_below_floor_{tag}'] = round(pct_below, 1)
        metrics[f'pct_above_ceil_{tag}'] = round(pct_above, 1)
        metrics[f'pct_in_p10_p90_{tag}'] = round(pct_in_80, 1)
        metrics[f'pct_in_iqr_{tag}'] = round(pct_in_iqr, 1)

        v = verdict(pct_in_80, 75, 85, 65, 92)
        metrics[f'verdict_dist_{tag}'] = v

        print(f"\n  {label} (n={n}):")
        print(f"    Below P10 (floor):   {pct_below:5.1f}%  (target ~10%)")
        print(f"    Above P90 (ceiling): {pct_above:5.1f}%  (target ~10%)")
        print(f"    Within P10-P90:      {pct_in_80:5.1f}%  (target ~80%)  [{v}]")
        if n_iqr > 0:
            print(f"    Within P25-P75:      {pct_in_iqr:5.1f}%  (target ~50%)")

        if pct_in_80 < 75:
            print(f"    >> Bands are TOO TIGHT — sim underestimates variance")
        elif pct_in_80 > 85:
            print(f"    >> Bands are TOO WIDE — sim overestimates variance (noisy projections)")
        else:
            print(f"    >> Distribution calibration looks good")

    return metrics


# ── Section B: Projection Accuracy by Segment ────────────────────────────────

def section_b(data):
    print(f"\n{'='*60}")
    print(f"  SECTION B: Projection Accuracy by Segment")
    print(f"{'='*60}")

    metrics = {}
    all_rows = data['matched']
    hitters = data['hitters']
    pitchers = data['pitchers']

    def compute_accuracy(rows, label, indent=4):
        if not rows:
            print(f"{' '*indent}{label}: no data")
            return {}
        projs = [safe(m['proj_dk_pts']) for m in rows]
        acts = [m['actual_dk_pts'] for m in rows]
        errors = [p - a for p, a in zip(projs, acts)]
        mae = np.mean(np.abs(errors))
        bias = np.mean(errors)
        corr = pearson_r(projs, acts)
        print(f"{' '*indent}{label} (n={len(rows)}):  MAE={mae:.2f}  Bias={bias:+.2f}  r={corr:.3f}")
        return {'mae': mae, 'bias': bias, 'corr': corr, 'n': len(rows)}

    # Overall
    overall = compute_accuracy(all_rows, "Overall")
    metrics['overall_mae'] = round(overall['mae'], 2)
    metrics['overall_bias'] = round(overall['bias'], 2)
    metrics['overall_corr'] = round(overall['corr'], 3)

    h = compute_accuracy(hitters, "Hitters")
    p = compute_accuracy(pitchers, "Pitchers")
    metrics['hitter_mae'] = round(h['mae'], 2) if h else 0
    metrics['pitcher_mae'] = round(p['mae'], 2) if p else 0
    metrics['verdict_hitter_mae'] = verdict_below(h['mae'], 6.0, 8.0) if h else 'N/A'
    metrics['verdict_pitcher_mae'] = verdict_below(p['mae'], 8.0, 11.0) if p else 'N/A'

    # By position
    print(f"\n    By Position:")
    pos_groups = defaultdict(list)
    for m in all_rows:
        pos_groups[m['position']].append(m)
    for pos in ['SP', 'C', '1B', '2B', 'SS', '3B', 'OF']:
        if pos in pos_groups:
            compute_accuracy(pos_groups[pos], pos, indent=6)

    # By salary tier
    print(f"\n    By Salary Tier:")
    tiers = [('$3-5K', 3000, 5000), ('$5-7K', 5000, 7000), ('$7-9K', 7000, 9000), ('$9K+', 9000, 99999)]
    for label, lo, hi in tiers:
        tier_rows = [m for m in all_rows if lo <= m['salary'] < hi]
        compute_accuracy(tier_rows, label, indent=6)

    # By batting order
    print(f"\n    By Batting Order:")
    for label, bo_lo, bo_hi in [('BO 1-3', 1, 3), ('BO 4-6', 4, 6), ('BO 7-9', 7, 9)]:
        bo_rows = [m for m in hitters if m.get('batting_order') and bo_lo <= m['batting_order'] <= bo_hi]
        compute_accuracy(bo_rows, label, indent=6)

    # By sim confidence
    sd_vals = [safe(m.get('sim_sd')) for m in hitters if m.get('sim_sd')]
    if sd_vals:
        print(f"\n    By Sim Confidence:")
        sd_median = np.median(sd_vals)
        low_sd = [m for m in hitters if m.get('sim_sd') and safe(m['sim_sd']) <= sd_median]
        high_sd = [m for m in hitters if m.get('sim_sd') and safe(m['sim_sd']) > sd_median]
        lo_r = compute_accuracy(low_sd, "Low SD (high confidence)", indent=6)
        hi_r = compute_accuracy(high_sd, "High SD (low confidence)", indent=6)
        if lo_r and hi_r and lo_r['mae'] < hi_r['mae']:
            print(f"      >> Low-SD players have lower MAE — sim correctly identifies certainty")
        elif lo_r and hi_r:
            print(f"      >> Low-SD players do NOT have lower MAE — sim confidence is miscalibrated")

    return metrics


# ── Section C: Pitcher Component Accuracy ─────────────────────────────────────

def section_c(data):
    print(f"\n{'='*60}")
    print(f"  SECTION C: Pitcher Component Accuracy")
    print(f"{'='*60}")

    metrics = {}
    pitchers = [m for m in data['pitchers'] if safe(m.get('proj_ip')) > 0]

    if not pitchers:
        print(f"  No pitcher projection data with components")
        return metrics

    # IP, Ks, ER
    components = [
        ('IP', 'proj_ip', 'actual_ip'),
        ('Ks', 'proj_ks', 'actual_ks'),
        ('ER', 'proj_er', 'actual_er'),
    ]

    print(f"\n  Component Projections (n={len(pitchers)} pitchers):")
    print(f"    {'Component':12s} {'MAE':>6s} {'Bias':>8s} {'Avg Proj':>9s} {'Avg Act':>8s}")
    print(f"    {'─'*46}")

    for label, proj_key, actual_key in components:
        rows = [(safe(m[proj_key]), m[actual_key]) for m in pitchers if m.get(proj_key) is not None]
        if not rows:
            continue
        projs, acts = zip(*rows)
        errors = [p - a for p, a in zip(projs, acts)]
        mae = np.mean(np.abs(errors))
        bias = np.mean(errors)
        print(f"    {label:12s} {mae:6.2f} {bias:+8.2f} {np.mean(projs):9.2f} {np.mean(acts):8.2f}")
        metrics[f'{label.lower()}_mae'] = round(mae, 2)
        metrics[f'{label.lower()}_bias'] = round(bias, 2)

    # Win probability calibration
    wp_rows = [(safe(m.get('win_prob')), m['actual_win']) for m in pitchers if m.get('win_prob') is not None]
    if len(wp_rows) >= 10:
        print(f"\n  Win Probability Calibration:")
        bins = [(0, 0.15, '0-15%'), (0.15, 0.25, '15-25%'), (0.25, 0.35, '25-35%'), (0.35, 0.50, '35-50%'), (0.50, 1.0, '50%+')]
        total_cal_error = 0
        cal_count = 0
        for lo, hi, label in bins:
            bin_rows = [(wp, w) for wp, w in wp_rows if lo <= wp < hi]
            if len(bin_rows) < 5:
                continue
            avg_prob = np.mean([wp for wp, _ in bin_rows])
            actual_rate = np.mean([1 if w else 0 for _, w in bin_rows])
            cal_err = abs(avg_prob - actual_rate)
            total_cal_error += cal_err
            cal_count += 1
            ok = 'OK' if cal_err < 0.10 else 'OFF'
            print(f"    {label:8s}  n={len(bin_rows):3d}  expected={avg_prob:.1%}  actual={actual_rate:.1%}  [{ok}]")
        if cal_count > 0:
            metrics['win_cal_error'] = round(total_cal_error / cal_count, 3)
    else:
        print(f"\n  Win probability: insufficient data ({len(wp_rows)} pitchers)")

    return metrics


# ── Section D: Multiplier Effectiveness ───────────────────────────────────────

def section_d(data):
    print(f"\n{'='*60}")
    print(f"  SECTION D: Multiplier Effectiveness")
    print(f"{'='*60}")

    metrics = {}
    hitters = data['hitters']

    mults = ['pitcher_mult', 'platoon_mult', 'context_mult', 'vegas_mult', 'park_mult', 'weather_mult']

    print(f"\n    {'Multiplier':16s} {'Corr':>7s} {'Q1 Act':>7s} {'Q5 Act':>7s} {'Lift':>6s} {'Verdict':>8s}")
    print(f"    {'─'*56}")

    pass_count = 0
    for mult_name in mults:
        rows = [(safe(m[mult_name]), m['actual_dk_pts'], safe(m['proj_dk_pts']))
                for m in hitters if m.get(mult_name) is not None and safe(m[mult_name]) != 0]

        if len(rows) < 20:
            print(f"    {mult_name:16s}  insufficient data ({len(rows)})")
            continue

        mult_vals, act_vals, proj_vals = zip(*rows)
        errors = [a - p for a, p in zip(act_vals, proj_vals)]
        corr = pearson_r(list(mult_vals), errors)

        # Quintile analysis
        sorted_rows = sorted(rows, key=lambda x: x[0])
        q_size = len(sorted_rows) // 5
        if q_size > 0:
            q1_act = np.mean([r[1] for r in sorted_rows[:q_size]])
            q5_act = np.mean([r[1] for r in sorted_rows[-q_size:]])
            lift = q5_act - q1_act
        else:
            q1_act = q5_act = lift = 0

        v = verdict_above(corr, 0.05, -0.05)
        if v == 'PASS':
            pass_count += 1
        metrics[f'{mult_name}_corr'] = round(corr, 3)

        print(f"    {mult_name:16s} {corr:+7.3f} {q1_act:7.1f} {q5_act:7.1f} {lift:+6.1f} [{v}]")

    total = len(mults)
    print(f"\n    >> {pass_count}/{total} multipliers show positive correlation with actual performance")
    if pass_count < 3:
        print(f"    >> CONCERN: Most multipliers are not helping — adjustments may be adding noise")

    return metrics


# ── Section E: Ownership Accuracy ─────────────────────────────────────────────

def section_e(data):
    print(f"\n{'='*60}")
    print(f"  SECTION E: Ownership Accuracy")
    print(f"{'='*60}")

    metrics = {}
    own = data['own_matched']

    if not own:
        print(f"  No ownership comparison data available")
        print(f"  (Run load_actual_ownership.py to populate actual_ownership)")
        return metrics

    projs = [o['proj_own'] for o in own]
    acts = [o['actual_own'] for o in own]
    errors = [p - a for p, a in zip(projs, acts)]
    mae = np.mean(np.abs(errors))
    bias = np.mean(errors)
    corr = pearson_r(projs, acts)

    v = verdict_below(mae, 4.0, 7.0)
    metrics['own_mae'] = round(mae, 2)
    metrics['own_bias'] = round(bias, 2)
    metrics['own_corr'] = round(corr, 3)
    metrics['verdict_ownership'] = v

    print(f"\n  Overall (n={len(own)}):")
    print(f"    MAE:         {mae:.2f}%")
    print(f"    Bias:        {bias:+.2f}%  {'(over-projecting ownership)' if bias > 0 else '(under-projecting)'}")
    print(f"    Correlation: {corr:.3f}")
    print(f"    Verdict:     [{v}]")

    # By position
    print(f"\n    By Position:")
    pos_groups = defaultdict(list)
    for o in own:
        pos_groups[o['position']].append(o)
    for pos in ['SP', 'C', '1B', '2B', 'SS', '3B', 'OF']:
        rows = pos_groups.get(pos, [])
        if len(rows) < 5:
            continue
        p = [o['proj_own'] for o in rows]
        a = [o['actual_own'] for o in rows]
        m = np.mean(np.abs([pp - aa for pp, aa in zip(p, a)]))
        print(f"      {pos:4s} (n={len(rows):3d}): MAE={m:.2f}%")

    # Biggest misses
    own_sorted = sorted(own, key=lambda o: abs(o['proj_own'] - o['actual_own']), reverse=True)
    print(f"\n    Top 5 Ownership Misses:")
    for o in own_sorted[:5]:
        delta = o['proj_own'] - o['actual_own']
        pid = o['player_id']
        print(f"      ID:{pid}  proj={o['proj_own']:.1f}%  actual={o['actual_own']:.1f}%  delta={delta:+.1f}%")

    return metrics


# ── Section F: Accumulation ───────────────────────────────────────────────────

def section_f(dates, metrics):
    print(f"\n{'='*60}")
    print(f"  SECTION F: Trend Tracking")
    print(f"{'='*60}")

    # Define CSV columns
    csv_cols = [
        'date', 'sample_n',
        'hitter_mae', 'pitcher_mae', 'overall_bias', 'overall_corr',
        'pct_in_p10_p90_hitters', 'pct_in_p10_p90_pitchers',
        'ip_mae', 'ks_mae', 'er_mae',
        'own_mae', 'own_bias',
        'pitcher_mult_corr', 'vegas_mult_corr', 'context_mult_corr',
        'verdict_hitter_mae', 'verdict_pitcher_mae', 'verdict_ownership',
    ]

    # Read existing history
    history = []
    if os.path.exists(HISTORY_PATH):
        with open(HISTORY_PATH, 'r', newline='') as f:
            reader = csv.DictReader(f)
            history = list(reader)

    existing_dates = {r['date'] for r in history}

    # Append one row per run (use latest date as key)
    run_date = max(dates)
    added = 0
    if run_date not in existing_dates:
        row = {'date': run_date}
        for col in csv_cols[1:]:
            row[col] = metrics.get(col, '')
        history.append(row)
        added = 1

    # Write back
    if added > 0:
        history.sort(key=lambda r: r.get('date', ''))
        with open(HISTORY_PATH, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=csv_cols, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(history)
        print(f"\n  Appended {added} row(s) to {HISTORY_PATH}")
    else:
        print(f"\n  All dates already in history — no new rows added")

    # Show trends if 2+ rows
    if len(history) >= 2:
        print(f"\n  Historical Trends ({len(history)} dates):")
        key_metrics = ['hitter_mae', 'pitcher_mae', 'overall_corr', 'pct_in_p10_p90_hitters', 'own_mae']
        print(f"    {'Date':12s} {'Hit MAE':>8s} {'Pit MAE':>8s} {'Corr':>7s} {'Dist%':>6s} {'Own MAE':>8s}")
        print(f"    {'─'*52}")
        for r in history[-10:]:
            def fv(v):
                return str(v) if v else '--'
            print(f"    {fv(r.get('date')):12s} "
                  f"{fv(r.get('hitter_mae')):>8s} "
                  f"{fv(r.get('pitcher_mae')):>8s} "
                  f"{fv(r.get('overall_corr')):>7s} "
                  f"{fv(r.get('pct_in_p10_p90_hitters')):>6s} "
                  f"{fv(r.get('own_mae')):>8s}")


# ── Verdicts ──────────────────────────────────────────────────────────────────

def print_verdicts(metrics):
    print(f"\n{'='*60}")
    print(f"  VALIDATION VERDICTS")
    print(f"{'='*60}")

    verdicts = [
        ('Distribution (Hitters)', metrics.get('verdict_dist_hitters', 'N/A'),
         f"{metrics.get('pct_in_p10_p90_hitters', '?')}% in P10-P90, target 80%"),
        ('Distribution (Pitchers)', metrics.get('verdict_dist_pitchers', 'N/A'),
         f"{metrics.get('pct_in_p10_p90_pitchers', '?')}% in P10-P90, target 80%"),
        ('Hitter Projection MAE', metrics.get('verdict_hitter_mae', 'N/A'),
         f"{metrics.get('hitter_mae', '?')} pts, target <6.0"),
        ('Pitcher Projection MAE', metrics.get('verdict_pitcher_mae', 'N/A'),
         f"{metrics.get('pitcher_mae', '?')} pts, target <8.0"),
        ('Ownership MAE', metrics.get('verdict_ownership', 'N/A'),
         f"{metrics.get('own_mae', '?')}%, target <4.0%"),
    ]

    for label, v, detail in verdicts:
        marker = '  ' if v == 'PASS' else '>>' if v == 'FAIL' else '> '
        print(f"  {marker} {label:28s}  [{v:4s}]  ({detail})")

    fail_count = sum(1 for _, v, _ in verdicts if v == 'FAIL')
    warn_count = sum(1 for _, v, _ in verdicts if v == 'WARN')
    if fail_count > 0:
        print(f"\n  >> {fail_count} FAIL(s) detected — sim needs calibration work")
    elif warn_count > 0:
        print(f"\n  >  {warn_count} WARNING(s) — sim is borderline in some areas")
    else:
        print(f"\n     All checks passed — sim is well-calibrated")


# ── Write Findings to MD ───────────────────────────────────────────────────────

def write_findings(dates, metrics, data):
    lines = [f"\n## Sim Validation — {', '.join(dates)}\n"]

    # Distribution
    lines.append("### Distribution Calibration")
    for tag in ['hitters', 'pitchers']:
        pct = metrics.get(f'pct_in_p10_p90_{tag}', '?')
        v = metrics.get(f'verdict_dist_{tag}', '?')
        below = metrics.get(f'pct_below_floor_{tag}', '?')
        above = metrics.get(f'pct_above_ceil_{tag}', '?')
        lines.append(f"- **{tag.title()}**: {pct}% in P10-P90 [{v}] (below floor={below}%, above ceiling={above}%)")

    # Accuracy
    lines.append("\n### Projection Accuracy")
    lines.append(f"- Overall: MAE={metrics.get('overall_mae','?')}, Bias={metrics.get('overall_bias','?'):+.2f}, r={metrics.get('overall_corr','?')}")
    lines.append(f"- Hitters: MAE={metrics.get('hitter_mae','?')} [{metrics.get('verdict_hitter_mae','?')}]")
    lines.append(f"- Pitchers: MAE={metrics.get('pitcher_mae','?')} [{metrics.get('verdict_pitcher_mae','?')}]")

    # Pitcher components
    if metrics.get('ip_mae'):
        lines.append("\n### Pitcher Components")
        lines.append(f"- IP: MAE={metrics.get('ip_mae','?')}, Bias={metrics.get('ip_bias','?'):+.2f}")
        lines.append(f"- Ks: MAE={metrics.get('ks_mae','?')}, Bias={metrics.get('ks_bias','?'):+.2f}")
        lines.append(f"- ER: MAE={metrics.get('er_mae','?')}, Bias={metrics.get('er_bias','?'):+.2f}")

    # Multipliers
    mults = ['pitcher_mult', 'platoon_mult', 'context_mult', 'vegas_mult', 'park_mult', 'weather_mult']
    lines.append("\n### Multiplier Effectiveness")
    for m in mults:
        corr = metrics.get(f'{m}_corr')
        if corr is not None:
            v = 'PASS' if corr > 0.05 else 'FAIL' if corr < -0.05 else 'WARN'
            lines.append(f"- `{m}`: r={corr:+.3f} [{v}]")

    # Ownership
    if metrics.get('own_mae'):
        lines.append(f"\n### Ownership: MAE={metrics['own_mae']}%, Bias={metrics.get('own_bias','?'):+.2f}% [{metrics.get('verdict_ownership','?')}]")

    lines.append("")

    with open(FINDINGS_PATH, 'a', encoding='utf-8') as f:
        f.write('\n'.join(lines))
    print(f"\n  Findings appended to {FINDINGS_PATH}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description='Simulation Validation — post-game sim accuracy diagnostic')
    parser.add_argument('--date', type=str, help='Specific date (YYYY-MM-DD)')
    parser.add_argument('--range', type=str, nargs=2, metavar=('START', 'END'),
                        help='Date range')
    args = parser.parse_args()

    if args.range:
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
        res = sb.table('actual_results').select('game_date').order('game_date', desc=True).limit(1).execute()
        if res.data:
            dates = [res.data[0]['game_date']]
            print(f"  Auto-detected latest date: {dates[0]}")
        else:
            print("  No actual results found.")
            return

    print(f"\n  Simulation Validation")
    print(f"  {'='*56}")
    print(f"  Dates: {', '.join(dates)}")

    data = load_data(dates)

    if not data['matched']:
        print("  No matched projection+actual data found.")
        return

    a = section_a(data)
    b = section_b(data)
    c = section_c(data)
    d = section_d(data)
    e = section_e(data)

    all_metrics = {**a, **b, **c, **d, **e}
    print_verdicts(all_metrics)
    write_findings(dates, all_metrics, data)
    section_f(dates, all_metrics)


if __name__ == '__main__':
    main()
