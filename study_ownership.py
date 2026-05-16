#!/usr/bin/env python3
"""
study_ownership.py — Ownership Model Accuracy Study

Joins actual_ownership (post-game truth) with slate_ownership (our projections)
across the full historical dataset, then runs:

  1. Accuracy breakdown across 7 dimensions (tier, position, salary, BO, total, slate, overall)
  2. Feature correlation analysis — validates whether current weights are still right
  3. Per-player systematic bias — who does the model consistently miss?
  4. Grid-search weight optimization — find weights that minimize MAE
  5. Global scale calibration check — should OWN_GLOBAL_SCALE still be 0.95?
  6. Recommendations for sim_ownership.py

Output: printed to console + appended to tasks/research_findings.md

Usage:
  py -3.12 study_ownership.py
"""

import sys
sys.stdout.reconfigure(encoding='utf-8', line_buffering=True)

import os, math, itertools
from collections import defaultdict
from datetime import datetime
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()
sb = create_client(os.environ['SUPABASE_URL'], os.environ['SUPABASE_KEY'])

# ── Current model constants (copy from sim_ownership.py for comparison) ───────
W_PROJ_CUR      = 0.35
W_SALARY_CUR    = 0.25
W_ENV_CUR       = 0.20
W_BAT_ORDER_CUR = 0.15
W_VALUE_CUR     = 0.05
PITCHER_BOOST   = 3.0
OWN_GLOBAL_SCALE_CUR = 0.95
POS_SLOTS = {'SP': 2, 'C': 1, '1B': 1, '2B': 1, '3B': 1, 'SS': 1, 'OF': 3}
SOFTMAX_TEMP_CUR = {'SP': 0.40, 'C': 0.50, '1B': 0.50, '2B': 0.50,
                    '3B': 0.50, 'SS': 0.50, 'OF': 0.50}

SEPARATOR = '=' * 65


def safe(v, default=0.0):
    if v is None:
        return default
    try:
        f = float(v)
        return default if (math.isnan(f) or math.isinf(f)) else f
    except (TypeError, ValueError):
        return default


def paginate(table, select_cols, filters=None):
    rows = []
    offset = 0
    while True:
        q = sb.table(table).select(select_cols)
        if filters:
            for f in filters:
                q = getattr(q, f[0])(*f[1:])
        batch = q.range(offset, offset + 999).execute().data or []
        rows.extend(batch)
        if len(batch) < 1000:
            break
        offset += 1000
    return rows


def pearson_r(xs, ys):
    n = len(xs)
    if n < 2:
        return 0.0
    mx = sum(xs) / n
    my = sum(ys) / n
    cov = sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / n
    sx = math.sqrt(sum((x - mx) ** 2 for x in xs) / n)
    sy = math.sqrt(sum((y - my) ** 2 for y in ys) / n)
    return cov / (sx * sy) if sx > 0 and sy > 0 else 0.0


def spearman_r(xs, ys):
    n = len(xs)
    if n < 2:
        return 0.0
    rx = [sorted(xs).index(x) for x in xs]
    ry = [sorted(ys).index(y) for y in ys]
    d2 = sum((a - b) ** 2 for a, b in zip(rx, ry))
    return 1 - 6 * d2 / (n * (n**2 - 1))


def softmax(scores, temperature=1.0):
    if not scores:
        return []
    scaled = [s / temperature for s in scores]
    max_s = max(scaled)
    exps = [math.exp(s - max_s) for s in scaled]
    total = sum(exps)
    return [e / total for e in exps] if total > 0 else [1.0 / len(scores)] * len(scores)


def stats_for(group, key='delta'):
    vals = [r[key] for r in group if r.get(key) is not None]
    if not vals:
        return {'n': 0, 'bias': None, 'mae': None, 'r': None}
    n = len(vals)
    bias = sum(vals) / n
    mae = sum(abs(v) for v in vals) / n
    projs = [r['proj_own'] for r in group]
    acts = [r['actual_own'] for r in group]
    r = pearson_r(projs, acts)
    return {'n': n, 'bias': bias, 'mae': mae, 'r': r}


def print_group_table(title, groups):
    print(f"\n  {title}")
    print(f"  {'Group':<28} {'n':>4}  {'Bias':>7}  {'MAE':>6}  {'r':>6}")
    print(f"  {'-'*28} {'----':>4}  {'-------':>7}  {'------':>6}  {'------':>6}")
    for label, s in groups:
        if s['n'] == 0:
            continue
        print(f"  {label:<28} {s['n']:>4}  {s['bias']:>+6.2f}%  {s['mae']:>5.2f}%  {s['r']:>6.3f}")


# ── STEP 1: Data Pull ─────────────────────────────────────────────────────────
print(f"\n{SEPARATOR}")
print("  Ownership Study — Data Pull")
print(SEPARATOR)

print("  Loading actual_ownership...")
actual_rows = paginate('actual_ownership',
    'player_id,ownership_pct,game_date,dk_slate,position,salary',
    [('gt', 'ownership_pct', 0)])
print(f"  actual_ownership: {len(actual_rows):,} rows")

print("  Loading slate_ownership...")
proj_rows = paginate('slate_ownership',
    'player_id,proj_ownership,game_date,dk_slate')
print(f"  slate_ownership: {len(proj_rows):,} rows")

print("  Loading player_projections...")
proj_enrich = paginate('player_projections',
    'player_id,game_date,game_pk,proj_dk_pts,batting_order,is_pitcher')
print(f"  player_projections: {len(proj_enrich):,} rows")

print("  Loading game_odds...")
odds_rows = paginate('game_odds', 'game_pk,game_total,home_implied,away_implied')
odds_by_pk = {r['game_pk']: r for r in odds_rows}
print(f"  game_odds: {len(odds_by_pk):,} games")

print("  Loading dk_salaries...")
sal_rows = paginate('dk_salaries',
    'player_id,salary,position,dk_slate',
    [('eq', 'contest_type', 'classic')])
print(f"  dk_salaries: {len(sal_rows):,} rows")


# ── STEP 2: Build lookup maps and join ───────────────────────────────────────
print("\n  Building lookup maps...")

# proj_own lookup: (player_id, game_date, dk_slate) → proj_ownership
proj_map = {}
for r in proj_rows:
    key = (r['player_id'], r['game_date'], r.get('dk_slate', 'main'))
    proj_map[key] = safe(r['proj_ownership'])
    # fallback: also index without slate for 'unknown' actuals
    key2 = (r['player_id'], r['game_date'])
    if key2 not in proj_map:
        proj_map[key2] = safe(r['proj_ownership'])

# enrichment: (player_id, game_date) → {proj_dk_pts, batting_order, game_pk, is_pitcher}
enrich_map = {}
for r in proj_enrich:
    key = (r['player_id'], r['game_date'])
    if key not in enrich_map or (r.get('proj_dk_pts') or 0) > (enrich_map[key].get('proj_dk_pts') or 0):
        enrich_map[key] = r

# game_pk → game_total
# enrich_map already has game_pk; join to odds
def get_game_total(player_id, game_date):
    e = enrich_map.get((player_id, game_date), {})
    gpk = e.get('game_pk')
    if gpk:
        o = odds_by_pk.get(gpk, {})
        return safe(o.get('game_total'), 8.5)
    return 8.5

# salary: (player_id, dk_slate) → salary and position
sal_map = {}
for r in sal_rows:
    key = (r['player_id'], r.get('dk_slate', 'main'))
    if key not in sal_map:
        sal_map[key] = r
    key2 = (r['player_id'],)
    if key2 not in sal_map:
        sal_map[key2] = r

# ── Inner join ────────────────────────────────────────────────────────────────
print("  Joining actual vs projected...")
matched = []
for a in actual_rows:
    pid = a['player_id']
    gdate = a['game_date']
    slate = a.get('dk_slate') or 'main'

    # Try slate-specific first, fall back to date-only
    proj_own = proj_map.get((pid, gdate, slate))
    if proj_own is None:
        proj_own = proj_map.get((pid, gdate))
    if proj_own is None:
        continue

    actual_own = safe(a['ownership_pct'])
    if actual_own <= 0:
        continue

    e = enrich_map.get((pid, gdate), {})
    proj_pts = safe(e.get('proj_dk_pts'), 0)
    batting_order = e.get('batting_order')
    is_pitcher = e.get('is_pitcher', False)
    game_pk = e.get('game_pk')

    # Salary and position — prefer from actual_ownership, fall back to dk_salaries
    salary = safe(a.get('salary'), 0)
    position = (a.get('position') or '').strip()
    if not salary:
        sr = sal_map.get((pid, slate)) or sal_map.get((pid,))
        if sr:
            salary = safe(sr.get('salary'), 0)
    if not position:
        sr = sal_map.get((pid, slate)) or sal_map.get((pid,))
        if sr:
            position = (sr.get('position') or '').strip()

    # Canonical position
    if is_pitcher or 'SP' in position:
        canon_pos = 'SP'
    elif 'C' in position:
        canon_pos = 'C'
    elif 'SS' in position:
        canon_pos = 'SS'
    elif '2B' in position:
        canon_pos = '2B'
    elif '3B' in position:
        canon_pos = '3B'
    elif '1B' in position:
        canon_pos = '1B'
    elif 'OF' in position:
        canon_pos = 'OF'
    else:
        canon_pos = 'OF'

    game_total = get_game_total(pid, gdate)

    # Features for correlation analysis
    proj_score = proj_pts ** 1.5 if proj_pts > 0 else 0
    salary_score = (salary / 3500) ** 1.3 if salary > 0 else 0
    value = (proj_pts / salary * 1000) if salary > 0 and proj_pts > 0 else 0
    value_score = value ** 0.6 if value > 0 else 0

    if canon_pos == 'SP':
        env_score = max(0.0, min(1.0, 1.5 - (game_total / 2.0) / 4.5))
    else:
        env_score = (game_total / 8.5) ** 2.0

    bo_map = {1: 1.30, 2: 1.25, 3: 1.20, 4: 1.15, 5: 1.05,
              6: 0.90, 7: 0.80, 8: 0.70, 9: 0.60}
    bo_score = bo_map.get(batting_order, 1.0) if batting_order else 1.0

    matched.append({
        'player_id':    pid,
        'game_date':    gdate,
        'dk_slate':     slate,
        'actual_own':   actual_own,
        'proj_own':     proj_own,
        'delta':        proj_own - actual_own,
        'canon_pos':    canon_pos,
        'salary':       salary,
        'proj_dk_pts':  proj_pts,
        'game_total':   game_total,
        'batting_order': batting_order,
        'is_pitcher':   is_pitcher or (canon_pos == 'SP'),
        # raw feature scores for correlation
        'proj_score':   proj_score,
        'salary_score': salary_score,
        'value_score':  value_score,
        'env_score':    env_score,
        'bo_score':     bo_score,
        'game_pk':      game_pk,
    })

dates = sorted({r['game_date'] for r in matched})
print(f"  Matched: {len(matched):,} player-slate records across {len(dates)} dates")

if len(matched) < 30:
    print("\n  Insufficient matched data for study — need ≥30 records.")
    print("  Run --postgame pipeline on more dates to accumulate actual_ownership data.")
    sys.exit(0)


# ── STEP 3: Accuracy Breakdown ────────────────────────────────────────────────
print(f"\n{SEPARATOR}")
print("  Accuracy Breakdown")
print(SEPARATOR)

# Overall
ov = stats_for(matched)
print(f"\n  Overall: n={ov['n']}, bias={ov['bias']:+.2f}%, MAE={ov['mae']:.2f}%, r={ov['r']:.3f}")

# By ownership tier (based on actual)
tiers = [
    ('Chalk  (actual >20%)',  [r for r in matched if r['actual_own'] > 20]),
    ('Mid    (actual 5-20%)', [r for r in matched if 5 <= r['actual_own'] <= 20]),
    ('Low    (actual <5%)',   [r for r in matched if r['actual_own'] < 5]),
]
print_group_table("By Ownership Tier", [(lbl, stats_for(g)) for lbl, g in tiers])

# By position
positions = sorted({r['canon_pos'] for r in matched})
pos_groups = [(pos, [r for r in matched if r['canon_pos'] == pos]) for pos in positions]
print_group_table("By Position", [(pos, stats_for(g)) for pos, g in pos_groups])

# By salary bucket
def sal_bucket(s):
    if s >= 9000: return '$9K+'
    if s >= 7000: return '$7K-9K'
    if s >= 5000: return '$5K-7K'
    return '<$5K'
sal_buckets_order = ['$9K+', '$7K-9K', '$5K-7K', '<$5K']
sal_groups = [(b, [r for r in matched if sal_bucket(r['salary']) == b and r['salary'] > 0])
              for b in sal_buckets_order]
print_group_table("By Salary Bucket", [(b, stats_for(g)) for b, g in sal_groups])

# By batting order
def bo_bucket(bo):
    if bo is None: return None
    if bo <= 2: return 'Leadoff (1-2)'
    if bo <= 5: return 'Middle (3-5)'
    return 'Bottom (6-9)'
bo_buckets_order = ['Leadoff (1-2)', 'Middle (3-5)', 'Bottom (6-9)']
hitters_only = [r for r in matched if not r['is_pitcher'] and r['batting_order']]
bo_groups = [(b, [r for r in hitters_only if bo_bucket(r['batting_order']) == b])
             for b in bo_buckets_order]
print_group_table("By Batting Order (hitters only)", [(b, stats_for(g)) for b, g in bo_groups])

# By game total
def total_bucket(gt):
    if gt < 8.5: return 'Low (<8.5)'
    if gt <= 9.5: return 'Medium (8.5-9.5)'
    return 'High (>9.5)'
total_buckets_order = ['Low (<8.5)', 'Medium (8.5-9.5)', 'High (>9.5)']
total_groups = [(b, [r for r in matched if total_bucket(r['game_total']) == b])
                for b in total_buckets_order]
print_group_table("By Game Total", [(b, stats_for(g)) for b, g in total_groups])

# By slate type
slates_present = sorted({r['dk_slate'] for r in matched})
slate_groups = [(s, [r for r in matched if r['dk_slate'] == s]) for s in slates_present]
print_group_table("By Slate Type", [(s, stats_for(g)) for s, g in slate_groups])

# SP vs Hitter summary
sp_group = [r for r in matched if r['is_pitcher']]
hit_group = [r for r in matched if not r['is_pitcher']]
print_group_table("SP vs Hitters", [('SP', stats_for(sp_group)), ('Hitters', stats_for(hit_group))])


# ── STEP 4: Feature Correlation Analysis ─────────────────────────────────────
print(f"\n{SEPARATOR}")
print("  Feature Correlation Analysis (vs actual_ownership_pct)")
print(SEPARATOR)

acts = [r['actual_own'] for r in matched]
features = [
    ('proj_dk_pts (raw)',   [r['proj_dk_pts']    for r in matched]),
    ('proj_dk_pts^1.5',    [r['proj_score']      for r in matched]),
    ('salary (raw)',       [r['salary']          for r in matched]),
    ('salary^1.3',        [r['salary_score']    for r in matched]),
    ('game_total',        [r['game_total']      for r in matched]),
    ('batting_order',     [r['batting_order'] or 5 for r in matched]),
    ('bo_score (mapped)', [r['bo_score']        for r in matched]),
    ('env_score',         [r['env_score']       for r in matched]),
    ('value (PPK)',       [r['proj_dk_pts'] / r['salary'] * 1000 if r['salary'] > 0 else 0
                           for r in matched]),
    ('value_score^0.6',   [r['value_score']     for r in matched]),
]

initial_r = {
    'proj_dk_pts (raw)':   0.557,
    'salary (raw)':        0.536,
    'batting_order':       0.203,
    'value (PPK)':        -0.055,
}

print(f"\n  {'Feature':<26} {'Current r':>9}  {'Initial r':>9}  {'Change':>8}")
print(f"  {'-'*26} {'---------':>9}  {'---------':>9}  {'--------':>8}")
correlations = {}
for name, vals in features:
    valid = [(v, a) for v, a in zip(vals, acts) if v is not None and a is not None]
    if len(valid) < 10:
        continue
    vs, as_ = zip(*valid)
    r = pearson_r(list(vs), list(as_))
    correlations[name] = r
    init = initial_r.get(name)
    init_str = f'{init:+.3f}' if init is not None else '   N/A'
    delta_str = f'{r - init:+.3f}' if init is not None else '      '
    print(f"  {name:<26} {r:>+9.3f}  {init_str:>9}  {delta_str:>8}")


# ── STEP 5: Per-Player Systematic Bias ───────────────────────────────────────
print(f"\n{SEPARATOR}")
print("  Per-Player Systematic Bias (≥3 appearances)")
print(SEPARATOR)

player_records = defaultdict(list)
player_names = {}
for r in matched:
    player_records[r['player_id']].append(r)

player_bias = []
for pid, recs in player_records.items():
    if len(recs) < 3:
        continue
    mean_delta = sum(r['delta'] for r in recs) / len(recs)
    mean_actual = sum(r['actual_own'] for r in recs) / len(recs)
    mean_proj = sum(r['proj_own'] for r in recs) / len(recs)
    name = recs[0].get('name') or str(pid)
    pos = recs[0]['canon_pos']
    player_bias.append({
        'player_id': pid, 'name': name, 'pos': pos,
        'n': len(recs), 'mean_delta': mean_delta,
        'mean_proj': mean_proj, 'mean_actual': mean_actual,
    })

# Enrich with names from dk_salaries
pid_to_name = {}
for r in sal_rows:
    if r.get('player_id') and r.get('name'):
        pid_to_name[r['player_id']] = r['name']
for p in player_bias:
    if p['player_id'] in pid_to_name:
        p['name'] = pid_to_name[p['player_id']]

player_bias.sort(key=lambda p: p['mean_delta'], reverse=True)

print(f"\n  Most over-projected (proj >> actual):")
print(f"  {'Player':<25} {'Pos':>4}  {'n':>3}  {'Proj':>6}  {'Actual':>6}  {'Bias':>7}")
print(f"  {'-'*25} {'----':>4}  {'---':>3}  {'------':>6}  {'------':>6}  {'-------':>7}")
for p in player_bias[:15]:
    print(f"  {p['name']:<25} {p['pos']:>4}  {p['n']:>3}  {p['mean_proj']:>5.1f}%  {p['mean_actual']:>5.1f}%  {p['mean_delta']:>+6.1f}%")

print(f"\n  Most under-projected (actual >> proj):")
print(f"  {'Player':<25} {'Pos':>4}  {'n':>3}  {'Proj':>6}  {'Actual':>6}  {'Bias':>7}")
print(f"  {'-'*25} {'----':>4}  {'---':>3}  {'------':>6}  {'------':>6}  {'-------':>7}")
for p in reversed(player_bias[-15:]):
    print(f"  {p['name']:<25} {p['pos']:>4}  {p['n']:>3}  {p['mean_proj']:>5.1f}%  {p['mean_actual']:>5.1f}%  {p['mean_delta']:>+6.1f}%")

candidates = [p for p in player_bias if abs(p['mean_delta']) > 3.0 and p['n'] >= 5]
print(f"\n  Players with |bias| >3% and ≥5 appearances: {len(candidates)}")
if len(candidates) >= 20:
    print("  → RECOMMEND adding PLAYER_BIAS_CORRECTIONS dict to sim_ownership.py")


# ── STEP 6: Grid Search Weight Optimization ───────────────────────────────────
print(f"\n{SEPARATOR}")
print("  Weight Grid Search (W_VALUE fixed at 0.05)")
print(SEPARATOR)

# Group matched records by (game_date, dk_slate, canon_pos) for softmax evaluation
groups_by_key = defaultdict(list)
for r in matched:
    key = (r['game_date'], r['dk_slate'], r['canon_pos'])
    groups_by_key[key].append(r)

# Filter groups with at least 2 players (softmax needs >1)
eval_groups = {k: v for k, v in groups_by_key.items() if len(v) >= 2}
print(f"  Evaluating on {len(eval_groups)} position-date-slate groups...")


def compute_mae_for_weights(wp, ws, we, wb, wv=0.05):
    """
    Recompute ownership with candidate weights and return MAE against actuals.
    Uses log-softmax per position group per date/slate.
    """
    total_err = 0.0
    total_n = 0
    for (gdate, slate, pos), group in eval_groups.items():
        slots = POS_SLOTS.get(pos, 1)
        target_sum = slots * 100.0
        temp = SOFTMAX_TEMP_CUR.get(pos, 0.50)

        scores = []
        for r in group:
            ps = r['proj_score']
            ss = r['salary_score']
            vs = r['value_score']
            es = r['env_score']
            bs = r['bo_score']

            base = ps * wp + ss * ws + vs * wv + es * we + bs * wb
            if r['is_pitcher']:
                base *= PITCHER_BOOST
            base = max(base, 0.001)
            scores.append(math.log(base))

        shares = softmax(scores, temperature=temp)
        raw_own = [s * target_sum for s in shares]

        for i, r in enumerate(group):
            pred = raw_own[i] * OWN_GLOBAL_SCALE_CUR
            total_err += abs(pred - r['actual_own'])
            total_n += 1

    return total_err / total_n if total_n > 0 else 999.0


# Baseline MAE with current weights
current_mae = compute_mae_for_weights(
    W_PROJ_CUR, W_SALARY_CUR, W_ENV_CUR, W_BAT_ORDER_CUR, W_VALUE_CUR)
print(f"  Current weights MAE: {current_mae:.3f}%")
print(f"  Searching weight space (this may take ~30s)...")

# Grid: W_PROJ 0.20-0.55, W_SALARY 0.10-0.45, W_ENV 0.05-0.35, W_BAT_ORDER 0.05-0.30
# W_VALUE fixed at 0.05; remaining 4 must sum to 0.95
step = 0.05
candidates_w = []
for wp in [round(x * step, 2) for x in range(4, 12)]:   # 0.20 .. 0.55
    for ws in [round(x * step, 2) for x in range(2, 10)]: # 0.10 .. 0.45
        for we in [round(x * step, 2) for x in range(1, 8)]:  # 0.05 .. 0.35
            wb = round(0.95 - wp - ws - we, 2)
            if wb < 0.05 or wb > 0.30:
                continue
            if abs(wp + ws + we + wb - 0.95) > 0.001:
                continue
            candidates_w.append((wp, ws, we, wb))

print(f"  Testing {len(candidates_w)} weight combinations...")

best_mae = current_mae
best_weights = (W_PROJ_CUR, W_SALARY_CUR, W_ENV_CUR, W_BAT_ORDER_CUR)
for wp, ws, we, wb in candidates_w:
    mae = compute_mae_for_weights(wp, ws, we, wb)
    if mae < best_mae:
        best_mae = mae
        best_weights = (wp, ws, we, wb)

improvement = current_mae - best_mae
print(f"\n  Best weights found:")
print(f"    W_PROJ      = {best_weights[0]:.2f}  (current: {W_PROJ_CUR:.2f})")
print(f"    W_SALARY    = {best_weights[1]:.2f}  (current: {W_SALARY_CUR:.2f})")
print(f"    W_ENV       = {best_weights[2]:.2f}  (current: {W_ENV_CUR:.2f})")
print(f"    W_BAT_ORDER = {best_weights[3]:.2f}  (current: {W_BAT_ORDER_CUR:.2f})")
print(f"    W_VALUE     = 0.05  (fixed)")
print(f"  Best MAE: {best_mae:.3f}%  (improvement: {improvement:+.3f}%)")

if improvement >= 0.3:
    print(f"  → MATERIAL improvement — RECOMMEND updating weights in sim_ownership.py")
elif improvement >= 0.05:
    print(f"  → Minor improvement ({improvement:.3f}%) — weights are close to optimal")
else:
    print(f"  → No meaningful improvement — current weights are well-calibrated")


# ── STEP 7: Global Scale Calibration Check ────────────────────────────────────
print(f"\n{SEPARATOR}")
print("  Global Scale Calibration Check")
print(SEPARATOR)

# The matched data has proj_own already with OWN_GLOBAL_SCALE applied.
# Compute what scale would minimize bias.
# bias = mean(proj_own - actual_own) — if positive, scale should be lower
total_bias = sum(r['delta'] for r in matched) / len(matched)
optimal_scale = OWN_GLOBAL_SCALE_CUR - (total_bias / (sum(r['proj_own'] for r in matched) / len(matched)))
optimal_scale = max(0.5, min(1.5, OWN_GLOBAL_SCALE_CUR * (1 - total_bias / max(0.01, sum(r['proj_own'] for r in matched) / len(matched)))))

print(f"  Current OWN_GLOBAL_SCALE: {OWN_GLOBAL_SCALE_CUR:.2f}")
print(f"  Residual mean bias across all matched data: {total_bias:+.2f}%")

# Compute by type
sp_bias = sum(r['delta'] for r in sp_group) / len(sp_group) if sp_group else 0
hit_bias = sum(r['delta'] for r in hit_group) / len(hit_group) if hit_group else 0
print(f"  SP bias: {sp_bias:+.2f}%")
print(f"  Hitter bias: {hit_bias:+.2f}%")

if abs(total_bias) > 1.0:
    direction = 'DOWN' if total_bias > 0 else 'UP'
    print(f"  → Bias of {total_bias:+.2f}% warrants adjusting OWN_GLOBAL_SCALE {direction}")
    print(f"  → Suggested: OWN_GLOBAL_SCALE = {OWN_GLOBAL_SCALE_CUR * (1 - total_bias / 20):.2f}")
else:
    print(f"  → Bias within ±1% — OWN_GLOBAL_SCALE = {OWN_GLOBAL_SCALE_CUR:.2f} is still appropriate")

# SP softmax temperature check — if SP bias is large, temperature needs adjustment
print(f"\n  Softmax temperature check (SP):")
print(f"  Current SP temp: {SOFTMAX_TEMP_CUR['SP']:.2f}")
if sp_group:
    sp_chalks = [r for r in sp_group if r['actual_own'] > 25]
    sp_mids = [r for r in sp_group if 10 <= r['actual_own'] <= 25]
    if sp_chalks:
        chalk_bias = sum(r['delta'] for r in sp_chalks) / len(sp_chalks)
        print(f"  Top SP (>25% actual): n={len(sp_chalks)}, bias={chalk_bias:+.2f}%")
        if chalk_bias > 3:
            print(f"  → Top SPs over-projected — consider raising SP temp (lower concentration)")
        elif chalk_bias < -3:
            print(f"  → Top SPs under-projected — consider lowering SP temp (higher concentration)")
    if sp_mids:
        mid_bias = sum(r['delta'] for r in sp_mids) / len(sp_mids)
        print(f"  Mid SP (10-25% actual): n={len(sp_mids)}, bias={mid_bias:+.2f}%")


# ── STEP 8: Summary & Recommendations ────────────────────────────────────────
print(f"\n{SEPARATOR}")
print("  RECOMMENDATIONS FOR sim_ownership.py")
print(SEPARATOR)

recs = []

if improvement >= 0.3:
    recs.append(f"UPDATE WEIGHTS: W_PROJ={best_weights[0]}, W_SALARY={best_weights[1]}, "
                f"W_ENV={best_weights[2]}, W_BAT_ORDER={best_weights[3]}")
else:
    recs.append(f"KEEP WEIGHTS: current weights within {improvement:.3f}% of optimal")

if abs(total_bias) > 1.0:
    new_scale = round(OWN_GLOBAL_SCALE_CUR * (1 - total_bias / 20), 2)
    recs.append(f"UPDATE OWN_GLOBAL_SCALE: {OWN_GLOBAL_SCALE_CUR} → {new_scale}  (residual bias={total_bias:+.2f}%)")
else:
    recs.append(f"KEEP OWN_GLOBAL_SCALE: {OWN_GLOBAL_SCALE_CUR} (bias={total_bias:+.2f}%, within tolerance)")

if candidates and len(candidates) >= 20:
    recs.append(f"ADD PLAYER_BIAS_CORRECTIONS: {len(candidates)} players with |bias|>3% and ≥5 appearances")
else:
    recs.append(f"PLAYER CORRECTIONS: insufficient data ({len(candidates)} players qualify) — skip for now")

for i, rec in enumerate(recs, 1):
    print(f"\n  {i}. {rec}")


# ── STEP 9: Write to research_findings.md ────────────────────────────────────
findings_path = os.path.join(os.path.dirname(__file__), 'tasks', 'research_findings.md')
run_date = datetime.now().strftime('%Y-%m-%d')

with open(findings_path, 'a', encoding='utf-8') as f:
    f.write(f"\n\n## Ownership Study — {run_date} ({len(matched):,} player-slate records, {len(dates)} dates)\n\n")

    f.write(f"### Overall Accuracy\n")
    f.write(f"- **Matched records**: {ov['n']:,}\n")
    f.write(f"- **Bias**: {ov['bias']:+.2f}% (positive = over-project)\n")
    f.write(f"- **MAE**: {ov['mae']:.2f}%\n")
    f.write(f"- **Correlation**: r={ov['r']:.3f}\n\n")

    f.write(f"### By Tier\n")
    for lbl, g in tiers:
        s = stats_for(g)
        if s['n']:
            f.write(f"- {lbl}: n={s['n']}, bias={s['bias']:+.2f}%, MAE={s['mae']:.2f}%, r={s['r']:.3f}\n")

    f.write(f"\n### Feature Correlations\n")
    for name, r in correlations.items():
        init = initial_r.get(name)
        init_s = f" (was {init:+.3f})" if init is not None else ''
        f.write(f"- {name}: r={r:+.3f}{init_s}\n")

    f.write(f"\n### Weight Optimization\n")
    f.write(f"- Current MAE: {current_mae:.3f}%\n")
    f.write(f"- Best MAE: {best_mae:.3f}% (improvement: {improvement:+.3f}%)\n")
    if improvement >= 0.3:
        f.write(f"- **Best weights**: PROJ={best_weights[0]}, SAL={best_weights[1]}, "
                f"ENV={best_weights[2]}, BO={best_weights[3]}\n")

    f.write(f"\n### Recommendations\n")
    for rec in recs:
        f.write(f"- {rec}\n")

    f.write(f"\n### Top Over-Projected Players\n")
    for p in player_bias[:10]:
        f.write(f"- {p['name']} ({p['pos']}): proj={p['mean_proj']:.1f}% actual={p['mean_actual']:.1f}%  "
                f"bias={p['mean_delta']:+.1f}%  n={p['n']}\n")

    f.write(f"\n### Top Under-Projected Players\n")
    for p in list(reversed(player_bias[-10:])):
        f.write(f"- {p['name']} ({p['pos']}): proj={p['mean_proj']:.1f}% actual={p['mean_actual']:.1f}%  "
                f"bias={p['mean_delta']:+.1f}%  n={p['n']}\n")

print(f"\n  Results appended to tasks/research_findings.md")
print(f"\n{SEPARATOR}")
print("  Study complete.")
print(SEPARATOR)
