"""
Ownership Calibration — compare projected ownership vs actual ownership
from DK contest CSVs across the full history.

Filters: only uses large-field GPP contests (1000+ entries) to avoid
small-field ownership skew where top projected players have inflated own%.

Outputs calibration metrics to tasks/research_findings.md and prints
recommendations for tuning sim_ownership.py weights.
"""
import sys
sys.stdout.reconfigure(encoding='utf-8', line_buffering=True)

import os, math
from collections import defaultdict
from dotenv import load_dotenv
load_dotenv()

from supabase import create_client

sb = create_client(os.environ['SUPABASE_URL'], os.environ['SUPABASE_KEY'])

print("=" * 60)
print("  Ownership Calibration")
print("=" * 60)

# ── Step 1: Load actual ownership from Supabase ──────────────────────────
print("\n  Loading actual ownership from actual_ownership table...")
actual_rows = []
offset = 0
while True:
    rows = sb.table('actual_ownership').select(
        'player_id,ownership_pct,game_date'
    ).range(offset, offset + 999).execute().data or []
    actual_rows.extend(rows)
    if len(rows) < 1000:
        break
    offset += 1000

# Group by (player_id, game_date) — average if player appears in multiple DGs same day
actual_by_key = defaultdict(list)
for r in actual_rows:
    pid = r.get('player_id')
    gd  = r.get('game_date')
    pct = r.get('ownership_pct')
    if pid and gd and pct is not None:
        actual_by_key[(pid, gd)].append(pct)

avg_actual_key = {k: sum(v) / len(v) for k, v in actual_by_key.items()}
game_dates = {gd for (_, gd) in avg_actual_key}
contest_count = len(game_dates)
print(f"  Actual ownership records: {len(avg_actual_key)} player-dates across {contest_count} game dates")

# ── Step 2: Load projected ownership from slate_ownership ────────────────
print("  Loading projected ownership from slate_ownership...")
proj_rows = []
offset = 0
while True:
    rows = sb.table('slate_ownership').select(
        'player_id,proj_ownership,game_date'
    ).range(offset, offset + 999).execute().data or []
    proj_rows.extend(rows)
    if len(rows) < 1000:
        break
    offset += 1000

proj_by_key = defaultdict(list)
for r in proj_rows:
    pid = r.get('player_id')
    gd  = r.get('game_date')
    pct = r.get('proj_ownership')
    if pid and gd and pct is not None:
        proj_by_key[(pid, gd)].append(pct)

avg_proj_key = {k: sum(v) / len(v) for k, v in proj_by_key.items()}
print(f"  Projected ownership records: {len(avg_proj_key)} player-dates")

# Load dk_salaries to map player_id → name (for reporting)
pid_to_name = {}
offset = 0
while True:
    rows = sb.table('dk_salaries').select('player_id,name').range(offset, offset + 999).execute().data or []
    for r in rows:
        if r.get('player_id') and r.get('name'):
            pid_to_name[r['player_id']] = r['name']
    if len(rows) < 1000:
        break
    offset += 1000

# ── Step 3: Match on (player_id, game_date) ──────────────────────────────
matched = []
for (pid, gd), proj_val in avg_proj_key.items():
    if (pid, gd) in avg_actual_key:
        matched.append({
            'name':       pid_to_name.get(pid, f'pid:{pid}'),
            'proj_own':   proj_val,
            'actual_own': avg_actual_key[(pid, gd)],
            'delta':      proj_val - avg_actual_key[(pid, gd)],
        })

matched_all = matched
matched = [m for m in matched if m['actual_own'] > 0]
ghost_count = len(matched_all) - len(matched)

print(f"  Matched players: {len(matched_all)} ({ghost_count} filtered as 0%-actual ghosts → {len(matched)} usable)")

if not matched:
    print("  No matches found — actual_ownership or slate_ownership table may be empty")
    sys.exit(1)


def norm(n):
    return n.lower().replace('.', '').replace("'", '').replace('-', ' ').strip()


def pearson_r(xs, ys):
    n = len(xs)
    if n < 2:
        return 0.0
    mx, my = sum(xs) / n, sum(ys) / n
    cov = sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / n
    sx = math.sqrt(sum((x - mx) ** 2 for x in xs) / n)
    sy = math.sqrt(sum((y - my) ** 2 for y in ys) / n)
    return cov / (sx * sy) if sx > 0 and sy > 0 else 0.0


def spearman_r(xs, ys):
    n = len(xs)
    if n < 2:
        return 0.0
    def rank(arr):
        order = sorted(range(n), key=lambda i: arr[i])
        ranks = [0] * n
        for r, i in enumerate(order):
            ranks[i] = r + 1
        return ranks
    return pearson_r(rank(xs), rank(ys))


# ── Step 4: Compute calibration metrics ─────────────────────────────────
deltas = [m['delta'] for m in matched]
abs_deltas = [abs(d) for d in deltas]
proj_vals = [m['proj_own'] for m in matched]
actual_vals = [m['actual_own'] for m in matched]

mean_bias = sum(deltas) / len(deltas)
mae = sum(abs_deltas) / len(abs_deltas)
corr = pearson_r(proj_vals, actual_vals)
rank_corr = spearman_r(proj_vals, actual_vals)

print(f"\n{'=' * 60}")
print(f"  Ownership Calibration Results")
print(f"{'=' * 60}")
print(f"  Matched players: {len(matched)}")
print(f"  Bias: {mean_bias:+.2f}% (positive = we over-project ownership)")
print(f"  MAE:  {mae:.2f}%")
print(f"  Correlation: r={corr:.3f}  Rank rho={rank_corr:.3f} (GPP leverage quality)")

# ── Step 5: Tier + position breakdown ───────────────────────────────────
tiers = {
    'Chalk (>20% actual)': [m for m in matched if m['actual_own'] > 20],
    'Mid (5-20% actual)': [m for m in matched if 5 <= m['actual_own'] <= 20],
    'Low (<5% actual)': [m for m in matched if m['actual_own'] < 5],
}

print(f"\n  By ownership tier:")
for label, group in tiers.items():
    if group:
        tier_bias = sum(m['delta'] for m in group) / len(group)
        tier_mae = sum(abs(m['delta']) for m in group) / len(group)
        tier_r = pearson_r([m['proj_own'] for m in group], [m['actual_own'] for m in group])
        tier_rho = spearman_r([m['proj_own'] for m in group], [m['actual_own'] for m in group])
        print(f"    {label:<25}: n={len(group):3d}, bias={tier_bias:+.2f}%, MAE={tier_mae:.2f}%, r={tier_r:.3f}, rho={tier_rho:.3f}")

# SP vs hitter breakdown — load position data from slate_ownership + dk_salaries
print(f"\n  By player type (SP vs hitter):")
sal_pos = {}
offset = 0
while True:
    rows = sb.table('dk_salaries').select('player_id,name,position').range(offset, offset + 999).execute().data or []
    for r in rows:
        if r.get('name') and r.get('position'):
            sal_pos[norm(r['name'])] = r['position']
    if len(rows) < 1000:
        break
    offset += 1000

for pname, m in zip([m['name'] for m in matched], matched):
    pos_str = sal_pos.get(norm(pname), '')
    m['is_sp'] = 'SP' in pos_str or pos_str == 'P'

sps = [m for m in matched if m['is_sp']]
hitters = [m for m in matched if not m['is_sp']]
for label, group in [('SPs', sps), ('Hitters', hitters)]:
    if group:
        gb = sum(m['delta'] for m in group) / len(group)
        gm = sum(abs(m['delta']) for m in group) / len(group)
        gr = pearson_r([m['proj_own'] for m in group], [m['actual_own'] for m in group])
        print(f"    {label:<10}: n={len(group):3d}, bias={gb:+.2f}%, MAE={gm:.2f}%, r={gr:.3f}")

# ── Step 6: Biggest misses (excluding 0%-actual ghosts already filtered) ──
print(f"\n  Biggest over-projections (we said high, actual was low):")
over = sorted(matched, key=lambda m: m['delta'], reverse=True)[:8]
for m in over:
    print(f"    {m['name']:25s} proj={m['proj_own']:5.1f}%  actual={m['actual_own']:5.1f}%  delta={m['delta']:+.1f}%")

print(f"\n  Biggest under-projections (we said low, actual was high):")
under = sorted(matched, key=lambda m: m['delta'])[:8]
for m in under:
    print(f"    {m['name']:25s} proj={m['proj_own']:5.1f}%  actual={m['actual_own']:5.1f}%  delta={m['delta']:+.1f}%")

# ── Step 7: Write to research_findings ──────────────────────────────────
findings_path = os.path.join(os.path.dirname(__file__), 'tasks', 'research_findings.md')
with open(findings_path, 'a', encoding='utf-8') as f:
    f.write(f"\n\n## Ownership Calibration — {contest_count} game dates\n\n")
    f.write(f"- **Matched players**: {len(matched)} ({ghost_count} 0%-actual ghosts excluded)\n")
    f.write(f"- **Bias**: {mean_bias:+.2f}% (positive = over-project ownership)\n")
    f.write(f"- **MAE**: {mae:.2f}%\n")
    f.write(f"- **Correlation**: r={corr:.3f}  Rank rho={rank_corr:.3f} (GPP leverage quality)\n\n")
    for label, group in tiers.items():
        if group:
            tier_bias = sum(m['delta'] for m in group) / len(group)
            tier_mae = sum(abs(m['delta']) for m in group) / len(group)
            f.write(f"- {label}: n={len(group)}, bias={tier_bias:+.2f}%, MAE={tier_mae:.2f}%\n")
    f.write(f"\n**Over-projected:**\n")
    for m in over[:5]:
        f.write(f"- {m['name']}: proj={m['proj_own']:.1f}% actual={m['actual_own']:.1f}%\n")
    f.write(f"\n**Under-projected:**\n")
    for m in under[:5]:
        f.write(f"- {m['name']}: proj={m['proj_own']:.1f}% actual={m['actual_own']:.1f}%\n")

print(f"\n  Results appended to tasks/research_findings.md")

# ── Step 8: Compute per-tier RMSE and write cache ───────────────────────────
# Tier by projected ownership (what we know at generation time)
import json as _json
tier_sq_errs = {'low': [], 'mid': [], 'high': [], 'chalk': []}
for m in matched:
    proj = m['proj_own']
    actual = m['actual_own']
    sq_err = (proj - actual) ** 2
    if proj >= 30:
        tier_sq_errs['chalk'].append(sq_err)
    elif proj >= 20:
        tier_sq_errs['high'].append(sq_err)
    elif proj >= 10:
        tier_sq_errs['mid'].append(sq_err)
    else:
        tier_sq_errs['low'].append(sq_err)

default_rmse = {'low': 7.0, 'mid': 5.5, 'high': 4.5, 'chalk': 3.5}
rmse_cache = {}
for tier, sq_errs in tier_sq_errs.items():
    rmse_cache[tier] = round(math.sqrt(sum(sq_errs) / len(sq_errs)), 2) if sq_errs else default_rmse[tier]

cache_path = os.path.join(os.path.dirname(__file__), 'ownership_rmse_cache.json')
with open(cache_path, 'w') as f:
    _json.dump(rmse_cache, f, indent=2)

print(f"\n  Ownership RMSE by projected tier (used for leverage uncertainty in pool gen):")
for tier, rmse_val in rmse_cache.items():
    n = len(tier_sq_errs[tier])
    print(f"    {tier:<8}: RMSE={rmse_val:.2f}%  (n={n})")
print(f"  Saved to ownership_rmse_cache.json")

print(f"\n  Done.")
