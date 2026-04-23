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

import csv, glob, os, math
from collections import defaultdict
from dotenv import load_dotenv
load_dotenv()

from supabase import create_client

sb = create_client(os.environ['SUPABASE_URL'], os.environ['SUPABASE_KEY'])

CONTEST_DIR = os.path.join(os.path.dirname(__file__), '..', 'Contest_CSVs')
if not os.path.exists(CONTEST_DIR):
    CONTEST_DIR = r"C:\Users\Steffen's PC\Desktop\WebDev\Contest_CSVs"

MIN_ENTRIES = 1000  # only large-field GPPs (avoids small-field skew)

print("=" * 60)
print("  Ownership Calibration")
print("=" * 60)

# ── Step 1: Parse all contest CSVs for actual ownership ─────────────────
files = glob.glob(os.path.join(CONTEST_DIR, 'contest-standings-*.csv'))
print(f"\n  Found {len(files)} contest CSV files")

# Aggregate actual ownership by player name across large-field contests
actual_own = defaultdict(list)  # player_name → [own%, own%, ...]
contest_count = 0
skipped_small = 0

for fpath in files:
    try:
        with open(fpath, encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            players = {}
            entries = set()
            for row in reader:
                entries.add(row.get('EntryId'))
                name = (row.get('Player') or '').strip()
                pct_raw = (row.get('%Drafted') or '').replace('%', '').strip()
                if name and pct_raw:
                    try:
                        players[name] = float(pct_raw)
                    except ValueError:
                        pass

            if len(entries) < MIN_ENTRIES:
                skipped_small += 1
                continue

            contest_count += 1
            for name, pct in players.items():
                actual_own[name].append(pct)
    except Exception as e:
        print(f"  Error reading {os.path.basename(fpath)}: {e}")

print(f"  Large-field contests (≥{MIN_ENTRIES} entries): {contest_count}")
print(f"  Skipped small-field: {skipped_small}")
print(f"  Unique players with actual ownership: {len(actual_own)}")

# Average actual ownership per player
avg_actual = {name: sum(vals) / len(vals) for name, vals in actual_own.items()}

# ── Step 2: Load our projected ownership from slate_ownership ───────────
print("\n  Loading projected ownership from slate_ownership...")
proj_rows = []
offset = 0
while True:
    rows = sb.table('slate_ownership').select(
        'player_id,proj_ownership,dk_slate,game_date'
    ).range(offset, offset + 999).execute().data or []
    proj_rows.extend(rows)
    if len(rows) < 1000:
        break
    offset += 1000

# Load dk_salaries to map player_id → name
sal_rows = []
offset = 0
while True:
    rows = sb.table('dk_salaries').select(
        'player_id,name'
    ).range(offset, offset + 999).execute().data or []
    sal_rows.extend(rows)
    if len(rows) < 1000:
        break
    offset += 1000
pid_to_name = {}
for r in sal_rows:
    if r.get('player_id') and r.get('name'):
        pid_to_name[r['player_id']] = r['name']

# Average projected ownership per player name
proj_own = defaultdict(list)
for r in proj_rows:
    pid = r.get('player_id')
    name = pid_to_name.get(pid, '')
    pct = r.get('proj_ownership')
    if name and pct is not None:
        proj_own[name].append(pct)

avg_proj = {name: sum(vals) / len(vals) for name, vals in proj_own.items()}
print(f"  Players with projected ownership: {len(avg_proj)}")

# ── Step 3: Match and compare ───────────────────────────────────────────
# Normalize names for matching
def norm(n):
    return n.lower().replace('.', '').replace("'", '').replace('-', ' ').strip()

actual_norm = {norm(k): (k, v) for k, v in avg_actual.items()}
proj_norm = {norm(k): (k, v) for k, v in avg_proj.items()}

matched = []
for nk, (pname, pval) in proj_norm.items():
    if nk in actual_norm:
        aname, aval = actual_norm[nk]
        matched.append({
            'name': pname,
            'proj_own': pval,
            'actual_own': aval,
            'delta': pval - aval,
            'n_contests': len(actual_own.get(aname, [])),
        })

print(f"  Matched players: {len(matched)}")

if not matched:
    print("  No matches found — check name normalization")
    sys.exit(1)

# ── Step 4: Compute calibration metrics ─────────────────────────────────
deltas = [m['delta'] for m in matched]
abs_deltas = [abs(d) for d in deltas]
proj_vals = [m['proj_own'] for m in matched]
actual_vals = [m['actual_own'] for m in matched]

mean_bias = sum(deltas) / len(deltas)
mae = sum(abs_deltas) / len(abs_deltas)

# Correlation
n = len(matched)
mean_p = sum(proj_vals) / n
mean_a = sum(actual_vals) / n
cov = sum((p - mean_p) * (a - mean_a) for p, a in zip(proj_vals, actual_vals)) / n
std_p = math.sqrt(sum((p - mean_p) ** 2 for p in proj_vals) / n)
std_a = math.sqrt(sum((a - mean_a) ** 2 for a in actual_vals) / n)
corr = cov / (std_p * std_a) if std_p > 0 and std_a > 0 else 0

print(f"\n{'=' * 60}")
print(f"  Ownership Calibration Results")
print(f"{'=' * 60}")
print(f"  Matched players: {len(matched)}")
print(f"  Bias: {mean_bias:+.2f}% (positive = we over-project ownership)")
print(f"  MAE:  {mae:.2f}%")
print(f"  Correlation: r={corr:.3f}")

# ── Step 5: Archetype analysis ──────────────────────────────────────────
# Bucket by ownership tier
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
        print(f"    {label}: n={len(group)}, bias={tier_bias:+.2f}%, MAE={tier_mae:.2f}%")

# ── Step 6: Biggest misses ─────────────────────────────────────────────
print(f"\n  Biggest over-projections (we said high, actual was low):")
over = sorted(matched, key=lambda m: m['delta'], reverse=True)[:5]
for m in over:
    print(f"    {m['name']:25s} proj={m['proj_own']:5.1f}%  actual={m['actual_own']:5.1f}%  delta={m['delta']:+.1f}%")

print(f"\n  Biggest under-projections (we said low, actual was high):")
under = sorted(matched, key=lambda m: m['delta'])[:5]
for m in under:
    print(f"    {m['name']:25s} proj={m['proj_own']:5.1f}%  actual={m['actual_own']:5.1f}%  delta={m['delta']:+.1f}%")

# ── Step 7: Write to research_findings ──────────────────────────────────
findings_path = os.path.join(os.path.dirname(__file__), 'tasks', 'research_findings.md')
with open(findings_path, 'a', encoding='utf-8') as f:
    f.write(f"\n\n## Ownership Calibration — {contest_count} large-field contests (≥{MIN_ENTRIES} entries)\n\n")
    f.write(f"- **Matched players**: {len(matched)}\n")
    f.write(f"- **Bias**: {mean_bias:+.2f}% (positive = over-project ownership)\n")
    f.write(f"- **MAE**: {mae:.2f}%\n")
    f.write(f"- **Correlation**: r={corr:.3f}\n\n")
    for label, group in tiers.items():
        if group:
            tier_bias = sum(m['delta'] for m in group) / len(group)
            f.write(f"- {label}: n={len(group)}, bias={tier_bias:+.2f}%\n")
    f.write(f"\n**Over-projected ownership:**\n")
    for m in over:
        f.write(f"- {m['name']}: proj={m['proj_own']:.1f}% actual={m['actual_own']:.1f}%\n")
    f.write(f"\n**Under-projected ownership:**\n")
    for m in under:
        f.write(f"- {m['name']}: proj={m['proj_own']:.1f}% actual={m['actual_own']:.1f}%\n")

print(f"\n  Results appended to tasks/research_findings.md")
print(f"\n  Done.")
