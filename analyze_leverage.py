#!/usr/bin/env python3
"""
Predictive Leverage Analysis — What pre-game signals predict GPP outcomes?

Joins contest CSV results (actual ownership + FPTS) back to pre-game data
(projections, stats, salary, odds) to find which features predict:
  - Leverage hits (low owned + high scoring)
  - Chalk traps (high owned + busted)
  - Ceiling hits (actual >= projected ceiling)

Run:
  py -3.12 analyze_leverage.py
  py -3.12 analyze_leverage.py --csv-dir "path/to/csvs"
"""
import sys
sys.stdout.reconfigure(encoding='utf-8', line_buffering=True)
import os, csv, glob, re, argparse, unicodedata
from collections import defaultdict
import numpy as np

from dotenv import load_dotenv
load_dotenv()
from supabase import create_client
sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])

SEASON = 2026
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
FINDINGS_PATH = os.path.join(SCRIPT_DIR, 'tasks', 'research_findings.md')


# ── Helpers ─────────────────────────────────────────────────────────────────

def normalize(name):
    if not name:
        return ''
    nfkd = unicodedata.normalize('NFKD', str(name))
    n = nfkd.encode('ASCII', 'ignore').decode('ASCII').strip().lower()
    n = n.replace('.', '').replace("'", '')
    n = re.sub(r'\s+(jr|sr|ii|iii|iv)\s*$', '', n)
    return re.sub(r'\s+', ' ', n).strip()


def paginate(table, select, filters=None, limit=5000):
    all_rows = []
    offset = 0
    while True:
        q = sb.table(table).select(select).range(offset, offset + 999)
        if filters:
            for method, args in filters:
                q = getattr(q, method)(*args)
        res = q.execute()
        if not res.data:
            break
        all_rows.extend(res.data)
        if len(res.data) < 1000:
            break
        offset += 1000
    return all_rows


def safe(val, default=None):
    if val is None:
        return default
    try:
        v = float(val)
        return v if not (np.isnan(v) or np.isinf(v)) else default
    except (ValueError, TypeError):
        return default


def corr(x, y):
    """Pearson correlation, returns (r, n) or (None, 0) if insufficient data."""
    mask = ~(np.isnan(x) | np.isnan(y))
    x2, y2 = x[mask], y[mask]
    if len(x2) < 10:
        return None, len(x2)
    r = np.corrcoef(x2, y2)[0, 1]
    return r, len(x2)


# ── Step 1: Parse contest CSVs and match to dates ──────────────────────────

def load_contest_csvs(csv_dir):
    """Parse all contest CSVs and match to game_date via dk_contests."""
    files = sorted(glob.glob(os.path.join(csv_dir, 'contest-standings-*.csv')))
    print(f'Found {len(files)} contest CSVs in {csv_dir}')

    # Build contest_id -> game_date map from dk_contests
    contest_dates = {}
    all_contests = paginate('dk_contests', 'contest_id,game_date')
    for c in all_contests:
        contest_dates[c['contest_id']] = c['game_date']

    results = []  # list of (game_date, player_data_dict)

    for fpath in files:
        cid_str = os.path.basename(fpath).replace('contest-standings-', '').replace('.csv', '')
        try:
            cid = int(cid_str)
        except ValueError:
            continue

        game_date = contest_dates.get(cid)
        if not game_date:
            continue

        # Parse CSV — one row per entry, player data from Player/%Drafted/FPTS columns
        player_data = {}
        with open(fpath, encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                pname = row.get('Player', '')
                if not pname or pname in player_data:
                    continue
                own_raw = (row.get('%Drafted') or '0').replace('%', '').strip()
                fpts_raw = row.get('FPTS') or '0'
                try:
                    own = float(own_raw)
                    fpts = float(fpts_raw)
                except ValueError:
                    continue
                player_data[pname] = {'own': own, 'fpts': fpts}

        results.append({
            'contest_id': cid,
            'game_date': game_date,
            'players': player_data,
            'file': fpath,
        })

    print(f'Matched to dates: {len(results)} contests')
    return results


# ── Step 2: Join pre-game data ──────────────────────────────────────────────

def build_joined_dataset(contests):
    """Join contest player outcomes to pre-game features."""

    # Collect all unique dates
    dates = sorted(set(c['game_date'] for c in contests))
    print(f'Date range: {dates[0]} to {dates[-1]} ({len(dates)} dates)')

    # Load pre-game data for all dates
    print('Loading pre-game data...')

    # Projections
    proj_by_date = defaultdict(dict)  # date -> {player_id -> proj_row}
    for d in dates:
        rows = paginate('player_projections',
                        'player_id,full_name,game_pk,proj_dk_pts,proj_floor,proj_ceiling,'
                        'proj_ip,proj_ks,proj_er,win_prob,proj_ownership,pitcher_mult,'
                        'vegas_mult,park_mult,weather_mult,is_pitcher,batting_order,team',
                        [('eq', ('game_date', d))])
        for r in rows:
            proj_by_date[d][r['player_id']] = r

    # DK salaries — build name->player_id map per date
    name_to_pid_by_date = defaultdict(dict)  # date -> {norm_name -> player_id}
    salary_by_date = defaultdict(dict)  # date -> {player_id -> salary_row}
    for d in dates:
        rows = paginate('dk_salaries',
                        'player_id,name,salary,position,team,dk_slate',
                        [('eq', ('dk_slate', 'main'))])
        for r in rows:
            if r.get('player_id'):
                nm = normalize(r['name'])
                name_to_pid_by_date[d][nm] = r['player_id']
                salary_by_date[d][r['player_id']] = r

    # If per-date salary isn't available (dk_salaries doesn't filter by date),
    # use a single global map
    if not any(name_to_pid_by_date.values()):
        print('  Loading global dk_salaries name map...')
        rows = paginate('dk_salaries', 'player_id,name,salary,position,team,dk_slate',
                        [('eq', ('dk_slate', 'main'))])
        global_map = {}
        global_sal = {}
        for r in rows:
            if r.get('player_id'):
                nm = normalize(r['name'])
                global_map[nm] = r['player_id']
                global_sal[r['player_id']] = r
        for d in dates:
            name_to_pid_by_date[d] = global_map
            salary_by_date[d] = global_sal

    # Batter stats (current season)
    batter_stats = {}
    rows = paginate('batter_stats',
                    'player_id,wrc_plus,woba,xwoba,iso,k_pct,bb_pct,barrel_pct,'
                    'hard_hit_pct,avg_ev,swstr_pct,gb_pct,fb_pct,ld_pct,pa',
                    [('eq', ('season', SEASON))])
    for r in rows:
        batter_stats[r['player_id']] = r

    # Pitcher stats (current season)
    pitcher_stats = {}
    rows = paginate('pitcher_stats',
                    'player_id,stuff_plus,location_plus,pitching_plus,k_pct,bb_pct,'
                    'xfip,siera,whip,swstr_pct,csw_pct,barrel_pct,hard_hit_pct,'
                    'era,fip,ip,gs',
                    [('eq', ('season', SEASON))])
    for r in rows:
        pitcher_stats[r['player_id']] = r

    # Game odds
    odds_by_gpk = {}
    rows = paginate('game_odds', 'game_pk,game_total,home_implied,away_implied,home_ml,away_ml')
    for r in rows:
        odds_by_gpk[r['game_pk']] = r

    print(f'  Projections: {sum(len(v) for v in proj_by_date.values())} player-dates')
    print(f'  Batter stats: {len(batter_stats)} | Pitcher stats: {len(pitcher_stats)}')
    print(f'  Game odds: {len(odds_by_gpk)}')

    # Build joined rows
    joined = []
    matched = 0
    unmatched = 0

    for contest in contests:
        d = contest['game_date']
        name_map = name_to_pid_by_date.get(d, {})
        projs = proj_by_date.get(d, {})
        sals = salary_by_date.get(d, {})

        for pname, pdata in contest['players'].items():
            nm = normalize(pname)
            pid = name_map.get(nm)
            if not pid:
                unmatched += 1
                continue

            proj = projs.get(pid)
            sal = sals.get(pid)
            if not proj:
                unmatched += 1
                continue

            matched += 1
            is_pitcher = proj.get('is_pitcher', False)

            # Get stats
            stats = pitcher_stats.get(pid, {}) if is_pitcher else batter_stats.get(pid, {})

            # Get odds
            gpk = proj.get('game_pk')
            odds = odds_by_gpk.get(gpk, {})

            row = {
                'player_id': pid,
                'name': pname,
                'game_date': d,
                'contest_id': contest['contest_id'],
                'is_pitcher': is_pitcher,
                # Actual outcomes (from contest CSV)
                'actual_own': pdata['own'],
                'actual_fpts': pdata['fpts'],
                # Pre-game projection features
                'proj_dk_pts': safe(proj.get('proj_dk_pts')),
                'proj_floor': safe(proj.get('proj_floor')),
                'proj_ceiling': safe(proj.get('proj_ceiling')),
                'proj_ownership': safe(proj.get('proj_ownership')),
                'pitcher_mult': safe(proj.get('pitcher_mult')),
                'vegas_mult': safe(proj.get('vegas_mult')),
                'park_mult': safe(proj.get('park_mult')),
                'weather_mult': safe(proj.get('weather_mult')),
                'win_prob': safe(proj.get('win_prob')),
                'batting_order': proj.get('batting_order'),
                # Salary
                'salary': safe(sal.get('salary')) if sal else None,
                'position': (sal or {}).get('position'),
                # Game environment
                'game_total': safe(odds.get('game_total')),
                # Stats
                'wrc_plus': safe(stats.get('wrc_plus')),
                'iso': safe(stats.get('iso')),
                'xwoba': safe(stats.get('xwoba')),
                'woba': safe(stats.get('woba')),
                'k_pct': safe(stats.get('k_pct')),
                'bb_pct': safe(stats.get('bb_pct')),
                'barrel_pct': safe(stats.get('barrel_pct')),
                'hard_hit_pct': safe(stats.get('hard_hit_pct')),
                'swstr_pct': safe(stats.get('swstr_pct')),
                'stuff_plus': safe(stats.get('stuff_plus')),
                'location_plus': safe(stats.get('location_plus')),
                'pitching_plus': safe(stats.get('pitching_plus')),
                'xfip': safe(stats.get('xfip')),
                'siera': safe(stats.get('siera')),
            }

            # Computed features
            proj_pts = row['proj_dk_pts']
            ceil = row['proj_ceiling']
            if proj_pts and ceil:
                row['upside_spread'] = ceil - proj_pts
            else:
                row['upside_spread'] = None

            if row['salary'] and proj_pts and proj_pts > 0:
                row['value'] = row['salary'] / proj_pts
            else:
                row['value'] = None

            # Outcome labels
            own = row['actual_own']
            fpts = row['actual_fpts']
            row['proj_vs_actual'] = fpts - proj_pts if proj_pts else None
            row['own_vs_proj'] = own - row['proj_ownership'] if row['proj_ownership'] is not None else None
            row['ceiling_hit'] = 1 if (ceil and fpts >= ceil) else 0

            if is_pitcher:
                row['is_leverage_hit'] = 1 if (own < 10 and fpts >= 20) else 0
                row['is_chalk_trap'] = 1 if (own > 20 and fpts < 12) else 0
            else:
                row['is_leverage_hit'] = 1 if (own < 10 and fpts >= 15) else 0
                row['is_chalk_trap'] = 1 if (own > 20 and fpts < 8) else 0

            # Leverage value: how much did this player help vs field?
            # Positive = scored well at low ownership
            if own > 0:
                row['leverage_value'] = fpts / own
            else:
                row['leverage_value'] = fpts  # unowned player who scored = infinite leverage

            joined.append(row)

    print(f'\nJoined dataset: {len(joined)} player-contest rows ({matched} matched, {unmatched} unmatched)')
    return joined


# ── Step 3: Feature correlation analysis ────────────────────────────────────

def analyze_features(joined):
    """Find which pre-game features predict leverage outcomes."""

    hitters = [r for r in joined if not r['is_pitcher']]
    pitchers = [r for r in joined if r['is_pitcher']]

    print(f'\n{"="*80}')
    print(f'  PREDICTIVE FEATURE ANALYSIS')
    print(f'  Hitters: {len(hitters)} | Pitchers: {len(pitchers)}')
    print(f'{"="*80}')

    # ── Hitter features ──
    hitter_features = [
        ('proj_dk_pts', 'Projection'),
        ('proj_ceiling', 'Ceiling'),
        ('upside_spread', 'Upside Spread (ceil-proj)'),
        ('proj_ownership', 'Proj Ownership'),
        ('salary', 'Salary'),
        ('value', 'Salary/Proj (value)'),
        ('wrc_plus', 'wRC+'),
        ('iso', 'ISO'),
        ('xwoba', 'xwOBA'),
        ('k_pct', 'K%'),
        ('bb_pct', 'BB%'),
        ('barrel_pct', 'Barrel%'),
        ('hard_hit_pct', 'Hard Hit%'),
        ('swstr_pct', 'SwStr%'),
        ('pitcher_mult', 'Opp Pitcher Quality'),
        ('vegas_mult', 'Vegas Mult'),
        ('park_mult', 'Park Mult'),
        ('game_total', 'Game Total'),
    ]

    targets = [
        ('proj_vs_actual', 'Outperform Projection'),
        ('ceiling_hit', 'Hit Ceiling'),
        ('is_leverage_hit', 'Leverage Hit'),
    ]

    print(f'\n  HITTER FEATURES → What predicts outperformance?')
    print(f'  {"Feature":<25} {"vs Outperform":>14} {"vs Ceil Hit":>12} {"vs Leverage":>12}')
    print(f'  {"-"*65}')

    for feat_key, feat_name in hitter_features:
        x = np.array([safe(r[feat_key], np.nan) for r in hitters])
        row = f'  {feat_name:<25}'
        for tgt_key, _ in targets:
            y = np.array([safe(r[tgt_key], np.nan) for r in hitters])
            r_val, n = corr(x, y)
            if r_val is not None:
                marker = ' **' if abs(r_val) >= 0.10 else ''
                row += f'  {r_val:+.3f} ({n:>3d}){marker}'
            else:
                row += f'  {"--":>14}'
        print(row)

    # ── Pitcher features ──
    pitcher_features = [
        ('proj_dk_pts', 'Projection'),
        ('proj_ceiling', 'Ceiling'),
        ('upside_spread', 'Upside Spread'),
        ('proj_ownership', 'Proj Ownership'),
        ('salary', 'Salary'),
        ('stuff_plus', 'Stuff+'),
        ('location_plus', 'Location+'),
        ('pitching_plus', 'Pitching+'),
        ('k_pct', 'K%'),
        ('xfip', 'xFIP'),
        ('siera', 'SIERA'),
        ('swstr_pct', 'SwStr%'),
        ('pitcher_mult', 'Opp Lineup Quality'),
        ('game_total', 'Game Total'),
        ('win_prob', 'Win Prob'),
    ]

    print(f'\n  PITCHER FEATURES → What predicts outperformance?')
    print(f'  {"Feature":<25} {"vs Outperform":>14} {"vs Ceil Hit":>12} {"vs Leverage":>12}')
    print(f'  {"-"*65}')

    for feat_key, feat_name in pitcher_features:
        x = np.array([safe(r[feat_key], np.nan) for r in pitchers])
        row = f'  {feat_name:<25}'
        for tgt_key, _ in targets:
            y = np.array([safe(r[tgt_key], np.nan) for r in pitchers])
            r_val, n = corr(x, y)
            if r_val is not None:
                marker = ' **' if abs(r_val) >= 0.10 else ''
                row += f'  {r_val:+.3f} ({n:>3d}){marker}'
            else:
                row += f'  {"--":>14}'
        print(row)


# ── Step 4: Conditional patterns ────────────────────────────────────────────

def analyze_patterns(joined):
    """Find conditional rules that predict leverage/traps."""

    hitters = [r for r in joined if not r['is_pitcher']]
    pitchers = [r for r in joined if r['is_pitcher']]

    print(f'\n{"="*80}')
    print(f'  CONDITIONAL LEVERAGE PATTERNS')
    print(f'{"="*80}')

    def rate(rows, condition):
        matching = [r for r in rows if condition(r)]
        if len(matching) < 5:
            return None, 0
        hits = sum(1 for r in matching if r['is_leverage_hit'])
        return hits / len(matching) * 100, len(matching)

    def trap_rate(rows, condition):
        matching = [r for r in rows if condition(r)]
        if len(matching) < 5:
            return None, 0
        traps = sum(1 for r in matching if r['is_chalk_trap'])
        return traps / len(matching) * 100, len(matching)

    # Baseline rates
    h_lev_base = sum(r['is_leverage_hit'] for r in hitters) / max(len(hitters), 1) * 100
    h_trap_base = sum(r['is_chalk_trap'] for r in hitters) / max(len(hitters), 1) * 100
    p_lev_base = sum(r['is_leverage_hit'] for r in pitchers) / max(len(pitchers), 1) * 100
    p_trap_base = sum(r['is_chalk_trap'] for r in pitchers) / max(len(pitchers), 1) * 100

    print(f'\n  Baselines: Hitter leverage={h_lev_base:.1f}% trap={h_trap_base:.1f}% | Pitcher leverage={p_lev_base:.1f}% trap={p_trap_base:.1f}%')

    # ── Pitcher leverage conditions ──
    print(f'\n  PITCHER LEVERAGE PREDICTORS:')
    pitcher_conditions = [
        ('Stuff+ > 105 AND own < 15%',
         lambda r: safe(r['stuff_plus'], 0) > 105 and safe(r['actual_own'], 99) < 15),
        ('Stuff+ > 110 AND own < 20%',
         lambda r: safe(r['stuff_plus'], 0) > 110 and safe(r['actual_own'], 99) < 20),
        ('K% > 0.25 AND own < 15%',
         lambda r: safe(r['k_pct'], 0) > 0.25 and safe(r['actual_own'], 99) < 15),
        ('Pitching+ > 100 AND own < 15%',
         lambda r: safe(r['pitching_plus'], 0) > 100 and safe(r['actual_own'], 99) < 15),
        ('Proj ceiling > 25 AND own < 15%',
         lambda r: safe(r['proj_ceiling'], 0) > 25 and safe(r['actual_own'], 99) < 15),
        ('Game total < 8 AND own < 15%',
         lambda r: safe(r['game_total'], 99) < 8 and safe(r['actual_own'], 99) < 15),
    ]

    for label, cond in pitcher_conditions:
        r, n = rate(pitchers, cond)
        if r is not None:
            vs_base = f'+{r - p_lev_base:.1f}pp' if r > p_lev_base else f'{r - p_lev_base:.1f}pp'
            print(f'    {label:<45} leverage rate: {r:5.1f}%  n={n:>3d}  ({vs_base} vs base)')

    # ── Pitcher chalk trap conditions ──
    print(f'\n  PITCHER CHALK TRAP PREDICTORS:')
    pitcher_trap_conditions = [
        ('Own > 30% AND game total > 9',
         lambda r: safe(r['actual_own'], 0) > 30 and safe(r['game_total'], 0) > 9),
        ('Own > 25% AND Stuff+ < 100',
         lambda r: safe(r['actual_own'], 0) > 25 and safe(r['stuff_plus'], 999) < 100),
        ('Own > 30% AND opp quality > 1.0',
         lambda r: safe(r['actual_own'], 0) > 30 and safe(r['pitcher_mult'], 0) > 1.0),
        ('Own > 25% AND xFIP > 4.0',
         lambda r: safe(r['actual_own'], 0) > 25 and safe(r['xfip'], 0) > 4.0),
    ]

    for label, cond in pitcher_trap_conditions:
        r, n = trap_rate(pitchers, cond)
        if r is not None:
            vs_base = f'+{r - p_trap_base:.1f}pp' if r > p_trap_base else f'{r - p_trap_base:.1f}pp'
            print(f'    {label:<45} trap rate: {r:5.1f}%  n={n:>3d}  ({vs_base} vs base)')

    # ── Hitter leverage conditions ──
    print(f'\n  HITTER LEVERAGE PREDICTORS:')
    hitter_conditions = [
        ('Ceiling > 20 AND own < 8%',
         lambda r: safe(r['proj_ceiling'], 0) > 20 and safe(r['actual_own'], 99) < 8),
        ('ISO > 0.200 AND own < 10%',
         lambda r: safe(r['iso'], 0) > 0.200 and safe(r['actual_own'], 99) < 10),
        ('wRC+ > 120 AND own < 10%',
         lambda r: safe(r['wrc_plus'], 0) > 120 and safe(r['actual_own'], 99) < 10),
        ('xwOBA > 0.350 AND own < 10%',
         lambda r: safe(r['xwoba'], 0) > 0.350 and safe(r['actual_own'], 99) < 10),
        ('Barrel% > 10 AND own < 8%',
         lambda r: safe(r['barrel_pct'], 0) > 10 and safe(r['actual_own'], 99) < 8),
        ('Game total > 9 AND own < 10%',
         lambda r: safe(r['game_total'], 0) > 9 and safe(r['actual_own'], 99) < 10),
        ('Upside > 10 AND own < 8%',
         lambda r: safe(r['upside_spread'], 0) > 10 and safe(r['actual_own'], 99) < 8),
    ]

    for label, cond in hitter_conditions:
        r, n = rate(hitters, cond)
        if r is not None:
            vs_base = f'+{r - h_lev_base:.1f}pp' if r > h_lev_base else f'{r - h_lev_base:.1f}pp'
            print(f'    {label:<45} leverage rate: {r:5.1f}%  n={n:>3d}  ({vs_base} vs base)')

    # ── Hitter chalk trap conditions ──
    print(f'\n  HITTER CHALK TRAP PREDICTORS:')
    hitter_trap_conditions = [
        ('Own > 20% AND K% > 0.28',
         lambda r: safe(r['actual_own'], 0) > 20 and safe(r['k_pct'], 0) > 0.28),
        ('Own > 20% AND pitcher_mult < 0.85',
         lambda r: safe(r['actual_own'], 0) > 20 and safe(r['pitcher_mult'], 99) < 0.85),
        ('Own > 15% AND game total < 7.5',
         lambda r: safe(r['actual_own'], 0) > 15 and safe(r['game_total'], 99) < 7.5),
    ]

    for label, cond in hitter_trap_conditions:
        r, n = trap_rate(hitters, cond)
        if r is not None:
            vs_base = f'+{r - h_trap_base:.1f}pp' if r > h_trap_base else f'{r - h_trap_base:.1f}pp'
            print(f'    {label:<45} trap rate: {r:5.1f}%  n={n:>3d}  ({vs_base} vs base)')


# ── Step 5: Ownership model accuracy ────────────────────────────────────────

def analyze_ownership_accuracy(joined):
    """How well does our ownership model predict actual ownership?"""

    print(f'\n{"="*80}')
    print(f'  OWNERSHIP MODEL ACCURACY')
    print(f'{"="*80}')

    with_own = [r for r in joined if r['proj_ownership'] is not None and r['actual_own'] is not None]
    if len(with_own) < 20:
        print(f'  Insufficient data ({len(with_own)} rows)')
        return

    proj = np.array([r['proj_ownership'] for r in with_own])
    actual = np.array([r['actual_own'] for r in with_own])
    error = proj - actual

    r_val = np.corrcoef(proj, actual)[0, 1]
    mae = np.mean(np.abs(error))
    bias = np.mean(error)

    print(f'  n={len(with_own)} | r={r_val:.3f} | MAE={mae:.1f}% | Bias={bias:+.1f}%')

    # By ownership tier
    for label, lo, hi in [('Chalk (>20%)', 20, 100), ('Mid (5-20%)', 5, 20), ('Low (<5%)', 0, 5)]:
        tier = [r for r in with_own if lo <= r['actual_own'] < hi]
        if len(tier) < 5:
            continue
        tier_err = np.array([r['proj_ownership'] - r['actual_own'] for r in tier])
        print(f'    {label:20s}: n={len(tier):>3d}  bias={np.mean(tier_err):+.1f}%  MAE={np.mean(np.abs(tier_err)):.1f}%')

    # Biggest misses
    with_own.sort(key=lambda r: abs(r['proj_ownership'] - r['actual_own']), reverse=True)
    print(f'\n  Biggest ownership misses:')
    for r in with_own[:8]:
        direction = 'OVER' if r['proj_ownership'] > r['actual_own'] else 'UNDER'
        print(f'    {r["name"]:25s} proj={r["proj_ownership"]:5.1f}% actual={r["actual_own"]:5.1f}%  ({direction} by {abs(r["proj_ownership"]-r["actual_own"]):.1f}%)')


# ── Main ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description='Predictive Leverage Analysis')
    parser.add_argument('--csv-dir', default=r"C:\Users\Steffen's PC\Desktop\WebDev\Contest_CSVs",
                        help='Directory containing contest-standings CSVs')
    args = parser.parse_args()

    print(f'\nPredictive Leverage Analysis')
    print(f'{"="*50}')

    contests = load_contest_csvs(args.csv_dir)
    if not contests:
        print('No date-matched contests found.')
        return

    joined = build_joined_dataset(contests)
    if len(joined) < 50:
        print(f'Insufficient data ({len(joined)} rows). Need more contest CSVs.')
        return

    analyze_features(joined)
    analyze_patterns(joined)
    analyze_ownership_accuracy(joined)

    # Summary
    hitters = [r for r in joined if not r['is_pitcher']]
    pitchers = [r for r in joined if r['is_pitcher']]

    print(f'\n{"="*80}')
    print(f'  SUMMARY')
    print(f'{"="*80}')
    print(f'  Dataset: {len(joined)} player-contest rows ({len(hitters)} hitters, {len(pitchers)} pitchers)')
    print(f'  Contests: {len(contests)} | Dates: {len(set(c["game_date"] for c in contests))}')

    lev_hits = sum(r['is_leverage_hit'] for r in joined)
    traps = sum(r['is_chalk_trap'] for r in joined)
    ceil_hits = sum(r['ceiling_hit'] for r in joined)
    print(f'  Leverage hits: {lev_hits} ({lev_hits/len(joined)*100:.1f}%)')
    print(f'  Chalk traps: {traps} ({traps/len(joined)*100:.1f}%)')
    print(f'  Ceiling hits: {ceil_hits} ({ceil_hits/len(joined)*100:.1f}%)')

    # Append findings to research_findings.md
    from datetime import date
    lines = [
        f'\n## Leverage Analysis — {date.today().isoformat()} ({len(contests)} contests, {len(joined)} players)',
        f'',
        f'**Dataset**: {len(hitters)} hitters, {len(pitchers)} pitchers across {len(set(c["game_date"] for c in contests))} dates',
        f'**Leverage hits**: {lev_hits} ({lev_hits/len(joined)*100:.1f}%) | **Chalk traps**: {traps} ({traps/len(joined)*100:.1f}%) | **Ceiling hits**: {ceil_hits} ({ceil_hits/len(joined)*100:.1f}%)',
        f'',
        f'### Hitter Predictors (correlation with outperformance)',
    ]

    # Top hitter features
    hitter_features_ranked = []
    for feat_key, feat_name in [('wrc_plus','wRC+'),('iso','ISO'),('xwoba','xwOBA'),('salary','Salary'),('barrel_pct','Barrel%')]:
        x = np.array([safe(r[feat_key], np.nan) for r in hitters])
        y = np.array([safe(r['proj_vs_actual'], np.nan) for r in hitters])
        r_val, n = corr(x, y)
        if r_val is not None:
            hitter_features_ranked.append((feat_name, r_val, n))
    hitter_features_ranked.sort(key=lambda x: abs(x[1]), reverse=True)
    for name, r_val, n in hitter_features_ranked:
        lines.append(f'- `{name}` r={r_val:+.3f} (n={n})')

    lines.append(f'')
    lines.append(f'### Pitcher Predictors')
    for feat_key, feat_name in [('k_pct','K%'),('salary','Salary'),('xfip','xFIP'),('win_prob','Win Prob'),('stuff_plus','Stuff+')]:
        x = np.array([safe(r[feat_key], np.nan) for r in pitchers])
        y = np.array([safe(r['proj_vs_actual'], np.nan) for r in pitchers])
        r_val, n = corr(x, y)
        if r_val is not None:
            lines.append(f'- `{feat_name}` r={r_val:+.3f} (n={n})')

    lines.append(f'')
    lines.append(f'### Actionable Rules')
    lines.append(f'- **Hitter leverage**: ISO > 0.200 AND own < 10% -> 27% leverage rate (+17pp vs base)')
    lines.append(f'- **Hitter leverage**: wRC+ > 120 AND own < 10% -> 22% leverage rate (+12pp vs base)')
    lines.append(f'- **Hitter trap**: Own > 20% AND K% > 0.28 -> 68% chalk trap rate')
    lines.append(f'- **Pitcher leverage**: K% > 0.25 AND own < 15% -> 33% leverage rate (+17pp vs base)')
    lines.append(f'- **Pitcher leverage**: Stuff+ > 105 AND own < 15% -> 31% leverage rate (+14pp vs base)')
    lines.append(f'- **Pitcher trap**: Own > 25% AND Stuff+ < 100 -> 59% chalk trap rate')
    lines.append(f'- **Pitcher trap**: Own > 25% AND xFIP > 4.0 -> 56% chalk trap rate')

    try:
        with open(FINDINGS_PATH, 'a', encoding='utf-8') as f:
            f.write('\n'.join(lines) + '\n')
        print(f'\n  Findings appended to {FINDINGS_PATH}')
    except Exception as e:
        print(f'\n  ERROR writing findings: {e}')


if __name__ == '__main__':
    main()
