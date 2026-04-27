#!/usr/bin/env python3
"""
Test pitcher projection approaches against actual results.
Compares three versions to find which produces the most accurate spread.

Version A: Current model (baseline) — uses projection_history
Version B: Trust Bayesian — strip global overlays from direct calc
Version C: Tune Bayesian priors — wider prior sensitivity
"""
import sys
sys.stdout.reconfigure(encoding='utf-8', line_buffering=True)
import os, math
from collections import defaultdict
from dotenv import load_dotenv
load_dotenv()
from supabase import create_client
import numpy as np

sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])

from sim_projections import (marcel_pitcher, reliability_blend_pitcher,
                              compute_opp_quality, safe, clip,
                              LEAGUE_AVG_K_PCT, LEAGUE_AVG_BB_PCT,
                              LEAGUE_AVG_XFIP, weather_hr_mult)
from config import SEASON


def paginate(table, select, filters=None):
    rows = []
    off = 0
    while True:
        q = sb.table(table).select(select).range(off, off + 999)
        if filters:
            for method, args in filters:
                q = getattr(q, method)(*args)
        r = q.execute()
        if not r.data:
            break
        rows.extend(r.data)
        if len(r.data) < 1000:
            break
        off += 1000
    return rows


# ── Load all data ────────────────────────────────────────────────────────────

print('Loading data...')

# Projection history (Version A baseline)
proj_hist = paginate('projection_history',
    'player_id,game_date,proj_dk_pts,proj_ip,proj_ks,proj_er,win_prob,is_pitcher',
    [('eq', ('is_pitcher', True))])
print(f'  Projection history: {len(proj_hist)} pitcher entries')

# Actual results
actuals = paginate('actual_results',
    'player_id,game_date,actual_dk_pts,ip,p_k,p_er,win,is_pitcher',
    [('eq', ('is_pitcher', True))])
print(f'  Actual results: {len(actuals)} pitcher entries')

# Pitcher stats (all seasons for Bayesian model)
p_stats_raw = paginate('pitcher_stats',
    'player_id,season,era,fip,xfip,siera,k_pct,bb_pct,whip,swstr_pct,csw_pct,'
    'stuff_plus,pitching_plus,location_plus,barrel_pct,hard_hit_pct,hr9,babip,'
    'gb_pct,fb_pct,ld_pct,lob_pct,ip,g,gs,k9,bb9,sample_size,full_name')

# Group by player_id → {season: row}
p_stats_by_player = defaultdict(dict)
for r in p_stats_raw:
    p_stats_by_player[r['player_id']][r['season']] = r
print(f'  Pitcher stats: {len(p_stats_by_player)} pitchers')

# Game odds
odds_raw = paginate('game_odds', 'game_pk,game_date,game_total,home_ml,away_ml,home_implied,away_implied')
odds_by_gpk = {r['game_pk']: r for r in odds_raw}

# Games (for matchup info)
games_raw = paginate('games',
    'game_pk,game_date,home_team,away_team,home_team_id,away_team_id,'
    'home_sp_id,away_sp_id,venue_id,home_sp_hand,away_sp_hand')

# Park factors
parks_raw = paginate('park_factors', 'venue_id,basic_factor,k_factor,hr_factor,bb_factor')
park_by_venue = {r['venue_id']: r for r in parks_raw}

# Build matchable dataset: projection_history + actuals on same (player_id, game_date)
proj_lookup = {}
for p in proj_hist:
    key = (p['player_id'], p['game_date'])
    proj_lookup[key] = p

matched = []
for a in actuals:
    if a.get('ip') is None or a['ip'] < 4:  # starters only
        continue
    key = (a['player_id'], a['game_date'])
    proj = proj_lookup.get(key)
    if proj:
        matched.append({
            'player_id': a['player_id'],
            'game_date': a['game_date'],
            'actual_dk': a['actual_dk_pts'],
            'actual_ip': a['ip'],
            'actual_k': a['p_k'],
            'actual_er': a['p_er'],
            'actual_win': a['win'],
            'proj_a_dk': proj['proj_dk_pts'],  # Version A
            'proj_a_ip': proj.get('proj_ip'),
            'proj_a_ks': proj.get('proj_ks'),
            'proj_a_er': proj.get('proj_er'),
        })

print(f'  Matched pitcher starts (IP>=4): {len(matched)}')

# Get K% tier for each pitcher
for m in matched:
    ps = p_stats_by_player.get(m['player_id'], {})
    curr = ps.get(SEASON) or ps.get(SEASON - 1) or {}
    m['k_pct'] = safe(curr.get('k_pct'), 0.22)
    m['xfip'] = safe(curr.get('xfip'), 4.0)
    m['stuff_plus'] = safe(curr.get('stuff_plus'), 100)
    m['name'] = curr.get('full_name', f'pid={m["player_id"]}')


# ── Compute Version B and C projections ──────────────────────────────────────

def compute_version_b(m):
    """Trust Bayesian — strip global overlays."""
    pid = m['player_id']
    ps = p_stats_by_player.get(pid, {})
    if not ps:
        return None

    talent = marcel_pitcher(ps, SEASON)

    # Find game info
    game = None
    for g in games_raw:
        if g['game_date'] == m['game_date']:
            if g.get('home_sp_id') == pid or g.get('away_sp_id') == pid:
                game = g
                break
    if not game:
        return None

    is_home = game.get('home_sp_id') == pid
    odds_row = odds_by_gpk.get(game['game_pk'])
    park_row = park_by_venue.get(game.get('venue_id'))

    # VERSION B: Trust Bayesian outputs directly
    PA_PER_IP = 4.3

    # IP: use talent directly, NO regression to 5.0
    exp_ip = talent['ip_per_gs']
    exp_bf = exp_ip * PA_PER_IP

    # K rate: use Bayesian K% directly, only park adjustment
    park_k = safe(park_row.get('k_factor'), 100) / 100.0 if park_row else 1.0
    park_k_edge = 1.0 + (park_k - 1.0) * 0.50
    exp_k_rate = clip(talent['k_pct'] * park_k_edge, 0.08, 0.45)
    exp_ks = exp_bf * exp_k_rate

    # BB rate
    park_bb = safe(park_row.get('bb_factor'), 100) / 100.0 if park_row else 1.0
    exp_bb_rate = clip(talent['bb_pct'] * park_bb, 0.03, 0.16)
    exp_bb = exp_bf * exp_bb_rate

    # Hit rate
    park_basic = safe(park_row.get('basic_factor'), 100) / 100.0 if park_row else 1.0
    park_basic_edge = 1.0 + (park_basic - 1.0) * 0.50
    contact_rate = max(0.15, 1.0 - exp_k_rate - exp_bb_rate)
    exp_h = exp_bf * contact_rate * clip(talent['babip'] * park_basic_edge, 0.22, 0.38)
    exp_hbp = exp_bf * 0.01

    # ER: use pure Bayesian xFIP (no SIERA blend)
    era_anchor = talent['xfip']
    park_hr_f = safe(park_row.get('hr_factor'), 100) / 100.0 if park_row else 1.0
    park_er_adj = 1.0 + (park_hr_f - 1.0) * 0.45
    exp_er = era_anchor * exp_ip / 9.0 * park_er_adj

    # Win prob
    exp_win = 0.25
    if odds_row:
        hml = odds_row.get('home_ml')
        aml = odds_row.get('away_ml')
        if hml and aml:
            def _tp(ml):
                ml = int(ml)
                return abs(ml) / (abs(ml) + 100) if ml < 0 else 100 / (ml + 100)
            hp, ap = _tp(hml), _tp(aml)
            tw = (hp / (hp + ap)) if is_home else (ap / (hp + ap))
            exp_win = clip(tw * 0.78 * clip(exp_ip / 5.1, 0.70, 1.30), 0.10, 0.52)

    dk = (exp_ip * 2.25 + exp_ks * 2.0 + exp_win * 4.0
          - exp_er * 2.0 - exp_h * 0.6 - exp_bb * 0.6 - exp_hbp * 0.6)
    return dk


def compute_version_c(m):
    """Tune Bayesian priors — wider sensitivity."""
    pid = m['player_id']
    ps = p_stats_by_player.get(pid, {})
    if not ps:
        return None

    # Build modified talent with tuned priors
    # We can't easily monkey-patch marcel_pitcher, so we'll call it
    # and then adjust the outputs to simulate what tuned priors would produce
    talent = marcel_pitcher(ps, SEASON)

    # Simulate tuned K% prior: more sensitive to Stuff+/Pitching+
    # Original: k_prior = 0.10 + (pp-100)*0.0035 + swstr*0.60
    # Tuned:    k_prior = 0.08 + (pp-100)*0.005 + swstr*0.80
    curr = ps.get(SEASON) or ps.get(SEASON - 1) or {}
    pp = safe(curr.get('pitching_plus'), 100)
    swstr = safe(curr.get('swstr_pct'), 0.11)
    old_k_prior = clip(0.10 + (pp - 100) * 0.0035 + swstr * 0.60, 0.10, 0.40)
    new_k_prior = clip(0.08 + (pp - 100) * 0.005 + swstr * 0.80, 0.08, 0.42)
    # Adjust talent K% by the prior shift (approximate)
    k_shift = new_k_prior - old_k_prior
    adjusted_k = clip(talent['k_pct'] + k_shift * 0.3, 0.08, 0.40)  # 30% of prior shift flows through

    # Simulate tuned xFIP prior: more sensitive
    stuff = safe(curr.get('stuff_plus'), 100)
    old_xfip_prior = clip(4.80 - (pp - 100) * 0.020 - (stuff - 100) * 0.008, 3.00, 6.00)
    new_xfip_prior = clip(4.80 - (pp - 100) * 0.030 - (stuff - 100) * 0.012, 2.80, 6.00)
    xfip_shift = new_xfip_prior - old_xfip_prior
    adjusted_xfip = clip(talent['xfip'] + xfip_shift * 0.3, 2.50, 6.00)

    # Now run the same direct calc as Version A but with adjusted talent
    game = None
    for g in games_raw:
        if g['game_date'] == m['game_date']:
            if g.get('home_sp_id') == pid or g.get('away_sp_id') == pid:
                game = g
                break
    if not game:
        return None

    is_home = game.get('home_sp_id') == pid
    odds_row = odds_by_gpk.get(game['game_pk'])
    park_row = park_by_venue.get(game.get('venue_id'))

    PA_PER_IP = 4.3
    # IP: same regression as current code
    career_ip = sum(safe((ps.get(yr) or {}).get('ip'), 0) for yr in [SEASON, SEASON - 1, SEASON - 2])
    ip_reg = clip(0.50 - (career_ip - 100) * 0.001, 0.25, 0.50)
    exp_ip = talent['ip_per_gs'] * (1.0 - ip_reg) + 5.0 * ip_reg
    exp_bf = exp_ip * PA_PER_IP

    # K rate with tuned prior
    park_k = safe(park_row.get('k_factor'), 100) / 100.0 if park_row else 1.0
    park_k_edge = 1.0 + (park_k - 1.0) * 0.50
    exp_k_rate = clip(adjusted_k * park_k_edge, 0.08, 0.45)
    exp_ks = exp_bf * exp_k_rate

    park_bb = safe(park_row.get('bb_factor'), 100) / 100.0 if park_row else 1.0
    exp_bb_rate = clip(talent['bb_pct'] * park_bb, 0.03, 0.16)
    exp_bb = exp_bf * exp_bb_rate

    park_basic = safe(park_row.get('basic_factor'), 100) / 100.0 if park_row else 1.0
    park_basic_edge = 1.0 + (park_basic - 1.0) * 0.50
    contact_rate = max(0.15, 1.0 - exp_k_rate - exp_bb_rate)
    exp_h = exp_bf * contact_rate * clip(talent['babip'] * park_basic_edge, 0.22, 0.38)
    exp_hbp = exp_bf * 0.01

    # ER with tuned xFIP (70/30 blend instead of 50/50)
    era_anchor = adjusted_xfip * 0.70 + talent['siera'] * 0.30
    park_hr_f = safe(park_row.get('hr_factor'), 100) / 100.0 if park_row else 1.0
    park_er_adj = 1.0 + (park_hr_f - 1.0) * 0.45
    exp_er = era_anchor * exp_ip / 9.0 * park_er_adj

    exp_win = 0.25
    if odds_row:
        hml = odds_row.get('home_ml')
        aml = odds_row.get('away_ml')
        if hml and aml:
            def _tp(ml):
                ml = int(ml)
                return abs(ml) / (abs(ml) + 100) if ml < 0 else 100 / (ml + 100)
            hp, ap = _tp(hml), _tp(aml)
            tw = (hp / (hp + ap)) if is_home else (ap / (hp + ap))
            exp_win = clip(tw * 0.78 * clip(exp_ip / 5.1, 0.70, 1.30), 0.10, 0.52)

    dk = (exp_ip * 2.25 + exp_ks * 2.0 + exp_win * 4.0
          - exp_er * 2.0 - exp_h * 0.6 - exp_bb * 0.6 - exp_hbp * 0.6)
    return dk


# ── Compute all versions ─────────────────────────────────────────────────────

print('\nComputing projections...')
for m in matched:
    m['proj_b_dk'] = compute_version_b(m)
    m['proj_c_dk'] = compute_version_c(m)

# Filter to those with all 3 versions
complete = [m for m in matched if m['proj_b_dk'] is not None and m['proj_c_dk'] is not None]
print(f'Complete comparisons: {len(complete)}')


# ── Analyze ──────────────────────────────────────────────────────────────────

def tier(k_pct):
    if k_pct > 0.27: return 'Elite (K%>.27)'
    if k_pct > 0.22: return 'Good (.22-.27)'
    if k_pct > 0.18: return 'Avg (.18-.22)'
    return 'Bad (K%<.18)'


def analyze(data, proj_key, label):
    projs = np.array([m[proj_key] for m in data])
    acts = np.array([m['actual_dk'] for m in data])
    errs = projs - acts
    mae = np.mean(np.abs(errs))
    bias = np.mean(errs)
    r = np.corrcoef(projs, acts)[0, 1] if len(projs) > 5 else 0
    spread = np.max(projs) - np.min(projs)

    print(f'\n  {label} (n={len(data)}):')
    print(f'    MAE={mae:.2f}  Bias={bias:+.2f}  r={r:.3f}  Spread={spread:.1f}')

    # By tier
    tiers = defaultdict(list)
    for m in data:
        tiers[tier(m['k_pct'])].append(m)

    print(f'    {"Tier":20s} {"N":>4s} {"AvgProj":>8s} {"AvgAct":>8s} {"MAE":>6s} {"Bias":>7s}')
    print(f'    {"-"*55}')
    for t in ['Elite (K%>.27)', 'Good (.22-.27)', 'Avg (.18-.22)', 'Bad (K%<.18)']:
        group = tiers.get(t, [])
        if not group:
            continue
        gp = np.array([m[proj_key] for m in group])
        ga = np.array([m['actual_dk'] for m in group])
        ge = gp - ga
        print(f'    {t:20s} {len(group):>4d} {np.mean(gp):>8.1f} {np.mean(ga):>8.1f} {np.mean(np.abs(ge)):>6.2f} {np.mean(ge):>+7.2f}')


print(f'\n{"="*70}')
print(f'  PITCHER PROJECTION COMPARISON ({len(complete)} starter game logs)')
print(f'{"="*70}')

analyze(complete, 'proj_a_dk', 'Version A: CURRENT MODEL (projection_history)')
analyze(complete, 'proj_b_dk', 'Version B: TRUST BAYESIAN (strip global overlays)')
analyze(complete, 'proj_c_dk', 'Version C: TUNED PRIORS (wider sensitivity)')

# Side-by-side best/worst
print(f'\n{"="*70}')
print(f'  SAMPLE COMPARISONS')
print(f'{"="*70}')
print(f'  {"Name":20s} {"Actual":>7s} {"Cur(A)":>7s} {"Bay(B)":>7s} {"Tune(C)":>7s} {"K%":>6s} {"Tier":>15s}')
print(f'  {"-"*70}')

# Show a mix of tiers
by_tier = defaultdict(list)
for m in complete:
    by_tier[tier(m['k_pct'])].append(m)
for t in ['Elite (K%>.27)', 'Good (.22-.27)', 'Avg (.18-.22)', 'Bad (K%<.18)']:
    group = sorted(by_tier.get(t, []), key=lambda m: m['actual_dk'], reverse=True)
    for m in group[:3]:
        print(f'  {m["name"]:20s} {m["actual_dk"]:>7.1f} {m["proj_a_dk"]:>7.1f} {m["proj_b_dk"]:>7.1f} {m["proj_c_dk"]:>7.1f} {m["k_pct"]:>6.3f} {t:>15s}')

print(f'\n{"="*70}')
print(f'  WINNER: Pick the version with lowest MAE + best tier separation')
print(f'{"="*70}')
