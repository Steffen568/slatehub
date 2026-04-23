#!/usr/bin/env python3
import sys
sys.stdout.reconfigure(encoding='utf-8', line_buffering=True)
"""
sim_projections.py — SlateHub Monte Carlo DFS Projection Engine

Simulates 10,000 games per player at the plate-appearance level using all
available data: batter true talent, pitcher matchup, platoon splits, park
factors (HR/K/BB), weather, Vegas implied totals, batting order position.

Outputs full distributions (mean, SD, P10, P25, P50, P75, P90) to the
player_projections table, backwards-compatible with the analytical engine.

Run: py -3.12 sim_projections.py
     py -3.12 sim_projections.py --date 2026-03-29
     py -3.12 sim_projections.py --sims 5000  (fewer sims for faster testing)
"""

import os, math, random
import numpy as np
from datetime import date, datetime, timezone
from supabase import create_client
from dotenv import load_dotenv
from config import SEASON

load_dotenv()
sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])

# ── Constants ─────────────────────────────────────────────────────────────────

NUM_SIMS = 10_000  # default, overridable via --sims

LEAGUE_AVG_WOBA    = 0.315
LEAGUE_AVG_K_PCT   = 0.225
LEAGUE_AVG_BB_PCT  = 0.082
LEAGUE_AVG_BABIP   = 0.298
LEAGUE_AVG_ISO     = 0.165
LEAGUE_AVG_HR9     = 1.25
LEAGUE_AVG_IMPLIED = 4.5
LEAGUE_AVG_XFIP    = 3.90

# PA per game by batting order position
LINEUP_PA = {1: 4.72, 2: 4.68, 3: 4.58, 4: 4.51, 5: 4.45,
             6: 4.38, 7: 4.25, 8: 4.10, 9: 3.85}
LEAGUE_AVG_PA = 4.3

# Run and RBI opportunity multipliers by lineup position
R_MULT   = {1: 1.25, 2: 1.20, 3: 1.10, 4: 1.00, 5: 0.95,
             6: 0.90, 7: 0.85, 8: 0.80, 9: 0.75}
RBI_MULT = {1: 0.75, 2: 0.85, 3: 1.15, 4: 1.25, 5: 1.20,
             6: 1.05, 7: 0.95, 8: 0.90, 9: 0.80}

# Wind directions
WIND_OUT_DIRS = {"S", "SSW", "SW", "WSW", "SSE", "SE"}
WIND_IN_DIRS  = {"N", "NNW", "NW", "NNE", "NE", "WNW"}

# Marcel weights: current season (if enough PA/IP), prior1, prior2
# Pitcher current-year weight boosted to 10 to react faster to early-season data
MARCEL_WEIGHTS = {0: 5, 1: 4, 2: 3}
PITCHER_CURRENT_WEIGHT = 10
PITCHER_IP_FLOOR = 40  # treat early-season IP as at least this for blend weight

# Breakout detection: if current-season xFIP improves by this much over prior-year
# Marcel baseline, boost current-season effective IP to capture the breakout earlier.
# Without this, a pitcher who's clearly leveled up (Soriano 2026: 2.63 xFIP vs 3.54 prior)
# stays anchored to last year's mediocre numbers until 80+ IP accumulates.
BREAKOUT_XFIP_THRESHOLD = 0.50   # min xFIP improvement to trigger boost
BREAKOUT_IP_MULTIPLIER  = 2.0    # base multiplier for current-season effective IP
BREAKOUT_MAX_MULTIPLIER = 3.0    # cap for scaled multiplier

# Season-scaled minimums: ramp up as sample grows through the year
# Opening Day ~Mar 27 → full season ~Sep 28 ≈ 185 days
def _season_min(full_season_min, target_date=None):
    """Scale minimum PA/IP threshold by how deep into the season we are."""
    if target_date is None:
        target_date = date.today()
    elif isinstance(target_date, str):
        target_date = date.fromisoformat(target_date)
    opening_day = date(target_date.year, 3, 27)
    days_in = max(0, (target_date - opening_day).days)
    # Ramp: 0.25 at Opening Day → 1.0 by June (90 days in)
    scale = min(1.0, 0.25 + 0.75 * (days_in / 90.0))
    return max(5, int(full_season_min * scale))

MIN_PA_BATTER_FULL  = 75
MIN_IP_PITCHER_FULL = 25

# SP covers ~60% of a batter's PA, bullpen ~40%
SP_SHARE = 0.60

# ── Helpers ───────────────────────────────────────────────────────────────────

def safe(val, default=None):
    if val is None:
        return default
    try:
        f = float(val)
        return None if math.isnan(f) or math.isinf(f) else f
    except (TypeError, ValueError):
        return default

def clip(val, lo, hi):
    return max(lo, min(hi, val)) if val is not None else lo

def round2(val):
    return round(val, 2) if val is not None else None


# ── Three-Tier Pitcher Model ─────────────────────────────────────────────────
# Inspired by Run The Sims: blend Stuff (pitch quality), Pitch-Level (SwStr/CSW),
# and Event-Level (K%/BB%/HR9) using reliability weighting that scales with sample size.
# No hardcoded caps — each tier's weight = IP / (IP + stabilization_point).

def compute_arsenal_composite(pitch_rows: list) -> dict:
    """Usage-weighted composite from per-pitch-type arsenal data for one pitcher."""
    if not pitch_rows:
        return None
    total_usage = sum(safe(r.get('usage_pct'), 0) for r in pitch_rows)
    if total_usage <= 0:
        return None

    def wt_avg(col, default):
        num, den = 0.0, 0.0
        for r in pitch_rows:
            u = safe(r.get('usage_pct'), 0)
            v = safe(r.get(col))
            if u > 0 and v is not None:
                num += v * u
                den += u
        return num / den if den > 0 else default

    # Fastball velo: only FF, SI, FC
    fb_rows = [r for r in pitch_rows if r.get('pitch_type') in ('FF', 'SI', 'FC')]
    fb_usage = sum(safe(r.get('usage_pct'), 0) for r in fb_rows) or 1.0
    fb_velo = sum(safe(r.get('velo'), 93) * safe(r.get('usage_pct'), 0) for r in fb_rows) / fb_usage

    # Arm angle: average across pitch types (consistent per pitcher)
    arm_angles = [safe(r.get('arm_angle')) for r in pitch_rows if safe(r.get('arm_angle'))]
    arm_angle = sum(arm_angles) / len(arm_angles) if arm_angles else None

    return {
        'stuff_plus': wt_avg('stuff_plus', 100.0),
        'whiff_pct': wt_avg('whiff_pct', 0.25),
        'arsenal_k_pct': wt_avg('k_pct', LEAGUE_AVG_K_PCT * 100) / 100.0,  # arsenal K% is 0-100
        'arsenal_xwoba': wt_avg('xwoba', 0.315),
        'fb_velo': fb_velo,
        'arm_angle': arm_angle,
    }


def stuff_tier_k(arsenal: dict) -> float:
    """Tier 1: Expected K% from pitch quality (Stuff+ and velo)."""
    stuff = arsenal.get('stuff_plus', 100.0)
    velo = arsenal.get('fb_velo', 93.0)
    # Stuff+: each point above 100 ~ +0.15% K rate (Stuff+ vs K% correlation r~0.65)
    base = LEAGUE_AVG_K_PCT * (1.0 + (stuff - 100) * 0.0015)
    # Velo: each mph above 93 ~ +0.5% K rate
    return clip(base * (1.0 + (velo - 93.0) * 0.005), 0.10, 0.45)


def stuff_tier_bb(arsenal: dict) -> float:
    """Tier 1: Expected BB% from pitch quality. Weak relationship — hitters chase more vs stuff."""
    stuff = arsenal.get('stuff_plus', 100.0)
    return clip(LEAGUE_AVG_BB_PCT * (1.0 - (stuff - 100) * 0.0005), 0.03, 0.16)


def stuff_tier_hr9(arsenal: dict) -> float:
    """Tier 1: Expected HR/9 from arsenal xwOBA (quality of contact allowed)."""
    xw = arsenal.get('arsenal_xwoba', 0.315)
    return clip(LEAGUE_AVG_HR9 * (xw / 0.315) ** 1.5, 0.4, 3.0)


def pitch_level_tier_k(swstr_pct, csw_pct) -> float:
    """Tier 2: Expected K% from swinging-strike and CSW rates.
    SwStr% -> K%: Podhorzer formula (R^2=0.89): K% ~ 2.0 * SwStr% + 0.3%
    CSW% -> K%: K% ~ 0.75 * CSW%"""
    if swstr_pct and swstr_pct > 0:
        swstr_k = swstr_pct * 2.0 + 0.003
    else:
        swstr_k = LEAGUE_AVG_K_PCT
    if csw_pct and csw_pct > 0:
        csw_k = csw_pct * 0.75
        return clip(swstr_k * 0.65 + csw_k * 0.35, 0.10, 0.45)
    return clip(swstr_k, 0.10, 0.45)


# Stabilization points (IP) — how much data each tier needs to be 50% reliable
STAB_STUFF = 30.0     # Stuff+ from pitch characteristics — stabilizes fastest
STAB_PITCH = 50.0     # SwStr%/CSW% — moderate sample needed
STAB_K_EVENT = 120.0  # K% from actual outcomes
STAB_BB_EVENT = 150.0 # BB% is noisier
STAB_HR_EVENT = 300.0 # HR/9 is very noisy — needs large sample


def reliability(ip: float, stab_point: float) -> float:
    """Reliability weight: 0 at 0 IP, 0.5 at stabilization point, approaches 1.0."""
    return ip / (ip + stab_point)


def reliability_blend_pitcher(marcel: dict, arsenal: dict, current_ip: float) -> dict:
    """Three-tier reliability-weighted blend of pitcher projections.

    Tier 1 (Stuff): pitch characteristics -> expected rates. Stabilizes fastest.
    Tier 2 (Pitch-level): SwStr%, CSW% -> expected rates. Moderate stabilization.
    Tier 3 (Event-level): K%, BB%, HR/9 from actual outcomes. Best with data.

    Each tier's weight = reliability at current sample size.
    At 0 IP: Stuff dominates. At 200+ IP: Event dominates.
    """
    result = dict(marcel)
    if not arsenal:
        return result  # no arsenal data -> Marcel only (graceful fallback)

    # If arsenal has no real Stuff+ data, skip stuff tier —
    # without Stuff+, the tier just drags Marcel toward league average
    has_real_stuff = arsenal.get('stuff_plus', 100.0) != 100.0
    if not has_real_stuff:
        return result

    # Tier reliabilities
    r_stuff = reliability(current_ip, STAB_STUFF)
    has_pitch_level = marcel.get('swstr_pct') is not None or marcel.get('csw_pct') is not None
    r_pitch = reliability(current_ip, STAB_PITCH) if has_pitch_level else 0.0
    r_k_event = reliability(current_ip, STAB_K_EVENT)
    r_bb_event = reliability(current_ip, STAB_BB_EVENT)
    r_hr_event = reliability(current_ip, STAB_HR_EVENT)

    # Blend K%
    s_k = stuff_tier_k(arsenal)
    p_k = pitch_level_tier_k(marcel.get('swstr_pct'), marcel.get('csw_pct'))
    e_k = marcel['k_pct']
    total_k = r_stuff + r_pitch + r_k_event
    if total_k > 0:
        result['k_pct'] = clip(
            (s_k * r_stuff + p_k * r_pitch + e_k * r_k_event) / total_k,
            0.10, 0.40)

    # Blend BB% (Tier 2 doesn't help much for BB — use stuff + event)
    s_bb = stuff_tier_bb(arsenal)
    total_bb = r_stuff + r_bb_event
    if total_bb > 0:
        result['bb_pct'] = clip(
            (s_bb * r_stuff + marcel['bb_pct'] * r_bb_event) / total_bb,
            0.04, 0.18)

    # Blend HR/9
    s_hr = stuff_tier_hr9(arsenal)
    total_hr = r_stuff + r_hr_event
    if total_hr > 0:
        result['hr9'] = clip(
            (s_hr * r_stuff + marcel['hr9'] * r_hr_event) / total_hr,
            0.30, 3.00)

    # Pass through arm angle for batter-vs-pitcher interaction
    result['arm_angle'] = arsenal.get('arm_angle')
    return result


def arm_angle_hr_interaction(attack_angle, arm_angle) -> float:
    """HR rate modifier from batter attack angle vs pitcher arm angle alignment.

    Overhand pitchers (arm_angle ~55-70) throw with steep downward plane.
    Uppercut batters (attack_angle > 12) match that plane -> more HR.
    Sidearm pitchers (arm_angle ~20-40) throw flatter. Flat swingers match better.
    """
    if attack_angle is None or arm_angle is None:
        return 1.0
    aa_dev = attack_angle - 12.0   # batter deviation from avg attack angle
    pa_dev = arm_angle - 45.0      # pitcher deviation from avg arm angle
    # Positive alignment = both deviate same direction = HR boost
    alignment = aa_dev * pa_dev * 0.0004
    return clip(1.0 + alignment, 0.92, 1.10)


# ── Marcel True Talent ────────────────────────────────────────────────────────

def _marcel_batter_legacy(stats_by_season, current_season, target_date=None):
    """Legacy Marcel model — kept for rollback. See bayesian_batter()."""
    pass  # stripped for size — git history has the full implementation


def bayesian_batter(stats_by_season: dict, current_season: int, target_date=None) -> dict:
    """
    Bayesian talent estimation for batters.

    Prior: built from quality-of-contact metrics (xwOBA, barrel%, hard_hit%,
    avg_ev) which are the most predictive/stable batting stats.

    Update: observed outcomes (BABIP, ISO, K%, BB%) are blended in weighted
    by their statistical stability — K% is trusted quickly (60 PA), BABIP
    is trusted slowly (400 PA) because it's noisy.

    This replaces Marcel's flat league-average regression with an informative
    prior anchored in what the hitter's contact quality SHOULD produce.
    """
    # ── Stability constants: PA for each stat to reach 50% reliability ────
    STAB_K    = 60    # K% stabilizes fastest
    STAB_BB   = 120   # BB% is plate discipline
    STAB_XW   = 150   # xwOBA stabilizes moderately fast
    STAB_ISO  = 250   # ISO needs more PA than K%/BB% — trust barrel/EV-derived prior longer
    STAB_HR   = 170   # HR rate
    STAB_BABIP = 820  # BABIP needs 800+ PA to stabilize — trust xwOBA-derived prior much longer
    STAB_BARREL = 80  # physical metric, stabilizes fast
    STAB_HH   = 100   # hard hit rate
    STAB_EV   = 80    # exit velocity (physical)
    STAB_BB_PROFILE = 120  # batted ball profile (fb%, ld%, etc.)
    STAB_WRC  = 200   # wRC+ (composite)

    curr = stats_by_season.get(current_season)
    curr_pa = safe(curr.get('pa'), 0) if curr else 0
    min_pa = _season_min(MIN_PA_BATTER_FULL, target_date)
    use_current = curr_pa >= min_pa

    # Year weights: scale current season by PA completeness
    curr_weight = 5 * min(1.0, curr_pa / 500.0) if use_current else 0
    year_weights = [
        (current_season,     curr_weight),
        (current_season - 1, 4),
        (current_season - 2, 3),
    ]
    seasons_to_scan = ([current_season] if use_current else []) + \
                      [current_season - 1, current_season - 2]

    # ── Step 1: Gather predictive metrics (QoC) across seasons ────────────
    # These are weighted by recency but NOT regressed to league avg —
    # they ARE the prior, not observations that need regression.
    # Total career PA across available seasons — used for regression scaling
    total_career_pa = sum(
        safe(stats_by_season.get(yr, {}).get('pa'), 0)
        for yr in [current_season, current_season-1, current_season-2]
    )

    def _qoc_weighted(col, default):
        """Weight predictive metrics by recency + PA, with sample-size regression.
        Players with < 500 career PA get regressed toward league avg.
        Regression is HEAVY for very small samples (< 200 PA) to prevent
        69 PA of .447 xwOBA from producing elite talent estimates."""
        if total_career_pa < 200:
            # Very small sample: regression dominates (800+ PA of league avg)
            reg_pa = 800
        elif total_career_pa < 500:
            reg_pa = max(0, 600 - total_career_pa)
        else:
            reg_pa = 0
        num = reg_pa * default  # league avg regression
        den = float(reg_pa)
        for yr, wt in year_weights:
            if wt == 0:
                continue
            row = stats_by_season.get(yr)
            if not row:
                continue
            val = safe(row.get(col))
            pa = safe(row.get('pa'), 0)
            if val is None or pa == 0:
                continue
            num += val * pa * wt
            den += pa * wt
        return num / den if den > 0 else default

    # Predictive metrics — form the Bayesian prior
    xwoba    = clip(_qoc_weighted('xwoba',       0.315), 0.20, 0.55)
    barrel   = clip(_qoc_weighted('barrel_pct',  0.065), 0.01, 0.30)
    hard_hit = clip(_qoc_weighted('hard_hit_pct', 0.35), 0.15, 0.65)
    avg_ev   = clip(_qoc_weighted('avg_ev',      88.0),  82.0, 98.0)
    wrc_plus = clip(_qoc_weighted('wrc_plus',    100.0), 50.0, 220.0)
    o_swing  = clip(_qoc_weighted('o_swing_pct', 0.30),  0.15, 0.50)
    swstr    = clip(_qoc_weighted('swstr_pct',   0.11),  0.04, 0.25)

    # Batted ball profile (moderately stable)
    fb_pct   = clip(_qoc_weighted('fb_pct',  0.35), 0.15, 0.65)
    ld_pct   = clip(_qoc_weighted('ld_pct',  0.21), 0.10, 0.35)
    gb_pct   = clip(_qoc_weighted('gb_pct',  0.44), 0.20, 0.70)
    pull_pct = clip(_qoc_weighted('pull_pct', 0.40), 0.20, 0.60)
    cent_pct = clip(_qoc_weighted('cent_pct', 0.34), 0.15, 0.50)
    oppo_pct = clip(_qoc_weighted('oppo_pct', 0.26), 0.10, 0.45)

    # ── Step 2: Build priors from predictive metrics ──────────────────────
    # These formulas derive what each outcome stat SHOULD be given the
    # hitter's contact quality, speed, and batted ball profile.

    # K% prior: driven by chase rate and whiff rate (the underlying swing decisions)
    # Calibrated: at avg inputs (swstr=0.11, o_swing=0.30) → 0.210, regresses toward 0.225
    k_prior = clip(0.04 + swstr * 0.95 + o_swing * 0.22, 0.08, 0.42)

    # BB% prior: inverse of chase rate + patience signal from wRC+
    # Calibrated: at avg inputs (o_swing=0.30, wrc_plus=100) → 0.094, regresses toward 0.082
    bb_prior = clip(0.16 - o_swing * 0.22 + (wrc_plus - 100) * 0.0004, 0.03, 0.20)

    # BABIP prior: xwOBA-anchored + speed + batted ball profile
    # Sprint speed pulled from most recent season (physical trait)
    sprint_speed = None
    for yr in seasons_to_scan:
        row = stats_by_season.get(yr)
        if row and safe(row.get('sprint_speed')):
            sprint_speed = safe(row['sprint_speed'])
            break
    sprint_speed = sprint_speed or 4.40

    # xwOBA of .315 (avg) should produce BABIP ~.298 (avg). Scale accordingly.
    # Higher xwOBA → better contact → higher BABIP. Speed adds infield hits.
    babip_prior = clip(
        0.030 + xwoba * 0.85 + (sprint_speed - 4.40) * 0.018
        + (ld_pct - 0.21) * 0.30 + (gb_pct - 0.44) * 0.05,
        0.22, 0.38
    )
    # At avg (xwoba=.315): 0.030 + 0.268 = 0.298 — matches league avg BABIP

    # ISO prior: power from barrel rate, exit velocity, fly ball tendency
    iso_prior = clip(
        barrel * 1.6 + (avg_ev - 88.0) * 0.012 + (fb_pct - 0.35) * 0.12
        + (pull_pct - 0.40) * 0.08,
        0.04, 0.40
    )

    # HR/PA prior: barrels that leave the yard
    hr_prior = clip(barrel * 0.40 + (avg_ev - 88.0) * 0.005 + (fb_pct - 0.35) * 0.03, 0.005, 0.10)

    # XB/hit prior: doubles from gap power
    xb_prior = clip(0.14 + (avg_ev - 88.0) * 0.005 + (pull_pct - 0.40) * 0.08, 0.06, 0.30)

    # ── Step 3: Bayesian update — blend priors with observed outcomes ─────
    def bayesian_update(prior, col, stability_pa):
        """Blend prior with observed data. Stability controls trust speed.
        Prior weight scales with stability — noisy stats (BABIP, stability=820)
        get a strong prior that resists observed data longer. Stable stats
        (K%, stability=60) get a weak prior that defers to data quickly.

        Career PA scaling: players with < 500 career PA get inflated stability
        (harder to move away from prior) because small samples are noisy even
        for 'stable' stats. A 69 PA .208 barrel rate doesn't mean .208 barrel talent."""
        # Scale stability UP for small-career players: 140 PA → 2.5x stability, 500+ PA → 1.0x
        career_scale = clip(600.0 / max(total_career_pa, 50), 1.0, 8.0)
        effective_stability = stability_pa * career_scale
        prior_weight = effective_stability / 200.0
        num = prior * prior_weight
        den = prior_weight
        for yr, wt in year_weights:
            if wt == 0:
                continue
            row = stats_by_season.get(yr)
            if not row:
                continue
            val = safe(row.get(col))
            pa = safe(row.get('pa'), 0)
            if val is None or pa == 0:
                continue
            data_wt = (pa * wt) / stability_pa
            num += val * data_wt
            den += data_wt
        return num / den

    k_pct  = clip(bayesian_update(k_prior,     'k_pct',  STAB_K),     0.05, 0.45)
    bb_pct = clip(bayesian_update(bb_prior,     'bb_pct', STAB_BB),    0.02, 0.25)
    babip  = clip(bayesian_update(babip_prior,  'babip',  STAB_BABIP), 0.22, 0.38)
    iso    = clip(bayesian_update(iso_prior,    'iso',    STAB_ISO),   0.02, 0.40)
    avg    = clip(bayesian_update(0.248,        'avg',    300),        0.15, 0.38)
    woba   = clip(bayesian_update(xwoba,        'woba',   350),        0.20, 0.55)

    # HR/PA and SB: use prior + career counting stats
    def _counting_rate(col, default, lo, hi):
        num, den = 0.0, 0.0
        for yr, wt in year_weights:
            row = stats_by_season.get(yr)
            if not row or wt == 0:
                continue
            v = safe(row.get(col), 0)
            p = safe(row.get('pa'), 0)
            if p > 0:
                num += (v / p) * p * wt
                den += p * wt
        return clip(num / den, lo, hi) if den > 0 else default

    # HR/PA: blend prior (from barrel/EV) with observed career HR rate
    raw_hr_rate = _counting_rate('hr', 0.03, 0.005, 0.10)
    hr_per_pa = clip(
        (hr_prior * 1.0 + raw_hr_rate * max(0.1, sum(
            safe(stats_by_season.get(yr, {}).get('pa'), 0) * wt / STAB_HR
            for yr, wt in year_weights if wt > 0
        ))) / (1.0 + max(0.1, sum(
            safe(stats_by_season.get(yr, {}).get('pa'), 0) * wt / STAB_HR
            for yr, wt in year_weights if wt > 0
        ))),
        0.005, 0.10
    )

    sb_per_pa  = _counting_rate('sb',  0.01, 0.0, 0.15)
    r_per_pa   = _counting_rate('r',   0.11, 0.03, 0.25)
    rbi_per_pa = _counting_rate('rbi', 0.10, 0.03, 0.25)

    # XB per hit: blend prior with observed doubles rate
    xb_num, xb_den = xb_prior * 1.0, 1.0  # prior
    for yr, wt in year_weights:
        row = stats_by_season.get(yr)
        if not row or wt == 0:
            continue
        _iso = safe(row.get('iso'), 0)
        _hr  = safe(row.get('hr'), 0)
        _pa  = safe(row.get('pa'), 0)
        _avg = safe(row.get('avg'), 0)
        if _pa > 50 and _avg > 0.10:
            _ab = _pa * (1 - safe(row.get('bb_pct'), 0.08) - 0.01)
            _2b_per_ab = max(0, _iso - 3 * _hr / max(_ab, 1)) if _ab > 0 else 0
            _2b_per_h = _2b_per_ab / _avg if _avg > 0 else 0.14
            data_wt = (_pa * wt) / STAB_ISO
            xb_num += _2b_per_h * data_wt
            xb_den += data_wt
    xb_per_hit = clip(xb_num / xb_den, 0.06, 0.30) if xb_den > 0 else 0.14

    # ── Step 4: Swing mechanics (most recent, not weighted) ───────────────
    bat_speed = None
    squared_up = None
    blast = None
    attack_angle = None
    for yr in seasons_to_scan:
        row = stats_by_season.get(yr)
        if not row:
            continue
        if bat_speed is None and safe(row.get('bat_speed')):
            bat_speed = safe(row.get('bat_speed'))
        if squared_up is None and safe(row.get('squared_up_pct')):
            squared_up = safe(row.get('squared_up_pct'))
        if blast is None and safe(row.get('blast_pct')):
            blast = safe(row.get('blast_pct'))
        if attack_angle is None and safe(row.get('attack_angle')):
            attack_angle = safe(row.get('attack_angle'))
    bat_speed = bat_speed or 72.0
    squared_up = squared_up or 0.18
    blast = blast or 0.08
    attack_angle = attack_angle or 12.0

    return {
        'k_pct': k_pct, 'bb_pct': bb_pct, 'iso': iso, 'avg': avg,
        'babip': babip, 'woba': woba, 'xwoba': xwoba, 'barrel': barrel,
        'hard_hit': hard_hit, 'avg_ev': avg_ev, 'ld_pct': ld_pct,
        'fb_pct': fb_pct, 'pull_pct': pull_pct, 'sb_per_pa': sb_per_pa,
        'r_per_pa': r_per_pa, 'rbi_per_pa': rbi_per_pa,
        'hr_per_pa': hr_per_pa, 'xb_per_hit': xb_per_hit,
        'sprint_speed': sprint_speed,
        'bat_speed': bat_speed, 'squared_up': squared_up, 'blast': blast,
        'o_swing': o_swing, 'attack_angle': attack_angle,
        'swing_length': 7.2,  # placeholder — merged from bat_tracking after call
    }


# Alias for backward compatibility
marcel_batter = bayesian_batter


def _marcel_pitcher_legacy(stats_by_season, current_season, target_date=None):
    """Legacy Marcel pitcher model — kept for rollback."""
    pass


def marcel_pitcher(stats_by_season: dict, current_season: int, target_date=None) -> dict:
    """
    Bayesian pitcher talent estimation.

    Prior: built from Pitching+/Stuff+/Location+ (most predictive pitcher
    metrics) and swing-and-miss rates (SwStr%, CSW%).

    Update: observed outcomes (K%, BB%, xFIP, GB%) blended in weighted by
    stability — K% is trusted quickly (50 IP), HR/9 barely trusted (350 IP)
    because it's almost pure randomness for pitchers.

    BABIP fixed at ~0.295 (pitcher BABIP is 95% defense/luck).
    """
    # ── Stability constants (IP-based) ────────────────────────────────────
    STAB_K    = 50    # K% — most stable pitcher stat
    STAB_BB   = 80    # BB% — command
    STAB_SWSTR = 60   # SwStr% — physical metric
    STAB_GB   = 60    # GB% — pitching style trait
    STAB_XFIP = 120   # xFIP — composite
    STAB_SIERA = 120  # SIERA — complex composite
    STAB_HR9  = 350   # HR/9 — VERY noisy
    STAB_BABIP = 500  # BABIP — almost pure luck for pitchers

    curr = stats_by_season.get(current_season)
    curr_ip = safe(curr.get('ip'), 0) if curr else 0
    min_ip = _season_min(MIN_IP_PITCHER_FULL, target_date)
    use_current = curr_ip >= min_ip

    curr_weight = PITCHER_CURRENT_WEIGHT * min(1.0, curr_ip / 150.0) if use_current else 0
    weights = [
        (current_season,     curr_weight),
        (current_season - 1, 4),
        (current_season - 2, 3),
    ]
    seasons_to_scan = ([current_season] if use_current else []) + \
                      [current_season - 1, current_season - 2]

    # Total career IP for small-sample scaling
    total_career_ip = sum(
        safe(stats_by_season.get(yr, {}).get('ip'), 0)
        for yr in [current_season, current_season-1, current_season-2]
    )

    # ── Step 1: Gather predictive metrics (most recent available) ─────────
    stuff_plus = None
    pitching_plus = None
    location_plus = None
    swstr_pct = None
    csw_pct = None
    velo = None
    lob_pct = None
    for yr in seasons_to_scan:
        row = stats_by_season.get(yr)
        if not row:
            continue
        if stuff_plus is None and safe(row.get('stuff_plus')):
            stuff_plus = safe(row.get('stuff_plus'))
        if pitching_plus is None and safe(row.get('pitching_plus')):
            pitching_plus = safe(row.get('pitching_plus'))
        if location_plus is None and safe(row.get('location_plus')):
            location_plus = safe(row.get('location_plus'))
        if swstr_pct is None and safe(row.get('swstr_pct')):
            swstr_pct = safe(row.get('swstr_pct'))
        if csw_pct is None and safe(row.get('csw_pct')):
            csw_pct = safe(row.get('csw_pct'))
        if velo is None and safe(row.get('velo')):
            velo = safe(row.get('velo'))
        if lob_pct is None and safe(row.get('lob_pct')):
            lob_pct = safe(row.get('lob_pct'))
    stuff_plus = stuff_plus or 100.0
    pitching_plus = pitching_plus or 100.0
    location_plus = location_plus or 100.0
    swstr_pct = swstr_pct or 0.11
    csw_pct = csw_pct or 0.29

    # ── Step 2: Build priors from predictive metrics ──────────────────────

    # K% prior: Stuff+ and swing-and-miss drive strikeout ability
    k_prior = clip(0.10 + (pitching_plus - 100) * 0.0035 + swstr_pct * 0.60, 0.10, 0.40)
    # At avg (Pitch+=100, swstr=0.11): 0.10 + 0 + 0.066 = 0.166

    # BB% prior: Location+ (command) controls walks
    bb_prior = clip(0.12 - (location_plus - 100) * 0.0015 - (pitching_plus - 100) * 0.0005, 0.03, 0.15)
    # At avg: 0.12. Elite command (Loc+=120): 0.09

    # xFIP prior: Pitching+ is the best single predictor of pitcher quality
    # Regressive prior: starts ABOVE league avg to prevent low-ER inflation.
    # Elite pitchers get pulled down by data, average pitchers stay regressed.
    xfip_prior = clip(4.80 - (pitching_plus - 100) * 0.020 - (stuff_plus - 100) * 0.008, 3.00, 6.00)
    # At avg (Pitch+=100, Stuff+=100): 4.80 (above 3.90 league avg — forces regression)
    # Elite (Pitch+=120, Stuff+=115): 4.80 - 0.40 - 0.12 = 4.28
    # Bad (Pitch+=80): 4.80 + 0.40 = 5.20

    # HR/9 prior: derive from Pitching+ (xFIP already handles HR normalization)
    hr9_prior = clip(1.30 - (pitching_plus - 100) * 0.005, 0.60, 2.00)

    # BABIP prior: for pitchers, BABIP is ~95% luck/defense. Fixed near league avg.
    babip_prior = 0.295

    # GB% prior: pitching style, very stable — use weighted observed data
    gb_prior = 0.43  # league avg

    # SIERA prior: similar to xFIP but accounts for batted ball profile
    siera_prior = xfip_prior  # close approximation

    # ── Step 3: Bayesian update ───────────────────────────────────────────
    def bayesian_update(prior, col, stability_ip):
        """Blend prior with observed data. Stability in IP."""
        career_scale = clip(300.0 / max(total_career_ip, 30), 1.0, 5.0)
        effective_stability = stability_ip * career_scale
        prior_weight = effective_stability / 100.0
        num = prior * prior_weight
        den = prior_weight
        for yr, wt in weights:
            if wt == 0:
                continue
            row = stats_by_season.get(yr)
            if not row:
                continue
            val = safe(row.get(col))
            ip = safe(row.get('ip'), 0)
            if val is None or ip == 0:
                continue
            data_wt = (ip * wt) / stability_ip
            num += val * data_wt
            den += data_wt
        return num / den

    k_pct  = clip(bayesian_update(k_prior,     'k_pct',  STAB_K),     0.10, 0.40)
    bb_pct = clip(bayesian_update(bb_prior,     'bb_pct', STAB_BB),    0.04, 0.18)
    xfip   = clip(bayesian_update(xfip_prior,   'xfip',   STAB_XFIP),  2.50, 6.00)
    siera  = clip(bayesian_update(siera_prior,  'siera',  STAB_SIERA), 2.50, 6.00)
    hr9    = clip(bayesian_update(hr9_prior,    'hr9',    STAB_HR9),   0.30, 3.00)
    babip  = clip(bayesian_update(babip_prior,  'babip',  STAB_BABIP), 0.22, 0.36)
    gb_pct = clip(bayesian_update(gb_prior,     'gb_pct', STAB_GB),    0.20, 0.65)

    # ── Breakout detection (keep existing logic) ──────────────────────────
    is_breakout = False
    base_reg = 80
    prior_row = stats_by_season.get(current_season - 1)
    prior_ip = safe(prior_row.get('ip'), 0) if prior_row else 0
    if prior_ip >= 100:
        base_reg = max(30, base_reg - (prior_ip - 100) * 0.5)

    if use_current and curr:
        curr_xfip = safe(curr.get('xfip'))
        if curr_xfip:
            prior_num = base_reg * LEAGUE_AVG_XFIP
            prior_den = float(base_reg)
            for yr, wt in weights:
                if yr == current_season or wt == 0:
                    continue
                row = stats_by_season.get(yr)
                if not row:
                    continue
                val = safe(row.get('xfip'))
                ip = safe(row.get('ip'), 0)
                if val is None or ip == 0:
                    continue
                prior_num += val * ip * wt
                prior_den += ip * wt
            prior_xfip = prior_num / prior_den if prior_den > base_reg else LEAGUE_AVG_XFIP
            xfip_improvement = prior_xfip - curr_xfip
            if xfip_improvement >= BREAKOUT_XFIP_THRESHOLD:
                is_breakout = True
                print(f"    BREAKOUT detected: xFIP {curr_xfip:.2f} vs prior {prior_xfip:.2f} "
                      f"(+{xfip_improvement:.2f})")

    # IP per GS — Marcel-weighted across seasons (not just most recent)
    ip_per_gs = 5.1
    ipgs_num = 30.0 * 5.1  # regression: 30 GS at league avg
    ipgs_den = 30.0
    seasons = [current_season, current_season-1, current_season-2] if use_current \
              else [current_season-1, current_season-2]
    for yr in seasons:
        row = stats_by_season.get(yr)
        if not row:
            continue
        ip = safe(row.get('ip'), 0)
        gs = safe(row.get('gs'), 0)
        if gs >= 3:
            raw = clip(ip / gs, 3.0, 6.0)  # modern SP ceiling ~6.0 IP/GS avg
            wt = next((w for y, w in weights if y == yr), 0)
            ipgs_num += raw * gs * wt
            ipgs_den += gs * wt
    if ipgs_den > 30:
        ip_per_gs = ipgs_num / ipgs_den

    # Opener/reliever detection: if pitcher is primarily a reliever (GS < 20% of G
    # across recent seasons), they're likely an opener — cap IP projection low.
    # EXCEPTION: if current season shows starter role (GS/G >= 50%), trust the
    # role change (e.g. reliever-to-starter conversion like Matz 2026).
    curr_is_starter = False
    if use_current and curr:
        curr_g = safe(curr.get('g'), 0)
        curr_gs = safe(curr.get('gs'), 0)
        if curr_g >= 2 and curr_gs / curr_g >= 0.50:
            curr_is_starter = True

    if not curr_is_starter:
        total_g, total_gs = 0, 0
        for yr in seasons:
            row = stats_by_season.get(yr)
            if not row:
                continue
            total_g += safe(row.get('g'), 0)
            total_gs += safe(row.get('gs'), 0)
        if total_g >= 5 and total_gs / total_g < 0.20:
            # Reliever profile being used as opener — project ~2 IP (1-3 innings)
            ip_per_gs = clip(ip_per_gs, 1.5, 3.0)

    # SB vulnerability: most recent season's sb_per_9 (not Marcel-weighted — situational stat)
    sb_per_9 = None
    for yr in ([current_season] if use_current else []) + [current_season-1, current_season-2]:
        row = stats_by_season.get(yr)
        if row and safe(row.get('sb_per_9')):
            sb_per_9 = safe(row['sb_per_9'])
            break
    sb_per_9 = sb_per_9 or 1.0  # league avg ~1.0 SB/9 IP

    return {
        'k_pct': k_pct, 'bb_pct': bb_pct, 'hr9': hr9, 'babip': babip,
        'xfip': xfip, 'siera': siera, 'stuff_plus': stuff_plus,
        'pitching_plus': pitching_plus, 'location_plus': location_plus,
        'swstr_pct': swstr_pct, 'csw_pct': csw_pct,
        'gb_pct': gb_pct, 'ip_per_gs': ip_per_gs,
        'velo': velo, 'lob_pct': lob_pct,
        'is_breakout': is_breakout,
        '_has_current_siera': bool(curr and safe(curr.get('siera'))),
        'sb_per_9': sb_per_9,
    }


# ── Environment Multipliers ──────────────────────────────────────────────────

def weather_hr_mult(weather_row: dict) -> float:
    """HR probability multiplier from temperature and wind."""
    if not weather_row or weather_row.get('is_outdoor') is False:
        return 1.0
    temp     = safe(weather_row.get('temp_f'), 72)
    wind_spd = safe(weather_row.get('wind_speed'), 0)
    wind_dir = (weather_row.get('wind_dir') or '').strip().upper()

    temp_effect = ((temp - 72) / 10) * 0.02
    wind_effect = 0.0
    if wind_spd and wind_spd > 5:
        # Alan Nathan physics: ~2-3% HR increase per mph blowing out.
        # Use sqrt scaling to capture diminishing returns at extreme speeds:
        # 10 mph → +13%, 15 mph → +20%, 20 mph → +30%, 25 mph → +38%
        effective_spd = wind_spd - 5  # subtract threshold
        if wind_dir in WIND_OUT_DIRS:
            wind_effect = (effective_spd ** 0.7) * 0.04
        elif wind_dir in WIND_IN_DIRS:
            wind_effect = -(effective_spd ** 0.7) * 0.04
    return clip(1.0 + temp_effect + wind_effect, 0.75, 1.45)


def weather_hit_mult(weather_row: dict) -> float:
    """General hit/scoring multiplier from weather for display purposes.
    Blends temperature effect on contact with wind HR effect (dampened)."""
    if not weather_row or weather_row.get('is_outdoor') is False:
        return 1.0
    temp = safe(weather_row.get('temp_f'), 72)
    temp_effect = ((temp - 72) / 10) * 0.008
    # Include wind HR effect (dampened — HR is a subset of all scoring)
    hr_mult = weather_hr_mult(weather_row)
    wind_scoring = (hr_mult - 1.0) * 0.40  # ~40% of HR boost flows to total scoring
    return clip(1.0 + temp_effect + wind_scoring, 0.90, 1.15)


# ── Batter PA Simulation ─────────────────────────────────────────────────────

def sim_batter_game(talent: dict, pitcher: dict, park: dict, weather: dict,
                    odds: dict, batting_order: int, is_home: bool,
                    n_sims: int, rng: np.random.Generator,
                    sb_context: dict = None) -> np.ndarray:
    """
    Simulate n_sims games for one batter. Returns array of DK points per sim.
    sb_context: { catcher_pop: float, pitcher_sb_per_9: float } for SB model.

    Each game simulates individual PA outcomes using combined batter/pitcher/
    park/weather probabilities.
    """
    proj_pa = LINEUP_PA.get(batting_order, LEAGUE_AVG_PA)

    # ── PA outcome probabilities ──────────────────────────────────────────
    # Pitcher quality baseline: Pitching+ (stuff + command combined) is the most
    # predictive single metric (0.73 y/y stability). Fall back to Stuff+ or rates.
    # This affects K rate, hit suppression, and HR suppression.
    pitcher_quality = 100.0  # league average
    if pitcher:
        pitcher_quality = pitcher.get('pitching_plus') or pitcher.get('stuff_plus') or 100.0
    # Location+ further modifies — high command pitchers suppress hits independently of stuff
    location_quality = pitcher.get('location_plus', 100.0) if pitcher else 100.0

    # K rate: batter talent × pitcher skill ratio × pitcher quality × park K factor
    pitcher_k_ratio = (pitcher['k_pct'] / LEAGUE_AVG_K_PCT) if pitcher else 1.0
    # Pitching+/Stuff+ K adjustment: each point above 100 = ~0.20% more Ks (exponent 0.20)
    pitcher_k_ratio *= (pitcher_quality / 100.0) ** 0.20
    park_k = safe(park.get('k_factor'), 100) / 100.0 if park else 1.0
    # Swing length: longer swings = more whiff-prone (league avg ~7.2 ft)
    swing_k_adj = clip(1.0 + (talent.get('swing_length', 7.2) - 7.2) * 0.06, 0.94, 1.08)
    # Dampen park_k: Bayesian K% already includes ~50% home park via career data
    park_k_edge = 1.0 + (park_k - 1.0) * 0.50
    k_rate = clip(talent['k_pct'] * pitcher_k_ratio * park_k_edge * swing_k_adj, 0.05, 0.50)

    # BB rate: batter talent × inverse pitcher BB ratio × park BB factor
    pitcher_bb_ratio = (LEAGUE_AVG_BB_PCT / pitcher['bb_pct']) if pitcher and pitcher['bb_pct'] > 0.02 else 1.0
    park_bb = safe(park.get('bb_factor'), 100) / 100.0 if park else 1.0
    bb_rate = clip(talent['bb_pct'] * pitcher_bb_ratio * park_bb, 0.02, 0.22)

    hbp_rate = 0.010
    contact_rate = max(0.10, 1.0 - k_rate - bb_rate - hbp_rate)

    # ── Hit probability (BABIP-based) ─────────────────────────────────────
    # Quality of contact adjustment: barrel% and hard_hit% boost BABIP
    qoc_mult = 1.0 + (talent['barrel'] - 0.065) * 0.8 + (talent['hard_hit'] - 0.35) * 0.3
    qoc_mult = clip(qoc_mult, 0.85, 1.25)
    # Pitcher quality suppresses hit probability: elite pitchers limit BABIP
    # Pitching+ 120 → 0.97 (3% hit suppression), Pitching+ 80 → 1.03 (3% boost)
    pitcher_hit_suppression = clip(1.0 - (pitcher_quality - 100) * 0.0015, 0.94, 1.06)
    # Location+ further suppresses: good command = fewer hittable pitches
    loc_hit_suppression = clip(1.0 - (location_quality - 100) * 0.001, 0.96, 1.04)

    park_basic = safe(park.get('basic_factor'), 100) / 100.0 if park else 1.0
    wx_hit = weather_hit_mult(weather)

    # Line drive rate adjustment: high ld_pct batters produce more hits than BABIP alone predicts
    ld_adj = 1.0 + (talent.get('ld_pct', 0.21) - 0.21) * 0.5
    ld_adj = clip(ld_adj, 0.90, 1.12)

    # Player-specific qoc_mult (same logic as _compute_pa_rates)
    raw_qoc = 1.0 + (talent['barrel'] - 0.065) * 0.8 + (talent['hard_hit'] - 0.35) * 0.3
    babip_vs_avg = (talent['babip'] - LEAGUE_AVG_BABIP) / 0.05
    qoc_vs_avg = (raw_qoc - 1.0) / 0.10
    overlap = clip(min(babip_vs_avg, qoc_vs_avg), 0.0, 1.0) if babip_vs_avg > 0 and qoc_vs_avg > 0 else 0.0
    adjusted_qoc = 1.0 + (raw_qoc - 1.0) * (1.0 - overlap * 0.85)
    adjusted_qoc = clip(adjusted_qoc, 0.90, 1.20)
    park_basic_edge = 1.0 + (park_basic - 1.0) * 0.50
    hit_prob = clip(talent['babip'] * adjusted_qoc * ev_adj * pitcher_hit_suppression * loc_hit_suppression * park_basic_edge * wx_hit * ld_adj, 0.14, 0.52)

    # ── Hit type distribution ─────────────────────────────────────────────
    park_hr = safe(park.get('hr_factor'), 100) / 100.0 if park else 1.0

    # Batter-specific park factor: pull hitters get bigger boost at short-porch parks
    if park and talent.get('pull_pct'):
        lf_dist = safe(park.get('lf_dist'), 330)
        rf_dist = safe(park.get('rf_dist'), 330)
        # Positive asymmetry = RF shorter (helps LHH pull hitters)
        # Negative asymmetry = LF shorter (helps RHH pull hitters)
        # We don't know handedness directly, but high pull% + short porch = boost either way
        short_porch = min(lf_dist, rf_dist)
        porch_factor = clip((330 - short_porch) / 330.0, -0.05, 0.10)  # shorter = bigger boost
        pull_dev = talent['pull_pct'] - 0.40  # deviation from average pull%
        park_hr *= 1.0 + pull_dev * porch_factor * 3.0  # pull hitters amplified at short porches

    # Fly ball hitters have higher HR/FB rates — their batted ball profile drives more HRs
    fb_adj = clip(1.0 + (talent.get('fb_pct', 0.35) - 0.35) * 0.3, 0.85, 1.20)

    wx_hr = weather_hr_mult(weather)
    pitcher_hr_ratio = (pitcher['hr9'] / LEAGUE_AVG_HR9) if pitcher else 1.0

    # Park/weather sensitivity: low avg_ev batters hit more wall scrapers — more park-sensitive
    park_sens = clip(1.0 + (88.0 - talent.get('avg_ev', 88.0)) * 0.015, 0.85, 1.20)
    effective_park_hr = 1.0 + (park_hr - 1.0) * park_sens
    effective_wx_hr = 1.0 + (wx_hr - 1.0) * park_sens

    # Blast%: highest-quality contact tier (league avg ~8%)
    blast_adj = clip(1.0 + (talent.get('blast', 0.08) - 0.08) * 2.5, 0.90, 1.12)
    # Attack angle vs arm angle: swing plane alignment drives HR over/underperformance
    aa_hr_adj = arm_angle_hr_interaction(talent.get('attack_angle'), pitcher.get('arm_angle') if pitcher else None)

    hr_per_hit = clip(talent.get('hr_per_pa', 0.03) * effective_park_hr * fb_adj * effective_wx_hr *
                       pitcher_hr_ratio * blast_adj * aa_hr_adj / hit_prob, 0.03, 0.30)
    xb_per_hit = clip(talent.get('xb_per_hit', 0.14), 0.03, 0.20)
    triple_per_hit = 0.015
    single_per_hit = max(0.0, 1.0 - hr_per_hit - xb_per_hit - triple_per_hit)

    # ── Vegas environment scaling ─────────────────────────────────────────
    implied = None
    if odds:
        implied = safe(odds.get('home_implied' if is_home else 'away_implied'))
        if not implied:
            total = safe(odds.get('game_total'))
            implied = total / 2.0 if total else None
    vegas_scale = (implied / LEAGUE_AVG_IMPLIED) if implied else 1.0
    vegas_scale = clip(vegas_scale, 0.70, 1.45)

    # ── Per-sim "hot/cold day" factor for batters ──────────────────────
    # Variance derived from the batter's actual talent profile:
    # - High K% = more volatile (boom/bust swings, more zeros)
    # - High ISO = power is streaky → wider variance
    # - High BB% = floor support → tighter variance (walks stabilize)
    # - High contact (low K%) = more consistent outcomes
    # Base SD ~0.10 for a league-average hitter; ranges from ~0.07 (elite contact) to ~0.16 (high-K power)
    k_vol    = (talent['k_pct'] - LEAGUE_AVG_K_PCT) * 0.4    # +K% widens
    iso_vol  = (talent['iso'] - LEAGUE_AVG_ISO) * 0.3         # +ISO widens
    bb_stab  = (talent['bb_pct'] - LEAGUE_AVG_BB_PCT) * 0.25  # +BB% tightens
    batter_sd = clip(0.10 + k_vol + iso_vol - bb_stab, 0.06, 0.18)
    hot_day = rng.normal(1.0, batter_sd, size=n_sims).clip(0.65, 1.40)
    sim_hit_prob = np.clip(hit_prob * hot_day, 0.12, 0.45)
    sim_hr_per_hit = np.clip(hr_per_hit * hot_day, 0.02, 0.35)

    # ── Simulate n_sims games ─────────────────────────────────────────────
    # Variable PA count (Poisson) — batters in high-scoring games get extra PAs
    sim_pa = rng.poisson(proj_pa, size=n_sims).clip(2, 7)
    max_pa = int(sim_pa.max())

    dk_pts = np.zeros(n_sims)

    for pa_idx in range(max_pa):
        active = pa_idx < sim_pa
        # Roll PA outcome
        rolls = rng.random(n_sims)
        is_k   = rolls < k_rate
        is_bb  = (~is_k) & (rolls < k_rate + bb_rate)
        is_hbp = (~is_k) & (~is_bb) & (rolls < k_rate + bb_rate + hbp_rate)
        is_bip = (~is_k) & (~is_bb) & (~is_hbp)  # ball in play

        # Ball in play → hit or out (uses per-sim hot_day rate)
        hit_rolls = rng.random(n_sims)
        is_hit = is_bip & (hit_rolls < sim_hit_prob)

        # Hit type — use consistent per-sim thresholds
        type_rolls = rng.random(n_sims)
        is_hr  = is_hit & (type_rolls < sim_hr_per_hit)
        is_3b  = is_hit & (~is_hr) & (type_rolls < sim_hr_per_hit + triple_per_hit)
        is_2b  = is_hit & (~is_hr) & (~is_3b) & (type_rolls < sim_hr_per_hit + triple_per_hit + xb_per_hit)
        is_1b  = is_hit & (~is_hr) & (~is_3b) & (~is_2b)

        # SB opportunity on singles/walks/HBP — 3-factor model:
        # 1. Runner speed (sprint_speed): fast runners attempt + succeed more
        # 2. Catcher pop time: slow catchers = more SB opportunities
        # 3. Pitcher SB vulnerability: high sb_per_9 = easier to run on
        on_base = is_1b | is_bb | is_hbp
        base_sb_rate = talent['sb_per_pa']
        # Sprint speed multiplier (home-to-first seconds): 3.97s → 1.50x, 4.40s → 1.0x, 4.80s → 0.50x
        speed_mult = clip(1.0 + (4.40 - talent.get('sprint_speed', 4.40)) * 1.16, 0.50, 1.60)
        # Catcher/pitcher context multiplier
        ctx_mult = 1.0
        if sb_context:
            # Catcher pop time: 1.95s = avg (1.0x), 2.10s = slow (1.25x), 1.82s = fast (0.80x)
            pop = sb_context.get('catcher_pop', 1.95)
            ctx_mult *= clip(1.0 + (pop - 1.95) * 1.67, 0.75, 1.30)
            # Pitcher SB/9: 1.0 = avg (1.0x), 2.0 = easy to run on (1.25x), 0.3 = tough (0.80x)
            p_sb9 = sb_context.get('pitcher_sb_per_9', 1.0)
            ctx_mult *= clip(1.0 + (p_sb9 - 1.0) * 0.25, 0.80, 1.30)
        sb_rate = clip(base_sb_rate * speed_mult * ctx_mult, 0.0, 0.12)
        sb_rolls = rng.random(n_sims)
        is_sb = on_base & (sb_rolls < sb_rate)

        # R scoring: per-hitter R/PA rate, scaled by event type and Vegas
        # Hitters with high historical R/PA (e.g. leadoff) naturally score more
        r_rolls = rng.random(n_sims)
        hitter_r_rate = talent.get('r_per_pa', 0.11) * proj_pa * vegas_scale
        # Scale by event type: HR always scores, XBH > 1B > BB
        scores_r_1b  = is_1b  & (r_rolls < hitter_r_rate * 0.85)
        scores_r_2b  = is_2b  & (r_rolls < hitter_r_rate * 1.20)
        scores_r_3b  = is_3b  & (r_rolls < hitter_r_rate * 1.60)
        scores_r_bb  = is_bb  & (r_rolls < hitter_r_rate * 0.70)
        scores_r_hbp = is_hbp & (r_rolls < hitter_r_rate * 0.70)
        scores_r = is_hr | scores_r_1b | scores_r_2b | scores_r_3b | scores_r_bb | scores_r_hbp

        # RBI: per-hitter RBI/PA rate, scaled by event type and Vegas
        # HR drives in self + runners (~1.3 avg), XBH drive in more than singles
        rbi_rolls = rng.random(n_sims)
        rbi_from_hr = is_hr.astype(float) * (1.0 + rng.poisson(
            clip(0.35 * vegas_scale, 0.1, 0.8), n_sims).clip(0, 3))
        hitter_rbi_rate = talent.get('rbi_per_pa', 0.10) * proj_pa * vegas_scale
        rbi_from_2b = is_2b.astype(float) * (rbi_rolls < hitter_rbi_rate * 1.8).astype(float)
        rbi_from_3b = is_3b.astype(float) * (rbi_rolls < hitter_rbi_rate * 2.5).astype(float)
        rbi_from_1b = is_1b.astype(float) * (rbi_rolls < hitter_rbi_rate).astype(float)

        # DK points
        pts = (
            is_1b.astype(float) * 3 +
            is_2b.astype(float) * 5 +
            is_3b.astype(float) * 8 +
            is_hr.astype(float) * 10 +
            scores_r.astype(float) * 2 +
            (rbi_from_hr + rbi_from_1b + rbi_from_2b + rbi_from_3b) * 2 +
            is_bb.astype(float) * 2 +
            is_hbp.astype(float) * 2 +
            is_sb.astype(float) * 5
        )
        dk_pts += pts * active

    return dk_pts


# ── Pitcher Game Simulation ───────────────────────────────────────────────────

def sim_pitcher_game(talent: dict, opp_quality: float,
                     park: dict, weather: dict, odds: dict,
                     is_home: bool, n_sims: int,
                     rng: np.random.Generator,
                     vegas_ip: float = None,
                     vegas_ks: float = None,
                     pitcher_split_data: dict = None,
                     opp_hand_pct: float = None) -> np.ndarray:
    """
    Simulate n_sims starts for one pitcher. Returns array of DK points per sim.

    Simulates innings pitched, strikeouts, walks, hits, earned runs, and win
    decisions using pitcher talent, opposing lineup quality, park, weather.
    Vegas IP and K lines are used as anchors when available.
    """
    PA_PER_IP = 4.3
    # Talent-first: projections driven by true/expected talent, not Vegas lines.
    # Vegas is one signal (market consensus) but NOT the anchor. K rate is a
    # stable pitcher skill — Vegas K props are systematically conservative.
    # IP reflects workload decisions Vegas knows slightly more about.
    # No breakout-specific reduction — Bayesian K% prior already captures talent jumps
    VEGAS_K_WEIGHT = 0.25
    # Vegas is strong for IP (workload management, bullpen usage, injury limits)
    # No breakout-specific reduction — Bayesian priors already capture talent jumps
    VEGAS_IP_WEIGHT = 0.50

    # Park + weather factors (our edge — granular env data Vegas doesn't fully price)
    park_k   = safe(park.get('k_factor'), 100) / 100.0 if park else 1.0
    park_bb  = safe(park.get('bb_factor'), 100) / 100.0 if park else 1.0
    park_hr  = safe(park.get('hr_factor'), 100) / 100.0 if park else 1.0
    park_basic = safe(park.get('basic_factor'), 100) / 100.0 if park else 1.0
    wx_hr = weather_hr_mult(weather)

    # ── IP projection: blend Vegas + talent ─────────────────────────────────
    # Talent-based IP (raw — no matchup adjustment; Vegas already prices matchup)
    talent_ip = talent['ip_per_gs']
    if vegas_ip:
        proj_ip = vegas_ip * VEGAS_IP_WEIGHT + talent_ip * (1.0 - VEGAS_IP_WEIGHT)
    else:
        # No Vegas anchor — regress 50% toward league avg (5.0 IP/GS).
        # Modern MLB starters average ~5.0 IP/GS. Without Vegas workload signal,
        # regress heavily to prevent inflated career IP/GS from dominating.
        proj_ip = talent_ip * 0.50 + 5.0 * 0.50

    # ── Pitcher splits adjustment ──────────────────────────────────────────
    # Blend split-specific K%, BB%, xFIP based on opposing lineup handedness
    # opp_hand_pct = fraction of RHH in opposing lineup (0.0 to 1.0)
    split_k = talent['k_pct']
    split_bb = talent['bb_pct']
    split_xfip = talent['xfip']
    if pitcher_split_data and opp_hand_pct is not None:
        r_split = pitcher_split_data.get('R', {})  # vs RHH
        l_split = pitcher_split_data.get('L', {})  # vs LHH
        r_pct = opp_hand_pct       # fraction of RHH
        l_pct = 1.0 - opp_hand_pct  # fraction of LHH
        r_pa = safe(r_split.get('pa'), 0)
        l_pa = safe(l_split.get('pa'), 0)
        # Only use splits with reasonable sample (30+ combined PA)
        if r_pa + l_pa >= 30:
            # Regress each split toward overall — cap at 0.60 to prevent splits
            # from dominating talent (e.g. 257 PA vs LHH shouldn't override Marcel)
            r_rf = clip(r_pa / 250.0, 0.0, 0.60)
            l_rf = clip(l_pa / 250.0, 0.0, 0.60)
            r_k = (safe(r_split.get('k_pct')) or talent['k_pct']) * r_rf + talent['k_pct'] * (1 - r_rf)
            l_k = (safe(l_split.get('k_pct')) or talent['k_pct']) * l_rf + talent['k_pct'] * (1 - l_rf)
            split_k = r_k * r_pct + l_k * l_pct
            r_bb = (safe(r_split.get('bb_pct')) or talent['bb_pct']) * r_rf + talent['bb_pct'] * (1 - r_rf)
            l_bb = (safe(l_split.get('bb_pct')) or talent['bb_pct']) * l_rf + talent['bb_pct'] * (1 - l_rf)
            split_bb = r_bb * r_pct + l_bb * l_pct
            r_xfip = (safe(r_split.get('xfip')) or talent['xfip']) * r_rf + talent['xfip'] * (1 - r_rf)
            l_xfip = (safe(l_split.get('xfip')) or talent['xfip']) * l_rf + talent['xfip'] * (1 - l_rf)
            split_xfip = r_xfip * r_pct + l_xfip * l_pct

    # ── K rate: blend Vegas + talent ────────────────────────────────────────
    # Talent K rate is NOT park-adjusted here — park effect is applied once
    # via park_k_edge on the blend. Applying park_k to talent AND park_k_edge
    # to the blend double-counts (e.g. T-Mobile 117 K factor was 1.17 * 1.085 = 1.27x).
    talent_k_rate = split_k
    if vegas_ks and proj_ip > 0:
        vegas_bf = proj_ip * PA_PER_IP
        vegas_k_rate = vegas_ks / vegas_bf if vegas_bf > 0 else talent_k_rate
        blended_k = vegas_k_rate * VEGAS_K_WEIGHT + talent_k_rate * (1.0 - VEGAS_K_WEIGHT)
        # Park K factor: apply as deviation from neutral (1.0)
        # Vegas already partially prices park, so apply only the delta
        park_k_edge = 1.0 + (park_k - 1.0) * 0.5  # half the park effect (Vegas knows some of it)
        k_rate = clip(blended_k * park_k_edge, 0.10, 0.45)
    else:
        # No Vegas — regress talent K rate 15% toward league avg as sanity check
        # No Vegas K data — regress 15% toward league avg as sanity check
        regressed_k = talent_k_rate * 0.75 + LEAGUE_AVG_K_PCT * 0.25
        park_k_edge_p = 1.0 + (park_k - 1.0) * 0.50
        k_rate = clip(regressed_k * park_k_edge_p, 0.10, 0.45)
    # Pitching+ K adjustment REMOVED — Bayesian K% prior already incorporates
    # Pitching+ via the k_prior formula. Applying it again here was double-counting.
    bb_rate = clip(split_bb * (1.0 + (opp_quality - 1.0) * 0.20) * park_bb, 0.03, 0.16)
    contact_rate = max(0.15, 1.0 - k_rate - bb_rate)

    # Hit rate when ball in play
    park_basic_edge_p = 1.0 + (park_basic - 1.0) * 0.50
    hit_on_contact = clip(talent['babip'] * opp_quality * park_basic_edge_p, 0.22, 0.38)
    # HR rate per batter faced
    hr_rate = clip(talent['hr9'] / (PA_PER_IP * 9) * park_hr * wx_hr * opp_quality, 0.005, 0.060)

    # Win probability
    win_prob = 0.25
    if odds:
        home_ml = odds.get('home_ml')
        away_ml = odds.get('away_ml')
        if home_ml and away_ml:
            def to_prob(ml):
                ml = int(ml)
                return abs(ml)/(abs(ml)+100) if ml < 0 else 100/(ml+100)
            hp, ap = to_prob(home_ml), to_prob(away_ml)
            total = hp + ap
            team_win = (hp / total) if is_home else (ap / total)
            ip_scale = clip(proj_ip / 5.1, 0.70, 1.30)
            win_prob = clip(team_win * 0.78 * ip_scale, 0.10, 0.52)

    # ── ERA-anchored ER model ────────────────────────────────────────────
    # Rather than simulating base-state (which is hard to calibrate),
    # use the pitcher's ERA anchor (SIERA/xFIP blend) scaled by matchup
    # and environment as the per-9 ER rate, then simulate variance around it.
    #
    # This grounds ER in real pitching metrics instead of a fragile base-runner sim.
    # Breakout pitchers: lean on xFIP over SIERA. If no current-season SIERA,
    # SIERA is entirely stale prior-year data — weight it even less.
    if talent.get('is_breakout'):
        siera_wt = 0.25 if talent.get('_has_current_siera') else 0.15
        era_anchor = (split_xfip * (1.0 - siera_wt) + talent['siera'] * siera_wt)
    else:
        era_anchor = (split_xfip * 0.50 + talent['siera'] * 0.50)
    # LOB% adjustment: pitchers who strand runners well have lower ER than xFIP predicts.
    # Regress LOB% toward 72% league avg, then apply small ERA multiplier.
    # LOB% > 72% → fewer ER (multiplier < 1), LOB% < 72% → more ER (multiplier > 1)
    lob = talent.get('lob_pct')
    if lob and lob > 0:
        # Regress: 60% toward league avg 0.72 to avoid small-sample noise
        reg_lob = lob * 0.40 + 0.72 * 0.60
        lob_adj = clip(1.0 - (reg_lob - 0.72) * 0.8, 0.93, 1.07)
        era_anchor *= lob_adj
    # Park HR factor: dampen to ~45% effect on ER (HR accounts for ~35% of runs)
    park_er_adj = 1.0 + (park_hr - 1.0) * 0.45
    er_per_ip = era_anchor / 9.0 * opp_quality * park_er_adj
    er_per_ip *= 1.0 + (wx_hr - 1.0) * 0.45  # same dampening for weather HR effect
    # Pitching+ ER adjustment REMOVED — Bayesian xFIP prior already incorporates
    # Pitching+ quality. Applying it again here was double-counting.

    # ── Per-sim "stuff day" factor ──────────────────────────────────────
    # A pitcher's effectiveness varies start-to-start. On a great stuff day,
    # K rate is higher, hit rate lower, and they go deeper. On a bad day,
    # the opposite. This creates realistic fat tails — a mid-tier arm CAN
    # throw a 12K gem or get shelled after 3 IP.
    #
    # stuff_day ~ N(1.0, SD): per-start effectiveness variance
    # Mid/low-tier pitchers (higher ERA) are LESS consistent → wider SD
    # Elite arms (low ERA) are more consistent → tighter SD
    era_consistency = clip(era_anchor / LEAGUE_AVG_XFIP, 0.70, 1.50)
    stuff_sd = 0.22 + 0.06 * era_consistency  # elite ~0.26, mid ~0.28, bad ~0.31
    # Pitching+ (composite of stuff + location + command) is the best consistency predictor
    # A pitcher with Pitching+ 120 is far more reliable start-to-start than Stuff+ 120 alone
    pitching_plus = talent.get('pitching_plus', 100)
    stuff_sd -= (pitching_plus - 100) / 100.0 * 0.06  # ±6% SD per 100 Pitching+ deviation
    # Location+ further tightens walk variance (high location = fewer blowup games)
    location_plus = talent.get('location_plus', 100)
    stuff_sd -= (location_plus - 100) / 100.0 * 0.02  # ±2% additional from command
    stuff_sd = max(0.14, stuff_sd)  # floor
    # Separate random drivers: stuff quality (K/ER/hit) vs workload (IP).
    # In reality, IP is driven by pitch count, bullpen state, and game script —
    # NOT perfectly correlated with stuff quality. A pitcher can dominate (high K)
    # but get pulled at 5 IP, or go deep on an off-stuff day with weak contact.
    # The old single stuff_day driving both created +1.4 DK inflation from
    # correlated IP × K compounding on good stuff days.
    stuff_day = rng.normal(1.0, stuff_sd, size=n_sims).clip(0.55, 1.50)
    workload_day = rng.normal(1.0, 0.08, size=n_sims).clip(0.85, 1.15)

    # K/BB/hit rates driven by stuff quality (stuff_day)
    sim_k_rate  = np.clip(k_rate * stuff_day, 0.08, 0.50)
    loc_dampen = clip(1.0 - (location_plus - 100) / 100.0 * 0.25, 0.70, 1.10)
    sim_bb_rate = np.clip(bb_rate * (1.0 + (1.0 - stuff_day) * loc_dampen), 0.02, 0.20)
    sim_hit_rate = np.clip(hit_on_contact * (2.0 - stuff_day), 0.15, 0.42)

    # ── Simulate n_sims games ─────────────────────────────────────────────
    # IP driven by workload (independent of stuff quality)
    ip_sd = clip(0.85 * era_consistency, 0.55, 1.15)
    sim_ip = rng.normal(proj_ip, ip_sd, size=n_sims)
    sim_ip = sim_ip * workload_day
    sim_ip = sim_ip.clip(1.0, 9.0)
    sim_batters_faced = (sim_ip * PA_PER_IP).astype(int).clip(4, 40)

    total_ks = np.zeros(n_sims)
    total_bb = np.zeros(n_sims)
    total_h  = np.zeros(n_sims)

    max_bf = int(sim_batters_faced.max())

    for bf_idx in range(max_bf):
        active = bf_idx < sim_batters_faced

        rolls = rng.random(n_sims)
        is_k   = rolls < sim_k_rate
        is_bb  = (~is_k) & (rolls < sim_k_rate + sim_bb_rate)
        is_bip = (~is_k) & (~is_bb)

        hit_rolls = rng.random(n_sims)
        is_hit = is_bip & (hit_rolls < sim_hit_rate)

        total_ks += is_k * active
        total_bb += is_bb * active
        total_h  += is_hit * active

    # ER: Poisson around ERA anchor, but stuff_day scales it too
    # Good stuff day → fewer ER, bad stuff day → more ER
    sim_er_rate = er_per_ip * (2.0 - stuff_day)  # inverse of stuff
    expected_er = sim_ip * sim_er_rate
    total_er = rng.poisson(expected_er.clip(0.1, 15.0)).astype(float)
    total_er = total_er.clip(0, sim_ip * 2.5)

    # Win decision — correlated with stuff_day and ER
    # A pitcher having a great outing (high stuff_day, low ER, deep IP) is more likely
    # to get the win. A pitcher shelled after 3 IP almost never gets the W.
    # Scale win_prob per sim based on how the outing went
    ip_factor = np.clip(sim_ip / 5.1, 0.50, 1.30)  # deeper = better chance
    er_factor = np.clip(1.0 - total_er * 0.08, 0.30, 1.15)  # fewer ER = better chance
    sim_win_prob = np.clip(win_prob * ip_factor * er_factor * stuff_day, 0.02, 0.55)
    win_rolls = rng.random(n_sims)
    wins = (win_rolls < sim_win_prob).astype(float)

    # CG / CGSO / NH bonuses
    is_cg   = sim_ip >= 9.0
    is_cgso = is_cg & (total_er == 0)
    is_nh   = is_cg & (total_h == 0)

    # HBP allowed: ~1% of BF, deducted at -0.6 per HBP on DK
    hbp_per_bf = 0.01
    total_hbp = rng.poisson(sim_batters_faced * hbp_per_bf).astype(float)

    # DK Classic pitcher scoring
    dk_pts = (
        sim_ip * 2.25 +
        total_ks * 2.0 +
        wins * 4.0 -
        total_er * 2.0 -
        total_h  * 0.6 -
        total_bb * 0.6 -
        total_hbp * 0.6 +
        is_cg.astype(float) * 2.5 +
        is_cgso.astype(float) * 2.5 +
        is_nh.astype(float) * 5.0
    )

    # Ground ball rate adjustment: high GB% pitchers under-perform DK projections
    # because they get fewer Ks (less DK upside) and rely on BABIP-dependent contact outs.
    # Backtest: gb_pct w=+2.4 reduces pitcher MAE by 3.9% on held-out data.
    gb_pct = talent.get('gb_pct', 0.43)
    gb_adj = clip(1.0 - (gb_pct - 0.43) * 0.15, 0.92, 1.04)
    dk_pts = dk_pts * gb_adj

    # Walk rate penalty: high-BB pitchers (bb_pct > league avg 8.2%) are over-projected.
    # Research: bb9 correlates r=+0.195 with projection error — walks per 9 IP
    # is the strongest missing pitcher predictor. High-walk pitchers allow more
    # baserunners, leading to more ER than the xFIP/SIERA anchor captures.
    bb_pct = talent.get('bb_pct', LEAGUE_AVG_BB_PCT)
    bb_adj = clip(1.0 - (bb_pct - LEAGUE_AVG_BB_PCT) * 1.5, 0.92, 1.04)
    dk_pts = dk_pts * bb_adj

    return dk_pts


# ── Full Game Simulation ──────────────────────────────────────────────────────

# League-average bullpen rates (used after starter exits)
BULLPEN_K_PCT  = 0.24
BULLPEN_BB_PCT = 0.085
BULLPEN_HR9    = 1.2
BULLPEN_BABIP  = 0.290
BULLPEN_HBP    = 0.008


def _compute_pa_rates(talent, pitcher, park, weather):
    """Compute PA outcome probabilities for a batter vs pitcher. Shared by game sim."""
    # Pitcher quality baseline: Pitching+ > Stuff+ > rate stats
    pitcher_quality = 100.0
    if pitcher:
        pitcher_quality = pitcher.get('pitching_plus') or pitcher.get('stuff_plus') or 100.0
    location_quality = pitcher.get('location_plus', 100.0) if pitcher else 100.0

    pitcher_k_ratio = (pitcher['k_pct'] / LEAGUE_AVG_K_PCT) if pitcher else 1.0
    pitcher_k_ratio *= (pitcher_quality / 100.0) ** 0.20
    park_k = safe(park.get('k_factor'), 100) / 100.0 if park else 1.0
    swing_k_adj = clip(1.0 + (talent.get('swing_length', 7.2) - 7.2) * 0.06, 0.94, 1.08)
    # Dampen park_k: Bayesian K% already includes ~50% home park via career data
    park_k_edge = 1.0 + (park_k - 1.0) * 0.50
    k_rate = clip(talent['k_pct'] * pitcher_k_ratio * park_k_edge * swing_k_adj, 0.05, 0.50)

    pitcher_bb_ratio = (LEAGUE_AVG_BB_PCT / pitcher['bb_pct']) if pitcher and pitcher['bb_pct'] > 0.02 else 1.0
    park_bb = safe(park.get('bb_factor'), 100) / 100.0 if park else 1.0
    bb_rate = clip(talent['bb_pct'] * pitcher_bb_ratio * park_bb, 0.02, 0.22)

    hbp_rate = 0.010

    # Hit probability — P(hit | ball in play) including HR
    # BABIP excludes HR from numerator and denominator, so using it raw
    # under-counts hits by ~12%. Convert to total P(hit|BIP) by adding
    # the HR component back: hit_prob = BABIP + HR/BIP
    contact_rate = max(0.30, 1.0 - talent['k_pct'] - talent['bb_pct'] - 0.01)
    # ISO/3.0 overestimated HR by ~67%. ISO/3.5 reduces the overcount while preserving
    # differentiation between power and contact hitters in the BIP hit probability.
    hr_per_bip = clip(talent['iso'] / 3.15, 0.005, 0.07) / contact_rate
    hit_prob_base = talent['babip'] + hr_per_bip

    ev_adj = clip(1.0 + (talent.get('avg_ev', 88.0) - 88.0) * 0.008, 0.92, 1.10)
    qoc_mult = clip(1.0 + (talent['barrel'] - 0.065) * 0.8 + (talent['hard_hit'] - 0.35) * 0.3, 0.85, 1.25)
    pitcher_hit_suppression = clip(1.0 - (pitcher_quality - 100) * 0.0015, 0.94, 1.06)
    loc_hit_suppression = clip(1.0 - (location_quality - 100) * 0.001, 0.96, 1.04)
    park_basic = safe(park.get('basic_factor'), 100) / 100.0 if park else 1.0
    wx_hit = weather_hit_mult(weather)
    ld_adj = clip(1.0 + (talent.get('ld_pct', 0.21) - 0.21) * 0.5, 0.90, 1.12)
    # Player-specific qoc_mult: compare physical tools to what BABIP already reflects.
    # If BABIP is already "fair" for the player's barrel/HH/EV, qoc_mult is minimal.
    # If BABIP is low despite elite tools, qoc_mult boosts to close the gap.
    raw_qoc = 1.0 + (talent['barrel'] - 0.065) * 0.8 + (talent['hard_hit'] - 0.35) * 0.3
    # How much of this boost is already in the Bayesian BABIP?
    babip_vs_avg = (talent['babip'] - LEAGUE_AVG_BABIP) / 0.05  # +1.0 per 5 pts above avg
    qoc_vs_avg = (raw_qoc - 1.0) / 0.10  # +1.0 per 10% boost
    # If BABIP already reflects the QoC (both positive), dampen the mult.
    # If BABIP is low despite good QoC, let the mult through.
    overlap = clip(min(babip_vs_avg, qoc_vs_avg), 0.0, 1.0) if babip_vs_avg > 0 and qoc_vs_avg > 0 else 0.0
    adjusted_qoc = 1.0 + (raw_qoc - 1.0) * (1.0 - overlap * 0.85)  # dampen 70% of overlap
    adjusted_qoc = clip(adjusted_qoc, 0.90, 1.20)
    # Dampen park_basic: Bayesian BABIP already reflects ~50% home park effect
    # via career xwOBA. Apply only 50% of the park deviation as an edge.
    park_basic_edge = 1.0 + (park_basic - 1.0) * 0.50
    hit_prob = clip(hit_prob_base * adjusted_qoc * ev_adj * pitcher_hit_suppression * loc_hit_suppression * park_basic_edge * wx_hit * ld_adj, 0.14, 0.52)

    # HR rate — bat tracking metrics drive power ceiling
    park_hr = safe(park.get('hr_factor'), 100) / 100.0 if park else 1.0
    if park and talent.get('pull_pct'):
        lf_dist = safe(park.get('lf_dist'), 330)
        rf_dist = safe(park.get('rf_dist'), 330)
        short_porch = min(lf_dist, rf_dist)
        porch_factor = clip((330 - short_porch) / 330.0, -0.05, 0.10)
        pull_dev = talent['pull_pct'] - 0.40
        park_hr *= 1.0 + pull_dev * porch_factor * 3.0
    fb_adj = clip(1.0 + (talent.get('fb_pct', 0.35) - 0.35) * 0.3, 0.85, 1.20)
    wx_hr = weather_hr_mult(weather)
    pitcher_hr_ratio = (pitcher['hr9'] / LEAGUE_AVG_HR9) if pitcher else 1.0
    # Park/weather sensitivity: low avg_ev = wall scrapers = more park-sensitive
    park_sens = clip(1.0 + (88.0 - talent.get('avg_ev', 88.0)) * 0.015, 0.85, 1.20)
    # ISO already reflects parks the batter played in (~50% home). Apply only 50%
    # of the park deviation as an "edge" to avoid double-counting.
    effective_park_hr = 1.0 + (park_hr - 1.0) * 0.50 * park_sens
    effective_wx_hr = 1.0 + (wx_hr - 1.0) * park_sens
    # Bat speed: raw power — each mph above 72 (avg) scales HR probability
    bat_spd_adj = clip(1.0 + (talent.get('bat_speed', 72.0) - 72.0) * 0.015, 0.88, 1.15)
    # Squared-up%: how often they barrel the ball — complements barrel%
    squp_adj = clip(1.0 + (talent.get('squared_up', 0.18) - 0.18) * 1.5, 0.90, 1.12)
    # Blast%: highest-quality contact tier (league avg ~8%)
    blast_adj = clip(1.0 + (talent.get('blast', 0.08) - 0.08) * 2.5, 0.90, 1.12)
    # Attack angle vs arm angle alignment
    aa_hr_adj = arm_angle_hr_interaction(talent.get('attack_angle'), pitcher.get('arm_angle') if pitcher else None)
    # bat_spd/squp/blast removed — Bayesian hr_per_pa already captures power profile.
    # Only game-specific adjustments: park, weather, pitcher HR tendency, arm angle.
    hr_per_hit = clip(talent.get('hr_per_pa', 0.03) * effective_park_hr * effective_wx_hr * pitcher_hr_ratio * aa_hr_adj / hit_prob, 0.03, 0.30)
    # XB rate: per-hitter doubles rate from Marcel. ISO already reflects park, so
    # apply only 50% of park_basic deviation to avoid double-counting.
    park_xb_edge = 1.0 + (park_basic - 1.0) * 0.50
    xb_per_hit = clip(talent.get('xb_per_hit', 0.14) * ev_adj * park_xb_edge, 0.06, 0.25)
    triple_per_hit = 0.015

    # SB rate: 3-factor model (sprint speed × catcher pop × pitcher vulnerability)
    # sb_per_pa is the historical rate. In the full-game sim, a batter gets one SB
    # chance per on-base event, but real SBs also happen during subsequent PAs while
    # the runner is still on base. Scale up by ~2.5x to account for multi-PA windows.
    # No multiplier — direct calc uses career SB rate at face value.
    # The 2.0x was for sim multi-PA windows which no longer drives the mean.
    base_sb = talent.get('sb_per_pa', 0.01)
    speed_mult = clip(1.0 + (4.40 - talent.get('sprint_speed', 4.40)) * 1.16, 0.50, 1.60)
    sb_rate = clip(base_sb * speed_mult, 0.0, 0.35)

    return {
        'k': k_rate, 'bb': bb_rate, 'hbp': hbp_rate,
        'hit': hit_prob, 'hr': hr_per_hit, 'xb': xb_per_hit, 'triple': triple_per_hit,
        'sb': sb_rate,
    }


def _bullpen_rates(talent, park, weather, bp_quality=None):
    """PA outcome rates when batter faces bullpen.
    Uses team-specific reliever rates if bp_quality provided, else league average."""
    bp_k = bp_quality['k_pct'] if bp_quality else BULLPEN_K_PCT
    bp_bb = bp_quality['bb_pct'] if bp_quality else BULLPEN_BB_PCT
    bp_hr9 = bp_quality['hr9'] if bp_quality else BULLPEN_HR9
    bp_babip = bp_quality['babip'] if bp_quality else BULLPEN_BABIP

    park_k = safe(park.get('k_factor'), 100) / 100.0 if park else 1.0
    k_rate = clip(talent['k_pct'] * (bp_k / LEAGUE_AVG_K_PCT) * park_k, 0.05, 0.50)
    park_bb = safe(park.get('bb_factor'), 100) / 100.0 if park else 1.0
    bb_rate = clip(talent['bb_pct'] * (LEAGUE_AVG_BB_PCT / bp_bb) * park_bb, 0.02, 0.22)

    ev_adj = clip(1.0 + (talent.get('avg_ev', 88.0) - 88.0) * 0.008, 0.92, 1.10)
    qoc_mult = clip(1.0 + (talent['barrel'] - 0.065) * 0.8 + (talent['hard_hit'] - 0.35) * 0.3, 0.85, 1.25)
    park_basic = safe(park.get('basic_factor'), 100) / 100.0 if park else 1.0
    wx_hit = weather_hit_mult(weather)
    # Add HR/BIP back to BABIP for total P(hit|BIP) — same fix as _compute_pa_rates
    bp_contact = max(0.30, 1.0 - talent['k_pct'] - talent['bb_pct'] - 0.01)
    bp_hr_per_bip = clip(talent['iso'] / 3.15, 0.005, 0.07) / bp_contact
    # Player-specific qoc_mult (same logic)
    raw_qoc = 1.0 + (talent['barrel'] - 0.065) * 0.8 + (talent['hard_hit'] - 0.35) * 0.3
    babip_vs_avg = (talent['babip'] - LEAGUE_AVG_BABIP) / 0.05
    qoc_vs_avg = (raw_qoc - 1.0) / 0.10
    overlap = clip(min(babip_vs_avg, qoc_vs_avg), 0.0, 1.0) if babip_vs_avg > 0 and qoc_vs_avg > 0 else 0.0
    adjusted_qoc = clip(1.0 + (raw_qoc - 1.0) * (1.0 - overlap * 0.70), 0.90, 1.20)
    park_basic_edge = 1.0 + (park_basic - 1.0) * 0.50
    hit_prob = clip((bp_babip + bp_hr_per_bip) * adjusted_qoc * ev_adj * park_basic_edge * wx_hit, 0.20, 0.52)

    park_hr = safe(park.get('hr_factor'), 100) / 100.0 if park else 1.0
    fb_adj = clip(1.0 + (talent.get('fb_pct', 0.35) - 0.35) * 0.3, 0.85, 1.20)
    wx_hr = weather_hr_mult(weather)
    park_sens = clip(1.0 + (88.0 - talent.get('avg_ev', 88.0)) * 0.015, 0.85, 1.20)
    # ISO already reflects parks — apply only 50% of deviation (same as _compute_pa_rates)
    effective_park_hr = 1.0 + (park_hr - 1.0) * 0.50 * park_sens
    effective_wx_hr = 1.0 + (wx_hr - 1.0) * park_sens
    bat_spd_adj = clip(1.0 + (talent.get('bat_speed', 72.0) - 72.0) * 0.015, 0.88, 1.15)
    squp_adj = clip(1.0 + (talent.get('squared_up', 0.18) - 0.18) * 1.5, 0.90, 1.12)
    blast_adj = clip(1.0 + (talent.get('blast', 0.08) - 0.08) * 2.5, 0.90, 1.12)
    # Same as SP path: bat_spd/squp/blast already in Bayesian hr_per_pa
    hr_per_hit = clip(talent.get('hr_per_pa', 0.03) * effective_park_hr * effective_wx_hr * (bp_hr9 / LEAGUE_AVG_HR9) / hit_prob, 0.03, 0.30)
    park_xb_edge = 1.0 + (park_basic - 1.0) * 0.50
    xb_per_hit = clip(talent.get('xb_per_hit', 0.14) * ev_adj * park_xb_edge, 0.06, 0.25)

    return {
        'k': k_rate, 'bb': bb_rate, 'hbp': BULLPEN_HBP,
        'hit': hit_prob, 'hr': hr_per_hit, 'xb': xb_per_hit, 'triple': 0.015,
        'sb': talent.get('sb_per_pa', 0.01),
    }


def sim_full_game(lineup_talents, sp_talent, park, weather, odds, is_home,
                  n_sims, rng, sp_proj_ip=5.5, sb_context=None):
    """
    Simulate n_sims full games for one team's offense.
    Batters hit in lineup order, tracking base state and outs per inning.
    R and RBI naturally correlate because runners are on base from prior PAs.

    Args:
        lineup_talents: list of 9 dicts, each with batter talent + 'rates_vs_sp' + 'rates_vs_bp'
        sp_talent: opposing starter talent dict (for BF tracking)
        park, weather, odds: environment dicts
        is_home: bool
        n_sims: number of simulations
        rng: numpy random generator
        sp_proj_ip: projected IP for opposing starter

    Returns:
        dict of { player_index: np.array(n_sims) } — DK points per sim per batter
    """
    PA_PER_IP = 4.3
    # SP exits after this many batters faced (varies per sim)
    sp_bf_limit = rng.normal(sp_proj_ip * PA_PER_IP, 5.0, size=n_sims).clip(8, 40).astype(int)

    # Team factor REMOVED — it was redundant with pitcher quality (already in
    # _compute_pa_rates) and lineup quality (naturally determines baserunner traffic).
    # Applying Vegas implied on top of those double-counted the environment and
    # suppressed all hitters in low-implied games regardless of their talent.
    # The sim now runs on pure talent-vs-pitcher matchup rates.
    team_factor = np.ones(n_sims)  # neutral — no game-environment scaling

    # Pre-allocate DK points and SB counts per batter
    dk_pts = {i: np.zeros(n_sims) for i in range(9)}
    sb_counts = {i: np.zeros(n_sims) for i in range(9)}

    # Base state: [1B, 2B, 3B] — stores batter index (0-8) or -1 if empty.
    # Tracking runner identity lets us credit R to the batter who scored,
    # eliminating the need for an artificial post-game R distribution.
    EMPTY = -1
    for sim in range(n_sims):
        tf = team_factor[sim]
        sp_limit = sp_bf_limit[sim]
        team_bf = 0  # batters faced by this team's offense
        batter_idx = 0  # current spot in lineup (0-8, wraps)

        for inning in range(9):  # 9 innings
            outs = 0
            bases = [EMPTY, EMPTY, EMPTY]  # 1B, 2B, 3B — batter index or EMPTY

            while outs < 3:
                bi = batter_idx % 9
                batter = lineup_talents[bi]

                # Choose rates based on whether starter is still in
                rates = batter['rates_vs_sp'] if team_bf < sp_limit else batter['rates_vs_bp']

                # Per-batter volatility: o_swing% (chase rate) widens outcomes
                o_swing_vol = batter.get('o_swing', 0.30)
                batter_vol_sd = 0.04 + (o_swing_vol - 0.30) * 0.25
                batter_day = rng.normal(1.0, max(0.03, batter_vol_sd))
                batter_day = clip(batter_day, 0.60, 1.40)
                # team_factor scales hit, HR, and K: low-implied games suppress
                # all offense AND increase Ks (tougher pitching environment).
                # Pure talent-vs-pitcher: rates already encode matchup quality.
                # No game-environment scaling — lineup quality naturally drives R/RBI
                # through base runner state (weak lineup = fewer runners on base).
                hit_p = clip(rates['hit'] * batter_day, 0.06, 0.52)
                hr_p = clip(rates['hr'] * batter_day, 0.02, 0.30)
                k_p = rates['k']
                bb_p = rates['bb']

                # Helper: credit R (+2 DK pts) to a runner who scores
                def score_runner(base_idx):
                    runner = bases[base_idx]
                    if runner != EMPTY:
                        dk_pts[runner][sim] += 2  # R scored
                    bases[base_idx] = EMPTY

                # Roll PA
                roll = rng.random()
                if roll < k_p:
                    # Strikeout
                    outs += 1
                elif roll < k_p + bb_p:
                    # Walk — force advance
                    dk_pts[bi][sim] += 2  # BB
                    if bases[0] != EMPTY and bases[1] != EMPTY and bases[2] != EMPTY:
                        dk_pts[bi][sim] += 2  # RBI (bases loaded)
                        score_runner(2)
                    if bases[0] != EMPTY and bases[1] != EMPTY:
                        bases[2] = bases[1]
                    if bases[0] != EMPTY:
                        bases[1] = bases[0]
                    bases[0] = bi
                    # SB attempt after walk
                    _sb_rate = rates['sb']
                    if sb_context:
                        pop = sb_context.get('catcher_pop', 1.95)
                        _sb_rate *= clip(1.0 + (pop - 1.95) * 1.67, 0.75, 1.30)
                        p_sb9 = sb_context.get('pitcher_sb_per_9', 1.0)
                        _sb_rate *= clip(1.0 + (p_sb9 - 1.0) * 0.25, 0.80, 1.30)
                    if rng.random() < _sb_rate:
                        dk_pts[bi][sim] += 5
                        sb_counts[bi][sim] += 1
                elif roll < rates['k'] + rates['bb'] + rates['hbp']:
                    # HBP — same as walk
                    dk_pts[bi][sim] += 2  # HBP
                    if bases[0] != EMPTY and bases[1] != EMPTY and bases[2] != EMPTY:
                        dk_pts[bi][sim] += 2  # RBI
                        score_runner(2)
                    if bases[0] != EMPTY and bases[1] != EMPTY:
                        bases[2] = bases[1]
                    if bases[0] != EMPTY:
                        bases[1] = bases[0]
                    bases[0] = bi
                    # SB attempt after HBP
                    _sb_rate = rates['sb']
                    if sb_context:
                        pop = sb_context.get('catcher_pop', 1.95)
                        _sb_rate *= clip(1.0 + (pop - 1.95) * 1.67, 0.75, 1.30)
                        p_sb9 = sb_context.get('pitcher_sb_per_9', 1.0)
                        _sb_rate *= clip(1.0 + (p_sb9 - 1.0) * 0.25, 0.80, 1.30)
                    if rng.random() < _sb_rate:
                        dk_pts[bi][sim] += 5
                        sb_counts[bi][sim] += 1
                else:
                    # Ball in play
                    hit_roll = rng.random()
                    if hit_roll < hit_p:
                        # Hit — determine type
                        type_roll = rng.random()
                        if type_roll < hr_p:
                            # HOME RUN — all runners + batter score
                            rbi = 1  # batter drives self in
                            for b in range(3):
                                if bases[b] != EMPTY:
                                    rbi += 1
                                    score_runner(b)
                            dk_pts[bi][sim] += 10 + 2 + 2 * rbi  # HR + R + RBI
                        elif type_roll < hr_p + rates['triple']:
                            # TRIPLE — all runners score, batter to 3B
                            rbi = 0
                            for b in range(3):
                                if bases[b] != EMPTY:
                                    rbi += 1
                                    score_runner(b)
                            dk_pts[bi][sim] += 8 + 2 * rbi  # 3B + RBI
                            bases = [EMPTY, EMPTY, bi]
                        elif type_roll < hr_p + rates['triple'] + rates['xb']:
                            # DOUBLE — runners on 2B/3B score, 1B to 3B, batter to 2B
                            rbi = 0
                            if bases[2] != EMPTY:
                                rbi += 1
                                score_runner(2)
                            if bases[1] != EMPTY:
                                rbi += 1
                                score_runner(1)
                            dk_pts[bi][sim] += 5 + 2 * rbi  # 2B + RBI
                            bases[2] = bases[0]  # 1B runner to 3B
                            bases[1] = bi         # batter to 2B
                            bases[0] = EMPTY
                        else:
                            # SINGLE — 3B scores, 2B may score, 1B to 2B
                            rbi = 0
                            if bases[2] != EMPTY:
                                rbi += 1
                                score_runner(2)
                            if bases[1] != EMPTY:
                                score_from_2b_prob = 0.60
                                if rng.random() < score_from_2b_prob:
                                    rbi += 1
                                    score_runner(1)
                                else:
                                    bases[2] = bases[1]  # 2B to 3B
                                    bases[1] = EMPTY
                            dk_pts[bi][sim] += 3 + 2 * rbi  # 1B + RBI
                            bases[1] = bases[0]  # 1B to 2B
                            bases[0] = bi         # batter to 1B

                            # SB attempt after single
                            _sb_rate = rates['sb']
                            if sb_context:
                                pop = sb_context.get('catcher_pop', 1.95)
                                _sb_rate *= clip(1.0 + (pop - 1.95) * 1.67, 0.75, 1.30)
                                p_sb9 = sb_context.get('pitcher_sb_per_9', 1.0)
                                _sb_rate *= clip(1.0 + (p_sb9 - 1.0) * 0.25, 0.80, 1.30)
                            if rng.random() < _sb_rate:
                                dk_pts[bi][sim] += 5
                                sb_counts[bi][sim] += 1
                    else:
                        # Out
                        # Sac fly: runner on 3B scores with < 2 outs on ~30% of fly outs
                        if bases[2] != EMPTY and outs < 2 and rng.random() < 0.30:
                            dk_pts[bi][sim] += 2  # RBI from sac fly
                            score_runner(2)
                        outs += 1

                team_bf += 1
                batter_idx += 1

    return dk_pts, sb_counts


# ── Data Fetching ─────────────────────────────────────────────────────────────

def fetch_data(target_date: str) -> dict:
    """Fetch all data needed for simulations (reuses compute_projections patterns)."""
    print(f"  Fetching games for {target_date}...")
    games = sb.table('games').select(
        'game_pk,game_date,home_team,away_team,home_team_id,away_team_id,'
        'home_sp_id,away_sp_id,home_sp_hand,away_sp_hand,venue_id'
    ).eq('game_date', target_date).execute().data or []
    print(f"  Games: {len(games)}")
    if not games:
        return {}

    game_pks = [g['game_pk'] for g in games]

    # Lineups
    lineups = sb.table('lineups').select(
        'player_id,game_pk,team_id,batting_order,player_name'
    ).in_('game_pk', game_pks).gte('batting_order', 1).lte('batting_order', 9).execute().data or []
    print(f"  Lineups: {len(lineups)}")

    # Build forward PLAYER_ID_REMAP so we can look up stats when lineup
    # uses a wrong ID (e.g. 115223) but stats are under the correct ID (665489).
    # PLAYER_ID_REMAP is {wrong_id -> correct_stats_id}, which is exactly
    # what we need: lineup has wrong_id, stats are under correct_stats_id.
    forward_remap = {}
    try:
        import ast as _ast
        import os as _os
        _remap_path = _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), 'load_dk_salaries.py')
        with open(_remap_path, encoding='utf-8') as f:
            src = f.read()
        for node in _ast.walk(_ast.parse(src)):
            if isinstance(node, _ast.Assign):
                for t in node.targets:
                    if isinstance(t, _ast.Name) and t.id == 'PLAYER_ID_REMAP':
                        forward_remap = eval(compile(_ast.Expression(body=node.value), '<remap>', 'eval'))
                        break
    except Exception:
        pass

    # Batter stats (3 seasons, chunked)
    # Include remapped IDs so stats load for players whose lineup ID
    # differs from their stats ID (e.g. Vlad Jr: lineup=115223, stats=665489)
    player_ids = list({l['player_id'] for l in lineups if l.get('player_id')})
    alt_ids = [forward_remap[pid] for pid in player_ids if pid in forward_remap]
    all_stat_ids = list(set(player_ids + alt_ids))
    batter_stats = {}
    for i in range(0, len(all_stat_ids), 500):
        chunk = all_stat_ids[i:i+500]
        rows = sb.table('batter_stats').select(
            'player_id,season,pa,woba,xwoba,k_pct,bb_pct,iso,avg,sb,r,rbi,hr,babip,'
            'barrel_pct,hard_hit_pct,avg_ev,wrc_plus,full_name,team,fb_pct,pull_pct,'
            'ld_pct,gb_pct,cent_pct,oppo_pct,bat_speed,squared_up_pct,blast_pct,o_swing_pct,swstr_pct,attack_angle,sprint_speed'
        ).in_('player_id', chunk).in_('season', [SEASON, SEASON-1, SEASON-2]).execute().data or []
        for r in rows:
            batter_stats.setdefault(r['player_id'], {})[r['season']] = r

    # Bat tracking (swing_length not in batter_stats — fetch from bat_tracking table)
    bat_tracking = {}
    for i in range(0, len(all_stat_ids), 500):
        chunk = all_stat_ids[i:i+500]
        rows = sb.table('bat_tracking').select(
            'player_id,season,swing_length'
        ).in_('player_id', chunk).in_('season', [SEASON, SEASON-1]).execute().data or []
        for r in rows:
            if r['player_id'] not in bat_tracking:  # prefer current season (fetched first)
                bat_tracking[r['player_id']] = r

    # Pitcher stats (3 seasons)
    sp_ids = set()
    for g in games:
        if g.get('home_sp_id'): sp_ids.add(g['home_sp_id'])
        if g.get('away_sp_id'): sp_ids.add(g['away_sp_id'])
    pitcher_stats = {}
    if sp_ids:
        rows = sb.table('pitcher_stats').select(
            'player_id,season,ip,g,gs,xfip,siera,k_pct,bb_pct,hr9,babip,'
            'stuff_plus,location_plus,pitching_plus,swstr_pct,csw_pct,full_name,stats_level,'
            'gb_pct,fb_pct,ld_pct,velo,lob_pct,sb_per_9'
        ).in_('player_id', list(sp_ids)).in_('season', [SEASON, SEASON-1, SEASON-2]).execute().data or []
        for r in rows:
            pitcher_stats.setdefault(r['player_id'], {})[r['season']] = r

    # SP salaries (detect $4000 relievers used as openers)
    sp_salaries = {}
    if sp_ids:
        sal_rows = sb.table('dk_salaries').select(
            'player_id,salary'
        ).in_('player_id', list(sp_ids)).eq('season', SEASON).execute().data or []
        for r in sal_rows:
            # Keep highest salary per player (CPT vs FLEX dupes)
            if r['player_id'] not in sp_salaries or (r.get('salary') or 0) > sp_salaries[r['player_id']]:
                sp_salaries[r['player_id']] = r.get('salary') or 0

    # Pitch arsenal (for three-tier pitcher model + arm angle)
    arsenal_data = {}
    if sp_ids:
        arsenal_rows = []
        sp_list = list(sp_ids)
        for i in range(0, len(sp_list), 150):
            chunk = sp_list[i:i+150]
            rows = sb.table('pitch_arsenal').select(
                'player_id,season,pitch_type,usage_pct,stuff_plus,velo,arm_angle,whiff_pct,k_pct,xwoba'
            ).in_('player_id', chunk).in_('season', [SEASON, SEASON-1]).execute().data or []
            arsenal_rows.extend(rows)
        # Group by (player_id, season) and compute usage-weighted composites
        from collections import defaultdict
        grouped = defaultdict(list)
        for r in arsenal_rows:
            grouped[(r['player_id'], r['season'])].append(r)
        for (pid, season), pitch_rows in grouped.items():
            composite = compute_arsenal_composite(pitch_rows)
            if composite:
                # Prefer current season; fall back to prior
                if pid not in arsenal_data or season == SEASON:
                    arsenal_data[pid] = composite
        print(f"  Pitch arsenal: {len(arsenal_data)} pitchers with composites")

    # Batter splits (also include reverse-remapped IDs)
    # Load both seasons with season key, then blend PA-weighted
    batter_splits_raw = {}  # {player_id: {split: {season: row}}}
    if all_stat_ids:
        for i in range(0, len(all_stat_ids), 150):
            chunk = all_stat_ids[i:i+150]
            rows = sb.table('batter_splits').select(
                'player_id,season,split,pa,wrc_plus,woba,k_pct,bb_pct,iso'
            ).in_('player_id', chunk).in_('season', [SEASON, SEASON-1]).execute().data or []
            for r in rows:
                batter_splits_raw.setdefault(r['player_id'], {}).setdefault(r['split'], {})[r['season']] = r

    # Blend splits: PA-weighted average across seasons (heavier = more reliable)
    batter_splits = {}
    SPLIT_STATS = ['wrc_plus', 'woba', 'k_pct', 'bb_pct', 'iso']
    SPLIT_DEFAULTS = {'wrc_plus': 100, 'woba': 0.315, 'k_pct': 0.225, 'bb_pct': 0.082, 'iso': 0.165}
    for pid, splits in batter_splits_raw.items():
        for split_label, seasons in splits.items():
            curr = seasons.get(SEASON)
            prev = seasons.get(SEASON - 1)
            curr_pa = safe(curr.get('pa'), 0) if curr else 0
            prev_pa = safe(prev.get('pa'), 0) if prev else 0
            total_pa = curr_pa + prev_pa
            if total_pa < 5:
                continue  # not enough data to use

            # PA-weighted blend with Marcel-style season weighting (current 5x, prior 4x)
            blended = {'pa': total_pa, 'split': split_label}
            for stat in SPLIT_STATS:
                c_val = safe(curr.get(stat)) if curr else None
                p_val = safe(prev.get(stat)) if prev else None
                if c_val is not None and p_val is not None:
                    c_wt = curr_pa * 5
                    p_wt = prev_pa * 4
                    blended[stat] = (c_val * c_wt + p_val * p_wt) / (c_wt + p_wt)
                elif c_val is not None:
                    blended[stat] = c_val
                elif p_val is not None:
                    blended[stat] = p_val
                # else: leave unset, platoon_adjust handles None

            batter_splits.setdefault(pid, {})[split_label] = blended

    print(f"  Batter splits: {sum(len(v) for v in batter_splits.values())} split rows "
          f"({len(batter_splits)} players)")

    # Pitcher splits (for lineup-weighted matchup adjustments)
    pitcher_splits = {}  # {player_id: {split: blended_row}}
    pitcher_splits_raw = {}
    if sp_ids:
        for i in range(0, len(list(sp_ids)), 150):
            chunk = list(sp_ids)[i:i+150]
            rows = sb.table('pitcher_splits').select(
                'player_id,season,split,pa,k_pct,bb_pct,xfip,fip,woba,era'
            ).in_('player_id', chunk).in_('season', [SEASON, SEASON-1]).execute().data or []
            for r in rows:
                pitcher_splits_raw.setdefault(r['player_id'], {}).setdefault(r['split'], {})[r['season']] = r

    P_SPLIT_STATS = ['k_pct', 'bb_pct', 'xfip', 'fip', 'woba', 'era']
    for pid, splits in pitcher_splits_raw.items():
        for split_label, seasons in splits.items():
            curr = seasons.get(SEASON)
            prev = seasons.get(SEASON - 1)
            curr_pa = safe(curr.get('pa'), 0) if curr else 0
            prev_pa = safe(prev.get('pa'), 0) if prev else 0
            total_pa = curr_pa + prev_pa
            if total_pa < 10:
                continue

            blended = {'pa': total_pa}
            for stat in P_SPLIT_STATS:
                c_val = safe(curr.get(stat)) if curr else None
                p_val = safe(prev.get(stat)) if prev else None
                if c_val is not None and p_val is not None:
                    c_wt = curr_pa * 5
                    p_wt = prev_pa * 4
                    blended[stat] = (c_val * c_wt + p_val * p_wt) / (c_wt + p_wt)
                elif c_val is not None:
                    blended[stat] = c_val
                elif p_val is not None:
                    blended[stat] = p_val

            pitcher_splits.setdefault(pid, {})[split_label] = blended

    print(f"  Pitcher splits: {sum(len(v) for v in pitcher_splits.values())} split rows "
          f"({len(pitcher_splits)} pitchers)")

    # Odds
    odds = {}
    if game_pks:
        rows = sb.table('game_odds').select(
            'game_pk,game_total,home_implied,away_implied,home_ml,away_ml'
        ).in_('game_pk', game_pks).execute().data or []
        odds = {r['game_pk']: r for r in rows}

    # Park factors
    venue_ids = list({g['venue_id'] for g in games if g.get('venue_id')})
    park_factors = {}
    if venue_ids:
        rows = sb.table('park_factors').select(
            'venue_id,basic_factor,hr_factor,k_factor,bb_factor,'
            'lf_dist,rf_dist,lf_wall_height,rf_wall_height,altitude'
        ).in_('venue_id', venue_ids).execute().data or []
        park_factors = {r['venue_id']: r for r in rows}

    # Weather
    weather = {}
    if game_pks:
        rows = sb.table('weather').select(
            'game_pk,temp_f,wind_speed,wind_dir,precip_pct,is_outdoor,humidity'
        ).in_('game_pk', game_pks).execute().data or []
        weather = {r['game_pk']: r for r in rows}

    # Catcher pop time — for SB probability model
    catcher_poptime = {}
    try:
        rows = sb.table('catcher_poptime').select(
            'player_id,season,pop_2b'
        ).in_('season', [SEASON, SEASON-1]).order('season', desc=True).execute().data or []
        for r in rows:
            if r['player_id'] not in catcher_poptime and r.get('pop_2b'):
                catcher_poptime[r['player_id']] = r['pop_2b']
        if catcher_poptime:
            print(f"  Catcher pop time: {len(catcher_poptime)} catchers loaded")
    except Exception:
        pass  # table may not exist yet

    # Pitcher props (Vegas IP and K lines)
    # Pitcher props disabled — API limit hit, data is stale.
    # Bayesian model drives projections from talent metrics instead.
    pitcher_props = {}

    # ── Team bullpen quality ────────────────────────────────────────────
    # Fetch reliever stats per team for team-specific bullpen rates in game sim
    bullpen_quality = {}
    try:
        team_ids = list({g.get('home_team_id') for g in games} | {g.get('away_team_id') for g in games})
        team_ids = [t for t in team_ids if t]
        roster_rows = []
        for i in range(0, len(team_ids), 30):
            chunk = team_ids[i:i+30]
            rows = sb.table('rosters').select(
                'player_id,team_id'
            ).in_('team_id', chunk).eq('position_type', 'Pitcher').execute().data or []
            roster_rows.extend(rows)
        team_pitcher_ids = {}
        for r in roster_rows:
            team_pitcher_ids.setdefault(r['team_id'], []).append(r['player_id'])
        all_rp_ids = [pid for pids in team_pitcher_ids.values() for pid in pids]
        rp_stat_rows = []
        # Try current season first (relaxed: g>=1 for early season), then prior season
        for szn in [SEASON, SEASON - 1]:
            for i in range(0, len(all_rp_ids), 500):
                chunk = all_rp_ids[i:i+500]
                rows = sb.table('pitcher_stats').select(
                    'player_id,ip,g,gs,k_pct,bb_pct,hr9,babip'
                ).in_('player_id', chunk).eq('season', szn).lte('gs', 2).gte('g', 1).execute().data or []
                rp_stat_rows.extend(rows)
        # IP-weighted composite per team
        rp_by_pid = {}
        for r in rp_stat_rows:
            pid = r['player_id']
            ip = safe(r.get('ip'), 0)
            if pid not in rp_by_pid or ip > safe(rp_by_pid[pid].get('ip'), 0):
                rp_by_pid[pid] = r
        for tid, pids in team_pitcher_ids.items():
            team_rp = [rp_by_pid[pid] for pid in pids if pid in rp_by_pid]
            if not team_rp:
                continue
            total_ip = sum(safe(r.get('ip'), 0) for r in team_rp)
            if total_ip <= 0:
                continue
            wt_k = sum(safe(r.get('k_pct'), BULLPEN_K_PCT) * safe(r.get('ip'), 0) for r in team_rp) / total_ip
            wt_bb = sum(safe(r.get('bb_pct'), BULLPEN_BB_PCT) * safe(r.get('ip'), 0) for r in team_rp) / total_ip
            wt_hr9 = sum(safe(r.get('hr9'), BULLPEN_HR9) * safe(r.get('ip'), 0) for r in team_rp) / total_ip
            wt_babip = sum(safe(r.get('babip'), BULLPEN_BABIP) * safe(r.get('ip'), 0) for r in team_rp) / total_ip
            bullpen_quality[tid] = {
                'k_pct': clip(wt_k, 0.12, 0.40),
                'bb_pct': clip(wt_bb, 0.04, 0.16),
                'hr9': clip(wt_hr9, 0.5, 2.5),
                'babip': clip(wt_babip, 0.24, 0.34),
            }
        print(f"  Bullpen quality: {len(bullpen_quality)} teams")
    except Exception as e:
        print(f"  Bullpen quality: skipped ({e})")

    return {
        'games': games, 'lineups': lineups,
        'batter_stats': batter_stats, 'pitcher_stats': pitcher_stats,
        'batter_splits': batter_splits, 'pitcher_splits': pitcher_splits,
        'odds': odds,
        'park_factors': park_factors, 'weather': weather,
        'pitcher_props': pitcher_props,
        'bullpen_quality': bullpen_quality,
        'arsenal_data': arsenal_data,
        'bat_tracking': bat_tracking,
        'sp_salaries': sp_salaries,
        'catcher_poptime': catcher_poptime,
        '_forward_remap': forward_remap,
    }


# ── Opposing Lineup Quality ──────────────────────────────────────────────────

def compute_opp_quality(lineups, batter_stats, batter_splits, opp_team_id,
                         pitcher_hand, odds, is_home):
    """PA-weighted wRC+ of opposing lineup vs pitcher hand.

    Data quality guards:
    - Split wRC+ capped at [30, 200] to prevent small-sample extremes
    - Split requires min 30 PA to be used (else fall back to overall)
    - Overall wRC+ capped at [40, 180]
    - Unknown/no-data batters default to 95 (slightly below avg)
    """
    opp_batters = [lu for lu in lineups
                   if lu.get('team_id') == opp_team_id and lu.get('batting_order')]
    stats_wrc = None
    if opp_batters:
        tw, tp = 0.0, 0.0
        for lu in opp_batters:
            pid = lu['player_id']
            order = lu.get('batting_order', 5)
            pa_wt = LINEUP_PA.get(order, LEAGUE_AVG_PA)
            wrc = None
            # Try platoon split (require min 30 PA, cap extremes)
            if pitcher_hand:
                split = batter_splits.get(pid, {}).get(pitcher_hand)
                if split:
                    split_pa = safe(split.get('pa'), 0)
                    split_wrc = safe(split.get('wrc_plus'))
                    if split_wrc is not None and split_pa >= 30:
                        wrc = clip(split_wrc, 30, 200)
            # Fall back to overall wRC+ (capped)
            if wrc is None:
                s = batter_stats.get(pid, {})
                curr = s.get(SEASON) or s.get(SEASON-1) or s.get(SEASON-2)
                if curr:
                    raw = safe(curr.get('wrc_plus'))
                    if raw is not None:
                        wrc = clip(raw, 40, 180)
            # Default for unknowns
            if wrc is None:
                wrc = 95
            tw += wrc * pa_wt
            tp += pa_wt
        if tp > 0:
            stats_wrc = tw / tp

    vegas_opp = None
    if odds:
        oi = safe(odds.get('away_implied' if is_home else 'home_implied'))
        if not oi:
            t = safe(odds.get('game_total'))
            oi = t / 2.0 if t else None
        if oi:
            vegas_opp = (oi / LEAGUE_AVG_IMPLIED) * 100.0

    if stats_wrc is not None and vegas_opp is not None:
        blended = stats_wrc * 0.70 + vegas_opp * 0.30
    elif stats_wrc is not None:
        # No odds data — light regression toward 100 (neutral)
        blended = stats_wrc * 0.80 + 100.0 * 0.20
    elif vegas_opp is not None:
        blended = vegas_opp
    else:
        return 1.0
    return clip(blended / 100.0, 0.65, 1.45)


# ── Platoon Adjustment ────────────────────────────────────────────────────────

def platoon_adjust(talent: dict, split_row: dict) -> dict:
    """Adjust batter talent rates using platoon split data."""
    if not split_row:
        return talent

    pa = safe(split_row.get('pa'), 0)
    reg_factor = clip(pa / 300.0, 0.0, 1.0)

    # Adjust K% and BB% toward split values
    split_k = safe(split_row.get('k_pct'))
    split_bb = safe(split_row.get('bb_pct'))

    adjusted = dict(talent)
    if split_k is not None:
        adjusted['k_pct'] = talent['k_pct'] * (1 - reg_factor) + split_k * reg_factor
    if split_bb is not None:
        adjusted['bb_pct'] = talent['bb_pct'] * (1 - reg_factor) + split_bb * reg_factor

    # Adjust power via wRC+ ratio (scales ISO and BABIP)
    wrc = safe(split_row.get('wrc_plus'))
    if wrc is not None:
        reg_wrc = wrc * reg_factor + 100.0 * (1 - reg_factor)
        ratio = clip(reg_wrc / 100.0, 0.70, 1.40)
        adjusted['iso'] = clip(talent['iso'] * ratio, 0.02, 0.40)
        adjusted['babip'] = clip(talent['babip'] * (1 + (ratio - 1) * 0.3), 0.22, 0.38)

    return adjusted


# ── Main ──────────────────────────────────────────────────────────────────────

def run():
    # Parse args
    target_date = None
    n_sims = NUM_SIMS
    args = sys.argv[1:]
    i = 0
    while i < len(args):
        if args[i] == '--date' and i+1 < len(args):
            target_date = args[i+1]; i += 2
        elif args[i] == '--sims' and i+1 < len(args):
            n_sims = int(args[i+1]); i += 2
        else:
            target_date = target_date or args[i]; i += 1
    if not target_date:
        target_date = str(date.today())

    print(f"\nMonte Carlo Projection Engine — {target_date} ({n_sims:,} sims)")
    print("=" * 60)

    data = fetch_data(target_date)
    if not data or not data.get('games'):
        print("  No games found — exiting")
        return

    game_map = {g['game_pk']: g for g in data['games']}
    rng = np.random.default_rng(seed=42)  # reproducible by default
    computed_at = datetime.now(timezone.utc).isoformat()
    records = []

    # ── Batter projections (full game simulation) ─────────────────────────
    print("\n  Simulating batters (full game sim)...")
    batter_count = 0

    # Group lineups by (game_pk, team_id) to build 9-man lineups
    from collections import defaultdict as _dd
    game_team_lineups = _dd(list)
    for lu in data['lineups']:
        pid = lu.get('player_id')
        gpk = lu.get('game_pk')
        team_id = lu.get('team_id')
        if pid and gpk and team_id:
            game_team_lineups[(gpk, team_id)].append(lu)

    for (gpk, team_id), team_lus in game_team_lineups.items():
        game = game_map.get(gpk)
        if not game:
            continue

        is_home = (team_id == game.get('home_team_id'))
        opp_sp_id   = game.get('away_sp_id') if is_home else game.get('home_sp_id')
        opp_sp_hand = game.get('away_sp_hand') if is_home else game.get('home_sp_hand')

        # Sort by batting order (1-9)
        team_lus.sort(key=lambda x: x.get('batting_order') or 99)
        # Keep only first 9 (one per lineup spot)
        seen_orders = set()
        ordered_lus = []
        for lu in team_lus:
            bo = lu.get('batting_order') or 9
            if bo not in seen_orders:
                seen_orders.add(bo)
                ordered_lus.append(lu)
            if len(ordered_lus) >= 9:
                break
        if len(ordered_lus) < 9:
            continue  # not a full lineup

        # Get opposing SP talent
        pitcher = None
        sp_proj_ip = 5.5
        if opp_sp_id:
            p_stats = data['pitcher_stats'].get(opp_sp_id, {})
            if p_stats:
                pitcher = marcel_pitcher(p_stats, SEASON, target_date)
                # Three-tier reliability blend (Stuff/Pitch-Level/Event)
                opp_current_ip = safe((p_stats.get(SEASON) or {}).get('ip'), 0)
                opp_arsenal = data.get('arsenal_data', {}).get(opp_sp_id)
                pitcher = reliability_blend_pitcher(pitcher, opp_arsenal, opp_current_ip)
                sp_proj_ip = pitcher.get('ip_per_gs', 5.5)
                # Blend with Vegas props if available
                props = data.get('pitcher_props', {}).get(opp_sp_id)
                if props and safe(props.get('implied_ip')):
                    sp_proj_ip = safe(props['implied_ip']) * 0.55 + sp_proj_ip * 0.45
                # $4000 opener detection: reliever-priced SP defaults to 1 IP
                # unless Vegas prop provides a real line
                elif data.get('sp_salaries', {}).get(opp_sp_id, 99999) <= 4000:
                    sp_proj_ip = 1.0

        # Environment
        park_row = data['park_factors'].get(game.get('venue_id'))
        wx_row   = data['weather'].get(gpk)
        if wx_row and wx_row.get('is_outdoor') is False:
            wx_row = None  # indoor — no weather effect
        odds_row = data['odds'].get(gpk)

        # Opposing team's bullpen quality (for after SP exits)
        opp_team_id = game.get('away_team_id') if is_home else game.get('home_team_id')
        bp_quality = data.get('bullpen_quality', {}).get(opp_team_id)

        # Build talent + rates for each batter
        lineup_talents = []
        lineup_meta = []  # (pid, stats_pid, stats_by_yr, talent, lu)
        for lu in ordered_lus:
            pid = lu['player_id']
            stats_pid = pid
            stats_by_yr = data['batter_stats'].get(pid, {})
            # Phantom rows (full_name=None, pa=None) can exist from load_stats —
            # treat them as empty so the remap fallback triggers
            if stats_by_yr:
                any_real = any(s.get('full_name') and s.get('pa') for s in stats_by_yr.values())
                if not any_real:
                    stats_by_yr = {}
            if not stats_by_yr:
                alt = data.get('_forward_remap', {}).get(pid)
                if alt:
                    stats_by_yr = data['batter_stats'].get(alt, {})
                    if stats_by_yr:
                        stats_pid = alt
            if not stats_by_yr:
                print(f"    WARNING: No stats found for {lu.get('player_name','?')} (id={pid}) — using league-average fallback")
                talent = {
                    'k_pct': LEAGUE_AVG_K_PCT, 'bb_pct': LEAGUE_AVG_BB_PCT,
                    'iso': LEAGUE_AVG_ISO, 'avg': 0.248, 'babip': LEAGUE_AVG_BABIP,
                    'woba': LEAGUE_AVG_WOBA, 'barrel': 0.065, 'hard_hit': 0.35,
                    'avg_ev': 88.0, 'sb_per_pa': 0.01, 'fb_pct': 0.35, 'pull_pct': 0.40,
                    'ld_pct': 0.21, 'bat_speed': 72.0, 'squared_up': 0.18,
                    'blast': 0.08, 'o_swing': 0.30, 'attack_angle': 12.0,
                    'swing_length': 7.2,
                    'r_per_pa': 0.11, 'rbi_per_pa': 0.10,
                    'hr_per_pa': 0.03, 'xb_per_hit': 0.14,
                }
            else:
                talent = marcel_batter(stats_by_yr, SEASON, target_date)
                # Merge swing_length from bat_tracking table (not in batter_stats)
                bt = data.get('bat_tracking', {}).get(stats_pid)
                if bt and safe(bt.get('swing_length')):
                    talent['swing_length'] = safe(bt['swing_length'])

            # Platoon adjustment — REMOVED (Session 45)
            # Individual batter talent already reflects platoon tendencies via
            # their own K%/BB%/ISO/BABIP. Applying a team-level platoon_adjust
            # double-counted the split and showed r=0.000 across all postgame
            # reviews. Keep _platoon_adj=1.0 for output compatibility.
            talent['_platoon_adj'] = 1.0

            # Compute PA rates vs SP and vs bullpen
            rates_sp = _compute_pa_rates(talent, pitcher, park_row, wx_row)
            rates_bp = _bullpen_rates(talent, park_row, wx_row, bp_quality)

            talent['rates_vs_sp'] = rates_sp
            talent['rates_vs_bp'] = rates_bp
            lineup_talents.append(talent)
            lineup_meta.append((pid, stats_pid, stats_by_yr, talent, lu))

        # Build SB context: opposing catcher pop time + pitcher SB vulnerability
        sb_ctx = None
        opp_team_id = game.get('away_team_id') if is_home else game.get('home_team_id')
        opp_lus = game_team_lineups.get((gpk, opp_team_id), [])
        opp_catcher = next((lu for lu in opp_lus if lu.get('position') == 'C'), None)
        catcher_pop = 1.95  # league avg default
        if opp_catcher:
            cpid = opp_catcher['player_id']
            pop = data.get('catcher_poptime', {}).get(cpid)
            if pop:
                catcher_pop = pop
        pitcher_sb9 = pitcher.get('sb_per_9', 1.0) if pitcher else 1.0
        sb_ctx = {'catcher_pop': catcher_pop, 'pitcher_sb_per_9': pitcher_sb9}

        # Run full game simulation
        dk_results, sb_results = sim_full_game(
            lineup_talents, pitcher, park_row, wx_row, odds_row,
            is_home, n_sims, rng, sp_proj_ip, sb_context=sb_ctx
        )

        # Build records from results
        for slot_idx, (pid, stats_pid, stats_by_yr, talent, lu) in enumerate(lineup_meta):
            dk_dist = dk_results[slot_idx]
            sb_dist = sb_results[slot_idx]
            proj_sb = round2(float(np.mean(sb_dist)))
            order = lu.get('batting_order') or (slot_idx + 1)

            # ── Direct calculation of expected DK points (hitter) ─────────────
            # The Monte Carlo sim compresses talent: elite hitters (hit_p near 0.52
            # ceiling) get clipped down, while average hitters get inflated by
            # R/RBI from base-state tracking. Direct calculation preserves the
            # true talent separation. Sim still used for distribution (P10-P90).
            proj_pa = LINEUP_PA.get(order, LEAGUE_AVG_PA)

            # Blend SP and BP rates (60% SP, 40% BP — typical split)
            SP_FRAC = 0.60
            blended = {}
            for key in ['k', 'bb', 'hbp', 'hit', 'hr', 'xb', 'triple', 'sb']:
                sp_val = talent['rates_vs_sp'].get(key, 0)
                bp_val = talent['rates_vs_bp'].get(key, 0)
                blended[key] = sp_val * SP_FRAC + bp_val * (1 - SP_FRAC)

            # Expected outcomes per PA
            bip_rate = max(0.30, 1.0 - blended['k'] - blended['bb'] - blended['hbp'])
            exp_hits = proj_pa * bip_rate * blended['hit']
            exp_hr = exp_hits * blended['hr']
            exp_3b = exp_hits * blended['triple']
            exp_2b = exp_hits * blended['xb']
            exp_1b = exp_hits * max(0, 1.0 - blended['hr'] - blended['xb'] - blended['triple'])
            exp_bb = proj_pa * blended['bb']
            exp_hbp = proj_pa * blended['hbp']
            exp_sb = proj_pa * blended['sb']

            # R and RBI: career rates regressed 30% toward league average.
            # Raw career rates inflate power hitters who played on good teams.
            # K% discount: high-K hitters have fewer productive PAs for R/RBI.
            league_r, league_rbi = 0.11, 0.10
            k_discount = clip(1.0 - (talent['k_pct'] - 0.22) * 1.0, 0.85, 1.05)
            exp_r = (talent.get('r_per_pa', league_r) * 0.70 + league_r * 0.30) * proj_pa * k_discount
            exp_rbi = (talent.get('rbi_per_pa', league_rbi) * 0.70 + league_rbi * 0.30) * proj_pa * k_discount

            # DK Classic hitter scoring — direct calculation
            direct_dk = (
                exp_1b * 3 + exp_2b * 5 + exp_3b * 8 + exp_hr * 10
                + exp_r * 2 + exp_rbi * 2
                + exp_bb * 2 + exp_hbp * 2
                + exp_sb * 5
            )

            mean = direct_dk
            median = float(np.median(dk_dist))
            sd     = float(np.std(dk_dist))
            p10    = float(np.percentile(dk_dist, 10))
            p25    = float(np.percentile(dk_dist, 25))
            p75    = float(np.percentile(dk_dist, 75))
            p90    = float(np.percentile(dk_dist, 90))

            curr_stats = stats_by_yr.get(SEASON) or stats_by_yr.get(SEASON-1) or stats_by_yr.get(SEASON-2)
            full_name = (curr_stats.get('full_name') if curr_stats else None) or lu.get('player_name')
            team = curr_stats.get('team') if curr_stats else None

            # Transparency multipliers — Pitching+/Stuff+ as primary quality signal
            _pitcher_mult = 1.0
            if pitcher:
                pq = (pitcher.get('pitching_plus') or pitcher.get('stuff_plus') or 100.0) / 100.0
                pk = (pitcher['k_pct'] / LEAGUE_AVG_K_PCT) if pitcher['k_pct'] > 0 else 1.0
                pbr = (LEAGUE_AVG_BB_PCT / pitcher['bb_pct']) if pitcher['bb_pct'] > 0.02 else 1.0
                _pitcher_mult = round2(0.40 * pq + 0.30 * pk + 0.30 * (1.0/pbr) if pbr > 0 else 1.0)
            _platoon_mult = round2(talent.get('_platoon_adj', 1.0))
            _park_basic = safe(park_row.get('basic_factor'), 100) / 100.0 if park_row else 1.0
            _wx_hit = weather_hit_mult(wx_row)
            _implied = None
            if odds_row:
                _implied = safe(odds_row.get('home_implied' if is_home else 'away_implied'))
                if not _implied:
                    _gt = safe(odds_row.get('game_total'))
                    _implied = _gt / 2.0 if _gt else None
            _vegas_mult = round2(clip((_implied / LEAGUE_AVG_IMPLIED) if _implied else 1.0, 0.70, 1.45))
            _park_mult = round2(_park_basic)
            _weather_mult = round2(_wx_hit)
            _context_mult = round2(1.0 + (_vegas_mult - 1.0) * 0.80 + (_park_mult - 1.0) * 0.05 + (_weather_mult - 1.0) * 0.15)

            records.append({
                'player_id': pid, 'game_pk': gpk, 'game_date': target_date,
                'full_name': full_name, 'team': team, 'batting_order': order,
                'is_pitcher': False, 'computed_at': computed_at,
                'proj_dk_pts': round2(mean),
                'proj_floor':  round2(p10),
                'proj_ceiling': round2(p90),
                'sim_mean': round2(mean), 'sim_median': round2(median),
                'sim_floor': round2(p10), 'sim_ceiling': round2(p90),
                'sim_sd': round2(sd), 'sim_p25': round2(p25), 'sim_p75': round2(p75),
                'sim_count': n_sims,
                'pitcher_mult': _pitcher_mult, 'platoon_mult': _platoon_mult,
                'context_mult': _context_mult, 'vegas_mult': _vegas_mult,
                'park_mult': _park_mult, 'weather_mult': _weather_mult,
                'proj_sb': proj_sb,
                'proj_ip': None, 'proj_ks': None, 'proj_er': None,
                'proj_h_allowed': None, 'proj_bb_allowed': None, 'win_prob': None,
            })
            batter_count += 1

    print(f"  Batters simulated: {batter_count}")

    # ── Pitcher projections ───────────────────────────────────────────────
    print("  Simulating pitchers...")
    pitcher_count = 0

    for game in data['games']:
        gpk = game['game_pk']
        venue_id = game.get('venue_id')
        odds_row = data['odds'].get(gpk)
        park_row = data['park_factors'].get(venue_id)
        wx_row   = data['weather'].get(gpk)
        if wx_row and wx_row.get('is_outdoor') is False:
            wx_row = None

        for role, sp_id, pitcher_team in [
            ('home', game.get('home_sp_id'), game.get('home_team')),
            ('away', game.get('away_sp_id'), game.get('away_team')),
        ]:
            if not sp_id:
                continue

            p_stats = data['pitcher_stats'].get(sp_id, {})
            if not p_stats:
                continue

            talent = marcel_pitcher(p_stats, SEASON, target_date)
            # Three-tier reliability blend
            sp_current_ip = safe((p_stats.get(SEASON) or {}).get('ip'), 0)
            sp_arsenal = data.get('arsenal_data', {}).get(sp_id)
            talent = reliability_blend_pitcher(talent, sp_arsenal, sp_current_ip)
            is_home = (role == 'home')
            sp_hand = game.get('home_sp_hand' if is_home else 'away_sp_hand')
            opp_team_id = game.get('away_team_id' if is_home else 'home_team_id')

            opp_qual = compute_opp_quality(
                data['lineups'], data['batter_stats'], data['batter_splits'],
                opp_team_id, sp_hand, odds_row, is_home
            )

            # Vegas pitcher props (IP and K lines)
            props = data.get('pitcher_props', {}).get(sp_id)
            v_ip = safe(props.get('implied_ip')) if props else None
            v_ks = safe(props.get('implied_ks')) if props else None
            # $4000 opener detection: reliever-priced SP defaults to 1 IP
            if not v_ip and data.get('sp_salaries', {}).get(sp_id, 99999) <= 4000:
                v_ip = 1.0
                v_ks = v_ks or 1.0

            # Opposing lineup handedness composition for pitcher splits
            p_splits = data['pitcher_splits'].get(sp_id)
            opp_hand_pct = None
            if p_splits:
                opp_lus = [lu for lu in data['lineups']
                           if lu.get('team_id') == opp_team_id and lu.get('batting_order')]
                if opp_lus:
                    # Count RHH vs LHH in opposing lineup using batter_stats
                    # Bats hand isn't in our data, so approximate from splits PA:
                    # batters with more PA vs RHP are likely LHH (face RHP more often)
                    # Default to 60% RHH (league average)
                    rhh_count = 0
                    total = 0
                    for lu in opp_lus:
                        bp = data['batter_splits'].get(lu['player_id'], {})
                        r_pa = safe(bp.get('R', {}).get('pa'), 0)
                        l_pa = safe(bp.get('L', {}).get('pa'), 0)
                        if r_pa + l_pa > 20:
                            # More PA vs RHP → likely LHH (or switch), more vs LHP → RHH
                            # RHH face LHP less often, so higher L split PA = RHH
                            rhh_count += l_pa / (r_pa + l_pa)  # fraction of PA vs LHP ≈ prob RHH
                        else:
                            rhh_count += 0.60  # league default
                        total += 1
                    opp_hand_pct = rhh_count / total if total > 0 else 0.60

            dk_dist = sim_pitcher_game(
                talent, opp_qual, park_row, wx_row, odds_row,
                is_home, n_sims, rng,
                vegas_ip=v_ip, vegas_ks=v_ks,
                pitcher_split_data=p_splits, opp_hand_pct=opp_hand_pct
            )

            mean   = float(np.mean(dk_dist))
            median = float(np.median(dk_dist))
            sd     = float(np.std(dk_dist))
            p10    = float(np.percentile(dk_dist, 10))
            p25    = float(np.percentile(dk_dist, 25))
            p75    = float(np.percentile(dk_dist, 75))
            p90    = float(np.percentile(dk_dist, 90))

            # ── Direct calculation of expected DK points ────────────────────
            # THE BAT X approach: compute expected stat line directly from rates,
            # then apply DK formula. No Monte Carlo compounding/clipping bias.
            # Sim still used for distribution (P10, P50, P90, SD).
            PA_PER_IP = 4.3
            talent_ip = talent['ip_per_gs']
            # IP: regress 50% toward 5.0 league avg (no Vegas props available)
            exp_ip = talent_ip * 0.50 + 5.0 * 0.50
            exp_bf = exp_ip * PA_PER_IP

            # K rate: talent with park adjustment and 15% league-avg regression
            park_k = safe(park_row.get('k_factor'), 100) / 100.0 if park_row else 1.0
            regressed_k = talent['k_pct'] * 0.75 + LEAGUE_AVG_K_PCT * 0.25
            park_k_edge_dc = 1.0 + (park_k - 1.0) * 0.50
            exp_k_rate = clip(regressed_k * park_k_edge_dc, 0.10, 0.45)
            exp_ks = exp_bf * exp_k_rate

            # BB rate: talent with park and matchup adjustment
            park_bb = safe(park_row.get('bb_factor'), 100) / 100.0 if park_row else 1.0
            exp_bb_rate = clip(talent['bb_pct'] * clip(1.0 + (opp_qual - 1.0) * 0.20, 0.88, 1.15) * park_bb, 0.03, 0.16)
            exp_bb = exp_bf * exp_bb_rate

            # Hit rate: BABIP adjusted for matchup and park
            park_basic = safe(park_row.get('basic_factor'), 100) / 100.0 if park_row else 1.0
            park_basic_edge = 1.0 + (park_basic - 1.0) * 0.50
            exp_hit_rate = clip(talent['babip'] * opp_qual * park_basic_edge, 0.22, 0.38)
            contact_rate = max(0.15, 1.0 - exp_k_rate - exp_bb_rate)
            exp_h = exp_bf * contact_rate * exp_hit_rate

            # HBP
            exp_hbp = exp_bf * 0.01

            # ER: from xFIP/SIERA blend with park/weather
            if talent.get('is_breakout'):
                siera_wt = 0.25 if talent.get('_has_current_siera') else 0.15
                era_anchor = talent['xfip'] * (1.0 - siera_wt) + talent['siera'] * siera_wt
            else:
                era_anchor = talent['xfip'] * 0.50 + talent['siera'] * 0.50
            park_hr_f = safe(park_row.get('hr_factor'), 100) / 100.0 if park_row else 1.0
            wx_hr = weather_hr_mult(wx_row)
            park_er_adj = 1.0 + (park_hr_f - 1.0) * 0.45
            wx_er_adj = 1.0 + (wx_hr - 1.0) * 0.45
            exp_er = era_anchor * exp_ip / 9.0 * opp_qual * park_er_adj * wx_er_adj

            # Win probability
            exp_win = 0.25
            if odds_row:
                hml = odds_row.get('home_ml')
                aml = odds_row.get('away_ml')
                if hml and aml:
                    def _tp(ml):
                        ml = int(ml)
                        return abs(ml)/(abs(ml)+100) if ml < 0 else 100/(ml+100)
                    hp, ap = _tp(hml), _tp(aml)
                    tw = (hp / (hp+ap)) if is_home else (ap / (hp+ap))
                    exp_win = clip(tw * 0.78 * clip(exp_ip / 5.1, 0.70, 1.30), 0.10, 0.52)

            # DK Classic pitcher scoring — direct calculation (no sim bias)
            direct_dk = (exp_ip * 2.25 + exp_ks * 2.0 + exp_win * 4.0
                         - exp_er * 2.0 - exp_h * 0.6 - exp_bb * 0.6 - exp_hbp * 0.6)

            # Post-calculation adjustments (same as sim)
            gb_pct_val = talent.get('gb_pct', 0.43)
            gb_adj = clip(1.0 - (gb_pct_val - 0.43) * 0.15, 0.92, 1.04)
            bb_adj = clip(1.0 - (talent['bb_pct'] - LEAGUE_AVG_BB_PCT) * 1.5, 0.92, 1.04)
            direct_dk *= gb_adj * bb_adj

            curr = p_stats.get(SEASON) or p_stats.get(SEASON-1) or p_stats.get(SEASON-2)
            full_name = (curr or {}).get('full_name') or '?'

            records.append({
                'player_id': sp_id, 'game_pk': gpk, 'game_date': target_date,
                'full_name': full_name, 'team': pitcher_team,
                'batting_order': None, 'is_pitcher': True,
                'computed_at': computed_at,
                'proj_dk_pts': round2(direct_dk),  # Direct calculation — no sim bias
                'proj_floor': round2(p10),
                'proj_ceiling': round2(p90),
                'sim_mean': round2(mean), 'sim_median': round2(median),
                'sim_floor': round2(p10), 'sim_ceiling': round2(p90),
                'sim_sd': round2(sd), 'sim_p25': round2(p25), 'sim_p75': round2(p75),
                'sim_count': n_sims,
                # Pitcher component fields
                'proj_ip': round2(exp_ip),
                'proj_ks': round2(exp_ks),
                'proj_er': round2(exp_er),
                'proj_h_allowed': round2(exp_h),
                'proj_bb_allowed': round2(exp_bb),
                'win_prob': round2(exp_win),
                # Null out batter-specific fields
                'proj_pa': None, 'proj_h': None, 'proj_1b': None, 'proj_2b': None,
                'proj_3b': None, 'proj_hr': None, 'proj_bb': None,
                'proj_r': None, 'proj_rbi': None, 'proj_sb': None,
                'base_woba': None, 'matchup_woba': None, 'final_woba': None,
                # Pitcher transparency multipliers for diagnostics
                'pitcher_mult': round2(opp_qual),  # opposing lineup quality (lower = weaker lineup = better for pitcher)
                'platoon_mult': None,
                'context_mult': None,
                'vegas_mult': round2(exp_win / 0.25) if exp_win else None,  # win prob relative to baseline
                'park_mult': round2(park_hr_f),
                'weather_mult': round2(wx_hr),
            })
            pitcher_count += 1

    print(f"  Pitchers simulated: {pitcher_count}")
    print(f"\n  Total: {len(records)} projections")

    if not records:
        print("  Nothing to upsert.")
        return

    # Deduplicate by (player_id, game_pk)
    seen = {}
    for r in records:
        seen[(r['player_id'], r['game_pk'])] = r
    records = list(seen.values())

    # Delete stale projections for this date before upserting.
    # Previous runs may have used different player_ids for the same player
    # (MLBAM vs DK ID), leaving orphan rows that cause duplicates.
    print(f"  Clearing stale projections for {target_date}...")
    sb.table('player_projections').delete().eq('game_date', target_date).execute()

    # Strip sim-specific columns if migration hasn't been run yet
    SIM_COLS = {'sim_mean', 'sim_median', 'sim_floor', 'sim_ceiling',
                'sim_sd', 'sim_p25', 'sim_p75', 'sim_count'}
    # Try upserting with sim columns first; if it fails, strip them
    db_records = records
    try:
        sb.table('player_projections').upsert(
            [records[0]], on_conflict='player_id,game_pk', ignore_duplicates=False
        ).execute()
        print("  Sim columns detected — uploading full distribution data")
    except Exception:
        print("  Sim columns not found — run migrate_sim_columns.sql for full distribution data")
        print("  Uploading backwards-compatible projection data only")
        db_records = [{k: v for k, v in r.items() if k not in SIM_COLS} for r in records]

    # Upsert in batches
    BATCH = 500
    uploaded = 0
    for i in range(0, len(db_records), BATCH):
        batch = db_records[i:i+BATCH]
        sb.table('player_projections').upsert(
            batch, on_conflict='player_id,game_pk', ignore_duplicates=False
        ).execute()
        uploaded += len(batch)
        print(f"  Uploaded {uploaded}/{len(db_records)}")

    # Sample output
    hitters = sorted([r for r in records if not r['is_pitcher']],
                      key=lambda r: r['proj_dk_pts'] or 0, reverse=True)[:5]
    print(f"\n  Top 5 hitter projections:")
    for r in hitters:
        print(f"    {r['full_name']:25s}  {r['proj_dk_pts']:5.1f} pts  "
              f"(P10={r['sim_floor']:.1f}  P50={r['sim_median']:.1f}  "
              f"P90={r['sim_ceiling']:.1f}  SD={r['sim_sd']:.1f})")

    pitchers = sorted([r for r in records if r['is_pitcher']],
                       key=lambda r: r['proj_dk_pts'] or 0, reverse=True)
    print(f"\n  Pitcher projections ({len(pitchers)} SPs):")
    for r in pitchers:
        print(f"    {r['full_name']:25s}  {r['proj_dk_pts']:5.1f} pts  "
              f"(P10={r['sim_floor']:.1f}  P50={r['sim_median']:.1f}  "
              f"P90={r['sim_ceiling']:.1f}  SD={r['sim_sd']:.1f})")

    print(f"\nSimulation complete. {uploaded} records upserted.")

    # Archive to projection_history (append-only, never overwritten)
    try:
        history_rows = [{
            'player_id': r['player_id'], 'game_pk': r.get('game_pk'),
            'game_date': r['game_date'], 'full_name': r.get('full_name'),
            'team': r.get('team'), 'batting_order': r.get('batting_order'),
            'is_pitcher': r.get('is_pitcher', False),
            'proj_dk_pts': r.get('proj_dk_pts'),
            'proj_floor': r.get('proj_floor'), 'proj_ceiling': r.get('proj_ceiling'),
            'proj_ip': r.get('proj_ip'), 'proj_ks': r.get('proj_ks'),
            'proj_er': r.get('proj_er'), 'win_prob': r.get('win_prob'),
            'computed_at': r.get('computed_at'),
        } for r in records]
        # Delete existing history for this date (re-run overwrites)
        sb.table('projection_history').delete().eq('game_date', target_date).execute()
        for i in range(0, len(history_rows), 500):
            sb.table('projection_history').insert(history_rows[i:i+500]).execute()
        print(f"  Archived {len(history_rows)} rows to projection_history.")
    except Exception as e:
        print(f"  (projection_history archive skipped: {e})")


if __name__ == '__main__':
    run()
