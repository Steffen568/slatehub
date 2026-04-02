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
MARCEL_WEIGHTS = {0: 5, 1: 4, 2: 3}
MIN_PA_BATTER  = 75
MIN_IP_PITCHER = 25

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


# ── Marcel True Talent ────────────────────────────────────────────────────────

def marcel_batter(stats_by_season: dict, current_season: int) -> dict:
    """Marcel-weighted batter true talent across 3 seasons."""
    curr = stats_by_season.get(current_season)
    curr_pa = safe(curr.get('pa'), 0) if curr else 0
    use_current = curr_pa >= MIN_PA_BATTER

    weights = [
        (current_season,     5 if use_current else 0),
        (current_season - 1, 4),
        (current_season - 2, 3),
    ]

    def weighted_stat(col, league_avg, reg_pa=200):
        num = reg_pa * league_avg
        den = float(reg_pa)
        for yr, wt in weights:
            if wt == 0:
                continue
            row = stats_by_season.get(yr)
            if not row:
                continue
            val = safe(row.get(col))
            pa  = safe(row.get('pa'), 0)
            if val is None or pa == 0:
                continue
            num += val * pa * wt
            den += pa * wt
        return num / den if den > reg_pa else league_avg

    k_pct  = clip(weighted_stat('k_pct',  LEAGUE_AVG_K_PCT),  0.05, 0.45)
    bb_pct = clip(weighted_stat('bb_pct', LEAGUE_AVG_BB_PCT), 0.02, 0.25)
    iso    = clip(weighted_stat('iso',    LEAGUE_AVG_ISO),     0.02, 0.40)
    avg    = clip(weighted_stat('avg',    0.248),              0.15, 0.38)
    babip  = clip(weighted_stat('babip',  LEAGUE_AVG_BABIP),   0.22, 0.38)
    woba   = clip(weighted_stat('woba',   LEAGUE_AVG_WOBA),    0.20, 0.55)

    # Quality of contact
    barrel  = safe(weighted_stat('barrel_pct',   0.065), 0.065)
    hard_hit = safe(weighted_stat('hard_hit_pct', 0.35),  0.35)
    avg_ev  = safe(weighted_stat('avg_ev',       88.0),   88.0)

    # SB rate
    sb_num = 0.0
    sb_den = 0.0
    for yr, wt in weights:
        row = stats_by_season.get(yr)
        if not row or wt == 0:
            continue
        s = safe(row.get('sb'), 0)
        p = safe(row.get('pa'), 0)
        if p > 0:
            sb_num += (s / p) * p * wt
            sb_den += p * wt
    sb_per_pa = clip(sb_num / sb_den, 0.0, 0.08) if sb_den > 0 else 0.01

    return {
        'k_pct': k_pct, 'bb_pct': bb_pct, 'iso': iso, 'avg': avg,
        'babip': babip, 'woba': woba, 'barrel': barrel,
        'hard_hit': hard_hit, 'avg_ev': avg_ev, 'sb_per_pa': sb_per_pa,
    }


def marcel_pitcher(stats_by_season: dict, current_season: int) -> dict:
    """Marcel-weighted pitcher true talent across 3 seasons."""
    curr = stats_by_season.get(current_season)
    curr_ip = safe(curr.get('ip'), 0) if curr else 0
    use_current = curr_ip >= MIN_IP_PITCHER

    weights = [
        (current_season,     5 if use_current else 0),
        (current_season - 1, 4),
        (current_season - 2, 3),
    ]

    def weighted_stat(col, league_avg, reg_ip=80):
        num = reg_ip * league_avg
        den = float(reg_ip)
        for yr, wt in weights:
            if wt == 0:
                continue
            row = stats_by_season.get(yr)
            if not row:
                continue
            val = safe(row.get(col))
            ip  = safe(row.get('ip'), 0)
            if val is None or ip == 0:
                continue
            num += val * ip * wt
            den += ip * wt
        return num / den if den > reg_ip else league_avg

    k_pct  = clip(weighted_stat('k_pct',  LEAGUE_AVG_K_PCT, reg_ip=120), 0.10, 0.40)
    bb_pct = clip(weighted_stat('bb_pct', LEAGUE_AVG_BB_PCT),             0.04, 0.18)
    hr9    = clip(weighted_stat('hr9',    LEAGUE_AVG_HR9),                0.30, 3.00)
    babip  = clip(weighted_stat('babip',  LEAGUE_AVG_BABIP),              0.22, 0.36)
    xfip   = clip(weighted_stat('xfip',   LEAGUE_AVG_XFIP),              2.50, 6.00)
    siera  = clip(weighted_stat('siera',  LEAGUE_AVG_XFIP),              2.50, 6.00)

    # Stuff+ / quality metrics (current or most recent)
    stuff_plus = None
    for yr in ([current_season] if use_current else []) + [current_season-1, current_season-2]:
        row = stats_by_season.get(yr)
        if row and safe(row.get('stuff_plus')):
            stuff_plus = safe(row.get('stuff_plus'))
            break
    stuff_plus = stuff_plus or 100.0

    # IP per GS
    ip_per_gs = 5.1
    seasons = [current_season, current_season-1, current_season-2] if use_current \
              else [current_season-1, current_season-2]
    for yr in seasons:
        row = stats_by_season.get(yr)
        if not row:
            continue
        ip = safe(row.get('ip'), 0)
        gs = safe(row.get('gs'), 0)
        if gs >= 5:
            raw = clip(ip / gs, 3.0, 6.5)
            ip_per_gs = raw * 0.92 + 5.1 * 0.08
            break

    return {
        'k_pct': k_pct, 'bb_pct': bb_pct, 'hr9': hr9, 'babip': babip,
        'xfip': xfip, 'siera': siera, 'stuff_plus': stuff_plus,
        'ip_per_gs': ip_per_gs,
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
        if wind_dir in WIND_OUT_DIRS:
            wind_effect = (wind_spd / 15.0) * 0.05
        elif wind_dir in WIND_IN_DIRS:
            wind_effect = -(wind_spd / 15.0) * 0.05
    return clip(1.0 + temp_effect + wind_effect, 0.85, 1.20)


def weather_hit_mult(weather_row: dict) -> float:
    """General hit probability multiplier from weather (smaller effect than HR)."""
    if not weather_row or weather_row.get('is_outdoor') is False:
        return 1.0
    temp = safe(weather_row.get('temp_f'), 72)
    return clip(1.0 + ((temp - 72) / 10) * 0.008, 0.96, 1.06)


# ── Batter PA Simulation ─────────────────────────────────────────────────────

def sim_batter_game(talent: dict, pitcher: dict, park: dict, weather: dict,
                    odds: dict, batting_order: int, is_home: bool,
                    n_sims: int, rng: np.random.Generator) -> np.ndarray:
    """
    Simulate n_sims games for one batter. Returns array of DK points per sim.

    Each game simulates individual PA outcomes using combined batter/pitcher/
    park/weather probabilities.
    """
    proj_pa = LINEUP_PA.get(batting_order, LEAGUE_AVG_PA)

    # ── PA outcome probabilities ──────────────────────────────────────────
    # K rate: batter talent × pitcher skill ratio × park K factor
    pitcher_k_ratio = (pitcher['k_pct'] / LEAGUE_AVG_K_PCT) if pitcher else 1.0
    park_k = safe(park.get('k_factor'), 100) / 100.0 if park else 1.0
    k_rate = clip(talent['k_pct'] * pitcher_k_ratio * park_k, 0.05, 0.50)

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

    park_basic = safe(park.get('basic_factor'), 100) / 100.0 if park else 1.0
    wx_hit = weather_hit_mult(weather)

    hit_prob = clip(talent['babip'] * qoc_mult * park_basic * wx_hit, 0.18, 0.42)

    # ── Hit type distribution ─────────────────────────────────────────────
    park_hr = safe(park.get('hr_factor'), 100) / 100.0 if park else 1.0
    wx_hr = weather_hr_mult(weather)
    pitcher_hr_ratio = (pitcher['hr9'] / LEAGUE_AVG_HR9) if pitcher else 1.0

    hr_per_hit = clip((talent['iso'] / 3.5) * park_hr * wx_hr * pitcher_hr_ratio / hit_prob,
                       0.03, 0.30)
    xb_per_hit = clip((talent['iso'] - 3 * talent['iso']/3.5) / hit_prob, 0.03, 0.20)
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

    # ── R/RBI multipliers ─────────────────────────────────────────────────
    r_mult   = R_MULT.get(batting_order, 1.0)
    rbi_mult = RBI_MULT.get(batting_order, 1.0)

    # ── Per-sim "hot/cold day" factor for batters ──────────────────────
    # Batters have day-to-day variance: timing, fatigue, approach.
    # hot_day ~ N(1.0, 0.18): scales hit probability and power
    hot_day = rng.normal(1.0, 0.18, size=n_sims).clip(0.50, 1.55)
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

        # Hit type (uses per-sim HR rate)
        type_rolls = rng.random(n_sims)
        is_hr  = is_hit & (type_rolls < sim_hr_per_hit)
        is_3b  = is_hit & (~is_hr) & (type_rolls < hr_per_hit + triple_per_hit)
        is_2b  = is_hit & (~is_hr) & (~is_3b) & (type_rolls < hr_per_hit + triple_per_hit + xb_per_hit)
        is_1b  = is_hit & (~is_hr) & (~is_3b) & (~is_2b)

        # SB opportunity on singles/walks/HBP
        on_base = is_1b | is_bb | is_hbp
        sb_rolls = rng.random(n_sims)
        is_sb = on_base & (sb_rolls < talent['sb_per_pa'])

        # R scoring: empirical run-scoring rates by event type, scaled by
        # lineup position and Vegas environment
        # Research: ~33% of runners who reach base eventually score (MLB avg)
        # HR always scores the batter; XBH score more often than 1B/BB
        r_rolls = rng.random(n_sims)
        base_r_rate = 0.30 * r_mult * vegas_scale
        scores_r_1b  = is_1b  & (r_rolls < base_r_rate * 0.85)
        scores_r_2b  = is_2b  & (r_rolls < base_r_rate * 1.20)
        scores_r_3b  = is_3b  & (r_rolls < base_r_rate * 1.60)
        scores_r_bb  = is_bb  & (r_rolls < base_r_rate * 0.70)
        scores_r_hbp = is_hbp & (r_rolls < base_r_rate * 0.70)
        scores_r = is_hr | scores_r_1b | scores_r_2b | scores_r_3b | scores_r_bb | scores_r_hbp

        # RBI: HR drives in self + runners (~1.3 avg), XBH drive in more
        # than singles, scaled by lineup position and environment
        rbi_rolls = rng.random(n_sims)
        rbi_from_hr = is_hr.astype(float) * (1.0 + rng.poisson(
            clip(0.35 * vegas_scale, 0.1, 0.8), n_sims).clip(0, 3))
        rbi_rate = 0.18 * rbi_mult * vegas_scale
        rbi_from_2b = is_2b.astype(float) * (rbi_rolls < rbi_rate * 1.8).astype(float)
        rbi_from_3b = is_3b.astype(float) * (rbi_rolls < rbi_rate * 2.5).astype(float)
        rbi_from_1b = is_1b.astype(float) * (rbi_rolls < rbi_rate).astype(float)

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
                     rng: np.random.Generator) -> np.ndarray:
    """
    Simulate n_sims starts for one pitcher. Returns array of DK points per sim.

    Simulates innings pitched, strikeouts, walks, hits, earned runs, and win
    decisions using pitcher talent, opposing lineup quality, park, weather.
    """
    PA_PER_IP = 4.3

    base_ip = talent['ip_per_gs']
    # Opposing lineup quality adjusts IP (strong lineups → shorter outings)
    ip_adj = clip(1.0 + (1.0 - opp_quality) * 0.30, 0.90, 1.10)
    proj_ip = base_ip * ip_adj

    # K/BB/Hit rates
    park_k   = safe(park.get('k_factor'), 100) / 100.0 if park else 1.0
    park_bb  = safe(park.get('bb_factor'), 100) / 100.0 if park else 1.0
    park_hr  = safe(park.get('hr_factor'), 100) / 100.0 if park else 1.0
    park_basic = safe(park.get('basic_factor'), 100) / 100.0 if park else 1.0
    wx_hr = weather_hr_mult(weather)

    # Adjust rates by opposing lineup quality
    k_rate  = clip(talent['k_pct'] * (1.0 + (1.0 - opp_quality) * 0.35) * park_k, 0.10, 0.45)
    bb_rate = clip(talent['bb_pct'] * (1.0 + (opp_quality - 1.0) * 0.20) * park_bb, 0.03, 0.16)
    contact_rate = max(0.15, 1.0 - k_rate - bb_rate)

    # Hit rate when ball in play
    hit_on_contact = clip(talent['babip'] * opp_quality * park_basic, 0.22, 0.38)
    # HR rate per batter faced
    hr_rate = clip(talent['hr9'] / (PA_PER_IP * 9) * park_hr * wx_hr * opp_quality, 0.005, 0.060)

    # Win probability
    win_prob = 0.17
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
            win_prob = clip(team_win * 0.68 * ip_scale, 0.10, 0.45)

    # ── ERA-anchored ER model ────────────────────────────────────────────
    # Rather than simulating base-state (which is hard to calibrate),
    # use the pitcher's ERA anchor (SIERA/xFIP blend) scaled by matchup
    # and environment as the per-9 ER rate, then simulate variance around it.
    #
    # This grounds ER in real pitching metrics instead of a fragile base-runner sim.
    era_anchor = (talent['xfip'] * 0.50 + talent['siera'] * 0.50)
    er_per_ip = era_anchor / 9.0 * opp_quality * (safe(park.get('hr_factor'), 100) / 100.0 if park else 1.0)
    er_per_ip *= weather_hr_mult(weather)  # HR-driven ER scales with weather

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
    stuff_sd = 0.18 + 0.06 * era_consistency  # elite ~0.22, mid ~0.24, bad ~0.27
    stuff_day = rng.normal(1.0, stuff_sd, size=n_sims).clip(0.40, 1.65)

    # Scale rates per sim: good stuff day → more Ks, fewer hits, deeper IP
    sim_k_rate  = np.clip(k_rate * stuff_day, 0.08, 0.50)
    sim_bb_rate = np.clip(bb_rate * (2.0 - stuff_day), 0.02, 0.20)  # inverse: good stuff → fewer walks
    sim_hit_rate = np.clip(hit_on_contact * (2.0 - stuff_day), 0.15, 0.42)

    # ── Simulate n_sims games ─────────────────────────────────────────────
    # IP variance wider (SD=1.2): allows 3 IP blowups and 8 IP gems
    sim_ip = rng.normal(proj_ip, 1.2, size=n_sims)
    # Stuff day affects IP: good stuff → deeper, bad stuff → shorter
    sim_ip = sim_ip * (0.85 + 0.15 * stuff_day)
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

    # Win decision
    win_rolls = rng.random(n_sims)
    wins = (win_rolls < win_prob).astype(float)

    # CG / CGSO / NH bonuses
    is_cg   = sim_ip >= 9.0
    is_cgso = is_cg & (total_er == 0)
    is_nh   = is_cg & (total_h == 0)

    # DK points
    dk_pts = (
        sim_ip * 2.25 +
        total_ks * 2.0 +
        wins * 4.0 -
        total_er * 2.0 -
        total_h  * 0.6 -
        total_bb * 0.6 +
        is_cg.astype(float) * 3.0 +
        is_cgso.astype(float) * 3.0 +
        is_nh.astype(float) * 5.0
    )

    # SP calibration: skill-scaled haircut matching compute_projections.py.
    # Aces (low ERA) get lighter haircut (~5%), back-end starters get heavier (~13%).
    # At league-avg ERA (3.90): calibration ~ 0.90.
    era_ratio = clip(era_anchor / LEAGUE_AVG_XFIP, 0.65, 1.55)
    sp_calibration = clip(0.87 + 0.03 * era_ratio, 0.87, 0.95)
    dk_pts = dk_pts * sp_calibration

    return dk_pts


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

    # Build reverse PLAYER_ID_REMAP so we can look up stats when lineup
    # uses a remapped ID (e.g. 115223) but stats are under the original (665489)
    reverse_remap = {}
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
                        remap = eval(compile(_ast.Expression(body=node.value), '<remap>', 'eval'))
                        for orig, remapped in remap.items():
                            reverse_remap[remapped] = orig
                        break
    except Exception:
        pass

    # Batter stats (3 seasons, chunked)
    # Include reverse-remapped IDs so stats load for players whose lineup ID
    # differs from their stats ID (e.g. Vlad Jr: lineup=115223, stats=665489)
    player_ids = list({l['player_id'] for l in lineups if l.get('player_id')})
    alt_ids = [reverse_remap[pid] for pid in player_ids if pid in reverse_remap]
    all_stat_ids = list(set(player_ids + alt_ids))
    batter_stats = {}
    for i in range(0, len(all_stat_ids), 500):
        chunk = all_stat_ids[i:i+500]
        rows = sb.table('batter_stats').select(
            'player_id,season,pa,woba,xwoba,k_pct,bb_pct,iso,avg,sb,babip,'
            'barrel_pct,hard_hit_pct,avg_ev,wrc_plus,full_name,team'
        ).in_('player_id', chunk).in_('season', [SEASON, SEASON-1, SEASON-2]).execute().data or []
        for r in rows:
            batter_stats.setdefault(r['player_id'], {})[r['season']] = r

    # Pitcher stats (3 seasons)
    sp_ids = set()
    for g in games:
        if g.get('home_sp_id'): sp_ids.add(g['home_sp_id'])
        if g.get('away_sp_id'): sp_ids.add(g['away_sp_id'])
    pitcher_stats = {}
    if sp_ids:
        rows = sb.table('pitcher_stats').select(
            'player_id,season,ip,g,gs,xfip,siera,k_pct,bb_pct,hr9,babip,'
            'stuff_plus,location_plus,swstr_pct,full_name,stats_level'
        ).in_('player_id', list(sp_ids)).in_('season', [SEASON, SEASON-1, SEASON-2]).execute().data or []
        for r in rows:
            pitcher_stats.setdefault(r['player_id'], {})[r['season']] = r

    # Batter splits (also include reverse-remapped IDs)
    batter_splits = {}
    if all_stat_ids:
        for i in range(0, len(all_stat_ids), 150):
            chunk = all_stat_ids[i:i+150]
            rows = sb.table('batter_splits').select(
                'player_id,split,pa,wrc_plus,woba,k_pct,bb_pct'
            ).in_('player_id', chunk).in_('season', [SEASON, SEASON-1]).execute().data or []
            for r in rows:
                batter_splits.setdefault(r['player_id'], {})[r['split']] = r

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
            'venue_id,basic_factor,hr_factor,k_factor,bb_factor'
        ).in_('venue_id', venue_ids).execute().data or []
        park_factors = {r['venue_id']: r for r in rows}

    # Weather
    weather = {}
    if game_pks:
        rows = sb.table('weather').select(
            'game_pk,temp_f,wind_speed,wind_dir,precip_pct,is_outdoor,humidity'
        ).in_('game_pk', game_pks).execute().data or []
        weather = {r['game_pk']: r for r in rows}

    return {
        'games': games, 'lineups': lineups,
        'batter_stats': batter_stats, 'pitcher_stats': pitcher_stats,
        'batter_splits': batter_splits, 'odds': odds,
        'park_factors': park_factors, 'weather': weather,
        '_reverse_remap': reverse_remap,
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

    # ── Batter projections ────────────────────────────────────────────────
    print("\n  Simulating batters...")
    batter_count = 0

    for lu in data['lineups']:
        pid = lu.get('player_id')
        gpk = lu.get('game_pk')
        order = lu.get('batting_order') or 9
        team_id = lu.get('team_id')
        if not pid or not gpk:
            continue

        game = game_map.get(gpk)
        if not game:
            continue

        is_home = (team_id == game.get('home_team_id'))
        opp_sp_id   = game.get('away_sp_id') if is_home else game.get('home_sp_id')
        opp_sp_hand = game.get('away_sp_hand') if is_home else game.get('home_sp_hand')

        # Batter talent — try lineup player_id first, then reverse-remap ID
        # (lineups may use DK-remapped ID while stats use MLBAM ID)
        stats_pid = pid
        stats_by_yr = data['batter_stats'].get(pid, {})
        if not stats_by_yr:
            alt = data.get('_reverse_remap', {}).get(pid)
            if alt:
                stats_by_yr = data['batter_stats'].get(alt, {})
                if stats_by_yr:
                    stats_pid = alt
        if not stats_by_yr:
            # No stats = league average fallback
            talent = {
                'k_pct': LEAGUE_AVG_K_PCT, 'bb_pct': LEAGUE_AVG_BB_PCT,
                'iso': LEAGUE_AVG_ISO, 'avg': 0.248, 'babip': LEAGUE_AVG_BABIP,
                'woba': LEAGUE_AVG_WOBA, 'barrel': 0.065, 'hard_hit': 0.35,
                'avg_ev': 88.0, 'sb_per_pa': 0.01,
            }
        else:
            talent = marcel_batter(stats_by_yr, SEASON)

        # Platoon adjustment
        split_row = data['batter_splits'].get(stats_pid, {}).get(opp_sp_hand)
        talent = platoon_adjust(talent, split_row)
        # Track platoon multiplier for diagnostics (wRC+ ratio, regressed)
        if split_row and safe(split_row.get('wrc_plus')):
            _pa = safe(split_row.get('pa'), 0)
            _rf = clip(_pa / 300.0, 0.0, 1.0)
            _rwrc = safe(split_row['wrc_plus']) * _rf + 100.0 * (1 - _rf)
            talent['_platoon_adj'] = clip(_rwrc / 100.0, 0.70, 1.40)
        else:
            talent['_platoon_adj'] = 1.0

        # Pitcher matchup
        pitcher = None
        if opp_sp_id:
            p_stats = data['pitcher_stats'].get(opp_sp_id, {})
            if p_stats:
                pitcher = marcel_pitcher(p_stats, SEASON)

        # Environment
        park_row = data['park_factors'].get(game.get('venue_id'))
        wx_row   = data['weather'].get(gpk)
        odds_row = data['odds'].get(gpk)

        # Run simulation
        dk_dist = sim_batter_game(
            talent, pitcher, park_row, wx_row, odds_row,
            order, is_home, n_sims, rng
        )

        # Compute distribution stats
        mean   = float(np.mean(dk_dist))
        median = float(np.median(dk_dist))
        sd     = float(np.std(dk_dist))
        p10    = float(np.percentile(dk_dist, 10))
        p25    = float(np.percentile(dk_dist, 25))
        p75    = float(np.percentile(dk_dist, 75))
        p90    = float(np.percentile(dk_dist, 90))

        # Name from stats
        curr_stats = stats_by_yr.get(SEASON) or stats_by_yr.get(SEASON-1) or stats_by_yr.get(SEASON-2)
        full_name = (curr_stats.get('full_name') if curr_stats else None) or lu.get('player_name')
        team = curr_stats.get('team') if curr_stats else None

        # Compute transparency multipliers (same factors used inside sim_batter_game)
        _pitcher_mult = 1.0
        if pitcher:
            pk = (pitcher['k_pct'] / LEAGUE_AVG_K_PCT) if pitcher['k_pct'] > 0 else 1.0
            pbr = (LEAGUE_AVG_BB_PCT / pitcher['bb_pct']) if pitcher['bb_pct'] > 0.02 else 1.0
            phr = (pitcher['hr9'] / LEAGUE_AVG_HR9) if pitcher.get('hr9') else 1.0
            _pitcher_mult = round2(0.35 * pk + 0.35 * phr + 0.30 * (1.0/pbr) if pbr > 0 else 1.0)
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
        _context_mult = round2(1.0 + (_vegas_mult - 1.0) * 0.58 + (_park_mult - 1.0) * 0.26 + (_weather_mult - 1.0) * 0.16)

        records.append({
            'player_id': pid, 'game_pk': gpk, 'game_date': target_date,
            'full_name': full_name, 'team': team, 'batting_order': order,
            'is_pitcher': False, 'computed_at': computed_at,
            # Backwards-compatible columns
            'proj_dk_pts': round2(mean),
            'proj_floor':  round2(p10),
            'proj_ceiling': round2(p90),
            # Sim distribution columns
            'sim_mean': round2(mean), 'sim_median': round2(median),
            'sim_floor': round2(p10), 'sim_ceiling': round2(p90),
            'sim_sd': round2(sd), 'sim_p25': round2(p25), 'sim_p75': round2(p75),
            'sim_count': n_sims,
            # Transparency multipliers for diagnostics
            'pitcher_mult': _pitcher_mult, 'platoon_mult': _platoon_mult,
            'context_mult': _context_mult, 'vegas_mult': _vegas_mult,
            'park_mult': _park_mult, 'weather_mult': _weather_mult,
            # Null out pitcher-specific fields
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

            talent = marcel_pitcher(p_stats, SEASON)
            is_home = (role == 'home')
            sp_hand = game.get('home_sp_hand' if is_home else 'away_sp_hand')
            opp_team_id = game.get('away_team_id' if is_home else 'home_team_id')

            opp_qual = compute_opp_quality(
                data['lineups'], data['batter_stats'], data['batter_splits'],
                opp_team_id, sp_hand, odds_row, is_home
            )

            dk_dist = sim_pitcher_game(
                talent, opp_qual, park_row, wx_row, odds_row,
                is_home, n_sims, rng
            )

            mean   = float(np.mean(dk_dist))
            median = float(np.median(dk_dist))
            sd     = float(np.std(dk_dist))
            p10    = float(np.percentile(dk_dist, 10))
            p25    = float(np.percentile(dk_dist, 25))
            p75    = float(np.percentile(dk_dist, 75))
            p90    = float(np.percentile(dk_dist, 90))

            # Compute expected component values for transparency
            PA_PER_IP = 4.3
            ip_adj = clip(1.0 + (1.0 - opp_qual) * 0.30, 0.90, 1.10)
            exp_ip = talent['ip_per_gs'] * ip_adj
            exp_pa = exp_ip * PA_PER_IP
            exp_ks = exp_pa * talent['k_pct'] * clip(1.0 + (1.0 - opp_qual) * 0.35, 0.85, 1.18)
            exp_bb = exp_pa * talent['bb_pct'] * clip(1.0 + (opp_qual - 1.0) * 0.20, 0.88, 1.15)
            era_anchor = talent['xfip'] * 0.50 + talent['siera'] * 0.50
            park_hr_f = safe(park_row.get('hr_factor'), 100) / 100.0 if park_row else 1.0
            wx_hr = weather_hr_mult(wx_row)
            exp_er = era_anchor * exp_ip / 9.0 * opp_qual * park_hr_f * wx_hr

            # Win probability (same as sim_pitcher_game)
            exp_win = 0.17
            if odds_row:
                hml = odds_row.get('home_ml')
                aml = odds_row.get('away_ml')
                if hml and aml:
                    def _tp(ml):
                        ml = int(ml)
                        return abs(ml)/(abs(ml)+100) if ml < 0 else 100/(ml+100)
                    hp, ap = _tp(hml), _tp(aml)
                    tw = (hp / (hp+ap)) if is_home else (ap / (hp+ap))
                    exp_win = clip(tw * 0.68 * clip(exp_ip / 5.1, 0.70, 1.30), 0.10, 0.45)

            curr = p_stats.get(SEASON) or p_stats.get(SEASON-1) or p_stats.get(SEASON-2)
            full_name = (curr or {}).get('full_name') or '?'

            records.append({
                'player_id': sp_id, 'game_pk': gpk, 'game_date': target_date,
                'full_name': full_name, 'team': pitcher_team,
                'batting_order': None, 'is_pitcher': True,
                'computed_at': computed_at,
                'proj_dk_pts': round2(mean),
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
                'proj_h_allowed': round2(exp_pa * max(0.10, 1.0 - talent['k_pct'] - talent['bb_pct']) * talent['babip'] * opp_qual),
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
                'vegas_mult': round2(exp_win / 0.17) if exp_win else None,  # win prob relative to baseline
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


if __name__ == '__main__':
    run()
