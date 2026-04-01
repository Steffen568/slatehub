#!/usr/bin/env python3
import sys
sys.stdout.reconfigure(encoding='utf-8', line_buffering=True)
"""
compute_projections.py — SlateHub DFS Projection Engine

Three-Tier Framework:
  Tier 1 (45%): Player True Talent
      Marcel-weighted 3yr wOBA (5/4/3 PA weights + regression to mean)
      + xwOBA luck correction (partial regression toward expected)

  Tier 2 (25%): Matchup Quality
      pitcher_mult: xFIP(35%) + Stuff+(25%) + K%(25%) + BB%(15%)
      platoon_mult: batter wRC+ vs pitcher hand, PA-regressed toward 100
      pa_mult:      batting order position (leadoff 4.72 PA, 9th 3.85 PA)

  Tier 3 (30%): Game Context (umpire skipped for v1 — 1.5% total weight)
      vegas_mult:   team implied total / 4.5 league avg
      park_mult:    park basic_factor / 100
      weather_mult: temp + wind direction combined
      combined as weighted avg: Vegas 58%, Park 26%, Weather 16%

Output per player stored in player_projections table:
  proj_dk_pts, proj_floor (×0.70), proj_ceiling (×1.45)
  Full stat line: proj_pa, proj_h, proj_1b, proj_2b, proj_3b, proj_hr,
                  proj_bb, proj_r, proj_rbi, proj_sb
  Transparency multipliers: pitcher_mult, platoon_mult, context_mult,
                             vegas_mult, park_mult, weather_mult

Run: py -3.12 compute_projections.py
     py -3.12 compute_projections.py --date 2026-03-26  (specific date)
"""

import os, math
from datetime import date, datetime, timezone
from supabase import create_client
from dotenv import load_dotenv
from config import SEASON

load_dotenv()
sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])

# ── Constants ─────────────────────────────────────────────────────────────────

LEAGUE_AVG_WOBA    = 0.315
LEAGUE_AVG_XFIP    = 3.90
LEAGUE_AVG_STUFF   = 100.0
LEAGUE_AVG_K_PCT   = 0.225
LEAGUE_AVG_BB_PCT  = 0.082
LEAGUE_AVG_IMPLIED = 4.5    # average team implied runs/game

# PA per game by batting order position (research-backed MLB averages)
LINEUP_PA = {1: 4.72, 2: 4.68, 3: 4.58, 4: 4.51, 5: 4.45,
             6: 4.38, 7: 4.25, 8: 4.10, 9: 3.85}
LEAGUE_AVG_PA = 4.3

# Sample size thresholds — current-season data excluded from Marcel until reached
MIN_PA_BATTER  = 75   # ~3 weeks of games for a regular starter
MIN_IP_PITCHER = 25   # ~4-5 starts for a SP

# Run and RBI opportunity multipliers by batting order position
# (Research: BO 1-3 had highest MAE — flattened top-of-order boost)
LINEUP_R_MULT   = {1: 1.18, 2: 1.15, 3: 1.08, 4: 1.00, 5: 0.95,
                   6: 0.90, 7: 0.87, 8: 0.82, 9: 0.78}
LINEUP_RBI_MULT = {1: 0.78, 2: 0.88, 3: 1.12, 4: 1.22, 5: 1.18,
                   6: 1.05, 7: 0.95, 8: 0.90, 9: 0.82}

# Wind directions that blow "out" at the majority of MLB parks
# (most parks face northeast, so wind from S/SW pushes ball to outfield)
WIND_OUT_DIRS = {"S", "SSW", "SW", "WSW", "SSE", "SE"}
WIND_IN_DIRS  = {"N", "NNW", "NW", "NNE", "NE", "WNW"}

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
    if val is None:
        return None
    return max(lo, min(hi, val))

def round2(val):
    return round(val, 2) if val is not None else None


# ── Tier 1: True Talent ───────────────────────────────────────────────────────

def compute_true_talent(stats_by_season: dict, current_season: int) -> dict:
    """
    Marcel-weighted wOBA + xwOBA luck correction.
    stats_by_season: {2026: row, 2025: row, 2024: row}  (None if missing)
    Returns: {'woba': float, 'k_pct': float, 'bb_pct': float,
              'iso': float, 'sb_pa': float, 'avg': float}
    """
    # Check if current season has enough PA to be meaningful
    curr = stats_by_season.get(current_season)
    curr_pa = safe(curr.get('pa'), 0) if curr else 0
    use_current = curr_pa >= MIN_PA_BATTER

    seasons_weights = [
        (current_season,     5 if use_current else 0),
        (current_season - 1, 4),
        (current_season - 2, 3),
    ]

    def marcel_stat(col, league_avg, regression_pa=200):
        """PA-weighted Marcel average with regression to league mean."""
        num = regression_pa * league_avg
        den = float(regression_pa)
        for yr, wt in seasons_weights:
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
        return (num / den) if den > regression_pa else league_avg

    base_woba = marcel_stat('woba', LEAGUE_AVG_WOBA)

    # Luck correction: regress 50% toward xwOBA — only if current season has enough PA
    if use_current and curr:
        xwoba = safe(curr.get('xwoba'))
        woba  = safe(curr.get('woba'))
        if xwoba and woba:
            luck_adj = (xwoba - woba) * 0.50
            base_woba = base_woba + luck_adj

    base_woba = clip(base_woba, 0.200, 0.550)

    # Marcel-weighted component skills (used for stat line conversion)
    k_pct   = clip(marcel_stat('k_pct',   LEAGUE_AVG_K_PCT),   0.05, 0.45)
    bb_pct  = clip(marcel_stat('bb_pct',  LEAGUE_AVG_BB_PCT),  0.02, 0.25)
    iso     = clip(marcel_stat('iso',     0.165),               0.02, 0.400)

    # SB rate: average SB per PA across available seasons
    sb_pa = _compute_sb_pa(stats_by_season, seasons_weights)

    # Projected AVG (used for hit distribution)
    avg = clip(marcel_stat('avg', 0.248), 0.150, 0.380)

    return {
        'woba':   base_woba,
        'k_pct':  k_pct,
        'bb_pct': bb_pct,
        'iso':    iso,
        'sb_pa':  sb_pa,
        'avg':    avg,
    }


def _compute_sb_pa(stats_by_season: dict, seasons_weights: list) -> float:
    """PA-weighted Marcel average for SB per PA."""
    num = 0.0
    den = 0.0
    for yr, wt in seasons_weights:
        row = stats_by_season.get(yr)
        if not row:
            continue
        sb = safe(row.get('sb'), 0.0)
        pa = safe(row.get('pa'), 0.0)
        if pa and pa > 0:
            num += (sb / pa) * pa * wt
            den += pa * wt
    if den == 0:
        return 0.0
    return clip(num / den, 0.0, 0.08)


# ── Tier 2A: Pitcher Quality Multiplier ──────────────────────────────────────

def compute_pitcher_mult(pitcher_row: dict) -> float:
    """
    Returns 0.55-1.65. Lower = tougher for batters.
    Weights: xFIP 35%, Stuff+ 25%, K% 25%, BB% 15%
    Falls back to 1.0 (neutral) if pitcher data is missing.
    """
    if not pitcher_row:
        return 1.0

    xfip       = safe(pitcher_row.get('xfip'))
    stuff_plus = safe(pitcher_row.get('stuff_plus'))
    k_pct      = safe(pitcher_row.get('k_pct'))
    bb_pct     = safe(pitcher_row.get('bb_pct'))

    components = []

    if xfip:
        xfip_mult = xfip / LEAGUE_AVG_XFIP
        components.append((xfip_mult, 0.35))

    if stuff_plus:
        # stuff+ > 100 = better pitcher → lower multiplier for batters
        stuff_mult = LEAGUE_AVG_STUFF / max(stuff_plus, 50)
        components.append((stuff_mult, 0.25))

    if k_pct:
        k_mult = LEAGUE_AVG_K_PCT / max(k_pct, 0.05)
        components.append((k_mult, 0.25))

    if bb_pct:
        bb_mult = bb_pct / LEAGUE_AVG_BB_PCT
        components.append((bb_mult, 0.15))

    if not components:
        return 1.0

    total_wt = sum(w for _, w in components)
    composite = sum(m * w for m, w in components) / total_wt

    return clip(composite, 0.55, 1.65)


# ── Tier 2A-2: Bullpen Quality Multiplier ────────────────────────────────────

def compute_bullpen_mult(rp_rows: list) -> float:
    """
    IP-weighted composite quality of an opposing team's bullpen.
    Returns 0.70-1.40. Lower = tougher for batters.
    Weights: xFIP 50%, K% 30%, BB% 20%  (no Stuff+ — less reliable for relievers)
    Falls back to 1.0 (neutral) if no bullpen data available.
    """
    if not rp_rows:
        return 1.0

    # Deduplicate by player_id — keep row with highest IP (handles load_stats +
    # load_reliever_stats double-rows for the same pitcher)
    seen = {}
    for row in rp_rows:
        pid = row['player_id']
        ip  = safe(row.get('ip'), 0)
        if pid not in seen or ip > safe(seen[pid].get('ip'), 0):
            seen[pid] = row
    rows = list(seen.values())

    num_xfip = den_xfip = 0.0
    num_k    = den_k    = 0.0
    num_bb   = den_bb   = 0.0

    for r in rows:
        ip     = safe(r.get('ip'), 0)
        xfip   = safe(r.get('xfip'))
        k_pct  = safe(r.get('k_pct'))
        bb_pct = safe(r.get('bb_pct'))
        if ip <= 0:
            continue
        if xfip:
            num_xfip += xfip * ip;  den_xfip += ip
        if k_pct:
            num_k    += k_pct * ip; den_k    += ip
        if bb_pct:
            num_bb   += bb_pct * ip; den_bb  += ip

    components = []
    if den_xfip > 0:
        components.append(((num_xfip / den_xfip) / LEAGUE_AVG_XFIP,         0.50))
    if den_k > 0:
        components.append((LEAGUE_AVG_K_PCT / max(num_k / den_k, 0.05),     0.30))
    if den_bb > 0:
        components.append(((num_bb / den_bb) / LEAGUE_AVG_BB_PCT,           0.20))

    if not components:
        return 1.0

    total_wt  = sum(w for _, w in components)
    composite = sum(m * w for m, w in components) / total_wt
    return clip(composite, 0.70, 1.40)


# ── Pitcher Helpers ───────────────────────────────────────────────────────────

def american_to_implied_prob(american_odds: int) -> float:
    if american_odds > 0:
        return 100 / (american_odds + 100)
    return abs(american_odds) / (abs(american_odds) + 100)


def remove_vig(prob_a: float, prob_b: float):
    total = prob_a + prob_b
    return prob_a / total, prob_b / total


def compute_win_prob(odds_row: dict, is_home: bool, ip_per_gs: float = 5.1) -> float:
    """
    Probability of SP getting a win decision.

    Research basis (2022-2024 empirical rates):
      - SPs get a decision in ~67% of starts
      - Of those, ~51% are wins
      - Combined: SP win rate ≈ 34% for a .500 team
      → multiplier = 0.34 / 0.50 = 0.68 (NOT 0.55)

    Also scales by projected IP: deeper starters have higher decision rates.
    League avg start = 5.1 IP (2023-24). Falls back to 0.17 (league avg SP win/GS).
    """
    if not odds_row:
        return clip(0.17 * (ip_per_gs / 5.1), 0.10, 0.45)
    home_ml = odds_row.get('home_ml')
    away_ml = odds_row.get('away_ml')
    if home_ml and away_ml:
        home_raw = american_to_implied_prob(int(home_ml))
        away_raw = american_to_implied_prob(int(away_ml))
        home_prob, away_prob = remove_vig(home_raw, away_raw)
        team_win_prob = home_prob if is_home else away_prob
    else:
        team_win_prob = 0.50
    # Scale by how deep SP is projected to go vs league avg (5.1 IP)
    ip_scale = clip(ip_per_gs / 5.1, 0.70, 1.30)
    return clip(team_win_prob * 0.68 * ip_scale, 0.10, 0.45)


def compute_opp_lineup_quality(lineups: list, batter_stats: dict,
                                batter_splits: dict, opp_team_id: int,
                                pitcher_hand: str, odds_row: dict,
                                is_home: bool) -> float:
    """
    PA-weighted wRC+ of the opposing lineup vs pitcher's handedness.

    Primary signal: actual lineup platoon splits (more granular than Vegas).
    Calibration: blend 70% stats-based / 30% Vegas implied to catch lineup news.

    Returns multiplier: 1.0 = league-avg offense (100 wRC+).
    Higher = tougher for the pitcher.
    """
    opp_batters = [lu for lu in lineups
                   if lu.get('team_id') == opp_team_id and lu.get('batting_order')]

    stats_wrc = None
    if opp_batters:
        total_pa = 0.0
        weighted_wrc = 0.0
        for lu in opp_batters:
            pid   = lu['player_id']
            order = lu.get('batting_order', 5)
            pa_wt = LINEUP_PA.get(order, LEAGUE_AVG_PA)

            # Prefer platoon split vs pitcher hand; fall back to overall wRC+
            wrc = None
            if pitcher_hand:
                split = batter_splits.get(pid, {}).get(pitcher_hand)
                if split:
                    wrc = safe(split.get('wrc_plus'))
            if wrc is None:
                stats = batter_stats.get(pid, {})
                curr  = stats.get(SEASON) or stats.get(SEASON-1) or stats.get(SEASON-2)
                if curr:
                    wrc = safe(curr.get('wrc_plus'))

            if wrc is not None:
                weighted_wrc += wrc * pa_wt
                total_pa     += pa_wt

        if total_pa > 0:
            stats_wrc = weighted_wrc / total_pa

    # Vegas implied for opposing team as calibration signal
    vegas_opp = None
    if odds_row:
        opp_implied = safe(odds_row.get('away_implied' if is_home else 'home_implied'))
        if not opp_implied:
            total = safe(odds_row.get('game_total'))
            opp_implied = total / 2.0 if total else None
        if opp_implied:
            # Convert implied runs to wRC+ scale: 4.5 runs ≈ 100 wRC+
            vegas_opp = (opp_implied / LEAGUE_AVG_IMPLIED) * 100.0

    # Blend: 70% stats lineup / 30% Vegas calibration (or 100% whichever is available)
    if stats_wrc is not None and vegas_opp is not None:
        blended_wrc = stats_wrc * 0.70 + vegas_opp * 0.30
    elif stats_wrc is not None:
        blended_wrc = stats_wrc
    elif vegas_opp is not None:
        blended_wrc = vegas_opp
    else:
        return 1.0

    return clip(blended_wrc / 100.0, 0.65, 1.45)


def compute_park_mult_pitcher(park_row: dict) -> float:
    """
    Pitcher-specific park factor: HR factor weighted more heavily than basic,
    since HRs are the primary driver of ER — the main negative DK scoring term.

    Research: xFIP already normalizes HR/FB, but forward park adjustment should
    use HR-specific factor for ER projection and basic for H/BB.
    Returns a blended multiplier applied to ER projection only.
    """
    if not park_row:
        return 1.0
    hr_factor    = safe(park_row.get('hr_factor'))
    basic_factor = safe(park_row.get('basic_factor'), 100)
    if hr_factor:
        blended = hr_factor * 0.60 + basic_factor * 0.40
    else:
        blended = basic_factor
    return clip(blended / 100.0, 0.80, 1.30)


# ── Pitcher True Talent (Marcel) ──────────────────────────────────────────────

def compute_pitcher_true_talent(stats_by_season: dict, current_season: int) -> dict:
    """
    IP-weighted Marcel average for pitcher skills across last 3 seasons.

    ERA anchor: SIERA (50%) + xFIP (50%)
      - SIERA (RMSE 0.871) outperforms xFIP (RMSE 0.892) because it incorporates
        GB% internally. Blending both captures outcome-based and process-based signal.

    K% anchor: Marcel K% (70%) + SwStr%-derived xK% (30%)
      - SwStr% Y2Y r ≈ 0.73 (more stable than K% itself); stabilizes faster.
      - Formula: xK% ≈ SwStr% × 2.0 + 1.5% (Podhorzer/RotoGraphs, R²=0.89)

    Returns: {era_anchor, k_pct, bb_pct, ip_per_gs}
    """
    # Check if current season has enough IP to be meaningful
    curr = stats_by_season.get(current_season)
    curr_ip = safe(curr.get('ip'), 0) if curr else 0
    use_current = curr_ip >= MIN_IP_PITCHER

    seasons_weights = [
        (current_season,     5 if use_current else 0),
        (current_season - 1, 4),
        (current_season - 2, 3),
    ]

    def ip_weighted(col, league_avg, regression_ip=80):
        num = regression_ip * league_avg
        den = float(regression_ip)
        for yr, wt in seasons_weights:
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
        return num / den if den > regression_ip else league_avg

    # ── ERA anchor: SIERA + xFIP blend ────────────────────────────────────────
    xfip_val  = clip(ip_weighted('xfip',  LEAGUE_AVG_XFIP), 2.50, 6.00)
    siera_val = ip_weighted('siera', LEAGUE_AVG_XFIP)
    siera_val = clip(siera_val, 2.50, 6.00) if siera_val else None

    if siera_val:
        era_anchor = xfip_val * 0.50 + siera_val * 0.50
    else:
        era_anchor = xfip_val  # fall back to xFIP alone if SIERA missing

    # ── K% anchor: Marcel + SwStr%-derived xK% ────────────────────────────────
    # Use regression_ip=120 (vs 80 for ERA) — K% stabilizes slower than ERA,
    # but SwStr% blend already provides stability, so less regression needed
    # than the original 200 IP mass which over-compressed elite K rates.
    marcel_k = clip(ip_weighted('k_pct', LEAGUE_AVG_K_PCT, regression_ip=120), 0.10, 0.40)

    # SwStr% from most recent season with sufficient data (stabilizes fastest)
    swstr_xk = None
    swstr_seasons = [current_season, current_season - 1, current_season - 2] if use_current \
                    else [current_season - 1, current_season - 2]
    for yr in swstr_seasons:
        row = stats_by_season.get(yr)
        if not row:
            continue
        swstr = safe(row.get('swstr_pct'))
        if swstr and swstr > 0.04:  # sanity check: >4% is real data
            # Podhorzer formula (simplified): xK% ≈ SwStr% × 2.0 + 0.015
            swstr_xk = clip(swstr * 2.0 + 0.015, 0.10, 0.45)
            break

    if swstr_xk:
        k_pct = marcel_k * 0.70 + swstr_xk * 0.30
    else:
        k_pct = marcel_k

    bb_pct = clip(ip_weighted('bb_pct', LEAGUE_AVG_BB_PCT), 0.04, 0.18)

    # ── IP per GS — most recent season with ≥5 starts ─────────────────────────
    ip_per_gs = 5.1  # 2023-24 league avg (down from 5.5 older era)
    ipgs_seasons = [current_season, current_season - 1, current_season - 2] if use_current \
                   else [current_season - 1, current_season - 2]
    for yr in ipgs_seasons:
        row = stats_by_season.get(yr)
        if not row:
            continue
        ip = safe(row.get('ip'), 0)
        gs = safe(row.get('gs'), 0)
        if gs and gs >= 5:
            raw_ip_per_gs = clip(ip / gs, 3.0, 6.5)   # cap at 6.5 (modern SP ceiling)
            # Regress 8% toward 5.1 league avg — light smoothing while preserving
            # the real workload gap between aces and back-end starters
            ip_per_gs = raw_ip_per_gs * 0.92 + 5.1 * 0.08
            break

    return {
        'era_anchor': era_anchor,
        'xfip'      : xfip_val,   # kept for transparency in output
        'siera'     : siera_val,
        'k_pct'     : k_pct,
        'bb_pct'    : bb_pct,
        'ip_per_gs' : ip_per_gs,
    }


# ── Pitcher DK Points Conversion ──────────────────────────────────────────────

def project_pitcher_dk_pts(talent: dict, opp_quality: float,
                            park_mult: float, weather_mult: float,
                            win_prob: float) -> dict:
    """
    Project SP DK points for one start.

    DK scoring: +2.25/IP  +2/K  +4/W  -2/ER  -0.6/H  -0.6/BB
    """
    PA_PER_IP    = 4.3   # avg batters faced per inning pitched
    BABIP_AGAINST = 0.297  # league avg BABIP against

    # ── Opposing lineup quality affects ALL stat categories ──────────────
    # opp_quality is wRC+-based: >1.0 = strong lineup, <1.0 = weak lineup
    #
    # IP: strong lineups work counts, drive up pitch counts → shorter outings
    #     30% passthrough — subtle effect
    ip_opp_factor = 1.0 + (1.0 - opp_quality) * 0.30
    base_ip       = talent['ip_per_gs'] * clip(ip_opp_factor, 0.90, 1.10)
    proj_pa       = base_ip * PA_PER_IP

    # Ks: weak lineups strike out more, strong lineups make more contact
    #     35% passthrough — meaningful but pitcher skill still dominates
    k_opp_factor = 1.0 + (1.0 - opp_quality) * 0.35
    proj_ks      = proj_pa * talent['k_pct'] * clip(k_opp_factor, 0.85, 1.18)

    # BBs: strong lineups are more patient / disciplined
    #     20% passthrough — BB% is mostly pitcher-driven
    bb_opp_factor = 1.0 + (opp_quality - 1.0) * 0.20
    proj_bb       = proj_pa * talent['bb_pct'] * clip(bb_opp_factor, 0.88, 1.15)

    # ER: SIERA+xFIP blend (era_anchor) per 9 IP, scaled by opponent and environment
    proj_er = talent['era_anchor'] * base_ip / 9.0 * opp_quality * park_mult * weather_mult
    proj_er = clip(proj_er, 0, base_ip * 1.5)

    # H allowed: contact% of PA faced × BABIP × opp quality scale
    contact_pct = max(0.10, 1.0 - talent['k_pct'] - talent['bb_pct'])
    proj_h      = proj_pa * contact_pct * BABIP_AGAINST * opp_quality
    proj_h      = clip(proj_h, 0, proj_pa * 0.40)

    dk_pts = (
        base_ip  * 2.25 +
        proj_ks  * 2.00 +
        win_prob * 4.00 -
        proj_er  * 2.00 -
        proj_h   * 0.60 -
        proj_bb  * 0.60
    )

    # Skill-scaled calibration: aces (low ERA) get lighter haircut (~5%),
    # back-end starters (high ERA) get heavier haircut (~15%).
    # Research: pitcher MAE=8.28 (target 7.0), bias only -0.56. Tighten range.
    era_ratio = clip(talent['era_anchor'] / LEAGUE_AVG_XFIP, 0.65, 1.55)
    SP_CALIBRATION = clip(0.85 + 0.04 * era_ratio, 0.85, 0.95)
    dk_pts = dk_pts * SP_CALIBRATION

    return {
        'proj_dk_pts'   : round2(dk_pts),
        'proj_floor'    : round2(dk_pts * 0.50),
        'proj_ceiling'  : round2(dk_pts * 1.60),
        'proj_ip'       : round2(base_ip),
        'proj_ks'       : round2(proj_ks),
        'proj_er'       : round2(proj_er),
        'proj_h_allowed': round2(proj_h),
        'proj_bb_allowed': round2(proj_bb),
        'win_prob'      : round2(win_prob),
    }


# ── Tier 2B: Platoon Multiplier ───────────────────────────────────────────────

def compute_platoon_mult(split_row: dict) -> float:
    """
    Returns 0.60-1.60.
    split_row: batter_splits row for the matching pitcher hand (split='L' or 'R').
    Regresses wRC+ toward 100 based on PA sample size.
    """
    if not split_row:
        return 1.0

    wrc_plus = safe(split_row.get('wrc_plus'))
    pa       = safe(split_row.get('pa'), 0)

    if wrc_plus is None:
        # Fall back to wOBA if wRC+ unavailable
        woba = safe(split_row.get('woba'))
        if woba:
            wrc_plus = (woba / LEAGUE_AVG_WOBA) * 100
        else:
            return 1.0

    # Regress toward 100 based on PA (full trust at 300 PA)
    reg_factor  = clip(pa / 300.0, 0.0, 1.0)
    regressed   = wrc_plus * reg_factor + 100.0 * (1.0 - reg_factor)
    return clip(regressed / 100.0, 0.60, 1.60)


# ── Tier 3: Context Multiplier ────────────────────────────────────────────────

def compute_vegas_mult(odds_row: dict, is_home: bool) -> float:
    if not odds_row:
        return 1.0
    implied = safe(odds_row.get('home_implied' if is_home else 'away_implied'))
    if not implied:
        # Fall back to game total / 2 if individual implied unavailable
        total = safe(odds_row.get('game_total'))
        if total:
            implied = total / 2.0
        else:
            return 1.0
    return clip(implied / LEAGUE_AVG_IMPLIED, 0.70, 1.45)


def compute_park_mult(park_row: dict) -> float:
    if not park_row:
        return 1.0
    basic = safe(park_row.get('basic_factor'), 100)
    return clip(basic / 100.0, 0.85, 1.20)


def compute_weather_mult(weather_row: dict) -> float:
    if not weather_row:
        return 1.0

    temp      = safe(weather_row.get('temp_f'), 72)
    wind_spd  = safe(weather_row.get('wind_speed'), 0)
    wind_dir  = (weather_row.get('wind_dir') or '').strip().upper()
    precip    = safe(weather_row.get('precip_pct'), 0)

    # Temperature: ±2% per 10°F from 72°F baseline
    temp_effect = ((temp - 72) / 10) * 0.02

    # Wind: ±0.05 at 15 mph full tailwind/headwind
    wind_effect = 0.0
    if wind_spd and wind_spd > 5:
        if wind_dir in WIND_OUT_DIRS:
            wind_effect = (wind_spd / 15.0) * 0.05
        elif wind_dir in WIND_IN_DIRS:
            wind_effect = -(wind_spd / 15.0) * 0.05

    # Precipitation penalty
    precip_effect = -0.03 if precip and precip > 30 else 0.0

    combined = 1.0 + temp_effect + wind_effect + precip_effect
    return clip(combined, 0.90, 1.12)


def compute_context_mult(vegas_mult, park_mult, weather_mult) -> float:
    """
    Weighted average (not multiplicative) to avoid double-counting.
    Weights: Vegas 62%, Park 18%, Weather 20%
    (Park reduced from 26% — research showed r=-0.170, hurting accuracy)
    """
    combined = (vegas_mult * 0.62) + (park_mult * 0.18) + (weather_mult * 0.20)
    return clip(combined, 0.70, 1.50)


# ── DK Points Conversion ──────────────────────────────────────────────────────

def project_stat_line(talent: dict, final_woba: float,
                      proj_pa: float, lineup_pos: int) -> dict:
    """
    Convert final adjusted wOBA + talent metrics to projected stat line + DK pts.

    Key design principle: scale relative to the PLAYER'S OWN baseline wOBA,
    not league average. This keeps projections grounded in realistic per-game
    outcomes (elite hitters ~7-10 pts, average hitters ~5-7 pts).
    """
    bb_pct  = talent['bb_pct']
    iso     = talent['iso']
    sb_pa   = talent['sb_pa']
    avg     = talent['avg']          # Marcel-weighted batting average
    base_woba = talent['woba']       # Player's true talent wOBA

    # PA components
    proj_bb  = proj_pa * bb_pct
    proj_hbp = proj_pa * 0.010
    proj_ab  = proj_pa - proj_bb - proj_hbp

    # Matchup ratio: how much the projection adjusts the player's own baseline
    # Caps at 1.40x so even elite matchups don't blow up AVG unrealistically
    woba_ratio = clip(final_woba / max(base_woba, 0.200), 0.65, 1.40)

    # Hits: use player's Marcel AVG scaled by matchup ratio (attenuated — AVG
    # doesn't swing as much as power with matchup quality)
    avg_ratio = 1.0 + (woba_ratio - 1.0) * 0.50   # 50% passthrough to AVG
    proj_avg  = clip(avg * avg_ratio, 0.100, 0.320)
    proj_h    = proj_ab * proj_avg

    # HR: ISO/3.5 gives realistic HR/AB rate (ISO=0.190 → 0.054 HR/AB ≈ actual)
    # Power scales more strongly with matchup than AVG does
    hr_per_ab = clip(iso / 3.5, 0.010, 0.080)
    proj_hr   = proj_ab * clip(hr_per_ab * woba_ratio, 0.008, 0.100)

    # Doubles: derived from ISO remainder after HR contribution
    # ISO ≈ 2B/AB + 2×3B/AB + 3×HR/AB → 2B/AB ≈ (ISO - 3×HR/AB) clipped
    two_b_per_ab = clip(iso - 3 * hr_per_ab, 0.008, 0.060)
    proj_2b   = proj_ab * two_b_per_ab * (1.0 + (woba_ratio - 1.0) * 0.60)
    proj_3b   = proj_ab * 0.005
    proj_1b   = max(0, proj_h - proj_2b - proj_3b - proj_hr)

    # Stolen bases
    proj_sb  = proj_pa * sb_pa

    # R: ~25% of times on base score, adjusted by lineup position
    run_mult      = LINEUP_R_MULT.get(lineup_pos, 1.0)
    rbi_mult      = LINEUP_RBI_MULT.get(lineup_pos, 1.0)
    times_on_base = proj_h + proj_bb + proj_hbp
    proj_r        = times_on_base * 0.25 * run_mult

    # RBI: HR drives in ~1.3, non-HR hits drive in ~16%
    proj_rbi = proj_hr * 1.30 + (proj_h - proj_hr) * 0.16 * rbi_mult

    # DK points
    dk_pts = (
        proj_1b  * 3  +
        proj_2b  * 5  +
        proj_3b  * 8  +
        proj_hr  * 10 +
        proj_r   * 2  +
        proj_rbi * 2  +
        proj_bb  * 2  +
        proj_hbp * 2  +
        proj_sb  * 5
    )

    return {
        'proj_dk_pts' : round2(dk_pts),
        'proj_floor'  : round2(dk_pts * 0.70),
        'proj_ceiling': round2(dk_pts * 1.45),
        'proj_pa'     : round2(proj_pa),
        'proj_h'      : round2(proj_h),
        'proj_1b'     : round2(proj_1b),
        'proj_2b'     : round2(proj_2b),
        'proj_3b'     : round2(proj_3b),
        'proj_hr'     : round2(proj_hr),
        'proj_bb'     : round2(proj_bb),
        'proj_r'      : round2(proj_r),
        'proj_rbi'    : round2(proj_rbi),
        'proj_sb'     : round2(proj_sb),
    }


# ── Data Fetching ─────────────────────────────────────────────────────────────

def fetch_today_data(target_date: str) -> dict:
    """Fetch all data needed for projections in bulk."""
    print(f"  Fetching games for {target_date}...")
    games = sb.table('games').select(
        'game_pk,game_date,home_team,away_team,home_team_id,away_team_id,'
        'home_sp_id,away_sp_id,home_sp_hand,away_sp_hand,venue_id'
    ).eq('game_date', target_date).execute().data or []

    game_pks = [g['game_pk'] for g in games]
    print(f"  Games: {len(games)}")

    if not games:
        return {}

    # Lineups
    lineups = sb.table('lineups').select(
        'player_id,game_pk,team_id,batting_order,player_name'
    ).in_('game_pk', game_pks).gte('batting_order', 1).lte('batting_order', 9).execute().data or []
    print(f"  Lineup entries: {len(lineups)}")

    # Batter stats (all 3 seasons)
    player_ids = list({l['player_id'] for l in lineups if l.get('player_id')})
    batter_stats_rows = []
    for i in range(0, len(player_ids), 500):
        chunk = player_ids[i:i+500]
        rows = sb.table('batter_stats').select(
            'player_id,season,pa,woba,xwoba,k_pct,bb_pct,iso,avg,sb,hard_hit_pct,barrel_pct,avg_ev,wrc_plus,full_name,team'
        ).in_('player_id', chunk).in_('season', [SEASON, SEASON-1, SEASON-2]).execute().data or []
        batter_stats_rows.extend(rows)

    # Build batter_stats lookup: {player_id: {season: row}}
    batter_stats = {}
    for row in batter_stats_rows:
        pid = row['player_id']
        yr  = row['season']
        if pid not in batter_stats:
            batter_stats[pid] = {}
        batter_stats[pid][yr] = row

    # Pitcher stats (starting pitchers) — 3 seasons for Marcel + gs/ip for IP/GS calc
    sp_ids = set()
    for g in games:
        if g.get('home_sp_id'): sp_ids.add(g['home_sp_id'])
        if g.get('away_sp_id'): sp_ids.add(g['away_sp_id'])
    pitcher_stats     = {}   # {player_id: current_season_row}  — for batter matchup mult
    pitcher_stats_all = {}   # {player_id: {season: row}}        — for pitcher Marcel
    if sp_ids:
        rows = sb.table('pitcher_stats').select(
            'player_id,season,ip,g,gs,xfip,stuff_plus,k_pct,bb_pct,era,fip,siera,full_name,stats_level,swstr_pct'
        ).in_('player_id', list(sp_ids)).in_('season', [SEASON, SEASON-1, SEASON-2, SEASON-3]).execute().data or []
        for r in rows:
            pid = r['player_id']
            yr  = r['season']
            pitcher_stats_all.setdefault(pid, {})[yr] = r
        # Build current-season map (fallback to most recent) for batter tier-2 mult
        for pid, seasons in pitcher_stats_all.items():
            pitcher_stats[pid] = seasons.get(SEASON) or seasons.get(SEASON-1) or seasons.get(SEASON-2) or seasons.get(SEASON-3)

    # Bullpen quality — IP-weighted composite per team
    # Step 1: get pitcher player_ids on each team from rosters
    all_team_ids = list({g['home_team_id'] for g in games} |
                        {g['away_team_id'] for g in games})
    roster_rows = sb.table('rosters').select('player_id,team_id').in_(
        'team_id', all_team_ids
    ).eq('position_type', 'Pitcher').execute().data or []

    team_pitcher_ids = {}
    for r in roster_rows:
        tid = r['team_id']
        team_pitcher_ids.setdefault(tid, []).append(r['player_id'])

    # Step 2: fetch reliever stats (gs <= 2, g >= 5) for those pitchers
    all_rp_ids = [pid for pids in team_pitcher_ids.values() for pid in pids]
    rp_stat_rows = []
    if all_rp_ids:
        for i in range(0, len(all_rp_ids), 500):
            chunk = all_rp_ids[i:i+500]
            chunk_rows = sb.table('pitcher_stats').select(
                'player_id,ip,g,gs,xfip,k_pct,bb_pct'
            ).in_('player_id', chunk).eq('season', SEASON).lte('gs', 2).gte('g', 5).execute().data or []
            rp_stat_rows.extend(chunk_rows)

    # Step 3: index reliever rows by player_id for lookup
    rp_stat_map = {}
    for r in rp_stat_rows:
        rp_stat_map.setdefault(r['player_id'], []).append(r)

    # Step 4: compute bullpen_mult per team
    bullpen_quality = {}
    for tid, pids in team_pitcher_ids.items():
        team_rp = [r for pid in pids for r in rp_stat_map.get(pid, [])]
        bullpen_quality[tid] = compute_bullpen_mult(team_rp)

    print(f"  Bullpen quality computed for {len(bullpen_quality)} teams")

    # Batter splits — use current season once avg PA >= 75, else fall back to prior season
    # Threshold of 75 PA is typically reached ~4-5 weeks into the season.
    # Chunked into <=150-ID batches to avoid silent URL-too-long failures.
    PA_THRESHOLD = 75
    batter_splits = {}
    if player_ids:
        def _fetch_splits(season):
            rows = []
            for i in range(0, len(player_ids), 150):
                chunk = player_ids[i:i + 150]
                chunk_rows = sb.table('batter_splits').select(
                    'player_id,split,pa,wrc_plus,woba,k_pct,bb_pct'
                ).in_('player_id', chunk).eq('season', season).execute().data or []
                rows.extend(chunk_rows)
            return rows

        current_rows = _fetch_splits(SEASON)
        if current_rows:
            avg_pa = sum(r.get('pa') or 0 for r in current_rows) / len(current_rows)
        else:
            avg_pa = 0

        if avg_pa >= PA_THRESHOLD:
            split_rows = current_rows
            print(f"  Batter splits: using {SEASON} (avg PA={avg_pa:.0f}, {len(split_rows)} rows)")
        else:
            fallback = SEASON - 1
            if current_rows:
                print(f"  Batter splits: {SEASON} sample too thin (avg PA={avg_pa:.0f}) — using {fallback}")
            else:
                print(f"  Batter splits: no {SEASON} data yet — using {fallback}")
            split_rows = _fetch_splits(fallback)
            print(f"  Batter splits: loaded {len(split_rows)} rows from {fallback}")

        for r in split_rows:
            pid = r['player_id']
            if pid not in batter_splits:
                batter_splits[pid] = {}
            batter_splits[pid][r['split']] = r

    # Game odds — include moneylines for pitcher win probability
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
            'game_pk,temp_f,wind_speed,wind_dir,precip_pct,is_outdoor'
        ).in_('game_pk', game_pks).execute().data or []
        weather = {r['game_pk']: r for r in rows}

    return {
        'games'            : games,
        'lineups'          : lineups,
        'batter_stats'     : batter_stats,
        'pitcher_stats'    : pitcher_stats,
        'pitcher_stats_all': pitcher_stats_all,
        'batter_splits'    : batter_splits,
        'odds'             : odds,
        'park_factors'     : park_factors,
        'weather'          : weather,
        'bullpen_quality'  : bullpen_quality,
    }


# ── Main Projection Loop ──────────────────────────────────────────────────────

def run():
    target_date = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] != '--date' else None
    if not target_date and '--date' in sys.argv:
        idx = sys.argv.index('--date')
        target_date = sys.argv[idx + 1] if idx + 1 < len(sys.argv) else None
    if not target_date:
        target_date = str(date.today())

    print(f"\nProjection Engine — {target_date}")
    print("=" * 50)

    data = fetch_today_data(target_date)
    if not data or not data.get('games'):
        print("  No games found — exiting")
        return

    # Build game lookup
    game_map = {g['game_pk']: g for g in data['games']}

    records = []
    skipped = 0
    computed_at = datetime.now(timezone.utc).isoformat()

    for lu in data['lineups']:
        player_id    = lu.get('player_id')
        game_pk      = lu.get('game_pk')
        batting_order = lu.get('batting_order') or 9
        team_id      = lu.get('team_id')

        if not player_id or not game_pk:
            skipped += 1
            continue

        game = game_map.get(game_pk)
        if not game:
            skipped += 1
            continue

        is_home = (team_id == game.get('home_team_id'))

        # Opposing pitcher
        if is_home:
            opp_sp_id   = game.get('away_sp_id')
            opp_sp_hand = game.get('away_sp_hand')
        else:
            opp_sp_id   = game.get('home_sp_id')
            opp_sp_hand = game.get('home_sp_hand')

        pitcher_row  = data['pitcher_stats'].get(opp_sp_id) if opp_sp_id else None
        splits_by_hand = data['batter_splits'].get(player_id, {})
        # Match split to pitcher hand: pitcher R → use batter's split 'R' (vs RHP)
        split_row    = splits_by_hand.get(opp_sp_hand) if opp_sp_hand else None

        stats_by_yr  = data['batter_stats'].get(player_id, {})
        odds_row     = data['odds'].get(game_pk)
        park_row     = data['park_factors'].get(game.get('venue_id'))
        weather_row  = data['weather'].get(game_pk)

        # ── Tier 1
        # No batter stats = new callup / prospect not yet in FanGraphs.
        # Fall back to league-average talent so they still get a projection row
        # and the salary + projection pills show in the UI.
        no_stats = not stats_by_yr
        if no_stats:
            talent = {
                'woba'  : LEAGUE_AVG_WOBA,
                'k_pct' : LEAGUE_AVG_K_PCT,
                'bb_pct': LEAGUE_AVG_BB_PCT,
                'iso'   : 0.155,
                'sb_pa' : 0.023,
                'avg'   : 0.248,
            }
        else:
            talent = compute_true_talent(stats_by_yr, SEASON)
        base_woba = talent['woba']

        # ── Tier 2
        # SP covers ~60% of batter PA (~5.5 IP avg); bullpen covers remaining 40%
        SP_SHARE   = 0.60
        opp_team_id = game.get('away_team_id') if is_home else game.get('home_team_id')
        sp_mult    = compute_pitcher_mult(pitcher_row)
        bull_mult  = data['bullpen_quality'].get(opp_team_id, 1.0)
        pitcher_mult = 1.0 + (sp_mult - 1.0) * SP_SHARE + (bull_mult - 1.0) * (1.0 - SP_SHARE)
        pitcher_mult = clip(pitcher_mult, 0.60, 1.55)

        platoon_mult = compute_platoon_mult(split_row)

        pa_mult   = LINEUP_PA.get(batting_order, LEAGUE_AVG_PA) / LEAGUE_AVG_PA
        proj_pa   = LEAGUE_AVG_PA * pa_mult

        # Cap combined Tier 2 multiplier — prevents two uncapped values compounding
        tier2_combined = clip(pitcher_mult * platoon_mult, 0.60, 1.45)
        matchup_woba = clip(base_woba * tier2_combined, 0.150, 0.520)

        # ── Tier 3
        # Skip weather mult for indoor venues (no weather effect)
        wx_row = weather_row
        if weather_row and weather_row.get('is_outdoor') is False:
            wx_row = None

        vegas_mult   = compute_vegas_mult(odds_row, is_home)
        park_mult    = compute_park_mult(park_row)
        weather_mult = compute_weather_mult(wx_row)
        context_mult = compute_context_mult(vegas_mult, park_mult, weather_mult)

        final_woba   = clip(matchup_woba * context_mult, 0.150, 0.560)

        # ── Stat line + DK points
        stat_line = project_stat_line(talent, final_woba, proj_pa, batting_order)

        # Name/team from most recent stats row; fall back to lineup name for no-stats players
        curr_stats = stats_by_yr.get(SEASON) or stats_by_yr.get(SEASON-1) or stats_by_yr.get(SEASON-2)

        records.append({
            'player_id'    : player_id,
            'game_pk'      : game_pk,
            'game_date'    : target_date,
            'full_name'    : (curr_stats.get('full_name') if curr_stats else None) or lu.get('player_name'),
            'team'         : curr_stats.get('team') if curr_stats else None,
            'batting_order': batting_order,
            'is_pitcher'   : False,
            'base_woba'    : round2(base_woba),
            'matchup_woba' : round2(matchup_woba),
            'final_woba'   : round2(final_woba),
            'pitcher_mult' : round2(pitcher_mult),
            'platoon_mult' : round2(platoon_mult),
            'context_mult' : round2(context_mult),
            'vegas_mult'   : round2(vegas_mult),
            'park_mult'    : round2(park_mult),
            'weather_mult' : round2(weather_mult),
            'computed_at'  : computed_at,
            # Pitcher-specific fields null for batters
            'proj_ip': None, 'proj_ks': None, 'proj_er': None,
            'proj_h_allowed': None, 'proj_bb_allowed': None, 'win_prob': None,
            **stat_line,
            # Local-only keys for sample output — stripped before upsert
            '__sp_mult'  : round2(sp_mult),
            '__bull_mult': round2(bull_mult),
        })

    print(f"\n  Hitters computed: {len(records)} | Skipped: {skipped}")

    # ── Pitcher Projection Loop ────────────────────────────────────────────────
    skipped_p = 0
    pitcher_records = []

    for game in data['games']:
        game_pk  = game['game_pk']
        venue_id = game.get('venue_id')
        odds_row    = data['odds'].get(game_pk)
        park_row    = data['park_factors'].get(venue_id)
        weather_row = data['weather'].get(game_pk)

        wx_row = weather_row
        if weather_row and weather_row.get('is_outdoor') is False:
            wx_row = None

        park_mult    = compute_park_mult(park_row)
        weather_mult = compute_weather_mult(wx_row)

        for role, sp_id, pitcher_team in [
            ('home', game.get('home_sp_id'), game.get('home_team')),
            ('away', game.get('away_sp_id'), game.get('away_team')),
        ]:
            if not sp_id:
                skipped_p += 1
                continue

            stats_by_yr = data['pitcher_stats_all'].get(sp_id, {})
            if not stats_by_yr:
                skipped_p += 1
                continue

            # ── Determine stats_level + MiLB discount ─────────────────────────
            # Check if any rows are genuine MLB stats and how much IP we have.
            mlb_ip = sum(
                safe(r.get('ip'), 0) for r in stats_by_yr.values()
                if (r.get('stats_level') or 'MLB') == 'MLB'
            )
            # Find the non-MLB level if present (highest level in the data)
            milb_level = None
            for r in stats_by_yr.values():
                lv = r.get('stats_level') or 'MLB'
                if lv != 'MLB':
                    milb_level = lv
                    break  # all MiLB rows for a player share the same level

            if mlb_ip >= 50:
                stats_level   = 'MLB'
            elif mlb_ip >= 20:
                stats_level   = 'MLB_THIN'
            elif milb_level:
                stats_level   = milb_level
            else:
                stats_level   = 'MLB_THIN'
            milb_discount = 1.0

            is_home = (role == 'home')
            sp_hand = game.get('home_sp_hand' if is_home else 'away_sp_hand')
            opp_team_id = game.get('away_team_id' if is_home else 'home_team_id')

            talent = compute_pitcher_true_talent(stats_by_yr, SEASON)

            # Opposing lineup quality: PA-weighted wRC+ vs pitcher hand (70%)
            # blended with Vegas implied for opp team (30%) as calibration
            opp_qual = compute_opp_lineup_quality(
                data['lineups'], data['batter_stats'], data['batter_splits'],
                opp_team_id, sp_hand, odds_row, is_home
            )

            # Win prob: team moneyline × 0.68 × IP-scale (research-corrected)
            win_prob = compute_win_prob(odds_row, is_home, talent['ip_per_gs'])

            # Park factor: HR-weighted for ER term (primary negative scoring driver)
            park_mult_p = compute_park_mult_pitcher(park_row)

            stat_line = project_pitcher_dk_pts(
                talent, opp_qual, park_mult_p, weather_mult, win_prob
            )

            # Apply MiLB competition discount on top of SP_CALIBRATION
            if milb_discount < 1.0:
                for k in ('proj_dk_pts', 'proj_floor', 'proj_ceiling', 'proj_ks', 'proj_er'):
                    if stat_line.get(k) is not None:
                        stat_line[k] = round(stat_line[k] * milb_discount, 2)

            curr = stats_by_yr.get(SEASON) or stats_by_yr.get(SEASON-1) or stats_by_yr.get(SEASON-2)
            full_name = (curr or {}).get('full_name') or next(
                (r.get('full_name') for r in stats_by_yr.values() if r.get('full_name')), '?'
            )

            pitcher_records.append({
                'player_id'    : sp_id,
                'game_pk'      : game_pk,
                'game_date'    : target_date,
                'full_name'    : full_name,
                'team'         : pitcher_team,
                'batting_order': None,
                'is_pitcher'   : True,
                'stats_level'  : stats_level,
                # Batter-specific fields null for pitchers
                'base_woba': None, 'matchup_woba': None, 'final_woba': None,
                'pitcher_mult': None, 'platoon_mult': None, 'context_mult': None,
                'vegas_mult'   : None,
                'park_mult'    : round2(park_mult_p),
                'weather_mult' : round2(weather_mult),
                'computed_at'  : computed_at,
                # Batter stat line fields null for pitchers
                'proj_pa': None, 'proj_h': None, 'proj_1b': None, 'proj_2b': None,
                'proj_3b': None, 'proj_hr': None, 'proj_bb': None,
                'proj_r': None, 'proj_rbi': None, 'proj_sb': None,
                # Debug keys (stripped before upsert)
                '__opp_qual'  : round2(opp_qual),
                '__win_prob'  : round2(win_prob),
                '__era_anchor': round2(talent['era_anchor']),
                '__siera'     : round2(talent['siera']) if talent['siera'] else None,
                **stat_line,
            })

    print(f"  Pitchers computed: {len(pitcher_records)} | Skipped: {skipped_p}")

    all_records = records + pitcher_records
    print(f"\n  Total: {len(all_records)} projections")

    if not all_records:
        print("  Nothing to upsert.")
        return

    # Strip local-only debug keys before upsert
    DB_SKIP = {'__sp_mult', '__bull_mult', '__opp_qual', '__win_prob', '__era_anchor', '__siera'}
    db_records = [{k: v for k, v in r.items() if k not in DB_SKIP} for r in all_records]

    # Deduplicate by (player_id, game_pk) — keep last occurrence
    seen_pk = {}
    for r in db_records:
        seen_pk[(r['player_id'], r['game_pk'])] = r
    db_records = list(seen_pk.values())
    dupes = len(all_records) - len(db_records)
    if dupes:
        print(f"  Deduped {dupes} duplicate (player_id, game_pk) rows before upsert")

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

    print(f"\nProjections complete. {uploaded} records upserted.")

    # Sample output — top 5 hitters and all pitchers
    top_hitters = sorted(
        [r for r in all_records if not r.get('is_pitcher')],
        key=lambda r: r.get('proj_dk_pts') or 0, reverse=True
    )[:5]
    print("\n  Top 5 hitter projections:")
    for r in top_hitters:
        pts      = r.get('proj_dk_pts')   or 0
        name     = r.get('full_name')     or '?'
        base_w   = r.get('base_woba')     or 0
        final_w  = r.get('final_woba')    or 0
        sp_m     = r.get('__sp_mult')     or 0
        bull_m   = r.get('__bull_mult')   or 0
        pit_m    = r.get('pitcher_mult')  or 0
        plat_m   = r.get('platoon_mult')  or 0
        ctx_m    = r.get('context_mult')  or 0
        print(f"    {name:25s}  {pts:5.1f} pts  "
              f"(wOBA: {base_w:.3f} -> {final_w:.3f}  "
              f"sp={sp_m:.2f} bull={bull_m:.2f} "
              f"pit={pit_m:.2f} plat={plat_m:.2f} "
              f"ctx={ctx_m:.2f})")

    sp_sorted = sorted(
        [r for r in all_records if r.get('is_pitcher')],
        key=lambda r: r.get('proj_dk_pts') or 0, reverse=True
    )
    print(f"\n  Pitcher projections ({len(sp_sorted)} SPs):")
    for r in sp_sorted:
        pts       = r.get('proj_dk_pts')   or 0
        name      = r.get('full_name')     or '?'
        proj_ip   = r.get('proj_ip')       or 0
        proj_ks   = r.get('proj_ks')       or 0
        proj_er   = r.get('proj_er')       or 0
        win_prob  = r.get('__win_prob')    or 0
        opp_qual  = r.get('__opp_qual')    or 0
        siera     = r.get('__siera')       or '—'
        era_anch  = r.get('__era_anchor')  or 0
        lv  = r.get('stats_level') or 'MLB'
        lv_tag = f'  [{lv}]' if lv != 'MLB' else ''
        print(f"    {name:25s}  {pts:5.1f} pts  "
              f"IP={proj_ip:.1f}  K={proj_ks:.1f}  "
              f"ER={proj_er:.1f}  W%={win_prob:.2f}  "
              f"opp={opp_qual:.2f}  "
              f"SIERA={siera}  xFIP+SIERA={era_anch:.2f}{lv_tag}")


if __name__ == '__main__':
    run()
