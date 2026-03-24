#!/usr/bin/env python3
sys_path_fix = __import__('sys'); sys_path_fix.stdout.reconfigure(encoding='utf-8', line_buffering=True)
"""
validate_pitcher_projections.py — SlateHub SP Projection Validator

Two validation modes:
  Option 1 (default): Compare proj_ks / proj_ip against Vegas pitcher prop O/U lines
                       fetched from The Odds API.  Most accurate when lines are posted
                       (typically 2-3 days before game time for regular season).

  Option 2 (fallback): Analytical benchmark validation when no prop lines are available
                        (spring training, too far out, etc.).  Compares each component
                        to league-average expectations and flags outliers.

Usage:
  py -3.12 validate_pitcher_projections.py                   # today, auto mode
  py -3.12 validate_pitcher_projections.py --date 2026-03-26 # specific date
  py -3.12 validate_pitcher_projections.py --mode analytical  # force Option 2
  py -3.12 validate_pitcher_projections.py --mode props       # force Option 1
"""

import os, sys, math, json, requests
from datetime import date
from difflib import SequenceMatcher
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()
sb        = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])
ODDS_KEY  = os.environ.get("ODDS_API_KEY", "383c454baf5a3d8e28347691408d4a9d")
ODDS_BASE = "https://api.the-odds-api.com/v4"

# ── League-average benchmarks (2023-24 MLB) ───────────────────────────────────
LG_IP     = 5.10   # avg IP per start
LG_K_PCT  = 0.225  # avg K% (22.5%)
LG_BB_PCT = 0.082  # avg BB%
LG_ERA    = 3.90   # avg ERA anchor (xFIP-based)
LG_PA_IP  = 4.30   # PA per inning
LG_BABIP  = 0.297

# What a league-average SP projects to (derived)
LG_PA     = LG_IP * LG_PA_IP             # 21.93
LG_KS     = LG_PA * LG_K_PCT             # 4.93
LG_BB     = LG_PA * LG_BB_PCT            # 1.80
LG_ER     = LG_ERA * LG_IP / 9.0         # 2.21
LG_H      = LG_PA * (1 - LG_K_PCT - LG_BB_PCT) * LG_BABIP  # 4.51
LG_WIN    = 0.17
LG_DK     = (LG_IP*2.25 + LG_KS*2.0 + LG_WIN*4.0
             - LG_ER*2.0 - LG_H*0.6 - LG_BB*0.6)   # ≈ 13.8

# Published DFS industry SP tier benchmarks (2025 season averages, DK scoring)
# Source: multi-site aggregation of top-500 SP starts by tier
TIER_BENCHMARKS = {
    'ace'   : {'dk': (20, 24), 'ks': (7.0, 9.0), 'ip': (5.8, 6.5)},
    'good'  : {'dk': (15, 19), 'ks': (5.0, 7.0), 'ip': (5.2, 6.0)},
    'avg'   : {'dk': (11, 15), 'ks': (3.5, 5.5), 'ip': (4.5, 5.5)},
    'below' : {'dk': ( 7, 12), 'ks': (2.5, 4.5), 'ip': (4.0, 5.2)},
}

# ── Helpers ───────────────────────────────────────────────────────────────────

def name_sim(a, b):
    """Fuzzy name similarity 0-1."""
    return SequenceMatcher(None, a.lower().strip(), b.lower().strip()).ratio()

def best_name_match(name, candidates, threshold=0.72):
    """Return best matching candidate name or None."""
    best, best_score = None, 0
    for c in candidates:
        s = name_sim(name, c)
        if s > best_score:
            best, best_score = c, s
    return best if best_score >= threshold else None

def pct_diff(our, their):
    """Return signed % difference: (our - their) / their * 100."""
    if not their:
        return None
    return (our - their) / their * 100

def flag(val, threshold_pct=20):
    """Return ⚠ if val is more than threshold_pct above zero."""
    if val is None:
        return ''
    return ' ⚠' if val > threshold_pct else (' ✓' if val < -5 else '')

def tier(dk_pts):
    if dk_pts >= 20: return 'ace'
    if dk_pts >= 15: return 'good'
    if dk_pts >= 11: return 'avg'
    return 'below'

# ── Data Fetching ─────────────────────────────────────────────────────────────

def fetch_our_projections(target_date):
    rows = (sb.table('player_projections')
              .select('player_id,full_name,team,proj_dk_pts,proj_ip,proj_ks,proj_er,proj_h_allowed,proj_bb_allowed,win_prob')
              .eq('is_pitcher', True)
              .eq('game_date', target_date)
              .order('proj_dk_pts', desc=True)
              .execute().data or [])
    return rows

def fetch_pitcher_stats(player_ids):
    """Fetch latest pitcher stats for context."""
    rows = (sb.table('pitcher_stats')
              .select('player_id,era,xfip,siera,k_pct,bb_pct,ip,gs,stuff_plus')
              .in_('player_id', player_ids)
              .eq('season', 2025)
              .execute().data or [])
    return {r['player_id']: r for r in rows}

def fetch_mlb_events():
    """Fetch upcoming MLB events from Odds API."""
    url = f"{ODDS_BASE}/sports/baseball_mlb/events"
    try:
        r = requests.get(url, params={'apiKey': ODDS_KEY}, timeout=10)
        return r.json() if r.ok else []
    except Exception:
        return []

def fetch_pitcher_props(event_id):
    """Fetch pitcher_strikeouts market for one event."""
    url = f"{ODDS_BASE}/sports/baseball_mlb/events/{event_id}/odds"
    try:
        r = requests.get(url, params={
            'apiKey': ODDS_KEY,
            'regions': 'us',
            'markets': 'pitcher_strikeouts',
            'oddsFormat': 'american',
        }, timeout=10)
        if not r.ok:
            return {}
        data = r.json()
        # Build: {pitcher_name: {'over': point, 'under': point}}
        props = {}
        for book in (data.get('bookmakers') or []):
            for mkt in book.get('markets', []):
                if mkt['key'] != 'pitcher_strikeouts':
                    continue
                for outcome in mkt.get('outcomes', []):
                    name = outcome.get('description', '')
                    side = outcome.get('name', '').lower()  # 'over' or 'under'
                    pt   = outcome.get('point')
                    if name and pt is not None:
                        if name not in props:
                            props[name] = {}
                        # Keep first book's line (DraftKings preferred)
                        if side not in props[name]:
                            props[name][side] = pt
        return props
    except Exception:
        return {}

def build_prop_map(target_date, our_pitchers):
    """
    Attempt to build {our_full_name -> {'over': K_line}} from Odds API.
    Returns (prop_map, games_found, props_found).
    """
    events = fetch_mlb_events()
    if not events:
        return {}, 0, 0

    # Filter to events on or near target_date (props usually day-of or day-before)
    target_events = [e for e in events
                     if e.get('commence_time', '')[:10] == target_date]

    if not target_events:
        return {}, 0, 0

    our_names = [p['full_name'] for p in our_pitchers if p.get('full_name')]
    prop_map  = {}
    props_found = 0

    for event in target_events:
        raw_props = fetch_pitcher_props(event['id'])
        if not raw_props:
            continue
        props_found += len(raw_props)
        # Match Odds API names → our names
        for odds_name, sides in raw_props.items():
            match = best_name_match(odds_name, our_names)
            if match and 'over' in sides:
                prop_map[match] = {
                    'k_line': sides['over'],
                    'odds_name': odds_name,
                }

    return prop_map, len(target_events), props_found

# ── Option 1: Props Validation ────────────────────────────────────────────────

def validate_with_props(pitchers, prop_map, stats_map):
    print("\n" + "═"*80)
    print("  OPTION 1 — Vegas Prop Line Comparison (pitcher_strikeouts O/U)")
    print("═"*80)
    print(f"\n{'Pitcher':<26} {'Team':<5} {'Our IP':>7} {'Our K':>6} {'K Line':>7} "
          f"{'K Δ':>6} {'K Δ%':>6} {'Our DK':>8} {'ER':>5}")
    print("-"*80)

    matched = [p for p in pitchers if p['full_name'] in prop_map]
    unmatched = [p for p in pitchers if p['full_name'] not in prop_map]

    k_deltas = []

    for p in matched:
        name     = p['full_name'] or '—'
        pm       = prop_map[name]
        k_line   = pm['k_line']
        our_k    = p['proj_ks']  or 0
        our_ip   = p['proj_ip']  or 0
        our_dk   = p['proj_dk_pts'] or 0
        our_er   = p['proj_er']  or 0
        k_delta  = our_k - k_line
        k_delta_pct = pct_diff(our_k, k_line)
        k_deltas.append(k_delta)

        warn = ' ⚠' if k_delta > 1.5 else (' ✓' if k_delta < -0.5 else '')
        print(f"{name:<26} {p.get('team',''):<5} {our_ip:>7.1f} {our_k:>6.1f} "
              f"{k_line:>7.1f} {k_delta:>+6.2f} {k_delta_pct:>+5.0f}%{warn} "
              f"{our_dk:>7.1f} {our_er:>5.2f}")

    if k_deltas:
        avg_delta = sum(k_deltas) / len(k_deltas)
        rec_scale = max(0.75, min(1.0, 1.0 - (avg_delta / (LG_KS + avg_delta))))
        print("-"*80)
        print(f"\n  Matched {len(matched)}/{len(pitchers)} pitchers to prop lines")
        print(f"  Avg K delta: {avg_delta:+.2f}  (our K - market line)")
        print(f"  Recommended K% scaling factor: {rec_scale:.2f}")
        if avg_delta > 0.8:
            print(f"  ⚠ Model projects {avg_delta:.1f} more Ks/start than market on average")
            print(f"    → Consider multiplying K% by {rec_scale:.2f} in compute_pitcher_true_talent()")
        elif avg_delta < -0.5:
            print(f"  ✓ Model is conservative on Ks vs market lines")
    else:
        print("\n  No pitchers matched to prop lines.")

    if unmatched:
        print(f"\n  Unmatched (no prop line found): {', '.join(p['full_name'] for p in unmatched[:8])}")

    return k_deltas

# ── Option 2: Analytical Validation ──────────────────────────────────────────

def validate_analytical(pitchers, stats_map):
    print("\n" + "═"*80)
    print("  OPTION 2 — Analytical Benchmark Validation (no prop lines available)")
    print(f"  League avg SP baseline: {LG_IP:.1f} IP | {LG_KS:.1f} K | {LG_ER:.2f} ER | {LG_DK:.1f} DK pts")
    print("═"*80)

    print(f"\n{'Pitcher':<26} {'IP':>5} {'K':>5} {'ER':>5} {'DK':>6}  "
          f"{'IP Δ%':>6} {'K Δ%':>6} {'ER Δ%':>6} {'DK Δ%':>6}  Tier  Flags")
    print("-"*100)

    issues = []

    for p in pitchers:
        name   = (p['full_name'] or '?')[:25]
        our_ip = p['proj_ip']  or 0
        our_k  = p['proj_ks']  or 0
        our_er = p['proj_er']  or 0
        our_dk = p['proj_dk_pts'] or 0
        pid    = p['player_id']
        stats  = stats_map.get(pid, {})

        ip_d  = pct_diff(our_ip, LG_IP)
        k_d   = pct_diff(our_k,  LG_KS)
        er_d  = pct_diff(our_er, LG_ER)
        dk_d  = pct_diff(our_dk, LG_DK)

        # Flag outliers
        flags = []
        if ip_d and ip_d > 25:
            flags.append(f'IP+{ip_d:.0f}%')
        if k_d and k_d > 30:
            flags.append(f'K+{k_d:.0f}%')
        if er_d and er_d < -40:
            flags.append(f'ER low ({our_er:.2f})')
        if dk_d and dk_d > 40:
            flags.append(f'DK+{dk_d:.0f}%')

        # Real K% from stats for context
        kpct_real = stats.get('k_pct')
        kpct_str  = f" [K%={kpct_real*100:.0f}%]" if kpct_real else ''

        flag_str = ', '.join(flags)
        t        = tier(our_dk)

        print(f"{name:<26} {our_ip:>5.1f} {our_k:>5.1f} {our_er:>5.2f} {our_dk:>6.1f}  "
              f"{ip_d:>+5.0f}% {k_d:>+5.0f}% {er_d:>+5.0f}% {dk_d:>+5.0f}%  "
              f"{t:<6} {flag_str}{kpct_str}")

        if flags:
            issues.append({'name': name, 'dk': our_dk, 'ip': our_ip, 'k': our_k, 'er': our_er, 'flags': flags})

    # Summary & recommendations
    avg_dk = sum(p['proj_dk_pts'] or 0 for p in pitchers) / max(len(pitchers), 1)
    avg_ip = sum(p['proj_ip']     or 0 for p in pitchers) / max(len(pitchers), 1)
    avg_k  = sum(p['proj_ks']     or 0 for p in pitchers) / max(len(pitchers), 1)

    print("-"*100)
    print(f"\n  AVERAGES across {len(pitchers)} pitchers:")
    print(f"    IP:  {avg_ip:.2f}  (league avg {LG_IP:.1f}  | delta {avg_ip-LG_IP:+.2f})")
    print(f"    K:   {avg_k:.2f}  (league avg {LG_KS:.1f}  | delta {avg_k-LG_KS:+.2f})")
    print(f"    DK:  {avg_dk:.1f}  (league avg {LG_DK:.1f} | delta {avg_dk-LG_DK:+.1f})")

    print(f"\n  CALIBRATION RECOMMENDATIONS:")

    ip_rec = max(0.80, min(1.0, LG_IP / avg_ip)) if avg_ip > LG_IP else 1.0
    k_rec  = max(0.75, min(1.0, LG_KS  / avg_k))  if avg_k  > LG_KS  else 1.0
    dk_rec = max(0.75, min(1.0, LG_DK  / avg_dk)) if avg_dk > LG_DK  else 1.0

    if ip_rec < 1.0:
        print(f"    IP regression: {ip_rec:.2f} toward {LG_IP} league avg  "
              f"(reduces avg {avg_ip:.1f} → {avg_ip*ip_rec:.1f})")
    else:
        print(f"    IP: ✓ reasonable (avg {avg_ip:.1f})")

    if k_rec < 1.0:
        print(f"    K% scale:      {k_rec:.2f}                           "
              f"(reduces avg {avg_k:.1f} K → {avg_k*k_rec:.1f} K)")
    else:
        print(f"    K:  ✓ reasonable (avg {avg_k:.1f})")

    if dk_rec < 1.0:
        print(f"    Global SP cal: {dk_rec:.2f}                           "
              f"(reduces avg {avg_dk:.1f} → {avg_dk*dk_rec:.1f} DK pts)")
    else:
        print(f"    DK: ✓ reasonable (avg {avg_dk:.1f})")

    if issues:
        print(f"\n  ⚠ {len(issues)} pitchers with outlier components:")
        for i in issues:
            print(f"    {i['name']:<26} DK:{i['dk']:.1f}  IP:{i['ip']:.1f}  K:{i['k']:.1f}  "
                  f"ER:{i['er']:.2f}  → {', '.join(i['flags'])}")

# ── Per-Pitcher Component Breakdown ──────────────────────────────────────────

def print_component_breakdown(pitchers):
    print("\n" + "═"*80)
    print("  PER-PITCHER COMPONENT BREAKDOWN  (DK pts by scoring category)")
    print("═"*80)
    print(f"\n{'Pitcher':<26} {'IP pts':>7} {'K pts':>7} {'Win pts':>8} "
          f"{'ER pts':>8} {'H pts':>7} {'BB pts':>7} {'Total':>8}")
    print("-"*80)

    for p in pitchers:
        name    = (p['full_name'] or '?')[:25]
        ip_pts  = (p['proj_ip']        or 0) * 2.25
        k_pts   = (p['proj_ks']        or 0) * 2.00
        win_pts = (p['win_prob']        or 0) * 4.00
        er_pts  = (p['proj_er']        or 0) * 2.00
        h_pts   = (p.get('proj_h_allowed') or 0) * 0.60
        bb_pts  = (p.get('proj_bb_allowed') or 0) * 0.60
        total   = ip_pts + k_pts + win_pts - er_pts - h_pts - bb_pts

        print(f"{name:<26} {ip_pts:>7.2f} {k_pts:>7.2f} {win_pts:>8.2f} "
              f"{-er_pts:>8.2f} {-h_pts:>7.2f} {-bb_pts:>7.2f} {total:>8.2f}")

# ── Main ──────────────────────────────────────────────────────────────────────

def run():
    target_date = str(date.today())
    force_mode  = None

    args = sys.argv[1:]
    if '--date' in args:
        idx = args.index('--date')
        if idx + 1 < len(args):
            target_date = args[idx + 1]
    if '--mode' in args:
        idx = args.index('--mode')
        if idx + 1 < len(args):
            force_mode = args[idx + 1]

    print(f"\n{'='*80}")
    print(f"  SlateHub — SP Projection Validator | {target_date}")
    print(f"{'='*80}")

    # ── Fetch our projections
    pitchers = fetch_our_projections(target_date)
    if not pitchers:
        print(f"\n  No pitcher projections found for {target_date}.")
        print("  Run: py -3.12 compute_projections.py --date " + target_date)
        return

    print(f"\n  Loaded {len(pitchers)} pitcher projections from player_projections.")

    # ── Fetch pitcher stats for context
    pids      = [p['player_id'] for p in pitchers if p.get('player_id')]
    stats_map = fetch_pitcher_stats(pids) if pids else {}

    # ── Try Option 1 unless forced otherwise
    mode = force_mode
    prop_map = {}

    if mode != 'analytical':
        print("  Checking Odds API for pitcher prop lines...")
        prop_map, events_found, props_raw = build_prop_map(target_date, pitchers)
        print(f"  Events on {target_date}: {events_found} | Raw prop entries: {props_raw} | Matched: {len(prop_map)}")

        if prop_map:
            mode = 'props'
        else:
            if mode == 'props':
                print("  ⚠ --mode props forced but no lines found. Showing analytical fallback.")
            else:
                print("  No prop lines available — using analytical benchmarks (Option 2).")
            mode = 'analytical'

    # ── Run selected mode
    if mode == 'props':
        validate_with_props(pitchers, prop_map, stats_map)
    else:
        validate_analytical(pitchers, stats_map)

    # ── Always show component breakdown
    print_component_breakdown(pitchers)

    print(f"\n{'='*80}\n")

if __name__ == '__main__':
    run()
