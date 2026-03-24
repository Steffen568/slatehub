#!/usr/bin/env python3
"""
seed_league_averages.py — Compute and seed the league_averages reference table

Structure: key-value store — one row per (stat_name, season)
  stat_name TEXT, season INT, value FLOAT
  PRIMARY KEY (stat_name, season)

Stats derived from your existing database (batter_stats, pitcher_stats,
batter_splits, pitcher_splits) wherever possible.
Hardcoded MLB baselines used only when a stat can't be reliably derived.

Run once per season after enough games have been played (50+ games).
Re-run any time you want to refresh the averages.

SQL to create the table (run in Supabase first):
  CREATE TABLE IF NOT EXISTS league_averages (
    stat_name TEXT NOT NULL,
    season    INT  NOT NULL,
    value     FLOAT,
    source    TEXT,   -- 'derived' or 'hardcoded'
    PRIMARY KEY (stat_name, season)
  );
"""

import os, sys
import math
from supabase import create_client
from dotenv import load_dotenv

sys.stdout.reconfigure(encoding='utf-8', errors='replace')
load_dotenv()

sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])

from config import SEASON as _DEFAULT_SEASON
SEASON = int(sys.argv[1]) if len(sys.argv) > 1 else _DEFAULT_SEASON

MIN_PA  = 100   # minimum PA to include a batter in avg calculation
MIN_BF  = 50    # minimum BF (pa in pitcher_stats) to include a pitcher


# ── Helpers ───────────────────────────────────────────────────────────────────

def fetch_all(table, cols, filters=None):
    """Fetch all rows from a table with optional eq filters."""
    rows = []
    offset = 0
    while True:
        q = sb.table(table).select(cols).eq('season', SEASON)
        if filters:
            for col, val in filters.items():
                q = q.eq(col, val)
        res = q.range(offset, offset + 999).execute()
        if not res.data:
            break
        rows.extend(res.data)
        if len(res.data) < 1000:
            break
        offset += 1000
    return rows

def weighted_avg(rows, stat_col, weight_col, min_weight=None):
    """PA/IP-weighted average of a stat across all rows."""
    total_weight = 0
    total_val    = 0
    for row in rows:
        w = row.get(weight_col)
        v = row.get(stat_col)
        if w is None or v is None:
            continue
        try:
            w = float(w)
            v = float(v)
        except (ValueError, TypeError):
            continue
        if math.isnan(v) or math.isinf(v):
            continue
        if min_weight and w < min_weight:
            continue
        total_weight += w
        total_val    += v * w
    if total_weight == 0:
        return None
    return round(total_val / total_weight, 4)

def simple_avg(rows, stat_col, min_pa_col=None, min_pa=None):
    """Simple mean of a stat, optionally filtered by minimum PA."""
    vals = []
    for row in rows:
        if min_pa_col and min_pa:
            pa = row.get(min_pa_col)
            if pa is None or float(pa) < min_pa:
                continue
        v = row.get(stat_col)
        if v is None:
            continue
        try:
            v = float(v)
            if not math.isnan(v) and not math.isinf(v):
                vals.append(v)
        except (ValueError, TypeError):
            pass
    if not vals:
        return None
    return round(sum(vals) / len(vals), 4)

def upsert_avg(records):
    """Upsert all computed averages to league_averages table."""
    sb.table('league_averages').upsert(records, on_conflict='stat_name,season').execute()
    print(f"  Upserted {len(records)} records")


# ── Derive ────────────────────────────────────────────────────────────────────

def derive_from_batter_stats():
    print("\n── Batter stats (overall, PA-weighted)")
    rows = fetch_all('batter_stats', 'pa,wrc_plus,woba,xwoba,k_pct,bb_pct,hard_hit_pct,barrel_pct,avg_ev,pull_pct,oppo_pct,fb_pct,gb_pct')
    print(f"  Loaded {len(rows)} batter rows")

    records = []
    stats = {
        'batter_wrc_plus'    : ('wrc_plus',    'pa'),
        'batter_woba'        : ('woba',        'pa'),
        'batter_xwoba'       : ('xwoba',       'pa'),
        'batter_k_pct'       : ('k_pct',       'pa'),
        'batter_bb_pct'      : ('bb_pct',      'pa'),
        'batter_hard_hit_pct': ('hard_hit_pct','pa'),
        'batter_barrel_pct'  : ('barrel_pct',  'pa'),
        'batter_avg_ev'      : ('avg_ev',       'pa'),
        'batter_pull_pct'    : ('pull_pct',    'pa'),
        'batter_oppo_pct'    : ('oppo_pct',    'pa'),
        'batter_fb_pct'      : ('fb_pct',      'pa'),
        'batter_gb_pct'      : ('gb_pct',      'pa'),
    }

    for stat_name, (val_col, wt_col) in stats.items():
        val = weighted_avg(rows, val_col, wt_col, min_weight=MIN_PA)
        if val is not None:
            records.append({'stat_name': stat_name, 'season': SEASON, 'value': val, 'source': 'derived'})
            print(f"  {stat_name}: {val}")
        else:
            print(f"  {stat_name}: no data (will use hardcoded fallback)")

    return records

def derive_from_pitcher_stats():
    print("\n── Pitcher stats (overall, BF-weighted)")
    rows = fetch_all('pitcher_stats', 'ip,era,xfip,fip,siera,k_pct,bb_pct,hard_hit_pct,barrel_pct,babip,csw_pct,swstr_pct,whip,stuff_plus')
    print(f"  Loaded {len(rows)} pitcher rows")

    records = []
    stats = {
        'pitcher_era'          : ('era',        'ip'),
        'pitcher_xfip'         : ('xfip',       'ip'),
        'pitcher_fip'          : ('fip',        'ip'),
        'pitcher_k_pct'        : ('k_pct',      'ip'),
        'pitcher_bb_pct'       : ('bb_pct',     'ip'),
        'pitcher_hard_hit_pct' : ('hard_hit_pct','ip'),
        'pitcher_barrel_pct'   : ('barrel_pct', 'ip'),
        'pitcher_babip'        : ('babip',      'ip'),
        'pitcher_csw_pct'      : ('csw_pct',    'ip'),
        'pitcher_swstr_pct'    : ('swstr_pct',  'ip'),
        'pitcher_whip'         : ('whip',       'ip'),
        'pitcher_stuff_plus'   : ('stuff_plus', 'ip'),
    }

    for stat_name, (val_col, wt_col) in stats.items():
        val = weighted_avg(rows, val_col, wt_col, min_weight=MIN_BF)
        if val is not None:
            records.append({'stat_name': stat_name, 'season': SEASON, 'value': val, 'source': 'derived'})
            print(f"  {stat_name}: {val}")
        else:
            print(f"  {stat_name}: no data (will use hardcoded fallback)")

    return records

def derive_from_batter_splits():
    print("\n── Batter splits (vs LHP and vs RHP)")
    records = []

    for hand in ['L', 'R']:
        label = 'vs_lhp' if hand == 'L' else 'vs_rhp'
        rows = fetch_all('batter_splits', 'pa,woba,xwoba,k_pct,bb_pct', filters={'split': hand})
        print(f"  Loaded {len(rows)} batter_splits split={hand} rows")

        stats = {
            f'batter_woba_{label}'  : ('woba',  'pa'),
            f'batter_xwoba_{label}' : ('xwoba', 'pa'),
            f'batter_k_pct_{label}' : ('k_pct', 'pa'),
            f'batter_bb_pct_{label}': ('bb_pct','pa'),
        }

        for stat_name, (val_col, wt_col) in stats.items():
            val = weighted_avg(rows, val_col, wt_col, min_weight=MIN_PA)
            if val is not None:
                records.append({'stat_name': stat_name, 'season': SEASON, 'value': val, 'source': 'derived'})
                print(f"  {stat_name}: {val}")
            else:
                print(f"  {stat_name}: no data")

    return records

def derive_from_pitcher_splits():
    print("\n── Pitcher splits (vs LHH and vs RHH)")
    records = []

    for hand in ['L', 'R']:
        label = 'vs_lhh' if hand == 'L' else 'vs_rhh'
        rows = fetch_all('pitcher_splits', 'pa,woba,xwoba,k_pct,bb_pct,xfip', filters={'split': hand})
        print(f"  Loaded {len(rows)} pitcher_splits split={hand} rows")

        stats = {
            f'pitcher_woba_allowed_{label}'  : ('woba',  'pa'),
            f'pitcher_xwoba_allowed_{label}' : ('xwoba', 'pa'),
            f'pitcher_k_pct_{label}'         : ('k_pct', 'pa'),
            f'pitcher_bb_pct_{label}'        : ('bb_pct','pa'),
            f'pitcher_xfip_{label}'          : ('xfip',  'pa'),
        }

        for stat_name, (val_col, wt_col) in stats.items():
            val = weighted_avg(rows, val_col, wt_col, min_weight=MIN_BF)
            if val is not None:
                records.append({'stat_name': stat_name, 'season': SEASON, 'value': val, 'source': 'derived'})
                print(f"  {stat_name}: {val}")
            else:
                print(f"  {stat_name}: no data")

    return records


# ── Hardcoded fallbacks ───────────────────────────────────────────────────────
# Used only when derived values aren't available (early in the season, or missing columns).
# Values are multi-year MLB baselines from FanGraphs/Savant 2022-2024 averages.
# Update these once per year if needed.

HARDCODED = [
    # ── Overall batter baselines
    ('batter_wrc_plus',     100.0),   # by definition
    ('batter_woba',         0.318),   # MLB 2022-24 avg
    ('batter_xwoba',        0.315),
    ('batter_k_pct',        0.225),   # ~22.5%
    ('batter_bb_pct',       0.083),   # ~8.3%
    ('batter_hard_hit_pct', 38.5),    # stored as raw %
    ('batter_barrel_pct',   8.0),     # stored as raw %
    ('batter_avg_ev',       88.5),    # mph
    ('batter_pull_pct',     0.40),    # ~40%
    ('batter_oppo_pct',     0.25),    # ~25%
    ('batter_fb_pct',       0.35),    # ~35%
    ('batter_gb_pct',       0.44),    # ~44%
    # ── Overall pitcher baselines
    ('pitcher_era',         4.20),
    ('pitcher_xfip',        4.00),
    ('pitcher_fip',         4.10),
    ('pitcher_k_pct',       0.220),
    ('pitcher_bb_pct',      0.085),
    ('pitcher_hard_hit_pct',38.5),
    ('pitcher_barrel_pct',  8.0),
    ('pitcher_babip',       0.298),
    ('pitcher_csw_pct',     0.270),   # ~27%
    ('pitcher_swstr_pct',   0.108),   # ~10.8%
    ('pitcher_whip',        1.30),
    ('pitcher_stuff_plus',  100.0),   # by definition
    # ── Batter splits vs LHP
    ('batter_woba_vs_lhp',  0.310),
    ('batter_xwoba_vs_lhp', 0.308),
    ('batter_k_pct_vs_lhp', 0.220),
    ('batter_bb_pct_vs_lhp',0.082),
    # ── Batter splits vs RHP
    ('batter_woba_vs_rhp',  0.318),
    ('batter_xwoba_vs_rhp', 0.315),
    ('batter_k_pct_vs_rhp', 0.226),
    ('batter_bb_pct_vs_rhp',0.083),
    # ── Pitcher splits vs LHH
    ('pitcher_woba_allowed_vs_lhh',  0.315),
    ('pitcher_xwoba_allowed_vs_lhh', 0.312),
    ('pitcher_k_pct_vs_lhh',         0.218),
    ('pitcher_bb_pct_vs_lhh',        0.088),
    ('pitcher_xfip_vs_lhh',          4.05),
    # ── Pitcher splits vs RHH
    ('pitcher_woba_allowed_vs_rhh',  0.320),
    ('pitcher_xwoba_allowed_vs_rhh', 0.316),
    ('pitcher_k_pct_vs_rhh',         0.222),
    ('pitcher_bb_pct_vs_rhh',        0.083),
    ('pitcher_xfip_vs_rhh',          4.00),
    # ── Park physics constants (used in PPIS)
    ('park_avg_wall_height', 9.0),    # ft — league avg pull-side wall height
    ('park_avg_pull_pct',    0.40),   # league avg pull%
    ('park_avg_fb_pct',      0.35),   # league avg fly ball%
    ('park_avg_barrel_pct',  8.0),    # league avg barrel% for normalization
]


# ── Main ──────────────────────────────────────────────────────────────────────

def run():
    print(f"\nseed_league_averages.py — Season {SEASON}")
    print("=" * 52)

    all_records = []

    # ── Derived from live DB data
    all_records += derive_from_batter_stats()
    all_records += derive_from_pitcher_stats()
    all_records += derive_from_batter_splits()
    all_records += derive_from_pitcher_splits()

    # ── Hardcoded fallbacks — only insert if not already derived
    derived_names = {r['stat_name'] for r in all_records}
    print(f"\n── Hardcoded fallbacks (for stats not derivable from DB)")
    added_fallbacks = 0
    for stat_name, value in HARDCODED:
        if stat_name not in derived_names:
            all_records.append({
                'stat_name': stat_name,
                'season':    SEASON,
                'value':     value,
                'source':    'hardcoded',
            })
            print(f"  {stat_name}: {value}  [hardcoded]")
            added_fallbacks += 1

    if added_fallbacks == 0:
        print(f"  All stats derived from live data — no hardcoded fallbacks needed.")

    # ── Upload
    print(f"\nUploading {len(all_records)} records to league_averages...")
    try:
        upsert_avg(all_records)
    except Exception as e:
        print(f"  ERROR: {e}")
        print("  Make sure the league_averages table exists. SQL:")
        print("    CREATE TABLE IF NOT EXISTS league_averages (")
        print("      stat_name TEXT NOT NULL,")
        print("      season    INT  NOT NULL,")
        print("      value     FLOAT,")
        print("      source    TEXT,")
        print("      PRIMARY KEY (stat_name, season)")
        print("    );")
        return

    # ── Summary
    derived_count  = sum(1 for r in all_records if r['source'] == 'derived')
    hardcoded_count = sum(1 for r in all_records if r['source'] == 'hardcoded')
    print(f"\nDone.")
    print(f"  Derived from DB:    {derived_count}")
    print(f"  Hardcoded baseline: {hardcoded_count}")
    print(f"  Total:              {len(all_records)}")


if __name__ == "__main__":
    run()
