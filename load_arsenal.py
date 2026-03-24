#!/usr/bin/env python3
"""
Phase 8 — Pitch Arsenal Data
Sources:
  - pitch-arsenal-stats  → usage%, RV/100, whiff%, K%, hard_hit%, xwOBA
  - pitch-movement       → IVB (pitcher_break_z_induced), HB (pitcher_break_x)
  - statcast_search/csv  → spin_rate, release_pos_z (height), release_extension, arm_angle
  - pybaseball           → velo (avg_speed per pitch type)
"""

import os, sys, requests, io, time
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv

sys.stdout.reconfigure(encoding='utf-8', errors='replace')

load_dotenv()
sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

from config import SEASON

PITCH_TYPES = ["FF", "SI", "FC", "SL", "ST", "CH", "CU", "FS"]

# ── Helpers ────────────────────────────────────────────────────────────────────

def _safe(row, *keys):
    for k in keys:
        if k in row.index and pd.notna(row[k]):
            try: return float(row[k])
            except: pass
    return None

def _get_csv(url, timeout=30):
    r = requests.get(url, headers=HEADERS, timeout=timeout)
    r.raise_for_status()
    df = pd.read_csv(io.StringIO(r.text))
    df.columns = [c.strip().lower().replace(" ", "_").replace(",", "") for c in df.columns]
    return df

# ── Data fetchers ──────────────────────────────────────────────────────────────

def fetch_results(pitch_code):
    """usage%, RV/100, whiff%, K%, hard_hit%, xwOBA per pitch type."""
    url = (f"https://baseballsavant.mlb.com/leaderboard/pitch-arsenal-stats"
           f"?type=pitcher&pitchType={pitch_code}&year={SEASON}&position=&team=&min=1&csv=true")
    return _get_csv(url)

def fetch_movement_all():
    """IVB + HB for all pitch types.
    Returns dict: (player_id, pitch_type) -> {ivb, hb}
    """
    mov = {}
    for pt in PITCH_TYPES:
        url = ("https://baseballsavant.mlb.com/leaderboard/pitch-movement"
               f"?pitchType={pt}&year={SEASON}&minP=1&deckType=2&csv=true")
        try:
            df = _get_csv(url)
            for _, row in df.iterrows():
                try:
                    pid = int(row.get("pitcher_id", row.get("player_id", 0)))
                except:
                    continue
                row_pt = str(row.get("pitch_type", pt)).strip().upper()
                ivb = _safe(row, "pitcher_break_z_induced", "pitcher_break_z")
                hb  = _safe(row, "pitcher_break_x")
                if pid:
                    mov[(pid, row_pt)] = {"ivb": ivb, "hb": hb}
            print(f"  Movement {pt}: {len([k for k in mov if k[1]==row_pt])} records")
            time.sleep(0.4)
        except Exception as e:
            print(f"  WARNING movement {pt}: {e}")
    return mov

def fetch_stuff():
    """Stuff+ per pitch type from Savant pitch-arsenals?type=stuff.
    Returns dict: (player_id, pitch_type) -> stuff_plus value
    """
    url = (f"https://baseballsavant.mlb.com/leaderboard/pitch-arsenals"
           f"?min=1&pos=&year={SEASON}&team=&type=stuff&csv=true")
    df = _get_csv(url)
    stuff_map = {}
    pt_cols = {
        'ff_stuff': 'FF', 'si_stuff': 'SI', 'fc_stuff': 'FC',
        'sl_stuff': 'SL', 'ch_stuff': 'CH', 'cu_stuff': 'CU',
        'fs_stuff': 'FS', 'kn_stuff': 'KC', 'st_stuff': 'ST', 'sv_stuff': 'SV',
    }
    for _, row in df.iterrows():
        try:
            pid = int(row.get("pitcher", 0))
        except:
            continue
        if not pid:
            continue
        for col, pt in pt_cols.items():
            if col in row.index and pd.notna(row[col]):
                try:
                    stuff_map[(pid, pt)] = round(float(row[col]), 1)
                except:
                    pass
    return stuff_map

def fetch_statcast_agg(pitch_code):
    """spin_rate, release_pos_z (height), release_extension, arm_angle
    via statcast_search grouped by pitcher name.
    Returns dict: player_id -> {spin_rate, release_height, extension, arm_angle}
    """
    url = (f"https://baseballsavant.mlb.com/statcast_search/csv"
           f"?all=true&player_type=pitcher&hfSea={SEASON}|&hfPT={pitch_code}|"
           f"&group_by=name&sort_col=pitches&sort_order=desc&min_pitches=1")
    df = _get_csv(url, timeout=45)
    agg = {}
    for _, row in df.iterrows():
        try:
            pid = int(row.get("player_id", 0))
        except:
            continue
        if not pid:
            continue
        agg[pid] = {
            "spin_rate"      : _safe(row, "spin_rate"),
            "release_height" : _safe(row, "release_pos_z"),
            "extension"      : _safe(row, "release_extension"),
            "arm_angle"      : _safe(row, "arm_angle"),
            "ivb"            : _safe(row, "api_break_z_induced", "pfx_z"),
            "hb"             : _safe(row, "api_break_x_arm", "pfx_x"),
        }
    return agg

# ── Main ───────────────────────────────────────────────────────────────────────

def run():
    print("\nPhase 8 — Pitch Arsenal Data")
    print("=" * 45)

    # 1. Velo from pybaseball
    print("Loading velo (pybaseball)...")
    velo_map = {}
    try:
        import pybaseball as pb
        pb.cache.enable()
        vdf = pb.statcast_pitcher_pitch_arsenal(SEASON, minP=50)
        for _, row in vdf.iterrows():
            pid = int(row["pitcher"])
            for col in vdf.columns:
                if col.endswith("_avg_speed"):
                    pt = col.replace("_avg_speed", "").upper()
                    pt = {"KN": "KC", "SV": "SV"}.get(pt, pt)
                    val = row[col]
                    if pd.notna(val):
                        velo_map[(pid, pt)] = round(float(val), 1)
        print(f"  OK: {len(velo_map)} velo entries")
    except Exception as e:
        print(f"  WARNING velo: {e}")

    # 2. Movement (IVB / HB) — single fetch covers all pitch types
    print("Loading movement (IVB / HB)...")
    mov_all = {}
    try:
        mov_all = fetch_movement_all()
        print(f"  OK: {len(mov_all)} pitcher-pitch records")
    except Exception as e:
        print(f"  WARNING movement: {e}")

    # 3. Statcast aggregate — spin, release height, extension, arm angle per pitch type
    print("Loading statcast aggregate (spin / release / arm angle)...")
    agg_all = {}   # (player_id, pitch_type) -> {spin_rate, release_height, extension, arm_angle}
    for pitch_code in PITCH_TYPES:
        try:
            agg = fetch_statcast_agg(pitch_code)
            for pid, vals in agg.items():
                agg_all[(pid, pitch_code)] = vals
            print(f"  OK {pitch_code}: {len(agg)} pitchers")
            time.sleep(0.5)   # be polite to Savant
        except Exception as e:
            print(f"  WARNING {pitch_code}: {e}")

    # 4. Stuff+ per pitch type
    print("Loading Stuff+ (Savant pitch-arsenals)...")
    stuff_map = {}
    try:
        stuff_map = fetch_stuff()
        print(f"  OK: {len(stuff_map)} pitcher-pitch entries")
    except Exception as e:
        print(f"  WARNING stuff+: {e}")

    # 5. Results (usage, RV/100, whiff, K%, hard_hit, xwOBA) per pitch type
    print("Loading pitch results (usage / RV / whiff / K)...")
    all_rows = []
    for pitch_code in PITCH_TYPES:
        try:
            df = fetch_results(pitch_code)
            if df.empty:
                print(f"  SKIP {pitch_code}: no data")
                continue
            print(f"  OK {pitch_code}: {len(df)} pitchers")

            for _, row in df.iterrows():
                pid = int(row["player_id"])
                m   = mov_all.get((pid, pitch_code), {})
                a   = agg_all.get((pid, pitch_code), {})
                record = {
                    "player_id"      : pid,
                    "player_name"    : str(row.get("last_name_first_name", "")),
                    "season"         : SEASON,
                    "pitch_type"     : pitch_code,
                    "pitch_name"     : str(row.get("pitch_name", pitch_code)),
                    "usage_pct"      : _safe(row, "pitch_usage", "pitch_percent"),
                    "rv100"          : _safe(row, "run_value_per_100"),
                    "whiff_pct"      : _safe(row, "whiff_percent"),
                    "k_pct"          : _safe(row, "k_percent"),
                    "hard_hit_pct"   : _safe(row, "hard_hit_percent"),
                    "xwoba"          : _safe(row, "est_woba"),
                    "velo"           : velo_map.get((pid, pitch_code)),
                    "ivb"            : m.get("ivb") or a.get("ivb"),
                    "hb"             : m.get("hb")  or a.get("hb"),
                    "spin_rate"      : a.get("spin_rate"),
                    "release_height" : a.get("release_height"),
                    "extension"      : a.get("extension"),
                    "arm_angle"      : a.get("arm_angle"),
                    "stuff_plus"     : stuff_map.get((pid, pitch_code)),
                    "location_plus"  : _safe(row, "location_plus", "pa_loc"),
                    "pitching_plus"  : _safe(row, "pitching_plus", "pa_pitching"),
                }
                all_rows.append(record)

            time.sleep(0.4)
        except Exception as e:
            print(f"  ERROR {pitch_code}: {e}")

    if not all_rows:
        print("No data collected.")
        return

    print(f"\nUploading {len(all_rows)} records...")
    BATCH = 500
    uploaded = 0
    for i in range(0, len(all_rows), BATCH):
        batch = all_rows[i:i + BATCH]
        sb.table("pitch_arsenal") \
          .upsert(batch, on_conflict="player_id,pitch_type,season", ignore_duplicates=False) \
          .execute()
        uploaded += len(batch)
        print(f"  OK {uploaded}/{len(all_rows)}")

    print(f"\nPhase 8 complete — {uploaded} records uploaded.")

    # Sanity check — Gausman (592332) as a known high-spin FF guy
    check = sb.table("pitch_arsenal") \
               .select("pitch_type,pitch_name,usage_pct,velo,ivb,hb,stuff_plus,spin_rate,release_height,extension,arm_angle") \
               .eq("player_id", 592332).eq("season", SEASON).execute()
    print(f"\nGausman sanity check ({len(check.data)} pitches):")
    for p in check.data:
        print(f"  {p['pitch_type']:3} {str(p['pitch_name']):<22} "
              f"velo={p['velo']}  ivb={p.get('ivb')}  hb={p.get('hb')}  "
              f"stuff+={p.get('stuff_plus')}  spin={p.get('spin_rate')}")

if __name__ == "__main__":
    run()
