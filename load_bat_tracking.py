#!/usr/bin/env python3
"""
Phase 9 — Batter Bat Tracking Data
Sources:
  - Savant bat-tracking leaderboard  → avg_bat_speed, swing_length,
                                        squared_up_per_bat_contact, blast_per_bat_contact
  - Savant statcast batter aggregate → attack_angle, swing_path_tilt,
                                        rate_ideal_attack_angle, bat_speed, swing_length
Stores in: bat_tracking table
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

def _get_csv(url, timeout=45):
    r = requests.get(url, headers=HEADERS, timeout=timeout)
    r.raise_for_status()
    df = pd.read_csv(io.StringIO(r.text))
    df.columns = [c.strip().lower().replace(" ", "_").replace(",", "") for c in df.columns]
    return df

def safe(row, *keys):
    for k in keys:
        if k in row.index and pd.notna(row[k]):
            try: return float(row[k])
            except: pass
    return None

def run():
    print("\nPhase 9 — Batter Bat Tracking Data")
    print("=" * 45)

    # ── Source 1: Bat-tracking leaderboard
    # Has: avg_bat_speed, swing_length, squared_up_per_bat_contact, blast_per_bat_contact
    print("Fetching bat-tracking leaderboard...")
    bt_map = {}
    try:
        url = ("https://baseballsavant.mlb.com/leaderboard/bat-tracking"
               "?attackZone=&batSide=&contactType=&count=&gameType=&isHardHit="
               "&isSwing=1&minSwings=100&minGroupSwings=1&pitchType=&playResult="
               f"&position=&seasonStart={SEASON}&seasonEnd={SEASON}&team=&csv=true")
        df = _get_csv(url)
        for _, row in df.iterrows():
            try:
                pid = int(row["id"])
            except:
                continue
            bt_map[pid] = {
                "bat_speed"      : safe(row, "avg_bat_speed"),
                "swing_length"   : safe(row, "swing_length"),
                "squared_up_pct" : safe(row, "squared_up_per_bat_contact"),
                "blast_pct"      : safe(row, "blast_per_bat_contact"),
            }
        print(f"  OK: {len(bt_map)} batters")
    except Exception as e:
        print(f"  ERROR: {e}")

    # ── Source 2: Statcast batter aggregate
    # Has: attack_angle, swing_path_tilt, rate_ideal_attack_angle, bat_speed, swing_length
    print("Fetching statcast batter aggregate...")
    sc_map = {}
    try:
        url = ("https://baseballsavant.mlb.com/statcast_search/csv"
               f"?all=true&player_type=batter&hfSea={SEASON}|"
               "&group_by=name&sort_col=pitches&sort_order=desc&min_pitches=100")
        df = _get_csv(url)
        for _, row in df.iterrows():
            try:
                pid = int(row["player_id"])
            except:
                continue
            sc_map[pid] = {
                "attack_angle"          : safe(row, "attack_angle"),
                "swing_path_tilt"       : safe(row, "swing_path_tilt"),
                "ideal_attack_angle_pct": safe(row, "rate_ideal_attack_angle"),
                "bat_speed_sc"          : safe(row, "bat_speed"),
                "swing_length_sc"       : safe(row, "swing_length"),
                "avg_launch_angle"      : safe(row, "launch_angle"),
            }
        print(f"  OK: {len(sc_map)} batters")
    except Exception as e:
        print(f"  ERROR: {e}")

    # ── Merge both sources
    all_pids = set(bt_map.keys()) | set(sc_map.keys())
    print(f"\nMerging {len(all_pids)} unique batters...")

    all_rows = []
    for pid in all_pids:
        bt = bt_map.get(pid, {})
        sc = sc_map.get(pid, {})
        # Prefer bat-tracking leaderboard for bat_speed/swing_length (larger sample, swing-specific)
        bat_speed    = bt.get("bat_speed")    or sc.get("bat_speed_sc")
        swing_length = bt.get("swing_length") or sc.get("swing_length_sc")
        record = {
            "player_id"             : pid,
            "season"                : SEASON,
            "bat_speed"             : bat_speed,
            "swing_length"          : swing_length,
            "squared_up_pct"        : bt.get("squared_up_pct"),
            "blast_pct"             : bt.get("blast_pct"),
            "attack_angle"          : sc.get("attack_angle"),
            "swing_path_tilt"       : sc.get("swing_path_tilt"),
            "ideal_attack_angle_pct": sc.get("ideal_attack_angle_pct"),
            "avg_launch_angle"      : sc.get("avg_launch_angle"),
        }
        all_rows.append(record)

    print(f"Uploading {len(all_rows)} records...")
    BATCH = 500
    uploaded = 0
    for i in range(0, len(all_rows), BATCH):
        batch = all_rows[i:i + BATCH]
        sb.table("bat_tracking") \
          .upsert(batch, on_conflict="player_id,season", ignore_duplicates=False) \
          .execute()
        uploaded += len(batch)
        print(f"  OK {uploaded}/{len(all_rows)}")

    print(f"\nPhase 9 complete — {uploaded} records uploaded.")

    # Sanity check — Schwarber (656941)
    check = sb.table("bat_tracking").select("*").eq("player_id", 656941).execute()
    if check.data:
        p = check.data[0]
        print(f"\nSchwarber check: bat_speed={p.get('bat_speed'):.1f}  "
              f"swing_length={p.get('swing_length'):.1f}  "
              f"attack_angle={p.get('attack_angle'):.1f}  "
              f"swing_path_tilt={p.get('swing_path_tilt'):.1f}  "
              f"sq_up={p.get('squared_up_pct'):.3f}  blast={p.get('blast_pct'):.3f}")

if __name__ == "__main__":
    run()
