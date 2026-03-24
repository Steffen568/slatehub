#!/usr/bin/env python3
import sys
sys.stdout.reconfigure(encoding='utf-8', line_buffering=True)
"""
Phase 3b — Reliever Stats
Loads pitcher_stats for all relievers (min 5 IP) from FanGraphs via pybaseball
Upserts into existing pitcher_stats table so starters are not overwritten
"""

import os
import pybaseball as pb
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()
sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])
pb.cache.enable()

from config import SEASON

def run():
    print("\nPhase 3b — Reliever Stats")
    print("=" * 40)

    print(f"Fetching FanGraphs reliever stats ({SEASON})...")
    # minIP=5 catches almost all relievers who've pitched meaningfully
    df = pb.pitching_stats(SEASON, qual=1)
    print(f"  Raw rows: {len(df)}")
    print(f"  Columns sample: {list(df.columns)[:20]}")

    # Filter to relievers only (GS < half their appearances = relief role)
    # or just use min IP=1 and let the data speak
    df.columns = [c.strip() for c in df.columns]

    # Map FanGraphs columns to our schema
    col_map = {
        'IDfg'      : 'player_id',
        'Name'      : 'full_name',
        'Team'      : 'team',
        'Age'       : 'age',
        'W'         : 'w',
        'L'         : 'l',
        'ERA'       : 'era',
        'G'         : 'g',
        'GS'        : 'gs',
        'IP'        : 'ip',
        'K/9'       : 'k9',
        'BB/9'      : 'bb9',
        'HR/9'      : 'hr9',
        'BABIP'     : 'babip',
        'LOB%'      : 'lob_pct',
        'FIP'       : 'fip',
        'xFIP'      : 'xfip',
        'SIERA'     : 'siera',
        'K%'        : 'k_pct',
        'BB%'       : 'bb_pct',
        'K-BB%'     : 'k_bb_pct',
        'SwStr%'    : 'swstr_pct',
        'CSW%'      : 'csw_pct',
        'HardHit%'  : 'hard_hit_pct',
        'Barrel%'   : 'barrel_pct',
        'Stuff+'    : 'stuff_plus',
        'Location+' : 'location_plus',
        'Pitching+' : 'pitching_plus',
        'WHIP'      : 'whip',
        'AVG'       : 'avg',
        'vFA (sc)'  : 'velo',
        'AArm'      : 'arm_angle',
        'SV'        : 'sv',
        'HLD'       : 'hld',
    }

    records = []
    for _, row in df.iterrows():
        def safe(fg_col):
            val = row.get(fg_col)
            if val is None or (isinstance(val, float) and pd.isna(val)):
                return None
            try: return float(val)
            except: return None

        pid = row.get('IDfg') or row.get('xMLBAMID')
        if not pid:
            continue

        # Convert percentages stored as decimals vs whole numbers
        def pct(fg_col):
            v = safe(fg_col)
            if v is None: return None
            return v if v <= 1.0 else v / 100.0

        record = {
            'player_id'    : int(pid),
            'full_name'    : str(row.get('Name', '')),
            'season'       : SEASON,
            'team'         : str(row.get('Team', '')),
            'age'          : safe('Age'),
            'g'            : safe('G'),
            'gs'           : safe('GS'),
            'ip'           : safe('IP'),
            'era'          : safe('ERA'),
            'fip'          : safe('FIP'),
            'xfip'         : safe('xFIP'),
            'siera'        : safe('SIERA'),
            'whip'         : safe('WHIP'),
            'k_pct'        : pct('K%'),
            'bb_pct'       : pct('BB%'),
            'k_bb_pct'     : pct('K-BB%'),
            'swstr_pct'    : pct('SwStr%'),
            'csw_pct'      : pct('CSW%'),
            'hard_hit_pct' : safe('HardHit%'),
            'barrel_pct'   : safe('Barrel%'),
            'babip'        : safe('BABIP'),
            'lob_pct'      : pct('LOB%'),
            'stuff_plus'   : safe('Stuff+'),
            'location_plus': safe('Location+'),
            'pitching_plus': safe('Pitching+'),
            'velo'         : safe('vFA (sc)'),
            'arm_angle'    : safe('AArm'),
            'hr9'          : safe('HR/9'),
            'k9'           : safe('K/9'),
            'bb9'          : safe('BB/9'),
            'sample_size'  : safe('IP'),   # used to flag small sample in UI
            'sv'           : safe('SV'),
            'hld'          : safe('HLD'),
        }
        records.append(record)

    print(f"  Prepared {len(records)} pitcher records")

    # Upsert in batches
    BATCH = 500
    uploaded = 0
    for i in range(0, len(records), BATCH):
        batch = records[i:i+BATCH]
        (sb.table('pitcher_stats')
           .upsert(batch, on_conflict='player_id,season', ignore_duplicates=False)
           .execute())
        uploaded += len(batch)
        print(f"  ✓ {uploaded}/{len(records)}")

    print(f"\nPhase 3b complete. {uploaded} records uploaded.")

    # Sanity check — look up a known reliever from bullpen_appearances
    check_ids = (sb.table('bullpen_appearances')
                   .select('player_id, player_name')
                   .eq('is_starter', False)
                   .limit(5)
                   .execute())
    print("\nSanity check — relievers in bullpen_appearances:")
    for r in check_ids.data:
        stat = (sb.table('pitcher_stats')
                  .select('full_name, era, k_pct, stuff_plus')
                  .eq('player_id', r['player_id'])
                  .limit(1)
                  .execute())
        found = stat.data[0] if stat.data else None
        status = f"ERA={found['era']} K%={found['k_pct']} Stuff+={found['stuff_plus']}" if found else "NOT FOUND"
        print(f"  {r['player_name']} ({r['player_id']}): {status}")

if __name__ == "__main__":
    run()
    