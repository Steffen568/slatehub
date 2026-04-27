#!/usr/bin/env python3
"""Trace PMS calculation for a specific batter to find frontend vs pool builder divergence."""
import sys
sys.stdout.reconfigure(encoding='utf-8', line_buffering=True)
import os, math
from dotenv import load_dotenv
load_dotenv()
from supabase import create_client
sb = create_client(os.environ['SUPABASE_URL'], os.environ['SUPABASE_KEY'])

SOTO_PID = 665742
DATE = '2026-04-25'

# Find opposing pitcher
games = sb.table('games').select('*').eq('game_date', DATE).execute()
opp_sp_id = None
for g in (games.data or []):
    if 'Yankees' in (g.get('home_team') or '') or 'Yankees' in (g.get('away_team') or ''):
        print(f'NYY game: {g["away_team"]} @ {g["home_team"]}')
        if 'Yankees' in g.get('home_team', ''):
            opp_sp_id = g.get('away_sp_id')
        else:
            opp_sp_id = g.get('home_sp_id')
        print(f'  Soto faces sp_id={opp_sp_id}')
        break

if not opp_sp_id:
    print('No NYY game found'); exit()

def safe(v, d=None):
    if v is None: return d
    try:
        f = float(v)
        return f if not (math.isnan(f) or math.isinf(f)) else d
    except: return d

def to_d(v):
    return None if v is None else (v / 100.0 if v > 1 else v)

def to_r(v):
    return None if v is None else (v if v > 1 else v * 100)

# Load data
pd_rows = sb.table('pitcher_stats').select('*').eq('player_id', opp_sp_id).order('season', desc=True).execute()
pd = pd_rows.data[0] if pd_rows.data else {}
print(f'\nPitcher: {pd.get("full_name")} (season {pd.get("season")})')
print(f'  pitching+={pd.get("pitching_plus")} stuff+={pd.get("stuff_plus")} xfip={pd.get("xfip")}')
print(f'  k_pct={pd.get("k_pct")} swstr={pd.get("swstr_pct")} barrel={pd.get("barrel_pct")} hard_hit={pd.get("hard_hit_pct")}')

# Pitcher splits
ps_rows = sb.table('pitcher_splits').select('*').eq('player_id', opp_sp_id).order('season', desc=True).execute()
p_splits = {}
for r in ps_rows.data:
    s = (r.get('split') or '').upper()
    if s in ('L', 'R') and s not in p_splits:
        p_splits[s] = r
for hand, sp in p_splits.items():
    print(f'  split vs {hand}HH: xwoba={sp.get("xwoba")} woba={sp.get("woba")} k_pct={sp.get("k_pct")} hard_hit={sp.get("hard_hit_pct")} season={sp.get("season")}')

# Batter stats
bs = sb.table('batter_stats').select('*').eq('player_id', SOTO_PID).order('season', desc=True).limit(1).execute()
b_stats = bs.data[0] if bs.data else {}
print(f'\nSoto stats (season {b_stats.get("season")}): xwoba={b_stats.get("xwoba")} barrel={b_stats.get("barrel_pct")} hard_hit={b_stats.get("hard_hit_pct")} o_swing={b_stats.get("o_swing_pct")}')

# Batter splits
bsp = sb.table('batter_splits').select('*').eq('player_id', SOTO_PID).order('season', desc=True).execute()
b_splits = {}
for r in bsp.data:
    s = (r.get('split') or '').upper()
    if s in ('L', 'R') and s not in b_splits:
        b_splits[s] = r
for hand, sp in b_splits.items():
    print(f'  Soto vs {hand}HP: xwoba={sp.get("xwoba")} woba={sp.get("woba")} season={sp.get("season")}')

# Bat tracking
bt = sb.table('bat_tracking').select('*').eq('player_id', SOTO_PID).order('season', desc=True).limit(1).execute()
bt_data = bt.data[0] if bt.data else {}
print(f'\nSoto bat tracking: attack_angle={bt_data.get("attack_angle")} squared_up={bt_data.get("squared_up_pct")}')

# Batter hand
roster = sb.table('rosters').select('bats').eq('player_id', SOTO_PID).limit(1).execute()
bat_hand = roster.data[0].get('bats') if roster.data else None
print(f'Soto bats: {bat_hand}')

# VAA
arsenal = sb.table('pitch_arsenal').select('release_height,extension').eq('player_id', opp_sp_id).in_('pitch_type', ['FF', 'SI']).order('season', desc=True).limit(1).execute()
vaa_val = None
if arsenal.data:
    rh = safe(arsenal.data[0].get('release_height'), 0)
    ext = safe(arsenal.data[0].get('extension'), 5.5)
    if rh > 0:
        dist = 60.5 - ext
        vaa_val = -math.atan((rh - 2.5) / dist) * (180 / math.pi)
print(f'Pitcher VAA: {vaa_val}')

# ── Trace PMS component by component ──
print(f'\n{"="*60}')
print(f'  PMS COMPONENT TRACE')
print(f'{"="*60}')

eff_hand = bat_hand
p_split = None
if bat_hand == 'S':
    xw_l = safe(p_splits.get('L', {}).get('xwoba') or p_splits.get('L', {}).get('woba'), 0)
    xw_r = safe(p_splits.get('R', {}).get('xwoba') or p_splits.get('R', {}).get('woba'), 0)
    p_split = p_splits.get('L') if xw_l >= xw_r else p_splits.get('R')
    eff_hand = 'L' if xw_l >= xw_r else 'R'
    print(f'  Switch hitter: xwL={xw_l} xwR={xw_r} -> hitting from {eff_hand} side')
elif bat_hand:
    p_split = p_splits.get(bat_hand)
    print(f'  {bat_hand}-handed hitter -> using pitcher vs {bat_hand}HH split')

b_split = b_splits.get(eff_hand) if b_splits else None

pts_list = []
max_list = []

# 1. Platoon
pxw = safe(p_split.get('xwoba') if p_split else None) or safe(p_split.get('woba') if p_split else None)
if pxw is not None:
    p = 3 if pxw >= 0.350 else 2 if pxw >= 0.320 else 1 if pxw >= 0.300 else 0
    pts_list.append(p); max_list.append(3)
    print(f'  1. Platoon Vulnerability: pxw={pxw:.3f} -> {p}/3')
else:
    print(f'  1. Platoon Vulnerability: NO DATA (skipped)')

# 2. Stuff
pp = safe(pd.get('pitching_plus'))
sp = safe(pd.get('stuff_plus'))
xf = safe(pd.get('xfip'))
if pp is not None or sp is not None or xf is not None:
    if pp is not None:
        p = 2 if pp <= 80 else 1 if pp <= 95 else 0
    elif sp is not None:
        p = 2 if sp <= 80 else 1 if sp <= 95 else 0
    else:
        p = 2 if xf >= 4.50 else 1 if xf >= 4.00 else 0
    pts_list.append(p); max_list.append(2)
    print(f'  2. Pitcher Stuff: pp={pp} sp={sp} xf={xf} -> {p}/2')
else:
    print(f'  2. Pitcher Stuff: NO DATA (skipped)')

# 3. Barrel
p_brl = to_r(safe(pd.get('barrel_pct')))
h_brl = to_r(safe(b_stats.get('barrel_pct')))
h_hh = to_r(safe(b_stats.get('hard_hit_pct')))
if p_brl is not None and (h_brl is not None or h_hh is not None):
    p = 0
    if p_brl >= 9 and h_brl is not None and h_brl >= 9: p = 2
    elif p_brl >= 9 or (h_brl is not None and h_brl >= 9): p = 1
    elif p_brl >= 7 and h_hh is not None and h_hh >= 38: p = 1
    pts_list.append(p); max_list.append(2)
    print(f'  3. Barrel Opp: p_brl={p_brl:.1f} h_brl={h_brl} h_hh={h_hh} -> {p}/2')
else:
    print(f'  3. Barrel Opp: NO DATA (p_brl={p_brl} h_brl={h_brl} h_hh={h_hh}) (skipped)')

# 4. Swing Plane
atk = safe(bt_data.get('attack_angle'))
if atk is not None and vaa_val is not None:
    gap = abs(atk - abs(vaa_val))
    p = 2 if gap <= 3 else 1 if gap <= 7 else 0
    pts_list.append(p); max_list.append(2)
    print(f'  4. Swing Plane: atk={atk:.1f} vaa={vaa_val:.1f} gap={gap:.1f} -> {p}/2')
else:
    print(f'  4. Swing Plane: NO DATA (atk={atk} vaa={vaa_val}) (skipped)')

# 5. K Environment
k_raw = safe(p_split.get('k_pct') if p_split else None) or safe(pd.get('k_pct'))
sw_raw = safe(pd.get('swstr_pct'))
if k_raw is not None:
    k = to_d(k_raw)
    p = 2 if k <= 0.18 else 1 if k <= 0.22 else 0
    if p < 2 and sw_raw is not None and to_d(sw_raw) <= 0.09:
        p = min(p + 1, 2)
    pts_list.append(p); max_list.append(2)
    print(f'  5. K Environment: k={k:.3f} sw={to_d(sw_raw) if sw_raw else None} -> {p}/2')
else:
    print(f'  5. K Environment: NO DATA (skipped)')

# 6. Contact Quality
sq_up = safe(bt_data.get('squared_up_pct'))
bxw = safe(b_split.get('xwoba') if b_split else None) or safe(b_split.get('woba') if b_split else None) or safe(b_stats.get('xwoba'))
p_hh_val = to_r(safe(p_split.get('hard_hit_pct') if p_split else None)) or to_r(safe(pd.get('hard_hit_pct')))
if bxw is not None or sq_up is not None:
    strong_bat = (bxw is not None and bxw >= 0.370) or (sq_up is not None and sq_up >= 13)
    strong_pit = p_hh_val is not None and p_hh_val >= 38
    p = 2 if strong_bat and strong_pit else 1 if strong_bat or strong_pit else 0
    pts_list.append(p); max_list.append(2)
    print(f'  6. Contact Quality: sq_up={sq_up} bxw={bxw} p_hh={p_hh_val} strong_bat={strong_bat} strong_pit={strong_pit} -> {p}/2')
else:
    print(f'  6. Contact Quality: NO DATA (skipped)')

# 7. Discipline
o_swing = to_d(safe(b_stats.get('o_swing_pct')))
p_bb = to_d(safe(p_split.get('bb_pct') if p_split else None) or safe(pd.get('bb_pct')))
if o_swing is not None and p_bb is not None:
    p = 1 if o_swing <= 0.28 and p_bb >= 0.09 else 0
    pts_list.append(p); max_list.append(1)
    print(f'  7. Discipline: o_swing={o_swing:.3f} p_bb={p_bb:.3f} -> {p}/1')
else:
    print(f'  7. Discipline: NO DATA (skipped)')

# 8. Form (pool builder may not have L7)
print(f'  8. Recent Form: skipped (no L7 data in pool builder)')

total = sum(pts_list)
max_total = sum(max_list)
score = round((total / max_total) * 10) if max_total > 0 else 5
print(f'\n  TOTAL: {total}/{max_total} = {total/max_total:.3f} * 10 = {score}')
print(f'  Pool builder PMS: {score}')
print(f'  Frontend PMS:     6 (reported by user)')
print(f'  Difference:       {score - 6}')

if score != 6:
    print(f'\n  The {score - 6:+d} point gap is likely from:')
    if vaa_val is None:
        print(f'    - Swing Plane (comp 4): pool builder has no VAA, frontend may have it')
    print(f'    - Frontend may use different season data or include L7 form')
    print(f'    - Check if frontend uses 3yr-blended splits vs pool builder using most-recent')
