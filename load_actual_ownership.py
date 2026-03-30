#!/usr/bin/env python3
"""
load_actual_ownership.py — Fetch actual ownership % from DraftKings after contests lock.

DK populates ownership data in the draftables endpoint (via draftStatAttributes)
once contests have started. This script fetches that data, maps DK player IDs to
MLBAM IDs, and stores results in the actual_ownership table.

Usage:
  py -3.12 load_actual_ownership.py                    # auto-discover locked Classic DGs
  py -3.12 load_actual_ownership.py --dg 12345,67890   # specific draft group IDs
  py -3.12 load_actual_ownership.py --compare           # compare actual vs projected
  py -3.12 load_actual_ownership.py --date 2026-03-27   # fetch for a specific date
  py -3.12 load_actual_ownership.py --discover          # just print DG info, don't fetch

Requires: migrate_actual_ownership.sql run in Supabase first.
"""

import sys
sys.stdout.reconfigure(encoding='utf-8', line_buffering=True)

import urllib.request
import json
import os
import time
import argparse
import unicodedata
import re
from datetime import date, datetime, timezone, timedelta
from collections import defaultdict
from supabase import create_client
from dotenv import load_dotenv
from config import SEASON

load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    print("ERROR: Missing SUPABASE_URL or SUPABASE_KEY in .env")
    sys.exit(1)

sb = create_client(SUPABASE_URL, SUPABASE_KEY)

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
}


def fetch_json(url):
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=20) as r:
        return json.loads(r.read())


# ── Name normalization (same as load_dk_salaries.py) ──────────────────────────

def normalize(name):
    if not name:
        return ''
    nfkd = unicodedata.normalize('NFKD', str(name))
    n = nfkd.encode('ASCII', 'ignore').decode('ASCII').strip().lower()
    n = n.replace('.', '').replace("'", '')
    n = re.sub(r'\s+(jr|sr|ii|iii|iv)\s*$', '', n)
    return re.sub(r'\s+', ' ', n).strip()


def drop_middle_initials(name):
    parts = name.split()
    if len(parts) <= 2:
        return name
    filtered = [parts[0]] + [w for w in parts[1:-1] if len(w) > 1] + [parts[-1]]
    return ' '.join(filtered)


# ── DK → MLBAM ID maps (copied from load_dk_salaries.py) ─────────────────────
# Keep in sync with load_dk_salaries.py when new entries are added there.

DK_TO_MLBAM = {
    295203 : 467793, 455056 : 543877, 500816 : 571448, 503161 : 573262,
    597768 : 605137, 597026 : 606466, 738605 : 641555, 737633 : 641933,
    828153 : 650333, 830274 : 650489, 828304 : 650559, 830359 : 655316,
    874181 : 664040, 920245 : 665489, 906282 : 665742, 917114 : 665926,
    665532 : 666126, 918435 : 666126, 824481 : 666152, 1118787: 666310,
    1056129: 668885, 1054992: 671218, 1071614: 672386, 1053474: 672695,
    915732 : 676391, 1055204: 676609, 692025 : 677578, 543713 : 677594,
    1118204: 677594, 1118821: 677950, 830104 : 702284,
    455591 : 514888, 598784 : 608070, 657963 : 608348, 830219 : 645277,
    877503 : 660670, 917940 : 666185, 873265 : 669257, 1072529: 672356,
    1169451: 677951, 1115762: 686469, 1453726: 692216, 1396848: 701358,
}

PLAYER_ID_REMAP = {
    392995 : 521692, 455627 : 500743, 830219 : 645277, 877503 : 660670,
    918766 : 666152, 918999 : 669701, 1053355: 672580, 1169451: 677951,
    1115762: 686469, 548255 : 592663, 828445 : 650968, 919910 : 665487,
    962605 : 671277, 1115760: 686780, 1118063: 677588, 1118963: 681807,
    1120962: 678246, 1284664: 682657, 1316803: 695578, 1318244: 696285,
    1053621: 671056, 467793 : 808652, 503373 : 691777, 608070 : 681459,
    657863 : 621566, 665489 : 115223, 669257 : 446920, 672356 : 699087,
    676609 : 691606, 677594 : 451219, 828962 : 700951, 872787 : 660821,
    1316799: 691777, 1396147: 673784, 1452073: 814526, 876320 : 665161,
    1055003: 669134, 657041 : 700951, 702284 : 673784, 805779 : 814526,
    573262 : 569209, 641933 : 828468, 664040 : 446184,
}


def load_player_maps():
    """Load name-to-MLBAM lookup from Supabase players table."""
    all_players = []
    offset = 0
    while True:
        res = sb.table('players').select('mlbam_id, name_normalized').range(offset, offset + 999).execute()
        if not res.data:
            break
        all_players.extend(res.data)
        if len(res.data) < 1000:
            break
        offset += 1000

    valid_ids = set()
    name_map = {}
    ambiguous = set()

    for p in all_players:
        mid = p.get('mlbam_id')
        if mid:
            valid_ids.add(mid)
        nn = p.get('name_normalized')
        if nn and mid:
            keys = {nn, normalize(nn), drop_middle_initials(normalize(nn))}
            for k in keys:
                if not k:
                    continue
                if k in name_map and name_map[k] != mid:
                    ambiguous.add(k)
                else:
                    name_map[k] = mid

    for a in ambiguous:
        name_map.pop(a, None)

    print(f"  Player map: {len(name_map)} names, {len(valid_ids)} valid MLBAM IDs")
    return name_map, valid_ids


def resolve_player_id(dk_player_id, display_name, name_map, valid_ids):
    """Resolve DK player ID to MLBAM ID using same priority as load_dk_salaries."""
    # Priority 0: DK_TO_MLBAM hardcoded
    if dk_player_id in DK_TO_MLBAM:
        resolved = DK_TO_MLBAM[dk_player_id]
        return PLAYER_ID_REMAP.get(resolved, resolved)

    # Priority 1: Name lookup
    nn = normalize(display_name)
    if nn in name_map:
        resolved = name_map[nn]
        return PLAYER_ID_REMAP.get(resolved, resolved)

    nn_nomi = drop_middle_initials(nn)
    if nn_nomi in name_map:
        resolved = name_map[nn_nomi]
        return PLAYER_ID_REMAP.get(resolved, resolved)

    # Priority 2: DK playerId if it's in our players table
    if dk_player_id in valid_ids:
        return PLAYER_ID_REMAP.get(dk_player_id, dk_player_id)

    # Fallback: raw DK ID (may not match lineups)
    return PLAYER_ID_REMAP.get(dk_player_id, dk_player_id)


# ── Discover DG IDs ───────────────────────────────────────────────────────────

def discover_from_lobby(target_date=None):
    """Fetch lobby and return Classic DG metadata (only shows upcoming DGs)."""
    print("Fetching DK contest lobby...")
    try:
        lobby = fetch_json('https://www.draftkings.com/lobby/getcontests?sport=MLB')
    except Exception as e:
        print(f"  Lobby fetch failed: {e}")
        return []

    contests = lobby.get('data', lobby).get('Contests', [])
    dg_list = lobby.get('data', lobby).get('DraftGroups', [])

    classic_dg_ids = set()
    for c in contests:
        if c.get('gameType') == 'Classic':
            classic_dg_ids.add(c.get('dg'))

    results = []
    for dg in dg_list:
        dgid = dg.get('DraftGroupId')
        if dgid not in classic_dg_ids:
            continue

        start_str = dg.get('StartDateEst', '')
        game_count = dg.get('GameCount', 0)
        game_date = None
        started = False
        slate_label = 'unknown'
        try:
            dt = datetime.fromisoformat(start_str.replace('Z', '+00:00'))
            game_date = dt.strftime('%Y-%m-%d')
            started = datetime.now(timezone.utc) > dt
            et = dt - timedelta(hours=4)
            et_hour = et.hour + et.minute / 60
            if et_hour < 13:     slate_label = 'early'
            elif et_hour < 17:   slate_label = 'afternoon'
            elif et_hour < 19.5: slate_label = 'main'
            else:                slate_label = 'late'
        except Exception:
            pass

        if target_date and game_date != target_date:
            continue

        results.append({
            'dg_id': dgid, 'game_date': game_date, 'game_count': game_count,
            'slate_label': slate_label, 'started': started,
        })
    return results


def discover_from_db():
    """Look up Classic DG IDs stored in dk_salaries.
    Works for in-progress slates that dropped off the lobby."""
    print("Checking dk_salaries for additional Classic DGs...")
    res = sb.table('dk_salaries') \
        .select('dg_id, dk_slate, contest_type') \
        .eq('contest_type', 'classic') \
        .limit(5000) \
        .execute()

    dg_set = {}
    for r in (res.data or []):
        dgid = r.get('dg_id')
        if dgid:
            dg_set[dgid] = r.get('dk_slate', 'unknown')

    results = []
    for dgid, slate in dg_set.items():
        results.append({
            'dg_id': dgid, 'game_date': None,  # filled below
            'game_count': 0, 'slate_label': slate,
            'started': True,  # in DB = already seen, likely started
        })
    return results


def discover_draft_groups(target_date=None):
    """Merge lobby + dk_salaries DB to find all Classic DGs.
    Lobby drops slates once they lock, so DB fills the gaps."""
    lobby_results = discover_from_lobby(target_date)
    db_results = discover_from_db()

    # Merge: lobby is authoritative for date/game_count, DB fills gaps
    seen_dgs = {}
    for r in lobby_results:
        seen_dgs[r['dg_id']] = r

    for r in db_results:
        if r['dg_id'] not in seen_dgs:
            # DB doesn't store game_date, but we can infer from target_date
            # or just use today as default (these are recent/active DGs)
            r['game_date'] = target_date or date.today().isoformat()
            seen_dgs[r['dg_id']] = r

    results = list(seen_dgs.values())

    # If target_date specified, filter to that date (lobby entries have dates)
    # DB entries without confirmed dates are kept (they're from dk_salaries = recent)
    return results


# ── Fetch ownership from draftables ───────────────────────────────────────────

def extract_ownership(draftable, known_attr_id=None):
    """Extract ownership % from a draftable's stat attributes.

    After contests lock, DK adds new attributes beyond the pre-lock id=408 (proj FPTS).
    Known pre-lock IDs: 408 (proj FPTS), -22 (batting hand).
    Any NEW numeric attribute that appears after lock and isn't FPTS is likely ownership.

    Returns (ownership_pct, attr_id_used) or (None, None).
    """
    PRELOCK_IDS = {408, -22}  # known pre-lock attribute IDs

    attrs = draftable.get('draftStatAttributes', [])

    # Direct top-level field check
    if 'ownership' in draftable:
        try:
            return float(draftable['ownership']), -1
        except (ValueError, TypeError):
            pass

    # If we already know which attribute ID is ownership, use it directly
    if known_attr_id is not None:
        for attr in attrs:
            if attr.get('id') == known_attr_id:
                try:
                    return float(attr.get('value', '')), known_attr_id
                except (ValueError, TypeError):
                    pass
        return None, None

    # Search by description
    for attr in attrs:
        desc = str(attr.get('description', '')).lower()
        if 'own' in desc or 'ownership' in desc:
            try:
                return float(attr.get('value', '')), attr.get('id')
            except (ValueError, TypeError):
                pass

    # Look for NEW attribute IDs that weren't present pre-lock
    # These are the post-lock additions (FPTS actual + ownership)
    new_attrs = [a for a in attrs if a.get('id') not in PRELOCK_IDS]
    if len(new_attrs) >= 2:
        # Typically: [actual FPTS, ownership %]
        # Ownership is usually the smaller value (0-60%), actual FPTS can be > 60
        try:
            vals = [(float(a.get('value', '')), a.get('id')) for a in new_attrs]
            vals.sort(key=lambda x: x[0])  # smallest first
            # The smallest new-attribute value is likely ownership
            if 0 <= vals[0][0] <= 100:
                return vals[0][0], vals[0][1]
        except (ValueError, TypeError):
            pass
    elif len(new_attrs) == 1:
        # Single new attribute — could be ownership or FPTS
        try:
            val = float(new_attrs[0].get('value', ''))
            if 0 < val <= 60:
                return val, new_attrs[0].get('id')
        except (ValueError, TypeError):
            pass

    return None, None


def fetch_ownership_for_dg(dg_info, name_map, valid_ids):
    """Fetch draftables for a DG and extract ownership from draftStatAttributes."""
    dgid = dg_info['dg_id']
    print(f"\n  Fetching draftables for DG {dgid} ({dg_info['slate_label']}, {dg_info['game_count']} games)...")

    url = f'https://api.draftkings.com/draftgroups/v1/draftgroups/{dgid}/draftables'
    try:
        data = fetch_json(url)
    except Exception as e:
        print(f"    ERROR fetching DG {dgid}: {e}")
        return []

    draftables = data.get('draftables', [])
    if not draftables:
        print(f"    No draftables found for DG {dgid}")
        return []

    # First pass: discover the ownership attribute ID from the first player
    known_attr_id = None
    sample = draftables[0]
    _, discovered_id = extract_ownership(sample)
    if discovered_id is not None:
        known_attr_id = discovered_id
        print(f"    Ownership attribute ID discovered: {known_attr_id}")

    # Deduplicate by playerDkId (Classic only needs one entry per player)
    seen_players = set()
    rows = []
    ownership_found = 0

    for d in draftables:
        dk_pid = d.get('playerDkId') or d.get('playerId')
        if not dk_pid or dk_pid in seen_players:
            continue
        seen_players.add(dk_pid)

        own_pct, _ = extract_ownership(d, known_attr_id)
        if own_pct is not None:
            ownership_found += 1

        mlbam_id = resolve_player_id(dk_pid, d.get('displayName', ''), name_map, valid_ids)

        rows.append({
            'player_id': mlbam_id,
            'dg_id': dgid,
            'game_date': dg_info['game_date'],
            'dk_name': d.get('displayName', ''),
            'team': d.get('teamAbbreviation', ''),
            'position': d.get('position', ''),
            'salary': d.get('salary', 0),
            'ownership_pct': round(own_pct, 2) if own_pct is not None else None,
            'dk_slate': dg_info['slate_label'],
            'contest_type': 'classic',
        })

    print(f"    Players: {len(rows)} | With ownership: {ownership_found} | Without: {len(rows) - ownership_found}")

    # Debug dump if no ownership found
    if ownership_found == 0:
        sample_attrs = sample.get('draftStatAttributes', [])
        if sample_attrs:
            print(f"    DEBUG — Attributes for '{sample.get('displayName', '?')}':")
            for a in sample_attrs:
                print(f"      id={a.get('id')}  value={a.get('value')}  sortValue={a.get('sortValue')}")
        else:
            print(f"    DEBUG — No draftStatAttributes (contest not started)")

    return rows


# ── Import from DK contest standings CSV ──────────────────────────────────────

def import_csv(csv_path, game_date, name_map, valid_ids):
    """Parse a DraftKings contest-standings CSV and extract ownership data.

    CSV format (columns):
      Rank, EntryId, EntryName, TimeRemaining, Points, Lineup, (empty),
      Player, Roster Position, %Drafted, FPTS

    The Player/%Drafted columns are ownership data listed alongside standings rows.
    Each row has one player's ownership — sorted by %Drafted descending.
    """
    import csv as csv_mod

    print(f"\nImporting ownership from: {csv_path}")

    # Extract contest ID from filename for DG mapping
    # Filename: contest-standings-189244042.csv → contest_id = 189244042
    basename = os.path.basename(csv_path)
    contest_id = None
    parts = basename.replace('.csv', '').split('-')
    for p in parts:
        if p.isdigit() and len(p) > 5:
            contest_id = int(p)
            break

    # Try to resolve the DG ID from the contest ID via lobby
    dg_id = 0
    slate_label = 'unknown'
    if contest_id:
        try:
            lobby = fetch_json('https://www.draftkings.com/lobby/getcontests?sport=MLB')
            contests = lobby.get('data', lobby).get('Contests', [])
            for c in contests:
                if c.get('id') == contest_id:
                    dg_id = c.get('dg', 0)
                    break
        except Exception:
            pass

    # If contest dropped from lobby, try matching via dk_salaries DG IDs
    if dg_id == 0:
        # Use contest_id as a fallback DG identifier
        dg_id = contest_id or 0
        print(f"  Contest {contest_id} not in lobby — using contest ID as DG key")

    # Look up slate label from dk_salaries if we found the DG
    if dg_id:
        try:
            res = sb.table('dk_salaries').select('dk_slate').eq('dg_id', dg_id).limit(1).execute()
            if res.data:
                slate_label = res.data[0].get('dk_slate', 'unknown')
        except Exception:
            pass

    # Build a fallback name→player_id map from dk_salaries for this DG
    # This resolves ambiguous names (Juan Soto, Vladimir Guerrero Jr., etc.)
    # because dk_salaries already has the correct MLBAM IDs for this slate
    salary_name_map = {}
    try:
        sal_res = sb.table('dk_salaries') \
            .select('player_id, name, salary, team') \
            .limit(5000) \
            .execute()
        for s in (sal_res.data or []):
            nn_sal = normalize(s.get('name', ''))
            if nn_sal and s.get('player_id'):
                salary_name_map[nn_sal] = s['player_id']
            # Also store without middle initials
            nn_nomi = drop_middle_initials(nn_sal)
            if nn_nomi and nn_nomi != nn_sal and s.get('player_id'):
                salary_name_map[nn_nomi] = s['player_id']
        print(f"  Salary name fallback map: {len(salary_name_map)} entries")
    except Exception as e:
        print(f"  WARN: Could not load salary name map: {e}")

    # Read CSV
    with open(csv_path, 'r', encoding='utf-8-sig') as f:
        reader = csv_mod.DictReader(f)
        rows = []
        seen_players = set()

        for row in reader:
            player_name = (row.get('Player') or '').strip()
            drafted_str = (row.get('%Drafted') or '').strip().replace('%', '')
            position = (row.get('Roster Position') or '').strip()
            fpts_str = (row.get('FPTS') or '').strip()

            if not player_name or not drafted_str:
                continue

            # Skip duplicate player rows (same player can appear in multiple standings rows)
            if player_name in seen_players:
                continue
            seen_players.add(player_name)

            try:
                own_pct = float(drafted_str)
            except ValueError:
                continue

            try:
                fpts = float(fpts_str)
            except (ValueError, TypeError):
                fpts = None

            # Resolve player name to MLBAM ID
            nn = normalize(player_name)
            mlbam_id = name_map.get(nn)
            if not mlbam_id:
                nn_nomi = drop_middle_initials(nn)
                mlbam_id = name_map.get(nn_nomi)
            # Fallback: dk_salaries name map (resolves ambiguous names)
            if not mlbam_id:
                mlbam_id = salary_name_map.get(nn)
            if not mlbam_id:
                nn_nomi = drop_middle_initials(nn)
                mlbam_id = salary_name_map.get(nn_nomi)
            if not mlbam_id:
                print(f"    WARN: No MLBAM match for '{player_name}' — skipping")
                continue

            mlbam_id = PLAYER_ID_REMAP.get(mlbam_id, mlbam_id)

            rows.append({
                'player_id': mlbam_id,
                'dg_id': dg_id,
                'game_date': game_date,
                'dk_name': player_name,
                'team': '',  # not in CSV
                'position': position,
                'salary': 0,  # not in this CSV format
                'ownership_pct': round(own_pct, 2),
                'dk_slate': slate_label,
                'contest_type': 'classic',
            })

    print(f"  Parsed {len(rows)} players with ownership data")

    # Show top 10
    rows.sort(key=lambda r: r['ownership_pct'], reverse=True)
    print(f"\n  {'Player':<25} {'Pos':<5} {'Own%':>6}")
    print(f"  {'-'*25} {'-'*5} {'-'*6}")
    for r in rows[:15]:
        print(f"  {r['dk_name']:<25} {r['position']:<5} {r['ownership_pct']:>5.1f}%")
    if len(rows) > 15:
        print(f"  ... and {len(rows) - 15} more")

    return rows


# ── Upload to Supabase ────────────────────────────────────────────────────────

def upload_ownership(rows):
    """Upsert ownership rows to actual_ownership table."""
    # Filter to rows that have ownership data
    with_own = [r for r in rows if r['ownership_pct'] is not None]
    if not with_own:
        print("  No rows with ownership data to upload.")
        return 0

    # Batch upsert
    batch_size = 500
    total = 0
    for i in range(0, len(with_own), batch_size):
        batch = with_own[i:i + batch_size]
        sb.table('actual_ownership').upsert(
            batch, on_conflict='player_id,dg_id'
        ).execute()
        total += len(batch)

    print(f"  Uploaded {total} ownership rows to actual_ownership")
    return total


# ── Compare actual vs projected ───────────────────────────────────────────────

def compare_ownership(target_date=None):
    """Compare actual ownership vs projected ownership for a date."""
    if not target_date:
        target_date = date.today().isoformat()

    print(f"\n{'='*80}")
    print(f"  OWNERSHIP COMPARISON — {target_date}")
    print(f"{'='*80}")

    # Fetch actual ownership
    actual_res = sb.table('actual_ownership') \
        .select('player_id, dk_name, team, position, salary, ownership_pct, dk_slate') \
        .eq('game_date', target_date) \
        .order('ownership_pct', desc=True) \
        .limit(500) \
        .execute()

    if not actual_res.data:
        print("  No actual ownership data found for this date.")
        print("  Run without --compare first to fetch ownership data.")
        return

    # Fetch projected ownership
    proj_res = sb.table('player_projections') \
        .select('player_id, full_name, team, proj_ownership, proj_dk_pts') \
        .eq('game_date', target_date) \
        .limit(500) \
        .execute()

    proj_map = {}
    for p in (proj_res.data or []):
        proj_map[p['player_id']] = p

    # Compare
    matched = 0
    total_abs_error = 0
    total_sq_error = 0
    big_misses = []

    print(f"\n  {'Player':<22} {'Pos':<5} {'Sal':>6} {'Actual':>7} {'Proj':>7} {'Diff':>7}  {'Slate'}")
    print(f"  {'-'*22} {'-'*5} {'-'*6} {'-'*7} {'-'*7} {'-'*7}  {'-'*8}")

    for row in actual_res.data:
        pid = row['player_id']
        actual = row['ownership_pct']
        if actual is None:
            continue

        proj_row = proj_map.get(pid)
        proj_own = proj_row['proj_ownership'] if proj_row and proj_row.get('proj_ownership') else None

        diff_str = '--'
        if proj_own is not None:
            diff = actual - proj_own
            diff_str = f"{diff:+.1f}%"
            abs_err = abs(diff)
            total_abs_error += abs_err
            total_sq_error += diff * diff
            matched += 1

            if abs_err >= 5:
                big_misses.append({
                    'name': row['dk_name'],
                    'actual': actual,
                    'projected': proj_own,
                    'diff': diff,
                })

        proj_str = f"{proj_own:.1f}%" if proj_own is not None else '  --'

        print(f"  {row['dk_name']:<22} {row.get('position',''):<5} "
              f"${row.get('salary',0):>5} "
              f"{actual:>6.1f}% {proj_str:>7} {diff_str:>7}  {row.get('dk_slate','')}")

    # Summary stats
    if matched > 0:
        mae = total_abs_error / matched
        rmse = (total_sq_error / matched) ** 0.5

        print(f"\n  {'='*60}")
        print(f"  SUMMARY")
        print(f"  {'='*60}")
        print(f"  Players compared:     {matched}")
        print(f"  Mean Absolute Error:  {mae:.2f}%")
        print(f"  RMSE:                 {rmse:.2f}%")
        print(f"  Big misses (>5%):     {len(big_misses)}")

        if big_misses:
            big_misses.sort(key=lambda x: abs(x['diff']), reverse=True)
            print(f"\n  TOP MISSES (>5% off):")
            print(f"  {'Player':<22} {'Actual':>7} {'Proj':>7} {'Diff':>7}")
            print(f"  {'-'*22} {'-'*7} {'-'*7} {'-'*7}")
            for m in big_misses[:15]:
                print(f"  {m['name']:<22} {m['actual']:>6.1f}% {m['projected']:>6.1f}% {m['diff']:>+6.1f}%")

        # Bias analysis
        over_proj = sum(1 for m in big_misses if m['diff'] < 0)
        under_proj = sum(1 for m in big_misses if m['diff'] > 0)
        if big_misses:
            print(f"\n  Bias: {under_proj} under-projected, {over_proj} over-projected")
            if under_proj > over_proj * 1.5:
                print("  --> Model tends to UNDERESTIMATE ownership (players more owned than predicted)")
            elif over_proj > under_proj * 1.5:
                print("  --> Model tends to OVERESTIMATE ownership (players less owned than predicted)")
    else:
        print("\n  No matching players found between actual and projected data.")
        print("  Check that projections have been computed for this date.")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description='Fetch actual DK ownership after contests lock')
    parser.add_argument('--dg', type=str, help='Comma-separated draft group IDs')
    parser.add_argument('--date', type=str, help='Target date (YYYY-MM-DD), default today')
    parser.add_argument('--csv', type=str, help='Import ownership from DK contest-standings CSV file')
    parser.add_argument('--compare', action='store_true', help='Compare actual vs projected ownership')
    parser.add_argument('--discover', action='store_true', help='Just list available DGs, do not fetch')
    args = parser.parse_args()

    target_date = args.date or date.today().isoformat()

    # Compare mode
    if args.compare:
        compare_ownership(target_date)
        return

    # Load player ID maps
    print("Loading player maps from Supabase...")
    name_map, valid_ids = load_player_maps()

    # CSV import mode
    if args.csv:
        rows = import_csv(args.csv, target_date, name_map, valid_ids)
        if rows:
            upload_ownership(rows)
            print(f"\nDone! Run with --compare --date {target_date} to see actual vs projected.")
        return

    # Determine which DGs to fetch
    if args.dg:
        dg_ids = [int(x.strip()) for x in args.dg.split(',')]
        dg_infos = [{'dg_id': d, 'game_date': target_date, 'game_count': 0,
                      'slate_label': 'unknown', 'started': True} for d in dg_ids]
    else:
        dg_infos = discover_draft_groups(target_date)
        if not dg_infos:
            print(f"No Classic DGs found for {target_date}. Try --dg to specify manually.")
            return

    # Display discovered DGs
    print(f"\nFound {len(dg_infos)} Classic draft group(s) for {target_date}:")
    for dg in dg_infos:
        status = 'STARTED' if dg['started'] else 'not started'
        print(f"  DG {dg['dg_id']:<12} {dg['slate_label']:<12} {dg['game_count']} games  [{status}]")

    if args.discover:
        return

    # Filter to started DGs only (ownership only available after lock)
    started = [dg for dg in dg_infos if dg['started']]
    not_started = [dg for dg in dg_infos if not dg['started']]

    if not_started:
        print(f"\n  Skipping {len(not_started)} DG(s) not yet started (no ownership available)")

    if not started:
        print("\nNo started DGs to fetch ownership from.")
        print("Ownership is only available after contests lock (games start).")
        return

    # Fetch ownership for each started DG
    all_rows = []
    for dg in started:
        rows = fetch_ownership_for_dg(dg, name_map, valid_ids)
        all_rows.extend(rows)
        time.sleep(0.3)  # polite rate limit

    # Upload
    if all_rows:
        total_with_own = sum(1 for r in all_rows if r['ownership_pct'] is not None)
        print(f"\nTotal: {len(all_rows)} players fetched, {total_with_own} with ownership data")

        if total_with_own > 0:
            upload_ownership(all_rows)
            print("\nDone! Run with --compare to see actual vs projected.")
        else:
            print("\nNo ownership data found in draftStatAttributes.")
            print("This can happen if:")
            print("  1. Games haven't started yet (ownership appears after lock)")
            print("  2. DK hasn't populated ownership for this DG yet")
            print("  3. The attribute format has changed")
            print("\nCheck the DEBUG output above for raw attribute data.")
    else:
        print("\nNo players fetched.")


if __name__ == '__main__':
    main()
