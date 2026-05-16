#!/usr/bin/env python3
"""
bulk_import_ownership.py — Batch import all DK contest CSVs into actual_ownership.

Scans Contest_CSVs folder, auto-detects game_date for each file by matching
player names against player_projections, then upserts to actual_ownership.

Usage:
  py -3.12 bulk_import_ownership.py
  py -3.12 bulk_import_ownership.py --dry-run        # show what would be imported, no uploads
  py -3.12 bulk_import_ownership.py --min-entries N  # skip contests with fewer entries (default 100)
"""

import sys
sys.stdout.reconfigure(encoding='utf-8', line_buffering=True)

import os, csv, re, unicodedata, argparse
from collections import defaultdict, Counter
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()
sb = create_client(os.environ['SUPABASE_URL'], os.environ['SUPABASE_KEY'])

CONTEST_DIR = r"C:\Users\Steffen's PC\Desktop\WebDev\Contest_CSVs"
MIN_VOTE_PLAYERS = 5   # need ≥5 player name matches to trust a date
MIN_CONFIDENCE   = 0.5 # ≥50% of matched players must agree on the same date


# ── Same normalization as load_actual_ownership.py ────────────────────────────

def normalize(name):
    if not name:
        return ''
    nfkd = unicodedata.normalize('NFKD', str(name))
    n = nfkd.encode('ASCII', 'ignore').decode('ASCII').strip().lower()
    n = n.replace('.', '').replace("'", '')
    n = re.sub(r'\s+(jr|sr|ii|iii|iv)\s*$', '', n)
    return re.sub(r'\s+', ' ', n).strip()


def drop_middle(name):
    parts = name.split()
    if len(parts) <= 2:
        return name
    return ' '.join([parts[0]] + [w for w in parts[1:-1] if len(w) > 1] + [parts[-1]])


PLAYER_ID_REMAP = {
    392995: 521692, 455627: 500743, 830219: 645277, 877503: 660670,
    918766: 666152, 918999: 669701, 1053355: 672580, 1169451: 677951,
    1115762: 686469, 548255: 592663, 828445: 650968, 919910: 665487,
    962605: 671277, 1115760: 686780, 1118063: 677588, 1118963: 681807,
    1120962: 678246, 1284664: 682657, 1316803: 695578, 1318244: 696285,
    1053621: 671056, 467793: 808652, 503373: 691777, 608070: 681459,
    657863: 621566, 665489: 115223, 669257: 446920, 672356: 699087,
    676609: 691606, 677594: 451219, 828962: 700951, 872787: 660821,
    1316799: 691777, 1396147: 673784, 1452073: 814526, 876320: 665161,
    1055003: 669134, 657041: 700951, 702284: 673784, 805779: 814526,
    573262: 569209, 641933: 828468, 664040: 446184,
}


# ── Step 1: Load player name → MLBAM ID map ───────────────────────────────────

def load_name_map():
    print("Loading player name map...")
    rows = []
    offset = 0
    while True:
        batch = sb.table('players').select('mlbam_id,name_normalized') \
            .range(offset, offset + 999).execute().data or []
        rows.extend(batch)
        if len(batch) < 1000:
            break
        offset += 1000

    name_map = {}
    ambiguous = set()
    for p in rows:
        mid = p.get('mlbam_id')
        nn = p.get('name_normalized')
        if not mid or not nn:
            continue
        for k in {nn, normalize(nn), drop_middle(normalize(nn))}:
            if not k:
                continue
            if k in name_map and name_map[k] != mid:
                ambiguous.add(k)
            else:
                name_map[k] = mid
    for a in ambiguous:
        name_map.pop(a, None)

    # Supplement with dk_salaries names (better coverage for current season)
    sal_rows = []
    offset = 0
    while True:
        batch = sb.table('dk_salaries').select('player_id,name,dk_slate') \
            .range(offset, offset + 999).execute().data or []
        sal_rows.extend(batch)
        if len(batch) < 1000:
            break
        offset += 1000
    for r in sal_rows:
        pid = r.get('player_id')
        n = r.get('name', '')
        if not pid or not n:
            continue
        for k in {normalize(n), drop_middle(normalize(n))}:
            if k and k not in name_map:
                name_map[k] = PLAYER_ID_REMAP.get(pid, pid)

    print(f"  Name map: {len(name_map)} entries")
    return name_map


# ── Step 2: Build player_id → [game_date] lookup from player_projections ──────

def load_projection_dates():
    print("Loading projection dates...")
    rows = []
    offset = 0
    while True:
        batch = sb.table('player_projections').select('player_id,game_date') \
            .range(offset, offset + 999).execute().data or []
        rows.extend(batch)
        if len(batch) < 1000:
            break
        offset += 1000
    pid_dates = defaultdict(set)
    for r in rows:
        pid = r.get('player_id')
        gd = r.get('game_date')
        if pid and gd:
            pid_dates[pid].add(gd)
    print(f"  {len(pid_dates)} players with projection dates")
    return pid_dates


# ── Step 3: Build (player_id, game_date) → dk_slate from slate_ownership ──────

def load_slate_map():
    print("Loading slate labels...")
    rows = []
    offset = 0
    while True:
        batch = sb.table('slate_ownership').select('player_id,game_date,dk_slate') \
            .range(offset, offset + 999).execute().data or []
        rows.extend(batch)
        if len(batch) < 1000:
            break
        offset += 1000
    slate_map = {}
    for r in rows:
        pid = r.get('player_id')
        gd = r.get('game_date')
        sl = r.get('dk_slate')
        if pid and gd and sl:
            slate_map[(pid, gd)] = sl
    print(f"  {len(slate_map)} slate entries")
    return slate_map


# ── Step 4: Parse one CSV → player rows ───────────────────────────────────────

def parse_csv(fpath, min_entries):
    """Returns (entries_count, player_dict {name: ownership_pct}, position_dict {name: pos})"""
    players = {}
    positions = {}
    entries = set()
    try:
        with open(fpath, encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                entries.add(row.get('EntryId', ''))
                name = (row.get('Player') or '').strip()
                pct_raw = (row.get('%Drafted') or '').replace('%', '').strip()
                pos = (row.get('Roster Position') or '').strip()
                if name and pct_raw:
                    try:
                        pct = float(pct_raw)
                        if name not in players:
                            players[name] = pct
                            positions[name] = pos
                    except ValueError:
                        pass
    except Exception as e:
        print(f"  ERROR reading {os.path.basename(fpath)}: {e}")
        return 0, {}, {}

    entries.discard('')
    n_entries = len(entries)
    if n_entries < min_entries:
        return n_entries, {}, {}  # too small, skip

    return n_entries, players, positions


# ── Step 5: Detect game_date via player name voting ───────────────────────────

def detect_date(player_names, name_map, pid_dates):
    """
    Match each player name to a player_id, then look up which game_dates
    that player has projections for. Vote across all players — the date
    that the most players agree on is the contest date.

    Returns (game_date_str, confidence, n_matched) or (None, 0, 0).
    """
    date_votes = Counter()
    matched = 0

    for name in player_names:
        nn = normalize(name)
        pid = name_map.get(nn) or name_map.get(drop_middle(nn))
        if not pid:
            continue
        pid = PLAYER_ID_REMAP.get(pid, pid)
        dates = pid_dates.get(pid, set())
        if dates:
            matched += 1
            for d in dates:
                date_votes[d] += 1

    if not date_votes or matched < MIN_VOTE_PLAYERS:
        return None, 0, matched

    best_date, best_count = date_votes.most_common(1)[0]
    confidence = best_count / matched
    if confidence < MIN_CONFIDENCE:
        return None, confidence, matched

    return best_date, confidence, matched


# ── Step 6: Detect slate label from matched players ────────────────────────────

def detect_slate(player_ids, game_date, slate_map):
    """Find the most common dk_slate for these players on this date."""
    votes = Counter()
    for pid in player_ids:
        sl = slate_map.get((pid, game_date))
        if sl:
            votes[sl] += 1
    if votes:
        return votes.most_common(1)[0][0]
    return 'main'  # safe default — most large GPPs are main slate


# ── Step 7: Upload ────────────────────────────────────────────────────────────

def upload(rows, dry_run):
    with_own = [r for r in rows if r['ownership_pct'] is not None and r['ownership_pct'] > 0]
    if not with_own:
        return 0
    if dry_run:
        return len(with_own)
    BATCH = 500
    total = 0
    for i in range(0, len(with_own), BATCH):
        batch = with_own[i:i + BATCH]
        sb.table('actual_ownership').upsert(
            batch, on_conflict='player_id,dg_id'
        ).execute()
        total += len(batch)
    return total


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--min-entries', type=int, default=100)
    args = parser.parse_args()

    print(f"\n{'='*65}")
    print(f"  Bulk Ownership Import — Contest_CSVs")
    print(f"{'='*65}")
    if args.dry_run:
        print("  DRY RUN — no uploads will be made")

    name_map = load_name_map()
    pid_dates = load_projection_dates()
    slate_map = load_slate_map()

    files = sorted([f for f in os.listdir(CONTEST_DIR) if f.startswith('contest-standings-') and f.endswith('.csv')])
    print(f"\n  Found {len(files)} CSV files in Contest_CSVs\n")

    results = {
        'imported': 0, 'skipped_small': 0, 'skipped_no_date': 0,
        'total_players': 0, 'dates_seen': set(),
    }

    # Track which contest IDs we've already imported to avoid double-counting
    # same contest across multiple CSV files for the same date
    already_imported = set()

    for fname in files:
        fpath = os.path.join(CONTEST_DIR, fname)

        # Extract contest ID from filename
        contest_id = None
        for part in fname.replace('.csv', '').split('-'):
            if part.isdigit() and len(part) > 5:
                contest_id = int(part)
                break

        n_entries, players, positions = parse_csv(fpath, args.min_entries)

        if not players:
            if n_entries > 0:
                results['skipped_small'] += 1
                print(f"  SKIP (small)   {fname}  [{n_entries} entries]")
            else:
                print(f"  SKIP (empty)   {fname}")
            continue

        # Date detection
        game_date, confidence, n_matched = detect_date(list(players.keys()), name_map, pid_dates)

        if not game_date:
            results['skipped_no_date'] += 1
            print(f"  SKIP (no date) {fname}  [matched {n_matched} players, confidence={confidence:.0%}]")
            continue

        # Slate detection — match player names to IDs first
        matched_pids = {}
        for name in players:
            nn = normalize(name)
            pid = name_map.get(nn) or name_map.get(drop_middle(nn))
            if pid:
                matched_pids[name] = PLAYER_ID_REMAP.get(pid, pid)

        slate_label = detect_slate(list(matched_pids.values()), game_date, slate_map)

        # Build rows for this contest
        dg_key = contest_id or hash(fname)
        rows = []
        for name, own_pct in players.items():
            pid = matched_pids.get(name)
            if not pid:
                continue
            rows.append({
                'player_id':   pid,
                'dg_id':       dg_key,
                'game_date':   game_date,
                'dk_name':     name,
                'team':        '',
                'position':    positions.get(name, ''),
                'salary':      0,
                'ownership_pct': round(own_pct, 2),
                'dk_slate':    slate_label,
                'contest_type': 'classic',
            })

        n_uploaded = upload(rows, args.dry_run)
        results['imported'] += 1
        results['total_players'] += n_uploaded
        results['dates_seen'].add(game_date)

        action = 'WOULD IMPORT' if args.dry_run else 'IMPORTED'
        print(f"  {action}  {fname}")
        print(f"           date={game_date}  slate={slate_label}  "
              f"entries={n_entries}  players={n_uploaded}/{len(players)}  "
              f"confidence={confidence:.0%}")

    print(f"\n{'='*65}")
    print(f"  Summary")
    print(f"{'='*65}")
    print(f"  Imported:          {results['imported']} contests")
    print(f"  Skipped (small):   {results['skipped_small']}")
    print(f"  Skipped (no date): {results['skipped_no_date']}")
    print(f"  Total player rows: {results['total_players']}")
    print(f"  Dates covered:     {len(results['dates_seen'])}")
    if results['dates_seen']:
        print(f"  Date range:        {min(results['dates_seen'])} → {max(results['dates_seen'])}")
    print()


if __name__ == '__main__':
    main()
