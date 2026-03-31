#!/usr/bin/env python3
import sys
sys.stdout.reconfigure(encoding='utf-8', line_buffering=True)
"""
load_contest_data.py — Fetch DK contest metadata for contest simulation

Pulls from DK lobby API:
  - Contest ID, name, entry fee, prize pool, max entries, entry count
  - Full payout structure (via contest details API)
  - Positions paid, payout %, first place, min cash

Stores in dk_contests table.

Run:
  py -3.12 load_contest_data.py
  py -3.12 load_contest_data.py --slate main
  py -3.12 load_contest_data.py --min-fee 5       # only GPP-sized contests
"""

import os, json, time, urllib.request
from datetime import datetime, timezone
from supabase import create_client
from dotenv import load_dotenv
from config import SEASON

load_dotenv()
sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
}


def fetch_json(url):
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.loads(r.read())


def fetch_payout_details(contest_id):
    """Fetch full payout structure for a contest."""
    url = f'https://api.draftkings.com/contests/v1/contests/{contest_id}'
    try:
        data = fetch_json(url)
        detail = data.get('contestDetail', {})
        summary = detail.get('contestSummary', '')
        payouts = detail.get('payoutSummary', [])

        # Extract positions paid from summary text
        # "This 53507-player contest ... pays out the top 11806 finishing positions"
        positions_paid = 0
        import re
        m = re.search(r'pays out the top (\d[\d,]*)', summary)
        if m:
            positions_paid = int(m.group(1).replace(',', ''))

        # First place prize
        first_place = 0.0
        min_cash = 0.0
        if payouts:
            for p in payouts:
                descs = p.get('payoutDescriptions', [])
                for d in descs:
                    val = d.get('value', 0)
                    if p.get('minPosition') == 1 and val > first_place:
                        first_place = val
                    if val > 0 and (min_cash == 0 or val < min_cash):
                        min_cash = val

        return {
            'positions_paid': positions_paid,
            'first_place': first_place,
            'min_cash': min_cash,
            'payout_json': payouts,
        }
    except Exception as e:
        print(f"    Payout fetch failed for {contest_id}: {e}")
        return None


def classify_slate(start_est):
    """Classify DK slate from start time (same logic as load_dk_salaries.py)."""
    if not start_est:
        return 'main'
    try:
        dt = datetime.fromisoformat(start_est.replace('Z', ''))
        et_hour = dt.hour + dt.minute / 60
        if et_hour < 13:     return 'early'
        elif et_hour < 17:   return 'afternoon'
        elif et_hour < 19.5: return 'main'
        else:                return 'late'
    except Exception:
        return 'main'


def run():
    args = sys.argv[1:]
    slate_filter = None
    min_fee = 0.25     # default: include $0.25+ contests
    max_contests = 100  # payout API calls

    i = 0
    while i < len(args):
        if args[i] == '--slate' and i+1 < len(args):
            slate_filter = args[i+1]; i += 2
        elif args[i] == '--min-fee' and i+1 < len(args):
            min_fee = float(args[i+1]); i += 2
        elif args[i] == '--max' and i+1 < len(args):
            max_contests = int(args[i+1]); i += 2
        else:
            i += 1

    print(f"\nDK Contest Data Loader")
    print("=" * 55)

    # 1. Fetch lobby
    print("  Fetching DK lobby...")
    lobby = fetch_json('https://www.draftkings.com/lobby/getcontests?sport=MLB')
    contests = lobby.get('Contests', [])
    dg_list = lobby.get('DraftGroups', [])
    print(f"  Total contests: {len(contests)}")

    # Build DG metadata
    dg_meta = {}
    for dg in dg_list:
        dgid = dg.get('DraftGroupId')
        start_est = dg.get('StartDateEst', '')
        game_date = ''
        if start_est:
            try:
                game_date = datetime.fromisoformat(start_est.replace('Z', '')).strftime('%Y-%m-%d')
            except Exception:
                pass
        dg_meta[dgid] = {
            'slate': classify_slate(start_est),
            'game_date': game_date,
            'game_count': dg.get('GameCount', 0),
        }

    # 2. Filter to Classic GPP contests
    classic = []
    for c in contests:
        if c.get('gameType') != 'Classic':
            continue
        fee = c.get('a', 0) or 0
        if fee < min_fee:
            continue
        dg_id = c.get('dg')
        meta = dg_meta.get(dg_id, {})
        slate = meta.get('slate', 'main')

        # Refine slate by game count (same logic as load_dk_salaries.py)
        game_count = meta.get('game_count', 15)
        if game_count <= 2:
            slate = 'late_night'
        elif game_count <= 5:
            slate = 'turbo'

        if slate_filter and slate != slate_filter:
            continue

        classic.append({
            'contest_id': c.get('id'),
            'dg_id': dg_id,
            'name': c.get('n', ''),
            'entry_fee': fee,
            'prize_pool': c.get('po', 0) or 0,
            'max_entries': c.get('m', 0) or 0,
            'entry_count': c.get('nt', 0) or 0,
            'max_per_user': c.get('mec', 1) or 1,
            'dk_slate': slate,
            'game_date': meta.get('game_date', ''),
            'contest_type': 'classic',
        })

    # Sort by prize pool descending — fetch payouts for biggest contests first
    classic.sort(key=lambda c: c['prize_pool'], reverse=True)
    print(f"  Classic contests (fee >= ${min_fee}): {len(classic)}")

    # 3. Fetch payout details for top contests
    to_fetch = classic[:max_contests]
    print(f"  Fetching payout details for top {len(to_fetch)} contests...")

    records = []
    for idx, c in enumerate(to_fetch):
        cid = c['contest_id']
        payout = fetch_payout_details(cid)
        time.sleep(0.3)  # rate limit

        positions_paid = 0
        payout_pct = 0.0
        first_place = 0.0
        min_cash = 0.0
        payout_json = None

        if payout:
            positions_paid = payout['positions_paid']
            first_place = payout['first_place']
            min_cash = payout['min_cash']
            payout_json = payout['payout_json']
            if c['max_entries'] > 0 and positions_paid > 0:
                payout_pct = round(positions_paid / c['max_entries'] * 100, 2)

        record = {
            'contest_id': cid,
            'dg_id': c['dg_id'],
            'name': c['name'],
            'entry_fee': c['entry_fee'],
            'prize_pool': c['prize_pool'],
            'max_entries': c['max_entries'],
            'entry_count': c['entry_count'],
            'max_per_user': c['max_per_user'],
            'positions_paid': positions_paid,
            'payout_pct': payout_pct,
            'first_place': first_place,
            'min_cash': min_cash,
            'dk_slate': c['dk_slate'],
            'contest_type': c['contest_type'],
            'game_date': c['game_date'] or None,
            'payout_json': json.dumps(payout_json) if payout_json else None,
            'fetched_at': datetime.now(timezone.utc).isoformat(),
        }
        records.append(record)

        if (idx + 1) % 10 == 0:
            print(f"    Fetched {idx + 1}/{len(to_fetch)}")

    # 4. Upsert to dk_contests
    print(f"\n  Upserting {len(records)} contests...")
    BATCH = 50
    uploaded = 0
    for i in range(0, len(records), BATCH):
        batch = records[i:i+BATCH]
        sb.table('dk_contests').upsert(
            batch, on_conflict='contest_id', ignore_duplicates=False
        ).execute()
        uploaded += len(batch)
    print(f"  Uploaded {uploaded} contests")

    # 5. Summary
    slates = {}
    for r in records:
        sl = r['dk_slate']
        if sl not in slates:
            slates[sl] = []
        slates[sl].append(r)

    print(f"\n  {'─'*55}")
    print(f"  Contest Summary")
    print(f"  {'─'*55}")

    for sl in sorted(slates):
        cs = slates[sl]
        print(f"\n  [{sl}] {len(cs)} contests:")
        for c in cs[:5]:
            pp = f"${c['prize_pool']:,.0f}" if c['prize_pool'] else "—"
            fp = f"${c['first_place']:,.0f}" if c['first_place'] else "—"
            print(f"    ${c['entry_fee']:>6}  {pp:>12}  1st={fp:>10}  "
                  f"max={c['max_entries']:>6,}  paid={c['payout_pct']:.1f}%  "
                  f"{c['name'][:45]}")
        if len(cs) > 5:
            print(f"    ... and {len(cs) - 5} more")

    print(f"\n  Done. {len(records)} contests loaded.")


if __name__ == '__main__':
    run()
