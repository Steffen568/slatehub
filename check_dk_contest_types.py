"""
Diagnostic: print all MLB gameType values and DG info from DK lobby API.
Run this to verify the gameType key used for Showdown contests.
"""
import urllib.request, json

HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

req = urllib.request.Request('https://www.draftkings.com/lobby/getcontests?sport=MLB', headers=HEADERS)
with urllib.request.urlopen(req, timeout=15) as r:
    data = json.loads(r.read())

contests  = data.get('Contests', [])
dg_list   = data.get('DraftGroups', [])

# Show all unique gameType values
game_types = {}
for c in contests:
    gt = c.get('gameType', 'MISSING')
    dg = c.get('dg')
    game_types.setdefault(gt, []).append(dg)

print("=== Contest gameType values ===")
for gt, dgs in sorted(game_types.items()):
    print(f"  '{gt}': {len(dgs)} contests, DG IDs: {sorted(set(dgs))[:5]}")

# Show DraftGroup metadata for any non-Classic DGs
classic_dgs = {c['dg'] for c in contests if c.get('gameType') == 'Classic' and c.get('dg')}
print("\n=== Non-Classic DraftGroups ===")
for dg in dg_list:
    dgid = dg.get('DraftGroupId')
    if dgid not in classic_dgs:
        games = dg.get('Games') or dg.get('games') or []
        print(f"  DG {dgid}: StartDate={dg.get('StartDateEst','')} Games={games}")

print("\n=== All DraftGroup keys (first entry) ===")
if dg_list:
    print(list(dg_list[0].keys()))
