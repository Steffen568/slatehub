#!/usr/bin/env python3
"""Compare RTS projections vs our system projections for 5/1."""
import os
import unicodedata
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()
sb = create_client(os.environ['SUPABASE_URL'], os.environ['SUPABASE_KEY'])

TARGET_DATE = '2026-05-01'

# ── RTS projections: name → (proj, own%) ────────────────────────────────────
RTS_PITCHERS = {
    'warren': (17.23, 22.1),
    'young': (7.81, 0.6),
    'wheeler': (18.57, 28.5),
    'perez': (14.80, 8.3),
    'crochet': (18.96, 17.5),
    'burrows': (13.46, 3.4),
    'mcclanahan': (15.38, 9.8),
    'ray': (13.96, 9.2),
    'corbin': (9.47, 2.2),
    'woods richardson': (10.81, 1.4),
    'sheehan': (17.64, 8.7),
    'liberatore': (8.85, 4.5),
    'holmes': (13.13, 3.2),
    'quintana': (5.31, 0.2),
    'urena': (11.40, 3.8),
    'scott': (11.21, 2.6),
    'cantillo': (12.05, 8.6),
    'ginn': (8.23, 1.7),
    'schultz': (12.33, 3.5),
    'marquez': (9.35, 2.4),
    'ragans': (20.40, 39.5),
    'woo': (15.47, 18.4),
}

RTS_HITTERS = {
    # Yankees
    'judge': (9.98, 12.9),
    'chisholm': (8.06, 6.1),
    'bellinger': (8.25, 5.6),
    'rosario': (7.14, 5.2),
    'goldschmidt': (6.94, 5.1),
    # Orioles
    'henderson': (8.48, 5.1),
    # Phillies
    'stott': (8.29, 7.1),
    'turner': (8.34, 5.8),
    'schwarber': (9.12, 5.3),
    'harper': (8.64, 4.8),
    # Red Sox
    'duran': (8.85, 12.0),
    'story': (7.27, 10.9),
    'abreu': (8.18, 7.4),
    'anthony': (8.43, 7.2),
    'contreras': (6.30, None),
    'durbin': (6.58, 5.9),
    'rafaela': (6.96, 4.6),
    'narvaez': (5.81, 4.0),
    'mayer': (6.09, 3.9),
    # Rays
    'diaz': (9.17, 9.1),
    'williamson': (6.73, 5.6),
    'caminero': (8.85, 5.4),
    'vilade': (6.99, 4.7),
    'mullins': (7.18, 2.6),
    'deluca': (6.72, 2.2),
    'aranda': (7.08, 1.8),
    'walls': (6.11, 1.7),
    'fortes': (5.61, 1.0),
    # Blue Jays
    'clement': (7.83, 7.8),
    'springer': (9.83, 5.6),
    'schneider': (6.80, 3.8),
    'sanchez': (7.39, 3.5),
    'varsho': (7.50, 3.2),
    'okamoto': (7.81, 3.0),
    'guerrero': (8.74, 2.9),
    'gimenez': (6.81, 2.4),
    # Cardinals
    'wetherholt': (7.49, 1.7),
    'burleson': (8.04, 1.5),
    'herrera': (6.76, 1.5),
    'walker': (6.38, 0.8),
    'winn': (5.76, 0.8),
    'gorman': (5.61, 0.4),
    'church': (4.90, 0.5),
    'pages': (4.61, 0.4),
    # Braves
    'acuna': (11.79, 24.3),
    'riley': (9.87, 18.52),
    'albies': (9.72, 18.4),
    'baldwin': (9.62, 18.4),
    'olson': (10.14, 15.2),
    'dubon': (7.80, 11.7),
    'harris': (8.61, 9.8),
    'white': (6.91, 6.4),
    'heim': (6.97, 6.2),
    # Rockies
    'karros': (6.85, 3.5),
    'doyle': (7.44, 3.9),
    'romero': (7.74, 4.5),  # 'rumfield' likely Romero
    'mccarthy': (6.43, 2.1),
    'moniak': (9.29, 7.8),
    'julien': (8.12, 6.4),
    'goodman': (8.81, 6.4),
    'tovar': (7.02, 3.5),
    # Mets
    'soto': (10.59, 7.5),
    'bichette': (7.82, 5.7),
    'alvarez': (7.72, 7.4),
    'semien': (7.25, 4.1),
    'baty': (7.12, 3.5),
    'taylor': (6.81, 7.0),
    'mauricio': (6.56, 4.5),
    'viento': (6.49, 2.8),
    'benge': (5.96, 1.4),
    # Angels
    'trout': (8.84, 5.2),
    'neto': (8.49, 7.6),
    'adell': (7.66, 2.7),
    'moncada': (7.05, 3.1),
    'soler': (6.70, 2.8),
    'lowe': (6.66, 2.7),
    'schanuel': (6.65, 2.5),
    # Guardians
    'ramirez': (11.17, 12.8),
    'kwan': (8.68, 6.6),
    'delauter': (8.59, 5.5),
    'martinez': (7.84, 3.4),
    'hoskins': (7.55, 4.6),
    'manzardo': (7.74, 5.6),
    'bazzana': (7.06, 3.8),
    'naylor': (6.88, 3.8),
    'rocchio': (6.52, 2.7),
    # White Sox
    'murakami': (9.99, 4.8),
    'vargas': (9.19, 6.0),
    'montgomery': (7.91, 5.4),
    'benintendi': (7.75, 5.2),
    'antonacci': (6.86, 4.9),
    'meidroth': (6.49, 2.1),
    'quero': (5.75, 1.7),
    'peters': (5.62, 2.2),
    # Padres
    'tatis': (9.43, 10.1),
    'bogaerts': (7.93, 6.4),
    'machado': (7.59, 3.8),
    'merrill': (7.57, 3.5),
    'laureano': (7.00, 2.5),
    'andujar': (6.02, 2.8),
    'france': (4.80, 1.6),
    'cronenworth': (5.79, 2.0),
    'campusano': (5.43, 1.4),
}


def norm(name: str) -> str:
    """Normalize name: strip diacritics, lowercase, keep letters/spaces."""
    n = unicodedata.normalize('NFD', name).encode('ASCII', 'ignore').decode('ASCII')
    return n.lower().strip()


def last_name(full_name: str) -> str:
    """Extract last name from full name."""
    parts = norm(full_name).split()
    return parts[-1] if parts else ''


def match_key(full_name: str, rts_keys) -> str | None:
    """Try to match a full player name to an RTS key."""
    ln = last_name(full_name)
    fn = norm(full_name)
    # Exact last-name match first
    if ln in rts_keys:
        return ln
    # Multi-word key check (e.g. 'woods richardson')
    for k in rts_keys:
        if k in fn:
            return k
    # Partial last-name starts-with
    for k in rts_keys:
        if ln.startswith(k) or k.startswith(ln):
            return k
    return None


# ── Pull our projections for today ──────────────────────────────────────────
print(f"Loading our projections for {TARGET_DATE}...")
rows = []
offset = 0
while True:
    chunk = (sb.table('player_projections')
               .select('player_id,full_name,is_pitcher,proj_dk_pts,proj_ownership,team')
               .eq('game_date', TARGET_DATE)
               .range(offset, offset + 999)
               .execute())
    rows += chunk.data
    if len(chunk.data) < 1000:
        break
    offset += 1000

print(f"  Loaded {len(rows)} projection rows")

# Split pitchers / hitters
our_pitchers = [r for r in rows if r.get('is_pitcher')]
our_hitters  = [r for r in rows if not r.get('is_pitcher')]

# ── Compare ──────────────────────────────────────────────────────────────────
def compare(our_players, rts_dict, label):
    print(f"\n{'='*70}")
    print(f"  {label}  --  RTS vs Ours  (sorted by |delta|)")
    print(f"{'='*70}")
    print(f"  {'Player':<22} {'RTS':>6}  {'Ours':>6}  {'D':>6}  {'RTS%':>6}  {'Our%':>6}  {'Down':>6}")
    print(f"  {'-'*62}")

    matched = []
    unmatched_rts = set(rts_dict.keys())

    for p in our_players:
        key = match_key(p['full_name'], rts_dict)
        if key is None:
            continue
        rts_proj, rts_own = rts_dict[key]
        our_proj = p.get('proj_dk_pts') or 0.0
        our_own  = p.get('proj_ownership') or 0.0
        delta    = our_proj - rts_proj
        d_own    = (our_own - rts_own) if rts_own is not None else None
        matched.append((abs(delta), p['full_name'], rts_proj, our_proj, delta,
                        rts_own, our_own, d_own, key))
        unmatched_rts.discard(key)

    matched.sort(key=lambda x: -x[0])

    for _, name, rp, op, delta, ro, oo, do, _ in matched:
        flag = ' <<<' if abs(delta) >= 3.0 else (' <<' if abs(delta) >= 2.0 else '')
        own_str = f'{ro:>6.1f}' if ro is not None else '   N/A'
        do_str  = f'{do:>+6.1f}' if do is not None else '   N/A'
        print(f"  {name:<22} {rp:>6.2f}  {op:>6.2f}  {delta:>+6.2f}  {own_str}  {oo:>6.1f}  {do_str}{flag}")

    if unmatched_rts:
        print(f"\n  RTS players NOT matched in our system: {', '.join(sorted(unmatched_rts))}")

    # Summary stats
    deltas = [x[4] for x in matched]
    if deltas:
        import statistics
        avg_d = sum(deltas) / len(deltas)
        mae   = sum(abs(d) for d in deltas) / len(deltas)
        print(f"\n  Matched: {len(matched)}  |  Avg delta: {avg_d:+.2f}  |  MAE: {mae:.2f}")
        big = [x for x in matched if abs(x[4]) >= 2.0]
        print(f"  Big gaps (>=2 pts): {len(big)}")


compare(our_pitchers, RTS_PITCHERS, 'PITCHERS')
compare(our_hitters,  RTS_HITTERS,  'HITTERS')
