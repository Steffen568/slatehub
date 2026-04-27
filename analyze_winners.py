#!/usr/bin/env python3
"""
GPP Game Theory Analysis — Contest CSV Postgame Review

Parses DraftKings contest-standings CSVs and analyzes winning lineup
patterns through a game-theory lens: leverage, pitcher selection,
stack construction, and structural comparison against our sim pool.

Run:
  py -3.12 analyze_winners.py                          # all CSVs in repo
  py -3.12 analyze_winners.py contest-standings-*.csv  # specific files
"""
import sys
sys.stdout.reconfigure(encoding='utf-8', line_buffering=True)
import csv, re, glob, os, argparse
from collections import Counter, defaultdict
import numpy as np


# ── CSV Parsing ─────────────────────────────────────────────────────────────

POS_TAGS = ['1B', '2B', '3B', 'SS', 'C', 'OF', 'P']
POS_PATTERN = re.compile(r'\b(' + '|'.join(POS_TAGS) + r')\s+')


def parse_contest_csv(fpath):
    """Parse a DK contest-standings CSV into structured entry data.

    Returns (entries, player_data, contest_id, n_entries) where:
      entries = list of dicts with lineup structural info
      player_data = {name: {own, fpts}} for all players
    """
    contest_id = os.path.basename(fpath).replace('contest-standings-', '').replace('.csv', '')

    player_data = {}  # name -> {own, fpts}
    entries_raw = []

    with open(fpath, encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                rank = int(row['Rank'])
                pts = float(row['Points'] or 0)
            except (ValueError, KeyError):
                continue

            pname = row.get('Player', '')
            own_raw = (row.get('%Drafted') or '0').replace('%', '').strip()
            fpts_raw = row.get('FPTS') or '0'
            try:
                own = float(own_raw)
                fpts = float(fpts_raw)
            except ValueError:
                own, fpts = 0, 0

            if pname and pname not in player_data:
                player_data[pname] = {'own': own, 'fpts': fpts}

            entries_raw.append({
                'rank': rank,
                'points': pts,
                'lineup_str': row.get('Lineup', ''),
            })

    n_entries = len(entries_raw)
    entries = []

    for entry in entries_raw:
        lu = entry['lineup_str']
        if not lu:
            continue

        parts = POS_PATTERN.split(lu)
        players = []
        i = 1
        while i < len(parts) - 1:
            pos = parts[i]
            name = parts[i + 1].strip()
            pd = player_data.get(name, {'own': 0, 'fpts': 0})
            players.append({'pos': pos, 'name': name, 'own': pd['own'], 'fpts': pd['fpts']})
            i += 2

        if len(players) != 10:
            continue

        owns = [p['own'] for p in players]
        pitchers = [p for p in players if p['pos'] == 'P']
        hitters = [p for p in players if p['pos'] != 'P']

        # Identify stacks — group hitters by name pattern isn't possible,
        # but we can group by the player_data to find team correlation later.
        # For now, we identify "same-team groups" by checking which hitters
        # appear together most often in top lineups.

        pitcher_pts = sum(p['fpts'] for p in pitchers)
        hitter_pts = sum(p['fpts'] for p in hitters)
        total_pts = entry['points']
        total_own = sum(owns)

        entries.append({
            'rank': entry['rank'],
            'points': total_pts,
            'pct_rank': entry['rank'] / n_entries * 100,
            'players': players,
            'pitchers': pitchers,
            'hitters': hitters,
            'total_own': total_own,
            'avg_own': np.mean(owns),
            'min_own': min(owns),
            'max_own': max(owns),
            'pitcher_pts': pitcher_pts,
            'hitter_pts': hitter_pts,
            'pitcher_pct': pitcher_pts / max(total_pts, 1) * 100,
            'chalk_count': sum(1 for o in owns if o >= 20),
            'low_own_count': sum(1 for o in owns if o < 5),
            'bust_count': sum(1 for p in players if p['fpts'] < 3),
            'boom_count': sum(1 for p in hitters if p['fpts'] >= 15),
            'smash_count': sum(1 for p in hitters if p['fpts'] >= 20),
            'top_player_pts': max(p['fpts'] for p in players),
        })

    return entries, player_data, contest_id, n_entries


# ── Section 1: Player-Level Leverage ────────────────────────────────────────

def section_leverage(player_data):
    """Analyze player leverage: ownership vs actual performance."""
    print(f'\n{"="*80}')
    print(f'  SECTION 1: PLAYER LEVERAGE ANALYSIS')
    print(f'{"="*80}')

    players = [(name, d['own'], d['fpts']) for name, d in player_data.items()]
    players.sort(key=lambda x: x[1], reverse=True)

    # Leverage hits: low owned + high scoring
    leverage_hits = [(n, o, f) for n, o, f in players if o < 10 and f >= 20]
    leverage_hits.sort(key=lambda x: x[2], reverse=True)

    # Chalk traps: high owned + busted
    chalk_traps = [(n, o, f) for n, o, f in players if o >= 20 and f < 10]
    chalk_traps.sort(key=lambda x: x[1], reverse=True)

    # Chalk delivers: high owned + delivered
    chalk_delivers = [(n, o, f) for n, o, f in players if o >= 20 and f >= 20]
    chalk_delivers.sort(key=lambda x: x[1], reverse=True)

    print(f'\n  Leverage Hits (<10% own, 20+ pts) — these win GPPs:')
    for name, own, fpts in leverage_hits[:10]:
        field_beat = 100 - own
        print(f'    {name:25s}  {own:5.1f}% own  {fpts:5.1f} pts  (beat {field_beat:.0f}% of field at this slot)')

    print(f'\n  Chalk Traps (>20% own, <10 pts) — these sink the field:')
    for name, own, fpts in chalk_traps[:10]:
        print(f'    {name:25s}  {own:5.1f}% own  {fpts:5.1f} pts  (dragged down {own:.0f}% of field)')

    print(f'\n  Chalk Delivers (>20% own, 20+ pts) — field consensus correct:')
    for name, own, fpts in chalk_delivers[:10]:
        print(f'    {name:25s}  {own:5.1f}% own  {fpts:5.1f} pts')

    # Summary stats
    n_lev = len(leverage_hits)
    n_trap = len(chalk_traps)
    n_deliver = len(chalk_delivers)
    total_chalk = sum(1 for _, o, _ in players if o >= 20)
    print(f'\n  Summary:')
    print(f'    Leverage hits (<10% own, 20+ pts):     {n_lev}')
    print(f'    Chalk traps (>20% own, <10 pts):       {n_trap} / {total_chalk} chalk players ({n_trap/max(total_chalk,1)*100:.0f}% bust rate)')
    print(f'    Chalk delivers (>20% own, 20+ pts):    {n_deliver} / {total_chalk} chalk players ({n_deliver/max(total_chalk,1)*100:.0f}% hit rate)')

    return {'leverage_hits': leverage_hits, 'chalk_traps': chalk_traps, 'chalk_delivers': chalk_delivers}


# ── Section 2: Pitcher Game Theory ──────────────────────────────────────────

def section_pitcher_gt(entries, player_data):
    """Analyze pitcher selection through game theory lens."""
    print(f'\n{"="*80}')
    print(f'  SECTION 2: PITCHER GAME THEORY')
    print(f'{"="*80}')

    # Identify all pitchers (players at P position in lineups)
    pitcher_names = set()
    for e in entries:
        for p in e['pitchers']:
            pitcher_names.add(p['name'])

    # Build pitcher stats
    pitcher_stats = {}
    for name in pitcher_names:
        pd = player_data.get(name, {'own': 0, 'fpts': 0})
        pitcher_stats[name] = {
            'own': pd['own'],
            'fpts': pd['fpts'],
            'in_top1': 0,
            'in_top5': 0,
            'in_top10': 0,
            'total_appearances': 0,
        }

    n_top1 = 0
    n_top5 = 0
    n_top10 = 0

    for e in entries:
        is_top1 = e['pct_rank'] <= 1
        is_top5 = e['pct_rank'] <= 5
        is_top10 = e['pct_rank'] <= 10

        if is_top1:
            n_top1 += 1
        if is_top5:
            n_top5 += 1
        if is_top10:
            n_top10 += 1

        for p in e['pitchers']:
            ps = pitcher_stats.get(p['name'])
            if ps:
                ps['total_appearances'] += 1
                if is_top1:
                    ps['in_top1'] += 1
                if is_top5:
                    ps['in_top5'] += 1
                if is_top10:
                    ps['in_top10'] += 1

    # Sort by ownership (chalk first)
    sorted_pitchers = sorted(pitcher_stats.items(), key=lambda x: x[1]['own'], reverse=True)

    print(f'\n  Pitcher Leverage Table:')
    print(f'  {"Name":25s} {"Own%":>6s} {"FPTS":>6s} {"Top1%":>7s} {"Top5%":>7s} {"Top10%":>7s} {"Verdict":>10s}')
    print(f'  {"-"*74}')

    for name, ps in sorted_pitchers[:15]:
        top1_pct = ps['in_top1'] / max(n_top1, 1) * 100
        top5_pct = ps['in_top5'] / max(n_top5, 1) * 100
        top10_pct = ps['in_top10'] / max(n_top10, 1) * 100
        own = ps['own']

        # Leverage verdict
        if top1_pct > own * 1.3:
            verdict = 'LEVERAGE+'
        elif top1_pct < own * 0.5 and own >= 10:
            verdict = 'TRAP'
        elif top1_pct >= own * 0.8:
            verdict = 'fair'
        else:
            verdict = 'fade'

        print(f'  {name:25s} {own:5.1f}% {ps["fpts"]:5.1f}  {top1_pct:5.1f}%  {top5_pct:5.1f}%  {top10_pct:5.1f}%  {verdict:>10s}')

    # Chalk pitcher analysis
    if sorted_pitchers:
        chalk_sp = sorted_pitchers[0]
        chalk_name, chalk_ps = chalk_sp
        print(f'\n  Chalk Pitcher Analysis:')
        print(f'    Highest owned SP: {chalk_name} ({chalk_ps["own"]:.1f}% own, {chalk_ps["fpts"]:.1f} pts)')

        top1_with_chalk = chalk_ps['in_top1']
        top1_without = n_top1 - top1_with_chalk
        print(f'    Top 1% lineups WITH {chalk_name}: {top1_with_chalk} ({top1_with_chalk/max(n_top1,1)*100:.1f}%)')
        print(f'    Top 1% lineups WITHOUT: {top1_without} ({top1_without/max(n_top1,1)*100:.1f}%)')

        if chalk_ps['fpts'] < 20 and chalk_ps['own'] >= 30:
            print(f'    >> CHALK TRAP: {chalk_name} was {chalk_ps["own"]:.0f}% owned but only scored {chalk_ps["fpts"]:.1f} pts')
            print(f'       Fading him gave you {chalk_ps["own"]:.0f}% field separation for free')

    return pitcher_stats


# ── Section 3: Stack Analysis ───────────────────────────────────────────────

def section_stacks(entries, player_data):
    """Analyze stack patterns in winning lineups.

    Since the CSV doesn't have team info, we identify stacks by finding
    groups of hitters that frequently appear together in top lineups.
    We also analyze the structural stack sizes by looking at co-occurrence.
    """
    print(f'\n{"="*80}')
    print(f'  SECTION 3: STACK PATTERN ANALYSIS')
    print(f'{"="*80}')

    # We can't directly identify teams from the CSV, but we can:
    # 1. Find groups of hitters that always appear together (implied stacks)
    # 2. Count how many unique hitter "clusters" each lineup has

    # Build co-occurrence matrix for hitters in top-1% lineups
    top1 = [e for e in entries if e['pct_rank'] <= 1]
    all_e = entries

    # Hitter frequency in top-1%
    hitter_freq_top1 = Counter()
    hitter_freq_all = Counter()
    for e in top1:
        for h in e['hitters']:
            hitter_freq_top1[h['name']] += 1
    for e in all_e:
        for h in e['hitters']:
            hitter_freq_all[h['name']] += 1

    # Co-occurrence: which hitters appear together most in top-1%?
    pair_count = Counter()
    for e in top1:
        hitter_names = [h['name'] for h in e['hitters']]
        for i in range(len(hitter_names)):
            for j in range(i + 1, len(hitter_names)):
                pair = tuple(sorted([hitter_names[i], hitter_names[j]]))
                pair_count[pair] += 1

    # Identify likely stacks: groups of 3+ players that co-occur in >50% of their appearances
    print(f'\n  Most Common Hitter Pairs in Top 1% (likely stack mates):')
    print(f'  {"Player A":25s} {"Player B":25s} {"Together":>8s} {"% of Top1":>10s}')
    print(f'  {"-"*72}')
    for (a, b), count in pair_count.most_common(15):
        pct_top1 = count / max(len(top1), 1) * 100
        print(f'  {a:25s} {b:25s} {count:>6d}    {pct_top1:>6.1f}%')

    # Hitter leverage: who appears MORE in top-1% than their ownership suggests?
    print(f'\n  Hitter Leverage (top-1% rate vs ownership):')
    print(f'  {"Name":25s} {"Own%":>6s} {"FPTS":>6s} {"Top1% Rate":>10s} {"Leverage":>10s}')
    print(f'  {"-"*62}')

    hitter_lev = []
    for name, count in hitter_freq_top1.most_common(30):
        pd = player_data.get(name, {'own': 0, 'fpts': 0})
        top1_rate = count / max(len(top1), 1) * 100
        own = pd['own']
        leverage = top1_rate - own
        hitter_lev.append((name, own, pd['fpts'], top1_rate, leverage))

    hitter_lev.sort(key=lambda x: x[4], reverse=True)
    for name, own, fpts, t1r, lev in hitter_lev[:15]:
        lev_str = f'+{lev:.1f}' if lev > 0 else f'{lev:.1f}'
        print(f'  {name:25s} {own:5.1f}% {fpts:5.1f}  {t1r:8.1f}%  {lev_str:>10s}')


# ── Section 4: Structural Profile Comparison ────────────────────────────────

def section_structural(entries):
    """Compare structural profiles across finish tiers."""
    print(f'\n{"="*80}')
    print(f'  SECTION 4: STRUCTURAL PROFILE BY FINISH TIER')
    print(f'{"="*80}')

    top1 = [e for e in entries if e['pct_rank'] <= 1]
    top5 = [e for e in entries if e['pct_rank'] <= 5]
    top10 = [e for e in entries if e['pct_rank'] <= 10]
    cash = [e for e in entries if e['pct_rank'] <= 22]
    bottom50 = [e for e in entries if e['pct_rank'] > 50]

    def avg(g, f):
        return np.mean([e[f] for e in g]) if g else 0

    header = f'  {"Metric":<28} {"Top 1%":>8} {"Top 5%":>8} {"Top 10%":>8} {"Cash":>8} {"Bot 50%":>8}'
    print(f'\n{header}')
    print(f'  {"-"*76}')

    for label, field in [
        ('Avg Points', 'points'),
        ('Total Lineup Own%', 'total_own'),
        ('Avg Player Own%', 'avg_own'),
        ('Chalk Players (>20%)', 'chalk_count'),
        ('Low-Own Players (<5%)', 'low_own_count'),
        ('Pitcher Pts', 'pitcher_pts'),
        ('Hitter Pts', 'hitter_pts'),
        ('Pitcher % of Total', 'pitcher_pct'),
        ('Top Player FPTS', 'top_player_pts'),
        ('Busts (<3 pts)', 'bust_count'),
        ('Booms (>=15 hitter)', 'boom_count'),
        ('Smashes (>=20 hitter)', 'smash_count'),
    ]:
        row = f'  {label:<28}'
        for group in [top1, top5, top10, cash, bottom50]:
            row += f' {avg(group, field):>8.1f}'
        print(row)

    # Correlations with finish
    print(f'\n  Correlation with Finish (negative = HELPS ranking):')
    print(f'  {"-"*50}')
    ranks = np.array([e['pct_rank'] for e in entries])
    for label, field in [
        ('Total Lineup Ownership', 'total_own'),
        ('Chalk Count (>20%)', 'chalk_count'),
        ('Pitcher Pts', 'pitcher_pts'),
        ('Hitter Pts', 'hitter_pts'),
        ('Boom Count', 'boom_count'),
        ('Bust Count', 'bust_count'),
        ('Top Player Pts', 'top_player_pts'),
    ]:
        vals = np.array([e[field] for e in entries])
        r = np.corrcoef(vals, ranks)[0, 1]
        direction = 'HELPS' if r < -0.05 else 'HURTS' if r > 0.05 else 'neutral'
        print(f'    {label:<28} r={r:+.3f}  {direction}')

    # Pitcher contribution
    print(f'\n  Pitcher Contribution:')
    for label, group in [('Top 1%', top1), ('Top 10%', top10), ('Bottom 50%', bottom50)]:
        if not group:
            continue
        elite_p = sum(1 for e in group if e['pitcher_pts'] >= 25) / len(group) * 100
        good_p = sum(1 for e in group if e['pitcher_pts'] >= 20) / len(group) * 100
        bad_p = sum(1 for e in group if e['pitcher_pts'] < 10) / len(group) * 100
        print(f'    {label:10}: avg {avg(group, "pitcher_pts"):.1f} pts | >=25: {elite_p:.0f}% | >=20: {good_p:.0f}% | <10: {bad_p:.0f}%')

    # Bust tolerance
    print(f'\n  Bust Tolerance:')
    for label, group in [('Top 1%', top1), ('Top 10%', top10), ('Cash', cash), ('Bottom 50%', bottom50)]:
        if not group:
            continue
        busts = [e['bust_count'] for e in group]
        dist = Counter(int(b) for b in busts)
        n = len(group)
        print(f'    {label:10}: avg {np.mean(busts):.1f} busts | 0: {dist.get(0, 0) / n * 100:.0f}% | 1: {dist.get(1, 0) / n * 100:.0f}% | 2+: {sum(v for k, v in dist.items() if k >= 2) / n * 100:.0f}%')

    return {'top1_n': len(top1), 'total_n': len(entries)}


# ── Section 5: Slate Grades ─────────────────────────────────────────────────

def section_grades(entries, player_data, pitcher_stats):
    """Generate letter grades for slate-level decisions."""
    print(f'\n{"="*80}')
    print(f'  SECTION 5: SLATE GRADES')
    print(f'{"="*80}')

    top1 = [e for e in entries if e['pct_rank'] <= 1]
    if not top1:
        print('  No top-1% data available')
        return

    # Pitcher Grade: Did chalk pitcher deliver or trap?
    sorted_sps = sorted(pitcher_stats.items(), key=lambda x: x[1]['own'], reverse=True)
    if sorted_sps:
        chalk_sp = sorted_sps[0]
        chalk_name, chalk_ps = chalk_sp
        chalk_fpts = chalk_ps['fpts']
        chalk_own = chalk_ps['own']
        top1_rate = chalk_ps['in_top1'] / max(len(top1), 1) * 100

        if chalk_fpts >= 25 and top1_rate >= chalk_own * 0.8:
            p_grade = 'A'
            p_note = f'Chalk SP {chalk_name} delivered ({chalk_fpts:.0f} pts at {chalk_own:.0f}% own)'
        elif chalk_fpts >= 20:
            p_grade = 'B'
            p_note = f'Chalk SP {chalk_name} was fine ({chalk_fpts:.0f} pts) but not a separator'
        elif chalk_fpts >= 15:
            p_grade = 'C'
            p_note = f'Chalk SP {chalk_name} was mediocre ({chalk_fpts:.0f} pts at {chalk_own:.0f}% own)'
        else:
            p_grade = 'F'
            p_note = f'Chalk SP {chalk_name} BUSTED ({chalk_fpts:.0f} pts at {chalk_own:.0f}% own) — faders had leverage'

        # Check if a contrarian pitcher was the real play
        best_lev_sp = max(sorted_sps, key=lambda x: x[1]['in_top1'] / max(len(top1), 1) * 100 - x[1]['own'])
        if best_lev_sp[0] != chalk_name and best_lev_sp[1]['fpts'] > chalk_fpts:
            p_note += f'\n           Best leverage SP: {best_lev_sp[0]} ({best_lev_sp[1]["own"]:.0f}% own, {best_lev_sp[1]["fpts"]:.0f} pts)'

        print(f'\n  Pitcher Selection:  {p_grade}')
        print(f'    {p_note}')

    # Ownership Profile Grade
    avg_total_own_top1 = np.mean([e['total_own'] for e in top1])
    if 100 <= avg_total_own_top1 <= 150:
        o_grade = 'A'
    elif 80 <= avg_total_own_top1 <= 170:
        o_grade = 'B'
    else:
        o_grade = 'C'
    print(f'\n  Ownership Profile:  {o_grade}')
    print(f'    Top 1% avg total ownership: {avg_total_own_top1:.0f}% (sweet spot: 100-150%)')

    # Boom Grade
    avg_boom_top1 = np.mean([e['boom_count'] for e in top1])
    avg_smash_top1 = np.mean([e['smash_count'] for e in top1])
    if avg_boom_top1 >= 3.5:
        b_grade = 'A'
    elif avg_boom_top1 >= 2.5:
        b_grade = 'B'
    else:
        b_grade = 'C'
    print(f'\n  Ceiling / Boom:     {b_grade}')
    print(f'    Top 1% avg booms (15+ hitter): {avg_boom_top1:.1f} | smashes (20+): {avg_smash_top1:.1f}')

    # Bust Grade
    avg_bust_top1 = np.mean([e['bust_count'] for e in top1])
    if avg_bust_top1 <= 1.0:
        bust_grade = 'A'
    elif avg_bust_top1 <= 1.5:
        bust_grade = 'B'
    else:
        bust_grade = 'C'
    print(f'\n  Bust Avoidance:     {bust_grade}')
    print(f'    Top 1% avg busts (<3 pts): {avg_bust_top1:.1f}')


# ── Main ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description='GPP Game Theory Analysis')
    parser.add_argument('files', nargs='*', help='Contest CSV files (default: all contest-standings-*.csv)')
    args = parser.parse_args()

    csv_dir = r"C:\Users\Steffen's PC\Desktop\WebDev\Contest_CSVs"
    files = args.files or sorted(glob.glob(os.path.join(csv_dir, 'contest-standings-*.csv')))
    if not files:
        files = sorted(glob.glob('contest-standings-*.csv'))
    if not files:
        print('No contest-standings CSV files found.')
        return

    # ── Per-contest analysis (ownership is slate-specific, never merge) ──
    all_entries_for_structure = []  # only used for aggregate structural patterns

    for fpath in files:
        entries, player_data, contest_id, n_entries = parse_contest_csv(fpath)
        print(f'  {contest_id}: {n_entries:,} entries, {len(entries):,} parsed, {len(player_data)} players')

        if not entries:
            continue

        print(f'\n{"#"*80}')
        print(f'  CONTEST: {contest_id} ({n_entries:,} entries)')
        print(f'{"#"*80}')

        # Sections 1-2, 5: per-contest (ownership is contest-specific)
        section_leverage(player_data)
        pitcher_stats = section_pitcher_gt(entries, player_data)
        section_grades(entries, player_data, pitcher_stats)

        all_entries_for_structure.extend(entries)

    if not all_entries_for_structure:
        print('No valid entries parsed.')
        return

    # ── Aggregate structural patterns (ownership-independent metrics) ──
    print(f'\n{"#"*80}')
    print(f'  AGGREGATE STRUCTURAL PATTERNS ({len(files)} contests, {len(all_entries_for_structure):,} entries)')
    print(f'{"#"*80}')

    section_structural(all_entries_for_structure)

    # Cross-contest summary
    print(f'\n{"="*80}')
    print(f'  KEY TAKEAWAYS (aggregate)')
    print(f'{"="*80}')

    top1 = [e for e in all_entries_for_structure if e['pct_rank'] <= 1]
    if top1:
        avg_own = np.mean([e['total_own'] for e in top1])
        avg_boom = np.mean([e['boom_count'] for e in top1])
        avg_bust = np.mean([e['bust_count'] for e in top1])
        avg_pp = np.mean([e['pitcher_pts'] for e in top1])
        print(f'  - Top 1% profile: {avg_own:.0f}% total own, {avg_boom:.1f} booms, {avg_bust:.1f} busts, {avg_pp:.0f} pitcher pts')
        print(f'  - Build to: ownership 100-150%, 3+ booms, <1 bust, 25+ pitcher pts')

    # Append summary to research_findings.md
    from datetime import date
    findings_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'tasks', 'research_findings.md')
    try:
        lines = [f'\n## Winner Pattern Analysis — {date.today().isoformat()} ({len(files)} contests, {len(all_entries_for_structure):,} entries)']
        if top1:
            lines.append(f'- Top 1% profile: {avg_own:.0f}% total own, {avg_boom:.1f} booms, {avg_bust:.1f} busts, {avg_pp:.0f} pitcher pts')
            lines.append(f'- Target: ownership 100-150%, 3+ booms, <1 bust, 25+ pitcher pts')
        with open(findings_path, 'a', encoding='utf-8') as f:
            f.write('\n'.join(lines) + '\n')
        print(f'\n  Findings appended to {findings_path}')
    except Exception as e:
        print(f'\n  ERROR writing findings: {e}')


if __name__ == '__main__':
    main()
