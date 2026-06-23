"""
Season accuracy review -- reads sim_validation_history.csv + research_findings.md
and prints a structured season report to stdout.
"""
import csv
import re
import statistics
import sys
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

BASE = Path(__file__).parent
CSV_PATH = BASE / "sim_validation_history.csv"
RF_PATH = BASE / "tasks" / "research_findings.md"

# ─────────────────────────────────────────────────────────────────────────────
# 1. LOAD CSV
# ─────────────────────────────────────────────────────────────────────────────

def load_csv():
    rows = []
    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if not row["date"].strip():
                continue
            def _f(k):
                v = row.get(k, "").strip()
                return float(v) if v else None
            rows.append({
                "date": row["date"].strip(),
                "hitter_mae": _f("hitter_mae"),
                "pitcher_mae": _f("pitcher_mae"),
                "overall_bias": _f("overall_bias"),
                "overall_corr": _f("overall_corr"),
                "rank_corr_hitters": _f("rank_corr_hitters"),
                "rank_corr_pitchers": _f("rank_corr_pitchers"),
                "pct_hitters": _f("pct_in_p10_p90_hitters"),
                "pct_pitchers": _f("pct_in_p10_p90_pitchers"),
                "ip_mae": _f("ip_mae"),
                "ks_mae": _f("ks_mae"),
                "er_mae": _f("er_mae"),
                "own_mae": _f("own_mae"),
                "own_bias": _f("own_bias"),
                "pitcher_mult_corr": _f("pitcher_mult_corr"),
                "vegas_mult_corr": _f("vegas_mult_corr"),
                "context_mult_corr": _f("context_mult_corr"),
                "verdict_hitter": row.get("verdict_hitter_mae", "").strip(),
                "verdict_pitcher": row.get("verdict_pitcher_mae", "").strip(),
                "verdict_rank_corr_hitters": row.get("verdict_rank_corr_hitters", "").strip(),
            })
    return rows

# ─────────────────────────────────────────────────────────────────────────────
# 2. PARSE research_findings.md
# ─────────────────────────────────────────────────────────────────────────────

def parse_research_findings():
    text = RF_PATH.read_text(encoding="utf-8")

    # --- Ownership calibration: take the LAST block ---
    own_blocks = list(re.finditer(
        r"## Ownership Calibration.*?\n(.*?)(?=\n## |\Z)", text, re.DOTALL))
    own = {}
    if own_blocks:
        blk = own_blocks[-1].group(1)
        m = re.search(r"Matched players.*?(\d+)", blk)
        if m: own["matched"] = int(m.group(1))
        m = re.search(r"Bias.*?([+-]?\d+\.?\d*)\s*%", blk)
        if m: own["bias"] = float(m.group(1))
        m = re.search(r"MAE.*?(\d+\.?\d*)\s*%", blk)
        if m: own["mae"] = float(m.group(1))
        m = re.search(r"Correlation.*?r=([+-]?\d+\.?\d*)", blk)
        if m: own["corr"] = float(m.group(1))
        for tier, key in [("Chalk", "chalk"), ("Mid", "mid"), ("Low", "low")]:
            m = re.search(
                rf"{tier}.*?n=(\d+).*?bias=([+-]?\d+\.?\d*)%.*?MAE=(\d+\.?\d*)%", blk)
            if m:
                own[key] = {"n": int(m.group(1)), "bias": float(m.group(2)),
                            "mae": float(m.group(3))}
        # worst over/under
        over = re.findall(r"Over-projected.*?\n((?:- .+\n)+)", blk)
        under = re.findall(r"Under-projected.*?\n((?:- .+\n)+)", blk)
        if over: own["over_examples"] = [l.strip("- \n") for l in over[-1].strip().split("\n") if l.strip()][:3]
        if under: own["under_examples"] = [l.strip("- \n") for l in under[-1].strip().split("\n") if l.strip()][:3]

    # --- Contest thresholds: scan all Research Findings entries ---
    winners, top1, cashlines = [], [], []
    for m in re.finditer(
            r"\*\*Contest\*\*:.*?Winner=([0-9.]+).*?Top1%=([0-9.]+)", text):
        winners.append(float(m.group(1)))
        top1.append(float(m.group(2)))
    for m in re.finditer(r"cash line is.*?(\d+\.?\d*) pts", text):
        cashlines.append(float(m.group(1)))

    # --- Latest winner pattern ---
    pattern = {}
    wp_blocks = list(re.finditer(
        r"## Winner Pattern Analysis.*?\n- Top 1% profile: (.*?)\n", text))
    if wp_blocks:
        line = wp_blocks[-1].group(1)
        m = re.search(r"(\d+)% total own", line)
        if m: pattern["total_own"] = int(m.group(1))
        m = re.search(r"(\d+\.?\d+) booms", line)
        if m: pattern["booms"] = float(m.group(1))
        m = re.search(r"(\d+\.?\d+) busts", line)
        if m: pattern["busts"] = float(m.group(1))
        m = re.search(r"(\d+) pitcher pts", line)
        if m: pattern["pitcher_pts"] = int(m.group(1))

    # --- Latest leverage rules ---
    lev_blocks = list(re.finditer(
        r"### Actionable Rules\n((?:- .+\n)+)", text))
    lev_rules = []
    if lev_blocks:
        for line in lev_blocks[-1].group(1).strip().split("\n"):
            lev_rules.append(line.strip("- "))

    return own, winners, top1, cashlines, pattern, lev_rules

# ─────────────────────────────────────────────────────────────────────────────
# 3. COMPUTE STATS
# ─────────────────────────────────────────────────────────────────────────────

def _vals(rows, key):
    return [r[key] for r in rows if r[key] is not None]

def _mean(lst):
    return statistics.mean(lst) if lst else None

def _pct(lst, fn):
    hits = sum(1 for x in lst if fn(x))
    return 100 * hits / len(lst) if lst else None

def third_means(rows, key):
    vals = _vals(rows, key)
    n = len(vals)
    if n < 3:
        return None, None, None
    t = n // 3
    return _mean(vals[:t]), _mean(vals[t:2*t]), _mean(vals[2*t:])

def trend_label(early, recent, threshold=0.3):
    if early is None or recent is None:
        return "-"
    delta = recent - early
    if abs(delta) < threshold:
        return "STABLE"
    return "IMPROVING v" if delta < 0 else "DEGRADING ^"

def mult_verdict(corrs):
    if not corrs:
        return "NO DATA"
    pos = sum(1 for c in corrs if c > 0)
    neg = len(corrs) - pos
    if pos >= 2 * neg:
        return f"HELPS  (+{pos}/{len(corrs)} dates positive)"
    if neg >= 2 * pos:
        return f"HURTS  (-{neg}/{len(corrs)} dates negative)"
    return f"INCONSISTENT  (+{pos}/-{neg})"

# ─────────────────────────────────────────────────────────────────────────────
# 4. REPORT
# ─────────────────────────────────────────────────────────────────────────────

SEP = "=" * 72
SEP2 = "-" * 72

def p(s=""): print(s)

def section(title):
    p()
    p(SEP)
    p(f"  {title}")
    p(SEP)

def sub(title):
    p()
    p(f"  {title}")
    p("  " + "-" * (len(title) + 2))

def row3(label, early, mid, recent, fmt=".2f"):
    e = f"{early:{fmt}}" if early is not None else "  —  "
    m = f"{mid:{fmt}}" if mid is not None else "  —  "
    r = f"{recent:{fmt}}" if recent is not None else "  —  "
    print(f"  {label:<34} {e:>7}  {m:>7}  {r:>7}")

def row1(label, val, fmt=".2f", suffix=""):
    v = f"{val:{fmt}}{suffix}" if val is not None else "—"
    print(f"  {label:<40} {v}")


def main():
    rows = load_csv()
    own, winners, top1, cashlines, pattern, lev_rules = parse_research_findings()

    n = len(rows)
    # Split into thirds for trend
    t = n // 3
    early_rows = rows[:t]
    mid_rows   = rows[t:2*t]
    recent_rows = rows[2*t:]

    # ── HEADER ───────────────────────────────────────────────────────────────
    p()
    p(SEP)
    p("  SLATEHUB — 2026 SEASON ACCURACY REVIEW")
    p(f"  Dates: {rows[0]['date']} to {rows[-1]['date']}  ({n} validation dates)")
    p(SEP)

    # ── SECTION 1: PROJECTION ACCURACY ───────────────────────────────────────
    section("1. PROJECTION ACCURACY")

    h_all = _vals(rows, "hitter_mae")
    p_all = _vals(rows, "pitcher_mae")
    b_all = _vals(rows, "overall_bias")
    c_all = _vals(rows, "overall_corr")

    p()
    p(f"  {'Metric':<34} {'Season avg':>10}  {'Best':>7}  {'Worst':>7}")
    p("  " + SEP2)
    p(f"  {'Hitter MAE (target <5.0)':<34} {_mean(h_all):>10.2f}  {min(h_all):>7.2f}  {max(h_all):>7.2f}")
    p(f"  {'Pitcher MAE (target <7.0)':<34} {_mean(p_all):>10.2f}  {min(p_all):>7.2f}  {max(p_all):>7.2f}")
    if b_all:
        p(f"  {'Overall bias (+ = over-proj)':<34} {_mean(b_all):>10.2f}  {min(b_all):>7.2f}  {max(b_all):>7.2f}")
    if c_all:
        p(f"  {'Correlation r':<34} {_mean(c_all):>10.3f}  {min(c_all):>7.3f}  {max(c_all):>7.3f}")
    rk_all = _vals(rows, "rank_corr_hitters")
    if rk_all:
        p(f"  {'Rank Corr rho (GPP quality, >0.45)':<34} {_mean(rk_all):>10.3f}  {min(rk_all):>7.3f}  {max(rk_all):>7.3f}")

    # Pass rates
    h_pass = _pct(_vals(rows, "verdict_hitter"), lambda v: v == "PASS")
    p_pass = _pct(_vals(rows, "verdict_pitcher"), lambda v: v == "PASS")
    p()
    p(f"  Hitter PASS rate (MAE<5.5):  {h_pass:.0f}%  ({sum(1 for r in rows if r['verdict_hitter']=='PASS')}/{n} dates)")
    p(f"  Pitcher PASS rate (MAE<7.0): {p_pass:.0f}%  ({sum(1 for r in rows if r['verdict_pitcher']=='PASS')}/{n} dates)")

    # Trend table
    sub("Trend (early / mid / recent thirds)")
    h_e, h_m, h_r = third_means(rows, "hitter_mae")
    p_e, p_m, p_r = third_means(rows, "pitcher_mae")
    b_e, b_m, b_r = third_means(rows, "overall_bias")
    c_e, c_m, c_r = third_means(rows, "overall_corr")

    p(f"  {'Metric':<34} {'Early':>7}  {'Mid':>7}  {'Recent':>7}  Trend")
    p("  " + SEP2)
    p(f"  {'Hitter MAE':<34} {h_e:>7.2f}  {h_m:>7.2f}  {h_r:>7.2f}  {trend_label(h_e, h_r)}")
    p(f"  {'Pitcher MAE':<34} {p_e:>7.2f}  {p_m:>7.2f}  {p_r:>7.2f}  {trend_label(p_e, p_r)}")
    p(f"  {'Overall bias':<34} {b_e:>7.2f}  {b_m:>7.2f}  {b_r:>7.2f}  {trend_label(b_e, b_r, 0.2)}")
    p(f"  {'Correlation r':<34} {c_e:>7.3f}  {c_m:>7.3f}  {c_r:>7.3f}  {trend_label(c_r, c_e, 0.02)}")
    rk_e, rk_m, rk_r = third_means(rows, "rank_corr_hitters")
    if rk_r is not None:
        p(f"  {'Rank Corr rho (GPP)':<34} {rk_e:>7.3f}  {rk_m:>7.3f}  {rk_r:>7.3f}  {trend_label(rk_r, rk_e, 0.02)}")

    # Distribution calibration
    sub("Distribution Calibration (% actuals inside P10-P90, target 80%)")
    ph_all = _vals(rows, "pct_hitters")
    pp_all = _vals(rows, "pct_pitchers")
    p(f"  Hitters: avg={_mean(ph_all):.1f}%  min={min(ph_all):.1f}%  max={max(ph_all):.1f}%")
    p(f"  Pitchers: avg={_mean(pp_all):.1f}%  min={min(pp_all):.1f}%  max={max(pp_all):.1f}%")
    h_fail = sum(1 for v in ph_all if v < 80)
    p_fail = sum(1 for v in pp_all if v < 80)
    p(f"  Hitter dates <80%: {h_fail}/{len(ph_all)}  |  Pitcher dates <80%: {p_fail}/{len(pp_all)}")

    # Pitcher component breakdown
    sub("Pitcher Components (season avg MAE)")
    ip_v = _vals(rows, "ip_mae")
    ks_v = _vals(rows, "ks_mae")
    er_v = _vals(rows, "er_mae")
    if ip_v: p(f"  IP MAE: {_mean(ip_v):.2f}  (target <0.80)")
    if ks_v: p(f"  K  MAE: {_mean(ks_v):.2f}  (target <1.50)")
    if er_v: p(f"  ER MAE: {_mean(er_v):.2f}  (target <1.50)")

    # ── SECTION 2: MULTIPLIER EFFECTIVENESS ──────────────────────────────────
    section("2. CONTEXT MULTIPLIER EFFECTIVENESS")

    p()
    p(f"  {'Multiplier':<28} Avg r     Range          Verdict")
    p("  " + SEP2)

    for key, label in [
        ("pitcher_mult_corr", "pitcher_mult"),
        ("vegas_mult_corr",   "vegas_mult  "),
        ("context_mult_corr", "context_mult"),
    ]:
        vals = _vals(rows, key)
        if vals:
            avg = _mean(vals)
            verdict = mult_verdict(vals)
            p(f"  {label:<28} {avg:>+.3f}   [{min(vals):+.3f} … {max(vals):+.3f}]  {verdict}")

    p()
    p("  Note: weather_mult consistently r=+0.05 to +0.13 (HELPS modestly — from research_findings)")
    p("  Note: park_mult frequently r<0 (more often hurts — from research_findings)")

    # ── SECTION 3: OWNERSHIP MODEL ────────────────────────────────────────────
    section("3. OWNERSHIP MODEL ACCURACY")

    if own:
        p()
        p(f"  Season calibration ({own.get('matched','?')} matched player-slate pairs, 42 dates)")
        p()
        p(f"  Overall MAE:         {own.get('mae', '?'):.2f}%  (target <4.0%)")
        p(f"  Overall bias:        {own.get('bias', '?'):+.2f}%  (+ = over-projecting ownership)")
        p(f"  Correlation r:       {own.get('corr', '?'):.3f}  (target >0.70)")
        p()
        p(f"  {'Tier':<12} {'n':>6}  {'Bias':>8}  {'MAE':>8}  Assessment")
        p("  " + SEP2)
        for tier, key in [("Chalk >20%", "chalk"), ("Mid 5-20%", "mid"), ("Low <5%", "low")]:
            d = own.get(key, {})
            bias = d.get("bias", 0)
            mae  = d.get("mae", 0)
            n_t  = d.get("n", 0)
            note = ""
            if tier.startswith("Chalk") and bias < -3:
                note = "← under-projecting chalk"
            elif tier.startswith("Low") and bias > 2:
                note = "← over-projecting low-own"
            p(f"  {tier:<12} {n_t:>6}  {bias:>+7.2f}%  {mae:>7.2f}%  {note}")

        if own.get("over_examples"):
            p()
            p("  Biggest over-projection errors:")
            for ex in own["over_examples"]:
                p(f"    {ex}")
        if own.get("under_examples"):
            p()
            p("  Biggest under-projection errors:")
            for ex in own["under_examples"]:
                p(f"    {ex}")
    else:
        p("  (no ownership calibration block found in research_findings.md)")

    # ── SECTION 4: LINEUP BUILDER / POOL ─────────────────────────────────────
    section("4. LINEUP BUILDER / POOL QUALITY")

    if winners:
        avg_winner   = _mean(winners[-20:])  # recent 20 entries
        avg_top1     = _mean(top1[-20:])
        avg_cashline = _mean(cashlines[-20:]) if cashlines else None
        p()
        p("  Contest benchmarks (rolling recent window):")
        p(f"    Avg winner score:   {avg_winner:.1f} pts")
        p(f"    Avg top 1% score:   {avg_top1:.1f} pts")
        if avg_cashline:
            p(f"    Avg cash line:      {avg_cashline:.1f} pts")

    if pattern:
        p()
        p("  Winner profile (top 1% composite from leverage analysis):")
        p(f"    Total ownership:    ~{pattern.get('total_own','?')}%")
        p(f"    Booms (>2x salary): {pattern.get('booms','?'):.1f} per lineup")
        p(f"    Busts (<0.5x):      {pattern.get('busts','?'):.1f} per lineup")
        p(f"    Pitcher pts:        ~{pattern.get('pitcher_pts','?')} pts")

    p()
    p("  Pool performance (from slate reviews):")
    p("    - Top-by-projection overlap with actual winners: 0/20 on most dates")
    p("    - Best selection strategy: PMS (not projection rank)")
    p("    - Pool avg actual: ~85-105 pts (usually beats cash line)")
    p("    - Pool ceiling rarely reaches GPP line (143-195 pts)")
    p("    - Best stack configs: 4-0 / 5-0 (recent), 3-2 (mid-season)")

    # ── SECTION 5: LEVERAGE RULES ─────────────────────────────────────────────
    section("5. VALIDATED LEVERAGE RULES  (54 dates, 1.2M+ entries)")

    if lev_rules:
        p()
        for rule in lev_rules:
            p(f"  • {rule}")
    else:
        p("  (no leverage rules parsed)")

    # ── SECTION 6: ARCHETYPE BIASES ──────────────────────────────────────────
    section("6. PERSISTENT ARCHETYPE BIASES  (from June research window)")

    p()
    p("  Hitter archetypes (all consistently over-projected in June):")
    p("    Power   (ISO>.200):   over-projected by +1.3 to +1.6 pts")
    p("    Contact (K%<15%):    over-projected by +1.1 to +1.3 pts")
    p("    Speed   (SB pace>15): over-projected by +1.2 to +1.8 pts")
    p("    High barrel (>10%):  over-projected by +1.0 to +1.4 pts")
    p()
    p("  Recurring missing predictors with signal (r ≥ 0.08):")
    p("    Hitters: swstr_pct, gb_pct, ld_pct, wRC+, ISO")
    p("    Pitchers: gb_pct, bb9, whip, barrel_pct allowed, ld_pct allowed")
    p("    Pitcher vs opp: opp_k_pct, opp_xwoba, opp_woba, opp_wrc_plus")

    # ── SECTION 7: ACTIONABLE IMPROVEMENTS ───────────────────────────────────
    section("7. ACTIONABLE IMPROVEMENTS  (prioritized)")

    p()
    items = [
        ("HIGH",   "Pitcher distribution bands",
         f"Pitcher P10-P90 hit rate avg={_mean(pp_all):.1f}% (target 80%), {p_fail}/{len(pp_all)} dates below.\n"
         "      >>Widen pitcher IP variance in sim (workload_day SD up, ip_sd multiplier up).\n"
         "        verify_sim.py Section A is the gate."),

        ("HIGH",   "Chalk ownership under-projection",
         "Chalk (>20% actual) has bias=-4.94% and MAE=11.92% — we're missing\n"
         "      the biggest chalk plays. Softmax temperature for SP too tight or\n"
         "      OWN_GLOBAL_SCALE overcorrecting after recalibration.\n"
         "      >>Run calibrate_ownership.py with chalk tier getting separate scale factor."),

        ("HIGH",   "Power/contact/speed/barrel archetype biases",
         "All four archetypes systematically over-projected in June.\n"
         "      >>Add per-archetype de-boost adjustments in compute_projections.py\n"
         "        conditional on talent profile (like the no-global-fixes rule requires)."),

        ("MEDIUM", "Pitcher MAE still above 7.0 target",
         f"Season avg pitcher MAE = {_mean(p_all):.2f} pts (target <7.0).\n"
         "      PASS rate only {:.0f}%. June 12 was FAIL at 11.4.\n".format(p_pass) +
         "      >>Root cause varies. Check if June 12 was a rain-out / early exit slate.\n"
         "        IP anchor and K/ER distributions are the levers."),

        ("MEDIUM", "Park multiplier is more often hurting than helping",
         "park_mult frequently r<0 across the season.\n"
         "      >>Reduce park_mult weight in Tier 3 from current level.\n"
         "        Optimal context weights from research: Vegas=80%, Park=5-15%, Weather=5-15%."),

        ("MEDIUM", "Pool ceiling doesn't reach GPP line",
         "Pool best lineup rarely reaches 175+ pts needed to compete for GPP prizes.\n"
         "      Winner profile needs ~136% ownership, 3.8 booms, 48 pitcher pts.\n"
         "      >>Weight STACK_CONFIGS toward 5-0 and 4-0 (best configs in June).\n"
         "        Consider adding a 'ceiling' portfolio tier that maximizes proj_ceiling."),

        ("LOW",    "Projection correlation is weak (r~0.28 avg)",
         "Modest correlation means projection rank alone doesn't win. PMS beats it.\n"
         "      >>Incorporate swstr_pct, gb_pct, ld_pct as hitter projection inputs.\n"
         "        Pitcher gb_pct and bb9 have r>0.18 and appear in 10+ consecutive sessions."),

        ("LOW",    "Ownership low-tier over-projection",
         "Low (<5%) tier: bias=+3.67% — we over-project obscure plays.\n"
         "      >>Floor projected ownership at ~0.5% for players below certain salary threshold."),
    ]

    for priority, title, desc in items:
        p(f"  [{priority}] {title}")
        p(f"      {desc}")
        p()

    # ── FOOTER ────────────────────────────────────────────────────────────────
    p(SEP)
    p(f"  Report generated from {n} validation dates ({rows[0]['date']} – {rows[-1]['date']})")
    p(f"  Source: sim_validation_history.csv + tasks/research_findings.md")
    p(SEP)
    p()


if __name__ == "__main__":
    main()
