# SlateHub Research Findings

## Session 33 — Comprehensive Analysis (2026-03-27 to 2026-03-30)
**Sample: 1,029 players, 4 dates, 8 contests, 128,145 total contest entries**

---

### Projection Accuracy (1,029 players)
| Metric | Hitters (930) | Pitchers (99) | Overall |
|--------|--------------|---------------|---------|
| MAE | 5.09 | 8.28 | 5.40 |
| Bias | +0.22 (slight over) | -0.56 (slight under) | -0.14 |

**By Salary:** $3k-4k MAE=4.75, $4k-5k MAE=5.64, $5k-6k MAE=5.67
**By Batting Order:** BO 1-3 MAE=5.49, BO 4-6 MAE=5.00, BO 7-9 MAE=4.80

### Tier Multiplier Signal (correlation with actual over/under-performance)
| Multiplier | Correlation | Verdict |
|-----------|-------------|---------|
| context_mult | +0.161 | HELPS — Vegas context is predictive |
| vegas_mult | +0.167 | HELPS — implied runs signal works |
| weather_mult | +0.126 | HELPS — weather adjustments add value |
| platoon_mult | -0.060 | HURTS slightly — platoon splits may be overweighted |
| pitcher_mult | -0.057 | HURTS slightly — SP matchup adjustment overshoots |
| park_mult | -0.170 | HURTS — park factors making predictions worse |

### Ownership Accuracy
- No matched ownership data available yet (need overlapping dates between slate_ownership and actual_ownership)

### Contest Scoring Thresholds (8 contests, 128K entries)
| Threshold | Avg Score | % of Field |
|-----------|-----------|-----------|
| Winner | 176.6 pts | — |
| Top 1% | 162.5 pts | 1% |
| Cash line | 131.6 pts | ~22% |
| Field avg | 96.1 pts | 50% |
| >= 150 pts | — | 2.98% |
| >= 160 pts | — | 1.18% |
| >= 170 pts | — | 0.45% |

### Sim Pool Quality (3,000 lineups scored with actuals)
- **Projections have signal**: top 10% by projection scored 6.3 pts higher than bottom 10%
- Best stack config: **5-naked** (avg 135.3 actual, best 225.7) and **4-3** (avg 134.5)
- Weakest config: **4-4** (avg 127.8) — too concentrated in two teams
- Best stack teams: HOU (153.7), COL (153.6), TBR (150.8), ARI (150.5)

---

## Actionable Recommendations

### Projection Engine
1. **Reduce park_mult influence** — r=-0.170 shows it's hurting. Consider reducing park factor weight from 26% to 15% in Tier 3, or capping park_mult deviation to +/-5%
2. **Reduce pitcher_mult slightly** — SP matchup adjustment overshooting. Consider reducing Tier 2 weight from 25% to 20%
3. **Keep vegas_mult and context_mult** — these are working (r=+0.167, +0.161)
4. **Pitcher MAE too high (8.28)** — target is 7.0. Consider tightening SP_CALIBRATION or reducing pitcher ceiling/floor spread
5. **BO 1-3 hitters slightly over-projected** (MAE 5.49 vs BO 7-9 MAE 4.80) — lineup position boost may be too strong for top of order

### Pool Construction
6. **Increase 5-naked and 4-3 configs** — these outperformed. Reduce 4-4 weight (underperformed by 7+ pts vs 5-naked)
7. **Pool ceiling needs to reach 150+ pts regularly** — only 2.98% of the field hits this. Focus on correlation/variance, not just projection mean
8. **Winners avg 176.6 pts** — our pool needs high-ceiling stacks with correlated upside

### Filtering
9. **Cash line is ~131.6 pts** — use this as the minimum projection quality benchmark
10. **Top 1% requires ~162.5 pts** — lineups need boom potential, not just high floor
11. **Track which sim metrics (boom%, sharpe, winUpside) predict actual finish** — insufficient data yet, need 2+ weeks

### Next Steps
- Run research daily after --postgame to build sample size
- After 2 weeks of data, backtest specific filter thresholds
- Consider reducing STACK_CONFIGS to remove 4-4, increase 5-naked and 4-3 weight

## Research Findings — 2026-03-27, 2026-03-28, 2026-03-29, 2026-03-30

**Projection**: MAE=5.40, Bias=-0.14, Hitter MAE=5.09, Pitcher MAE=8.28
**Ownership**: MAE=5.53%, Bias=-1.56%
**Pool**: MAE=46.61, Bias=+45.80
**Contest**: Winner=176.59375, Top1%=147.4

**Recommendations:**
- POOL: Best performing stack config is 5-0 — increase its weight in STACK_CONFIGS
- POOL: Projections have 6.3 pt spread — use projection rank as primary sort for portfolio selection
- CONTEST: Avg Top 1% threshold is 147.4 pts across 8 contests
- CONTEST: Avg cash line is 111.2 pts — pool floor should exceed this
- CONTEST: Avg winner scores 176.6 pts — need high-ceiling correlated stacks
- OWNERSHIP: MAE is 5.5% — needs significant model improvement
- PROJECTION: park_mult is hurting accuracy (r=-0.170) — reduce its weight or cap its range
- TRACKING: Run this analysis daily to build sample size — patterns stabilize after 2+ weeks
