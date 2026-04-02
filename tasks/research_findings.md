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

## Research Findings — 2026-04-01

**Projection**: MAE=5.19, Bias=-0.04, Hitter MAE=4.80, Pitcher MAE=8.69
**Pool**: MAE=22.22, Bias=-12.71
**Contest**: Winner=175.29000000000002, Top1%=146.48999999999998

**Recommendations:**
- POOL: Best performing stack config is 5-0 — increase its weight in STACK_CONFIGS
- POOL: Projection spread only 1.0 pts — diversify selection, don't over-rely on projection ranking
- CONTEST: Avg Top 1% threshold is 146.5 pts across 10 contests
- CONTEST: Avg cash line is 111.1 pts — pool floor should exceed this
- CONTEST: Avg winner scores 175.3 pts — need high-ceiling correlated stacks
- TRACKING: Run this analysis daily to build sample size — patterns stabilize after 2+ weeks

## Research Findings — 2026-04-01

**Projection**: MAE=5.19, Bias=-0.04, Hitter MAE=4.80, Pitcher MAE=8.69
**Pool**: MAE=22.22, Bias=-12.71
**Contest**: Winner=175.29000000000002, Top1%=146.48999999999998

### Predictive Diagnostics

**Hitter Missing Predictors** (correlated with error but not in model):
- `swstr_pct` r=-0.111 (n=261)
- `hr` r=-0.094 (n=261)
- `oppo_pct` r=+0.089 (n=261)
- `slg` r=-0.087 (n=261)
- `fb_pct` r=-0.084 (n=261)

**Pitcher Missing Predictors** (correlated with error but not in model):
- `fip` r=+0.361 (n=29)
- `k_bb_pct` r=-0.352 (n=30)
- `era` r=+0.323 (n=30)
- `swstr_pct` r=-0.307 (n=29)
- `whip` r=+0.291 (n=30)

**Archetype Biases:**
- Power (ISO>.200): over-projected by 1.0 pts (n=58)
- Strikeout (K%>28%): over-projected by 1.0 pts (n=36)

**Recommendations:**
- POOL: Best performing stack config is 5-0 — increase its weight in STACK_CONFIGS
- POOL: Projection spread only 1.0 pts — diversify selection, don't over-rely on projection ranking
- CONTEST: Avg Top 1% threshold is 146.5 pts across 10 contests
- CONTEST: Avg cash line is 111.1 pts — pool floor should exceed this
- CONTEST: Avg winner scores 175.3 pts — need high-ceiling correlated stacks
- TRACKING: Run this analysis daily to build sample size — patterns stabilize after 2+ weeks

## Research Findings — 2026-03-26, 2026-03-27, 2026-03-28, 2026-03-29, 2026-03-30, 2026-03-31, 2026-04-01

**Projection**: MAE=5.46, Bias=-0.24, Hitter MAE=5.11, Pitcher MAE=8.57
**Ownership**: MAE=5.83%, Bias=-1.76%
**Pool**: MAE=59.42, Bias=+59.29
**Contest**: Winner=175.29000000000002, Top1%=146.48999999999998

### Predictive Diagnostics

**Hitter Missing Predictors** (correlated with error but not in model):
- `rbi` r=-0.117 (n=819)
- `hr` r=-0.116 (n=819)
- `r` r=-0.101 (n=819)
- `pa` r=-0.101 (n=819)

**Pitcher Missing Predictors** (correlated with error but not in model):
- `g` r=-0.197 (n=106)
- `gb_pct` r=+0.195 (n=106)
- `bb9` r=+0.195 (n=106)

**Optimal Context Weights**: Vegas=80% Park=5% Weather=15% (saves 0.03 MAE)

**Archetype Biases:**
- Power (ISO>.200): over-projected by 1.1 pts (n=172)

**Recommendations:**
- POOL: Best performing stack config is 4-3 — increase its weight in STACK_CONFIGS
- POOL: Projection spread only 3.1 pts — diversify selection, don't over-rely on projection ranking
- CONTEST: Avg Top 1% threshold is 146.5 pts across 10 contests
- CONTEST: Avg cash line is 111.1 pts — pool floor should exceed this
- CONTEST: Avg winner scores 175.3 pts — need high-ceiling correlated stacks
- OWNERSHIP: MAE is 5.8% — needs significant model improvement
- TRACKING: Run this analysis daily to build sample size — patterns stabilize after 2+ weeks

## Research Findings — 2026-03-26, 2026-03-27, 2026-03-28, 2026-03-29, 2026-03-30, 2026-03-31, 2026-04-01

**Projection**: MAE=5.46, Bias=-0.24, Hitter MAE=5.11, Pitcher MAE=8.57
**Ownership**: MAE=5.83%, Bias=-1.76%
**Pool**: MAE=59.42, Bias=+59.29
**Contest**: Winner=175.29000000000002, Top1%=146.48999999999998

### Predictive Diagnostics

**Hitter Missing Predictors** (correlated with error but not in model):
- `rbi` r=-0.117 (n=819)
- `hr` r=-0.116 (n=819)
- `r` r=-0.101 (n=819)
- `pa` r=-0.101 (n=819)

**Pitcher Missing Predictors** (correlated with error but not in model):
- `g` r=-0.197 (n=106)
- `gb_pct` r=+0.195 (n=106)
- `bb9` r=+0.195 (n=106)

**Optimal Context Weights**: Vegas=80% Park=5% Weather=15% (saves 0.03 MAE)

**Archetype Biases:**
- Power (ISO>.200): over-projected by 1.1 pts (n=172)

**Recommendations:**
- POOL: Best performing stack config is 4-3 — increase its weight in STACK_CONFIGS
- POOL: Projection spread only 3.1 pts — diversify selection, don't over-rely on projection ranking
- CONTEST: Avg Top 1% threshold is 146.5 pts across 10 contests
- CONTEST: Avg cash line is 111.1 pts — pool floor should exceed this
- CONTEST: Avg winner scores 175.3 pts — need high-ceiling correlated stacks
- OWNERSHIP: MAE is 5.8% — needs significant model improvement
- TRACKING: Run this analysis daily to build sample size — patterns stabilize after 2+ weeks

## Research Findings — 2026-03-25, 2026-03-26, 2026-03-27, 2026-03-28, 2026-03-29, 2026-03-30, 2026-03-31, 2026-04-01

**Projection**: MAE=5.49, Bias=-0.37, Hitter MAE=5.14, Pitcher MAE=8.64
**Ownership**: MAE=5.53%, Bias=-1.56%
**Pool**: MAE=60.96, Bias=+60.84
**Contest**: Winner=175.29000000000002, Top1%=146.48999999999998

### Predictive Diagnostics

**Hitter Missing Predictors** (correlated with error but not in model):
- `rbi` r=-0.117 (n=819)
- `hr` r=-0.116 (n=819)
- `r` r=-0.105 (n=819)
- `pa` r=-0.100 (n=819)

**Pitcher Missing Predictors** (correlated with error but not in model):
- `bb9` r=+0.199 (n=106)
- `g` r=-0.193 (n=106)
- `gb_pct` r=+0.193 (n=106)

**Optimal Context Weights**: Vegas=80% Park=5% Weather=15% (saves 0.03 MAE)

**Archetype Biases:**
- Power (ISO>.200): over-projected by 0.9 pts (n=172)

**Recommendations:**
- POOL: Best performing stack config is 4-3 — increase its weight in STACK_CONFIGS
- POOL: Projection spread only 2.3 pts — diversify selection, don't over-rely on projection ranking
- CONTEST: Avg Top 1% threshold is 146.5 pts across 10 contests
- CONTEST: Avg cash line is 111.1 pts — pool floor should exceed this
- CONTEST: Avg winner scores 175.3 pts — need high-ceiling correlated stacks
- OWNERSHIP: MAE is 5.5% — needs significant model improvement
- TRACKING: Run this analysis daily to build sample size — patterns stabilize after 2+ weeks

## Research Findings — 2026-03-26, 2026-03-27, 2026-03-28, 2026-03-29, 2026-03-30, 2026-03-31, 2026-04-01

**Projection**: MAE=5.49, Bias=-0.37, Hitter MAE=5.14, Pitcher MAE=8.64
**Ownership**: MAE=5.83%, Bias=-1.76%
**Pool**: MAE=59.59, Bias=+59.46
**Contest**: Winner=175.29000000000002, Top1%=146.48999999999998

### Predictive Diagnostics

**Hitter Missing Predictors** (correlated with error but not in model):
- `rbi` r=-0.107 (n=947)
- `hr` r=-0.106 (n=947)
- `r` r=-0.093 (n=947)
- `pa` r=-0.092 (n=947)
- `ld_pct` r=+0.064 (n=947)

**Pitcher Missing Predictors** (correlated with error but not in model):
- `bb9` r=+0.199 (n=106)
- `g` r=-0.193 (n=106)
- `gb_pct` r=+0.193 (n=106)
- `whip` r=+0.095 (n=106)
- `era` r=+0.093 (n=106)

**Optimal Context Weights**: Vegas=80% Park=5% Weather=15% (saves 0.03 MAE)

**Archetype Biases:**
- Contact (K%<15%): under-projected by 0.9 pts (n=169)

**Recommendations:**
- POOL: Best performing stack config is 4-3 — increase its weight in STACK_CONFIGS
- POOL: Projection spread only 3.1 pts — diversify selection, don't over-rely on projection ranking
- CONTEST: Avg Top 1% threshold is 146.5 pts across 10 contests
- CONTEST: Avg cash line is 111.1 pts — pool floor should exceed this
- CONTEST: Avg winner scores 175.3 pts — need high-ceiling correlated stacks
- OWNERSHIP: MAE is 5.8% — needs significant model improvement
- TRACKING: Run this analysis daily to build sample size — patterns stabilize after 2+ weeks

## Research Findings — 2026-03-26, 2026-03-27, 2026-03-28, 2026-03-29, 2026-03-30, 2026-03-31, 2026-04-01

**Projection**: MAE=5.49, Bias=-0.37, Hitter MAE=5.14, Pitcher MAE=8.64
**Ownership**: MAE=5.83%, Bias=-1.76%
**Pool**: MAE=59.59, Bias=+59.46
**Contest**: Winner=175.29000000000002, Top1%=146.48999999999998

### Predictive Diagnostics

**Hitter Missing Predictors** (correlated with error but not in model):
- `rbi` r=-0.107 (n=947)
- `hr` r=-0.106 (n=947)
- `r` r=-0.093 (n=947)
- `pa` r=-0.092 (n=947)
- `ld_pct` r=+0.064 (n=947)

**Pitcher Missing Predictors** (correlated with error but not in model):
- `bb9` r=+0.199 (n=106)
- `g` r=-0.193 (n=106)
- `gb_pct` r=+0.193 (n=106)
- `whip` r=+0.095 (n=106)
- `era` r=+0.093 (n=106)

**Optimal Context Weights**: Vegas=80% Park=5% Weather=15% (saves 0.03 MAE)

**Archetype Biases:**
- Contact (K%<15%): under-projected by 0.9 pts (n=169)

**Recommendations:**
- POOL: Best performing stack config is 4-3 — increase its weight in STACK_CONFIGS
- POOL: Projection spread only 3.1 pts — diversify selection, don't over-rely on projection ranking
- CONTEST: Avg Top 1% threshold is 146.5 pts across 10 contests
- CONTEST: Avg cash line is 111.1 pts — pool floor should exceed this
- CONTEST: Avg winner scores 175.3 pts — need high-ceiling correlated stacks
- OWNERSHIP: MAE is 5.8% — needs significant model improvement
- TRACKING: Run this analysis daily to build sample size — patterns stabilize after 2+ weeks

## Research Findings — 2026-03-25, 2026-03-26, 2026-03-27, 2026-03-28, 2026-03-29, 2026-03-30, 2026-03-31, 2026-04-01

**Projection**: MAE=5.49, Bias=-0.37, Hitter MAE=5.14, Pitcher MAE=8.64
**Ownership**: MAE=5.53%, Bias=-1.56%
**Pool**: MAE=60.96, Bias=+60.84
**Contest**: Winner=175.29000000000002, Top1%=146.48999999999998

### Predictive Diagnostics

**Hitter Missing Predictors** (correlated with error but not in model):
- `rbi` r=-0.107 (n=947)
- `hr` r=-0.106 (n=947)
- `r` r=-0.093 (n=947)
- `pa` r=-0.092 (n=947)
- `ld_pct` r=+0.064 (n=947)

**Pitcher Missing Predictors** (correlated with error but not in model):
- `bb9` r=+0.199 (n=106)
- `g` r=-0.193 (n=106)
- `gb_pct` r=+0.193 (n=106)
- `whip` r=+0.095 (n=106)
- `era` r=+0.093 (n=106)

**Optimal Context Weights**: Vegas=80% Park=5% Weather=15% (saves 0.03 MAE)

**Archetype Biases:**
- Contact (K%<15%): under-projected by 0.9 pts (n=169)

**Recommendations:**
- POOL: Best performing stack config is 4-3 — increase its weight in STACK_CONFIGS
- POOL: Projection spread only 2.3 pts — diversify selection, don't over-rely on projection ranking
- CONTEST: Avg Top 1% threshold is 146.5 pts across 10 contests
- CONTEST: Avg cash line is 111.1 pts — pool floor should exceed this
- CONTEST: Avg winner scores 175.3 pts — need high-ceiling correlated stacks
- OWNERSHIP: MAE is 5.5% — needs significant model improvement
- TRACKING: Run this analysis daily to build sample size — patterns stabilize after 2+ weeks

## Research Findings — 2026-03-26, 2026-03-27, 2026-03-28, 2026-03-29, 2026-03-30, 2026-03-31, 2026-04-01

**Projection**: MAE=5.49, Bias=-0.35, Hitter MAE=5.14, Pitcher MAE=8.64
**Ownership**: MAE=5.83%, Bias=-1.76%
**Pool**: MAE=59.59, Bias=+59.46
**Contest**: Winner=175.29000000000002, Top1%=146.48999999999998

### Predictive Diagnostics

**Hitter Missing Predictors** (correlated with error but not in model):
- `hr` r=-0.108 (n=947)
- `rbi` r=-0.107 (n=947)
- `r` r=-0.093 (n=947)
- `pa` r=-0.092 (n=947)
- `ld_pct` r=+0.067 (n=947)

**Pitcher Missing Predictors** (correlated with error but not in model):
- `bb9` r=+0.199 (n=106)
- `g` r=-0.193 (n=106)
- `gb_pct` r=+0.193 (n=106)
- `whip` r=+0.095 (n=106)
- `era` r=+0.093 (n=106)

**Optimal Context Weights**: Vegas=80% Park=5% Weather=15% (saves 0.03 MAE)

**Archetype Biases:**
- Contact (K%<15%): under-projected by 0.9 pts (n=169)

**Recommendations:**
- POOL: Best performing stack config is 4-3 — increase its weight in STACK_CONFIGS
- POOL: Projection spread only 3.1 pts — diversify selection, don't over-rely on projection ranking
- CONTEST: Avg Top 1% threshold is 146.5 pts across 10 contests
- CONTEST: Avg cash line is 111.1 pts — pool floor should exceed this
- CONTEST: Avg winner scores 175.3 pts — need high-ceiling correlated stacks
- OWNERSHIP: MAE is 5.8% — needs significant model improvement
- TRACKING: Run this analysis daily to build sample size — patterns stabilize after 2+ weeks
