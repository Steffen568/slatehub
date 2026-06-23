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

## Research Findings — 2026-03-28, 2026-03-29, 2026-03-30, 2026-03-31, 2026-04-01, 2026-04-02, 2026-04-03, 2026-04-04

**Projection**: MAE=5.38, Bias=-0.05, Hitter MAE=5.01, Pitcher MAE=8.67
**Ownership**: MAE=5.83%, Bias=-1.76%
**Pool**: MAE=40.92, Bias=+39.43
**Contest**: Winner=178.59411764705882, Top1%=150.56176470588235

### Predictive Diagnostics

**Hitter Missing Predictors** (correlated with error but not in model):
- `hr` r=-0.112 (n=756)
- `rbi` r=-0.107 (n=756)
- `r` r=-0.090 (n=756)
- `pa` r=-0.082 (n=756)
- `slg` r=-0.068 (n=756)

**Pitcher Missing Predictors** (correlated with error but not in model):
- `bb9` r=+0.195 (n=84)
- `g` r=-0.191 (n=84)
- `gb_pct` r=+0.135 (n=84)
- `era` r=+0.095 (n=84)
- `k_bb_pct` r=-0.092 (n=84)

**Optimal Context Weights**: Vegas=80% Park=15% Weather=5% (saves 0.02 MAE)

**Archetype Biases:**
- Power (ISO>.200): over-projected by 1.1 pts (n=168)

**Recommendations:**
- POOL: Best performing stack config is 4-3 — increase its weight in STACK_CONFIGS
- POOL: Projection spread only 3.5 pts — diversify selection, don't over-rely on projection ranking
- CONTEST: Avg Top 1% threshold is 150.6 pts across 17 contests
- CONTEST: Avg cash line is 114.2 pts — pool floor should exceed this
- CONTEST: Avg winner scores 178.6 pts — need high-ceiling correlated stacks
- OWNERSHIP: MAE is 5.8% — needs significant model improvement
- TRACKING: Run this analysis daily to build sample size — patterns stabilize after 2+ weeks

## Research Findings — 2026-03-28, 2026-03-29, 2026-03-30, 2026-03-31, 2026-04-01, 2026-04-02, 2026-04-03, 2026-04-04

**Projection**: MAE=5.37, Bias=+0.02, Hitter MAE=4.97, Pitcher MAE=8.96
**Ownership**: MAE=5.53%, Bias=-1.56%
**Pool**: MAE=47.84, Bias=+47.39
**Contest**: Winner=175.53333333333333, Top1%=148.9625

### Predictive Diagnostics

**Hitter Missing Predictors** (correlated with error but not in model):
- `hr` r=-0.111 (n=1015)
- `rbi` r=-0.096 (n=1015)
- `r` r=-0.079 (n=1015)
- `pa` r=-0.070 (n=1015)
- `slg` r=-0.064 (n=1015)

**Pitcher Missing Predictors** (correlated with error but not in model):
- `gb_pct` r=+0.167 (n=111)
- `gs` r=+0.144 (n=111)
- `whip` r=+0.101 (n=111)
- `era` r=+0.101 (n=111)
- `k_bb_pct` r=-0.101 (n=111)

**Optimal Context Weights**: Vegas=80% Park=5% Weather=15% (saves 0.02 MAE)

**Archetype Biases:**
- Power (ISO>.200): over-projected by 1.2 pts (n=224)

**Recommendations:**
- POOL: Best performing stack config is 4-3 — increase its weight in STACK_CONFIGS
- POOL: Projections have 8.5 pt spread — use projection rank as primary sort for portfolio selection
- CONTEST: Avg Top 1% threshold is 149.0 pts across 24 contests
- CONTEST: Avg cash line is 111.9 pts — pool floor should exceed this
- CONTEST: Avg winner scores 175.5 pts — need high-ceiling correlated stacks
- OWNERSHIP: MAE is 5.5% — needs significant model improvement
- TRACKING: Run this analysis daily to build sample size — patterns stabilize after 2+ weeks

## Research Findings — 2026-03-29, 2026-03-30, 2026-03-31, 2026-04-01, 2026-04-02, 2026-04-03, 2026-04-04, 2026-04-05

**Projection**: MAE=5.43, Bias=-0.14, Hitter MAE=5.05, Pitcher MAE=8.88
**Ownership**: MAE=5.95%, Bias=-2.61%
**Pool**: MAE=53.16, Bias=+52.67
**Contest**: Winner=172.09629629629632, Top1%=146.92962962962963

### Predictive Diagnostics

**Hitter Missing Predictors** (correlated with error but not in model):
- `hr` r=-0.070 (n=1270)
- `rbi` r=-0.062 (n=1270)

**Optimal Context Weights**: Vegas=80% Park=5% Weather=15% (saves 0.01 MAE)

**Recommendations:**
- OWNERSHIP: decrease baseline ownership estimates — bias is -2.6%
- POOL: Best performing stack config is 4-3 — increase its weight in STACK_CONFIGS
- POOL: Projections have 5.9 pt spread — use projection rank as primary sort for portfolio selection
- CONTEST: Avg Top 1% threshold is 146.9 pts across 27 contests
- CONTEST: Avg cash line is 110.9 pts — pool floor should exceed this
- CONTEST: Avg winner scores 172.1 pts — need high-ceiling correlated stacks
- OWNERSHIP: decrease baseline estimates — bias is -2.6%
- OWNERSHIP: MAE is 5.9% — needs significant model improvement
- TRACKING: Run this analysis daily to build sample size — patterns stabilize after 2+ weeks

## Research Findings — 2026-04-06, 2026-04-07, 2026-04-08, 2026-04-09, 2026-04-10, 2026-04-11, 2026-04-12, 2026-04-13

**Projection**: MAE=6.73, Bias=-2.44, Hitter MAE=6.38, Pitcher MAE=9.83
**Pool**: MAE=43.90, Bias=-38.54
**Contest**: Winner=174.0030303030303, Top1%=146.6651515151515

### Predictive Diagnostics

**Hitter Missing Predictors** (correlated with error but not in model):
- `oppo_pct` r=-0.145 (n=166)
- `pull_pct` r=+0.080 (n=166)
- `barrel_pct` r=-0.065 (n=169)
- `fb_pct` r=+0.065 (n=166)
- `swstr_pct` r=+0.065 (n=166)

**Pitcher Missing Predictors** (correlated with error but not in model):
- `location_plus` r=+0.346 (n=20)
- `g` r=+0.266 (n=20)
- `l` r=+0.253 (n=20)
- `lob_pct` r=-0.231 (n=20)
- `hard_hit_pct` r=+0.191 (n=20)

**Opposing Lineup Factors** (for pitcher projections):
- `opp_bb_pct` r=-0.490 (n=20)
- `opp_o_swing_pct` r=+0.404 (n=20)
- `opp_iso` r=+0.167 (n=20)
- `opp_k_pct` r=+0.129 (n=20)
- `opp_xwoba` r=-0.119 (n=20)

**Optimal Context Weights**: Vegas=45% Park=25% Weather=30% (saves 0.01 MAE)

**Archetype Biases:**
- Power (ISO>.200): under-projected by 4.2 pts (n=36)
- Contact (K%<15%): under-projected by 2.5 pts (n=33)
- Strikeout (K%>28%): under-projected by 3.0 pts (n=24)
- Speed (SB pace>15): under-projected by 2.9 pts (n=55)

**Recommendations:**
- PROJECTION: increase SP_CALIBRATION by ~0.16 (current 0.90)
- PROJECTION: reduce context multiplier weights — hitter bias is +2.99 pts
- POOL: Best performing stack config is 6-2 — increase its weight in STACK_CONFIGS
- POOL: Projection spread only -8.8 pts — diversify selection, don't over-rely on projection ranking
- CONTEST: Avg Top 1% threshold is 146.7 pts across 33 contests
- CONTEST: Avg cash line is 110.2 pts — pool floor should exceed this
- CONTEST: Avg winner scores 174.0 pts — need high-ceiling correlated stacks
- TRACKING: Run this analysis daily to build sample size — patterns stabilize after 2+ weeks

## Sim Validation — 2026-04-13

### Distribution Calibration
- **Hitters**: 81.5% in P10-P90 [PASS] (below floor=2.2%, above ceiling=16.3%)
- **Pitchers**: 65.0% in P10-P90 [WARN] (below floor=25.0%, above ceiling=10.0%)

### Projection Accuracy
- Overall: MAE=6.68, Bias=-2.34, r=0.159
- Hitters: MAE=6.32 [WARN]
- Pitchers: MAE=9.91 [WARN]

### Pitcher Components
- IP: MAE=1.13, Bias=+0.49
- Ks: MAE=2.15, Bias=-0.43
- ER: MAE=1.87, Bias=-0.64

### Multiplier Effectiveness
- `pitcher_mult`: r=-0.047 [WARN]
- `platoon_mult`: r=+0.000 [WARN]
- `context_mult`: r=+0.085 [PASS]
- `vegas_mult`: r=+0.080 [PASS]
- `park_mult`: r=+0.056 [PASS]
- `weather_mult`: r=+0.056 [PASS]

## Slate Review — 2026-04-13 / main

- **Pool**: 10000 lineups, avg actual=88.2, cash line=87.9, GPP line=170.8, best=222.9
- **Proj accuracy**: r=-0.128, MAE=30.5, bias=-0.6
- **Overlap**: 0/20 top-by-proj were actual winners
- **Best strategy**: Salary (highest salary)
- **Top stack**: MIN (avg actual=134.8, 3.2% exposure)
- **Biggest bust**: Garrett Crochet (proj=21.4, actual=-23.4, 42% exp)
- **Biggest missed opp**: Kyle Schwarber (actual=38.0, 3.5% exp)

## Research Findings — 2026-04-08, 2026-04-09, 2026-04-10, 2026-04-11, 2026-04-12, 2026-04-13, 2026-04-14, 2026-04-15

**Projection**: MAE=6.68, Bias=-2.34, Hitter MAE=6.32, Pitcher MAE=9.91
**Pool**: MAE=39.35, Bias=-33.93
**Contest**: Winner=174.09411764705882, Top1%=146.63529411764708

### Predictive Diagnostics

**Hitter Missing Predictors** (correlated with error but not in model):
- `oppo_pct` r=-0.132 (n=170)
- `swstr_pct` r=+0.082 (n=170)
- `pull_pct` r=+0.074 (n=170)
- `hr` r=+0.070 (n=170)
- `avg_ev` r=+0.066 (n=173)

**Pitcher Missing Predictors** (correlated with error but not in model):
- `sb_per_9` r=-0.452 (n=20)
- `cs_allowed` r=-0.445 (n=20)
- `sb_allowed` r=-0.404 (n=20)
- `location_plus` r=+0.350 (n=20)
- `g` r=+0.269 (n=20)

**Opposing Lineup Factors** (for pitcher projections):
- `opp_bb_pct` r=-0.423 (n=20)
- `opp_o_swing_pct` r=+0.315 (n=20)
- `opp_iso` r=+0.175 (n=20)
- `opp_xwoba` r=-0.139 (n=20)
- `opp_k_pct` r=+0.101 (n=20)

**Optimal Context Weights**: Vegas=45% Park=25% Weather=30% (saves 0.02 MAE)

**Archetype Biases:**
- Power (ISO>.200): under-projected by 3.8 pts (n=38)
- Contact (K%<15%): under-projected by 2.2 pts (n=35)
- Strikeout (K%>28%): under-projected by 3.0 pts (n=24)
- Speed (SB pace>15): under-projected by 2.9 pts (n=55)

**Recommendations:**
- PROJECTION: increase SP_CALIBRATION by ~0.15 (current 0.90)
- PROJECTION: reduce context multiplier weights — hitter bias is +2.86 pts
- POOL: Best performing stack config is 6-2 — increase its weight in STACK_CONFIGS
- POOL: Projections have 16.6 pt spread — use projection rank as primary sort for portfolio selection
- CONTEST: Avg Top 1% threshold is 146.6 pts across 34 contests
- CONTEST: Avg cash line is 110.0 pts — pool floor should exceed this
- CONTEST: Avg winner scores 174.1 pts — need high-ceiling correlated stacks
- TRACKING: Run this analysis daily to build sample size — patterns stabilize after 2+ weeks

## Sim Validation — 2026-04-13

### Distribution Calibration
- **Hitters**: 81.5% in P10-P90 [PASS] (below floor=2.2%, above ceiling=16.3%)
- **Pitchers**: 65.0% in P10-P90 [WARN] (below floor=25.0%, above ceiling=10.0%)

### Projection Accuracy
- Overall: MAE=6.68, Bias=-2.34, r=0.159
- Hitters: MAE=6.32 [WARN]
- Pitchers: MAE=9.91 [WARN]

### Pitcher Components
- IP: MAE=1.13, Bias=+0.49
- Ks: MAE=2.15, Bias=-0.43
- ER: MAE=1.87, Bias=-0.64

### Multiplier Effectiveness
- `pitcher_mult`: r=-0.047 [WARN]
- `platoon_mult`: r=+0.000 [WARN]
- `context_mult`: r=+0.085 [PASS]
- `vegas_mult`: r=+0.080 [PASS]
- `park_mult`: r=+0.056 [PASS]
- `weather_mult`: r=+0.056 [PASS]

## Slate Review — 2026-04-13 / main

- **Pool**: 10000 lineups, avg actual=88.2, cash line=87.9, GPP line=170.8, best=222.9
- **Proj accuracy**: r=-0.128, MAE=30.5, bias=-0.6
- **Overlap**: 0/20 top-by-proj were actual winners
- **Best strategy**: Salary (highest salary)
- **Top stack**: MIN (avg actual=134.8, 3.2% exposure)
- **Biggest bust**: Garrett Crochet (proj=21.4, actual=-23.4, 42% exp)
- **Biggest missed opp**: Kyle Schwarber (actual=38.0, 3.5% exp)

## Research Findings — 2026-04-09, 2026-04-10, 2026-04-11, 2026-04-12, 2026-04-13, 2026-04-14, 2026-04-15, 2026-04-16

**Projection**: MAE=6.68, Bias=-2.34, Hitter MAE=6.32, Pitcher MAE=9.91
**Pool**: MAE=38.55, Bias=-29.10
**Contest**: Winner=174.5142857142857, Top1%=147.13714285714286

### Predictive Diagnostics

**Hitter Missing Predictors** (correlated with error but not in model):
- `oppo_pct` r=-0.132 (n=170)
- `swstr_pct` r=+0.082 (n=170)
- `pull_pct` r=+0.074 (n=170)
- `hr` r=+0.070 (n=170)
- `avg_ev` r=+0.066 (n=173)

**Pitcher Missing Predictors** (correlated with error but not in model):
- `sb_per_9` r=-0.452 (n=20)
- `cs_allowed` r=-0.445 (n=20)
- `sb_allowed` r=-0.404 (n=20)
- `location_plus` r=+0.350 (n=20)
- `g` r=+0.269 (n=20)

**Opposing Lineup Factors** (for pitcher projections):
- `opp_bb_pct` r=-0.423 (n=20)
- `opp_o_swing_pct` r=+0.315 (n=20)
- `opp_iso` r=+0.175 (n=20)
- `opp_xwoba` r=-0.139 (n=20)
- `opp_k_pct` r=+0.101 (n=20)

**Optimal Context Weights**: Vegas=45% Park=25% Weather=30% (saves 0.02 MAE)

**Archetype Biases:**
- Power (ISO>.200): under-projected by 3.8 pts (n=38)
- Contact (K%<15%): under-projected by 2.2 pts (n=35)
- Strikeout (K%>28%): under-projected by 3.0 pts (n=24)
- Speed (SB pace>15): under-projected by 2.9 pts (n=55)

**Recommendations:**
- PROJECTION: increase SP_CALIBRATION by ~0.15 (current 0.90)
- PROJECTION: reduce context multiplier weights — hitter bias is +2.86 pts
- POOL: Best performing stack config is 3-2 — increase its weight in STACK_CONFIGS
- POOL: Projections have 24.0 pt spread — use projection rank as primary sort for portfolio selection
- CONTEST: Avg Top 1% threshold is 147.1 pts across 35 contests
- CONTEST: Avg cash line is 110.6 pts — pool floor should exceed this
- CONTEST: Avg winner scores 174.5 pts — need high-ceiling correlated stacks
- TRACKING: Run this analysis daily to build sample size — patterns stabilize after 2+ weeks

## Sim Validation — 2026-04-13

### Distribution Calibration
- **Hitters**: 81.5% in P10-P90 [PASS] (below floor=2.2%, above ceiling=16.3%)
- **Pitchers**: 65.0% in P10-P90 [WARN] (below floor=25.0%, above ceiling=10.0%)

### Projection Accuracy
- Overall: MAE=6.68, Bias=-2.34, r=0.159
- Hitters: MAE=6.32 [WARN]
- Pitchers: MAE=9.91 [WARN]

### Pitcher Components
- IP: MAE=1.13, Bias=+0.49
- Ks: MAE=2.15, Bias=-0.43
- ER: MAE=1.87, Bias=-0.64

### Multiplier Effectiveness
- `pitcher_mult`: r=-0.047 [WARN]
- `platoon_mult`: r=+0.000 [WARN]
- `context_mult`: r=+0.085 [PASS]
- `vegas_mult`: r=+0.080 [PASS]
- `park_mult`: r=+0.056 [PASS]
- `weather_mult`: r=+0.056 [PASS]

## Slate Review — 2026-04-13 / main

- **Pool**: 10000 lineups, avg actual=88.2, cash line=87.9, GPP line=170.8, best=222.9
- **Proj accuracy**: r=-0.128, MAE=30.5, bias=-0.6
- **Overlap**: 0/20 top-by-proj were actual winners
- **Best strategy**: Salary (highest salary)
- **Top stack**: MIN (avg actual=134.8, 3.2% exposure)
- **Biggest bust**: Garrett Crochet (proj=21.4, actual=-23.4, 42% exp)
- **Biggest missed opp**: Kyle Schwarber (actual=38.0, 3.5% exp)

## Research Findings — 2026-04-10, 2026-04-11, 2026-04-12, 2026-04-13, 2026-04-14, 2026-04-15, 2026-04-16, 2026-04-17

**Projection**: MAE=6.68, Bias=-2.34, Hitter MAE=6.32, Pitcher MAE=9.91
**Pool**: MAE=40.61, Bias=-30.98
**Contest**: Winner=174.93333333333334, Top1%=147.56666666666666

### Predictive Diagnostics

**Hitter Missing Predictors** (correlated with error but not in model):
- `oppo_pct` r=-0.132 (n=170)
- `swstr_pct` r=+0.082 (n=170)
- `pull_pct` r=+0.074 (n=170)
- `hr` r=+0.070 (n=170)
- `avg_ev` r=+0.066 (n=173)

**Pitcher Missing Predictors** (correlated with error but not in model):
- `sb_per_9` r=-0.452 (n=20)
- `cs_allowed` r=-0.445 (n=20)
- `sb_allowed` r=-0.404 (n=20)
- `location_plus` r=+0.350 (n=20)
- `g` r=+0.269 (n=20)

**Opposing Lineup Factors** (for pitcher projections):
- `opp_bb_pct` r=-0.423 (n=20)
- `opp_o_swing_pct` r=+0.315 (n=20)
- `opp_iso` r=+0.175 (n=20)
- `opp_xwoba` r=-0.139 (n=20)
- `opp_k_pct` r=+0.101 (n=20)

**Optimal Context Weights**: Vegas=45% Park=25% Weather=30% (saves 0.02 MAE)

**Archetype Biases:**
- Power (ISO>.200): under-projected by 3.8 pts (n=38)
- Contact (K%<15%): under-projected by 2.2 pts (n=35)
- Strikeout (K%>28%): under-projected by 3.0 pts (n=24)
- Speed (SB pace>15): under-projected by 2.9 pts (n=55)

**Recommendations:**
- PROJECTION: increase SP_CALIBRATION by ~0.15 (current 0.90)
- PROJECTION: reduce context multiplier weights — hitter bias is +2.86 pts
- POOL: Best performing stack config is 3-0 — increase its weight in STACK_CONFIGS
- POOL: Projections have 22.4 pt spread — use projection rank as primary sort for portfolio selection
- CONTEST: Avg Top 1% threshold is 147.6 pts across 36 contests
- CONTEST: Avg cash line is 110.9 pts — pool floor should exceed this
- CONTEST: Avg winner scores 174.9 pts — need high-ceiling correlated stacks
- TRACKING: Run this analysis daily to build sample size — patterns stabilize after 2+ weeks

## Sim Validation — 2026-04-13

### Distribution Calibration
- **Hitters**: 81.5% in P10-P90 [PASS] (below floor=2.2%, above ceiling=16.3%)
- **Pitchers**: 65.0% in P10-P90 [WARN] (below floor=25.0%, above ceiling=10.0%)

### Projection Accuracy
- Overall: MAE=6.68, Bias=-2.34, r=0.159
- Hitters: MAE=6.32 [WARN]
- Pitchers: MAE=9.91 [WARN]

### Pitcher Components
- IP: MAE=1.13, Bias=+0.49
- Ks: MAE=2.15, Bias=-0.43
- ER: MAE=1.87, Bias=-0.64

### Multiplier Effectiveness
- `pitcher_mult`: r=-0.047 [WARN]
- `platoon_mult`: r=+0.000 [WARN]
- `context_mult`: r=+0.085 [PASS]
- `vegas_mult`: r=+0.080 [PASS]
- `park_mult`: r=+0.056 [PASS]
- `weather_mult`: r=+0.056 [PASS]

## Slate Review — 2026-04-13 / main

- **Pool**: 10000 lineups, avg actual=88.2, cash line=87.9, GPP line=170.8, best=222.9
- **Proj accuracy**: r=-0.128, MAE=30.5, bias=-0.6
- **Overlap**: 0/20 top-by-proj were actual winners
- **Best strategy**: Salary (highest salary)
- **Top stack**: MIN (avg actual=134.8, 3.2% exposure)
- **Biggest bust**: Garrett Crochet (proj=21.4, actual=-23.4, 42% exp)
- **Biggest missed opp**: Kyle Schwarber (actual=38.0, 3.5% exp)

## Slate Review — 2026-04-13 / main

- **Pool**: 10000 lineups, avg actual=88.2, cash line=87.9, GPP line=170.8, best=222.9
- **Proj accuracy**: r=-0.128, MAE=30.5, bias=-0.6
- **Overlap**: 0/20 top-by-proj were actual winners
- **Best strategy**: Salary (highest salary)
- **Top stack**: MIN (avg actual=134.8, 3.2% exposure)
- **Biggest bust**: Garrett Crochet (proj=21.4, actual=-23.4, 42% exp)
- **Biggest missed opp**: Kyle Schwarber (actual=38.0, 3.5% exp)

## Research Findings — 2026-04-13, 2026-04-14, 2026-04-15, 2026-04-16, 2026-04-17, 2026-04-18, 2026-04-19, 2026-04-20

**Projection**: MAE=6.61, Bias=-1.81, Hitter MAE=6.25, Pitcher MAE=9.73
**Pool**: MAE=41.16, Bias=-28.27
**Contest**: Winner=174.5157894736842, Top1%=146.8394736842105

### Predictive Diagnostics

**Hitter Missing Predictors** (correlated with error but not in model):
- `oppo_pct` r=-0.133 (n=186)
- `pull_pct` r=+0.083 (n=186)
- `swstr_pct` r=+0.075 (n=186)
- `barrel_pct` r=-0.055 (n=189)
- `hr` r=+0.054 (n=186)

**Pitcher Missing Predictors** (correlated with error but not in model):
- `sb_per_9` r=-0.440 (n=22)
- `sb_allowed` r=-0.412 (n=22)
- `cs_allowed` r=-0.399 (n=22)
- `location_plus` r=+0.356 (n=22)
- `g` r=+0.231 (n=22)

**Opposing Lineup Factors** (for pitcher projections):
- `opp_bb_pct` r=-0.394 (n=22)
- `opp_o_swing_pct` r=+0.335 (n=22)
- `opp_iso` r=+0.174 (n=22)
- `opp_xwoba` r=-0.162 (n=22)

**Optimal Context Weights**: Vegas=60% Park=10% Weather=30% (saves 0.02 MAE)

**Archetype Biases:**
- Power (ISO>.200): under-projected by 3.1 pts (n=41)
- Contact (K%<15%): under-projected by 1.8 pts (n=39)
- Strikeout (K%>28%): under-projected by 2.7 pts (n=26)
- Speed (SB pace>15): under-projected by 2.5 pts (n=58)

**Recommendations:**
- PROJECTION: increase SP_CALIBRATION by ~0.18 (current 0.90)
- PROJECTION: reduce context multiplier weights — hitter bias is +2.32 pts
- POOL: Best performing stack config is 3-0 — increase its weight in STACK_CONFIGS
- POOL: Projections have 5.7 pt spread — use projection rank as primary sort for portfolio selection
- CONTEST: Avg Top 1% threshold is 146.8 pts across 38 contests
- CONTEST: Avg cash line is 110.4 pts — pool floor should exceed this
- CONTEST: Avg winner scores 174.5 pts — need high-ceiling correlated stacks
- TRACKING: Run this analysis daily to build sample size — patterns stabilize after 2+ weeks

## Sim Validation — 2026-04-20

### Distribution Calibration
- **Hitters**: 100.0% in P10-P90 [FAIL] (below floor=0.0%, above ceiling=0.0%)
- **Pitchers**: 100.0% in P10-P90 [FAIL] (below floor=0.0%, above ceiling=0.0%)

### Projection Accuracy
- Overall: MAE=5.82, Bias=+3.47, r=-0.138
- Hitters: MAE=5.59 [PASS]
- Pitchers: MAE=7.9 [PASS]

### Pitcher Components
- IP: MAE=1.76, Bias=+1.76
- Ks: MAE=2.24, Bias=+2.24
- ER: MAE=1.11, Bias=+1.11

### Multiplier Effectiveness

## Research Findings — 2026-04-14, 2026-04-15, 2026-04-16, 2026-04-17, 2026-04-18, 2026-04-19, 2026-04-20, 2026-04-21

**Projection**: MAE=5.68, Bias=+0.75, Hitter MAE=5.52, Pitcher MAE=7.16
**Pool**: MAE=37.76, Bias=-34.94
**Contest**: Winner=176.2621951219512, Top1%=147.49146341463413

### Predictive Diagnostics

**Hitter Missing Predictors** (correlated with error but not in model):
- `swstr_pct` r=-0.123 (n=264)
- `ld_pct` r=-0.118 (n=264)
- `gb_pct` r=+0.104 (n=264)
- `sb` r=+0.087 (n=264)

**Pitcher Missing Predictors** (correlated with error but not in model):
- `barrel_pct` r=+0.391 (n=30)
- `ld_pct` r=-0.376 (n=30)
- `bb9` r=+0.342 (n=30)
- `avg` r=-0.329 (n=30)
- `wild_pitches` r=+0.309 (n=30)

**Opposing Lineup Factors** (for pitcher projections):
- `opp_o_swing_pct` r=+0.216 (n=32)
- `opp_woba` r=+0.149 (n=32)

**Optimal Context Weights**: Vegas=65% Park=5% Weather=30% (saves 0.01 MAE)

**Recommendations:**
- PROJECTION: increase SP_CALIBRATION by ~0.15 (current 0.90)
- PROJECTION: increase context multiplier weights — hitter bias is -0.58 pts
- POOL: Best performing stack config is 3-0 — increase its weight in STACK_CONFIGS
- POOL: Projection spread only -0.9 pts — diversify selection, don't over-rely on projection ranking
- CONTEST: Avg Top 1% threshold is 147.5 pts across 41 contests
- CONTEST: Avg cash line is 110.2 pts — pool floor should exceed this
- CONTEST: Avg winner scores 176.3 pts — need high-ceiling correlated stacks
- PROJECTION: context_mult is hurting accuracy (r=-0.224) — reduce its weight or cap its range
- PROJECTION: vegas_mult is hurting accuracy (r=-0.221) — reduce its weight or cap its range
- PROJECTION: park_mult is hurting accuracy (r=-0.231) — reduce its weight or cap its range
- TRACKING: Run this analysis daily to build sample size — patterns stabilize after 2+ weeks

## Sim Validation — 2026-04-21

### Distribution Calibration
- **Hitters**: 71.4% in P10-P90 [WARN] (below floor=15.8%, above ceiling=12.8%)
- **Pitchers**: 80.0% in P10-P90 [PASS] (below floor=10.0%, above ceiling=10.0%)

### Projection Accuracy
- Overall: MAE=5.69, Bias=+0.60, r=0.218
- Hitters: MAE=5.54 [PASS]
- Pitchers: MAE=7.02 [PASS]

### Pitcher Components
- IP: MAE=1.02, Bias=+0.05
- Ks: MAE=1.83, Bias=+0.69
- ER: MAE=1.42, Bias=+0.07

### Multiplier Effectiveness
- `pitcher_mult`: r=+0.011 [WARN]
- `platoon_mult`: r=+0.000 [WARN]
- `context_mult`: r=-0.225 [FAIL]
- `vegas_mult`: r=-0.222 [FAIL]
- `park_mult`: r=-0.228 [FAIL]
- `weather_mult`: r=+0.065 [PASS]

## Slate Review — 2026-04-21 / main

- **Pool**: 650 lineups, avg actual=79.3, cash line=77.3, GPP line=148.9, best=179.9
- **Proj accuracy**: r=-0.151, MAE=26.0, bias=+15.3
- **Overlap**: 1/20 top-by-proj were actual winners
- **Best strategy**: PMS (highest avg_pms)
- **Top stack**: CWS (avg actual=116.9, 6.8% exposure)
- **Biggest bust**: Jesús Luzardo (proj=15.7, actual=9.1, 22% exp)
- **Biggest missed opp**: Randy Vásquez (actual=27.9, 4.0% exp)

## Research Findings — 2026-04-16, 2026-04-17, 2026-04-18, 2026-04-19, 2026-04-20, 2026-04-21, 2026-04-22, 2026-04-23

**Projection**: MAE=5.68, Bias=+0.75, Hitter MAE=5.52, Pitcher MAE=7.16
**Pool**: MAE=41.75, Bias=-38.99
**Contest**: Winner=177.81309523809523, Top1%=148.39404761904763

### Predictive Diagnostics

**Hitter Missing Predictors** (correlated with error but not in model):
- `swstr_pct` r=-0.123 (n=264)
- `ld_pct` r=-0.118 (n=264)
- `gb_pct` r=+0.104 (n=264)
- `sb` r=+0.087 (n=264)

**Pitcher Missing Predictors** (correlated with error but not in model):
- `barrel_pct` r=+0.391 (n=30)
- `ld_pct` r=-0.376 (n=30)
- `bb9` r=+0.342 (n=30)
- `avg` r=-0.329 (n=30)
- `wild_pitches` r=+0.309 (n=30)

**Opposing Lineup Factors** (for pitcher projections):
- `opp_o_swing_pct` r=+0.216 (n=32)
- `opp_woba` r=+0.149 (n=32)

**Optimal Context Weights**: Vegas=65% Park=5% Weather=30% (saves 0.01 MAE)

**Recommendations:**
- PROJECTION: increase SP_CALIBRATION by ~0.15 (current 0.90)
- PROJECTION: increase context multiplier weights — hitter bias is -0.58 pts
- POOL: Best performing stack config is 3-2 — increase its weight in STACK_CONFIGS
- POOL: Projection spread only -8.9 pts — diversify selection, don't over-rely on projection ranking
- CONTEST: Avg Top 1% threshold is 148.4 pts across 42 contests
- CONTEST: Avg cash line is 110.7 pts — pool floor should exceed this
- CONTEST: Avg winner scores 177.8 pts — need high-ceiling correlated stacks
- PROJECTION: context_mult is hurting accuracy (r=-0.224) — reduce its weight or cap its range
- PROJECTION: vegas_mult is hurting accuracy (r=-0.221) — reduce its weight or cap its range
- PROJECTION: park_mult is hurting accuracy (r=-0.231) — reduce its weight or cap its range
- TRACKING: Run this analysis daily to build sample size — patterns stabilize after 2+ weeks

## Sim Validation — 2026-04-21

### Distribution Calibration
- **Hitters**: 71.4% in P10-P90 [WARN] (below floor=15.8%, above ceiling=12.8%)
- **Pitchers**: 80.0% in P10-P90 [PASS] (below floor=10.0%, above ceiling=10.0%)

### Projection Accuracy
- Overall: MAE=5.69, Bias=+0.60, r=0.218
- Hitters: MAE=5.54 [PASS]
- Pitchers: MAE=7.02 [PASS]

### Pitcher Components
- IP: MAE=1.02, Bias=+0.05
- Ks: MAE=1.83, Bias=+0.69
- ER: MAE=1.42, Bias=+0.07

### Multiplier Effectiveness
- `pitcher_mult`: r=+0.011 [WARN]
- `platoon_mult`: r=+0.000 [WARN]
- `context_mult`: r=-0.225 [FAIL]
- `vegas_mult`: r=-0.222 [FAIL]
- `park_mult`: r=-0.228 [FAIL]
- `weather_mult`: r=+0.065 [PASS]

## Slate Review — 2026-04-21 / main

- **Pool**: 650 lineups, avg actual=79.3, cash line=77.3, GPP line=148.9, best=179.9
- **Proj accuracy**: r=-0.151, MAE=26.0, bias=+15.3
- **Overlap**: 1/20 top-by-proj were actual winners
- **Best strategy**: PMS (highest avg_pms)
- **Top stack**: CWS (avg actual=116.9, 6.8% exposure)
- **Biggest bust**: Jesús Luzardo (proj=15.7, actual=9.1, 22% exp)
- **Biggest missed opp**: Randy Vásquez (actual=27.9, 4.0% exp)

## Research Findings — 2026-04-16, 2026-04-17, 2026-04-18, 2026-04-19, 2026-04-20, 2026-04-21, 2026-04-22, 2026-04-23

**Projection**: MAE=5.68, Bias=+0.75, Hitter MAE=5.52, Pitcher MAE=7.16
**Pool**: MAE=41.75, Bias=-38.99
**Contest**: Winner=177.81309523809523, Top1%=148.39404761904763

### Predictive Diagnostics

**Hitter Missing Predictors** (correlated with error but not in model):
- `swstr_pct` r=-0.123 (n=264)
- `ld_pct` r=-0.118 (n=264)
- `gb_pct` r=+0.104 (n=264)
- `sb` r=+0.087 (n=264)

**Pitcher Missing Predictors** (correlated with error but not in model):
- `barrel_pct` r=+0.391 (n=30)
- `ld_pct` r=-0.376 (n=30)
- `bb9` r=+0.342 (n=30)
- `avg` r=-0.329 (n=30)
- `wild_pitches` r=+0.309 (n=30)

**Opposing Lineup Factors** (for pitcher projections):
- `opp_o_swing_pct` r=+0.216 (n=32)
- `opp_woba` r=+0.149 (n=32)

**Optimal Context Weights**: Vegas=65% Park=5% Weather=30% (saves 0.01 MAE)

**Recommendations:**
- PROJECTION: increase SP_CALIBRATION by ~0.15 (current 0.90)
- PROJECTION: increase context multiplier weights — hitter bias is -0.58 pts
- POOL: Best performing stack config is 3-2 — increase its weight in STACK_CONFIGS
- POOL: Projection spread only -8.9 pts — diversify selection, don't over-rely on projection ranking
- CONTEST: Avg Top 1% threshold is 148.4 pts across 42 contests
- CONTEST: Avg cash line is 110.7 pts — pool floor should exceed this
- CONTEST: Avg winner scores 177.8 pts — need high-ceiling correlated stacks
- PROJECTION: context_mult is hurting accuracy (r=-0.224) — reduce its weight or cap its range
- PROJECTION: vegas_mult is hurting accuracy (r=-0.221) — reduce its weight or cap its range
- PROJECTION: park_mult is hurting accuracy (r=-0.231) — reduce its weight or cap its range
- TRACKING: Run this analysis daily to build sample size — patterns stabilize after 2+ weeks

## Sim Validation — 2026-04-21

### Distribution Calibration
- **Hitters**: 71.4% in P10-P90 [WARN] (below floor=15.8%, above ceiling=12.8%)
- **Pitchers**: 80.0% in P10-P90 [PASS] (below floor=10.0%, above ceiling=10.0%)

### Projection Accuracy
- Overall: MAE=5.69, Bias=+0.60, r=0.218
- Hitters: MAE=5.54 [PASS]
- Pitchers: MAE=7.02 [PASS]

### Pitcher Components
- IP: MAE=1.02, Bias=+0.05
- Ks: MAE=1.83, Bias=+0.69
- ER: MAE=1.42, Bias=+0.07

### Multiplier Effectiveness
- `pitcher_mult`: r=+0.011 [WARN]
- `platoon_mult`: r=+0.000 [WARN]
- `context_mult`: r=-0.225 [FAIL]
- `vegas_mult`: r=-0.222 [FAIL]
- `park_mult`: r=-0.228 [FAIL]
- `weather_mult`: r=+0.065 [PASS]

## Slate Review — 2026-04-21 / main

- **Pool**: 650 lineups, avg actual=79.3, cash line=77.3, GPP line=148.9, best=179.9
- **Proj accuracy**: r=-0.151, MAE=26.0, bias=+15.3
- **Overlap**: 1/20 top-by-proj were actual winners
- **Best strategy**: PMS (highest avg_pms)
- **Top stack**: CWS (avg actual=116.9, 6.8% exposure)
- **Biggest bust**: Jesús Luzardo (proj=15.7, actual=9.1, 22% exp)
- **Biggest missed opp**: Randy Vásquez (actual=27.9, 4.0% exp)

## Research Findings — 2026-04-16, 2026-04-17, 2026-04-18, 2026-04-19, 2026-04-20, 2026-04-21, 2026-04-22, 2026-04-23

**Projection**: MAE=5.72, Bias=+1.47, Hitter MAE=5.57, Pitcher MAE=7.04
**Pool**: MAE=41.75, Bias=-38.99
**Contest**: Winner=177.81309523809523, Top1%=148.39404761904763

### Predictive Diagnostics

**Hitter Missing Predictors** (correlated with error but not in model):
- `swstr_pct` r=-0.121 (n=267)
- `gb_pct` r=+0.094 (n=267)
- `ld_pct` r=-0.068 (n=267)
- `hr` r=-0.061 (n=267)
- `fb_pct` r=-0.057 (n=267)

**Pitcher Missing Predictors** (correlated with error but not in model):
- `barrel_pct` r=+0.401 (n=30)
- `ld_pct` r=-0.378 (n=30)
- `bb9` r=+0.335 (n=30)
- `avg` r=-0.332 (n=30)
- `wild_pitches` r=+0.308 (n=30)

**Opposing Lineup Factors** (for pitcher projections):
- `opp_woba` r=+0.298 (n=32)
- `opp_wrc_plus` r=+0.199 (n=32)
- `opp_xwoba` r=+0.180 (n=32)
- `opp_bb_pct` r=+0.161 (n=32)
- `opp_gb_pct` r=+0.128 (n=32)

**Optimal Context Weights**: Vegas=80% Park=5% Weather=15% (saves -0.00 MAE)

**Archetype Biases:**
- Power (ISO>.200): over-projected by 1.8 pts (n=60)
- Strikeout (K%>28%): over-projected by 0.9 pts (n=42)
- Speed (SB pace>15): over-projected by 1.2 pts (n=76)

**Recommendations:**
- PROJECTION: increase SP_CALIBRATION by ~0.16 (current 0.90)
- PROJECTION: increase context multiplier weights — hitter bias is -1.37 pts
- POOL: Best performing stack config is 3-2 — increase its weight in STACK_CONFIGS
- POOL: Projection spread only -8.9 pts — diversify selection, don't over-rely on projection ranking
- CONTEST: Avg Top 1% threshold is 148.4 pts across 42 contests
- CONTEST: Avg cash line is 110.7 pts — pool floor should exceed this
- CONTEST: Avg winner scores 177.8 pts — need high-ceiling correlated stacks
- PROJECTION: context_mult is hurting accuracy (r=-0.147) — reduce its weight or cap its range
- PROJECTION: vegas_mult is hurting accuracy (r=-0.150) — reduce its weight or cap its range
- PROJECTION: park_mult is hurting accuracy (r=-0.150) — reduce its weight or cap its range
- TRACKING: Run this analysis daily to build sample size — patterns stabilize after 2+ weeks

## Sim Validation — 2026-04-21

### Distribution Calibration
- **Hitters**: 89.2% in P10-P90 [WARN] (below floor=2.2%, above ceiling=8.6%)
- **Pitchers**: 80.0% in P10-P90 [PASS] (below floor=10.0%, above ceiling=10.0%)

### Projection Accuracy
- Overall: MAE=5.74, Bias=+1.39, r=0.318
- Hitters: MAE=5.61 [PASS]
- Pitchers: MAE=6.91 [PASS]

### Pitcher Components
- IP: MAE=1.05, Bias=+0.09
- Ks: MAE=1.86, Bias=+0.75
- ER: MAE=1.42, Bias=+0.08

### Multiplier Effectiveness

## Slate Review — 2026-04-21 / main

- **Pool**: 650 lineups, avg actual=79.3, cash line=77.3, GPP line=148.9, best=179.9
- **Proj accuracy**: r=-0.151, MAE=26.0, bias=+15.3
- **Overlap**: 1/20 top-by-proj were actual winners
- **Best strategy**: PMS (highest avg_pms)
- **Top stack**: CWS (avg actual=116.9, 6.8% exposure)
- **Biggest bust**: Jesús Luzardo (proj=15.3, actual=9.1, 22% exp)
- **Biggest missed opp**: Randy Vásquez (actual=27.9, 4.0% exp)

## Research Findings — 2026-04-16, 2026-04-17, 2026-04-18, 2026-04-19, 2026-04-20, 2026-04-21, 2026-04-22, 2026-04-23

**Projection**: MAE=5.57, Bias=+0.99, Hitter MAE=5.41, Pitcher MAE=7.05
**Pool**: MAE=41.75, Bias=-38.99
**Contest**: Winner=177.81309523809523, Top1%=148.39404761904763

### Predictive Diagnostics

**Hitter Missing Predictors** (correlated with error but not in model):
- `swstr_pct` r=-0.108 (n=267)
- `gb_pct` r=+0.093 (n=267)
- `ld_pct` r=-0.072 (n=267)
- `fb_pct` r=-0.054 (n=267)

**Pitcher Missing Predictors** (correlated with error but not in model):
- `barrel_pct` r=+0.399 (n=30)
- `ld_pct` r=-0.380 (n=30)
- `avg` r=-0.337 (n=30)
- `bb9` r=+0.332 (n=30)
- `wild_pitches` r=+0.304 (n=30)

**Opposing Lineup Factors** (for pitcher projections):
- `opp_woba` r=+0.302 (n=32)
- `opp_wrc_plus` r=+0.204 (n=32)
- `opp_xwoba` r=+0.184 (n=32)
- `opp_bb_pct` r=+0.165 (n=32)
- `opp_gb_pct` r=+0.124 (n=32)

**Optimal Context Weights**: Vegas=80% Park=5% Weather=15% (saves -0.00 MAE)

**Archetype Biases:**
- Power (ISO>.200): over-projected by 1.0 pts (n=60)

**Recommendations:**
- PROJECTION: increase SP_CALIBRATION by ~0.16 (current 0.90)
- PROJECTION: increase context multiplier weights — hitter bias is -0.83 pts
- POOL: Best performing stack config is 3-2 — increase its weight in STACK_CONFIGS
- POOL: Projection spread only -8.9 pts — diversify selection, don't over-rely on projection ranking
- CONTEST: Avg Top 1% threshold is 148.4 pts across 42 contests
- CONTEST: Avg cash line is 110.7 pts — pool floor should exceed this
- CONTEST: Avg winner scores 177.8 pts — need high-ceiling correlated stacks
- PROJECTION: context_mult is hurting accuracy (r=-0.149) — reduce its weight or cap its range
- PROJECTION: vegas_mult is hurting accuracy (r=-0.151) — reduce its weight or cap its range
- PROJECTION: park_mult is hurting accuracy (r=-0.151) — reduce its weight or cap its range
- TRACKING: Run this analysis daily to build sample size — patterns stabilize after 2+ weeks

## Sim Validation — 2026-04-21

### Distribution Calibration
- **Hitters**: 89.6% in P10-P90 [WARN] (below floor=1.5%, above ceiling=8.9%)
- **Pitchers**: 80.0% in P10-P90 [PASS] (below floor=10.0%, above ceiling=10.0%)

### Projection Accuracy
- Overall: MAE=5.6, Bias=+0.90, r=0.301
- Hitters: MAE=5.45 [PASS]
- Pitchers: MAE=6.92 [PASS]

### Pitcher Components
- IP: MAE=1.05, Bias=+0.09
- Ks: MAE=1.87, Bias=+0.76
- ER: MAE=1.42, Bias=+0.08

### Multiplier Effectiveness

## Slate Review — 2026-04-21 / main

- **Pool**: 650 lineups, avg actual=79.3, cash line=77.3, GPP line=148.9, best=179.9
- **Proj accuracy**: r=-0.151, MAE=26.0, bias=+15.3
- **Overlap**: 1/20 top-by-proj were actual winners
- **Best strategy**: PMS (highest avg_pms)
- **Top stack**: CWS (avg actual=116.9, 6.8% exposure)
- **Biggest bust**: Jesús Luzardo (proj=15.2, actual=9.1, 22% exp)
- **Biggest missed opp**: Randy Vásquez (actual=27.9, 4.0% exp)


## Ownership Calibration — 38 large-field contests (≥1000 entries)

- **Matched players**: 357
- **Bias**: +5.11% (positive = over-project ownership)
- **MAE**: 5.66%
- **Correlation**: r=0.796

- Chalk (>20% actual): n=24, bias=+4.10%
- Mid (5-20% actual): n=120, bias=+6.13%
- Low (<5% actual): n=213, bias=+4.66%

**Over-projected ownership:**
- Jesus Luzardo: proj=51.4% actual=16.7%
- Brandon Woodruff: proj=40.6% actual=11.4%
- Nathan Eovaldi: proj=34.9% actual=8.5%
- Shota Imanaga: proj=34.5% actual=10.3%
- Braxton Ashcraft: proj=37.3% actual=13.3%

**Under-projected ownership:**
- David Peterson: proj=13.1% actual=24.7%
- Chad Patrick: proj=11.4% actual=20.5%
- Logan Webb: proj=30.1% actual=38.8%
- Grant Holmes: proj=14.7% actual=21.2%
- Bubba Chandler: proj=19.6% actual=24.4%


## Ownership Calibration — 38 large-field contests (≥1000 entries)

- **Matched players**: 357
- **Bias**: +5.12% (positive = over-project ownership)
- **MAE**: 5.66%
- **Correlation**: r=0.794

- Chalk (>20% actual): n=24, bias=+3.94%
- Mid (5-20% actual): n=120, bias=+6.10%
- Low (<5% actual): n=213, bias=+4.69%

**Over-projected ownership:**
- Jesus Luzardo: proj=51.4% actual=16.7%
- Brandon Woodruff: proj=40.6% actual=11.4%
- Nathan Eovaldi: proj=34.9% actual=8.5%
- Shota Imanaga: proj=34.5% actual=10.3%
- Braxton Ashcraft: proj=37.3% actual=13.3%

**Under-projected ownership:**
- David Peterson: proj=13.1% actual=24.7%
- Logan Webb: proj=29.7% actual=38.8%
- Chad Patrick: proj=11.4% actual=20.5%
- Grant Holmes: proj=14.7% actual=21.2%
- Jacob Misiorowski: proj=15.2% actual=19.6%

## Research Findings — 2026-04-17, 2026-04-18, 2026-04-19, 2026-04-20, 2026-04-21, 2026-04-22, 2026-04-23, 2026-04-24

**Projection**: MAE=5.57, Bias=+0.99, Hitter MAE=5.41, Pitcher MAE=7.05
**Pool**: MAE=44.79, Bias=-42.90
**Contest**: Winner=178.78372093023256, Top1%=149.1139534883721

### Predictive Diagnostics

**Hitter Missing Predictors** (correlated with error but not in model):
- `swstr_pct` r=-0.108 (n=267)
- `gb_pct` r=+0.093 (n=267)
- `ld_pct` r=-0.072 (n=267)
- `fb_pct` r=-0.054 (n=267)

**Pitcher Missing Predictors** (correlated with error but not in model):
- `barrel_pct` r=+0.399 (n=30)
- `ld_pct` r=-0.380 (n=30)
- `avg` r=-0.337 (n=30)
- `bb9` r=+0.332 (n=30)
- `wild_pitches` r=+0.304 (n=30)

**Opposing Lineup Factors** (for pitcher projections):
- `opp_woba` r=+0.302 (n=32)
- `opp_wrc_plus` r=+0.204 (n=32)
- `opp_xwoba` r=+0.184 (n=32)
- `opp_bb_pct` r=+0.165 (n=32)
- `opp_gb_pct` r=+0.124 (n=32)

**Optimal Context Weights**: Vegas=80% Park=5% Weather=15% (saves -0.00 MAE)

**Archetype Biases:**
- Power (ISO>.200): over-projected by 1.0 pts (n=60)

**Recommendations:**
- PROJECTION: increase SP_CALIBRATION by ~0.16 (current 0.90)
- PROJECTION: increase context multiplier weights — hitter bias is -0.83 pts
- POOL: Best performing stack config is 4-4 — increase its weight in STACK_CONFIGS
- POOL: Projection spread only 2.6 pts — diversify selection, don't over-rely on projection ranking
- CONTEST: Avg Top 1% threshold is 149.1 pts across 43 contests
- CONTEST: Avg cash line is 111.2 pts — pool floor should exceed this
- CONTEST: Avg winner scores 178.8 pts — need high-ceiling correlated stacks
- PROJECTION: context_mult is hurting accuracy (r=-0.149) — reduce its weight or cap its range
- PROJECTION: vegas_mult is hurting accuracy (r=-0.151) — reduce its weight or cap its range
- PROJECTION: park_mult is hurting accuracy (r=-0.151) — reduce its weight or cap its range
- TRACKING: Run this analysis daily to build sample size — patterns stabilize after 2+ weeks

## Sim Validation — 2026-04-21

### Distribution Calibration
- **Hitters**: 89.6% in P10-P90 [WARN] (below floor=1.5%, above ceiling=8.9%)
- **Pitchers**: 80.0% in P10-P90 [PASS] (below floor=10.0%, above ceiling=10.0%)

### Projection Accuracy
- Overall: MAE=5.6, Bias=+0.90, r=0.301
- Hitters: MAE=5.45 [PASS]
- Pitchers: MAE=6.92 [PASS]

### Pitcher Components
- IP: MAE=1.05, Bias=+0.09
- Ks: MAE=1.87, Bias=+0.76
- ER: MAE=1.42, Bias=+0.08

### Multiplier Effectiveness

## Slate Review — 2026-04-21 / main

- **Pool**: 650 lineups, avg actual=79.3, cash line=77.3, GPP line=148.9, best=179.9
- **Proj accuracy**: r=-0.151, MAE=26.0, bias=+15.3
- **Overlap**: 1/20 top-by-proj were actual winners
- **Best strategy**: PMS (highest avg_pms)
- **Top stack**: CWS (avg actual=116.9, 6.8% exposure)
- **Biggest bust**: Jesús Luzardo (proj=15.2, actual=9.1, 22% exp)
- **Biggest missed opp**: Randy Vásquez (actual=27.9, 4.0% exp)


## Ownership Calibration — 39 large-field contests (≥1000 entries)

- **Matched players**: 362
- **Bias**: +5.15% (positive = over-project ownership)
- **MAE**: 5.67%
- **Correlation**: r=0.796

- Chalk (>20% actual): n=23, bias=+3.87%
- Mid (5-20% actual): n=123, bias=+6.13%
- Low (<5% actual): n=216, bias=+4.73%

**Over-projected ownership:**
- Jesus Luzardo: proj=51.4% actual=16.7%
- Brandon Woodruff: proj=40.6% actual=11.4%
- Nathan Eovaldi: proj=34.9% actual=8.5%
- Shota Imanaga: proj=34.5% actual=10.3%
- Braxton Ashcraft: proj=37.3% actual=13.3%

**Under-projected ownership:**
- David Peterson: proj=13.1% actual=24.7%
- Chad Patrick: proj=11.4% actual=20.5%
- Grant Holmes: proj=14.7% actual=21.2%
- Ozzie Albies: proj=6.1% actual=10.6%
- Jacob Misiorowski: proj=15.2% actual=19.6%

## Sim Validation — 2026-04-23

### Distribution Calibration
- **Hitters**: 87.7% in P10-P90 [WARN] (below floor=1.2%, above ceiling=11.1%)
- **Pitchers**: 47.1% in P10-P90 [FAIL] (below floor=23.5%, above ceiling=29.4%)

### Projection Accuracy
- Overall: MAE=6.76, Bias=+0.38, r=0.317
- Hitters: MAE=6.38 [WARN]
- Pitchers: MAE=10.43 [WARN]

### Pitcher Components
- IP: MAE=1.15, Bias=-0.55
- Ks: MAE=2.01, Bias=-1.07
- ER: MAE=1.57, Bias=-0.35

### Multiplier Effectiveness

## Research Findings — 2026-04-17, 2026-04-18, 2026-04-19, 2026-04-20, 2026-04-21, 2026-04-22, 2026-04-23, 2026-04-24

**Projection**: MAE=5.95, Bias=+0.88, Hitter MAE=5.68, Pitcher MAE=8.40
**Pool**: MAE=29.82, Bias=+18.41
**Contest**: Winner=178.78372093023256, Top1%=149.1139534883721

### Predictive Diagnostics

**Hitter Missing Predictors** (correlated with error but not in model):
- `swstr_pct` r=-0.053 (n=673)

**Opposing Lineup Factors** (for pitcher projections):
- `opp_bb_pct` r=+0.218 (n=79)
- `opp_hard_hit_pct` r=-0.147 (n=79)
- `opp_barrel_pct` r=-0.146 (n=79)
- `opp_woba` r=+0.119 (n=79)
- `opp_wrc_plus` r=+0.106 (n=79)

**Optimal Context Weights**: Vegas=80% Park=5% Weather=15% (saves -0.00 MAE)

**Archetype Biases:**
- Speed (SB pace>15): over-projected by 1.1 pts (n=195)

**Recommendations:**
- PROJECTION: increase context multiplier weights — hitter bias is -0.85 pts
- POOL: Best performing stack config is 4-4 — increase its weight in STACK_CONFIGS
- POOL: Projections have 57.0 pt spread — use projection rank as primary sort for portfolio selection
- CONTEST: Avg Top 1% threshold is 149.1 pts across 43 contests
- CONTEST: Avg cash line is 111.2 pts — pool floor should exceed this
- CONTEST: Avg winner scores 178.8 pts — need high-ceiling correlated stacks
- TRACKING: Run this analysis daily to build sample size — patterns stabilize after 2+ weeks

## Leverage Analysis — 2026-04-24 (26 contests, 3301 players)

**Dataset**: 2991 hitters, 310 pitchers across 20 dates
**Leverage hits**: 364 (11.0%) | **Chalk traps**: 68 (2.1%) | **Ceiling hits**: 309 (9.4%)

### Hitter Predictors (correlation with outperformance)
- `wRC+` r=+0.218 (n=2991)
- `ISO` r=+0.192 (n=2991)
- `xwOBA` r=+0.159 (n=2991)
- `Salary` r=+0.110 (n=2991)
- `Barrel%` r=+0.094 (n=2991)

### Pitcher Predictors
- `K%` r=+0.320 (n=310)
- `Salary` r=+0.319 (n=310)
- `xFIP` r=-0.294 (n=310)
- `Win Prob` r=+0.243 (n=310)
- `Stuff+` r=+0.179 (n=309)

### Actionable Rules
- **Hitter leverage**: ISO > 0.200 AND own < 10% -> 27% leverage rate (+17pp vs base)
- **Hitter leverage**: wRC+ > 120 AND own < 10% -> 22% leverage rate (+12pp vs base)
- **Hitter trap**: Own > 20% AND K% > 0.28 -> 68% chalk trap rate
- **Pitcher leverage**: K% > 0.25 AND own < 15% -> 33% leverage rate (+17pp vs base)
- **Pitcher leverage**: Stuff+ > 105 AND own < 15% -> 31% leverage rate (+14pp vs base)
- **Pitcher trap**: Own > 25% AND Stuff+ < 100 -> 59% chalk trap rate
- **Pitcher trap**: Own > 25% AND xFIP > 4.0 -> 56% chalk trap rate

## Research Findings — 2026-04-18, 2026-04-19, 2026-04-20, 2026-04-21, 2026-04-22, 2026-04-23, 2026-04-24, 2026-04-25

**Projection**: MAE=5.95, Bias=+0.88, Hitter MAE=5.68, Pitcher MAE=8.40
**Pool**: MAE=27.46, Bias=+14.26
**Contest**: Winner=179.92045454545453, Top1%=149.47954545454547

### Predictive Diagnostics

**Hitter Missing Predictors** (correlated with error but not in model):
- `swstr_pct` r=-0.053 (n=673)

**Opposing Lineup Factors** (for pitcher projections):
- `opp_bb_pct` r=+0.218 (n=79)
- `opp_hard_hit_pct` r=-0.147 (n=79)
- `opp_barrel_pct` r=-0.146 (n=79)
- `opp_woba` r=+0.119 (n=79)
- `opp_wrc_plus` r=+0.106 (n=79)

**Optimal Context Weights**: Vegas=80% Park=5% Weather=15% (saves -0.00 MAE)

**Archetype Biases:**
- Speed (SB pace>15): over-projected by 1.1 pts (n=195)

**Recommendations:**
- PROJECTION: increase context multiplier weights — hitter bias is -0.85 pts
- POOL: Best performing stack config is 4-4 — increase its weight in STACK_CONFIGS
- POOL: Projections have 58.1 pt spread — use projection rank as primary sort for portfolio selection
- CONTEST: Avg Top 1% threshold is 149.5 pts across 44 contests
- CONTEST: Avg cash line is 111.3 pts — pool floor should exceed this
- CONTEST: Avg winner scores 179.9 pts — need high-ceiling correlated stacks
- TRACKING: Run this analysis daily to build sample size — patterns stabilize after 2+ weeks

## Sim Validation — 2026-04-23

### Distribution Calibration
- **Hitters**: 87.7% in P10-P90 [WARN] (below floor=1.2%, above ceiling=11.1%)
- **Pitchers**: 47.1% in P10-P90 [FAIL] (below floor=23.5%, above ceiling=29.4%)

### Projection Accuracy
- Overall: MAE=6.76, Bias=+0.38, r=0.317
- Hitters: MAE=6.38 [WARN]
- Pitchers: MAE=10.43 [WARN]

### Pitcher Components
- IP: MAE=1.15, Bias=-0.55
- Ks: MAE=2.01, Bias=-1.07
- ER: MAE=1.57, Bias=-0.35

### Multiplier Effectiveness

## Slate Review — 2026-04-23 / main

- **Pool**: 10000 lineups, avg actual=96.0, cash line=95.1, GPP line=172.9, best=216.8
- **Proj accuracy**: r=0.148, MAE=27.7, bias=+10.2
- **Overlap**: 0/20 top-by-proj were actual winners
- **Best strategy**: Salary (highest salary)
- **Top stack**: CHC (avg actual=128.9, 9.1% exposure)
- **Biggest bust**: Tarik Skubal (proj=20.6, actual=11.3, 50% exp)
- **Biggest missed opp**: Brandon Marsh (actual=35.0, 8.4% exp)


## Ownership Calibration — 40 large-field contests (≥1000 entries)

- **Matched players**: 562
- **Bias**: +4.73% (positive = over-project ownership)
- **MAE**: 5.36%
- **Correlation**: r=0.705

- Chalk (>20% actual): n=31, bias=+1.83%
- Mid (5-20% actual): n=175, bias=+5.36%
- Low (<5% actual): n=356, bias=+4.67%

**Over-projected ownership:**
- Mason Fluharty: proj=71.4% actual=0.6%
- Logan Gilbert: proj=45.5% actual=0.0%
- Jesus Luzardo: proj=51.4% actual=16.7%
- Ryan Weiss: proj=33.8% actual=0.0%
- Kyle Bradish: proj=38.1% actual=9.8%

**Under-projected ownership:**
- Cody Ponce: proj=22.8% actual=47.3%
- Dustin May: proj=13.6% actual=31.5%
- David Peterson: proj=13.1% actual=24.7%
- Dylan Cease: proj=33.6% actual=44.1%
- Chad Patrick: proj=11.4% actual=20.5%

## Leverage Analysis — 2026-04-25 (27 contests, 3557 players)

**Dataset**: 3224 hitters, 333 pitchers across 21 dates
**Leverage hits**: 401 (11.3%) | **Chalk traps**: 69 (1.9%) | **Ceiling hits**: 343 (9.6%)

### Hitter Predictors (correlation with outperformance)
- `wRC+` r=+0.195 (n=3222)
- `ISO` r=+0.176 (n=3222)
- `xwOBA` r=+0.136 (n=3222)
- `Salary` r=+0.092 (n=3224)
- `Barrel%` r=+0.079 (n=3222)

### Pitcher Predictors
- `K%` r=+0.320 (n=333)
- `Salary` r=+0.305 (n=333)
- `xFIP` r=-0.292 (n=333)
- `Win Prob` r=+0.226 (n=333)
- `Stuff+` r=+0.180 (n=332)

### Actionable Rules
- **Hitter leverage**: ISO > 0.200 AND own < 10% -> 27% leverage rate (+17pp vs base)
- **Hitter leverage**: wRC+ > 120 AND own < 10% -> 22% leverage rate (+12pp vs base)
- **Hitter trap**: Own > 20% AND K% > 0.28 -> 68% chalk trap rate
- **Pitcher leverage**: K% > 0.25 AND own < 15% -> 33% leverage rate (+17pp vs base)
- **Pitcher leverage**: Stuff+ > 105 AND own < 15% -> 31% leverage rate (+14pp vs base)
- **Pitcher trap**: Own > 25% AND Stuff+ < 100 -> 59% chalk trap rate
- **Pitcher trap**: Own > 25% AND xFIP > 4.0 -> 56% chalk trap rate

## Winner Pattern Analysis — 2026-04-25 (44 contests, 595,517 entries)
- Top 1% profile: 141% total own, 3.9 booms, 0.7 busts, 48 pitcher pts
- Target: ownership 100-150%, 3+ booms, <1 bust, 25+ pitcher pts

## Sim Validation — 2026-04-24

### Distribution Calibration
- **Hitters**: 85.3% in P10-P90 [WARN] (below floor=1.6%, above ceiling=13.1%)
- **Pitchers**: 71.4% in P10-P90 [WARN] (below floor=21.4%, above ceiling=7.1%)

### Projection Accuracy
- Overall: MAE=6.43, Bias=+0.36, r=0.11
- Hitters: MAE=6.09 [WARN]
- Pitchers: MAE=9.5 [WARN]

### Pitcher Components
- IP: MAE=1.09, Bias=-0.06
- Ks: MAE=1.94, Bias=+0.75
- ER: MAE=1.9, Bias=-0.94

### Multiplier Effectiveness

## Slate Review — 2026-04-24 / main

- **Pool**: 15000 lineups, avg actual=87.1, cash line=85.8, GPP line=162.0, best=220.0
- **Proj accuracy**: r=0.130, MAE=24.5, bias=+8.4
- **Overlap**: 0/20 top-by-proj were actual winners
- **Best strategy**: Salary (highest salary)
- **Top stack**: BAL (avg actual=124.9, 4.2% exposure)
- **Biggest bust**: Nathan Eovaldi (proj=16.8, actual=3.3, 16% exp)
- **Biggest missed opp**: Adley Rutschman (actual=39.0, 4.1% exp)

## Research Findings — 2026-04-18, 2026-04-19, 2026-04-20, 2026-04-21, 2026-04-22, 2026-04-23, 2026-04-24, 2026-04-25

**Projection**: MAE=6.11, Bias=+0.54, Hitter MAE=5.83, Pitcher MAE=8.59
**Pool**: MAE=51.31, Bias=+50.70
**Contest**: Winner=180.55543478260867, Top1%=149.9076086956522

### Predictive Diagnostics

**Hitter Missing Predictors** (correlated with error but not in model):
- `hard_hit_pct` r=+0.082 (n=1156)
- `barrel_pct` r=+0.075 (n=1156)

**Pitcher Missing Predictors** (correlated with error but not in model):
- `k9` r=+0.128 (n=126)
- `avg` r=-0.126 (n=126)
- `g` r=-0.106 (n=126)

**Opposing Lineup Factors** (for pitcher projections):
- `opp_hard_hit_pct` r=-0.190 (n=135)
- `opp_barrel_pct` r=-0.176 (n=135)

**Optimal Context Weights**: Vegas=80% Park=5% Weather=15% (saves -0.00 MAE)

**Recommendations:**
- PROJECTION: increase SP_CALIBRATION by ~0.13 (current 0.90)
- POOL: Best performing stack config is 4-4 — increase its weight in STACK_CONFIGS
- POOL: Projections have 40.5 pt spread — use projection rank as primary sort for portfolio selection
- CONTEST: Avg Top 1% threshold is 149.9 pts across 46 contests
- CONTEST: Avg cash line is 111.6 pts — pool floor should exceed this
- CONTEST: Avg winner scores 180.6 pts — need high-ceiling correlated stacks
- TRACKING: Run this analysis daily to build sample size — patterns stabilize after 2+ weeks

## Sim Validation — 2026-04-25

### Distribution Calibration
- **Hitters**: 86.9% in P10-P90 [WARN] (below floor=1.2%, above ceiling=11.9%)
- **Pitchers**: 64.3% in P10-P90 [FAIL] (below floor=21.4%, above ceiling=14.3%)

### Projection Accuracy
- Overall: MAE=6.24, Bias=-0.24, r=0.146
- Hitters: MAE=6.02 [WARN]
- Pitchers: MAE=8.2 [WARN]

### Pitcher Components
- IP: MAE=1.04, Bias=+0.33
- Ks: MAE=2.1, Bias=+0.16
- ER: MAE=1.44, Bias=-0.25

### Multiplier Effectiveness

## Slate Review — 2026-04-25 / early

- **Pool**: 300 lineups, avg actual=89.4, cash line=87.0, GPP line=158.5, best=166.0
- **Proj accuracy**: r=-0.075, MAE=24.2, bias=+5.9
- **Overlap**: 0/20 top-by-proj were actual winners
- **Best strategy**: Value (proj/salary)
- **Top stack**: SEA (avg actual=122.0, 11.7% exposure)
- **Biggest bust**: Bryan Woo (proj=17.9, actual=-10.6, 30% exp)
- **Biggest missed opp**: Nathan Church (actual=32.0, 3.7% exp)

## Slate Review — 2026-04-25 / main

- **Pool**: 10500 lineups, avg actual=95.9, cash line=94.9, GPP line=158.1, best=212.1
- **Proj accuracy**: r=0.283, MAE=20.3, bias=+4.8
- **Overlap**: 0/20 top-by-proj were actual winners
- **Best strategy**: Salary (highest salary)
- **Top stack**: CIN (avg actual=122.1, 10.8% exposure)
- **Biggest bust**: Zack Wheeler (proj=22.3, actual=15.6, 53% exp)
- **Biggest missed opp**: Sal Stewart (actual=30.0, 10.1% exp)


## Ownership Calibration — 42 large-field contests (≥1000 entries)

- **Matched players**: 566
- **Bias**: +4.78% (positive = over-project ownership)
- **MAE**: 5.37%
- **Correlation**: r=0.708

- Chalk (>20% actual): n=30, bias=+2.23%
- Mid (5-20% actual): n=193, bias=+5.38%
- Low (<5% actual): n=343, bias=+4.67%

**Over-projected ownership:**
- Mason Fluharty: proj=71.4% actual=0.6%
- Logan Gilbert: proj=45.5% actual=0.0%
- Jesus Luzardo: proj=51.4% actual=16.7%
- Ryan Weiss: proj=33.8% actual=0.0%
- Zack Wheeler: proj=39.8% actual=7.4%

**Under-projected ownership:**
- Cody Ponce: proj=22.8% actual=47.3%
- Dustin May: proj=13.6% actual=31.5%
- Dylan Cease: proj=33.6% actual=44.1%
- Chad Patrick: proj=11.4% actual=20.5%
- Jacob Misiorowski: proj=16.0% actual=24.9%

## Leverage Analysis — 2026-04-25 (29 contests, 2007 players)

**Dataset**: 1811 hitters, 196 pitchers across 22 dates
**Leverage hits**: 216 (10.8%) | **Chalk traps**: 65 (3.2%) | **Ceiling hits**: 189 (9.4%)

### Hitter Predictors (correlation with outperformance)
- `wRC+` r=+0.205 (n=1810)
- `ISO` r=+0.174 (n=1810)
- `xwOBA` r=+0.145 (n=1810)
- `Salary` r=+0.108 (n=1811)
- `Barrel%` r=+0.081 (n=1810)

### Pitcher Predictors
- `K%` r=+0.244 (n=195)
- `Salary` r=+0.149 (n=196)
- `xFIP` r=-0.198 (n=195)
- `Win Prob` r=+0.222 (n=196)
- `Stuff+` r=+0.077 (n=195)

### Actionable Rules
- **Hitter leverage**: ISO > 0.200 AND own < 10% -> 27% leverage rate (+17pp vs base)
- **Hitter leverage**: wRC+ > 120 AND own < 10% -> 22% leverage rate (+12pp vs base)
- **Hitter trap**: Own > 20% AND K% > 0.28 -> 68% chalk trap rate
- **Pitcher leverage**: K% > 0.25 AND own < 15% -> 33% leverage rate (+17pp vs base)
- **Pitcher leverage**: Stuff+ > 105 AND own < 15% -> 31% leverage rate (+14pp vs base)
- **Pitcher trap**: Own > 25% AND Stuff+ < 100 -> 59% chalk trap rate
- **Pitcher trap**: Own > 25% AND xFIP > 4.0 -> 56% chalk trap rate

## Winner Pattern Analysis — 2026-04-25 (46 contests, 604,404 entries)
- Top 1% profile: 141% total own, 3.9 booms, 0.7 busts, 48 pitcher pts
- Target: ownership 100-150%, 3+ booms, <1 bust, 25+ pitcher pts

## Research Findings — 2026-04-19, 2026-04-20, 2026-04-21, 2026-04-22, 2026-04-23, 2026-04-24, 2026-04-25, 2026-04-26

**Projection**: MAE=6.12, Bias=+0.66, Hitter MAE=5.85, Pitcher MAE=8.61
**Pool**: MAE=64.11, Bias=+64.03
**Contest**: Winner=180.8382978723404, Top1%=149.8968085106383

### Predictive Diagnostics

**Hitter Missing Predictors** (correlated with error but not in model):
- `hard_hit_pct` r=+0.083 (n=1404)
- `barrel_pct` r=+0.079 (n=1404)

**Pitcher Missing Predictors** (correlated with error but not in model):
- `g` r=-0.130 (n=153)
- `k9` r=+0.129 (n=153)
- `ip` r=-0.106 (n=153)
- `avg` r=-0.097 (n=153)
- `w` r=-0.092 (n=153)

**Optimal Context Weights**: Vegas=80% Park=5% Weather=15% (saves 0.00 MAE)

**Archetype Biases:**
- Speed (SB pace>15): over-projected by 0.8 pts (n=398)

**Recommendations:**
- PROJECTION: increase SP_CALIBRATION by ~0.10 (current 0.90)
- PROJECTION: increase context multiplier weights — hitter bias is -0.57 pts
- POOL: Best performing stack config is 4-4 — increase its weight in STACK_CONFIGS
- POOL: Projections have 30.8 pt spread — use projection rank as primary sort for portfolio selection
- CONTEST: Avg Top 1% threshold is 149.9 pts across 47 contests
- CONTEST: Avg cash line is 111.6 pts — pool floor should exceed this
- CONTEST: Avg winner scores 180.8 pts — need high-ceiling correlated stacks
- TRACKING: Run this analysis daily to build sample size — patterns stabilize after 2+ weeks

## Sim Validation — 2026-04-26

### Distribution Calibration
- **Hitters**: 89.7% in P10-P90 [WARN] (below floor=1.5%, above ceiling=8.8%)
- **Pitchers**: 70.0% in P10-P90 [WARN] (below floor=10.0%, above ceiling=20.0%)

### Projection Accuracy
- Overall: MAE=6.2, Bias=+1.24, r=0.372
- Hitters: MAE=5.91 [PASS]
- Pitchers: MAE=8.73 [WARN]

### Pitcher Components
- IP: MAE=0.88, Bias=-0.11
- Ks: MAE=1.76, Bias=-0.16
- ER: MAE=1.61, Bias=+0.18

### Multiplier Effectiveness

## Slate Review — 2026-04-26 / main

- **Pool**: 700 lineups, avg actual=87.3, cash line=87.1, GPP line=141.8, best=169.4
- **Proj accuracy**: r=0.195, MAE=22.7, bias=+15.0
- **Overlap**: 1/20 top-by-proj were actual winners
- **Best strategy**: Salary (highest salary)
- **Top stack**: DET (avg actual=111.0, 6.9% exposure)
- **Biggest bust**: Kyle Bradish (proj=16.2, actual=6.4, 14% exp)
- **Biggest missed opp**: Kyle Harrison (actual=40.3, 13.6% exp)


## Ownership Calibration — 43 large-field contests (≥1000 entries)

- **Matched players**: 610
- **Bias**: +4.88% (positive = over-project ownership)
- **MAE**: 5.46%
- **Correlation**: r=0.706

- Chalk (>20% actual): n=31, bias=+1.62%
- Mid (5-20% actual): n=195, bias=+5.60%
- Low (<5% actual): n=384, bias=+4.78%

**Over-projected ownership:**
- Mason Fluharty: proj=71.4% actual=0.6%
- Logan Gilbert: proj=45.5% actual=0.0%
- Jesus Luzardo: proj=51.4% actual=16.7%
- Ryan Weiss: proj=33.8% actual=0.0%
- Zack Wheeler: proj=39.8% actual=7.4%

**Under-projected ownership:**
- Cody Ponce: proj=22.8% actual=47.3%
- Dustin May: proj=13.6% actual=31.5%
- David Peterson: proj=13.1% actual=24.7%
- Dylan Cease: proj=33.6% actual=44.1%
- Chad Patrick: proj=11.4% actual=20.5%

## Leverage Analysis — 2026-04-26 (30 contests, 3387 players)

**Dataset**: 3071 hitters, 316 pitchers across 23 dates
**Leverage hits**: 372 (11.0%) | **Chalk traps**: 72 (2.1%) | **Ceiling hits**: 318 (9.4%)

### Hitter Predictors (correlation with outperformance)
- `wRC+` r=+0.173 (n=3068)
- `ISO` r=+0.161 (n=3070)
- `xwOBA` r=+0.118 (n=3070)
- `Salary` r=+0.093 (n=3071)
- `Barrel%` r=+0.040 (n=3070)

### Pitcher Predictors
- `K%` r=+0.335 (n=316)
- `Salary` r=+0.296 (n=316)
- `xFIP` r=-0.298 (n=316)
- `Win Prob` r=+0.151 (n=316)
- `Stuff+` r=+0.148 (n=315)

### Actionable Rules
- **Hitter leverage**: ISO > 0.200 AND own < 10% -> 27% leverage rate (+17pp vs base)
- **Hitter leverage**: wRC+ > 120 AND own < 10% -> 22% leverage rate (+12pp vs base)
- **Hitter trap**: Own > 20% AND K% > 0.28 -> 68% chalk trap rate
- **Pitcher leverage**: K% > 0.25 AND own < 15% -> 33% leverage rate (+17pp vs base)
- **Pitcher leverage**: Stuff+ > 105 AND own < 15% -> 31% leverage rate (+14pp vs base)
- **Pitcher trap**: Own > 25% AND Stuff+ < 100 -> 59% chalk trap rate
- **Pitcher trap**: Own > 25% AND xFIP > 4.0 -> 56% chalk trap rate

## Winner Pattern Analysis — 2026-04-26 (47 contests, 616,213 entries)
- Top 1% profile: 141% total own, 3.9 booms, 0.7 busts, 48 pitcher pts
- Target: ownership 100-150%, 3+ booms, <1 bust, 25+ pitcher pts

## Research Findings — 2026-04-22, 2026-04-23, 2026-04-24, 2026-04-25, 2026-04-26, 2026-04-27, 2026-04-28, 2026-04-29

**Projection**: MAE=6.26, Bias=+0.59, Hitter MAE=5.95, Pitcher MAE=8.99
**Pool**: MAE=57.94, Bias=+57.72
**Contest**: Winner=180.4375, Top1%=150.140625

### Predictive Diagnostics

**Hitter Missing Predictors** (correlated with error but not in model):
- `barrel_pct` r=+0.096 (n=1134)
- `hard_hit_pct` r=+0.094 (n=1134)

**Pitcher Missing Predictors** (correlated with error but not in model):
- `g` r=-0.134 (n=123)
- `k9` r=+0.118 (n=123)
- `ip` r=-0.118 (n=123)
- `k_bb_pct` r=+0.099 (n=123)
- `gb_pct` r=+0.091 (n=123)

**Opposing Lineup Factors** (for pitcher projections):
- `opp_barrel_pct` r=-0.103 (n=133)

**Optimal Context Weights**: Vegas=80% Park=15% Weather=5% (saves 0.00 MAE)

**Archetype Biases:**
- Speed (SB pace>15): over-projected by 1.0 pts (n=322)

**Recommendations:**
- PROJECTION: increase context multiplier weights — hitter bias is -0.50 pts
- POOL: Best performing stack config is 5-0 — increase its weight in STACK_CONFIGS
- POOL: Projections have 31.9 pt spread — use projection rank as primary sort for portfolio selection
- CONTEST: Avg Top 1% threshold is 150.1 pts across 48 contests
- CONTEST: Avg cash line is 111.6 pts — pool floor should exceed this
- CONTEST: Avg winner scores 180.4 pts — need high-ceiling correlated stacks
- TRACKING: Run this analysis daily to build sample size — patterns stabilize after 2+ weeks

## Sim Validation — 2026-04-26

### Distribution Calibration
- **Hitters**: 89.7% in P10-P90 [WARN] (below floor=1.5%, above ceiling=8.8%)
- **Pitchers**: 70.0% in P10-P90 [WARN] (below floor=10.0%, above ceiling=20.0%)

### Projection Accuracy
- Overall: MAE=6.2, Bias=+1.24, r=0.372
- Hitters: MAE=5.91 [PASS]
- Pitchers: MAE=8.73 [WARN]

### Pitcher Components
- IP: MAE=0.88, Bias=-0.11
- Ks: MAE=1.76, Bias=-0.16
- ER: MAE=1.61, Bias=+0.18

### Multiplier Effectiveness

## Slate Review — 2026-04-26 / main

- **Pool**: 700 lineups, avg actual=87.3, cash line=87.1, GPP line=141.8, best=169.4
- **Proj accuracy**: r=0.195, MAE=22.7, bias=+15.0
- **Overlap**: 1/20 top-by-proj were actual winners
- **Best strategy**: Salary (highest salary)
- **Top stack**: DET (avg actual=111.0, 6.9% exposure)
- **Biggest bust**: Kyle Bradish (proj=16.2, actual=6.4, 14% exp)
- **Biggest missed opp**: Kyle Harrison (actual=40.3, 13.6% exp)


## Ownership Calibration — 43 large-field contests (≥1000 entries)

- **Matched players**: 610
- **Bias**: +4.82% (positive = over-project ownership)
- **MAE**: 5.39%
- **Correlation**: r=0.713

- Chalk (>20% actual): n=32, bias=+1.27%
- Mid (5-20% actual): n=195, bias=+5.58%
- Low (<5% actual): n=383, bias=+4.73%

**Over-projected ownership:**
- Mason Fluharty: proj=71.4% actual=0.6%
- Logan Gilbert: proj=42.6% actual=0.0%
- Ryan Weiss: proj=33.8% actual=0.0%
- Zack Wheeler: proj=39.8% actual=7.4%
- Jesus Luzardo: proj=48.6% actual=16.7%

**Under-projected ownership:**
- Cody Ponce: proj=22.8% actual=47.3%
- Dustin May: proj=13.0% actual=31.5%
- David Peterson: proj=13.1% actual=24.7%
- Dylan Cease: proj=33.8% actual=44.1%
- Chad Patrick: proj=11.0% actual=20.5%

## Leverage Analysis — 2026-04-29 (30 contests, 3004 players)

**Dataset**: 2722 hitters, 282 pitchers across 23 dates
**Leverage hits**: 340 (11.3%) | **Chalk traps**: 76 (2.5%) | **Ceiling hits**: 268 (8.9%)

### Hitter Predictors (correlation with outperformance)
- `wRC+` r=+0.211 (n=2722)
- `ISO` r=+0.186 (n=2722)
- `xwOBA` r=+0.131 (n=2722)
- `Barrel%` r=+0.083 (n=2722)
- `Salary` r=+0.080 (n=2722)

### Pitcher Predictors
- `K%` r=+0.311 (n=282)
- `Salary` r=+0.227 (n=282)
- `xFIP` r=-0.359 (n=282)
- `Win Prob` r=+0.138 (n=282)
- `Stuff+` r=+0.179 (n=282)

### Actionable Rules
- **Hitter leverage**: ISO > 0.200 AND own < 10% -> 27% leverage rate (+17pp vs base)
- **Hitter leverage**: wRC+ > 120 AND own < 10% -> 22% leverage rate (+12pp vs base)
- **Hitter trap**: Own > 20% AND K% > 0.28 -> 68% chalk trap rate
- **Pitcher leverage**: K% > 0.25 AND own < 15% -> 33% leverage rate (+17pp vs base)
- **Pitcher leverage**: Stuff+ > 105 AND own < 15% -> 31% leverage rate (+14pp vs base)
- **Pitcher trap**: Own > 25% AND Stuff+ < 100 -> 59% chalk trap rate
- **Pitcher trap**: Own > 25% AND xFIP > 4.0 -> 56% chalk trap rate

## Winner Pattern Analysis — 2026-04-29 (48 contests, 616,390 entries)
- Top 1% profile: 141% total own, 3.9 booms, 0.7 busts, 48 pitcher pts
- Target: ownership 100-150%, 3+ booms, <1 bust, 25+ pitcher pts

## Slate Review — 2026-04-26 / main

- **Pool**: 700 lineups, avg actual=87.3, cash line=87.1, GPP line=141.8, best=169.4
- **Proj accuracy**: r=0.195, MAE=22.7, bias=+15.0
- **Overlap**: 1/20 top-by-proj were actual winners
- **Best strategy**: Salary (highest salary)
- **Top stack**: DET (avg actual=111.0, 6.9% exposure)
- **Biggest bust**: Kyle Bradish (proj=16.2, actual=6.4, 14% exp)
- **Biggest missed opp**: Kyle Harrison (actual=40.3, 13.6% exp)

## Research Findings — 2026-04-23, 2026-04-24, 2026-04-25, 2026-04-26, 2026-04-27, 2026-04-28, 2026-04-29, 2026-04-30

**Projection**: MAE=6.18, Bias=+0.73, Hitter MAE=5.90, Pitcher MAE=8.74
**Pool**: MAE=71.73, Bias=+71.71
**Contest**: Winner=181.18571428571428, Top1%=150.55714285714285

### Predictive Diagnostics

**Hitter Missing Predictors** (correlated with error but not in model):
- `barrel_pct` r=+0.081 (n=1496)
- `hard_hit_pct` r=+0.071 (n=1496)
- `slg` r=-0.056 (n=1480)
- `ops` r=-0.054 (n=1480)
- `avg_ev` r=-0.051 (n=1496)

**Pitcher Missing Predictors** (correlated with error but not in model):
- `ip` r=-0.135 (n=166)
- `w` r=-0.126 (n=166)
- `gs` r=-0.116 (n=166)
- `k9` r=+0.102 (n=166)
- `k_bb_pct` r=+0.093 (n=166)

**Optimal Context Weights**: Vegas=80% Park=15% Weather=5% (saves 0.00 MAE)

**Archetype Biases:**
- Power (ISO>.200): over-projected by 1.2 pts (n=335)
- Speed (SB pace>15): over-projected by 0.8 pts (n=433)

**Recommendations:**
- PROJECTION: increase context multiplier weights — hitter bias is -0.74 pts
- POOL: Best performing stack config is 4-4 — increase its weight in STACK_CONFIGS
- POOL: Projections have 14.0 pt spread — use projection rank as primary sort for portfolio selection
- CONTEST: Avg Top 1% threshold is 150.6 pts across 49 contests
- CONTEST: Avg cash line is 111.7 pts — pool floor should exceed this
- CONTEST: Avg winner scores 181.2 pts — need high-ceiling correlated stacks
- TRACKING: Run this analysis daily to build sample size — patterns stabilize after 2+ weeks

## Sim Validation — 2026-04-29

### Distribution Calibration
- **Hitters**: 88.4% in P10-P90 [WARN] (below floor=3.0%, above ceiling=8.6%)
- **Pitchers**: 65.4% in P10-P90 [WARN] (below floor=15.4%, above ceiling=19.2%)

### Projection Accuracy
- Overall: MAE=5.86, Bias=+0.95, r=0.346
- Hitters: MAE=5.62 [PASS]
- Pitchers: MAE=8.04 [WARN]

### Pitcher Components
- IP: MAE=1.08, Bias=-0.07
- Ks: MAE=1.72, Bias=-0.19
- ER: MAE=1.2, Bias=+0.36

### Multiplier Effectiveness

## Slate Review — 2026-04-29 / early

- **Pool**: 4500 lineups, avg actual=86.0, cash line=86.0, GPP line=136.1, best=162.7
- **Proj accuracy**: r=0.201, MAE=18.8, bias=+10.2
- **Overlap**: 0/20 top-by-proj were actual winners
- **Best strategy**: Salary (highest salary)
- **Top stack**: TOR (avg actual=96.0, 15.4% exposure)
- **Biggest bust**: Yusei Kikuchi (proj=15.6, actual=4.7, 20% exp)
- **Biggest missed opp**: Brandon Valenzuela (actual=23.0, 8.6% exp)

## Slate Review — 2026-04-29 / main

- **Pool**: 6500 lineups, avg actual=98.3, cash line=95.6, GPP line=171.8, best=221.0
- **Proj accuracy**: r=0.099, MAE=24.6, bias=-3.2
- **Overlap**: 0/20 top-by-proj were actual winners
- **Best strategy**: Salary (highest salary)
- **Top stack**: COL (avg actual=133.9, 13.9% exposure)
- **Biggest bust**: Elly De La Cruz (proj=10.4, actual=2.0, 20% exp)
- **Biggest missed opp**: Curtis Mead (actual=35.0, 5.2% exp)


## Ownership Calibration — 44 large-field contests (≥1000 entries)

- **Matched players**: 520
- **Bias**: +4.72% (positive = over-project ownership)
- **MAE**: 5.32%
- **Correlation**: r=0.722

- Chalk (>20% actual): n=29, bias=+0.74%
- Mid (5-20% actual): n=160, bias=+5.44%
- Low (<5% actual): n=331, bias=+4.72%

**Over-projected ownership:**
- Mason Fluharty: proj=71.4% actual=0.6%
- Logan Gilbert: proj=42.6% actual=0.0%
- Shota Imanaga: proj=37.2% actual=10.3%
- Yoshinobu Yamamoto: proj=41.9% actual=15.2%
- Noah Cameron: proj=29.8% actual=5.9%

**Under-projected ownership:**
- Cody Ponce: proj=22.8% actual=47.3%
- Dustin May: proj=13.0% actual=31.5%
- David Peterson: proj=14.2% actual=29.7%
- Dylan Cease: proj=33.8% actual=44.1%
- Chad Patrick: proj=11.0% actual=20.5%

## Leverage Analysis — 2026-04-30 (31 contests, 1588 players)

**Dataset**: 1435 hitters, 153 pitchers across 24 dates
**Leverage hits**: 149 (9.4%) | **Chalk traps**: 32 (2.0%) | **Ceiling hits**: 151 (9.5%)

### Hitter Predictors (correlation with outperformance)
- `wRC+` r=+0.186 (n=1434)
- `xwOBA` r=+0.136 (n=1434)
- `ISO` r=+0.131 (n=1434)
- `Salary` r=+0.126 (n=1435)
- `Barrel%` r=+0.117 (n=1434)

### Pitcher Predictors
- `K%` r=+0.424 (n=153)
- `Salary` r=+0.348 (n=153)
- `xFIP` r=-0.393 (n=153)
- `Win Prob` r=+0.156 (n=153)
- `Stuff+` r=+0.208 (n=153)

### Actionable Rules
- **Hitter leverage**: ISO > 0.200 AND own < 10% -> 27% leverage rate (+17pp vs base)
- **Hitter leverage**: wRC+ > 120 AND own < 10% -> 22% leverage rate (+12pp vs base)
- **Hitter trap**: Own > 20% AND K% > 0.28 -> 68% chalk trap rate
- **Pitcher leverage**: K% > 0.25 AND own < 15% -> 33% leverage rate (+17pp vs base)
- **Pitcher leverage**: Stuff+ > 105 AND own < 15% -> 31% leverage rate (+14pp vs base)
- **Pitcher trap**: Own > 25% AND Stuff+ < 100 -> 59% chalk trap rate
- **Pitcher trap**: Own > 25% AND xFIP > 4.0 -> 56% chalk trap rate

## Winner Pattern Analysis — 2026-04-30 (49 contests, 623,729 entries)
- Top 1% profile: 141% total own, 3.9 booms, 0.7 busts, 48 pitcher pts
- Target: ownership 100-150%, 3+ booms, <1 bust, 25+ pitcher pts

## Slate Review — 2026-04-29 / early

- **Pool**: 4500 lineups, avg actual=86.0, cash line=86.0, GPP line=136.1, best=162.7
- **Proj accuracy**: r=0.201, MAE=18.8, bias=+10.2
- **Overlap**: 0/20 top-by-proj were actual winners
- **Best strategy**: Salary (highest salary)
- **Top stack**: TOR (avg actual=96.0, 15.4% exposure)
- **Biggest bust**: Yusei Kikuchi (proj=15.6, actual=4.7, 20% exp)
- **Biggest missed opp**: Brandon Valenzuela (actual=23.0, 8.6% exp)

## Slate Review — 2026-04-29 / main

- **Pool**: 6500 lineups, avg actual=98.3, cash line=95.6, GPP line=171.8, best=221.0
- **Proj accuracy**: r=0.099, MAE=24.6, bias=-3.2
- **Overlap**: 0/20 top-by-proj were actual winners
- **Best strategy**: Salary (highest salary)
- **Top stack**: COL (avg actual=133.9, 13.9% exposure)
- **Biggest bust**: Elly De La Cruz (proj=10.4, actual=2.0, 20% exp)
- **Biggest missed opp**: Curtis Mead (actual=35.0, 5.2% exp)

## Research Findings — 2026-04-23, 2026-04-24, 2026-04-25, 2026-04-26, 2026-04-27, 2026-04-28, 2026-04-29, 2026-04-30

**Projection**: MAE=6.13, Bias=+0.70, Hitter MAE=5.87, Pitcher MAE=8.55
**Pool**: MAE=79.51, Bias=+79.50
**Contest**: Winner=181.0200002, Top1%=150.531

### Predictive Diagnostics

**Hitter Missing Predictors** (correlated with error but not in model):
- `barrel_pct` r=+0.075 (n=1674)
- `hard_hit_pct` r=+0.066 (n=1674)
- `slg` r=-0.056 (n=1658)
- `ops` r=-0.051 (n=1658)

**Pitcher Missing Predictors** (correlated with error but not in model):
- `gb_pct` r=+0.109 (n=183)
- `ip` r=-0.104 (n=183)
- `k9` r=+0.094 (n=183)
- `gs` r=-0.092 (n=183)
- `w` r=-0.087 (n=183)

**Optimal Context Weights**: Vegas=80% Park=15% Weather=5% (saves 0.00 MAE)

**Archetype Biases:**
- Power (ISO>.200): over-projected by 1.1 pts (n=372)
- Speed (SB pace>15): over-projected by 0.8 pts (n=479)

**Recommendations:**
- PROJECTION: increase context multiplier weights — hitter bias is -0.70 pts
- POOL: Best performing stack config is 4-4 — increase its weight in STACK_CONFIGS
- POOL: Projections have 16.4 pt spread — use projection rank as primary sort for portfolio selection
- CONTEST: Avg Top 1% threshold is 150.5 pts across 50 contests
- CONTEST: Avg cash line is 111.6 pts — pool floor should exceed this
- CONTEST: Avg winner scores 181.0 pts — need high-ceiling correlated stacks
- TRACKING: Run this analysis daily to build sample size — patterns stabilize after 2+ weeks

## Sim Validation — 2026-04-30

### Distribution Calibration
- **Hitters**: 88.6% in P10-P90 [WARN] (below floor=0.6%, above ceiling=10.9%)
- **Pitchers**: 83.3% in P10-P90 [PASS] (below floor=11.1%, above ceiling=5.6%)

### Projection Accuracy
- Overall: MAE=5.75, Bias=+0.63, r=0.254
- Hitters: MAE=5.65 [PASS]
- Pitchers: MAE=6.76 [PASS]

### Pitcher Components
- IP: MAE=1.03, Bias=+0.26
- Ks: MAE=1.73, Bias=+0.30
- ER: MAE=1.42, Bias=-0.13

### Multiplier Effectiveness

## Slate Review — 2026-04-30 / main

- **Pool**: 500 lineups, avg actual=81.7, cash line=79.1, GPP line=147.2, best=168.1
- **Proj accuracy**: r=0.075, MAE=26.1, bias=+16.4
- **Overlap**: 3/20 top-by-proj were actual winners
- **Best strategy**: Salary (highest salary)
- **Top stack**: MIL (avg actual=123.2, 6.2% exposure)
- **Biggest bust**: Michael Soroka (proj=15.5, actual=-12.4, 25% exp)
- **Biggest missed opp**: William Contreras (actual=37.0, 7.4% exp)


## Ownership Calibration — 45 large-field contests (≥1000 entries)

- **Matched players**: 379
- **Bias**: +4.91% (positive = over-project ownership)
- **MAE**: 5.70%
- **Correlation**: r=0.660

- Chalk (>20% actual): n=19, bias=-0.63%
- Mid (5-20% actual): n=118, bias=+5.98%
- Low (<5% actual): n=242, bias=+4.82%

**Over-projected ownership:**
- Mason Fluharty: proj=71.4% actual=0.6%
- Ryan Weiss: proj=33.8% actual=0.0%
- Zack Wheeler: proj=39.8% actual=7.4%
- Jesus Luzardo: proj=48.6% actual=16.7%
- Cody Bolton: proj=25.6% actual=0.0%

**Under-projected ownership:**
- Cody Ponce: proj=22.8% actual=47.3%
- Dustin May: proj=13.0% actual=31.5%
- David Peterson: proj=14.2% actual=29.7%
- Dylan Cease: proj=33.8% actual=44.1%
- Chad Patrick: proj=11.0% actual=20.5%

## Leverage Analysis — 2026-04-30 (32 contests, 2316 players)

**Dataset**: 2095 hitters, 221 pitchers across 25 dates
**Leverage hits**: 232 (10.0%) | **Chalk traps**: 55 (2.4%) | **Ceiling hits**: 241 (10.4%)

### Hitter Predictors (correlation with outperformance)
- `wRC+` r=+0.219 (n=2095)
- `ISO` r=+0.181 (n=2095)
- `xwOBA` r=+0.151 (n=2095)
- `Salary` r=+0.124 (n=2095)
- `Barrel%` r=+0.103 (n=2095)

### Pitcher Predictors
- `K%` r=+0.356 (n=221)
- `Salary` r=+0.306 (n=221)
- `xFIP` r=-0.273 (n=221)
- `Win Prob` r=+0.189 (n=221)
- `Stuff+` r=+0.154 (n=221)

### Actionable Rules
- **Hitter leverage**: ISO > 0.200 AND own < 10% -> 27% leverage rate (+17pp vs base)
- **Hitter leverage**: wRC+ > 120 AND own < 10% -> 22% leverage rate (+12pp vs base)
- **Hitter trap**: Own > 20% AND K% > 0.28 -> 68% chalk trap rate
- **Pitcher leverage**: K% > 0.25 AND own < 15% -> 33% leverage rate (+17pp vs base)
- **Pitcher leverage**: Stuff+ > 105 AND own < 15% -> 31% leverage rate (+14pp vs base)
- **Pitcher trap**: Own > 25% AND Stuff+ < 100 -> 59% chalk trap rate
- **Pitcher trap**: Own > 25% AND xFIP > 4.0 -> 56% chalk trap rate

## Winner Pattern Analysis — 2026-04-30 (50 contests, 633,219 entries)
- Top 1% profile: 141% total own, 3.9 booms, 0.7 busts, 48 pitcher pts
- Target: ownership 100-150%, 3+ booms, <1 bust, 25+ pitcher pts

## Research Findings — 2026-04-26, 2026-04-27, 2026-04-28, 2026-04-29, 2026-04-30, 2026-05-01, 2026-05-02, 2026-05-03

**Projection**: MAE=5.83, Bias=+1.02, Hitter MAE=5.60, Pitcher MAE=7.99
**Pool**: MAE=72.68, Bias=+72.67
**Contest**: Winner=181.4215688235294, Top1%=150.76568627450982

### Predictive Diagnostics

**Hitter Missing Predictors** (correlated with error but not in model):
- `swstr_pct` r=+0.060 (n=1773)
- `ops` r=-0.056 (n=1773)
- `babip` r=-0.054 (n=1773)
- `slg` r=-0.052 (n=1773)

**Pitcher Missing Predictors** (correlated with error but not in model):
- `gb_pct` r=+0.146 (n=191)
- `l` r=-0.117 (n=191)
- `gs` r=-0.096 (n=191)
- `ip` r=-0.092 (n=191)

**Optimal Context Weights**: Vegas=80% Park=15% Weather=5% (saves 0.00 MAE)

**Archetype Biases:**
- Power (ISO>.200): over-projected by 1.4 pts (n=398)
- Speed (SB pace>15): over-projected by 1.5 pts (n=508)

**Recommendations:**
- PROJECTION: increase context multiplier weights — hitter bias is -1.12 pts
- POOL: Best performing stack config is 4-0 — increase its weight in STACK_CONFIGS
- POOL: Projections have 11.7 pt spread — use projection rank as primary sort for portfolio selection
- CONTEST: Avg Top 1% threshold is 150.8 pts across 51 contests
- CONTEST: Avg cash line is 111.8 pts — pool floor should exceed this
- CONTEST: Avg winner scores 181.4 pts — need high-ceiling correlated stacks
- TRACKING: Run this analysis daily to build sample size — patterns stabilize after 2+ weeks

## Sim Validation — 2026-05-03

### Distribution Calibration
- **Hitters**: 93.1% in P10-P90 [FAIL] (below floor=1.5%, above ceiling=5.3%)
- **Pitchers**: 89.7% in P10-P90 [WARN] (below floor=6.9%, above ceiling=3.4%)

### Projection Accuracy
- Overall: MAE=5.45, Bias=+1.38, r=0.376
- Hitters: MAE=5.3 [PASS]
- Pitchers: MAE=6.81 [PASS]

### Pitcher Components
- IP: MAE=1.23, Bias=-0.06
- Ks: MAE=1.69, Bias=-0.20
- ER: MAE=1.49, Bias=+0.42

### Multiplier Effectiveness

## Slate Review — 2026-05-03 / main

- **Pool**: 25000 lineups, avg actual=94.5, cash line=93.7, GPP line=146.9, best=186.1
- **Proj accuracy**: r=0.451, MAE=17.9, bias=+7.4
- **Overlap**: 0/20 top-by-proj were actual winners
- **Best strategy**: Salary (highest salary)
- **Top stack**: NYY (avg actual=114.9, 4.9% exposure)
- **Biggest bust**: Chris Paddack (proj=12.7, actual=-11.4, 5% exp)
- **Biggest missed opp**: Jasson Domínguez (actual=30.0, 0.5% exp)


## Ownership Calibration — 46 large-field contests (≥1000 entries)

- **Matched players**: 615
- **Bias**: +4.93% (positive = over-project ownership)
- **MAE**: 5.43%
- **Correlation**: r=0.731

- Chalk (>20% actual): n=31, bias=+2.92%
- Mid (5-20% actual): n=197, bias=+5.40%
- Low (<5% actual): n=387, bias=+4.85%

**Over-projected ownership:**
- Mason Fluharty: proj=71.4% actual=0.6%
- Logan Gilbert: proj=42.6% actual=0.0%
- Ryan Weiss: proj=33.8% actual=0.1%
- Yoshinobu Yamamoto: proj=41.9% actual=15.2%
- Shota Imanaga: proj=36.6% actual=10.3%

**Under-projected ownership:**
- Cody Ponce: proj=22.8% actual=47.3%
- David Peterson: proj=14.2% actual=29.7%
- Dylan Cease: proj=34.5% actual=44.1%
- Cristian Javier: proj=1.4% actual=9.4%
- Connor Prielipp: proj=14.9% actual=21.5%

## Leverage Analysis — 2026-05-03 (33 contests, 2775 players)

**Dataset**: 2514 hitters, 261 pitchers across 26 dates
**Leverage hits**: 331 (11.9%) | **Chalk traps**: 71 (2.6%) | **Ceiling hits**: 300 (10.8%)

### Hitter Predictors (correlation with outperformance)
- `wRC+` r=+0.189 (n=2514)
- `ISO` r=+0.153 (n=2514)
- `xwOBA` r=+0.134 (n=2514)
- `Salary` r=+0.114 (n=2514)
- `Barrel%` r=+0.072 (n=2514)

### Pitcher Predictors
- `K%` r=+0.351 (n=261)
- `Salary` r=+0.260 (n=261)
- `xFIP` r=-0.284 (n=261)
- `Win Prob` r=+0.251 (n=261)
- `Stuff+` r=+0.197 (n=261)

### Actionable Rules
- **Hitter leverage**: ISO > 0.200 AND own < 10% -> 27% leverage rate (+17pp vs base)
- **Hitter leverage**: wRC+ > 120 AND own < 10% -> 22% leverage rate (+12pp vs base)
- **Hitter trap**: Own > 20% AND K% > 0.28 -> 68% chalk trap rate
- **Pitcher leverage**: K% > 0.25 AND own < 15% -> 33% leverage rate (+17pp vs base)
- **Pitcher leverage**: Stuff+ > 105 AND own < 15% -> 31% leverage rate (+14pp vs base)
- **Pitcher trap**: Own > 25% AND Stuff+ < 100 -> 59% chalk trap rate
- **Pitcher trap**: Own > 25% AND xFIP > 4.0 -> 56% chalk trap rate

## Winner Pattern Analysis — 2026-05-03 (51 contests, 646,280 entries)
- Top 1% profile: 142% total own, 3.9 booms, 0.7 busts, 48 pitcher pts
- Target: ownership 100-150%, 3+ booms, <1 bust, 25+ pitcher pts


## Ownership Calibration — 4 large-field contests (≥1000 entries)

- **Matched players**: 347
- **Bias**: +5.70% (positive = over-project ownership)
- **MAE**: 6.72%
- **Correlation**: r=0.580

- Chalk (>20% actual): n=12, bias=-2.13%
- Mid (5-20% actual): n=84, bias=+4.97%
- Low (<5% actual): n=251, bias=+6.31%

**Over-projected ownership:**
- Logan Gilbert: proj=43.0% actual=0.0%
- Brandon Woodruff: proj=34.2% actual=0.0%
- Zack Wheeler: proj=32.6% actual=0.0%
- Framber Valdez: proj=33.6% actual=6.0%
- Nick Pivetta: proj=36.7% actual=9.4%

**Under-projected ownership:**
- Sandy Alcantara: proj=14.2% actual=37.9%
- Eury Perez: proj=20.8% actual=40.9%
- Pete Crow-Armstrong: proj=9.7% actual=23.6%
- Dylan Cease: proj=34.5% actual=47.3%
- Michael Busch: proj=9.2% actual=21.0%


## Ownership Calibration — 4 large-field contests (≥1000 entries)

- **Matched players**: 321 (26 0%-actual ghosts excluded)
- **Bias**: +5.15% (positive = over-project ownership)
- **MAE**: 6.26%
- **Correlation**: r=0.648

- Chalk (>20% actual): n=12, bias=-2.13%, MAE=12.07%
- Mid (5-20% actual): n=84, bias=+4.97%, MAE=7.10%
- Low (<5% actual): n=225, bias=+5.60%, MAE=5.63%

**Over-projected:**
- Framber Valdez: proj=33.6% actual=6.0%
- Nick Pivetta: proj=36.7% actual=9.4%
- Shota Imanaga: proj=36.6% actual=9.4%
- Tatsuya Imai: proj=41.4% actual=14.4%
- Nolan McLean: proj=35.0% actual=9.7%

**Under-projected:**
- Sandy Alcantara: proj=14.2% actual=37.9%
- Eury Perez: proj=20.8% actual=40.9%
- Pete Crow-Armstrong: proj=9.7% actual=23.6%
- Dylan Cease: proj=34.5% actual=47.3%
- Michael Busch: proj=9.2% actual=21.0%


## Ownership Calibration — 4 large-field contests (≥1000 entries)

- **Matched players**: 321 (26 0%-actual ghosts excluded)
- **Bias**: +5.31% (positive = over-project ownership)
- **MAE**: 6.34%
- **Correlation**: r=0.653

- Chalk (>20% actual): n=12, bias=-2.18%, MAE=12.06%
- Mid (5-20% actual): n=84, bias=+5.24%, MAE=7.08%
- Low (<5% actual): n=225, bias=+5.74%, MAE=5.76%

**Over-projected:**
- Framber Valdez: proj=33.6% actual=6.0%
- Nick Pivetta: proj=36.7% actual=9.4%
- Shota Imanaga: proj=36.6% actual=9.4%
- Tatsuya Imai: proj=41.4% actual=14.4%
- Nolan McLean: proj=35.0% actual=9.7%

**Under-projected:**
- Sandy Alcantara: proj=14.2% actual=37.9%
- Eury Perez: proj=21.8% actual=40.9%
- Pete Crow-Armstrong: proj=9.4% actual=23.6%
- Dylan Cease: proj=34.0% actual=47.3%
- Michael Busch: proj=8.8% actual=21.0%

## Slate Review — 2026-03-30 / main

- **Pool**: 3000 lineups, avg actual=86.7, cash line=84.4, GPP line=157.3, best=202.4
- **Proj accuracy**: r=0.030, MAE=21.3, bias=-1.0
- **Overlap**: 0/20 top-by-proj were actual winners
- **Best strategy**: Salary (highest salary)
- **Top stack**: COL (avg actual=127.2, 4.8% exposure)
- **Biggest bust**: Jacob Lopez (proj=13.1, actual=-3.0, 13% exp)
- **Biggest missed opp**: Jose Altuve (actual=40.0, 4.2% exp)
- **Ownership accuracy**: r=0.379, MAE=4.64%, bias=-0.26% (n=235)

## Research Findings — 2026-04-28, 2026-04-29, 2026-04-30, 2026-05-01, 2026-05-02, 2026-05-03, 2026-05-04, 2026-05-05

**Projection**: MAE=5.74, Bias=+0.98, Hitter MAE=5.51, Pitcher MAE=7.86
**Pool**: MAE=65.45, Bias=+65.28
**Contest**: Winner=181.7951923076923, Top1%=151.02980769230768

### Predictive Diagnostics

**Hitter Missing Predictors** (correlated with error but not in model):
- `swstr_pct` r=+0.070 (n=1594)
- `ops` r=-0.053 (n=1594)
- `slg` r=-0.053 (n=1594)
- `o_swing_pct` r=+0.053 (n=1594)
- `cent_pct` r=+0.050 (n=1590)

**Pitcher Missing Predictors** (correlated with error but not in model):
- `l` r=-0.118 (n=174)
- `gs` r=-0.096 (n=174)
- `ip` r=-0.086 (n=174)

**Opposing Lineup Factors** (for pitcher projections):
- `opp_o_swing_pct` r=-0.107 (n=185)

**Optimal Context Weights**: Vegas=80% Park=5% Weather=15% (saves -0.00 MAE)

**Archetype Biases:**
- Power (ISO>.200): over-projected by 1.4 pts (n=361)
- Speed (SB pace>15): over-projected by 1.3 pts (n=456)

**Recommendations:**
- PROJECTION: increase context multiplier weights — hitter bias is -1.11 pts
- POOL: Best performing stack config is 4-0 — increase its weight in STACK_CONFIGS
- POOL: Projections have 15.0 pt spread — use projection rank as primary sort for portfolio selection
- CONTEST: Avg Top 1% threshold is 151.0 pts across 52 contests
- CONTEST: Avg cash line is 112.0 pts — pool floor should exceed this
- CONTEST: Avg winner scores 181.8 pts — need high-ceiling correlated stacks
- TRACKING: Run this analysis daily to build sample size — patterns stabilize after 2+ weeks

## Sim Validation — 2026-05-04

### Distribution Calibration
- **Hitters**: 90.6% in P10-P90 [WARN] (below floor=3.3%, above ceiling=6.1%)
- **Pitchers**: 70.8% in P10-P90 [WARN] (below floor=16.7%, above ceiling=12.5%)

### Projection Accuracy
- Overall: MAE=5.75, Bias=+1.27, r=0.326
- Hitters: MAE=5.43 [PASS]
- Pitchers: MAE=8.55 [WARN]

### Pitcher Components
- IP: MAE=1.0, Bias=-0.57
- Ks: MAE=2.0, Bias=-0.05
- ER: MAE=1.24, Bias=-0.05

### Multiplier Effectiveness

## Slate Review — 2026-05-04 / main

- **Pool**: 30000 lineups, avg actual=87.7, cash line=86.5, GPP line=148.7, best=185.6
- **Proj accuracy**: r=0.123, MAE=22.5, bias=+12.3
- **Overlap**: 0/20 top-by-proj were actual winners
- **Best strategy**: Salary (highest salary)
- **Top stack**: CWS (avg actual=121.7, 3.2% exposure)
- **Biggest bust**: José Soriano (proj=17.7, actual=2.4, 21% exp)
- **Biggest missed opp**: Davis Martin (actual=36.8, 8.6% exp)


## Ownership Calibration — 4 large-field contests (≥1000 entries)

- **Matched players**: 321 (26 0%-actual ghosts excluded)
- **Bias**: +5.31% (positive = over-project ownership)
- **MAE**: 6.34%
- **Correlation**: r=0.652

- Chalk (>20% actual): n=12, bias=-2.17%, MAE=12.05%
- Mid (5-20% actual): n=84, bias=+5.25%, MAE=7.09%
- Low (<5% actual): n=225, bias=+5.74%, MAE=5.76%

**Over-projected:**
- Framber Valdez: proj=33.6% actual=6.0%
- Nick Pivetta: proj=36.7% actual=9.4%
- Shota Imanaga: proj=36.6% actual=9.4%
- Tatsuya Imai: proj=41.4% actual=14.4%
- Nolan McLean: proj=35.0% actual=9.7%

**Under-projected:**
- Sandy Alcantara: proj=14.2% actual=37.9%
- Eury Perez: proj=21.8% actual=40.9%
- Pete Crow-Armstrong: proj=9.4% actual=23.6%
- Dylan Cease: proj=34.0% actual=47.3%
- Michael Busch: proj=8.8% actual=21.0%

## Leverage Analysis — 2026-05-05 (34 contests, 2675 players)

**Dataset**: 2427 hitters, 248 pitchers across 27 dates
**Leverage hits**: 307 (11.5%) | **Chalk traps**: 74 (2.8%) | **Ceiling hits**: 287 (10.7%)

### Hitter Predictors (correlation with outperformance)
- `wRC+` r=+0.192 (n=2424)
- `ISO` r=+0.168 (n=2424)
- `xwOBA` r=+0.129 (n=2424)
- `Salary` r=+0.103 (n=2427)
- `Barrel%` r=+0.063 (n=2424)

### Pitcher Predictors
- `K%` r=+0.300 (n=246)
- `Salary` r=+0.144 (n=248)
- `xFIP` r=-0.269 (n=246)
- `Win Prob` r=+0.131 (n=248)
- `Stuff+` r=+0.048 (n=246)

### Actionable Rules
- **Hitter leverage**: ISO > 0.200 AND own < 10% -> 27% leverage rate (+17pp vs base)
- **Hitter leverage**: wRC+ > 120 AND own < 10% -> 22% leverage rate (+12pp vs base)
- **Hitter trap**: Own > 20% AND K% > 0.28 -> 68% chalk trap rate
- **Pitcher leverage**: K% > 0.25 AND own < 15% -> 33% leverage rate (+17pp vs base)
- **Pitcher leverage**: Stuff+ > 105 AND own < 15% -> 31% leverage rate (+14pp vs base)
- **Pitcher trap**: Own > 25% AND Stuff+ < 100 -> 59% chalk trap rate
- **Pitcher trap**: Own > 25% AND xFIP > 4.0 -> 56% chalk trap rate

## Winner Pattern Analysis — 2026-05-05 (52 contests, 692,636 entries)
- Top 1% profile: 141% total own, 3.9 booms, 0.6 busts, 48 pitcher pts
- Target: ownership 100-150%, 3+ booms, <1 bust, 25+ pitcher pts

## Slate Review — 2026-05-04 / main

- **Pool**: 30000 lineups, avg actual=87.7, cash line=86.5, GPP line=148.7, best=185.6
- **Proj accuracy**: r=0.123, MAE=22.5, bias=+12.3
- **Overlap**: 0/20 top-by-proj were actual winners
- **Best strategy**: Salary (highest salary)
- **Top stack**: CWS (avg actual=121.7, 3.2% exposure)
- **Biggest bust**: José Soriano (proj=17.7, actual=2.4, 21% exp)
- **Biggest missed opp**: Davis Martin (actual=36.8, 8.6% exp)

## Research Findings — 2026-04-29, 2026-04-30, 2026-05-01, 2026-05-02, 2026-05-03, 2026-05-04, 2026-05-05, 2026-05-06

**Projection**: MAE=5.83, Bias=+1.05, Hitter MAE=5.60, Pitcher MAE=7.92
**Pool**: MAE=65.81, Bias=+65.74
**Contest**: Winner=181.71296296296296, Top1%=150.54907407407407

### Predictive Diagnostics

**Pitcher Missing Predictors** (correlated with error but not in model):
- `l` r=-0.122 (n=170)

**Opposing Lineup Factors** (for pitcher projections):
- `opp_o_swing_pct` r=-0.114 (n=182)

**Optimal Context Weights**: Vegas=80% Park=15% Weather=5% (saves -0.00 MAE)

**Archetype Biases:**
- Power (ISO>.200): over-projected by 1.3 pts (n=355)
- Strikeout (K%>28%): over-projected by 0.8 pts (n=238)
- Speed (SB pace>15): over-projected by 1.6 pts (n=453)

**Recommendations:**
- PROJECTION: increase context multiplier weights — hitter bias is -1.13 pts
- POOL: Best performing stack config is 5-0 — increase its weight in STACK_CONFIGS
- POOL: Projections have 19.6 pt spread — use projection rank as primary sort for portfolio selection
- CONTEST: Avg Top 1% threshold is 150.5 pts across 54 contests
- CONTEST: Avg cash line is 111.6 pts — pool floor should exceed this
- CONTEST: Avg winner scores 181.7 pts — need high-ceiling correlated stacks
- TRACKING: Run this analysis daily to build sample size — patterns stabilize after 2+ weeks

## Sim Validation — 2026-05-05

### Distribution Calibration
- **Hitters**: 85.3% in P10-P90 [WARN] (below floor=2.6%, above ceiling=12.1%)
- **Pitchers**: 73.1% in P10-P90 [WARN] (below floor=15.4%, above ceiling=11.5%)

### Projection Accuracy
- Overall: MAE=6.35, Bias=+0.92, r=0.271
- Hitters: MAE=6.11 [WARN]
- Pitchers: MAE=8.52 [WARN]

### Pitcher Components
- IP: MAE=0.99, Bias=-0.39
- Ks: MAE=1.72, Bias=-0.00
- ER: MAE=1.76, Bias=-0.45

### Multiplier Effectiveness

## Slate Review — 2026-05-05 / main

- **Pool**: 35000 lineups, avg actual=77.6, cash line=76.9, GPP line=126.0, best=163.0
- **Proj accuracy**: r=0.091, MAE=22.8, bias=+18.5
- **Overlap**: 0/20 top-by-proj were actual winners
- **Best strategy**: Salary (highest salary)
- **Top stack**: ARI (avg actual=95.5, 5.6% exposure)
- **Biggest bust**: Jacob deGrom (proj=17.9, actual=11.4, 38% exp)
- **Biggest missed opp**: Isaac Collins (actual=29.0, 3.0% exp)


## Ownership Calibration — 4 large-field contests (≥1000 entries)

- **Matched players**: 411 (30 0%-actual ghosts excluded)
- **Bias**: +5.73% (positive = over-project ownership)
- **MAE**: 6.55%
- **Correlation**: r=0.622

- Chalk (>20% actual): n=12, bias=-2.26%, MAE=12.08%
- Mid (5-20% actual): n=95, bias=+5.69%, MAE=7.37%
- Low (<5% actual): n=304, bias=+6.06%, MAE=6.08%

**Over-projected:**
- Jacob deGrom: proj=38.1% actual=0.2%
- Nathan Eovaldi: proj=33.2% actual=2.9%
- Nick Pivetta: proj=36.7% actual=9.4%
- Shota Imanaga: proj=36.6% actual=9.4%
- Tatsuya Imai: proj=41.4% actual=14.4%

**Under-projected:**
- Sandy Alcantara: proj=13.7% actual=37.9%
- Eury Perez: proj=21.8% actual=40.9%
- Pete Crow-Armstrong: proj=9.3% actual=23.6%
- Dylan Cease: proj=34.0% actual=47.3%
- Michael Busch: proj=8.7% actual=21.0%

## Leverage Analysis — 2026-05-06 (36 contests, 3710 players)

**Dataset**: 3364 hitters, 346 pitchers across 28 dates
**Leverage hits**: 385 (10.4%) | **Chalk traps**: 95 (2.6%) | **Ceiling hits**: 360 (9.7%)

### Hitter Predictors (correlation with outperformance)
- `wRC+` r=+0.197 (n=3364)
- `ISO` r=+0.162 (n=3364)
- `xwOBA` r=+0.137 (n=3364)
- `Barrel%` r=+0.069 (n=3364)
- `Salary` r=+0.067 (n=3364)

### Pitcher Predictors
- `K%` r=+0.314 (n=344)
- `Salary` r=+0.238 (n=346)
- `xFIP` r=-0.287 (n=344)
- `Win Prob` r=+0.121 (n=346)
- `Stuff+` r=+0.116 (n=342)

### Actionable Rules
- **Hitter leverage**: ISO > 0.200 AND own < 10% -> 27% leverage rate (+17pp vs base)
- **Hitter leverage**: wRC+ > 120 AND own < 10% -> 22% leverage rate (+12pp vs base)
- **Hitter trap**: Own > 20% AND K% > 0.28 -> 68% chalk trap rate
- **Pitcher leverage**: K% > 0.25 AND own < 15% -> 33% leverage rate (+17pp vs base)
- **Pitcher leverage**: Stuff+ > 105 AND own < 15% -> 31% leverage rate (+14pp vs base)
- **Pitcher trap**: Own > 25% AND Stuff+ < 100 -> 59% chalk trap rate
- **Pitcher trap**: Own > 25% AND xFIP > 4.0 -> 56% chalk trap rate

## Winner Pattern Analysis — 2026-05-06 (54 contests, 749,654 entries)
- Top 1% profile: 138% total own, 3.9 booms, 0.7 busts, 48 pitcher pts
- Target: ownership 100-150%, 3+ booms, <1 bust, 25+ pitcher pts

## Research Findings — 2026-04-30, 2026-05-01, 2026-05-02, 2026-05-03, 2026-05-04, 2026-05-05, 2026-05-06, 2026-05-07

**Projection**: MAE=5.84, Bias=+1.10, Hitter MAE=5.61, Pitcher MAE=7.97
**Pool**: MAE=70.49, Bias=+70.47
**Contest**: Winner=182.25272727272727, Top1%=150.85909072727273

### Predictive Diagnostics

**Pitcher Missing Predictors** (correlated with error but not in model):
- `l` r=-0.128 (n=172)

**Optimal Context Weights**: Vegas=80% Park=15% Weather=5% (saves 0.00 MAE)

**Archetype Biases:**
- Power (ISO>.200): over-projected by 1.3 pts (n=361)
- Contact (K%<15%): over-projected by 0.9 pts (n=348)
- Speed (SB pace>15): over-projected by 1.3 pts (n=451)

**Recommendations:**
- PROJECTION: increase context multiplier weights — hitter bias is -1.19 pts
- POOL: Best performing stack config is 5-0 — increase its weight in STACK_CONFIGS
- POOL: Projections have 35.9 pt spread — use projection rank as primary sort for portfolio selection
- CONTEST: Avg Top 1% threshold is 150.9 pts across 55 contests
- CONTEST: Avg cash line is 111.5 pts — pool floor should exceed this
- CONTEST: Avg winner scores 182.3 pts — need high-ceiling correlated stacks
- TRACKING: Run this analysis daily to build sample size — patterns stabilize after 2+ weeks

## Sim Validation — 2026-05-06

### Distribution Calibration
- **Hitters**: 89.3% in P10-P90 [WARN] (below floor=3.3%, above ceiling=7.4%)
- **Pitchers**: 63.3% in P10-P90 [FAIL] (below floor=16.7%, above ceiling=20.0%)

### Projection Accuracy
- Overall: MAE=5.9, Bias=+1.17, r=0.303
- Hitters: MAE=5.63 [PASS]
- Pitchers: MAE=8.33 [WARN]

### Pitcher Components
- IP: MAE=1.24, Bias=-0.46
- Ks: MAE=1.71, Bias=+0.12
- ER: MAE=1.77, Bias=-0.13

### Multiplier Effectiveness

## Slate Review — 2026-05-06 / early

- **Pool**: 1000 lineups, avg actual=67.9, cash line=66.0, GPP line=145.5, best=175.9
- **Proj accuracy**: r=0.173, MAE=32.3, bias=+26.2
- **Overlap**: 0/20 top-by-proj were actual winners
- **Best strategy**: Salary (highest salary)
- **Top stack**: LAD (avg actual=112.9, 6.3% exposure)
- **Biggest bust**: Tyler Glasnow (proj=18.6, actual=3.6, 46% exp)
- **Biggest missed opp**: Andy Pages (actual=48.0, 6.8% exp)

## Slate Review — 2026-05-06 / main

- **Pool**: 1500 lineups, avg actual=84.7, cash line=80.8, GPP line=170.4, best=182.8
- **Proj accuracy**: r=-0.138, MAE=28.4, bias=+15.3
- **Overlap**: 0/20 top-by-proj were actual winners
- **Best strategy**: Salary (highest salary)
- **Top stack**: WSH (avg actual=147.6, 4.8% exposure)
- **Biggest bust**: Zack Wheeler (proj=21.0, actual=12.6, 48% exp)
- **Biggest missed opp**: Nathan Eovaldi (actual=34.2, 13.6% exp)


## Ownership Calibration — 4 large-field contests (≥1000 entries)

- **Matched players**: 411 (30 0%-actual ghosts excluded)
- **Bias**: +5.75% (positive = over-project ownership)
- **MAE**: 6.57%
- **Correlation**: r=0.620

- Chalk (>20% actual): n=12, bias=-2.39%, MAE=12.18%
- Mid (5-20% actual): n=95, bias=+5.76%, MAE=7.43%
- Low (<5% actual): n=304, bias=+6.07%, MAE=6.08%

**Over-projected:**
- Jacob deGrom: proj=38.1% actual=0.2%
- Nathan Eovaldi: proj=31.4% actual=2.9%
- Nick Pivetta: proj=36.7% actual=9.4%
- Shota Imanaga: proj=36.6% actual=9.4%
- Tatsuya Imai: proj=41.4% actual=14.4%

**Under-projected:**
- Sandy Alcantara: proj=13.7% actual=37.9%
- Eury Perez: proj=20.7% actual=40.9%
- Pete Crow-Armstrong: proj=9.1% actual=23.6%
- Dylan Cease: proj=34.0% actual=47.3%
- Michael Busch: proj=8.6% actual=21.0%

## Leverage Analysis — 2026-05-07 (37 contests, 3380 players)

**Dataset**: 3057 hitters, 323 pitchers across 29 dates
**Leverage hits**: 384 (11.4%) | **Chalk traps**: 81 (2.4%) | **Ceiling hits**: 322 (9.5%)

### Hitter Predictors (correlation with outperformance)
- `wRC+` r=+0.175 (n=3057)
- `ISO` r=+0.148 (n=3057)
- `xwOBA` r=+0.118 (n=3057)
- `Salary` r=+0.080 (n=3057)
- `Barrel%` r=+0.076 (n=3057)

### Pitcher Predictors
- `K%` r=+0.242 (n=323)
- `Salary` r=+0.194 (n=323)
- `xFIP` r=-0.222 (n=323)
- `Win Prob` r=+0.128 (n=323)
- `Stuff+` r=+0.149 (n=323)

### Actionable Rules
- **Hitter leverage**: ISO > 0.200 AND own < 10% -> 27% leverage rate (+17pp vs base)
- **Hitter leverage**: wRC+ > 120 AND own < 10% -> 22% leverage rate (+12pp vs base)
- **Hitter trap**: Own > 20% AND K% > 0.28 -> 68% chalk trap rate
- **Pitcher leverage**: K% > 0.25 AND own < 15% -> 33% leverage rate (+17pp vs base)
- **Pitcher leverage**: Stuff+ > 105 AND own < 15% -> 31% leverage rate (+14pp vs base)
- **Pitcher trap**: Own > 25% AND Stuff+ < 100 -> 59% chalk trap rate
- **Pitcher trap**: Own > 25% AND xFIP > 4.0 -> 56% chalk trap rate

## Winner Pattern Analysis — 2026-05-07 (55 contests, 759,160 entries)
- Top 1% profile: 137% total own, 3.9 booms, 0.7 busts, 48 pitcher pts
- Target: ownership 100-150%, 3+ booms, <1 bust, 25+ pitcher pts

## Research Findings — 2026-04-30, 2026-05-01, 2026-05-02, 2026-05-03, 2026-05-04, 2026-05-05, 2026-05-06, 2026-05-07

**Projection**: MAE=5.86, Bias=+1.05, Hitter MAE=5.62, Pitcher MAE=8.02
**Pool**: MAE=73.85, Bias=+73.84
**Contest**: Winner=182.4125, Top1%=150.73660696428573

### Predictive Diagnostics

**Pitcher Missing Predictors** (correlated with error but not in model):
- `l` r=-0.117 (n=188)
- `bb9` r=-0.088 (n=188)
- `era` r=-0.087 (n=188)

**Optimal Context Weights**: Vegas=80% Park=15% Weather=5% (saves 0.00 MAE)

**Archetype Biases:**
- Power (ISO>.200): over-projected by 1.2 pts (n=396)
- Contact (K%<15%): over-projected by 0.8 pts (n=382)
- Speed (SB pace>15): over-projected by 1.3 pts (n=500)

**Recommendations:**
- PROJECTION: increase context multiplier weights — hitter bias is -1.13 pts
- POOL: Best performing stack config is 5-0 — increase its weight in STACK_CONFIGS
- POOL: Projections have 32.6 pt spread — use projection rank as primary sort for portfolio selection
- CONTEST: Avg Top 1% threshold is 150.7 pts across 56 contests
- CONTEST: Avg cash line is 111.4 pts — pool floor should exceed this
- CONTEST: Avg winner scores 182.4 pts — need high-ceiling correlated stacks
- TRACKING: Run this analysis daily to build sample size — patterns stabilize after 2+ weeks

## Sim Validation — 2026-05-07

### Distribution Calibration
- **Hitters**: 88.2% in P10-P90 [WARN] (below floor=1.2%, above ceiling=10.7%)
- **Pitchers**: 70.0% in P10-P90 [WARN] (below floor=20.0%, above ceiling=10.0%)

### Projection Accuracy
- Overall: MAE=5.98, Bias=+0.61, r=0.147
- Hitters: MAE=5.68 [PASS]
- Pitchers: MAE=8.53 [WARN]

### Pitcher Components
- IP: MAE=1.22, Bias=-0.72
- Ks: MAE=1.88, Bias=+0.04
- ER: MAE=1.46, Bias=-0.61

### Multiplier Effectiveness

## Slate Review — 2026-05-07 / main

- **Pool**: 10000 lineups, avg actual=89.8, cash line=89.9, GPP line=145.0, best=171.0
- **Proj accuracy**: r=0.430, MAE=18.6, bias=+6.4
- **Overlap**: 0/20 top-by-proj were actual winners
- **Best strategy**: Salary (highest salary)
- **Top stack**: KC (avg actual=106.3, 13.4% exposure)
- **Biggest bust**: Jake Irvin (proj=11.7, actual=1.9, 24% exp)
- **Biggest missed opp**: Keibert Ruiz (actual=34.0, 4.3% exp)


## Ownership Calibration — 4 large-field contests (≥1000 entries)

- **Matched players**: 284 (22 0%-actual ghosts excluded)
- **Bias**: +6.12% (positive = over-project ownership)
- **MAE**: 6.97%
- **Correlation**: r=0.518

- Chalk (>20% actual): n=6, bias=-7.49%, MAE=15.88%
- Mid (5-20% actual): n=58, bias=+6.43%, MAE=8.15%
- Low (<5% actual): n=220, bias=+6.41%, MAE=6.42%

**Over-projected:**
- Jacob deGrom: proj=38.1% actual=0.2%
- Shota Imanaga: proj=44.2% actual=9.4%
- Nathan Eovaldi: proj=31.4% actual=2.9%
- Nick Pivetta: proj=36.7% actual=9.4%
- Seth Lugo: proj=26.7% actual=0.8%

**Under-projected:**
- Sandy Alcantara: proj=13.7% actual=37.9%
- Eury Perez: proj=20.7% actual=40.9%
- Pete Crow-Armstrong: proj=9.8% actual=23.6%
- Michael Busch: proj=9.1% actual=21.0%
- Ian Happ: proj=10.2% actual=18.8%

## Leverage Analysis — 2026-05-07 (38 contests, 2461 players)

**Dataset**: 2224 hitters, 237 pitchers across 30 dates
**Leverage hits**: 267 (10.8%) | **Chalk traps**: 59 (2.4%) | **Ceiling hits**: 241 (9.8%)

### Hitter Predictors (correlation with outperformance)
- `wRC+` r=+0.166 (n=2222)
- `ISO` r=+0.156 (n=2222)
- `xwOBA` r=+0.107 (n=2222)
- `Barrel%` r=+0.073 (n=2222)
- `Salary` r=+0.062 (n=2224)

### Pitcher Predictors
- `K%` r=+0.288 (n=237)
- `Salary` r=+0.248 (n=237)
- `xFIP` r=-0.266 (n=237)
- `Win Prob` r=+0.186 (n=237)
- `Stuff+` r=+0.239 (n=237)

### Actionable Rules
- **Hitter leverage**: ISO > 0.200 AND own < 10% -> 27% leverage rate (+17pp vs base)
- **Hitter leverage**: wRC+ > 120 AND own < 10% -> 22% leverage rate (+12pp vs base)
- **Hitter trap**: Own > 20% AND K% > 0.28 -> 68% chalk trap rate
- **Pitcher leverage**: K% > 0.25 AND own < 15% -> 33% leverage rate (+17pp vs base)
- **Pitcher leverage**: Stuff+ > 105 AND own < 15% -> 31% leverage rate (+14pp vs base)
- **Pitcher trap**: Own > 25% AND Stuff+ < 100 -> 59% chalk trap rate
- **Pitcher trap**: Own > 25% AND xFIP > 4.0 -> 56% chalk trap rate

## Winner Pattern Analysis — 2026-05-07 (56 contests, 767,249 entries)
- Top 1% profile: 137% total own, 3.9 booms, 0.7 busts, 48 pitcher pts
- Target: ownership 100-150%, 3+ booms, <1 bust, 25+ pitcher pts

## Research Findings — 2026-05-02, 2026-05-03, 2026-05-04, 2026-05-05, 2026-05-06, 2026-05-07, 2026-05-08, 2026-05-09

**Projection**: MAE=5.90, Bias=+1.17, Hitter MAE=5.63, Pitcher MAE=8.32
**Pool**: MAE=58.75, Bias=+58.61
**Contest**: Winner=182.6298245614035, Top1%=150.95526298245613

### Predictive Diagnostics

**Hitter Missing Predictors** (correlated with error but not in model):
- `avg_ev` r=-0.061 (n=1585)
- `ops` r=-0.051 (n=1573)
- `obp` r=-0.051 (n=1573)

**Pitcher Missing Predictors** (correlated with error but not in model):
- `l` r=-0.127 (n=173)
- `gs` r=-0.123 (n=173)
- `ip` r=-0.119 (n=173)
- `bb9` r=-0.111 (n=173)
- `w` r=-0.104 (n=173)

**Optimal Context Weights**: Vegas=80% Park=15% Weather=5% (saves 0.00 MAE)

**Archetype Biases:**
- Power (ISO>.200): over-projected by 1.4 pts (n=360)
- Contact (K%<15%): over-projected by 1.2 pts (n=352)
- Speed (SB pace>15): over-projected by 1.5 pts (n=458)

**Recommendations:**
- PROJECTION: increase context multiplier weights — hitter bias is -1.37 pts
- POOL: Best performing stack config is 4-0 — increase its weight in STACK_CONFIGS
- POOL: Projections have 20.7 pt spread — use projection rank as primary sort for portfolio selection
- CONTEST: Avg Top 1% threshold is 151.0 pts across 57 contests
- CONTEST: Avg cash line is 111.5 pts — pool floor should exceed this
- CONTEST: Avg winner scores 182.6 pts — need high-ceiling correlated stacks
- TRACKING: Run this analysis daily to build sample size — patterns stabilize after 2+ weeks

## Sim Validation — 2026-05-08

### Distribution Calibration
- **Hitters**: 90.3% in P10-P90 [WARN] (below floor=2.6%, above ceiling=7.1%)
- **Pitchers**: 57.1% in P10-P90 [FAIL] (below floor=14.3%, above ceiling=28.6%)

### Projection Accuracy
- Overall: MAE=6.04, Bias=+1.43, r=0.366
- Hitters: MAE=5.63 [PASS]
- Pitchers: MAE=9.95 [WARN]

### Pitcher Components
- IP: MAE=1.14, Bias=-0.59
- Ks: MAE=1.98, Bias=-0.69
- ER: MAE=1.53, Bias=+0.13

### Multiplier Effectiveness

## Slate Review — 2026-05-08 / main

- **Pool**: 1500 lineups, avg actual=88.3, cash line=87.9, GPP line=149.0, best=189.9
- **Proj accuracy**: r=0.211, MAE=23.3, bias=+16.4
- **Overlap**: 0/20 top-by-proj were actual winners
- **Best strategy**: Salary (highest salary)
- **Top stack**: SEA (avg actual=120.8, 3.9% exposure)
- **Biggest bust**: Reid Detmers (proj=18.3, actual=5.5, 26% exp)
- **Biggest missed opp**: Luke Raley (actual=38.0, 1.5% exp)


## Ownership Calibration — 4 large-field contests (≥1000 entries)

- **Matched players**: 411 (32 0%-actual ghosts excluded)
- **Bias**: +5.86% (positive = over-project ownership)
- **MAE**: 6.64%
- **Correlation**: r=0.612

- Chalk (>20% actual): n=12, bias=-1.73%, MAE=12.51%
- Mid (5-20% actual): n=95, bias=+5.84%, MAE=7.38%
- Low (<5% actual): n=304, bias=+6.17%, MAE=6.18%

**Over-projected:**
- Jacob deGrom: proj=38.1% actual=0.2%
- Shota Imanaga: proj=44.2% actual=9.4%
- Jesus Luzardo: proj=52.3% actual=21.9%
- Nathan Eovaldi: proj=31.4% actual=2.9%
- Nick Pivetta: proj=36.7% actual=9.4%

**Under-projected:**
- Sandy Alcantara: proj=13.7% actual=37.9%
- Eury Perez: proj=20.7% actual=40.9%
- Pete Crow-Armstrong: proj=9.6% actual=23.6%
- Dylan Cease: proj=34.1% actual=47.3%
- Michael Busch: proj=9.0% actual=21.0%

## Leverage Analysis — 2026-05-09 (39 contests, 5116 players)

**Dataset**: 4631 hitters, 485 pitchers across 31 dates
**Leverage hits**: 597 (11.7%) | **Chalk traps**: 109 (2.1%) | **Ceiling hits**: 511 (10.0%)

### Hitter Predictors (correlation with outperformance)
- `wRC+` r=+0.189 (n=4627)
- `ISO` r=+0.158 (n=4627)
- `xwOBA` r=+0.131 (n=4627)
- `Salary` r=+0.089 (n=4631)
- `Barrel%` r=+0.069 (n=4627)

### Pitcher Predictors
- `K%` r=+0.276 (n=485)
- `Salary` r=+0.213 (n=485)
- `xFIP` r=-0.283 (n=485)
- `Win Prob` r=+0.037 (n=485)
- `Stuff+` r=+0.126 (n=485)

### Actionable Rules
- **Hitter leverage**: ISO > 0.200 AND own < 10% -> 27% leverage rate (+17pp vs base)
- **Hitter leverage**: wRC+ > 120 AND own < 10% -> 22% leverage rate (+12pp vs base)
- **Hitter trap**: Own > 20% AND K% > 0.28 -> 68% chalk trap rate
- **Pitcher leverage**: K% > 0.25 AND own < 15% -> 33% leverage rate (+17pp vs base)
- **Pitcher leverage**: Stuff+ > 105 AND own < 15% -> 31% leverage rate (+14pp vs base)
- **Pitcher trap**: Own > 25% AND Stuff+ < 100 -> 59% chalk trap rate
- **Pitcher trap**: Own > 25% AND xFIP > 4.0 -> 56% chalk trap rate

## Winner Pattern Analysis — 2026-05-09 (57 contests, 776,736 entries)
- Top 1% profile: 137% total own, 3.9 booms, 0.7 busts, 48 pitcher pts
- Target: ownership 100-150%, 3+ booms, <1 bust, 25+ pitcher pts

## Research Findings — 2026-05-03, 2026-05-04, 2026-05-05, 2026-05-06, 2026-05-07, 2026-05-08, 2026-05-09, 2026-05-10

**Projection**: MAE=5.93, Bias=+1.19, Hitter MAE=5.65, Pitcher MAE=8.45
**Pool**: MAE=58.12, Bias=+58.04
**Contest**: Winner=182.72413793103448, Top1%=151.06120672413792

### Predictive Diagnostics

**Pitcher Missing Predictors** (correlated with error but not in model):
- `g` r=-0.128 (n=172)
- `l` r=-0.127 (n=172)
- `ip` r=-0.125 (n=172)
- `gs` r=-0.123 (n=172)
- `whip` r=-0.121 (n=172)

**Opposing Lineup Factors** (for pitcher projections):
- `opp_o_swing_pct` r=-0.108 (n=185)

**Optimal Context Weights**: Vegas=80% Park=15% Weather=5% (saves 0.00 MAE)

**Archetype Biases:**
- Power (ISO>.200): over-projected by 1.7 pts (n=362)
- Contact (K%<15%): over-projected by 1.4 pts (n=351)
- Speed (SB pace>15): over-projected by 1.3 pts (n=450)

**Recommendations:**
- PROJECTION: increase context multiplier weights — hitter bias is -1.43 pts
- POOL: Best performing stack config is 4-0 — increase its weight in STACK_CONFIGS
- POOL: Projections have 18.7 pt spread — use projection rank as primary sort for portfolio selection
- CONTEST: Avg Top 1% threshold is 151.1 pts across 58 contests
- CONTEST: Avg cash line is 111.6 pts — pool floor should exceed this
- CONTEST: Avg winner scores 182.7 pts — need high-ceiling correlated stacks
- TRACKING: Run this analysis daily to build sample size — patterns stabilize after 2+ weeks

## Sim Validation — 2026-05-09

### Distribution Calibration
- **Hitters**: 90.0% in P10-P90 [WARN] (below floor=2.8%, above ceiling=7.2%)
- **Pitchers**: 60.7% in P10-P90 [FAIL] (below floor=14.3%, above ceiling=25.0%)

### Projection Accuracy
- Overall: MAE=5.9, Bias=+0.96, r=0.319
- Hitters: MAE=5.6 [PASS]
- Pitchers: MAE=8.57 [WARN]

### Pitcher Components
- IP: MAE=0.99, Bias=-0.32
- Ks: MAE=1.78, Bias=-0.70
- ER: MAE=1.76, Bias=+0.35

### Multiplier Effectiveness

## Slate Review — 2026-05-09 / early

- **Pool**: 6000 lineups, avg actual=76.5, cash line=76.4, GPP line=143.3, best=175.3
- **Proj accuracy**: r=0.102, MAE=28.0, bias=+19.7
- **Overlap**: 0/20 top-by-proj were actual winners
- **Best strategy**: Salary (highest salary)
- **Top stack**: WSH (avg actual=105.7, 6.4% exposure)
- **Biggest bust**: Liam Hicks (proj=7.7, actual=2.0, 30% exp)
- **Biggest missed opp**: Brandon Valenzuela (actual=35.0, 8.5% exp)


## Ownership Calibration — 4 large-field contests (≥1000 entries)

- **Matched players**: 410 (33 0%-actual ghosts excluded)
- **Bias**: +5.90% (positive = over-project ownership)
- **MAE**: 6.70%
- **Correlation**: r=0.609

- Chalk (>20% actual): n=12, bias=-1.73%, MAE=12.55%
- Mid (5-20% actual): n=95, bias=+5.85%, MAE=7.45%
- Low (<5% actual): n=303, bias=+6.22%, MAE=6.23%

**Over-projected:**
- Jacob deGrom: proj=38.1% actual=0.2%
- Shota Imanaga: proj=44.2% actual=9.4%
- Jesus Luzardo: proj=52.3% actual=21.9%
- Nathan Eovaldi: proj=31.4% actual=2.9%
- Nick Pivetta: proj=36.7% actual=9.4%

**Under-projected:**
- Sandy Alcantara: proj=13.7% actual=37.9%
- Eury Perez: proj=20.7% actual=40.9%
- Pete Crow-Armstrong: proj=9.6% actual=23.6%
- Dylan Cease: proj=34.1% actual=47.3%
- Michael Busch: proj=8.9% actual=21.0%

## Leverage Analysis — 2026-05-10 (39 contests, 3245 players)

**Dataset**: 2937 hitters, 308 pitchers across 31 dates
**Leverage hits**: 348 (10.7%) | **Chalk traps**: 87 (2.7%) | **Ceiling hits**: 318 (9.8%)

### Hitter Predictors (correlation with outperformance)
- `wRC+` r=+0.209 (n=2933)
- `ISO` r=+0.174 (n=2933)
- `xwOBA` r=+0.148 (n=2933)
- `Salary` r=+0.092 (n=2937)
- `Barrel%` r=+0.077 (n=2933)

### Pitcher Predictors
- `K%` r=+0.301 (n=308)
- `Salary` r=+0.223 (n=308)
- `xFIP` r=-0.285 (n=308)
- `Win Prob` r=+0.049 (n=308)
- `Stuff+` r=+0.083 (n=308)

### Actionable Rules
- **Hitter leverage**: ISO > 0.200 AND own < 10% -> 27% leverage rate (+17pp vs base)
- **Hitter leverage**: wRC+ > 120 AND own < 10% -> 22% leverage rate (+12pp vs base)
- **Hitter trap**: Own > 20% AND K% > 0.28 -> 68% chalk trap rate
- **Pitcher leverage**: K% > 0.25 AND own < 15% -> 33% leverage rate (+17pp vs base)
- **Pitcher leverage**: Stuff+ > 105 AND own < 15% -> 31% leverage rate (+14pp vs base)
- **Pitcher trap**: Own > 25% AND Stuff+ < 100 -> 59% chalk trap rate
- **Pitcher trap**: Own > 25% AND xFIP > 4.0 -> 56% chalk trap rate

## Winner Pattern Analysis — 2026-05-10 (58 contests, 782,669 entries)
- Top 1% profile: 138% total own, 3.9 booms, 0.7 busts, 48 pitcher pts
- Target: ownership 100-150%, 3+ booms, <1 bust, 25+ pitcher pts

## Research Findings — 2026-05-03, 2026-05-04, 2026-05-05, 2026-05-06, 2026-05-07, 2026-05-08, 2026-05-09, 2026-05-10

**Projection**: MAE=5.88, Bias=+1.29, Hitter MAE=5.61, Pitcher MAE=8.31
**Pool**: MAE=72.60, Bias=+72.58
**Contest**: Winner=182.54237288135593, Top1%=151.04661000000002

### Predictive Diagnostics

**Pitcher Missing Predictors** (correlated with error but not in model):
- `bb9` r=-0.135 (n=200)
- `whip` r=-0.128 (n=200)
- `era` r=-0.119 (n=200)
- `k_bb_pct` r=+0.110 (n=200)
- `avg` r=-0.092 (n=200)

**Optimal Context Weights**: Vegas=80% Park=15% Weather=5% (saves 0.00 MAE)

**Archetype Biases:**
- Power (ISO>.200): over-projected by 1.8 pts (n=417)
- Contact (K%<15%): over-projected by 1.6 pts (n=411)
- Strikeout (K%>28%): over-projected by 1.1 pts (n=279)
- Speed (SB pace>15): over-projected by 1.5 pts (n=528)

**Recommendations:**
- PROJECTION: increase context multiplier weights — hitter bias is -1.56 pts
- POOL: Best performing stack config is 4-0 — increase its weight in STACK_CONFIGS
- POOL: Projections have 17.4 pt spread — use projection rank as primary sort for portfolio selection
- CONTEST: Avg Top 1% threshold is 151.0 pts across 59 contests
- CONTEST: Avg cash line is 111.6 pts — pool floor should exceed this
- CONTEST: Avg winner scores 182.5 pts — need high-ceiling correlated stacks
- TRACKING: Run this analysis daily to build sample size — patterns stabilize after 2+ weeks

## Sim Validation — 2026-05-10

### Distribution Calibration
- **Hitters**: 93.2% in P10-P90 [FAIL] (below floor=2.3%, above ceiling=4.5%)
- **Pitchers**: 63.3% in P10-P90 [FAIL] (below floor=6.7%, above ceiling=30.0%)

### Projection Accuracy
- Overall: MAE=5.56, Bias=+1.89, r=0.473
- Hitters: MAE=5.34 [PASS]
- Pitchers: MAE=7.45 [PASS]

### Pitcher Components
- IP: MAE=1.27, Bias=-0.66
- Ks: MAE=1.62, Bias=-0.23
- ER: MAE=1.44, Bias=+0.35

### Multiplier Effectiveness

## Slate Review — 2026-05-10 / main

- **Pool**: 10000 lineups, avg actual=83.7, cash line=82.9, GPP line=142.7, best=171.5
- **Proj accuracy**: r=0.302, MAE=23.5, bias=+14.9
- **Overlap**: 0/20 top-by-proj were actual winners
- **Best strategy**: Salary (highest salary)
- **Top stack**: PHI (avg actual=111.7, 11.0% exposure)
- **Biggest bust**: Gavin Williams (proj=18.1, actual=8.9, 22% exp)
- **Biggest missed opp**: Jo Adell (actual=33.0, 2.5% exp)


## Ownership Calibration — 4 large-field contests (≥1000 entries)

- **Matched players**: 409 (33 0%-actual ghosts excluded)
- **Bias**: +5.90% (positive = over-project ownership)
- **MAE**: 6.68%
- **Correlation**: r=0.609

- Chalk (>20% actual): n=12, bias=-1.72%, MAE=12.54%
- Mid (5-20% actual): n=95, bias=+5.86%, MAE=7.40%
- Low (<5% actual): n=302, bias=+6.21%, MAE=6.23%

**Over-projected:**
- Jacob deGrom: proj=40.5% actual=0.2%
- Shota Imanaga: proj=44.2% actual=9.4%
- Jesus Luzardo: proj=52.3% actual=21.9%
- Nathan Eovaldi: proj=31.4% actual=2.9%
- Nick Pivetta: proj=36.7% actual=9.4%

**Under-projected:**
- Sandy Alcantara: proj=13.7% actual=37.9%
- Eury Perez: proj=20.7% actual=40.9%
- Pete Crow-Armstrong: proj=9.6% actual=23.6%
- Dylan Cease: proj=34.1% actual=47.3%
- Michael Busch: proj=8.9% actual=21.0%

## Leverage Analysis — 2026-05-10 (40 contests, 3741 players)

**Dataset**: 3391 hitters, 350 pitchers across 32 dates
**Leverage hits**: 428 (11.4%) | **Chalk traps**: 78 (2.1%) | **Ceiling hits**: 375 (10.0%)

### Hitter Predictors (correlation with outperformance)
- `wRC+` r=+0.162 (n=3388)
- `ISO` r=+0.132 (n=3388)
- `xwOBA` r=+0.117 (n=3388)
- `Salary` r=+0.090 (n=3391)
- `Barrel%` r=+0.068 (n=3388)

### Pitcher Predictors
- `K%` r=+0.283 (n=349)
- `Salary` r=+0.191 (n=350)
- `xFIP` r=-0.256 (n=349)
- `Win Prob` r=+0.102 (n=350)
- `Stuff+` r=+0.132 (n=349)

### Actionable Rules
- **Hitter leverage**: ISO > 0.200 AND own < 10% -> 27% leverage rate (+17pp vs base)
- **Hitter leverage**: wRC+ > 120 AND own < 10% -> 22% leverage rate (+12pp vs base)
- **Hitter trap**: Own > 20% AND K% > 0.28 -> 68% chalk trap rate
- **Pitcher leverage**: K% > 0.25 AND own < 15% -> 33% leverage rate (+17pp vs base)
- **Pitcher leverage**: Stuff+ > 105 AND own < 15% -> 31% leverage rate (+14pp vs base)
- **Pitcher trap**: Own > 25% AND Stuff+ < 100 -> 59% chalk trap rate
- **Pitcher trap**: Own > 25% AND xFIP > 4.0 -> 56% chalk trap rate

## Winner Pattern Analysis — 2026-05-10 (59 contests, 792,148 entries)
- Top 1% profile: 138% total own, 3.9 booms, 0.7 busts, 48 pitcher pts
- Target: ownership 100-150%, 3+ booms, <1 bust, 25+ pitcher pts

## Research Findings — 2026-05-05, 2026-05-06, 2026-05-07, 2026-05-08, 2026-05-09, 2026-05-10, 2026-05-11, 2026-05-12

**Projection**: MAE=5.94, Bias=+1.23, Hitter MAE=5.66, Pitcher MAE=8.44
**Pool**: MAE=64.66, Bias=+64.64
**Contest**: Winner=182.495, Top1%=150.85749983333332

### Predictive Diagnostics

**Hitter Missing Predictors** (correlated with error but not in model):
- `slg` r=-0.069 (n=1443)
- `ops` r=-0.068 (n=1443)
- `avg_ev` r=-0.065 (n=1457)
- `obp` r=-0.051 (n=1443)

**Pitcher Missing Predictors** (correlated with error but not in model):
- `bb9` r=-0.229 (n=159)
- `whip` r=-0.211 (n=159)
- `era` r=-0.153 (n=159)
- `k_bb_pct` r=+0.146 (n=159)
- `gb_pct` r=-0.135 (n=159)

**Optimal Context Weights**: Vegas=80% Park=5% Weather=15% (saves -0.00 MAE)

**Archetype Biases:**
- Power (ISO>.200): over-projected by 1.9 pts (n=343)
- Contact (K%<15%): over-projected by 1.6 pts (n=345)
- Strikeout (K%>28%): over-projected by 1.4 pts (n=221)
- Speed (SB pace>15): over-projected by 1.5 pts (n=430)

**Recommendations:**
- PROJECTION: increase context multiplier weights — hitter bias is -1.51 pts
- POOL: Best performing stack config is 4-4 — increase its weight in STACK_CONFIGS
- POOL: Projections have 21.1 pt spread — use projection rank as primary sort for portfolio selection
- CONTEST: Avg Top 1% threshold is 150.9 pts across 60 contests
- CONTEST: Avg cash line is 111.4 pts — pool floor should exceed this
- CONTEST: Avg winner scores 182.5 pts — need high-ceiling correlated stacks
- TRACKING: Run this analysis daily to build sample size — patterns stabilize after 2+ weeks

## Sim Validation — 2026-05-11

### Distribution Calibration
- **Hitters**: 93.3% in P10-P90 [FAIL] (below floor=1.0%, above ceiling=5.7%)
- **Pitchers**: 83.3% in P10-P90 [PASS] (below floor=8.3%, above ceiling=8.3%)

### Projection Accuracy
- Overall: MAE=5.8, Bias=+1.71, r=0.334
- Hitters: MAE=5.7 [PASS]
- Pitchers: MAE=6.7 [PASS]

### Pitcher Components
- IP: MAE=0.95, Bias=-0.63
- Ks: MAE=1.01, Bias=-0.59
- ER: MAE=1.37, Bias=-0.01

### Multiplier Effectiveness

## Slate Review — 2026-05-11 / main

- **Pool**: 10000 lineups, avg actual=74.1, cash line=73.2, GPP line=130.4, best=153.7
- **Proj accuracy**: r=-0.029, MAE=27.7, bias=+22.9
- **Overlap**: 0/20 top-by-proj were actual winners
- **Best strategy**: Salary (highest salary)
- **Top stack**: TB (avg actual=105.5, 3.0% exposure)
- **Biggest bust**: Kevin Gausman (proj=18.6, actual=2.5, 27% exp)
- **Biggest missed opp**: Andrés Giménez (actual=34.0, 1.8% exp)


## Ownership Calibration — 4 large-field contests (≥1000 entries)

- **Matched players**: 143 (13 0%-actual ghosts excluded)
- **Bias**: +5.98% (positive = over-project ownership)
- **MAE**: 6.41%
- **Correlation**: r=0.604

- Chalk (>20% actual): n=3, bias=+0.27%, MAE=9.05%
- Mid (5-20% actual): n=43, bias=+5.53%, MAE=6.32%
- Low (<5% actual): n=97, bias=+6.35%, MAE=6.36%

**Over-projected:**
- Jacob deGrom: proj=40.5% actual=0.2%
- Nathan Eovaldi: proj=34.4% actual=2.9%
- Tatsuya Imai: proj=41.4% actual=14.4%
- Jose Soriano: proj=25.1% actual=0.7%
- Emmet Sheehan: proj=34.8% actual=12.9%

**Under-projected:**
- Dylan Cease: proj=34.1% actual=47.3%
- Gunnar Henderson: proj=11.3% actual=14.6%
- Jose Altuve: proj=12.8% actual=15.5%
- Cedric Mullins: proj=4.7% actual=7.1%
- Daulton Varsho: proj=7.3% actual=9.4%

## Leverage Analysis — 2026-05-12 (41 contests, 2597 players)

**Dataset**: 2356 hitters, 241 pitchers across 33 dates
**Leverage hits**: 281 (10.8%) | **Chalk traps**: 77 (3.0%) | **Ceiling hits**: 258 (9.9%)

### Hitter Predictors (correlation with outperformance)
- `wRC+` r=+0.168 (n=2353)
- `ISO` r=+0.118 (n=2353)
- `xwOBA` r=+0.108 (n=2353)
- `Salary` r=+0.055 (n=2356)
- `Barrel%` r=+0.027 (n=2353)

### Pitcher Predictors
- `K%` r=+0.315 (n=241)
- `Salary` r=+0.125 (n=241)
- `xFIP` r=-0.302 (n=241)
- `Win Prob` r=+0.015 (n=241)
- `Stuff+` r=+0.153 (n=241)

### Actionable Rules
- **Hitter leverage**: ISO > 0.200 AND own < 10% -> 27% leverage rate (+17pp vs base)
- **Hitter leverage**: wRC+ > 120 AND own < 10% -> 22% leverage rate (+12pp vs base)
- **Hitter trap**: Own > 20% AND K% > 0.28 -> 68% chalk trap rate
- **Pitcher leverage**: K% > 0.25 AND own < 15% -> 33% leverage rate (+17pp vs base)
- **Pitcher leverage**: Stuff+ > 105 AND own < 15% -> 31% leverage rate (+14pp vs base)
- **Pitcher trap**: Own > 25% AND Stuff+ < 100 -> 59% chalk trap rate
- **Pitcher trap**: Own > 25% AND xFIP > 4.0 -> 56% chalk trap rate

## Winner Pattern Analysis — 2026-05-12 (60 contests, 801,622 entries)
- Top 1% profile: 138% total own, 3.9 booms, 0.7 busts, 48 pitcher pts
- Target: ownership 100-150%, 3+ booms, <1 bust, 25+ pitcher pts


## Ownership Study — 2026-05-16 (657 player-slate records, 2 dates)

### Overall Accuracy
- **Matched records**: 657
- **Bias**: +2.86% (positive = over-project)
- **MAE**: 5.63%
- **Correlation**: r=0.330

### By Tier
- Chalk  (actual >20%): n=16, bias=-7.81%, MAE=14.82%, r=-0.296
- Mid    (actual 5-20%): n=176, bias=+2.36%, MAE=7.89%, r=0.172
- Low    (actual <5%): n=465, bias=+3.42%, MAE=4.46%, r=0.165

### Feature Correlations
- proj_dk_pts (raw): r=+0.337 (was +0.557)
- proj_dk_pts^1.5: r=+0.387
- salary (raw): r=+0.374 (was +0.536)
- salary^1.3: r=+0.397
- game_total: r=+0.017
- batting_order: r=-0.431 (was +0.203)
- bo_score (mapped): r=+0.417
- env_score: r=+0.026
- value (PPK): r=+0.166 (was -0.055)
- value_score^0.6: r=+0.197

### Weight Optimization
- Current MAE: 3.347%
- Best MAE: 3.340% (improvement: +0.007%)

### Recommendations
- KEEP WEIGHTS: current weights within 0.007% of optimal
- UPDATE OWN_GLOBAL_SCALE: 0.95 → 0.81  (residual bias=+2.86%)
- PLAYER CORRECTIONS: insufficient data (0 players qualify) — skip for now

### Top Over-Projected Players
- 695734 (OF): proj=58.4% actual=2.6%  bias=+55.8%  n=3
- 682928 (SS): proj=30.8% actual=1.0%  bias=+29.8%  n=3
- 543807 (OF): proj=41.8% actual=15.6%  bias=+26.2%  n=3
- 660821 (OF): proj=26.4% actual=3.6%  bias=+22.8%  n=3
- 656941 (OF): proj=30.5% actual=9.2%  bias=+21.3%  n=3
- 680718 (3B): proj=23.4% actual=4.5%  bias=+18.9%  n=3
- 607208 (SS): proj=24.5% actual=6.4%  bias=+18.1%  n=3
- 664770 (OF): proj=21.0% actual=2.9%  bias=+18.1%  n=3
- 669127 (C): proj=27.0% actual=9.0%  bias=+18.0%  n=3
- 691016 (OF): proj=21.8% actual=4.0%  bias=+17.8%  n=3

### Top Under-Projected Players
- 683737 (1B): proj=8.1% actual=21.0%  bias=-12.9%  n=3
- 691718 (OF): proj=12.6% actual=23.6%  bias=-11.0%  n=3
- 608324 (3B): proj=8.8% actual=19.7%  bias=-10.9%  n=3
- 665161 (SS): proj=0.0% actual=9.6%  bias=-9.6%  n=3
- 596115 (SS): proj=5.0% actual=14.2%  bias=-9.2%  n=3
- 664023 (OF): proj=10.1% actual=18.8%  bias=-8.7%  n=3
- 670541 (OF): proj=11.1% actual=19.6%  bias=-8.5%  n=3
- 575929 (1B): proj=1.4% actual=9.4%  bias=-8.0%  n=3
- 656775 (OF): proj=0.0% actual=7.1%  bias=-7.1%  n=3
- 514888 (2B): proj=9.0% actual=15.5%  bias=-6.5%  n=3


## Ownership Study — 2026-05-16 (8,766 player-slate records, 39 dates)

### Overall Accuracy
- **Matched records**: 8,766
- **Bias**: +0.66% (positive = over-project)
- **MAE**: 4.04%
- **Correlation**: r=0.666

### By Tier
- Chalk  (actual >20%): n=506, bias=-7.33%, MAE=12.61%, r=0.505
- Mid    (actual 5-20%): n=2817, bias=-0.87%, MAE=4.85%, r=0.379
- Low    (actual <5%): n=5443, bias=+2.20%, MAE=2.83%, r=0.149

### Feature Correlations
- proj_dk_pts (raw): r=+0.491 (was +0.557)
- proj_dk_pts^1.5: r=+0.547
- salary (raw): r=+0.401 (was +0.536)
- salary^1.3: r=+0.437
- game_total: r=+0.186
- batting_order: r=-0.274 (was +0.203)
- bo_score (mapped): r=+0.273
- env_score: r=+0.078
- value (PPK): r=+0.073 (was -0.055)
- value_score^0.6: r=+0.110

### Weight Optimization
- Current MAE: 3.635%
- Best MAE: 3.624% (improvement: +0.011%)

### Recommendations
- KEEP WEIGHTS: current weights within 0.011% of optimal
- KEEP OWN_GLOBAL_SCALE: 0.95 (bias=+0.66%, within tolerance)
- ADD PLAYER_BIAS_CORRECTIONS: 112 players with |bias|>3% and ≥5 appearances

### Top Over-Projected Players
- 837227 (SP): proj=41.6% actual=14.2%  bias=+27.4%  n=4
- 605400 (SP): proj=23.9% actual=9.3%  bias=+14.6%  n=6
- 691783 (3B): proj=13.6% actual=2.0%  bias=+11.6%  n=6
- 669302 (SP): proj=25.5% actual=15.7%  bias=+9.8%  n=3
- 666200 (SP): proj=44.5% actual=34.8%  bias=+9.7%  n=6
- 543135 (SP): proj=17.8% actual=8.7%  bias=+9.1%  n=6
- 594798 (SP): proj=38.0% actual=29.0%  bias=+8.9%  n=5
- 571945 (SP): proj=9.1% actual=0.4%  bias=+8.7%  n=6
- 680694 (SP): proj=21.6% actual=13.0%  bias=+8.6%  n=7
- 624133 (SP): proj=17.9% actual=9.4%  bias=+8.5%  n=5

### Top Under-Projected Players
- 656849 (SP): proj=17.6% actual=39.6%  bias=-22.0%  n=3
- 607192 (SP): proj=38.1% actual=53.3%  bias=-15.2%  n=5
- 656302 (SP): proj=29.5% actual=44.5%  bias=-15.0%  n=7
- 695505 (SP): proj=24.5% actual=38.3%  bias=-13.8%  n=3
- 681293 (SP): proj=10.8% actual=24.3%  bias=-13.5%  n=3
- 686218 (SP): proj=13.0% actual=25.7%  bias=-12.7%  n=7
- 642547 (SP): proj=15.5% actual=26.6%  bias=-11.1%  n=6
- 667755 (SP): proj=20.5% actual=31.2%  bias=-10.7%  n=3
- 592662 (SP): proj=25.8% actual=35.6%  bias=-9.8%  n=4
- 693645 (SP): proj=17.3% actual=27.0%  bias=-9.7%  n=5

## Research Findings — 2026-05-12, 2026-05-13, 2026-05-14, 2026-05-15, 2026-05-16, 2026-05-17, 2026-05-18, 2026-05-19

**Projection**: MAE=6.55, Bias=+0.29, Hitter MAE=6.21, Pitcher MAE=9.67
**Ownership**: MAE=3.16%, Bias=+0.89%
**Pool**: MAE=26.16, Bias=+1.23
**Contest**: Winner=183.49677419354836, Top1%=151.17419338709678

### Predictive Diagnostics

**Hitter Missing Predictors** (correlated with error but not in model):
- `pull_pct` r=-0.185 (n=227)
- `cent_pct` r=+0.172 (n=227)
- `ops` r=-0.130 (n=228)
- `slg` r=-0.124 (n=228)
- `obp` r=-0.118 (n=228)

**Pitcher Missing Predictors** (correlated with error but not in model):
- `pickoffs` r=-0.462 (n=20)
- `swstr_pct` r=-0.383 (n=19)
- `wild_pitches` r=-0.360 (n=20)
- `cs_allowed` r=-0.317 (n=20)
- `fb_pct` r=-0.309 (n=19)

**Opposing Lineup Factors** (for pitcher projections):
- `opp_gb_pct` r=-0.373 (n=28)
- `opp_avg_ev` r=+0.321 (n=28)
- `opp_k_pct` r=+0.310 (n=28)
- `opp_bb_pct` r=+0.165 (n=28)
- `opp_iso` r=+0.159 (n=28)

**Optimal Context Weights**: Vegas=80% Park=15% Weather=5% (saves -0.00 MAE)

**Archetype Biases:**
- Power (ISO>.200): over-projected by 1.7 pts (n=51)
- Contact (K%<15%): under-projected by 2.1 pts (n=57)

**Recommendations:**
- POOL: Best performing stack config is 4-4 — increase its weight in STACK_CONFIGS
- POOL: Projections have 5.7 pt spread — use projection rank as primary sort for portfolio selection
- CONTEST: Avg Top 1% threshold is 151.2 pts across 62 contests
- CONTEST: Avg cash line is 111.6 pts — pool floor should exceed this
- CONTEST: Avg winner scores 183.5 pts — need high-ceiling correlated stacks
- OWNERSHIP: MAE is 3.2% (needs work)
- TRACKING: Run this analysis daily to build sample size — patterns stabilize after 2+ weeks

## Sim Validation — 2026-05-18

### Distribution Calibration
- **Hitters**: 82.8% in P10-P90 [PASS] (below floor=3.6%, above ceiling=13.6%)
- **Pitchers**: 64.3% in P10-P90 [FAIL] (below floor=17.9%, above ceiling=17.9%)

### Projection Accuracy
- Overall: MAE=6.55, Bias=+0.29, r=0.207
- Hitters: MAE=6.21 [WARN]
- Pitchers: MAE=9.67 [WARN]

### Pitcher Components
- IP: MAE=1.08, Bias=-0.47
- Ks: MAE=1.84, Bias=-0.38
- ER: MAE=1.74, Bias=-0.53

### Multiplier Effectiveness

## Slate Review — 2026-05-18 / main

- **Pool**: 1200 lineups, avg actual=74.6, cash line=73.2, GPP line=142.6, best=173.0
- **Proj accuracy**: r=0.197, MAE=32.1, bias=+25.8
- **Overlap**: 0/20 top-by-proj were actual winners
- **Best strategy**: PMS (highest avg_pms)
- **Top stack**: MIL (avg actual=105.5, 4.5% exposure)
- **Biggest bust**: MacKenzie Gore (proj=16.9, actual=-0.7, 30% exp)
- **Biggest missed opp**: Josh Bell (actual=35.0, 2.3% exp)


## Ownership Calibration — 4 large-field contests (≥1000 entries)

- **Matched players**: 379 (34 0%-actual ghosts excluded)
- **Bias**: +5.65% (positive = over-project ownership)
- **MAE**: 6.48%
- **Correlation**: r=0.616

- Chalk (>20% actual): n=12, bias=-1.48%, MAE=12.40%
- Mid (5-20% actual): n=93, bias=+5.26%, MAE=6.82%
- Low (<5% actual): n=274, bias=+6.10%, MAE=6.11%

**Over-projected:**
- Jacob deGrom: proj=41.8% actual=0.2%
- Nathan Eovaldi: proj=34.4% actual=2.9%
- Jose Soriano: proj=31.3% actual=0.7%
- Shota Imanaga: proj=38.3% actual=9.4%
- Jesus Luzardo: proj=49.9% actual=21.9%

**Under-projected:**
- Sandy Alcantara: proj=14.9% actual=37.9%
- Eury Perez: proj=20.4% actual=40.9%
- Pete Crow-Armstrong: proj=9.3% actual=23.6%
- Dylan Cease: proj=34.4% actual=47.3%
- Michael Busch: proj=8.5% actual=21.0%

## Leverage Analysis — 2026-05-19 (43 contests, 4025 players)

**Dataset**: 3648 hitters, 377 pitchers across 35 dates
**Leverage hits**: 435 (10.8%) | **Chalk traps**: 99 (2.5%) | **Ceiling hits**: 379 (9.4%)

### Hitter Predictors (correlation with outperformance)
- `wRC+` r=+0.152 (n=3642)
- `ISO` r=+0.122 (n=3642)
- `xwOBA` r=+0.102 (n=3642)
- `Salary` r=+0.073 (n=3648)
- `Barrel%` r=+0.039 (n=3642)

### Pitcher Predictors
- `K%` r=+0.275 (n=377)
- `Salary` r=+0.211 (n=377)
- `xFIP` r=-0.247 (n=377)
- `Win Prob` r=+0.090 (n=377)
- `Stuff+` r=+0.178 (n=377)

### Actionable Rules
- **Hitter leverage**: ISO > 0.200 AND own < 10% -> 27% leverage rate (+17pp vs base)
- **Hitter leverage**: wRC+ > 120 AND own < 10% -> 22% leverage rate (+12pp vs base)
- **Hitter trap**: Own > 20% AND K% > 0.28 -> 68% chalk trap rate
- **Pitcher leverage**: K% > 0.25 AND own < 15% -> 33% leverage rate (+17pp vs base)
- **Pitcher leverage**: Stuff+ > 105 AND own < 15% -> 31% leverage rate (+14pp vs base)
- **Pitcher trap**: Own > 25% AND Stuff+ < 100 -> 59% chalk trap rate
- **Pitcher trap**: Own > 25% AND xFIP > 4.0 -> 56% chalk trap rate

## Winner Pattern Analysis — 2026-05-19 (62 contests, 857,180 entries)
- Top 1% profile: 137% total own, 3.8 booms, 0.7 busts, 49 pitcher pts
- Target: ownership 100-150%, 3+ booms, <1 bust, 25+ pitcher pts

## Research Findings — 2026-05-13, 2026-05-14, 2026-05-15, 2026-05-16, 2026-05-17, 2026-05-18, 2026-05-19, 2026-05-20

**Projection**: MAE=6.14, Bias=+0.75, Hitter MAE=5.89, Pitcher MAE=8.32
**Ownership**: MAE=5.33%, Bias=+4.55%
**Pool**: MAE=27.76, Bias=+18.36
**Contest**: Winner=183.73174603174604, Top1%=151.2166665079365

### Predictive Diagnostics

**Hitter Missing Predictors** (correlated with error but not in model):
- `pull_pct` r=-0.153 (n=471)
- `oppo_pct` r=+0.106 (n=471)
- `cent_pct` r=+0.105 (n=471)
- `avg_ev` r=+0.075 (n=476)
- `rbi` r=+0.074 (n=472)

**Pitcher Missing Predictors** (correlated with error but not in model):
- `era` r=+0.267 (n=48)
- `avg` r=+0.228 (n=48)
- `whip` r=+0.194 (n=48)
- `ip` r=-0.091 (n=48)

**Opposing Lineup Factors** (for pitcher projections):
- `opp_k_pct` r=+0.272 (n=58)
- `opp_gb_pct` r=-0.251 (n=58)
- `opp_avg_ev` r=+0.208 (n=58)
- `opp_o_swing_pct` r=-0.124 (n=58)
- `opp_bb_pct` r=+0.120 (n=58)

**Optimal Context Weights**: Vegas=80% Park=15% Weather=5% (saves 0.00 MAE)

**Archetype Biases:**
- Power (ISO>.200): over-projected by 0.9 pts (n=109)

**Recommendations:**
- PROJECTION: increase context multiplier weights — hitter bias is -0.75 pts
- OWNERSHIP: increase baseline ownership estimates — bias is +4.5%
- POOL: Best performing stack config is 5-0 — increase its weight in STACK_CONFIGS
- POOL: Projections have 11.6 pt spread — use projection rank as primary sort for portfolio selection
- CONTEST: Avg Top 1% threshold is 151.2 pts across 63 contests
- CONTEST: Avg cash line is 111.6 pts — pool floor should exceed this
- CONTEST: Avg winner scores 183.7 pts — need high-ceiling correlated stacks
- OWNERSHIP: increase baseline estimates — bias is +4.5%
- OWNERSHIP: MAE is 5.3% — needs significant model improvement
- TRACKING: Run this analysis daily to build sample size — patterns stabilize after 2+ weeks

## Sim Validation — 2026-05-19

### Distribution Calibration
- **Hitters**: 90.7% in P10-P90 [WARN] (below floor=1.1%, above ceiling=8.2%)
- **Pitchers**: 70.0% in P10-P90 [WARN] (below floor=16.7%, above ceiling=13.3%)

### Projection Accuracy
- Overall: MAE=5.75, Bias=+1.18, r=0.353
- Hitters: MAE=5.6 [PASS]
- Pitchers: MAE=7.06 [PASS]

### Pitcher Components
- IP: MAE=0.82, Bias=-0.12
- Ks: MAE=1.82, Bias=-0.07
- ER: MAE=1.47, Bias=-0.51

### Multiplier Effectiveness

## Slate Review — 2026-05-19 / main

- **Pool**: 10000 lineups, avg actual=85.8, cash line=83.6, GPP line=151.6, best=200.6
- **Proj accuracy**: r=0.037, MAE=25.5, bias=+15.1
- **Overlap**: 0/20 top-by-proj were actual winners
- **Best strategy**: PMS (highest avg_pms)
- **Top stack**: ATH (avg actual=124.7, 3.2% exposure)
- **Biggest bust**: Emmet Sheehan (proj=18.5, actual=1.4, 31% exp)
- **Biggest missed opp**: Jarren Duran (actual=34.0, 7.4% exp)


## Ownership Calibration — 4 large-field contests (≥1000 entries)

- **Matched players**: 412 (36 0%-actual ghosts excluded)
- **Bias**: +5.76% (positive = over-project ownership)
- **MAE**: 6.53%
- **Correlation**: r=0.614

- Chalk (>20% actual): n=12, bias=-1.30%, MAE=12.70%
- Mid (5-20% actual): n=95, bias=+5.38%, MAE=6.93%
- Low (<5% actual): n=305, bias=+6.15%, MAE=6.16%

**Over-projected:**
- Jacob deGrom: proj=41.8% actual=0.2%
- Nathan Eovaldi: proj=34.4% actual=2.9%
- Jose Soriano: proj=31.3% actual=0.7%
- Shota Imanaga: proj=38.3% actual=9.4%
- Jesus Luzardo: proj=49.8% actual=21.9%

**Under-projected:**
- Sandy Alcantara: proj=14.9% actual=37.9%
- Eury Perez: proj=20.4% actual=40.9%
- Pete Crow-Armstrong: proj=9.2% actual=23.6%
- Dylan Cease: proj=33.8% actual=47.3%
- Michael Busch: proj=8.5% actual=21.0%

## Leverage Analysis — 2026-05-20 (44 contests, 4657 players)

**Dataset**: 4217 hitters, 440 pitchers across 36 dates
**Leverage hits**: 503 (10.8%) | **Chalk traps**: 102 (2.2%) | **Ceiling hits**: 445 (9.6%)

### Hitter Predictors (correlation with outperformance)
- `wRC+` r=+0.161 (n=4209)
- `ISO` r=+0.128 (n=4209)
- `xwOBA` r=+0.111 (n=4209)
- `Salary` r=+0.079 (n=4217)
- `Barrel%` r=+0.049 (n=4208)

### Pitcher Predictors
- `K%` r=+0.263 (n=439)
- `Salary` r=+0.226 (n=440)
- `xFIP` r=-0.248 (n=439)
- `Win Prob` r=+0.077 (n=440)
- `Stuff+` r=+0.170 (n=439)

### Actionable Rules
- **Hitter leverage**: ISO > 0.200 AND own < 10% -> 27% leverage rate (+17pp vs base)
- **Hitter leverage**: wRC+ > 120 AND own < 10% -> 22% leverage rate (+12pp vs base)
- **Hitter trap**: Own > 20% AND K% > 0.28 -> 68% chalk trap rate
- **Pitcher leverage**: K% > 0.25 AND own < 15% -> 33% leverage rate (+17pp vs base)
- **Pitcher leverage**: Stuff+ > 105 AND own < 15% -> 31% leverage rate (+14pp vs base)
- **Pitcher trap**: Own > 25% AND Stuff+ < 100 -> 59% chalk trap rate
- **Pitcher trap**: Own > 25% AND xFIP > 4.0 -> 56% chalk trap rate

## Winner Pattern Analysis — 2026-05-20 (63 contests, 866,660 entries)
- Top 1% profile: 136% total own, 3.8 booms, 0.7 busts, 49 pitcher pts
- Target: ownership 100-150%, 3+ booms, <1 bust, 25+ pitcher pts

## Research Findings — 2026-05-14, 2026-05-15, 2026-05-16, 2026-05-17, 2026-05-18, 2026-05-19, 2026-05-20, 2026-05-21

**Projection**: MAE=5.99, Bias=+0.91, Hitter MAE=5.74, Pitcher MAE=8.19
**Ownership**: MAE=5.33%, Bias=+4.55%
**Pool**: MAE=34.57, Bias=+30.05
**Contest**: Winner=183.59921875, Top1%=151.09999984375

### Predictive Diagnostics

**Hitter Missing Predictors** (correlated with error but not in model):
- `pull_pct` r=-0.127 (n=714)
- `oppo_pct` r=+0.097 (n=714)
- `cent_pct` r=+0.078 (n=714)
- `fb_pct` r=-0.075 (n=716)
- `gb_pct` r=+0.054 (n=716)

**Pitcher Missing Predictors** (correlated with error but not in model):
- `whip` r=+0.152 (n=76)
- `avg` r=+0.145 (n=76)
- `k9` r=+0.125 (n=76)
- `era` r=+0.123 (n=76)
- `gb_pct` r=+0.091 (n=76)

**Opposing Lineup Factors** (for pitcher projections):
- `opp_gb_pct` r=-0.197 (n=87)
- `opp_avg_ev` r=+0.181 (n=87)
- `opp_o_swing_pct` r=-0.156 (n=87)
- `opp_k_pct` r=+0.145 (n=87)
- `opp_iso` r=+0.139 (n=87)

**Optimal Context Weights**: Vegas=80% Park=5% Weather=15% (saves 0.00 MAE)

**Archetype Biases:**
- Power (ISO>.200): over-projected by 1.3 pts (n=159)
- Speed (SB pace>15): over-projected by 1.0 pts (n=216)

**Recommendations:**
- PROJECTION: increase context multiplier weights — hitter bias is -0.97 pts
- OWNERSHIP: increase baseline ownership estimates — bias is +4.5%
- POOL: Best performing stack config is 5-0 — increase its weight in STACK_CONFIGS
- POOL: Projections have 9.3 pt spread — use projection rank as primary sort for portfolio selection
- CONTEST: Avg Top 1% threshold is 151.1 pts across 64 contests
- CONTEST: Avg cash line is 111.5 pts — pool floor should exceed this
- CONTEST: Avg winner scores 183.6 pts — need high-ceiling correlated stacks
- OWNERSHIP: increase baseline estimates — bias is +4.5%
- OWNERSHIP: MAE is 5.3% — needs significant model improvement
- TRACKING: Run this analysis daily to build sample size — patterns stabilize after 2+ weeks

## Sim Validation — 2026-05-20

### Distribution Calibration
- **Hitters**: 89.9% in P10-P90 [WARN] (below floor=1.5%, above ceiling=8.6%)
- **Pitchers**: 75.9% in P10-P90 [PASS] (below floor=13.8%, above ceiling=10.3%)

### Projection Accuracy
- Overall: MAE=5.69, Bias=+1.23, r=0.381
- Hitters: MAE=5.45 [PASS]
- Pitchers: MAE=7.92 [PASS]

### Pitcher Components
- IP: MAE=0.86, Bias=-0.57
- Ks: MAE=1.7, Bias=-0.05
- ER: MAE=1.5, Bias=-0.20

### Multiplier Effectiveness


## Ownership Calibration — 4 large-field contests (≥1000 entries)

- **Matched players**: 412 (37 0%-actual ghosts excluded)
- **Bias**: +5.80% (positive = over-project ownership)
- **MAE**: 6.57%
- **Correlation**: r=0.617

- Chalk (>20% actual): n=12, bias=-1.09%, MAE=12.93%
- Mid (5-20% actual): n=95, bias=+5.40%, MAE=6.95%
- Low (<5% actual): n=305, bias=+6.19%, MAE=6.20%

**Over-projected:**
- Jacob deGrom: proj=41.8% actual=0.2%
- Nathan Eovaldi: proj=34.4% actual=2.9%
- Jose Soriano: proj=31.3% actual=0.7%
- Shota Imanaga: proj=38.3% actual=9.4%
- Jesus Luzardo: proj=49.8% actual=21.9%

**Under-projected:**
- Sandy Alcantara: proj=14.9% actual=37.9%
- Eury Perez: proj=20.4% actual=40.9%
- Pete Crow-Armstrong: proj=9.1% actual=23.6%
- Dylan Cease: proj=33.8% actual=47.3%
- Michael Busch: proj=8.4% actual=21.0%

## Leverage Analysis — 2026-05-21 (45 contests, 2882 players)

**Dataset**: 2599 hitters, 283 pitchers across 37 dates
**Leverage hits**: 324 (11.2%) | **Chalk traps**: 65 (2.3%) | **Ceiling hits**: 278 (9.6%)

### Hitter Predictors (correlation with outperformance)
- `wRC+` r=+0.148 (n=2592)
- `ISO` r=+0.131 (n=2592)
- `xwOBA` r=+0.094 (n=2590)
- `Salary` r=+0.083 (n=2599)
- `Barrel%` r=+0.049 (n=2592)

### Pitcher Predictors
- `K%` r=+0.260 (n=283)
- `Salary` r=+0.216 (n=283)
- `xFIP` r=-0.258 (n=283)
- `Win Prob` r=+0.075 (n=283)
- `Stuff+` r=+0.216 (n=283)

### Actionable Rules
- **Hitter leverage**: ISO > 0.200 AND own < 10% -> 27% leverage rate (+17pp vs base)
- **Hitter leverage**: wRC+ > 120 AND own < 10% -> 22% leverage rate (+12pp vs base)
- **Hitter trap**: Own > 20% AND K% > 0.28 -> 68% chalk trap rate
- **Pitcher leverage**: K% > 0.25 AND own < 15% -> 33% leverage rate (+17pp vs base)
- **Pitcher leverage**: Stuff+ > 105 AND own < 15% -> 31% leverage rate (+14pp vs base)
- **Pitcher trap**: Own > 25% AND Stuff+ < 100 -> 59% chalk trap rate
- **Pitcher trap**: Own > 25% AND xFIP > 4.0 -> 56% chalk trap rate

## Winner Pattern Analysis — 2026-05-21 (64 contests, 873,786 entries)
- Top 1% profile: 136% total own, 3.8 booms, 0.7 busts, 49 pitcher pts
- Target: ownership 100-150%, 3+ booms, <1 bust, 25+ pitcher pts

## Research Findings — 2026-05-15, 2026-05-16, 2026-05-17, 2026-05-18, 2026-05-19, 2026-05-20, 2026-05-21, 2026-05-22

**Projection**: MAE=5.95, Bias=+0.96, Hitter MAE=5.70, Pitcher MAE=8.22
**Ownership**: MAE=5.33%, Bias=+4.55%
**Pool**: MAE=51.98, Bias=+49.90
**Contest**: Winner=183.47615384615386, Top1%=151.04307676923077

### Predictive Diagnostics

**Hitter Missing Predictors** (correlated with error but not in model):
- `pull_pct` r=-0.117 (n=821)
- `oppo_pct` r=+0.089 (n=821)
- `fb_pct` r=-0.077 (n=823)
- `cent_pct` r=+0.073 (n=821)
- `gb_pct` r=+0.053 (n=823)

**Pitcher Missing Predictors** (correlated with error but not in model):
- `whip` r=+0.188 (n=90)
- `avg` r=+0.141 (n=90)
- `era` r=+0.113 (n=90)
- `ip` r=-0.105 (n=90)
- `bb9` r=+0.100 (n=90)

**Opposing Lineup Factors** (for pitcher projections):
- `opp_gb_pct` r=-0.218 (n=101)
- `opp_o_swing_pct` r=-0.173 (n=101)
- `opp_avg_ev` r=+0.137 (n=101)
- `opp_iso` r=+0.134 (n=101)
- `opp_k_pct` r=+0.129 (n=101)

**Optimal Context Weights**: Vegas=80% Park=5% Weather=15% (saves 0.00 MAE)

**Archetype Biases:**
- Power (ISO>.200): over-projected by 1.6 pts (n=187)
- Strikeout (K%>28%): over-projected by 0.9 pts (n=139)
- Speed (SB pace>15): over-projected by 1.4 pts (n=244)

**Recommendations:**
- PROJECTION: increase context multiplier weights — hitter bias is -1.14 pts
- OWNERSHIP: increase baseline ownership estimates — bias is +4.5%
- POOL: Best performing stack config is 4-4 — increase its weight in STACK_CONFIGS
- POOL: Projection spread only -8.5 pts — diversify selection, don't over-rely on projection ranking
- CONTEST: Avg Top 1% threshold is 151.0 pts across 65 contests
- CONTEST: Avg cash line is 111.6 pts — pool floor should exceed this
- CONTEST: Avg winner scores 183.5 pts — need high-ceiling correlated stacks
- OWNERSHIP: increase baseline estimates — bias is +4.5%
- OWNERSHIP: MAE is 5.3% — needs significant model improvement
- TRACKING: Run this analysis daily to build sample size — patterns stabilize after 2+ weeks

## Sim Validation — 2026-05-21

### Distribution Calibration
- **Hitters**: 91.2% in P10-P90 [WARN] (below floor=1.6%, above ceiling=7.2%)
- **Pitchers**: 50.0% in P10-P90 [FAIL] (below floor=0.0%, above ceiling=50.0%)

### Projection Accuracy
- Overall: MAE=5.76, Bias=+1.23, r=0.466
- Hitters: MAE=5.46 [PASS]
- Pitchers: MAE=8.41 [WARN]

### Pitcher Components
- IP: MAE=1.51, Bias=-1.51
- Ks: MAE=2.43, Bias=-1.93
- ER: MAE=1.02, Bias=+0.08

### Multiplier Effectiveness

## Slate Review — 2026-05-21 / main

- **Pool**: 10000 lineups, avg actual=83.6, cash line=82.7, GPP line=140.7, best=166.5
- **Proj accuracy**: r=0.274, MAE=21.6, bias=+12.8
- **Overlap**: 0/20 top-by-proj were actual winners
- **Best strategy**: PMS (highest avg_pms)
- **Top stack**: ATL (avg actual=106.7, 14.6% exposure)
- **Biggest bust**: Aaron Judge (proj=11.3, actual=0.0, 21% exp)
- **Biggest missed opp**: Kyle Stowers (actual=28.0, 14.4% exp)


## Ownership Calibration — 4 large-field contests (≥1000 entries)

- **Matched players**: 190 (14 0%-actual ghosts excluded)
- **Bias**: +6.37% (positive = over-project ownership)
- **MAE**: 7.11%
- **Correlation**: r=0.637

- Chalk (>20% actual): n=5, bias=-6.56%, MAE=16.48%
- Mid (5-20% actual): n=33, bias=+8.17%, MAE=8.86%
- Low (<5% actual): n=152, bias=+6.41%, MAE=6.42%

**Over-projected:**
- Jose Soriano: proj=38.8% actual=0.7%
- Nolan McLean: proj=36.8% actual=9.7%
- Framber Valdez: proj=28.1% actual=6.0%
- Mitch Keller: proj=22.7% actual=0.8%
- Freddy Peralta: proj=31.0% actual=9.3%

**Under-projected:**
- Sandy Alcantara: proj=14.4% actual=37.9%
- Eury Perez: proj=20.4% actual=40.9%
- Dylan Cease: proj=33.8% actual=47.3%
- Grant Holmes: proj=12.2% actual=17.0%
- Agustin Ramirez: proj=10.5% actual=13.1%

## Leverage Analysis — 2026-05-22 (46 contests, 1966 players)

**Dataset**: 1788 hitters, 178 pitchers across 38 dates
**Leverage hits**: 199 (10.1%) | **Chalk traps**: 54 (2.7%) | **Ceiling hits**: 194 (9.9%)

### Hitter Predictors (correlation with outperformance)
- `wRC+` r=+0.160 (n=1783)
- `ISO` r=+0.128 (n=1783)
- `xwOBA` r=+0.114 (n=1783)
- `Salary` r=+0.106 (n=1788)
- `Barrel%` r=+0.055 (n=1783)

### Pitcher Predictors
- `K%` r=+0.260 (n=178)
- `Salary` r=+0.184 (n=178)
- `xFIP` r=-0.109 (n=178)
- `Win Prob` r=+0.090 (n=178)
- `Stuff+` r=+0.113 (n=177)

### Actionable Rules
- **Hitter leverage**: ISO > 0.200 AND own < 10% -> 27% leverage rate (+17pp vs base)
- **Hitter leverage**: wRC+ > 120 AND own < 10% -> 22% leverage rate (+12pp vs base)
- **Hitter trap**: Own > 20% AND K% > 0.28 -> 68% chalk trap rate
- **Pitcher leverage**: K% > 0.25 AND own < 15% -> 33% leverage rate (+17pp vs base)
- **Pitcher leverage**: Stuff+ > 105 AND own < 15% -> 31% leverage rate (+14pp vs base)
- **Pitcher trap**: Own > 25% AND Stuff+ < 100 -> 59% chalk trap rate
- **Pitcher trap**: Own > 25% AND xFIP > 4.0 -> 56% chalk trap rate

## Winner Pattern Analysis — 2026-05-22 (65 contests, 891,595 entries)
- Top 1% profile: 138% total own, 3.8 booms, 0.7 busts, 49 pitcher pts
- Target: ownership 100-150%, 3+ booms, <1 bust, 25+ pitcher pts


## Ownership Calibration — 4 large-field contests (≥1000 entries)

- **Matched players**: 411 (37 0%-actual ghosts excluded)
- **Bias**: +5.89% (positive = over-project ownership)
- **MAE**: 6.68%
- **Correlation**: r=0.602

- Chalk (>20% actual): n=12, bias=-1.24%, MAE=12.99%
- Mid (5-20% actual): n=94, bias=+5.61%, MAE=7.18%
- Low (<5% actual): n=305, bias=+6.26%, MAE=6.28%

**Over-projected:**
- Jacob deGrom: proj=45.3% actual=0.2%
- Jose Soriano: proj=38.8% actual=0.7%
- Nathan Eovaldi: proj=36.7% actual=2.9%
- Cristopher Sanchez: proj=45.5% actual=15.0%
- Shota Imanaga: proj=38.3% actual=9.4%

**Under-projected:**
- Sandy Alcantara: proj=14.4% actual=37.9%
- Eury Perez: proj=19.3% actual=40.9%
- Pete Crow-Armstrong: proj=9.3% actual=23.6%
- Dylan Cease: proj=33.8% actual=47.3%
- Michael Busch: proj=8.6% actual=21.0%

## Leverage Analysis — 2026-05-23 (46 contests, 3028 players)

**Dataset**: 2743 hitters, 285 pitchers across 38 dates
**Leverage hits**: 347 (11.5%) | **Chalk traps**: 56 (1.8%) | **Ceiling hits**: 317 (10.5%)

### Hitter Predictors (correlation with outperformance)
- `wRC+` r=+0.157 (n=2742)
- `ISO` r=+0.141 (n=2742)
- `xwOBA` r=+0.119 (n=2742)
- `Salary` r=+0.101 (n=2743)
- `Barrel%` r=+0.071 (n=2742)

### Pitcher Predictors
- `K%` r=+0.215 (n=285)
- `Salary` r=+0.227 (n=285)
- `xFIP` r=-0.202 (n=285)
- `Win Prob` r=+0.037 (n=285)
- `Stuff+` r=+0.016 (n=285)

### Actionable Rules
- **Hitter leverage**: ISO > 0.200 AND own < 10% -> 27% leverage rate (+17pp vs base)
- **Hitter leverage**: wRC+ > 120 AND own < 10% -> 22% leverage rate (+12pp vs base)
- **Hitter trap**: Own > 20% AND K% > 0.28 -> 68% chalk trap rate
- **Pitcher leverage**: K% > 0.25 AND own < 15% -> 33% leverage rate (+17pp vs base)
- **Pitcher leverage**: Stuff+ > 105 AND own < 15% -> 31% leverage rate (+14pp vs base)
- **Pitcher trap**: Own > 25% AND Stuff+ < 100 -> 59% chalk trap rate
- **Pitcher trap**: Own > 25% AND xFIP > 4.0 -> 56% chalk trap rate

## Research Findings — 2026-05-17, 2026-05-18, 2026-05-19, 2026-05-20, 2026-05-21, 2026-05-22, 2026-05-23, 2026-05-24

**Projection**: MAE=5.88, Bias=+1.18, Hitter MAE=5.62, Pitcher MAE=8.22
**Pool**: MAE=66.55, Bias=+66.43
**Contest**: Winner=183.4149253731343, Top1%=150.8820894029851

### Predictive Diagnostics

**Hitter Missing Predictors** (correlated with error but not in model):
- `pull_pct` r=-0.091 (n=1259)
- `slg` r=-0.065 (n=1262)
- `oppo_pct` r=+0.063 (n=1259)
- `ops` r=-0.062 (n=1262)
- `cent_pct` r=+0.062 (n=1259)

**Opposing Lineup Factors** (for pitcher projections):
- `opp_avg_ev` r=+0.141 (n=157)
- `opp_bb_pct` r=+0.132 (n=157)
- `opp_o_swing_pct` r=-0.130 (n=157)
- `opp_barrel_pct` r=+0.113 (n=157)

**Optimal Context Weights**: Vegas=80% Park=5% Weather=15% (saves 0.00 MAE)

**Archetype Biases:**
- Power (ISO>.200): over-projected by 2.0 pts (n=287)
- Contact (K%<15%): over-projected by 1.3 pts (n=345)
- Strikeout (K%>28%): over-projected by 1.1 pts (n=204)
- Speed (SB pace>15): over-projected by 1.7 pts (n=380)

**Recommendations:**
- PROJECTION: increase context multiplier weights — hitter bias is -1.43 pts
- POOL: Best performing stack config is 4-4 — increase its weight in STACK_CONFIGS
- POOL: Projection spread only -6.3 pts — diversify selection, don't over-rely on projection ranking
- CONTEST: Avg Top 1% threshold is 150.9 pts across 67 contests
- CONTEST: Avg cash line is 111.4 pts — pool floor should exceed this
- CONTEST: Avg winner scores 183.4 pts — need high-ceiling correlated stacks
- TRACKING: Run this analysis daily to build sample size — patterns stabilize after 2+ weeks

## Sim Validation — 2026-05-23

### Distribution Calibration
- **Hitters**: 89.2% in P10-P90 [WARN] (below floor=2.8%, above ceiling=8.0%)
- **Pitchers**: 67.9% in P10-P90 [WARN] (below floor=7.1%, above ceiling=25.0%)

### Projection Accuracy
- Overall: MAE=5.88, Bias=+1.45, r=0.348
- Hitters: MAE=5.65 [PASS]
- Pitchers: MAE=7.98 [PASS]

### Pitcher Components
- IP: MAE=1.15, Bias=-0.71
- Ks: MAE=1.97, Bias=-0.71
- ER: MAE=1.53, Bias=-0.01

### Multiplier Effectiveness

## Slate Review — 2026-05-23 / main

- **Pool**: 9000 lineups, avg actual=75.0, cash line=73.1, GPP line=142.5, best=178.8
- **Proj accuracy**: r=-0.026, MAE=28.8, bias=+20.6
- **Overlap**: 0/20 top-by-proj were actual winners
- **Best strategy**: PMS (highest avg_pms)
- **Top stack**: SF (avg actual=112.6, 9.7% exposure)
- **Biggest bust**: George Kirby (proj=16.5, actual=8.1, 17% exp)
- **Biggest missed opp**: Stephen Kolek (actual=31.2, 6.4% exp)

## Slate Review — 2026-05-23 / night

- **Pool**: 500 lineups, avg actual=84.7, cash line=83.3, GPP line=141.1, best=168.1
- **Proj accuracy**: r=0.208, MAE=18.6, bias=+5.9
- **Overlap**: 1/20 top-by-proj were actual winners
- **Best strategy**: PMS (highest avg_pms)
- **Top stack**: STL (avg actual=111.8, 11.0% exposure)
- **Biggest bust**: Chase Petty (proj=9.2, actual=1.1, 25% exp)
- **Biggest missed opp**: Teoscar Hernández (actual=34.0, 7.6% exp)

## Slate Review — 2026-05-23 / turbo

- **Pool**: 6000 lineups, avg actual=73.3, cash line=71.2, GPP line=145.2, best=180.0
- **Proj accuracy**: r=-0.255, MAE=32.7, bias=+23.1
- **Overlap**: 0/20 top-by-proj were actual winners
- **Best strategy**: PMS (highest avg_pms)
- **Top stack**: TOR (avg actual=105.9, 3.6% exposure)
- **Biggest bust**: Paul Skenes (proj=21.5, actual=1.3, 68% exp)
- **Biggest missed opp**: Bryan Torres (actual=21.0, 9.5% exp)


## Ownership Calibration — 4 large-field contests (≥1000 entries)

- **Matched players**: 411 (37 0%-actual ghosts excluded)
- **Bias**: +5.90% (positive = over-project ownership)
- **MAE**: 6.68%
- **Correlation**: r=0.602

- Chalk (>20% actual): n=12, bias=-1.21%, MAE=13.01%
- Mid (5-20% actual): n=94, bias=+5.62%, MAE=7.18%
- Low (<5% actual): n=305, bias=+6.27%, MAE=6.28%

**Over-projected:**
- Jacob deGrom: proj=45.3% actual=0.2%
- Jose Soriano: proj=38.8% actual=0.7%
- Nathan Eovaldi: proj=36.7% actual=2.9%
- Cristopher Sanchez: proj=45.5% actual=15.0%
- Shota Imanaga: proj=38.3% actual=9.4%

**Under-projected:**
- Sandy Alcantara: proj=14.4% actual=37.9%
- Eury Perez: proj=19.3% actual=40.9%
- Pete Crow-Armstrong: proj=9.4% actual=23.6%
- Dylan Cease: proj=33.8% actual=47.3%
- Michael Busch: proj=8.6% actual=21.0%

## Leverage Analysis — 2026-05-24 (46 contests, 3028 players)

**Dataset**: 2743 hitters, 285 pitchers across 38 dates
**Leverage hits**: 347 (11.5%) | **Chalk traps**: 56 (1.8%) | **Ceiling hits**: 317 (10.5%)

### Hitter Predictors (correlation with outperformance)
- `wRC+` r=+0.157 (n=2742)
- `ISO` r=+0.141 (n=2742)
- `xwOBA` r=+0.119 (n=2742)
- `Salary` r=+0.101 (n=2743)
- `Barrel%` r=+0.071 (n=2742)

### Pitcher Predictors
- `K%` r=+0.215 (n=285)
- `Salary` r=+0.227 (n=285)
- `xFIP` r=-0.202 (n=285)
- `Win Prob` r=+0.037 (n=285)
- `Stuff+` r=+0.016 (n=285)

### Actionable Rules
- **Hitter leverage**: ISO > 0.200 AND own < 10% -> 27% leverage rate (+17pp vs base)
- **Hitter leverage**: wRC+ > 120 AND own < 10% -> 22% leverage rate (+12pp vs base)
- **Hitter trap**: Own > 20% AND K% > 0.28 -> 68% chalk trap rate
- **Pitcher leverage**: K% > 0.25 AND own < 15% -> 33% leverage rate (+17pp vs base)
- **Pitcher leverage**: Stuff+ > 105 AND own < 15% -> 31% leverage rate (+14pp vs base)
- **Pitcher trap**: Own > 25% AND Stuff+ < 100 -> 59% chalk trap rate
- **Pitcher trap**: Own > 25% AND xFIP > 4.0 -> 56% chalk trap rate

## Winner Pattern Analysis — 2026-05-24 (67 contests, 910,651 entries)
- Top 1% profile: 139% total own, 3.8 booms, 0.7 busts, 49 pitcher pts
- Target: ownership 100-150%, 3+ booms, <1 bust, 25+ pitcher pts

## Research Findings — 2026-05-17, 2026-05-18, 2026-05-19, 2026-05-20, 2026-05-21, 2026-05-22, 2026-05-23, 2026-05-24

**Projection**: MAE=5.80, Bias=+1.25, Hitter MAE=5.54, Pitcher MAE=8.15
**Pool**: MAE=72.03, Bias=+72.01
**Contest**: Winner=183.04264705882352, Top1%=150.48235286764705

### Predictive Diagnostics

**Hitter Missing Predictors** (correlated with error but not in model):
- `pull_pct` r=-0.084 (n=1488)
- `cent_pct` r=+0.060 (n=1488)
- `slg` r=-0.059 (n=1491)
- `oppo_pct` r=+0.055 (n=1488)
- `ops` r=-0.051 (n=1491)

**Pitcher Missing Predictors** (correlated with error but not in model):
- `gb_pct` r=+0.120 (n=165)
- `w` r=-0.090 (n=165)
- `avg` r=+0.084 (n=165)

**Opposing Lineup Factors** (for pitcher projections):
- `opp_avg_ev` r=+0.123 (n=186)
- `opp_o_swing_pct` r=-0.105 (n=186)

**Optimal Context Weights**: Vegas=80% Park=5% Weather=15% (saves 0.00 MAE)

**Archetype Biases:**
- Power (ISO>.200): over-projected by 2.1 pts (n=339)
- Contact (K%<15%): over-projected by 1.2 pts (n=405)
- Strikeout (K%>28%): over-projected by 1.4 pts (n=242)
- Speed (SB pace>15): over-projected by 1.8 pts (n=452)

**Recommendations:**
- PROJECTION: increase context multiplier weights — hitter bias is -1.49 pts
- POOL: Best performing stack config is 4-0 — increase its weight in STACK_CONFIGS
- POOL: Projections have 9.2 pt spread — use projection rank as primary sort for portfolio selection
- CONTEST: Avg Top 1% threshold is 150.5 pts across 68 contests
- CONTEST: Avg cash line is 111.0 pts — pool floor should exceed this
- CONTEST: Avg winner scores 183.0 pts — need high-ceiling correlated stacks
- TRACKING: Run this analysis daily to build sample size — patterns stabilize after 2+ weeks

## Sim Validation — 2026-05-24

### Distribution Calibration
- **Hitters**: 94.1% in P10-P90 [FAIL] (below floor=0.8%, above ceiling=5.1%)
- **Pitchers**: 69.0% in P10-P90 [WARN] (below floor=17.2%, above ceiling=13.8%)

### Projection Accuracy
- Overall: MAE=5.4, Bias=+1.61, r=0.421
- Hitters: MAE=5.13 [PASS]
- Pitchers: MAE=7.79 [PASS]

### Pitcher Components
- IP: MAE=1.33, Bias=-0.58
- Ks: MAE=1.64, Bias=+0.19
- ER: MAE=1.63, Bias=-0.00

### Multiplier Effectiveness

## Slate Review — 2026-05-24 / afternoon

- **Pool**: 6000 lineups, avg actual=69.4, cash line=66.6, GPP line=143.2, best=172.1
- **Proj accuracy**: r=0.386, MAE=29.8, bias=+22.7
- **Overlap**: 1/20 top-by-proj were actual winners
- **Best strategy**: PMS (highest avg_pms)
- **Top stack**: SF (avg actual=99.0, 11.3% exposure)
- **Biggest bust**: Noah Schultz (proj=10.3, actual=-5.2, 26% exp)
- **Biggest missed opp**: Rafael Devers (actual=27.0, 5.8% exp)

## Slate Review — 2026-05-24 / early

- **Pool**: 4500 lineups, avg actual=66.0, cash line=65.4, GPP line=103.4, best=129.8
- **Proj accuracy**: r=0.091, MAE=31.7, bias=+31.0
- **Overlap**: 0/20 top-by-proj were actual winners
- **Best strategy**: PMS (highest avg_pms)
- **Top stack**: DET (avg actual=79.7, 12.6% exposure)
- **Biggest bust**: George Springer (proj=9.9, actual=2.0, 47% exp)
- **Biggest missed opp**: Colton Cowser (actual=18.0, 5.9% exp)

## Slate Review — 2026-05-24 / main

- **Pool**: 20000 lineups, avg actual=69.5, cash line=69.8, GPP line=121.3, best=153.7
- **Proj accuracy**: r=0.349, MAE=29.6, bias=+27.5
- **Overlap**: 0/20 top-by-proj were actual winners
- **Best strategy**: PMS (highest avg_pms)
- **Top stack**: BOS (avg actual=89.5, 7.5% exposure)
- **Biggest bust**: Bryan Woo (proj=17.3, actual=5.7, 24% exp)
- **Biggest missed opp**: Nick Allen (actual=31.0, 1.1% exp)

## Slate Review — 2026-05-24 / turbo

- **Pool**: 4500 lineups, avg actual=83.1, cash line=83.1, GPP line=120.6, best=142.7
- **Proj accuracy**: r=0.175, MAE=18.7, bias=+15.7
- **Overlap**: 0/20 top-by-proj were actual winners
- **Best strategy**: PMS (highest avg_pms)
- **Top stack**: KC (avg actual=89.5, 25.3% exposure)
- **Biggest bust**: Bryan Woo (proj=17.3, actual=5.7, 38% exp)
- **Biggest missed opp**: Nick Allen (actual=31.0, 3.7% exp)


## Ownership Calibration — 4 large-field contests (≥1000 entries)

- **Matched players**: 411 (37 0%-actual ghosts excluded)
- **Bias**: +5.94% (positive = over-project ownership)
- **MAE**: 6.70%
- **Correlation**: r=0.606

- Chalk (>20% actual): n=12, bias=-0.94%, MAE=12.72%
- Mid (5-20% actual): n=94, bias=+5.65%, MAE=7.20%
- Low (<5% actual): n=305, bias=+6.29%, MAE=6.31%

**Over-projected:**
- Jacob deGrom: proj=45.3% actual=0.2%
- Jose Soriano: proj=38.8% actual=0.7%
- Nathan Eovaldi: proj=36.7% actual=2.9%
- Cristopher Sanchez: proj=45.5% actual=15.0%
- Shota Imanaga: proj=39.8% actual=9.4%

**Under-projected:**
- Sandy Alcantara: proj=14.4% actual=37.9%
- Eury Perez: proj=19.3% actual=40.9%
- Pete Crow-Armstrong: proj=9.7% actual=23.6%
- Michael Busch: proj=8.6% actual=21.0%
- Dylan Cease: proj=36.9% actual=47.3%

## Leverage Analysis — 2026-05-24 (47 contests, 3883 players)

**Dataset**: 3515 hitters, 368 pitchers across 39 dates
**Leverage hits**: 443 (11.4%) | **Chalk traps**: 99 (2.5%) | **Ceiling hits**: 392 (10.1%)

### Hitter Predictors (correlation with outperformance)
- `wRC+` r=+0.114 (n=3506)
- `ISO` r=+0.092 (n=3506)
- `Salary` r=+0.066 (n=3515)
- `xwOBA` r=+0.061 (n=3506)
- `Barrel%` r=+0.027 (n=3506)

### Pitcher Predictors
- `K%` r=+0.196 (n=367)
- `Salary` r=+0.169 (n=368)
- `xFIP` r=-0.184 (n=367)
- `Win Prob` r=+0.104 (n=368)
- `Stuff+` r=+0.140 (n=367)

### Actionable Rules
- **Hitter leverage**: ISO > 0.200 AND own < 10% -> 27% leverage rate (+17pp vs base)
- **Hitter leverage**: wRC+ > 120 AND own < 10% -> 22% leverage rate (+12pp vs base)
- **Hitter trap**: Own > 20% AND K% > 0.28 -> 68% chalk trap rate
- **Pitcher leverage**: K% > 0.25 AND own < 15% -> 33% leverage rate (+17pp vs base)
- **Pitcher leverage**: Stuff+ > 105 AND own < 15% -> 31% leverage rate (+14pp vs base)
- **Pitcher trap**: Own > 25% AND Stuff+ < 100 -> 59% chalk trap rate
- **Pitcher trap**: Own > 25% AND xFIP > 4.0 -> 56% chalk trap rate

## Winner Pattern Analysis — 2026-05-24 (68 contests, 946,017 entries)
- Top 1% profile: 138% total own, 3.7 booms, 0.7 busts, 48 pitcher pts
- Target: ownership 100-150%, 3+ booms, <1 bust, 25+ pitcher pts

## Research Findings — 2026-05-20, 2026-05-21, 2026-05-22, 2026-05-23, 2026-05-24, 2026-05-25, 2026-05-26, 2026-05-27

**Projection**: MAE=5.76, Bias=+1.20, Hitter MAE=5.51, Pitcher MAE=7.98
**Pool**: MAE=55.98, Bias=+55.82
**Contest**: Winner=183.981884057971, Top1%=151.26014485507244

### Predictive Diagnostics

**Hitter Missing Predictors** (correlated with error but not in model):
- `slg` r=-0.056 (n=1463)

**Pitcher Missing Predictors** (correlated with error but not in model):
- `k9` r=+0.112 (n=169)

**Optimal Context Weights**: Vegas=80% Park=5% Weather=15% (saves 0.00 MAE)

**Archetype Biases:**
- Power (ISO>.200): over-projected by 2.1 pts (n=331)
- Contact (K%<15%): over-projected by 1.9 pts (n=417)
- Strikeout (K%>28%): over-projected by 1.7 pts (n=238)
- Speed (SB pace>15): over-projected by 1.7 pts (n=447)

**Recommendations:**
- PROJECTION: increase context multiplier weights — hitter bias is -1.46 pts
- POOL: Best performing stack config is 4-0 — increase its weight in STACK_CONFIGS
- POOL: Projections have 9.3 pt spread — use projection rank as primary sort for portfolio selection
- CONTEST: Avg Top 1% threshold is 151.3 pts across 69 contests
- CONTEST: Avg cash line is 111.6 pts — pool floor should exceed this
- CONTEST: Avg winner scores 184.0 pts — need high-ceiling correlated stacks
- PROJECTION: pitcher_mult is hurting accuracy (r=-0.115) — reduce its weight or cap its range
- TRACKING: Run this analysis daily to build sample size — patterns stabilize after 2+ weeks

## Sim Validation — 2026-05-26

### Distribution Calibration
- **Hitters**: 88.5% in P10-P90 [WARN] (below floor=1.5%, above ceiling=10.0%)
- **Pitchers**: 62.1% in P10-P90 [FAIL] (below floor=27.6%, above ceiling=10.3%)

### Projection Accuracy
- Overall: MAE=6.23, Bias=+0.04, r=0.318
- Hitters: MAE=5.96 [PASS]
- Pitchers: MAE=8.76 [WARN]

### Pitcher Components
- IP: MAE=1.04, Bias=-0.46
- Ks: MAE=1.63, Bias=-0.13
- ER: MAE=2.13, Bias=-1.21

### Multiplier Effectiveness

## Slate Review — 2026-05-26 / main

- **Pool**: 20000 lineups, avg actual=100.1, cash line=98.4, GPP line=176.8, best=232.2
- **Proj accuracy**: r=0.283, MAE=24.1, bias=-0.1
- **Overlap**: 0/20 top-by-proj were actual winners
- **Best strategy**: PMS (highest avg_pms)
- **Top stack**: NYY (avg actual=142.6, 6.2% exposure)
- **Biggest bust**: David Peterson (proj=12.9, actual=-1.1, 15% exp)
- **Biggest missed opp**: Amed Rosario (actual=40.0, 4.4% exp)


## Ownership Calibration — 4 large-field contests (≥1000 entries)

- **Matched players**: 411 (38 0%-actual ghosts excluded)
- **Bias**: +5.91% (positive = over-project ownership)
- **MAE**: 6.70%
- **Correlation**: r=0.603

- Chalk (>20% actual): n=12, bias=-0.59%, MAE=13.16%
- Mid (5-20% actual): n=94, bias=+5.54%, MAE=7.21%
- Low (<5% actual): n=305, bias=+6.27%, MAE=6.29%

**Over-projected:**
- Jacob deGrom: proj=45.3% actual=0.2%
- Jose Soriano: proj=38.8% actual=0.7%
- Nathan Eovaldi: proj=36.7% actual=2.9%
- Cristopher Sanchez: proj=45.5% actual=15.0%
- Shota Imanaga: proj=39.8% actual=9.4%

**Under-projected:**
- Sandy Alcantara: proj=13.6% actual=37.9%
- Eury Perez: proj=19.3% actual=40.9%
- Pete Crow-Armstrong: proj=9.9% actual=23.6%
- Michael Busch: proj=8.6% actual=21.0%
- Dylan Cease: proj=36.9% actual=47.3%

## Leverage Analysis — 2026-05-27 (48 contests, 5029 players)

**Dataset**: 4562 hitters, 467 pitchers across 40 dates
**Leverage hits**: 528 (10.5%) | **Chalk traps**: 118 (2.3%) | **Ceiling hits**: 483 (9.6%)

### Hitter Predictors (correlation with outperformance)
- `wRC+` r=+0.143 (n=4550)
- `ISO` r=+0.120 (n=4550)
- `xwOBA` r=+0.090 (n=4550)
- `Salary` r=+0.087 (n=4562)
- `Barrel%` r=+0.038 (n=4549)

### Pitcher Predictors
- `K%` r=+0.258 (n=467)
- `Salary` r=+0.196 (n=467)
- `xFIP` r=-0.120 (n=467)
- `Win Prob` r=+0.131 (n=467)
- `Stuff+` r=+0.167 (n=466)

### Actionable Rules
- **Hitter leverage**: ISO > 0.200 AND own < 10% -> 27% leverage rate (+17pp vs base)
- **Hitter leverage**: wRC+ > 120 AND own < 10% -> 22% leverage rate (+12pp vs base)
- **Hitter trap**: Own > 20% AND K% > 0.28 -> 68% chalk trap rate
- **Pitcher leverage**: K% > 0.25 AND own < 15% -> 33% leverage rate (+17pp vs base)
- **Pitcher leverage**: Stuff+ > 105 AND own < 15% -> 31% leverage rate (+14pp vs base)
- **Pitcher trap**: Own > 25% AND Stuff+ < 100 -> 59% chalk trap rate
- **Pitcher trap**: Own > 25% AND xFIP > 4.0 -> 56% chalk trap rate

## Winner Pattern Analysis — 2026-05-27 (69 contests, 991,560 entries)
- Top 1% profile: 137% total own, 3.8 booms, 0.7 busts, 48 pitcher pts
- Target: ownership 100-150%, 3+ booms, <1 bust, 25+ pitcher pts

## Sim Validation — 2026-05-20, 2026-05-21, 2026-05-22, 2026-05-23, 2026-05-24, 2026-05-25, 2026-05-26

### Distribution Calibration
- **Hitters**: 90.6% in P10-P90 [WARN] (below floor=1.9%, above ceiling=7.5%)
- **Pitchers**: 68.9% in P10-P90 [WARN] (below floor=14.2%, above ceiling=16.9%)

### Projection Accuracy
- Overall: MAE=5.76, Bias=+1.20, r=0.376
- Hitters: MAE=5.51 [PASS]
- Pitchers: MAE=7.98 [PASS]

### Pitcher Components
- IP: MAE=1.14, Bias=-0.61
- Ks: MAE=1.85, Bias=-0.29
- ER: MAE=1.64, Bias=-0.22

### Multiplier Effectiveness

## Sim Validation — 2026-05-20, 2026-05-21, 2026-05-22, 2026-05-23, 2026-05-24, 2026-05-25, 2026-05-26

### Distribution Calibration
- **Hitters**: 90.6% in P10-P90 [WARN] (below floor=1.9%, above ceiling=7.5%)
- **Pitchers**: 68.9% in P10-P90 [WARN] (below floor=14.2%, above ceiling=16.9%)

### Projection Accuracy
- Overall: MAE=5.76, Bias=+1.20, r=0.376
- Hitters: MAE=5.51 [PASS]
- Pitchers: MAE=7.98 [WARN]

### Pitcher Components
- IP: MAE=1.14, Bias=-0.61
- Ks: MAE=1.85, Bias=-0.29
- ER: MAE=1.64, Bias=-0.22

### Multiplier Effectiveness

## Sim Validation — 2026-05-26

### Distribution Calibration
- **Hitters**: 88.5% in P10-P90 [WARN] (below floor=1.5%, above ceiling=10.0%)
- **Pitchers**: 62.1% in P10-P90 [FAIL] (below floor=27.6%, above ceiling=10.3%)

### Projection Accuracy
- Overall: MAE=6.23, Bias=+0.04, r=0.318
- Hitters: MAE=5.96 [PASS]
- Pitchers: MAE=8.76 [WARN]

### Pitcher Components
- IP: MAE=1.04, Bias=-0.46
- Ks: MAE=1.63, Bias=-0.13
- ER: MAE=2.13, Bias=-1.21

### Multiplier Effectiveness

## Research Findings — 2026-05-22, 2026-05-23, 2026-05-24, 2026-05-25, 2026-05-26, 2026-05-27, 2026-05-28, 2026-05-29

**Projection**: MAE=5.87, Bias=+1.39, Hitter MAE=5.67, Pitcher MAE=7.68
**Pool**: MAE=58.18, Bias=+58.15
**Contest**: Winner=183.58424657534246, Top1%=151.00410952054793

### Predictive Diagnostics

**Pitcher Missing Predictors** (correlated with error but not in model):
- `k9` r=+0.127 (n=165)
- `k_bb_pct` r=+0.080 (n=165)

**Opposing Lineup Factors** (for pitcher projections):
- `opp_iso` r=-0.132 (n=181)

**Optimal Context Weights**: Vegas=80% Park=5% Weather=15% (saves 0.00 MAE)

**Archetype Biases:**
- Power (ISO>.200): over-projected by 2.1 pts (n=331)
- Contact (K%<15%): over-projected by 1.8 pts (n=386)
- Strikeout (K%>28%): over-projected by 1.5 pts (n=220)
- High barrel (>10%): over-projected by 1.5 pts (n=580)
- Speed (SB pace>15): over-projected by 1.5 pts (n=442)

**Recommendations:**
- PROJECTION: increase context multiplier weights — hitter bias is -1.67 pts
- POOL: Best performing stack config is 4-4 — increase its weight in STACK_CONFIGS
- POOL: Projections have 10.9 pt spread — use projection rank as primary sort for portfolio selection
- CONTEST: Avg Top 1% threshold is 151.0 pts across 73 contests
- CONTEST: Avg cash line is 111.5 pts — pool floor should exceed this
- CONTEST: Avg winner scores 183.6 pts — need high-ceiling correlated stacks
- TRACKING: Run this analysis daily to build sample size — patterns stabilize after 2+ weeks

## Sim Validation — 2026-05-28

### Distribution Calibration
- **Hitters**: 87.5% in P10-P90 [WARN] (below floor=8.7%, above ceiling=3.8%)
- **Pitchers**: 91.7% in P10-P90 [WARN] (below floor=8.3%, above ceiling=0.0%)

### Projection Accuracy
- Overall: MAE=5.95, Bias=+1.62, r=0.482
- Hitters: MAE=6.06 [WARN]
- Pitchers: MAE=5.02 [PASS]

### Pitcher Components
- IP: MAE=0.94, Bias=-0.37
- Ks: MAE=1.46, Bias=-0.80
- ER: MAE=1.25, Bias=+0.14

### Multiplier Effectiveness

## Slate Review — 2026-05-28 / early

- **Pool**: 5000 lineups, avg actual=89.4, cash line=88.4, GPP line=149.4, best=173.0
- **Proj accuracy**: r=0.224, MAE=22.2, bias=+13.4
- **Overlap**: 0/20 top-by-proj were actual winners
- **Best strategy**: PMS (highest avg_pms)
- **Top stack**: ATL (avg actual=118.7, 11.6% exposure)
- **Biggest bust**: Kevin McGonigle (proj=9.7, actual=0.0, 35% exp)
- **Biggest missed opp**: Wenceel Pérez (actual=21.0, 7.0% exp)

## Slate Review — 2026-05-28 / main

- **Pool**: 5000 lineups, avg actual=81.2, cash line=81.0, GPP line=114.9, best=125.9
- **Proj accuracy**: r=-0.041, MAE=20.2, bias=+18.5
- **Overlap**: 0/20 top-by-proj were actual winners
- **Best strategy**: PMS (highest avg_pms)
- **Top stack**: HOU (avg actual=89.9, 16.1% exposure)
- **Biggest bust**: Gunnar Henderson (proj=10.3, actual=0.0, 29% exp)


## Ownership Calibration — 4 large-field contests (≥1000 entries)

- **Matched players**: 179 (10 0%-actual ghosts excluded)
- **Bias**: +5.47% (positive = over-project ownership)
- **MAE**: 6.43%
- **Correlation**: r=0.612

- Chalk (>20% actual): n=6, bias=-1.51%, MAE=10.60%
- Mid (5-20% actual): n=49, bias=+4.17%, MAE=6.18%
- Low (<5% actual): n=124, bias=+6.32%, MAE=6.33%

**Over-projected:**
- Jacob deGrom: proj=48.5% actual=0.2%
- Jose Soriano: proj=37.8% actual=0.7%
- Nathan Eovaldi: proj=38.6% actual=2.9%
- Shota Imanaga: proj=39.8% actual=9.4%
- Paul Skenes: proj=39.8% actual=13.8%

**Under-projected:**
- Pete Crow-Armstrong: proj=10.1% actual=23.6%
- Michael Busch: proj=8.6% actual=21.0%
- Dylan Cease: proj=36.9% actual=47.3%
- Ian Happ: proj=9.5% actual=18.8%
- Alex Bregman: proj=12.2% actual=19.7%

## Leverage Analysis — 2026-05-29 (50 contests, 1642 players)

**Dataset**: 1489 hitters, 153 pitchers across 42 dates
**Leverage hits**: 169 (10.3%) | **Chalk traps**: 35 (2.1%) | **Ceiling hits**: 154 (9.4%)

### Hitter Predictors (correlation with outperformance)
- `wRC+` r=+0.118 (n=1489)
- `Salary` r=+0.079 (n=1489)
- `ISO` r=+0.070 (n=1489)
- `xwOBA` r=+0.053 (n=1489)
- `Barrel%` r=-0.018 (n=1489)

### Pitcher Predictors
- `K%` r=+0.256 (n=153)
- `Salary` r=+0.194 (n=153)
- `xFIP` r=-0.231 (n=153)
- `Win Prob` r=+0.181 (n=153)
- `Stuff+` r=+0.139 (n=153)

### Actionable Rules
- **Hitter leverage**: ISO > 0.200 AND own < 10% -> 27% leverage rate (+17pp vs base)
- **Hitter leverage**: wRC+ > 120 AND own < 10% -> 22% leverage rate (+12pp vs base)
- **Hitter trap**: Own > 20% AND K% > 0.28 -> 68% chalk trap rate
- **Pitcher leverage**: K% > 0.25 AND own < 15% -> 33% leverage rate (+17pp vs base)
- **Pitcher leverage**: Stuff+ > 105 AND own < 15% -> 31% leverage rate (+14pp vs base)
- **Pitcher trap**: Own > 25% AND Stuff+ < 100 -> 59% chalk trap rate
- **Pitcher trap**: Own > 25% AND xFIP > 4.0 -> 56% chalk trap rate

## Winner Pattern Analysis — 2026-05-29 (73 contests, 1,056,800 entries)
- Top 1% profile: 138% total own, 3.8 booms, 0.7 busts, 48 pitcher pts
- Target: ownership 100-150%, 3+ booms, <1 bust, 25+ pitcher pts

## Research Findings — 2026-05-23, 2026-05-24, 2026-05-25, 2026-05-26, 2026-05-27, 2026-05-28, 2026-05-29, 2026-05-30

**Projection**: MAE=5.93, Bias=+1.27, Hitter MAE=5.75, Pitcher MAE=7.49
**Pool**: MAE=60.69, Bias=+60.67
**Contest**: Winner=183.6777027027027, Top1%=150.96351344594595

### Predictive Diagnostics

**Hitter Missing Predictors** (correlated with error but not in model):
- `oppo_pct` r=+0.061 (n=1465)
- `pull_pct` r=-0.056 (n=1465)
- `pa` r=+0.053 (n=1488)

**Pitcher Missing Predictors** (correlated with error but not in model):
- `k9` r=+0.158 (n=167)
- `k_bb_pct` r=+0.123 (n=167)

**Opposing Lineup Factors** (for pitcher projections):
- `opp_wrc_plus` r=-0.142 (n=183)
- `opp_woba` r=-0.141 (n=183)
- `opp_iso` r=-0.122 (n=183)
- `opp_o_swing_pct` r=+0.108 (n=183)
- `opp_xwoba` r=-0.108 (n=183)

**Optimal Context Weights**: Vegas=80% Park=5% Weather=15% (saves 0.00 MAE)

**Archetype Biases:**
- Power (ISO>.200): over-projected by 1.8 pts (n=328)
- Contact (K%<15%): over-projected by 1.6 pts (n=392)
- Strikeout (K%>28%): over-projected by 1.4 pts (n=230)
- High barrel (>10%): over-projected by 1.2 pts (n=589)
- Speed (SB pace>15): over-projected by 1.5 pts (n=443)

**Recommendations:**
- PROJECTION: increase context multiplier weights — hitter bias is -1.45 pts
- POOL: Best performing stack config is 4-0 — increase its weight in STACK_CONFIGS
- POOL: Projections have 8.3 pt spread — use projection rank as primary sort for portfolio selection
- CONTEST: Avg Top 1% threshold is 151.0 pts across 74 contests
- CONTEST: Avg cash line is 111.4 pts — pool floor should exceed this
- CONTEST: Avg winner scores 183.7 pts — need high-ceiling correlated stacks
- TRACKING: Run this analysis daily to build sample size — patterns stabilize after 2+ weeks

## Sim Validation — 2026-05-29

### Distribution Calibration
- **Hitters**: 86.2% in P10-P90 [WARN] (below floor=7.4%, above ceiling=6.3%)
- **Pitchers**: 76.7% in P10-P90 [PASS] (below floor=20.0%, above ceiling=3.3%)

### Projection Accuracy
- Overall: MAE=5.94, Bias=+0.95, r=0.171
- Hitters: MAE=5.8 [PASS]
- Pitchers: MAE=7.22 [PASS]

### Pitcher Components
- IP: MAE=1.0, Bias=+0.01
- Ks: MAE=1.7, Bias=+0.82
- ER: MAE=1.55, Bias=-0.53

### Multiplier Effectiveness

## Slate Review — 2026-05-29 / main

- **Pool**: 10000 lineups, avg actual=79.7, cash line=78.2, GPP line=138.7, best=176.9
- **Proj accuracy**: r=0.004, MAE=25.9, bias=+20.0
- **Overlap**: 0/20 top-by-proj were actual winners
- **Best strategy**: PMS (highest avg_pms)
- **Top stack**: SEA (avg actual=109.2, 3.3% exposure)
- **Biggest bust**: Luis Severino (proj=13.0, actual=2.5, 15% exp)
- **Biggest missed opp**: Ezequiel Tovar (actual=41.0, 3.3% exp)


## Ownership Calibration — 4 large-field contests (≥1000 entries)

- **Matched players**: 410 (39 0%-actual ghosts excluded)
- **Bias**: +5.90% (positive = over-project ownership)
- **MAE**: 6.70%
- **Correlation**: r=0.599

- Chalk (>20% actual): n=12, bias=-0.55%, MAE=13.26%
- Mid (5-20% actual): n=94, bias=+5.51%, MAE=7.21%
- Low (<5% actual): n=304, bias=+6.27%, MAE=6.29%

**Over-projected:**
- Jacob deGrom: proj=48.5% actual=0.2%
- Jose Soriano: proj=37.8% actual=0.7%
- Nathan Eovaldi: proj=38.6% actual=2.9%
- Cristopher Sanchez: proj=48.0% actual=15.0%
- Shota Imanaga: proj=38.8% actual=9.4%

**Under-projected:**
- Sandy Alcantara: proj=13.6% actual=37.9%
- Eury Perez: proj=18.8% actual=40.9%
- Pete Crow-Armstrong: proj=10.0% actual=23.6%
- Michael Busch: proj=8.5% actual=21.0%
- Dylan Cease: proj=36.9% actual=47.3%

## Leverage Analysis — 2026-05-30 (51 contests, 6245 players)

**Dataset**: 5659 hitters, 586 pitchers across 43 dates
**Leverage hits**: 697 (11.2%) | **Chalk traps**: 145 (2.3%) | **Ceiling hits**: 616 (9.9%)

### Hitter Predictors (correlation with outperformance)
- `wRC+` r=+0.140 (n=5656)
- `ISO` r=+0.112 (n=5656)
- `xwOBA` r=+0.092 (n=5656)
- `Salary` r=+0.086 (n=5659)
- `Barrel%` r=+0.049 (n=5656)

### Pitcher Predictors
- `K%` r=+0.222 (n=586)
- `Salary` r=+0.162 (n=586)
- `xFIP` r=-0.106 (n=586)
- `Win Prob` r=+0.057 (n=586)
- `Stuff+` r=+0.130 (n=585)

### Actionable Rules
- **Hitter leverage**: ISO > 0.200 AND own < 10% -> 27% leverage rate (+17pp vs base)
- **Hitter leverage**: wRC+ > 120 AND own < 10% -> 22% leverage rate (+12pp vs base)
- **Hitter trap**: Own > 20% AND K% > 0.28 -> 68% chalk trap rate
- **Pitcher leverage**: K% > 0.25 AND own < 15% -> 33% leverage rate (+17pp vs base)
- **Pitcher leverage**: Stuff+ > 105 AND own < 15% -> 31% leverage rate (+14pp vs base)
- **Pitcher trap**: Own > 25% AND Stuff+ < 100 -> 59% chalk trap rate
- **Pitcher trap**: Own > 25% AND xFIP > 4.0 -> 56% chalk trap rate

## Winner Pattern Analysis — 2026-05-30 (74 contests, 1,066,305 entries)
- Top 1% profile: 137% total own, 3.8 booms, 0.7 busts, 48 pitcher pts
- Target: ownership 100-150%, 3+ booms, <1 bust, 25+ pitcher pts

## Research Findings — 2026-05-24, 2026-05-25, 2026-05-26, 2026-05-27, 2026-05-28, 2026-05-29, 2026-05-30, 2026-05-31

**Projection**: MAE=5.95, Bias=+1.15, Hitter MAE=5.79, Pitcher MAE=7.36
**Pool**: MAE=65.58, Bias=+65.56
**Contest**: Winner=183.934, Top1%=151.20599993333335

### Predictive Diagnostics

**Hitter Missing Predictors** (correlated with error but not in model):
- `hard_hit_pct` r=+0.059 (n=1504)
- `avg_ev` r=+0.051 (n=1504)

**Pitcher Missing Predictors** (correlated with error but not in model):
- `k9` r=+0.265 (n=166)
- `k_bb_pct` r=+0.199 (n=166)
- `gs` r=-0.083 (n=166)

**Optimal Context Weights**: Vegas=80% Park=5% Weather=15% (saves 0.00 MAE)

**Archetype Biases:**
- Power (ISO>.200): over-projected by 1.5 pts (n=339)
- Contact (K%<15%): over-projected by 1.5 pts (n=395)
- Strikeout (K%>28%): over-projected by 1.2 pts (n=229)
- High barrel (>10%): over-projected by 1.1 pts (n=596)
- Speed (SB pace>15): over-projected by 1.4 pts (n=454)

**Recommendations:**
- PROJECTION: increase context multiplier weights — hitter bias is -1.26 pts
- POOL: Best performing stack config is 4-0 — increase its weight in STACK_CONFIGS
- POOL: Projections have 22.4 pt spread — use projection rank as primary sort for portfolio selection
- CONTEST: Avg Top 1% threshold is 151.2 pts across 75 contests
- CONTEST: Avg cash line is 111.6 pts — pool floor should exceed this
- CONTEST: Avg winner scores 183.9 pts — need high-ceiling correlated stacks
- TRACKING: Run this analysis daily to build sample size — patterns stabilize after 2+ weeks

## Sim Validation — 2026-05-30

### Distribution Calibration
- **Hitters**: 88.9% in P10-P90 [WARN] (below floor=4.4%, above ceiling=6.7%)
- **Pitchers**: 83.3% in P10-P90 [PASS] (below floor=13.3%, above ceiling=3.3%)

### Projection Accuracy
- Overall: MAE=6.01, Bias=+0.69, r=0.31
- Hitters: MAE=5.88 [PASS]
- Pitchers: MAE=7.18 [PASS]

### Pitcher Components
- IP: MAE=0.87, Bias=-0.60
- Ks: MAE=1.62, Bias=-0.06
- ER: MAE=1.74, Bias=-0.34

### Multiplier Effectiveness

## Slate Review — 2026-05-30 / main

- **Pool**: 10000 lineups, avg actual=89.1, cash line=88.0, GPP line=149.0, best=171.4
- **Proj accuracy**: r=0.055, MAE=21.3, bias=+9.1
- **Overlap**: 0/20 top-by-proj were actual winners
- **Best strategy**: PMS (highest avg_pms)
- **Top stack**: LAA (avg actual=115.2, 4.6% exposure)
- **Biggest bust**: Drew Rasmussen (proj=18.5, actual=3.4, 18% exp)
- **Biggest missed opp**: Jake Mangum (actual=31.0, 1.3% exp)

## Slate Review — 2026-05-30 / night

- **Pool**: 500 lineups, avg actual=95.0, cash line=93.7, GPP line=152.9, best=173.1
- **Proj accuracy**: r=0.087, MAE=21.0, bias=+4.4
- **Overlap**: 0/20 top-by-proj were actual winners
- **Best strategy**: PMS (highest avg_pms)
- **Top stack**: COL (avg actual=113.4, 16.2% exposure)
- **Biggest bust**: Ryne Nelson (proj=11.7, actual=5.8, 31% exp)
- **Biggest missed opp**: Ronald Acuña Jr. (actual=40.0, 9.2% exp)


## Ownership Calibration — 4 large-field contests (≥1000 entries)

- **Matched players**: 410 (39 0%-actual ghosts excluded)
- **Bias**: +5.86% (positive = over-project ownership)
- **MAE**: 6.67%
- **Correlation**: r=0.600

- Chalk (>20% actual): n=12, bias=-0.68%, MAE=13.15%
- Mid (5-20% actual): n=94, bias=+5.46%, MAE=7.19%
- Low (<5% actual): n=304, bias=+6.24%, MAE=6.25%

**Over-projected:**
- Jacob deGrom: proj=48.5% actual=0.2%
- Jose Soriano: proj=37.8% actual=0.7%
- Nathan Eovaldi: proj=38.6% actual=2.9%
- Cristopher Sanchez: proj=48.0% actual=15.0%
- Shota Imanaga: proj=38.8% actual=9.4%

**Under-projected:**
- Sandy Alcantara: proj=13.6% actual=37.9%
- Eury Perez: proj=18.8% actual=40.9%
- Pete Crow-Armstrong: proj=10.0% actual=23.6%
- Michael Busch: proj=8.5% actual=21.0%
- Dylan Cease: proj=36.9% actual=47.3%

## Leverage Analysis — 2026-05-31 (52 contests, 4048 players)

**Dataset**: 3672 hitters, 376 pitchers across 44 dates
**Leverage hits**: 458 (11.3%) | **Chalk traps**: 64 (1.6%) | **Ceiling hits**: 391 (9.7%)

### Hitter Predictors (correlation with outperformance)
- `wRC+` r=+0.130 (n=3672)
- `ISO` r=+0.103 (n=3672)
- `xwOBA` r=+0.064 (n=3672)
- `Salary` r=+0.063 (n=3672)
- `Barrel%` r=+0.025 (n=3672)

### Pitcher Predictors
- `K%` r=+0.239 (n=376)
- `Salary` r=+0.276 (n=376)
- `xFIP` r=-0.244 (n=376)
- `Win Prob` r=+0.073 (n=376)
- `Stuff+` r=+0.192 (n=376)

### Actionable Rules
- **Hitter leverage**: ISO > 0.200 AND own < 10% -> 27% leverage rate (+17pp vs base)
- **Hitter leverage**: wRC+ > 120 AND own < 10% -> 22% leverage rate (+12pp vs base)
- **Hitter trap**: Own > 20% AND K% > 0.28 -> 68% chalk trap rate
- **Pitcher leverage**: K% > 0.25 AND own < 15% -> 33% leverage rate (+17pp vs base)
- **Pitcher leverage**: Stuff+ > 105 AND own < 15% -> 31% leverage rate (+14pp vs base)
- **Pitcher trap**: Own > 25% AND Stuff+ < 100 -> 59% chalk trap rate
- **Pitcher trap**: Own > 25% AND xFIP > 4.0 -> 56% chalk trap rate

## Winner Pattern Analysis — 2026-05-31 (75 contests, 1,071,037 entries)
- Top 1% profile: 137% total own, 3.8 booms, 0.7 busts, 48 pitcher pts
- Target: ownership 100-150%, 3+ booms, <1 bust, 25+ pitcher pts

## Research Findings — 2026-05-25, 2026-05-26, 2026-05-27, 2026-05-28, 2026-05-29, 2026-05-30, 2026-05-31, 2026-06-01

**Projection**: MAE=6.06, Bias=+1.07, Hitter MAE=5.90, Pitcher MAE=7.48
**Pool**: MAE=71.96, Bias=+71.94
**Contest**: Winner=184.00197368421053, Top1%=151.21907888157895

### Predictive Diagnostics

**Hitter Missing Predictors** (correlated with error but not in model):
- `hard_hit_pct` r=+0.057 (n=1519)

**Pitcher Missing Predictors** (correlated with error but not in model):
- `k9` r=+0.280 (n=169)
- `k_bb_pct` r=+0.221 (n=169)
- `avg` r=-0.167 (n=169)
- `era` r=-0.112 (n=169)
- `whip` r=-0.106 (n=169)

**Opposing Lineup Factors** (for pitcher projections):
- `opp_o_swing_pct` r=+0.145 (n=186)

**Optimal Context Weights**: Vegas=80% Park=15% Weather=5% (saves 0.00 MAE)

**Archetype Biases:**
- Power (ISO>.200): over-projected by 1.4 pts (n=339)
- Contact (K%<15%): over-projected by 1.7 pts (n=400)
- High barrel (>10%): over-projected by 0.9 pts (n=602)
- Speed (SB pace>15): over-projected by 1.2 pts (n=452)

**Recommendations:**
- PROJECTION: increase context multiplier weights — hitter bias is -1.17 pts
- POOL: Best performing stack config is 4-2 — increase its weight in STACK_CONFIGS
- POOL: Projection spread only 1.9 pts — diversify selection, don't over-rely on projection ranking
- CONTEST: Avg Top 1% threshold is 151.2 pts across 76 contests
- CONTEST: Avg cash line is 111.6 pts — pool floor should exceed this
- CONTEST: Avg winner scores 184.0 pts — need high-ceiling correlated stacks
- TRACKING: Run this analysis daily to build sample size — patterns stabilize after 2+ weeks

## Sim Validation — 2026-05-31

### Distribution Calibration
- **Hitters**: 87.0% in P10-P90 [WARN] (below floor=6.7%, above ceiling=6.3%)
- **Pitchers**: 70.0% in P10-P90 [WARN] (below floor=20.0%, above ceiling=10.0%)

### Projection Accuracy
- Overall: MAE=6.13, Bias=+1.11, r=0.377
- Hitters: MAE=5.87 [PASS]
- Pitchers: MAE=8.47 [WARN]

### Pitcher Components
- IP: MAE=1.1, Bias=-0.05
- Ks: MAE=1.97, Bias=-0.22
- ER: MAE=1.6, Bias=-0.09

### Multiplier Effectiveness

## Slate Review — 2026-05-31 / main

- **Pool**: 10000 lineups, avg actual=94.9, cash line=94.6, GPP line=148.5, best=177.5
- **Proj accuracy**: r=0.361, MAE=20.0, bias=+12.1
- **Overlap**: 0/20 top-by-proj were actual winners
- **Best strategy**: PMS (highest avg_pms)
- **Top stack**: BOS (avg actual=122.5, 4.0% exposure)
- **Biggest bust**: Gavin Sheets (proj=9.4, actual=0.0, 23% exp)
- **Biggest missed opp**: Jack Leiter (actual=33.8, 0.7% exp)


## Ownership Calibration — 4 large-field contests (≥1000 entries)

- **Matched players**: 379 (40 0%-actual ghosts excluded)
- **Bias**: +6.07% (positive = over-project ownership)
- **MAE**: 6.84%
- **Correlation**: r=0.589

- Chalk (>20% actual): n=11, bias=+0.72%, MAE=13.90%
- Mid (5-20% actual): n=81, bias=+5.92%, MAE=7.67%
- Low (<5% actual): n=287, bias=+6.32%, MAE=6.33%

**Over-projected:**
- Jacob deGrom: proj=48.5% actual=0.2%
- Jose Soriano: proj=37.8% actual=0.7%
- Nathan Eovaldi: proj=38.6% actual=2.9%
- Cristopher Sanchez: proj=48.0% actual=15.0%
- Shota Imanaga: proj=38.8% actual=9.4%

**Under-projected:**
- Sandy Alcantara: proj=13.6% actual=37.9%
- Eury Perez: proj=18.8% actual=40.9%
- Pete Crow-Armstrong: proj=10.0% actual=23.6%
- Michael Busch: proj=8.5% actual=21.0%
- Ian Happ: proj=9.4% actual=18.8%

## Leverage Analysis — 2026-06-01 (52 contests, 4547 players)

**Dataset**: 4123 hitters, 424 pitchers across 44 dates
**Leverage hits**: 504 (11.1%) | **Chalk traps**: 76 (1.7%) | **Ceiling hits**: 440 (9.7%)

### Hitter Predictors (correlation with outperformance)
- `wRC+` r=+0.146 (n=4123)
- `ISO` r=+0.107 (n=4123)
- `xwOBA` r=+0.091 (n=4123)
- `Salary` r=+0.086 (n=4123)
- `Barrel%` r=+0.041 (n=4123)

### Pitcher Predictors
- `K%` r=+0.233 (n=424)
- `Salary` r=+0.266 (n=424)
- `xFIP` r=-0.240 (n=424)
- `Win Prob` r=+0.042 (n=424)
- `Stuff+` r=+0.110 (n=424)

### Actionable Rules
- **Hitter leverage**: ISO > 0.200 AND own < 10% -> 27% leverage rate (+17pp vs base)
- **Hitter leverage**: wRC+ > 120 AND own < 10% -> 22% leverage rate (+12pp vs base)
- **Hitter trap**: Own > 20% AND K% > 0.28 -> 68% chalk trap rate
- **Pitcher leverage**: K% > 0.25 AND own < 15% -> 33% leverage rate (+17pp vs base)
- **Pitcher leverage**: Stuff+ > 105 AND own < 15% -> 31% leverage rate (+14pp vs base)
- **Pitcher trap**: Own > 25% AND Stuff+ < 100 -> 59% chalk trap rate
- **Pitcher trap**: Own > 25% AND xFIP > 4.0 -> 56% chalk trap rate

## Winner Pattern Analysis — 2026-06-01 (76 contests, 1,081,318 entries)
- Top 1% profile: 137% total own, 3.8 booms, 0.7 busts, 48 pitcher pts
- Target: ownership 100-150%, 3+ booms, <1 bust, 25+ pitcher pts

## Research Findings — 2026-05-26, 2026-05-27, 2026-05-28, 2026-05-29, 2026-05-30, 2026-05-31, 2026-06-01, 2026-06-02

**Projection**: MAE=6.16, Bias=+0.99, Hitter MAE=6.01, Pitcher MAE=7.51
**Pool**: MAE=69.24, Bias=+69.23
**Contest**: Winner=184.35974025974028, Top1%=151.39740253246754

### Predictive Diagnostics

**Pitcher Missing Predictors** (correlated with error but not in model):
- `k9` r=+0.269 (n=161)
- `k_bb_pct` r=+0.214 (n=161)
- `avg` r=-0.210 (n=161)
- `whip` r=-0.148 (n=161)
- `era` r=-0.138 (n=161)

**Opposing Lineup Factors** (for pitcher projections):
- `opp_o_swing_pct` r=+0.157 (n=178)

**Optimal Context Weights**: Vegas=80% Park=10% Weather=10% (saves 0.00 MAE)

**Archetype Biases:**
- Power (ISO>.200): over-projected by 1.2 pts (n=322)
- Contact (K%<15%): over-projected by 1.6 pts (n=383)
- High barrel (>10%): over-projected by 0.8 pts (n=572)
- Speed (SB pace>15): over-projected by 1.1 pts (n=424)

**Recommendations:**
- PROJECTION: increase context multiplier weights — hitter bias is -1.04 pts
- POOL: Best performing stack config is 4-2 — increase its weight in STACK_CONFIGS
- POOL: Projection spread only -1.8 pts — diversify selection, don't over-rely on projection ranking
- CONTEST: Avg Top 1% threshold is 151.4 pts across 77 contests
- CONTEST: Avg cash line is 111.7 pts — pool floor should exceed this
- CONTEST: Avg winner scores 184.4 pts — need high-ceiling correlated stacks
- TRACKING: Run this analysis daily to build sample size — patterns stabilize after 2+ weeks

## Sim Validation — 2026-06-01

### Distribution Calibration
- **Hitters**: 78.3% in P10-P90 [PASS] (below floor=8.9%, above ceiling=12.7%)
- **Pitchers**: 72.2% in P10-P90 [WARN] (below floor=22.2%, above ceiling=5.6%)

### Projection Accuracy
- Overall: MAE=6.56, Bias=+0.46, r=0.202
- Hitters: MAE=6.56 [WARN]
- Pitchers: MAE=6.56 [PASS]

### Pitcher Components
- IP: MAE=0.84, Bias=-0.40
- Ks: MAE=1.27, Bias=-0.25
- ER: MAE=1.76, Bias=-1.13

### Multiplier Effectiveness

## Slate Review — 2026-06-01 / main

- **Pool**: 10000 lineups, avg actual=80.5, cash line=78.8, GPP line=154.7, best=192.5
- **Proj accuracy**: r=0.232, MAE=31.9, bias=+24.3
- **Overlap**: 0/20 top-by-proj were actual winners
- **Best strategy**: PMS (highest avg_pms)
- **Top stack**: MIL (avg actual=118.1, 3.5% exposure)
- **Biggest bust**: Landen Roupp (proj=15.1, actual=-6.8, 19% exp)
- **Biggest missed opp**: Miguel Vargas (actual=37.0, 4.7% exp)

## Leverage Analysis — 2026-06-02 (53 contests, 3971 players)

**Dataset**: 3600 hitters, 371 pitchers across 45 dates
**Leverage hits**: 408 (10.3%) | **Chalk traps**: 80 (2.0%) | **Ceiling hits**: 378 (9.5%)

### Hitter Predictors (correlation with outperformance)
- `wRC+` r=+0.121 (n=3600)
- `ISO` r=+0.101 (n=3600)
- `xwOBA` r=+0.086 (n=3600)
- `Salary` r=+0.058 (n=3600)
- `Barrel%` r=+0.039 (n=3600)

### Pitcher Predictors
- `K%` r=+0.233 (n=371)
- `Salary` r=+0.216 (n=371)
- `xFIP` r=-0.101 (n=370)
- `Win Prob` r=+0.070 (n=371)
- `Stuff+` r=+0.195 (n=369)

### Actionable Rules
- **Hitter leverage**: ISO > 0.200 AND own < 10% -> 27% leverage rate (+17pp vs base)
- **Hitter leverage**: wRC+ > 120 AND own < 10% -> 22% leverage rate (+12pp vs base)
- **Hitter trap**: Own > 20% AND K% > 0.28 -> 68% chalk trap rate
- **Pitcher leverage**: K% > 0.25 AND own < 15% -> 33% leverage rate (+17pp vs base)
- **Pitcher leverage**: Stuff+ > 105 AND own < 15% -> 31% leverage rate (+14pp vs base)
- **Pitcher trap**: Own > 25% AND Stuff+ < 100 -> 59% chalk trap rate
- **Pitcher trap**: Own > 25% AND xFIP > 4.0 -> 56% chalk trap rate

## Winner Pattern Analysis — 2026-06-02 (77 contests, 1,128,862 entries)
- Top 1% profile: 137% total own, 3.8 booms, 0.7 busts, 48 pitcher pts
- Target: ownership 100-150%, 3+ booms, <1 bust, 25+ pitcher pts

## Research Findings — 2026-05-27, 2026-05-28, 2026-05-29, 2026-05-30, 2026-05-31, 2026-06-01, 2026-06-02, 2026-06-03

**Projection**: MAE=6.15, Bias=+1.16, Hitter MAE=6.01, Pitcher MAE=7.47
**Pool**: MAE=68.61, Bias=+68.59
**Contest**: Winner=184.54615384615386, Top1%=151.54999993589743

### Predictive Diagnostics

**Pitcher Missing Predictors** (correlated with error but not in model):
- `k9` r=+0.224 (n=158)
- `avg` r=-0.161 (n=158)
- `k_bb_pct` r=+0.157 (n=158)
- `g` r=-0.090 (n=158)
- `whip` r=-0.086 (n=158)

**Opposing Lineup Factors** (for pitcher projections):
- `opp_o_swing_pct` r=+0.158 (n=178)

**Optimal Context Weights**: Vegas=80% Park=5% Weather=15% (saves 0.00 MAE)

**Archetype Biases:**
- Power (ISO>.200): over-projected by 1.6 pts (n=317)
- Contact (K%<15%): over-projected by 1.5 pts (n=384)
- High barrel (>10%): over-projected by 1.1 pts (n=567)
- Speed (SB pace>15): over-projected by 1.5 pts (n=421)

**Recommendations:**
- PROJECTION: increase context multiplier weights — hitter bias is -1.26 pts
- POOL: Best performing stack config is 4-4 — increase its weight in STACK_CONFIGS
- POOL: Projections have 5.4 pt spread — use projection rank as primary sort for portfolio selection
- CONTEST: Avg Top 1% threshold is 151.5 pts across 78 contests
- CONTEST: Avg cash line is 111.8 pts — pool floor should exceed this
- CONTEST: Avg winner scores 184.5 pts — need high-ceiling correlated stacks
- TRACKING: Run this analysis daily to build sample size — patterns stabilize after 2+ weeks

## Sim Validation — 2026-06-02

### Distribution Calibration
- **Hitters**: 86.6% in P10-P90 [WARN] (below floor=6.7%, above ceiling=6.7%)
- **Pitchers**: 69.0% in P10-P90 [WARN] (below floor=17.2%, above ceiling=13.8%)

### Projection Accuracy
- Overall: MAE=6.18, Bias=+1.06, r=0.227
- Hitters: MAE=5.92 [PASS]
- Pitchers: MAE=8.52 [WARN]

### Pitcher Components
- IP: MAE=1.02, Bias=-0.53
- Ks: MAE=2.08, Bias=-0.74
- ER: MAE=1.46, Bias=-0.84

### Multiplier Effectiveness

## Slate Review — 2026-06-02 / main

- **Pool**: 10000 lineups, avg actual=93.0, cash line=92.2, GPP line=156.6, best=186.2
- **Proj accuracy**: r=-0.117, MAE=24.1, bias=+10.9
- **Overlap**: 0/20 top-by-proj were actual winners
- **Best strategy**: PMS (highest avg_pms)
- **Top stack**: MIL (avg actual=121.0, 2.2% exposure)
- **Biggest bust**: Grayson Rodriguez (proj=18.5, actual=-8.3, 21% exp)
- **Biggest missed opp**: Endy Rodríguez (actual=32.0, 7.9% exp)

## Leverage Analysis — 2026-06-03 (54 contests, 6397 players)

**Dataset**: 5799 hitters, 598 pitchers across 46 dates
**Leverage hits**: 682 (10.7%) | **Chalk traps**: 149 (2.3%) | **Ceiling hits**: 627 (9.8%)

### Hitter Predictors (correlation with outperformance)
- `wRC+` r=+0.117 (n=5796)
- `ISO` r=+0.101 (n=5799)
- `xwOBA` r=+0.081 (n=5799)
- `Salary` r=+0.080 (n=5799)
- `Barrel%` r=+0.047 (n=5799)

### Pitcher Predictors
- `K%` r=+0.231 (n=598)
- `Salary` r=+0.197 (n=598)
- `xFIP` r=-0.115 (n=598)
- `Win Prob` r=+0.069 (n=598)
- `Stuff+` r=+0.143 (n=596)

### Actionable Rules
- **Hitter leverage**: ISO > 0.200 AND own < 10% -> 27% leverage rate (+17pp vs base)
- **Hitter leverage**: wRC+ > 120 AND own < 10% -> 22% leverage rate (+12pp vs base)
- **Hitter trap**: Own > 20% AND K% > 0.28 -> 68% chalk trap rate
- **Pitcher leverage**: K% > 0.25 AND own < 15% -> 33% leverage rate (+17pp vs base)
- **Pitcher leverage**: Stuff+ > 105 AND own < 15% -> 31% leverage rate (+14pp vs base)
- **Pitcher trap**: Own > 25% AND Stuff+ < 100 -> 59% chalk trap rate
- **Pitcher trap**: Own > 25% AND xFIP > 4.0 -> 56% chalk trap rate

## Winner Pattern Analysis — 2026-06-03 (78 contests, 1,150,245 entries)
- Top 1% profile: 137% total own, 3.8 booms, 0.7 busts, 48 pitcher pts
- Target: ownership 100-150%, 3+ booms, <1 bust, 25+ pitcher pts

## Research Findings — 2026-05-28, 2026-05-29, 2026-05-30, 2026-05-31, 2026-06-01, 2026-06-02, 2026-06-03, 2026-06-04

**Projection**: MAE=6.16, Bias=+1.02, Hitter MAE=5.98, Pitcher MAE=7.73
**Pool**: MAE=65.13, Bias=+65.10
**Contest**: Winner=184.62025316455697, Top1%=151.60886069620253

### Predictive Diagnostics

**Pitcher Missing Predictors** (correlated with error but not in model):
- `k9` r=+0.147 (n=160)
- `avg` r=-0.145 (n=160)
- `bb9` r=+0.111 (n=160)
- `g` r=-0.091 (n=160)

**Opposing Lineup Factors** (for pitcher projections):
- `opp_o_swing_pct` r=+0.122 (n=179)

**Optimal Context Weights**: Vegas=80% Park=5% Weather=15% (saves -0.00 MAE)

**Archetype Biases:**
- Power (ISO>.200): over-projected by 1.2 pts (n=317)
- Contact (K%<15%): over-projected by 1.2 pts (n=387)
- High barrel (>10%): over-projected by 1.0 pts (n=569)
- Speed (SB pace>15): over-projected by 1.6 pts (n=417)

**Recommendations:**
- PROJECTION: increase context multiplier weights — hitter bias is -1.06 pts
- POOL: Best performing stack config is 4-4 — increase its weight in STACK_CONFIGS
- POOL: Projections have 27.2 pt spread — use projection rank as primary sort for portfolio selection
- CONTEST: Avg Top 1% threshold is 151.6 pts across 79 contests
- CONTEST: Avg cash line is 111.9 pts — pool floor should exceed this
- CONTEST: Avg winner scores 184.6 pts — need high-ceiling correlated stacks
- TRACKING: Run this analysis daily to build sample size — patterns stabilize after 2+ weeks

## Sim Validation — 2026-06-03

### Distribution Calibration
- **Hitters**: 88.7% in P10-P90 [WARN] (below floor=5.3%, above ceiling=6.0%)
- **Pitchers**: 63.3% in P10-P90 [FAIL] (below floor=23.3%, above ceiling=13.3%)

### Projection Accuracy
- Overall: MAE=6.37, Bias=+1.41, r=0.322
- Hitters: MAE=6.06 [WARN]
- Pitchers: MAE=9.07 [FAIL]

### Pitcher Components
- IP: MAE=0.99, Bias=-0.45
- Ks: MAE=1.4, Bias=+0.07
- ER: MAE=1.55, Bias=-0.32

### Multiplier Effectiveness

## Slate Review — 2026-06-03 / early

- **Pool**: 4500 lineups, avg actual=64.6, cash line=62.0, GPP line=132.4, best=164.8
- **Proj accuracy**: r=-0.333, MAE=35.7, bias=+31.1
- **Overlap**: 0/20 top-by-proj were actual winners
- **Best strategy**: PMS (highest avg_pms)
- **Top stack**: CWS (avg actual=89.4, 21.6% exposure)
- **Biggest bust**: Colson Montgomery (proj=9.2, actual=0.0, 35% exp)
- **Biggest missed opp**: Gleyber Torres (actual=17.0, 14.4% exp)

## Slate Review — 2026-06-03 / main

- **Pool**: 10000 lineups, avg actual=88.9, cash line=88.9, GPP line=146.8, best=174.6
- **Proj accuracy**: r=0.238, MAE=23.2, bias=+13.4
- **Overlap**: 0/20 top-by-proj were actual winners
- **Best strategy**: PMS (highest avg_pms)
- **Top stack**: HOU (avg actual=116.1, 5.7% exposure)
- **Biggest bust**: Gerrit Cole (proj=15.0, actual=3.8, 18% exp)
- **Biggest missed opp**: José Ramírez (actual=29.0, 6.7% exp)

## Leverage Analysis — 2026-06-04 (55 contests, 5450 players)

**Dataset**: 4928 hitters, 522 pitchers across 47 dates
**Leverage hits**: 581 (10.7%) | **Chalk traps**: 129 (2.4%) | **Ceiling hits**: 533 (9.8%)

### Hitter Predictors (correlation with outperformance)
- `wRC+` r=+0.112 (n=4925)
- `ISO` r=+0.099 (n=4928)
- `xwOBA` r=+0.082 (n=4928)
- `Salary` r=+0.073 (n=4928)
- `Barrel%` r=+0.045 (n=4928)

### Pitcher Predictors
- `K%` r=+0.229 (n=522)
- `Salary` r=+0.206 (n=522)
- `xFIP` r=-0.111 (n=522)
- `Win Prob` r=+0.083 (n=522)
- `Stuff+` r=+0.152 (n=520)

### Actionable Rules
- **Hitter leverage**: ISO > 0.200 AND own < 10% -> 27% leverage rate (+17pp vs base)
- **Hitter leverage**: wRC+ > 120 AND own < 10% -> 22% leverage rate (+12pp vs base)
- **Hitter trap**: Own > 20% AND K% > 0.28 -> 68% chalk trap rate
- **Pitcher leverage**: K% > 0.25 AND own < 15% -> 33% leverage rate (+17pp vs base)
- **Pitcher leverage**: Stuff+ > 105 AND own < 15% -> 31% leverage rate (+14pp vs base)
- **Pitcher trap**: Own > 25% AND Stuff+ < 100 -> 59% chalk trap rate
- **Pitcher trap**: Own > 25% AND xFIP > 4.0 -> 56% chalk trap rate

## Winner Pattern Analysis — 2026-06-04 (79 contests, 1,159,062 entries)
- Top 1% profile: 137% total own, 3.8 booms, 0.7 busts, 48 pitcher pts
- Target: ownership 100-150%, 3+ booms, <1 bust, 25+ pitcher pts


## Ownership Calibration — 42 game dates

- **Matched players**: 7295 (2 0%-actual ghosts excluded)
- **Bias**: +2.40% (positive = over-project ownership)
- **MAE**: 4.87%
- **Correlation**: r=0.635

- Chalk (>20% actual): n=399, bias=-4.94%, MAE=11.92%
- Mid (5-20% actual): n=2339, bias=+1.18%, MAE=5.27%
- Low (<5% actual): n=4557, bias=+3.67%, MAE=4.05%

**Over-projected:**
- Mason Fluharty: proj=71.4% actual=2.4%
- Brendan Donovan: proj=60.3% actual=0.2%
- Daylen Lile: proj=58.4% actual=2.9%
- Drew Millas: proj=54.2% actual=1.8%
- Matthew Boyd: proj=54.1% actual=4.3%

**Under-projected:**
- David Peterson: proj=23.0% actual=63.0%
- Nolan McLean: proj=19.4% actual=58.4%
- Ryan Weathers: proj=5.0% actual=42.7%
- Chad Patrick: proj=10.8% actual=47.2%
- Jacob Misiorowski: proj=19.7% actual=56.1%

## Research Findings — 2026-05-29, 2026-05-30, 2026-05-31, 2026-06-01, 2026-06-02, 2026-06-03, 2026-06-04, 2026-06-05

**Projection**: MAE=6.15, Bias=+1.01, Hitter MAE=5.95, Pitcher MAE=7.97
**Pool**: MAE=67.64, Bias=+67.61
**Contest**: Winner=184.523125, Top1%=151.5712499375

### Predictive Diagnostics

**Pitcher Missing Predictors** (correlated with error but not in model):
- `avg` r=-0.149 (n=164)
- `k9` r=+0.139 (n=164)
- `gb_pct` r=+0.108 (n=164)
- `k_bb_pct` r=+0.096 (n=164)
- `g` r=-0.094 (n=164)

**Optimal Context Weights**: Vegas=80% Park=5% Weather=15% (saves -0.00 MAE)

**Archetype Biases:**
- Power (ISO>.200): over-projected by 1.1 pts (n=327)
- Contact (K%<15%): over-projected by 1.2 pts (n=404)
- High barrel (>10%): over-projected by 0.8 pts (n=588)
- Speed (SB pace>15): over-projected by 1.6 pts (n=438)

**Recommendations:**
- PROJECTION: increase context multiplier weights — hitter bias is -1.01 pts
- POOL: Best performing stack config is 4-0 — increase its weight in STACK_CONFIGS
- POOL: Projections have 23.7 pt spread — use projection rank as primary sort for portfolio selection
- CONTEST: Avg Top 1% threshold is 151.6 pts across 80 contests
- CONTEST: Avg cash line is 111.9 pts — pool floor should exceed this
- CONTEST: Avg winner scores 184.5 pts — need high-ceiling correlated stacks
- TRACKING: Run this analysis daily to build sample size — patterns stabilize after 2+ weeks

## Sim Validation — 2026-06-04

### Distribution Calibration
- **Hitters**: 85.7% in P10-P90 [WARN] (below floor=8.7%, above ceiling=5.6%)
- **Pitchers**: 66.7% in P10-P90 [WARN] (below floor=27.8%, above ceiling=5.6%)

### Projection Accuracy
- Overall: MAE=5.99, Bias=+1.26, r=0.318
- Hitters: MAE=5.72 [PASS]
- Pitchers: MAE=8.37 [WARN]

### Pitcher Components
- IP: MAE=1.02, Bias=-0.18
- Ks: MAE=1.58, Bias=+0.33
- ER: MAE=1.78, Bias=-0.18

### Multiplier Effectiveness

## Slate Review — 2026-06-04 / early

- **Pool**: 10000 lineups, avg actual=93.9, cash line=93.0, GPP line=158.0, best=195.0
- **Proj accuracy**: r=0.123, MAE=23.1, bias=+7.9
- **Overlap**: 0/20 top-by-proj were actual winners
- **Best strategy**: PMS (highest avg_pms)
- **Top stack**: SF (avg actual=131.4, 12.5% exposure)
- **Biggest bust**: Coleman Crow (proj=10.6, actual=-9.4, 22% exp)
- **Biggest missed opp**: Jackson Chourio (actual=41.0, 8.9% exp)

## Slate Review — 2026-06-04 / main

- **Pool**: 10000 lineups, avg actual=87.3, cash line=88.3, GPP line=140.5, best=166.3
- **Proj accuracy**: r=0.046, MAE=25.4, bias=+20.0
- **Overlap**: 0/20 top-by-proj were actual winners
- **Best strategy**: PMS (highest avg_pms)
- **Top stack**: MIN (avg actual=101.0, 12.0% exposure)
- **Biggest bust**: Chris Sale (proj=23.3, actual=11.6, 58% exp)
- **Biggest missed opp**: J.T. Ginn (actual=25.7, 9.8% exp)


## Ownership Calibration — 42 game dates

- **Matched players**: 7295 (2 0%-actual ghosts excluded)
- **Bias**: +2.40% (positive = over-project ownership)
- **MAE**: 4.87%
- **Correlation**: r=0.635

- Chalk (>20% actual): n=399, bias=-4.94%, MAE=11.92%
- Mid (5-20% actual): n=2339, bias=+1.18%, MAE=5.27%
- Low (<5% actual): n=4557, bias=+3.67%, MAE=4.05%

**Over-projected:**
- Mason Fluharty: proj=71.4% actual=2.4%
- pid:680977: proj=60.3% actual=0.2%
- pid:695734: proj=58.4% actual=2.9%
- pid:686452: proj=54.2% actual=1.8%
- Matthew Boyd: proj=54.1% actual=4.3%

**Under-projected:**
- pid:656849: proj=23.0% actual=63.0%
- pid:690997: proj=19.4% actual=58.4%
- Ryan Weathers: proj=5.0% actual=42.7%
- Chad Patrick: proj=10.8% actual=47.2%
- Jacob Misiorowski: proj=19.7% actual=56.1%

## Leverage Analysis — 2026-06-05 (56 contests, 3018 players)

**Dataset**: 2736 hitters, 282 pitchers across 48 dates
**Leverage hits**: 305 (10.1%) | **Chalk traps**: 76 (2.5%) | **Ceiling hits**: 272 (9.0%)

### Hitter Predictors (correlation with outperformance)
- `wRC+` r=+0.097 (n=2736)
- `ISO` r=+0.085 (n=2736)
- `Salary` r=+0.072 (n=2736)
- `xwOBA` r=+0.060 (n=2736)
- `Barrel%` r=+0.040 (n=2736)

### Pitcher Predictors
- `K%` r=+0.223 (n=282)
- `Salary` r=+0.185 (n=282)
- `xFIP` r=-0.230 (n=282)
- `Win Prob` r=+0.107 (n=282)
- `Stuff+` r=+0.167 (n=282)

### Actionable Rules
- **Hitter leverage**: ISO > 0.200 AND own < 10% -> 27% leverage rate (+17pp vs base)
- **Hitter leverage**: wRC+ > 120 AND own < 10% -> 22% leverage rate (+12pp vs base)
- **Hitter trap**: Own > 20% AND K% > 0.28 -> 68% chalk trap rate
- **Pitcher leverage**: K% > 0.25 AND own < 15% -> 33% leverage rate (+17pp vs base)
- **Pitcher leverage**: Stuff+ > 105 AND own < 15% -> 31% leverage rate (+14pp vs base)
- **Pitcher trap**: Own > 25% AND Stuff+ < 100 -> 59% chalk trap rate
- **Pitcher trap**: Own > 25% AND xFIP > 4.0 -> 56% chalk trap rate

## Winner Pattern Analysis — 2026-06-05 (80 contests, 1,166,164 entries)
- Top 1% profile: 137% total own, 3.8 booms, 0.7 busts, 48 pitcher pts
- Target: ownership 100-150%, 3+ booms, <1 bust, 25+ pitcher pts

## Research Findings — 2026-06-01, 2026-06-02, 2026-06-03, 2026-06-04, 2026-06-05, 2026-06-06, 2026-06-07, 2026-06-08

**Projection**: MAE=6.27, Bias=+1.36, Hitter MAE=6.08, Pitcher MAE=7.97
**Pool**: MAE=52.82, Bias=+52.61
**Contest**: Winner=184.67777777777778, Top1%=151.54876537037035

### Predictive Diagnostics

**Hitter Missing Predictors** (correlated with error but not in model):
- `ops` r=-0.061 (n=1224)
- `slg` r=-0.060 (n=1224)
- `fb_pct` r=+0.059 (n=1204)
- `obp` r=-0.052 (n=1224)

**Pitcher Missing Predictors** (correlated with error but not in model):
- `gb_pct` r=+0.134 (n=135)
- `w` r=+0.104 (n=135)
- `g` r=-0.092 (n=135)
- `avg` r=-0.092 (n=135)
- `whip` r=-0.088 (n=135)

**Opposing Lineup Factors** (for pitcher projections):
- `opp_gb_pct` r=+0.216 (n=153)
- `opp_k_pct` r=-0.141 (n=153)
- `opp_xwoba` r=+0.123 (n=153)

**Optimal Context Weights**: Vegas=80% Park=5% Weather=15% (saves -0.00 MAE)

**Archetype Biases:**
- Power (ISO>.200): over-projected by 1.8 pts (n=264)
- Contact (K%<15%): over-projected by 1.2 pts (n=338)
- Strikeout (K%>28%): over-projected by 1.2 pts (n=190)
- High barrel (>10%): over-projected by 1.7 pts (n=475)
- Speed (SB pace>15): over-projected by 1.6 pts (n=354)

**Recommendations:**
- PROJECTION: increase context multiplier weights — hitter bias is -1.41 pts
- POOL: Best performing stack config is 4-0 — increase its weight in STACK_CONFIGS
- POOL: Projection spread only 0.8 pts — diversify selection, don't over-rely on projection ranking
- CONTEST: Avg Top 1% threshold is 151.5 pts across 81 contests
- CONTEST: Avg cash line is 111.8 pts — pool floor should exceed this
- CONTEST: Avg winner scores 184.7 pts — need high-ceiling correlated stacks
- TRACKING: Run this analysis daily to build sample size — patterns stabilize after 2+ weeks

## Sim Validation — 2026-06-07

### Distribution Calibration
- **Hitters**: 81.8% in P10-P90 [PASS] (below floor=11.6%, above ceiling=6.6%)
- **Pitchers**: 76.7% in P10-P90 [PASS] (below floor=16.7%, above ceiling=6.7%)

### Projection Accuracy
- Overall: MAE=6.1, Bias=+1.53, r=0.241
- Hitters: MAE=6.0 [WARN]
- Pitchers: MAE=6.92 [PASS]

### Pitcher Components
- IP: MAE=1.02, Bias=-0.27
- Ks: MAE=1.53, Bias=+0.19
- ER: MAE=1.38, Bias=+0.09

### Multiplier Effectiveness

## Slate Review — 2026-06-07 / main

- **Pool**: 1500 lineups, avg actual=79.7, cash line=78.8, GPP line=144.1, best=177.2
- **Proj accuracy**: r=0.212, MAE=28.1, bias=+22.2
- **Overlap**: 0/20 top-by-proj were actual winners
- **Best strategy**: PMS (highest avg_pms)
- **Top stack**: TEX (avg actual=117.1, 3.2% exposure)
- **Biggest bust**: Aaron Nola (proj=15.8, actual=1.7, 14% exp)
- **Biggest missed opp**: Noah Cameron (actual=29.7, 14.3% exp)


## Ownership Calibration — 42 game dates

- **Matched players**: 7295 (2 0%-actual ghosts excluded)
- **Bias**: +2.40% (positive = over-project ownership)
- **MAE**: 4.87%
- **Correlation**: r=0.635

- Chalk (>20% actual): n=399, bias=-4.94%, MAE=11.92%
- Mid (5-20% actual): n=2339, bias=+1.18%, MAE=5.27%
- Low (<5% actual): n=4557, bias=+3.67%, MAE=4.05%

**Over-projected:**
- Mason Fluharty: proj=71.4% actual=2.4%
- Brendan Donovan: proj=60.3% actual=0.2%
- Daylen Lile: proj=58.4% actual=2.9%
- Drew Millas: proj=54.2% actual=1.8%
- Matthew Boyd: proj=54.1% actual=4.3%

**Under-projected:**
- David Peterson: proj=23.0% actual=63.0%
- Nolan McLean: proj=19.4% actual=58.4%
- Ryan Weathers: proj=5.0% actual=42.7%
- Chad Patrick: proj=10.8% actual=47.2%
- Jacob Misiorowski: proj=19.7% actual=56.1%

## Leverage Analysis — 2026-06-08 (57 contests, 5801 players)

**Dataset**: 5262 hitters, 539 pitchers across 49 dates
**Leverage hits**: 674 (11.6%) | **Chalk traps**: 122 (2.1%) | **Ceiling hits**: 578 (10.0%)

### Hitter Predictors (correlation with outperformance)
- `wRC+` r=+0.127 (n=5262)
- `ISO` r=+0.100 (n=5262)
- `Salary` r=+0.086 (n=5262)
- `xwOBA` r=+0.080 (n=5262)
- `Barrel%` r=+0.044 (n=5262)

### Pitcher Predictors
- `K%` r=+0.148 (n=539)
- `Salary` r=+0.120 (n=539)
- `xFIP` r=-0.172 (n=539)
- `Win Prob` r=+0.038 (n=539)
- `Stuff+` r=+0.037 (n=539)

### Actionable Rules
- **Hitter leverage**: ISO > 0.200 AND own < 10% -> 27% leverage rate (+17pp vs base)
- **Hitter leverage**: wRC+ > 120 AND own < 10% -> 22% leverage rate (+12pp vs base)
- **Hitter trap**: Own > 20% AND K% > 0.28 -> 68% chalk trap rate
- **Pitcher leverage**: K% > 0.25 AND own < 15% -> 33% leverage rate (+17pp vs base)
- **Pitcher leverage**: Stuff+ > 105 AND own < 15% -> 31% leverage rate (+14pp vs base)
- **Pitcher trap**: Own > 25% AND Stuff+ < 100 -> 59% chalk trap rate
- **Pitcher trap**: Own > 25% AND xFIP > 4.0 -> 56% chalk trap rate

## Winner Pattern Analysis — 2026-06-08 (81 contests, 1,175,630 entries)
- Top 1% profile: 137% total own, 3.8 booms, 0.7 busts, 48 pitcher pts
- Target: ownership 100-150%, 3+ booms, <1 bust, 25+ pitcher pts

## Research Findings — 2026-06-02, 2026-06-03, 2026-06-04, 2026-06-05, 2026-06-06, 2026-06-07, 2026-06-08, 2026-06-09

**Projection**: MAE=6.20, Bias=+1.42, Hitter MAE=6.01, Pitcher MAE=7.94
**Pool**: MAE=52.66, Bias=+52.12
**Contest**: Winner=184.95914634146342, Top1%=151.84390237804877

### Predictive Diagnostics

**Pitcher Missing Predictors** (correlated with error but not in model):
- `gb_pct` r=+0.199 (n=133)
- `w` r=+0.127 (n=133)

**Opposing Lineup Factors** (for pitcher projections):
- `opp_gb_pct` r=+0.217 (n=151)
- `opp_k_pct` r=-0.207 (n=151)
- `opp_xwoba` r=+0.135 (n=151)

**Optimal Context Weights**: Vegas=80% Park=5% Weather=15% (saves -0.00 MAE)

**Archetype Biases:**
- Power (ISO>.200): over-projected by 1.9 pts (n=261)
- Contact (K%<15%): over-projected by 1.1 pts (n=324)
- Strikeout (K%>28%): over-projected by 1.2 pts (n=193)
- High barrel (>10%): over-projected by 1.7 pts (n=476)
- Speed (SB pace>15): over-projected by 1.7 pts (n=355)

**Recommendations:**
- PROJECTION: increase context multiplier weights — hitter bias is -1.49 pts
- POOL: Best performing stack config is 4-0 — increase its weight in STACK_CONFIGS
- POOL: Projections have 6.3 pt spread — use projection rank as primary sort for portfolio selection
- CONTEST: Avg Top 1% threshold is 151.8 pts across 82 contests
- CONTEST: Avg cash line is 112.0 pts — pool floor should exceed this
- CONTEST: Avg winner scores 185.0 pts — need high-ceiling correlated stacks
- TRACKING: Run this analysis daily to build sample size — patterns stabilize after 2+ weeks

## Sim Validation — 2026-06-08

### Distribution Calibration
- **Hitters**: 87.2% in P10-P90 [WARN] (below floor=7.1%, above ceiling=5.7%)
- **Pitchers**: 81.2% in P10-P90 [PASS] (below floor=12.5%, above ceiling=6.2%)

### Projection Accuracy
- Overall: MAE=6.0, Bias=+1.01, r=0.273
- Hitters: MAE=5.93 [PASS]
- Pitchers: MAE=6.63 [PASS]

### Pitcher Components
- IP: MAE=0.9, Bias=-0.05
- Ks: MAE=1.51, Bias=+0.01
- ER: MAE=1.21, Bias=-0.55

### Multiplier Effectiveness

## Slate Review — 2026-06-08 / main

- **Pool**: 10000 lineups, avg actual=88.5, cash line=86.7, GPP line=170.9, best=215.9
- **Proj accuracy**: r=0.263, MAE=25.5, bias=+14.8
- **Overlap**: 0/20 top-by-proj were actual winners
- **Best strategy**: PMS (highest avg_pms)
- **Top stack**: ATH (avg actual=146.6, 5.6% exposure)
- **Biggest bust**: Kyle Harrison (proj=17.6, actual=-8.8, 18% exp)
- **Biggest missed opp**: Tyler Soderstrom (actual=39.0, 3.7% exp)


## Ownership Calibration — 42 game dates

- **Matched players**: 7295 (2 0%-actual ghosts excluded)
- **Bias**: +2.40% (positive = over-project ownership)
- **MAE**: 4.87%
- **Correlation**: r=0.635

- Chalk (>20% actual): n=399, bias=-4.94%, MAE=11.92%
- Mid (5-20% actual): n=2339, bias=+1.18%, MAE=5.27%
- Low (<5% actual): n=4557, bias=+3.67%, MAE=4.05%

**Over-projected:**
- Mason Fluharty: proj=71.4% actual=2.4%
- Brendan Donovan: proj=60.3% actual=0.2%
- Daylen Lile: proj=58.4% actual=2.9%
- Drew Millas: proj=54.2% actual=1.8%
- pid:571510: proj=54.1% actual=4.3%

**Under-projected:**
- pid:656849: proj=23.0% actual=63.0%
- pid:690997: proj=19.4% actual=58.4%
- Ryan Weathers: proj=5.0% actual=42.7%
- Chad Patrick: proj=10.8% actual=47.2%
- Jacob Misiorowski: proj=19.7% actual=56.1%

## Leverage Analysis — 2026-06-09 (58 contests, 4712 players)

**Dataset**: 4258 hitters, 454 pitchers across 50 dates
**Leverage hits**: 564 (12.0%) | **Chalk traps**: 112 (2.4%) | **Ceiling hits**: 495 (10.5%)

### Hitter Predictors (correlation with outperformance)
- `wRC+` r=+0.142 (n=4258)
- `ISO` r=+0.118 (n=4258)
- `Salary` r=+0.094 (n=4258)
- `xwOBA` r=+0.092 (n=4258)
- `Barrel%` r=+0.056 (n=4257)

### Pitcher Predictors
- `K%` r=+0.206 (n=454)
- `Salary` r=+0.160 (n=454)
- `xFIP` r=-0.183 (n=454)
- `Win Prob` r=+0.033 (n=454)
- `Stuff+` r=+0.156 (n=454)

### Actionable Rules
- **Hitter leverage**: ISO > 0.200 AND own < 10% -> 27% leverage rate (+17pp vs base)
- **Hitter leverage**: wRC+ > 120 AND own < 10% -> 22% leverage rate (+12pp vs base)
- **Hitter trap**: Own > 20% AND K% > 0.28 -> 68% chalk trap rate
- **Pitcher leverage**: K% > 0.25 AND own < 15% -> 33% leverage rate (+17pp vs base)
- **Pitcher leverage**: Stuff+ > 105 AND own < 15% -> 31% leverage rate (+14pp vs base)
- **Pitcher trap**: Own > 25% AND Stuff+ < 100 -> 59% chalk trap rate
- **Pitcher trap**: Own > 25% AND xFIP > 4.0 -> 56% chalk trap rate

## Winner Pattern Analysis — 2026-06-09 (82 contests, 1,185,133 entries)
- Top 1% profile: 137% total own, 3.8 booms, 0.7 busts, 48 pitcher pts
- Target: ownership 100-150%, 3+ booms, <1 bust, 25+ pitcher pts

## Research Findings — 2026-06-03, 2026-06-04, 2026-06-05, 2026-06-06, 2026-06-07, 2026-06-08, 2026-06-09, 2026-06-10

**Projection**: MAE=6.14, Bias=+1.41, Hitter MAE=5.99, Pitcher MAE=7.49
**Pool**: MAE=53.25, Bias=+52.88
**Contest**: Winner=184.85843373493975, Top1%=151.75903608433734

### Predictive Diagnostics

**Pitcher Missing Predictors** (correlated with error but not in model):
- `gb_pct` r=+0.210 (n=135)
- `whip` r=-0.127 (n=135)
- `era` r=-0.119 (n=135)
- `bb9` r=-0.117 (n=135)
- `w` r=+0.108 (n=135)

**Opposing Lineup Factors** (for pitcher projections):
- `opp_k_pct` r=-0.227 (n=152)
- `opp_gb_pct` r=+0.210 (n=152)
- `opp_xwoba` r=+0.183 (n=152)
- `opp_woba` r=+0.152 (n=152)
- `opp_wrc_plus` r=+0.144 (n=152)

**Optimal Context Weights**: Vegas=80% Park=15% Weather=5% (saves 0.01 MAE)

**Archetype Biases:**
- Power (ISO>.200): over-projected by 1.6 pts (n=258)
- Contact (K%<15%): over-projected by 1.1 pts (n=326)
- High barrel (>10%): over-projected by 1.4 pts (n=480)
- Speed (SB pace>15): over-projected by 1.8 pts (n=355)

**Recommendations:**
- PROJECTION: increase context multiplier weights — hitter bias is -1.42 pts
- POOL: Best performing stack config is 4-0 — increase its weight in STACK_CONFIGS
- POOL: Projections have 12.6 pt spread — use projection rank as primary sort for portfolio selection
- CONTEST: Avg Top 1% threshold is 151.8 pts across 83 contests
- CONTEST: Avg cash line is 112.0 pts — pool floor should exceed this
- CONTEST: Avg winner scores 184.9 pts — need high-ceiling correlated stacks
- TRACKING: Run this analysis daily to build sample size — patterns stabilize after 2+ weeks

## Sim Validation — 2026-06-09

### Distribution Calibration
- **Hitters**: 85.9% in P10-P90 [WARN] (below floor=8.6%, above ceiling=5.6%)
- **Pitchers**: 73.3% in P10-P90 [WARN] (below floor=23.3%, above ceiling=3.3%)

### Projection Accuracy
- Overall: MAE=5.87, Bias=+1.00, r=0.232
- Hitters: MAE=5.83 [PASS]
- Pitchers: MAE=6.24 [PASS]

### Pitcher Components
- IP: MAE=0.68, Bias=-0.33
- Ks: MAE=1.46, Bias=+0.04
- ER: MAE=1.42, Bias=-0.65

### Multiplier Effectiveness

## Slate Review — 2026-06-09 / main

- **Pool**: 10000 lineups, avg actual=88.8, cash line=88.2, GPP line=146.5, best=176.5
- **Proj accuracy**: r=0.113, MAE=24.7, bias=+18.1
- **Overlap**: 0/20 top-by-proj were actual winners
- **Best strategy**: PMS (highest avg_pms)
- **Top stack**: LAA (avg actual=110.0, 2.9% exposure)
- **Biggest bust**: Kai-Wei Teng (proj=15.2, actual=3.0, 26% exp)
- **Biggest missed opp**: Jac Caglianone (actual=40.0, 9.3% exp)


## Ownership Calibration — 42 game dates

- **Matched players**: 7295 (2 0%-actual ghosts excluded)
- **Bias**: +2.40% (positive = over-project ownership)
- **MAE**: 4.87%
- **Correlation**: r=0.635

- Chalk (>20% actual): n=399, bias=-4.94%, MAE=11.92%
- Mid (5-20% actual): n=2339, bias=+1.18%, MAE=5.27%
- Low (<5% actual): n=4557, bias=+3.67%, MAE=4.05%

**Over-projected:**
- Mason Fluharty: proj=71.4% actual=2.4%
- Brendan Donovan: proj=60.3% actual=0.2%
- Daylen Lile: proj=58.4% actual=2.9%
- Drew Millas: proj=54.2% actual=1.8%
- Matthew Boyd: proj=54.1% actual=4.3%

**Under-projected:**
- David Peterson: proj=23.0% actual=63.0%
- Nolan McLean: proj=19.4% actual=58.4%
- Ryan Weathers: proj=5.0% actual=42.7%
- Chad Patrick: proj=10.8% actual=47.2%
- Jacob Misiorowski: proj=19.7% actual=56.1%

## Leverage Analysis — 2026-06-10 (59 contests, 5584 players)

**Dataset**: 5043 hitters, 541 pitchers across 51 dates
**Leverage hits**: 599 (10.7%) | **Chalk traps**: 103 (1.8%) | **Ceiling hits**: 546 (9.8%)

### Hitter Predictors (correlation with outperformance)
- `wRC+` r=+0.144 (n=5042)
- `ISO` r=+0.110 (n=5042)
- `Salary` r=+0.102 (n=5043)
- `xwOBA` r=+0.095 (n=5042)
- `Barrel%` r=+0.054 (n=5042)

### Pitcher Predictors
- `K%` r=+0.196 (n=541)
- `Salary` r=+0.178 (n=541)
- `xFIP` r=-0.089 (n=541)
- `Win Prob` r=+0.099 (n=541)
- `Stuff+` r=+0.130 (n=540)

### Actionable Rules
- **Hitter leverage**: ISO > 0.200 AND own < 10% -> 27% leverage rate (+17pp vs base)
- **Hitter leverage**: wRC+ > 120 AND own < 10% -> 22% leverage rate (+12pp vs base)
- **Hitter trap**: Own > 20% AND K% > 0.28 -> 68% chalk trap rate
- **Pitcher leverage**: K% > 0.25 AND own < 15% -> 33% leverage rate (+17pp vs base)
- **Pitcher leverage**: Stuff+ > 105 AND own < 15% -> 31% leverage rate (+14pp vs base)
- **Pitcher trap**: Own > 25% AND Stuff+ < 100 -> 59% chalk trap rate
- **Pitcher trap**: Own > 25% AND xFIP > 4.0 -> 56% chalk trap rate

## Winner Pattern Analysis — 2026-06-10 (83 contests, 1,194,595 entries)
- Top 1% profile: 137% total own, 3.8 booms, 0.7 busts, 48 pitcher pts
- Target: ownership 100-150%, 3+ booms, <1 bust, 25+ pitcher pts

## Research Findings — 2026-06-04, 2026-06-05, 2026-06-06, 2026-06-07, 2026-06-08, 2026-06-09, 2026-06-10, 2026-06-11

**Projection**: MAE=6.09, Bias=+1.21, Hitter MAE=5.96, Pitcher MAE=7.27
**Pool**: MAE=55.40, Bias=+55.20
**Contest**: Winner=184.8375, Top1%=151.70416660714287

### Predictive Diagnostics

**Pitcher Missing Predictors** (correlated with error but not in model):
- `w` r=+0.203 (n=132)
- `gb_pct` r=+0.191 (n=132)
- `bb9` r=-0.180 (n=132)
- `ip` r=+0.169 (n=132)
- `l` r=+0.148 (n=132)

**Opposing Lineup Factors** (for pitcher projections):
- `opp_k_pct` r=-0.176 (n=152)
- `opp_xwoba` r=+0.156 (n=152)
- `opp_woba` r=+0.128 (n=152)
- `opp_wrc_plus` r=+0.127 (n=152)

**Optimal Context Weights**: Vegas=80% Park=15% Weather=5% (saves 0.01 MAE)

**Archetype Biases:**
- Power (ISO>.200): over-projected by 1.4 pts (n=255)
- Contact (K%<15%): over-projected by 1.3 pts (n=330)
- High barrel (>10%): over-projected by 1.1 pts (n=478)
- Speed (SB pace>15): over-projected by 1.2 pts (n=359)

**Recommendations:**
- PROJECTION: increase context multiplier weights — hitter bias is -1.24 pts
- POOL: Best performing stack config is 5-0 — increase its weight in STACK_CONFIGS
- POOL: Projections have 14.4 pt spread — use projection rank as primary sort for portfolio selection
- CONTEST: Avg Top 1% threshold is 151.7 pts across 84 contests
- CONTEST: Avg cash line is 111.9 pts — pool floor should exceed this
- CONTEST: Avg winner scores 184.8 pts — need high-ceiling correlated stacks
- TRACKING: Run this analysis daily to build sample size — patterns stabilize after 2+ weeks

## Sim Validation — 2026-06-10

### Distribution Calibration
- **Hitters**: 86.5% in P10-P90 [WARN] (below floor=6.4%, above ceiling=7.1%)
- **Pitchers**: 70.0% in P10-P90 [WARN] (below floor=13.3%, above ceiling=16.7%)

### Projection Accuracy
- Overall: MAE=6.15, Bias=+0.42, r=0.332
- Hitters: MAE=5.95 [PASS]
- Pitchers: MAE=7.96 [WARN]

### Pitcher Components
- IP: MAE=1.0, Bias=-0.51
- Ks: MAE=1.65, Bias=-0.49
- ER: MAE=1.51, Bias=+0.10

### Multiplier Effectiveness

## Slate Review — 2026-06-10 / early

- **Pool**: 10000 lineups, avg actual=104.3, cash line=103.5, GPP line=172.9, best=219.8
- **Proj accuracy**: r=0.332, MAE=23.9, bias=-5.3
- **Overlap**: 0/20 top-by-proj were actual winners
- **Best strategy**: PMS (highest avg_pms)
- **Top stack**: SF (avg actual=117.6, 16.0% exposure)
- **Biggest bust**: Casey Schmitt (proj=9.8, actual=3.0, 27% exp)
- **Biggest missed opp**: Matt Chapman (actual=40.0, 12.6% exp)

## Slate Review — 2026-06-10 / main

- **Pool**: 10000 lineups, avg actual=85.6, cash line=84.3, GPP line=143.0, best=186.3
- **Proj accuracy**: r=0.252, MAE=25.0, bias=+19.4
- **Overlap**: 0/20 top-by-proj were actual winners
- **Best strategy**: PMS (highest avg_pms)
- **Top stack**: PHI (avg actual=118.6, 4.2% exposure)
- **Biggest bust**: Chris Sale (proj=23.2, actual=16.6, 50% exp)
- **Biggest missed opp**: Jesús Luzardo (actual=26.0, 7.2% exp)


## Ownership Calibration — 42 game dates

- **Matched players**: 7295 (2 0%-actual ghosts excluded)
- **Bias**: +2.40% (positive = over-project ownership)
- **MAE**: 4.87%
- **Correlation**: r=0.635

- Chalk (>20% actual): n=399, bias=-4.94%, MAE=11.92%
- Mid (5-20% actual): n=2339, bias=+1.18%, MAE=5.27%
- Low (<5% actual): n=4557, bias=+3.67%, MAE=4.05%

**Over-projected:**
- Mason Fluharty: proj=71.4% actual=2.4%
- Brendan Donovan: proj=60.3% actual=0.2%
- Daylen Lile: proj=58.4% actual=2.9%
- Drew Millas: proj=54.2% actual=1.8%
- Matthew Boyd: proj=54.1% actual=4.3%

**Under-projected:**
- David Peterson: proj=23.0% actual=63.0%
- Nolan McLean: proj=19.4% actual=58.4%
- Ryan Weathers: proj=5.0% actual=42.7%
- Chad Patrick: proj=10.8% actual=47.2%
- Jacob Misiorowski: proj=19.7% actual=56.1%

## Leverage Analysis — 2026-06-11 (60 contests, 4563 players)

**Dataset**: 4125 hitters, 438 pitchers across 52 dates
**Leverage hits**: 471 (10.3%) | **Chalk traps**: 83 (1.8%) | **Ceiling hits**: 440 (9.6%)

### Hitter Predictors (correlation with outperformance)
- `wRC+` r=+0.138 (n=4122)
- `ISO` r=+0.110 (n=4125)
- `Salary` r=+0.099 (n=4125)
- `xwOBA` r=+0.086 (n=4125)
- `Barrel%` r=+0.039 (n=4125)

### Pitcher Predictors
- `K%` r=+0.190 (n=438)
- `Salary` r=+0.182 (n=438)
- `xFIP` r=-0.091 (n=438)
- `Win Prob` r=+0.133 (n=438)
- `Stuff+` r=+0.108 (n=437)

### Actionable Rules
- **Hitter leverage**: ISO > 0.200 AND own < 10% -> 27% leverage rate (+17pp vs base)
- **Hitter leverage**: wRC+ > 120 AND own < 10% -> 22% leverage rate (+12pp vs base)
- **Hitter trap**: Own > 20% AND K% > 0.28 -> 68% chalk trap rate
- **Pitcher leverage**: K% > 0.25 AND own < 15% -> 33% leverage rate (+17pp vs base)
- **Pitcher leverage**: Stuff+ > 105 AND own < 15% -> 31% leverage rate (+14pp vs base)
- **Pitcher trap**: Own > 25% AND Stuff+ < 100 -> 59% chalk trap rate
- **Pitcher trap**: Own > 25% AND xFIP > 4.0 -> 56% chalk trap rate

## Winner Pattern Analysis — 2026-06-11 (84 contests, 1,203,417 entries)
- Top 1% profile: 137% total own, 3.8 booms, 0.7 busts, 48 pitcher pts
- Target: ownership 100-150%, 3+ booms, <1 bust, 25+ pitcher pts

## Research Findings — 2026-06-06, 2026-06-07, 2026-06-08, 2026-06-09, 2026-06-10, 2026-06-11, 2026-06-12, 2026-06-13

**Projection**: MAE=6.17, Bias=+1.25, Hitter MAE=5.98, Pitcher MAE=7.91
**Pool**: MAE=58.85, Bias=+58.62
**Contest**: Winner=185.40232558139536, Top1%=152.01104645348838

### Predictive Diagnostics

**Hitter Missing Predictors** (correlated with error but not in model):
- `hard_hit_pct` r=+0.069 (n=1410)

**Pitcher Missing Predictors** (correlated with error but not in model):
- `w` r=+0.183 (n=155)
- `ip` r=+0.145 (n=155)
- `gb_pct` r=+0.127 (n=155)
- `g` r=+0.126 (n=155)
- `gs` r=+0.117 (n=155)

**Opposing Lineup Factors** (for pitcher projections):
- `opp_k_pct` r=-0.166 (n=177)
- `opp_xwoba` r=+0.119 (n=177)
- `opp_o_swing_pct` r=-0.113 (n=177)

**Optimal Context Weights**: Vegas=80% Park=15% Weather=5% (saves 0.01 MAE)

**Archetype Biases:**
- Power (ISO>.200): over-projected by 1.3 pts (n=297)
- Contact (K%<15%): over-projected by 1.2 pts (n=388)
- High barrel (>10%): over-projected by 1.0 pts (n=565)
- Speed (SB pace>15): over-projected by 1.3 pts (n=414)

**Recommendations:**
- PROJECTION: increase context multiplier weights — hitter bias is -1.23 pts
- POOL: Best performing stack config is 5-0 — increase its weight in STACK_CONFIGS
- POOL: Projections have 18.5 pt spread — use projection rank as primary sort for portfolio selection
- CONTEST: Avg Top 1% threshold is 152.0 pts across 86 contests
- CONTEST: Avg cash line is 112.1 pts — pool floor should exceed this
- CONTEST: Avg winner scores 185.4 pts — need high-ceiling correlated stacks
- TRACKING: Run this analysis daily to build sample size — patterns stabilize after 2+ weeks

## Sim Validation — 2026-06-12

### Distribution Calibration
- **Hitters**: 85.0% in P10-P90 [WARN] (below floor=9.4%, above ceiling=5.6%)
- **Pitchers**: 51.7% in P10-P90 [FAIL] (below floor=27.6%, above ceiling=20.7%)

### Projection Accuracy
- Overall: MAE=6.42, Bias=+1.27, r=0.317
- Hitters: MAE=5.88 [PASS]
- Pitchers: MAE=11.4 [FAIL]

### Pitcher Components
- IP: MAE=1.28, Bias=+0.06
- Ks: MAE=2.2, Bias=-0.06
- ER: MAE=1.8, Bias=-1.04

### Multiplier Effectiveness

## Slate Review — 2026-06-12 / main

- **Pool**: 10000 lineups, avg actual=94.2, cash line=91.3, GPP line=195.2, best=254.3
- **Proj accuracy**: r=0.403, MAE=32.3, bias=+9.8
- **Overlap**: 0/20 top-by-proj were actual winners
- **Best strategy**: PMS (highest avg_pms)
- **Top stack**: BOS (avg actual=144.9, 5.9% exposure)
- **Biggest bust**: Ryan Weathers (proj=18.6, actual=-1.9, 12% exp)
- **Biggest missed opp**: Yordan Alvarez (actual=39.0, 8.0% exp)


## Ownership Calibration — 42 game dates

- **Matched players**: 7295 (2 0%-actual ghosts excluded)
- **Bias**: +2.40% (positive = over-project ownership)
- **MAE**: 4.87%
- **Correlation**: r=0.635

- Chalk (>20% actual): n=399, bias=-4.94%, MAE=11.92%
- Mid (5-20% actual): n=2339, bias=+1.18%, MAE=5.27%
- Low (<5% actual): n=4557, bias=+3.67%, MAE=4.05%

**Over-projected:**
- Mason Fluharty: proj=71.4% actual=2.4%
- Brendan Donovan: proj=60.3% actual=0.2%
- Daylen Lile: proj=58.4% actual=2.9%
- Drew Millas: proj=54.2% actual=1.8%
- Matthew Boyd: proj=54.1% actual=4.3%

**Under-projected:**
- David Peterson: proj=23.0% actual=63.0%
- Nolan McLean: proj=19.4% actual=58.4%
- Ryan Weathers: proj=5.0% actual=42.7%
- Chad Patrick: proj=10.8% actual=47.2%
- Jacob Misiorowski: proj=19.7% actual=56.1%

## Leverage Analysis — 2026-06-13 (62 contests, 8427 players)

**Dataset**: 7625 hitters, 802 pitchers across 54 dates
**Leverage hits**: 940 (11.2%) | **Chalk traps**: 186 (2.2%) | **Ceiling hits**: 797 (9.5%)

### Hitter Predictors (correlation with outperformance)
- `wRC+` r=+0.128 (n=7625)
- `ISO` r=+0.103 (n=7625)
- `xwOBA` r=+0.083 (n=7625)
- `Salary` r=+0.077 (n=7625)
- `Barrel%` r=+0.050 (n=7625)

### Pitcher Predictors
- `K%` r=+0.168 (n=802)
- `Salary` r=+0.172 (n=802)
- `xFIP` r=-0.172 (n=802)
- `Win Prob` r=+0.043 (n=802)
- `Stuff+` r=+0.130 (n=802)

### Actionable Rules
- **Hitter leverage**: ISO > 0.200 AND own < 10% -> 27% leverage rate (+17pp vs base)
- **Hitter leverage**: wRC+ > 120 AND own < 10% -> 22% leverage rate (+12pp vs base)
- **Hitter trap**: Own > 20% AND K% > 0.28 -> 68% chalk trap rate
- **Pitcher leverage**: K% > 0.25 AND own < 15% -> 33% leverage rate (+17pp vs base)
- **Pitcher leverage**: Stuff+ > 105 AND own < 15% -> 31% leverage rate (+14pp vs base)
- **Pitcher trap**: Own > 25% AND Stuff+ < 100 -> 59% chalk trap rate
- **Pitcher trap**: Own > 25% AND xFIP > 4.0 -> 56% chalk trap rate

## Winner Pattern Analysis — 2026-06-13 (86 contests, 1,219,997 entries)
- Top 1% profile: 136% total own, 3.8 booms, 0.7 busts, 48 pitcher pts
- Target: ownership 100-150%, 3+ booms, <1 bust, 25+ pitcher pts

## Research Findings — 2026-06-08, 2026-06-09, 2026-06-10, 2026-06-11, 2026-06-12, 2026-06-13, 2026-06-14, 2026-06-15

**Projection**: MAE=6.18, Bias=+1.05, Hitter MAE=6.00, Pitcher MAE=7.83
**Pool**: MAE=66.94, Bias=+66.86
**Contest**: Winner=185.19540229885058, Top1%=151.79425281609196

### Predictive Diagnostics

**Hitter Missing Predictors** (correlated with error but not in model):
- `hard_hit_pct` r=+0.092 (n=1417)
- `barrel_pct` r=+0.089 (n=1417)
- `avg_ev` r=+0.060 (n=1416)

**Opposing Lineup Factors** (for pitcher projections):
- `opp_k_pct` r=-0.102 (n=177)

**Optimal Context Weights**: Vegas=80% Park=15% Weather=5% (saves 0.01 MAE)

**Archetype Biases:**
- Power (ISO>.200): over-projected by 0.9 pts (n=288)
- Contact (K%<15%): over-projected by 1.1 pts (n=392)
- Speed (SB pace>15): over-projected by 1.3 pts (n=413)

**Recommendations:**
- PROJECTION: increase context multiplier weights — hitter bias is -1.03 pts
- POOL: Best performing stack config is 5-0 — increase its weight in STACK_CONFIGS
- POOL: Projections have 19.9 pt spread — use projection rank as primary sort for portfolio selection
- CONTEST: Avg Top 1% threshold is 151.8 pts across 87 contests
- CONTEST: Avg cash line is 112.0 pts — pool floor should exceed this
- CONTEST: Avg winner scores 185.2 pts — need high-ceiling correlated stacks
- TRACKING: Run this analysis daily to build sample size — patterns stabilize after 2+ weeks

## Sim Validation — 2026-06-14

### Distribution Calibration
- **Hitters**: 82.9% in P10-P90 [PASS] (below floor=9.1%, above ceiling=7.9%)
- **Pitchers**: 64.3% in P10-P90 [FAIL] (below floor=21.4%, above ceiling=14.3%)

### Projection Accuracy
- Overall: MAE=6.49, Bias=+0.99, r=0.223
- Hitters: MAE=6.3 [WARN]
- Pitchers: MAE=8.15 [WARN]

### Pitcher Components
- IP: MAE=1.09, Bias=-0.33
- Ks: MAE=1.97, Bias=+0.47
- ER: MAE=1.52, Bias=-0.38

### Multiplier Effectiveness

## Slate Review — 2026-06-14 / main

- **Pool**: 1500 lineups, avg actual=77.2, cash line=76.7, GPP line=131.9, best=157.6
- **Proj accuracy**: r=0.107, MAE=31.6, bias=+28.2
- **Overlap**: 1/20 top-by-proj were actual winners
- **Best strategy**: PMS (highest avg_pms)
- **Top stack**: WSH (avg actual=108.8, 3.7% exposure)
- **Biggest bust**: Emerson Hancock (proj=14.3, actual=-4.4, 24% exp)
- **Biggest missed opp**: James Wood (actual=28.0, 5.9% exp)


## Ownership Calibration — 42 game dates

- **Matched players**: 7295 (2 0%-actual ghosts excluded)
- **Bias**: +2.40% (positive = over-project ownership)
- **MAE**: 4.87%
- **Correlation**: r=0.635

- Chalk (>20% actual): n=399, bias=-4.94%, MAE=11.92%
- Mid (5-20% actual): n=2339, bias=+1.18%, MAE=5.27%
- Low (<5% actual): n=4557, bias=+3.67%, MAE=4.05%

**Over-projected:**
- Mason Fluharty: proj=71.4% actual=2.4%
- Brendan Donovan: proj=60.3% actual=0.2%
- Daylen Lile: proj=58.4% actual=2.9%
- Drew Millas: proj=54.2% actual=1.8%
- Matthew Boyd: proj=54.1% actual=4.3%

**Under-projected:**
- David Peterson: proj=23.0% actual=63.0%
- Nolan McLean: proj=19.4% actual=58.4%
- Ryan Weathers: proj=5.0% actual=42.7%
- Chad Patrick: proj=10.8% actual=47.2%
- Jacob Misiorowski: proj=19.7% actual=56.1%

## Leverage Analysis — 2026-06-15 (63 contests, 6464 players)

**Dataset**: 5857 hitters, 607 pitchers across 55 dates
**Leverage hits**: 734 (11.4%) | **Chalk traps**: 148 (2.3%) | **Ceiling hits**: 622 (9.6%)

### Hitter Predictors (correlation with outperformance)
- `wRC+` r=+0.121 (n=5856)
- `ISO` r=+0.101 (n=5856)
- `Salary` r=+0.077 (n=5857)
- `xwOBA` r=+0.075 (n=5856)
- `Barrel%` r=+0.048 (n=5856)

### Pitcher Predictors
- `K%` r=+0.201 (n=607)
- `Salary` r=+0.191 (n=607)
- `xFIP` r=-0.208 (n=607)
- `Win Prob` r=+0.099 (n=607)
- `Stuff+` r=+0.129 (n=607)

### Actionable Rules
- **Hitter leverage**: ISO > 0.200 AND own < 10% -> 27% leverage rate (+17pp vs base)
- **Hitter leverage**: wRC+ > 120 AND own < 10% -> 22% leverage rate (+12pp vs base)
- **Hitter trap**: Own > 20% AND K% > 0.28 -> 68% chalk trap rate
- **Pitcher leverage**: K% > 0.25 AND own < 15% -> 33% leverage rate (+17pp vs base)
- **Pitcher leverage**: Stuff+ > 105 AND own < 15% -> 31% leverage rate (+14pp vs base)
- **Pitcher trap**: Own > 25% AND Stuff+ < 100 -> 59% chalk trap rate
- **Pitcher trap**: Own > 25% AND xFIP > 4.0 -> 56% chalk trap rate

## Winner Pattern Analysis — 2026-06-15 (87 contests, 1,230,145 entries)
- Top 1% profile: 136% total own, 3.8 booms, 0.7 busts, 48 pitcher pts
- Target: ownership 100-150%, 3+ booms, <1 bust, 25+ pitcher pts

## Research Findings — 2026-06-09, 2026-06-10, 2026-06-11, 2026-06-12, 2026-06-13, 2026-06-14, 2026-06-15, 2026-06-16

**Projection**: MAE=6.26, Bias=+1.12, Hitter MAE=6.06, Pitcher MAE=8.02
**Pool**: MAE=71.14, Bias=+71.03
**Contest**: Winner=185.78522727272727, Top1%=152.17386357954544

### Predictive Diagnostics

**Hitter Missing Predictors** (correlated with error but not in model):
- `hard_hit_pct` r=+0.091 (n=1448)
- `barrel_pct` r=+0.091 (n=1448)
- `avg_ev` r=+0.062 (n=1447)

**Optimal Context Weights**: Vegas=80% Park=15% Weather=5% (saves 0.01 MAE)

**Archetype Biases:**
- Power (ISO>.200): over-projected by 0.9 pts (n=299)
- Contact (K%<15%): over-projected by 1.1 pts (n=408)
- Speed (SB pace>15): over-projected by 1.5 pts (n=415)

**Recommendations:**
- PROJECTION: increase context multiplier weights — hitter bias is -1.13 pts
- POOL: Best performing stack config is 5-0 — increase its weight in STACK_CONFIGS
- POOL: Projections have 27.5 pt spread — use projection rank as primary sort for portfolio selection
- CONTEST: Avg Top 1% threshold is 152.2 pts across 88 contests
- CONTEST: Avg cash line is 112.2 pts — pool floor should exceed this
- CONTEST: Avg winner scores 185.8 pts — need high-ceiling correlated stacks
- TRACKING: Run this analysis daily to build sample size — patterns stabilize after 2+ weeks

## Sim Validation — 2026-06-15

### Distribution Calibration
- **Hitters**: 82.0% in P10-P90 [PASS] (below floor=10.7%, above ceiling=7.3%)
- **Pitchers**: 75.0% in P10-P90 [PASS] (below floor=20.0%, above ceiling=5.0%)

### Projection Accuracy
- Overall: MAE=6.73, Bias=+1.59, r=0.295
- Hitters: MAE=6.52 [WARN]
- Pitchers: MAE=8.57 [WARN]

### Pitcher Components
- IP: MAE=1.28, Bias=-0.30
- Ks: MAE=2.15, Bias=-0.19
- ER: MAE=1.76, Bias=-0.15

### Multiplier Effectiveness

## Slate Review — 2026-06-15 / main

- **Pool**: 10000 lineups, avg actual=89.2, cash line=86.8, GPP line=166.2, best=210.3
- **Proj accuracy**: r=0.179, MAE=25.1, bias=+11.5
- **Overlap**: 0/20 top-by-proj were actual winners
- **Best strategy**: PMS (highest avg_pms)
- **Top stack**: CIN (avg actual=126.6, 6.8% exposure)
- **Biggest bust**: Shohei Ohtani (proj=12.1, actual=0.0, 9% exp)
- **Biggest missed opp**: Colt Keith (actual=50.0, 1.8% exp)


## Ownership Calibration — 42 game dates

- **Matched players**: 7295 (2 0%-actual ghosts excluded)
- **Bias**: +2.40% (positive = over-project ownership)
- **MAE**: 4.87%
- **Correlation**: r=0.635

- Chalk (>20% actual): n=399, bias=-4.94%, MAE=11.92%
- Mid (5-20% actual): n=2339, bias=+1.18%, MAE=5.27%
- Low (<5% actual): n=4557, bias=+3.67%, MAE=4.05%

**Over-projected:**
- pid:689254: proj=71.4% actual=2.4%
- pid:680977: proj=60.3% actual=0.2%
- Daylen Lile: proj=58.4% actual=2.9%
- Drew Millas: proj=54.2% actual=1.8%
- Matthew Boyd: proj=54.1% actual=4.3%

**Under-projected:**
- David Peterson: proj=23.0% actual=63.0%
- Nolan McLean: proj=19.4% actual=58.4%
- pid:677960: proj=5.0% actual=42.7%
- pid:694477: proj=10.8% actual=47.2%
- pid:694819: proj=19.7% actual=56.1%

## Leverage Analysis — 2026-06-16 (64 contests, 5509 players)

**Dataset**: 5001 hitters, 508 pitchers across 56 dates
**Leverage hits**: 581 (10.5%) | **Chalk traps**: 113 (2.1%) | **Ceiling hits**: 489 (8.9%)

### Hitter Predictors (correlation with outperformance)
- `wRC+` r=+0.106 (n=5001)
- `xwOBA` r=+0.072 (n=5001)
- `ISO` r=+0.071 (n=5001)
- `Salary` r=+0.060 (n=5001)
- `Barrel%` r=+0.033 (n=5001)

### Pitcher Predictors
- `K%` r=+0.173 (n=508)
- `Salary` r=+0.203 (n=508)
- `xFIP` r=-0.172 (n=508)
- `Win Prob` r=+0.048 (n=508)
- `Stuff+` r=+0.162 (n=508)

### Actionable Rules
- **Hitter leverage**: ISO > 0.200 AND own < 10% -> 27% leverage rate (+17pp vs base)
- **Hitter leverage**: wRC+ > 120 AND own < 10% -> 22% leverage rate (+12pp vs base)
- **Hitter trap**: Own > 20% AND K% > 0.28 -> 68% chalk trap rate
- **Pitcher leverage**: K% > 0.25 AND own < 15% -> 33% leverage rate (+17pp vs base)
- **Pitcher leverage**: Stuff+ > 105 AND own < 15% -> 31% leverage rate (+14pp vs base)
- **Pitcher trap**: Own > 25% AND Stuff+ < 100 -> 59% chalk trap rate
- **Pitcher trap**: Own > 25% AND xFIP > 4.0 -> 56% chalk trap rate

## Winner Pattern Analysis — 2026-06-16 (88 contests, 1,275,942 entries)
- Top 1% profile: 136% total own, 3.8 booms, 0.7 busts, 49 pitcher pts
- Target: ownership 100-150%, 3+ booms, <1 bust, 25+ pitcher pts

## Research Findings — 2026-06-10, 2026-06-11, 2026-06-12, 2026-06-13, 2026-06-14, 2026-06-15, 2026-06-16, 2026-06-17

**Projection**: MAE=6.35, Bias=+1.31, Hitter MAE=6.12, Pitcher MAE=8.42
**Pool**: MAE=66.86, Bias=+66.62
**Contest**: Winner=186.0876404494382, Top1%=152.235955

### Predictive Diagnostics

**Hitter Missing Predictors** (correlated with error but not in model):
- `barrel_pct` r=+0.082 (n=1449)
- `hard_hit_pct` r=+0.079 (n=1449)
- `avg_ev` r=+0.053 (n=1447)

**Optimal Context Weights**: Vegas=80% Park=15% Weather=5% (saves 0.01 MAE)

**Archetype Biases:**
- Power (ISO>.200): over-projected by 1.0 pts (n=299)
- Contact (K%<15%): over-projected by 1.6 pts (n=409)
- Strikeout (K%>28%): over-projected by 0.8 pts (n=244)
- High barrel (>10%): over-projected by 1.0 pts (n=568)
- Speed (SB pace>15): over-projected by 1.8 pts (n=415)

**Recommendations:**
- PROJECTION: increase context multiplier weights — hitter bias is -1.40 pts
- POOL: Best performing stack config is 5-0 — increase its weight in STACK_CONFIGS
- POOL: Projections have 23.0 pt spread — use projection rank as primary sort for portfolio selection
- CONTEST: Avg Top 1% threshold is 152.2 pts across 89 contests
- CONTEST: Avg cash line is 112.2 pts — pool floor should exceed this
- CONTEST: Avg winner scores 186.1 pts — need high-ceiling correlated stacks
- TRACKING: Run this analysis daily to build sample size — patterns stabilize after 2+ weeks

## Sim Validation — 2026-06-16

### Distribution Calibration
- **Hitters**: 84.9% in P10-P90 [PASS] (below floor=9.8%, above ceiling=5.3%)
- **Pitchers**: 63.3% in P10-P90 [FAIL] (below floor=26.7%, above ceiling=10.0%)

### Projection Accuracy
- Overall: MAE=6.43, Bias=+2.15, r=0.378
- Hitters: MAE=6.17 [WARN]
- Pitchers: MAE=8.71 [WARN]

### Pitcher Components
- IP: MAE=1.26, Bias=-0.51
- Ks: MAE=1.71, Bias=-0.54
- ER: MAE=1.63, Bias=-0.33

### Multiplier Effectiveness

## Slate Review — 2026-06-16 / main

- **Pool**: 10000 lineups, avg actual=79.0, cash line=76.0, GPP line=159.8, best=193.0
- **Proj accuracy**: r=0.197, MAE=32.5, bias=+23.0
- **Overlap**: 0/20 top-by-proj were actual winners
- **Best strategy**: PMS (highest avg_pms)
- **Top stack**: MIN (avg actual=123.9, 5.9% exposure)
- **Biggest bust**: Davis Martin (proj=14.4, actual=-9.1, 8% exp)
- **Biggest missed opp**: Bryan Reynolds (actual=38.0, 5.3% exp)


## Ownership Calibration — 42 game dates

- **Matched players**: 7295 (2 0%-actual ghosts excluded)
- **Bias**: +2.40% (positive = over-project ownership)
- **MAE**: 4.87%
- **Correlation**: r=0.635

- Chalk (>20% actual): n=399, bias=-4.94%, MAE=11.92%
- Mid (5-20% actual): n=2339, bias=+1.18%, MAE=5.27%
- Low (<5% actual): n=4557, bias=+3.67%, MAE=4.05%

**Over-projected:**
- Mason Fluharty: proj=71.4% actual=2.4%
- Brendan Donovan: proj=60.3% actual=0.2%
- Daylen Lile: proj=58.4% actual=2.9%
- Drew Millas: proj=54.2% actual=1.8%
- Matthew Boyd: proj=54.1% actual=4.3%

**Under-projected:**
- David Peterson: proj=23.0% actual=63.0%
- Nolan McLean: proj=19.4% actual=58.4%
- Ryan Weathers: proj=5.0% actual=42.7%
- Chad Patrick: proj=10.8% actual=47.2%
- Jacob Misiorowski: proj=19.7% actual=56.1%

## Leverage Analysis — 2026-06-17 (65 contests, 8349 players)

**Dataset**: 7572 hitters, 777 pitchers across 57 dates
**Leverage hits**: 937 (11.2%) | **Chalk traps**: 178 (2.1%) | **Ceiling hits**: 797 (9.5%)

### Hitter Predictors (correlation with outperformance)
- `wRC+` r=+0.119 (n=7571)
- `ISO` r=+0.091 (n=7571)
- `xwOBA` r=+0.082 (n=7571)
- `Salary` r=+0.074 (n=7572)
- `Barrel%` r=+0.046 (n=7571)

### Pitcher Predictors
- `K%` r=+0.171 (n=777)
- `Salary` r=+0.191 (n=777)
- `xFIP` r=-0.171 (n=777)
- `Win Prob` r=+0.032 (n=777)
- `Stuff+` r=+0.138 (n=777)

### Actionable Rules
- **Hitter leverage**: ISO > 0.200 AND own < 10% -> 27% leverage rate (+17pp vs base)
- **Hitter leverage**: wRC+ > 120 AND own < 10% -> 22% leverage rate (+12pp vs base)
- **Hitter trap**: Own > 20% AND K% > 0.28 -> 68% chalk trap rate
- **Pitcher leverage**: K% > 0.25 AND own < 15% -> 33% leverage rate (+17pp vs base)
- **Pitcher leverage**: Stuff+ > 105 AND own < 15% -> 31% leverage rate (+14pp vs base)
- **Pitcher trap**: Own > 25% AND Stuff+ < 100 -> 59% chalk trap rate
- **Pitcher trap**: Own > 25% AND xFIP > 4.0 -> 56% chalk trap rate

## Winner Pattern Analysis — 2026-06-17 (89 contests, 1,281,885 entries)
- Top 1% profile: 136% total own, 3.8 booms, 0.7 busts, 49 pitcher pts
- Target: ownership 100-150%, 3+ booms, <1 bust, 25+ pitcher pts

## Sim Validation — 2026-06-16

### Distribution Calibration
- **Hitters**: 84.9% in P10-P90 [PASS] (below floor=9.8%, above ceiling=5.3%)
- **Pitchers**: 63.3% in P10-P90 [FAIL] (below floor=26.7%, above ceiling=10.0%)

### Projection Accuracy
- Overall: MAE=6.43, Bias=+2.15, r=0.378
- Hitters: MAE=6.17 [WARN]
- Pitchers: MAE=8.71 [WARN]

### Pitcher Components
- IP: MAE=1.26, Bias=-0.51
- Ks: MAE=1.71, Bias=-0.54
- ER: MAE=1.63, Bias=-0.33

### Multiplier Effectiveness

## Sim Validation — 2026-06-16

### Distribution Calibration
- **Hitters**: 84.9% in P10-P90 [PASS] (below floor=9.8%, above ceiling=5.3%)
- **Pitchers**: 63.3% in P10-P90 [FAIL] (below floor=26.7%, above ceiling=10.0%)

### Projection Accuracy
- Overall: MAE=6.43, Bias=+2.15, r=0.378
- Hitters: MAE=6.17 [WARN]
- Pitchers: MAE=8.71 [WARN]

### Pitcher Components
- IP: MAE=1.26, Bias=-0.51
- Ks: MAE=1.71, Bias=-0.54
- ER: MAE=1.63, Bias=-0.33

### Multiplier Effectiveness

## Sim Validation — 2026-06-16

### Distribution Calibration
- **Hitters**: 84.9% in P10-P90 [PASS] (below floor=9.8%, above ceiling=5.3%)
- **Pitchers**: 63.3% in P10-P90 [FAIL] (below floor=26.7%, above ceiling=10.0%)

### Projection Accuracy
- Overall: MAE=6.43, Bias=+2.15, r=0.378
- Hitters: MAE=6.17 [WARN]
- Pitchers: MAE=8.71 [WARN]

### Pitcher Components
- IP: MAE=1.26, Bias=-0.51
- Ks: MAE=1.71, Bias=-0.54
- ER: MAE=1.63, Bias=-0.33

### Multiplier Effectiveness

## Sim Validation — 2026-06-16

### Distribution Calibration
- **Hitters**: 90.8% in P10-P90 [WARN] (below floor=2.8%, above ceiling=6.4%)
- **Pitchers**: 67.9% in P10-P90 [WARN] (below floor=21.4%, above ceiling=10.7%)

### Projection Accuracy
- Overall: MAE=6.6, Bias=+2.32, r=0.374
- Hitters: MAE=6.42 [WARN]
- Pitchers: MAE=8.18 [WARN]

### Pitcher Components
- IP: MAE=0.99, Bias=-0.42
- Ks: MAE=1.51, Bias=-0.37
- ER: MAE=1.67, Bias=-0.23

### Multiplier Effectiveness

## Research Findings — 2026-06-13, 2026-06-14, 2026-06-15, 2026-06-16, 2026-06-17, 2026-06-18, 2026-06-19, 2026-06-20

**Projection**: MAE=6.35, Bias=+1.41, Hitter MAE=6.17, Pitcher MAE=7.94
**Pool**: MAE=62.00, Bias=+61.60
**Contest**: Winner=186.28, Top1%=152.40277772222223

### Predictive Diagnostics

**Opposing Lineup Factors** (for pitcher projections):
- `opp_k_pct` r=+0.110 (n=151)

**Optimal Context Weights**: Vegas=80% Park=15% Weather=5% (saves 0.01 MAE)

**Archetype Biases:**
- Power (ISO>.200): over-projected by 1.5 pts (n=243)
- Contact (K%<15%): over-projected by 1.9 pts (n=341)
- Strikeout (K%>28%): over-projected by 1.1 pts (n=194)
- High barrel (>10%): over-projected by 1.3 pts (n=467)
- Speed (SB pace>15): over-projected by 1.8 pts (n=343)

**Recommendations:**
- PROJECTION: increase context multiplier weights — hitter bias is -1.55 pts
- POOL: Best performing stack config is 4-0 — increase its weight in STACK_CONFIGS
- POOL: Projections have 19.6 pt spread — use projection rank as primary sort for portfolio selection
- CONTEST: Avg Top 1% threshold is 152.4 pts across 90 contests
- CONTEST: Avg cash line is 112.3 pts — pool floor should exceed this
- CONTEST: Avg winner scores 186.3 pts — need high-ceiling correlated stacks
- TRACKING: Run this analysis daily to build sample size — patterns stabilize after 2+ weeks

## Sim Validation — 2026-06-19

### Distribution Calibration
- **Hitters**: 90.3% in P10-P90 [WARN] (below floor=2.0%, above ceiling=7.7%)
- **Pitchers**: 67.9% in P10-P90 [WARN] (below floor=21.4%, above ceiling=10.7%)

### Projection Accuracy
- Overall: MAE=6.05, Bias=+0.31, r=0.275
- Hitters: MAE=5.77 [PASS]
- Pitchers: MAE=8.53 [WARN]

### Pitcher Components
- IP: MAE=1.18, Bias=+0.00
- Ks: MAE=2.29, Bias=-0.11
- ER: MAE=1.48, Bias=-0.45

### Multiplier Effectiveness

## Slate Review — 2026-06-19 / main

- **Pool**: 10000 lineups, avg actual=100.0, cash line=99.1, GPP line=163.1, best=210.0
- **Proj accuracy**: r=0.257, MAE=20.5, bias=+3.5
- **Overlap**: 0/20 top-by-proj were actual winners
- **Best strategy**: PMS (highest avg_pms)
- **Top stack**: TEX (avg actual=125.5, 4.2% exposure)
- **Biggest bust**: Jacob Misiorowski (proj=28.3, actual=19.9, 38% exp)
- **Biggest missed opp**: Ty France (actual=39.0, 0.5% exp)


## Ownership Calibration — 42 game dates

- **Matched players**: 7295 (2 0%-actual ghosts excluded)
- **Bias**: +2.40% (positive = over-project ownership)
- **MAE**: 4.87%
- **Correlation**: r=0.635

- Chalk (>20% actual): n=399, bias=-4.94%, MAE=11.92%
- Mid (5-20% actual): n=2339, bias=+1.18%, MAE=5.27%
- Low (<5% actual): n=4557, bias=+3.67%, MAE=4.05%

**Over-projected:**
- Mason Fluharty: proj=71.4% actual=2.4%
- Brendan Donovan: proj=60.3% actual=0.2%
- Daylen Lile: proj=58.4% actual=2.9%
- Drew Millas: proj=54.2% actual=1.8%
- Matthew Boyd: proj=54.1% actual=4.3%

**Under-projected:**
- pid:656849: proj=23.0% actual=63.0%
- pid:690997: proj=19.4% actual=58.4%
- Ryan Weathers: proj=5.0% actual=42.7%
- Chad Patrick: proj=10.8% actual=47.2%
- Jacob Misiorowski: proj=19.7% actual=56.1%

## Leverage Analysis — 2026-06-20 (66 contests, 8300 players)

**Dataset**: 7524 hitters, 776 pitchers across 58 dates
**Leverage hits**: 964 (11.6%) | **Chalk traps**: 163 (2.0%) | **Ceiling hits**: 812 (9.8%)

### Hitter Predictors (correlation with outperformance)
- `wRC+` r=+0.113 (n=7524)
- `ISO` r=+0.089 (n=7524)
- `xwOBA` r=+0.073 (n=7524)
- `Salary` r=+0.064 (n=7524)
- `Barrel%` r=+0.043 (n=7524)

### Pitcher Predictors
- `K%` r=+0.154 (n=776)
- `Salary` r=+0.167 (n=776)
- `xFIP` r=-0.144 (n=776)
- `Win Prob` r=+0.056 (n=776)
- `Stuff+` r=+0.127 (n=776)

### Actionable Rules
- **Hitter leverage**: ISO > 0.200 AND own < 10% -> 27% leverage rate (+17pp vs base)
- **Hitter leverage**: wRC+ > 120 AND own < 10% -> 22% leverage rate (+12pp vs base)
- **Hitter trap**: Own > 20% AND K% > 0.28 -> 68% chalk trap rate
- **Pitcher leverage**: K% > 0.25 AND own < 15% -> 33% leverage rate (+17pp vs base)
- **Pitcher leverage**: Stuff+ > 105 AND own < 15% -> 31% leverage rate (+14pp vs base)
- **Pitcher trap**: Own > 25% AND Stuff+ < 100 -> 59% chalk trap rate
- **Pitcher trap**: Own > 25% AND xFIP > 4.0 -> 56% chalk trap rate

## Winner Pattern Analysis — 2026-06-20 (90 contests, 1,291,343 entries)
- Top 1% profile: 135% total own, 3.8 booms, 0.7 busts, 49 pitcher pts
- Target: ownership 100-150%, 3+ booms, <1 bust, 25+ pitcher pts

## Research Findings — 2026-06-14, 2026-06-15, 2026-06-16, 2026-06-17, 2026-06-18, 2026-06-19, 2026-06-20, 2026-06-21

**Projection**: MAE=6.45, Bias=+1.27, Hitter MAE=6.21, Pitcher MAE=8.63
**Pool**: MAE=60.83, Bias=+60.48
**Contest**: Winner=186.73406593406594, Top1%=152.58846148351648

### Predictive Diagnostics

**Opposing Lineup Factors** (for pitcher projections):
- `opp_k_pct` r=+0.103 (n=149)

**Optimal Context Weights**: Vegas=80% Park=15% Weather=5% (saves 0.00 MAE)

**Archetype Biases:**
- Power (ISO>.200): over-projected by 1.3 pts (n=237)
- Contact (K%<15%): over-projected by 1.5 pts (n=338)
- High barrel (>10%): over-projected by 1.3 pts (n=463)
- Speed (SB pace>15): over-projected by 1.6 pts (n=340)

**Recommendations:**
- PROJECTION: increase context multiplier weights — hitter bias is -1.41 pts
- POOL: Best performing stack config is 4-0 — increase its weight in STACK_CONFIGS
- POOL: Projections have 11.9 pt spread — use projection rank as primary sort for portfolio selection
- CONTEST: Avg Top 1% threshold is 152.6 pts across 91 contests
- CONTEST: Avg cash line is 112.4 pts — pool floor should exceed this
- CONTEST: Avg winner scores 186.7 pts — need high-ceiling correlated stacks
- TRACKING: Run this analysis daily to build sample size — patterns stabilize after 2+ weeks

## Sim Validation — 2026-06-20

### Distribution Calibration
- **Hitters**: 88.9% in P10-P90 [WARN] (below floor=3.3%, above ceiling=7.8%)
- **Pitchers**: 60.7% in P10-P90 [FAIL] (below floor=14.3%, above ceiling=25.0%)

### Projection Accuracy
- Overall: MAE=6.61, Bias=+0.60, r=0.248
- Hitters: MAE=6.27 [WARN]
- Pitchers: MAE=9.56 [FAIL]

### Pitcher Components
- IP: MAE=1.04, Bias=-0.19
- Ks: MAE=1.84, Bias=-0.37
- ER: MAE=1.82, Bias=-0.05

### Multiplier Effectiveness

## Slate Review — 2026-06-20 / early

- **Pool**: 1000 lineups, avg actual=86.4, cash line=86.5, GPP line=137.6, best=155.6
- **Proj accuracy**: r=-0.047, MAE=22.9, bias=+13.9
- **Overlap**: 0/20 top-by-proj were actual winners
- **Best strategy**: PMS (highest avg_pms)
- **Top stack**: SD (avg actual=104.8, 5.4% exposure)
- **Biggest bust**: Colson Montgomery (proj=9.3, actual=0.0, 19% exp)
- **Biggest missed opp**: Ozzie Albies (actual=35.0, 8.1% exp)

## Slate Review — 2026-06-20 / main

- **Pool**: 10000 lineups, avg actual=87.4, cash line=85.2, GPP line=172.0, best=215.6
- **Proj accuracy**: r=0.090, MAE=30.2, bias=+11.5
- **Overlap**: 0/20 top-by-proj were actual winners
- **Best strategy**: PMS (highest avg_pms)
- **Top stack**: PHI (avg actual=139.7, 2.9% exposure)
- **Biggest bust**: Freddy Peralta (proj=15.6, actual=-16.6, 19% exp)
- **Biggest missed opp**: Kyle Schwarber (actual=53.0, 2.2% exp)


## Ownership Calibration — 42 game dates

- **Matched players**: 7295 (2 0%-actual ghosts excluded)
- **Bias**: +2.40% (positive = over-project ownership)
- **MAE**: 4.87%
- **Correlation**: r=0.635

- Chalk (>20% actual): n=399, bias=-4.94%, MAE=11.92%
- Mid (5-20% actual): n=2339, bias=+1.18%, MAE=5.27%
- Low (<5% actual): n=4557, bias=+3.67%, MAE=4.05%

**Over-projected:**
- Mason Fluharty: proj=71.4% actual=2.4%
- Brendan Donovan: proj=60.3% actual=0.2%
- Daylen Lile: proj=58.4% actual=2.9%
- Drew Millas: proj=54.2% actual=1.8%
- Matthew Boyd: proj=54.1% actual=4.3%

**Under-projected:**
- David Peterson: proj=23.0% actual=63.0%
- Nolan McLean: proj=19.4% actual=58.4%
- Ryan Weathers: proj=5.0% actual=42.7%
- Chad Patrick: proj=10.8% actual=47.2%
- Jacob Misiorowski: proj=19.7% actual=56.1%

## Leverage Analysis — 2026-06-21 (67 contests, 4955 players)

**Dataset**: 4502 hitters, 453 pitchers across 59 dates
**Leverage hits**: 531 (10.7%) | **Chalk traps**: 101 (2.0%) | **Ceiling hits**: 468 (9.4%)

### Hitter Predictors (correlation with outperformance)
- `wRC+` r=+0.107 (n=4502)
- `ISO` r=+0.084 (n=4502)
- `xwOBA` r=+0.067 (n=4502)
- `Salary` r=+0.062 (n=4502)
- `Barrel%` r=+0.046 (n=4502)

### Pitcher Predictors
- `K%` r=+0.130 (n=453)
- `Salary` r=+0.123 (n=453)
- `xFIP` r=-0.115 (n=453)
- `Win Prob` r=+0.056 (n=453)
- `Stuff+` r=+0.118 (n=453)

### Actionable Rules
- **Hitter leverage**: ISO > 0.200 AND own < 10% -> 27% leverage rate (+17pp vs base)
- **Hitter leverage**: wRC+ > 120 AND own < 10% -> 22% leverage rate (+12pp vs base)
- **Hitter trap**: Own > 20% AND K% > 0.28 -> 68% chalk trap rate
- **Pitcher leverage**: K% > 0.25 AND own < 15% -> 33% leverage rate (+17pp vs base)
- **Pitcher leverage**: Stuff+ > 105 AND own < 15% -> 31% leverage rate (+14pp vs base)
- **Pitcher trap**: Own > 25% AND Stuff+ < 100 -> 59% chalk trap rate
- **Pitcher trap**: Own > 25% AND xFIP > 4.0 -> 56% chalk trap rate

## Winner Pattern Analysis — 2026-06-21 (91 contests, 1,298,376 entries)
- Top 1% profile: 135% total own, 3.8 booms, 0.7 busts, 49 pitcher pts
- Target: ownership 100-150%, 3+ booms, <1 bust, 25+ pitcher pts

## Research Findings — 2026-06-14, 2026-06-15, 2026-06-16, 2026-06-17, 2026-06-18, 2026-06-19, 2026-06-20, 2026-06-21

**Projection**: MAE=6.41, Bias=+1.28, Hitter MAE=6.17, Pitcher MAE=8.51
**Pool**: MAE=72.73, Bias=+72.68
**Contest**: Winner=187.09782608695653, Top1%=152.89891298913045

### Predictive Diagnostics

**Opposing Lineup Factors** (for pitcher projections):
- `opp_avg_ev` r=-0.111 (n=176)

**Optimal Context Weights**: Vegas=80% Park=15% Weather=5% (saves 0.01 MAE)

**Archetype Biases:**
- Power (ISO>.200): over-projected by 1.2 pts (n=277)
- Contact (K%<15%): over-projected by 1.5 pts (n=399)
- Strikeout (K%>28%): over-projected by 0.9 pts (n=230)
- High barrel (>10%): over-projected by 1.2 pts (n=546)
- Speed (SB pace>15): over-projected by 1.6 pts (n=400)

**Recommendations:**
- PROJECTION: increase context multiplier weights — hitter bias is -1.36 pts
- POOL: Best performing stack config is 4-2 — increase its weight in STACK_CONFIGS
- POOL: Projections have 12.8 pt spread — use projection rank as primary sort for portfolio selection
- CONTEST: Avg Top 1% threshold is 152.9 pts across 92 contests
- CONTEST: Avg cash line is 112.5 pts — pool floor should exceed this
- CONTEST: Avg winner scores 187.1 pts — need high-ceiling correlated stacks
- TRACKING: Run this analysis daily to build sample size — patterns stabilize after 2+ weeks

## Sim Validation — 2026-06-21

### Distribution Calibration
- **Hitters**: 92.6% in P10-P90 [FAIL] (below floor=2.1%, above ceiling=5.4%)
- **Pitchers**: 77.8% in P10-P90 [PASS] (below floor=14.8%, above ceiling=7.4%)

### Projection Accuracy
- Overall: MAE=6.14, Bias=+1.35, r=0.295
- Hitters: MAE=5.95 [PASS]
- Pitchers: MAE=7.82 [WARN]

### Pitcher Components
- IP: MAE=1.19, Bias=+0.07
- Ks: MAE=1.78, Bias=+0.43
- ER: MAE=1.62, Bias=-0.69

### Multiplier Effectiveness

## Slate Review — 2026-06-21 / main

- **Pool**: 10000 lineups, avg actual=87.0, cash line=84.2, GPP line=170.4, best=222.9
- **Proj accuracy**: r=0.453, MAE=27.7, bias=+11.1
- **Overlap**: 2/20 top-by-proj were actual winners
- **Best strategy**: PMS (highest avg_pms)
- **Top stack**: STL (avg actual=132.1, 8.5% exposure)
- **Biggest bust**: Dustin May (proj=15.3, actual=-7.7, 19% exp)
- **Biggest missed opp**: JJ Wetherholt (actual=37.0, 9.7% exp)


## Ownership Calibration — 42 game dates

- **Matched players**: 7295 (2 0%-actual ghosts excluded)
- **Bias**: +2.40% (positive = over-project ownership)
- **MAE**: 4.87%
- **Correlation**: r=0.635

- Chalk (>20% actual): n=399, bias=-4.94%, MAE=11.92%
- Mid (5-20% actual): n=2339, bias=+1.18%, MAE=5.27%
- Low (<5% actual): n=4557, bias=+3.67%, MAE=4.05%

**Over-projected:**
- Mason Fluharty: proj=71.4% actual=2.4%
- Brendan Donovan: proj=60.3% actual=0.2%
- Daylen Lile: proj=58.4% actual=2.9%
- Drew Millas: proj=54.2% actual=1.8%
- Matthew Boyd: proj=54.1% actual=4.3%

**Under-projected:**
- David Peterson: proj=23.0% actual=63.0%
- Nolan McLean: proj=19.4% actual=58.4%
- Ryan Weathers: proj=5.0% actual=42.7%
- Chad Patrick: proj=10.8% actual=47.2%
- Jacob Misiorowski: proj=19.7% actual=56.1%

## Leverage Analysis — 2026-06-21 (68 contests, 6354 players)

**Dataset**: 5747 hitters, 607 pitchers across 60 dates
**Leverage hits**: 748 (11.8%) | **Chalk traps**: 123 (1.9%) | **Ceiling hits**: 620 (9.8%)

### Hitter Predictors (correlation with outperformance)
- `wRC+` r=+0.120 (n=5747)
- `ISO` r=+0.091 (n=5747)
- `Salary` r=+0.086 (n=5747)
- `xwOBA` r=+0.081 (n=5747)
- `Barrel%` r=+0.038 (n=5746)

### Pitcher Predictors
- `K%` r=+0.173 (n=607)
- `Salary` r=+0.157 (n=607)
- `xFIP` r=-0.184 (n=607)
- `Win Prob` r=+0.037 (n=607)
- `Stuff+` r=+0.112 (n=607)

### Actionable Rules
- **Hitter leverage**: ISO > 0.200 AND own < 10% -> 27% leverage rate (+17pp vs base)
- **Hitter leverage**: wRC+ > 120 AND own < 10% -> 22% leverage rate (+12pp vs base)
- **Hitter trap**: Own > 20% AND K% > 0.28 -> 68% chalk trap rate
- **Pitcher leverage**: K% > 0.25 AND own < 15% -> 33% leverage rate (+17pp vs base)
- **Pitcher leverage**: Stuff+ > 105 AND own < 15% -> 31% leverage rate (+14pp vs base)
- **Pitcher trap**: Own > 25% AND Stuff+ < 100 -> 59% chalk trap rate
- **Pitcher trap**: Own > 25% AND xFIP > 4.0 -> 56% chalk trap rate

## Winner Pattern Analysis — 2026-06-21 (92 contests, 1,307,536 entries)
- Top 1% profile: 135% total own, 3.8 booms, 0.7 busts, 49 pitcher pts
- Target: ownership 100-150%, 3+ booms, <1 bust, 25+ pitcher pts

## Research Findings — 2026-06-16, 2026-06-17, 2026-06-18, 2026-06-19, 2026-06-20, 2026-06-21, 2026-06-22, 2026-06-23

**Projection**: MAE=6.23, Bias=+1.41, Hitter MAE=5.96, Pitcher MAE=8.62
**Pool**: MAE=57.51, Bias=+57.06
**Contest**: Winner=186.8784946236559, Top1%=152.7892472580645

### Predictive Diagnostics

**Opposing Lineup Factors** (for pitcher projections):
- `opp_avg_ev` r=-0.162 (n=152)
- `opp_xwoba` r=-0.145 (n=152)
- `opp_hard_hit_pct` r=-0.136 (n=152)
- `opp_k_pct` r=+0.108 (n=152)
- `opp_o_swing_pct` r=+0.105 (n=152)

**Optimal Context Weights**: Vegas=80% Park=15% Weather=5% (saves 0.01 MAE)

**Archetype Biases:**
- Power (ISO>.200): over-projected by 1.3 pts (n=234)
- Contact (K%<15%): over-projected by 1.9 pts (n=337)
- Strikeout (K%>28%): over-projected by 1.3 pts (n=202)
- High barrel (>10%): over-projected by 1.4 pts (n=477)
- Speed (SB pace>15): over-projected by 1.4 pts (n=352)

**Recommendations:**
- PROJECTION: increase context multiplier weights — hitter bias is -1.62 pts
- POOL: Best performing stack config is 4-2 — increase its weight in STACK_CONFIGS
- POOL: Projections have 25.8 pt spread — use projection rank as primary sort for portfolio selection
- CONTEST: Avg Top 1% threshold is 152.8 pts across 93 contests
- CONTEST: Avg cash line is 112.4 pts — pool floor should exceed this
- CONTEST: Avg winner scores 186.9 pts — need high-ceiling correlated stacks
- TRACKING: Run this analysis daily to build sample size — patterns stabilize after 2+ weeks

## Sim Validation — 2026-06-22

### Distribution Calibration
- **Hitters**: 91.2% in P10-P90 [WARN] (below floor=5.6%, above ceiling=3.3%)
- **Pitchers**: 66.7% in P10-P90 [WARN] (below floor=0.0%, above ceiling=33.3%)

### Projection Accuracy
- Overall: MAE=5.66, Bias=+2.03, r=0.461
- Hitters: MAE=5.3 [PASS]
- Pitchers: MAE=8.86 [WARN]

### Pitcher Components
- IP: MAE=1.4, Bias=-0.79
- Ks: MAE=1.9, Bias=-0.77
- ER: MAE=1.44, Bias=+0.81

### Multiplier Effectiveness

## Slate Review — 2026-06-22 / main

- **Pool**: 10000 lineups, avg actual=61.4, cash line=60.2, GPP line=123.3, best=179.7
- **Proj accuracy**: r=-0.041, MAE=40.7, bias=+38.6
- **Overlap**: 0/20 top-by-proj were actual winners
- **Best strategy**: PMS (highest avg_pms)
- **Top stack**: TOR (avg actual=92.3, 2.5% exposure)
- **Biggest bust**: Shota Imanaga (proj=16.6, actual=0.0, 15% exp)
- **Biggest missed opp**: Anthony Kay (actual=26.5, 3.3% exp)


## Ownership Calibration — 42 game dates

- **Matched players**: 7295 (2 0%-actual ghosts excluded)
- **Bias**: +2.40% (positive = over-project ownership)
- **MAE**: 4.87%
- **Correlation**: r=0.635

- Chalk (>20% actual): n=399, bias=-4.94%, MAE=11.92%
- Mid (5-20% actual): n=2339, bias=+1.18%, MAE=5.27%
- Low (<5% actual): n=4557, bias=+3.67%, MAE=4.05%

**Over-projected:**
- Mason Fluharty: proj=71.4% actual=2.4%
- pid:680977: proj=60.3% actual=0.2%
- Daylen Lile: proj=58.4% actual=2.9%
- Drew Millas: proj=54.2% actual=1.8%
- Matthew Boyd: proj=54.1% actual=4.3%

**Under-projected:**
- David Peterson: proj=23.0% actual=63.0%
- Nolan McLean: proj=19.4% actual=58.4%
- Ryan Weathers: proj=5.0% actual=42.7%
- Chad Patrick: proj=10.8% actual=47.2%
- Jacob Misiorowski: proj=19.7% actual=56.1%

## Leverage Analysis — 2026-06-23 (69 contests, 6672 players)

**Dataset**: 6046 hitters, 626 pitchers across 61 dates
**Leverage hits**: 732 (11.0%) | **Chalk traps**: 111 (1.7%) | **Ceiling hits**: 624 (9.4%)

### Hitter Predictors (correlation with outperformance)
- `wRC+` r=+0.091 (n=6045)
- `ISO` r=+0.074 (n=6045)
- `xwOBA` r=+0.053 (n=6045)
- `Salary` r=+0.050 (n=6046)
- `Barrel%` r=+0.027 (n=6045)

### Pitcher Predictors
- `K%` r=+0.174 (n=626)
- `Salary` r=+0.176 (n=626)
- `xFIP` r=-0.174 (n=626)
- `Win Prob` r=+0.060 (n=626)
- `Stuff+` r=+0.159 (n=626)

### Actionable Rules
- **Hitter leverage**: ISO > 0.200 AND own < 10% -> 27% leverage rate (+17pp vs base)
- **Hitter leverage**: wRC+ > 120 AND own < 10% -> 22% leverage rate (+12pp vs base)
- **Hitter trap**: Own > 20% AND K% > 0.28 -> 68% chalk trap rate
- **Pitcher leverage**: K% > 0.25 AND own < 15% -> 33% leverage rate (+17pp vs base)
- **Pitcher leverage**: Stuff+ > 105 AND own < 15% -> 31% leverage rate (+14pp vs base)
- **Pitcher trap**: Own > 25% AND Stuff+ < 100 -> 59% chalk trap rate
- **Pitcher trap**: Own > 25% AND xFIP > 4.0 -> 56% chalk trap rate

## Winner Pattern Analysis — 2026-06-23 (93 contests, 1,317,014 entries)
- Top 1% profile: 135% total own, 3.8 booms, 0.7 busts, 49 pitcher pts
- Target: ownership 100-150%, 3+ booms, <1 bust, 25+ pitcher pts
