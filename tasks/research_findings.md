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
