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
