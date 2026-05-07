# SlateHub UI Style Guide

This guide captures the visual system from the redesign mockup so it can be applied to the production site without needing to match the mockup pixel-for-pixel. Hand this file to Claude Code along with `SlateHub Redesign.html` as reference.

---

## 1. Design Philosophy

- **Sportsbook-meets-terminal.** Dense data, near-black backgrounds, electric orange accent, monospaced numerics for tabular alignment.
- **Information hierarchy via color, not ornament.** Hot/Warm/Cold semantic colors do the work — no gradients, no glass, no rounded "cards within cards."
- **Layout matches current arrangement.** TopNav → Slate Selector → Park & Weather (collapsible) → 2-col body (Lineup LEFT, Pitcher RIGHT).

---

## 2. Color Tokens

Replace your current `:root` block with these. All other rules can keep referring to `var(--xxx)` names you already use; the values just shift.

```css
:root {
  /* Backgrounds — true black with cool tilt */
  --bg-0: #000000;        /* page */
  --bg-1: #07090c;        /* recessed surface */
  --bg-2: #0a0d12;        /* nav / status bars */
  --bg-3: #0d1217;        /* card surface */

  /* Borders */
  --line-1: #1a2028;      /* hairline */
  --line-2: #232c38;      /* divider */
  --line-3: #2f3a4a;      /* hover */

  /* Ink (text) */
  --ink-0: #f5f8fb;       /* primary */
  --ink-1: #c7d2dd;       /* secondary */
  --ink-2: #8294a8;       /* tertiary / labels */
  --ink-3: #4a5668;       /* muted micro-copy */
  --ink-4: #2a3340;       /* disabled */

  /* Semantic — favorable / neutral / unfavorable */
  --hot:  #4ade80;        /* green   — edge / hot / elite */
  --warm: #fbbf24;        /* amber   — neutral / playable */
  --cold: #f87171;        /* red     — fade / cold / risk */
  --info: #60a5fa;        /* blue    — informational */

  /* Brand accent — electric orange */
  --accent: #ff7a00;
  --accent-soft: rgba(255,122,0,.15);
  --accent-line: rgba(255,122,0,.45);
}
```

### Existing variable mapping

If your current site uses these variable names, map them to the new ones:

| Old              | New        |
| ---------------- | ---------- |
| `--bg`           | `--bg-0`   |
| `--surface`      | `--bg-1`   |
| `--border`       | `--line-1` |
| `--border2`      | `--line-2` |
| `--text`         | `--ink-0`  |
| `--muted`        | `--ink-2`  |
| `--hi`           | `--hot`    |
| `--mid`          | `--warm`   |
| `--lo`           | `--cold`   |

You don't have to rename — just update the `:root` values. The visual change happens for free.

---

## 3. Typography

```css
@import url('https://fonts.googleapis.com/css2?family=Inter+Tight:wght@400;500;600;700;800;900&family=Geist+Mono:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;500;600;700&display=swap');

:root {
  --font-ui:   'Inter Tight', system-ui, sans-serif;   /* labels, headings */
  --font-num:  'Geist Mono', ui-monospace, monospace;  /* numbers, stats */
  --font-mono: 'JetBrains Mono', ui-monospace;         /* micro-copy, tags */
}

body { font-family: var(--font-ui); font-feature-settings: 'tnum' 1; }
.num { font-family: var(--font-num); font-variant-numeric: tabular-nums; }
.mono { font-family: var(--font-mono); }
```

### Type scale

| Use                | Size | Weight | Family     | Notes                          |
| ------------------ | ---- | ------ | ---------- | ------------------------------ |
| Hero number        | 26-32 | 800   | Geist Mono | matchup bar, grades            |
| Stat value         | 14-18 | 700   | Geist Mono | row cells                      |
| Body / row label   | 13-14 | 700   | Inter Tight | player names                   |
| Section title      | 12   | 800   | Inter Tight, UPPERCASE, +1px ls |               |
| Micro label        | 9-10 | 700   | JetBrains Mono, UPPERCASE, +1.2px ls | "wOBA", "Stuff+" |
| Dense table        | 10-11 | 600   | Geist Mono | arsenal, splits      |

**Letter spacing rules:**
- All-caps labels: `letter-spacing: 1px to 1.4px`
- Hero numbers: `letter-spacing: -0.5px` to `-1px` (tight)
- Body: default

---

## 4. Spacing & Layout

```css
:root {
  --pad-x: 18px;          /* horizontal section padding */
  --pad-y: 10px;          /* vertical section padding */
  --gap-tight: 4px;
  --gap-sm: 8px;
  --gap-md: 12px;
  --gap-lg: 16px;
  --radius-sm: 3px;
  --radius-md: 4px;
  --radius-lg: 6px;
}
```

### Component-specific layouts

- **TopNav:** 52px tall, 1px bottom border, 3px active-tab bottom-border accent
- **Slate Selector Bar:** 48-52px tall, 8px vertical padding, button group with 4px gaps
- **Park & Weather Bar (collapsed):** ~30px toggle row only; (open):** 5-column grid, 14px gap
- **Two-column body:** `grid-template-columns: 1.15fr 1fr` (lineup gets slightly more width — it's the primary surface)
- **Batter row:** 10px vertical padding, 10px column gap. Grid: `32px 220px 56px 56px 56px 56px 56px 1fr 60px 16px`

---

## 5. Component Patterns

### 5a. Section header (collapsible)
```css
.sec-hdr {
  display: flex; align-items: center; justify-content: space-between;
  padding: 10px var(--pad-x);
  border-left: 3px solid transparent;
  cursor: pointer;
}
.sec-hdr.open {
  background: rgba(255,122,0,.04);
  border-left-color: var(--accent); /* or section-specific dot color */
}
.sec-hdr .sec-title {
  font-family: var(--font-ui);
  font-size: 12px; font-weight: 800;
  letter-spacing: 1px; text-transform: uppercase;
  color: var(--ink-0);
  display: flex; align-items: center; gap: 8px;
}
.sec-hdr .pip { width: 6px; height: 6px; border-radius: 50%; }
```

Section accent colors used in the mockup:
- Signal Breakdown: `#fb923c` (orange)
- Rate Stats: `#34d399` (green)
- Pitch Arsenal: `#a78bfa` (purple)
- Arm Angle / Physics: `#60a5fa` (blue)
- Environment & Stack: `#fb923c` (orange)

### 5b. Stat cell (inside a card grid)
```css
.det-cell {
  background: var(--bg-3);
  border: 1px solid var(--line-1);
  border-radius: var(--radius-sm);
  padding: 6px 8px;
}
.det-cell .det-lbl {
  font-family: var(--font-mono);
  font-size: 8px;
  color: var(--ink-3);
  letter-spacing: 0.8px;
  text-transform: uppercase;
}
.det-cell .det-val {
  font-family: var(--font-num);
  font-size: 14px; font-weight: 700;
  color: var(--ink-0);
  margin-top: 2px;
  line-height: 1;
}
.det-cell .det-val.hi { color: var(--hot); }
.det-cell .det-val.lo { color: var(--cold); }
.det-cell .det-ctx {
  font-family: var(--font-mono);
  font-size: 8.5px;
  color: var(--ink-3);
  margin-top: 2px;
}
```

### 5c. Tag / pill
```css
.tag {
  display: inline-block;
  padding: 0 4px;
  border-radius: 2px;
  font-family: var(--font-mono);
  font-size: 8.5px; font-weight: 700;
  letter-spacing: 0.5px;
  border: 1px solid;
}
/* Per-tag color: use the rule
   background: <hue>1a;     (10% alpha)
   color:      <hue>;
   border-color: <hue>33;   (20% alpha)
*/
```

Tag → hue map:
- `CHALK` → `#fbbf24`
- `STAR` → `#a78bfa`
- `PLATOON+`, `PARK+`, `HOT` → `#4ade80`
- `HR-UP` → `#ff7a00`
- `VALUE`, `CONTACT` → `#60a5fa`
- `SB` → `#a78bfa`
- `MIN-PRICE` → `#9aa9b8`

### 5d. Color-coded values

Numbers should pick up status color based on these thresholds:

| Stat       | Hot (green)  | Warm (amber)    | Cold (red)   |
| ---------- | ------------ | --------------- | ------------ |
| wRC+       | ≥ 130        | 100-129         | < 100        |
| wOBA       | ≥ .380       | .320-.379       | < .320       |
| K%         | < 18         | 18-25           | > 25         |
| ISO        | ≥ .220       | .150-.219       | < .150       |
| Barrel%    | ≥ 13         | 8-12            | < 8          |
| HardHit%   | ≥ 48         | 38-47           | < 38         |
| Bat Speed  | ≥ 75         | 70-74           | < 70         |
| Squared-Up%| ≥ 30         | 25-29           | < 25         |
| Attack°    | 8-18 ideal (hot), else warm |   |   |
| PMS        | ≥ 8.0        | 5.0-7.9         | < 5.0        |
| ERA        | < 3.5        | 3.5-4.2         | > 4.2        |
| WHIP       | < 1.15       | 1.15-1.30       | > 1.30       |
| Stuff+/Loc+/Pitching+ | ≥ 110 | 95-109     | < 95         |

---

## 6. Iconography & Decoration

- **No emojis** in production UI (the ⛅ in the park bar can stay or swap for a small SVG).
- **Pulse dots** for live indicators: `6px circle`, `box-shadow: 0 0 6px currentColor`, `animation: pulse 1.4s ease-in-out infinite`.
- **No drop shadows** on cards. Use 1px borders instead.
- **No border-radius > 6px** anywhere. Stay sharp.

---

## 7. Migration Order (recommended)

When pointing Claude Code at this guide, I'd do it in this order so each step is independently shippable:

1. **Token swap** — update `:root` with the new colors + fonts. The site will instantly look closer.
2. **TopNav** — restyle the existing `.sh-topnav` (orange logo accent, accent-tab borders).
3. **Matchup Bar + Stack Bar** — these are the visual headline of the lineup column.
4. **Batter row main** — restyle `.batter-row .br-main` with the new grid + cell typography.
5. **Batter row detail tabs** — restyle `.det-tabs` and `.det-cell` blocks.
6. **Pitcher Hero** — grade box + Stuff/Loc/Pit+ trio.
7. **Pitcher sections** — Arsenal, Arm Angle, Rate Stats.
8. **Slate Selector + Park bar** — last, since they're already functional.

After step 1, you can pause and ship — the rest is incremental.

---

## 8. Files to reference in the mockup

| File                       | What it shows                              |
| -------------------------- | ------------------------------------------ |
| `tokens.css`               | Color/typography variables                 |
| `chrome.jsx`               | TopNav, Slate Selector, Park & Weather Bar |
| `lineup-panel.jsx`         | Matchup bar, stack bar, batter rows + tabs |
| `pitcher-panel.jsx`        | Hero, signal breakdown, arsenal, arm angle |
| `shared-components.jsx`    | Sparkline, ScoreRing, LiveDot, StatCell    |
| `data.js`                  | Field shapes — what data each component expects |

The JSX is React for the mockup, but the HTML/CSS structure translates 1:1 to your existing template-rendered markup. Claude Code can read the JSX as a layout/styling spec and apply it to your existing DOM.
