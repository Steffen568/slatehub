# SlateHub — Claude Project Guide

> **READ THIS FIRST, EVERY SESSION.** This file contains everything needed to work on SlateHub
> without repeating past mistakes. Do not skip it. Update it whenever a new lesson is learned.

---

## What This Project Is

**SlateHub** is an all-in-one MLB DFS (DraftKings) research tool for fantasy sports and betting.
It has two main parts:

1. **Data Pipeline (Python)** — Scripts that pull MLB data from external sources and load it into
   a Supabase database. Run locally on Windows.
2. **Frontend (HTML/JS)** — Static HTML pages hosted on GitHub Pages that read from Supabase
   and display slates, projections, player stats, and lineup builders.

**Live site:** https://steffen568.github.io/slatehub/

**Supabase backend:** Connected via `SUPABASE_URL` and `SUPABASE_KEY` in `.env` (never commit `.env`).

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Database | Supabase (Postgres) |
| Backend scripts | Python 3.12 |
| Frontend | Vanilla HTML + JavaScript (no framework) |
| Hosting | GitHub Pages (serves from repo root) |
| Python package manager | pip under Python 3.12 |
| Version control | Git → GitHub |

---

## Session Start Checklist

**Do these at the start of every session before writing any code:**

1. `git status` — verify the repo exists and is clean. If `.git` is missing, see "Git Disasters" below.
2. Confirm the branch is `main`.
3. Read any relevant section of this file before touching that area of code.
4. Read the actual file before editing it (never edit blindly).
5. Check `tasks/lessons.md` for any patterns relevant to the current task.

---

## How Claude Should Behave — Working Rules

### Plan Before You Act

For any task that involves **3 or more steps**, or touches **more than one file**, or involves a **database change**:

1. Write out the plan in plain English first — what files will be touched, what will change, and why.
2. **Stop and wait for approval** before writing any code or making any edits.
3. Only proceed once the plan is confirmed.

This prevents going down the wrong path and making a mess that takes longer to undo than it would have taken to just plan first. Quick one-liner fixes don't need a plan — use judgment.

### Verify Before Calling Something Done

Never say a task is complete without proving it works:

- For Python scripts: run the script and confirm the output looks correct.
- For frontend changes: describe what to check on the live site or in the browser.
- For database changes: query the table and confirm the data is what's expected.
- If behavior changed: describe what was different before vs. after.

"It should work" is not verification. Show the evidence.

### Self-Improvement Loop — Capturing Lessons

**After any bug fix or correction**, immediately do both of these before moving on:

1. **Update `tasks/lessons.md`** — add the new lesson under the right section heading with:
   - What went wrong
   - Why it happened
   - The rule that prevents it from happening again

2. **Update `CLAUDE.md`** — if the fix reveals a pattern that should be a permanent rule, add it to the relevant section here.

This is not optional. The whole point of these files is to make each session smarter than the last. If lessons aren't captured, the same bugs will keep happening.

**Format for a new lesson in `tasks/lessons.md`:**
```
### [Short title of what went wrong]
**What happened:** [one sentence]
**Rule:** [the rule that prevents it]
```

---

## Critical Rules — Non-Negotiable

### Python

- **Always run scripts as `py -3.12 script.py`** — never `python`, never `py` alone.
  - `python` or bare `py` uses Python 3.14 which does NOT have `supabase` installed.
  - Install new packages with: `py -3.12 -m pip install <package>`

### Git / GitHub Pages

- **All HTML files and image assets must live at the repo root** — never in subdirectories.
  - GitHub Pages serves from root. Files in `html/` or any subfolder will 404.
  - Correct: `hand-builders-hub.html`, `index.html`, `american_family_field.webp`
  - Wrong: `html/hand-builders-hub.html`
- If files accidentally moved to a subdirectory: `git mv html/filename.html filename.html` for each file.

### Supabase

- **Always add `.limit(5000)`** to any `.select()` on a large table.
  - Default limit is 1000 rows — it truncates silently with no error.
  - Large tables: `dk_salaries`, `player_projections`, `lineups`, `batter_stats`, `batter_splits`
- **Never use `.in()` with more than ~150 IDs** — it breaks silently (URL too long).
  - Use the `batchIn()` helper (defined in each HTML file) to chunk into ≤150-ID batches.
- **Run SQL migrations BEFORE running the pipeline** when adding new columns.
  - If you add a field to an upsert dict in `load_*.py`, first run the `ALTER TABLE` in Supabase SQL editor.
  - Migration SQL files live in the repo as `migrate_*.sql`.
  - Skipping this step: the delete step wipes old data, the insert fails silently, data is gone.

### Database: Correct Column Names

Silent nulls (not errors) happen when you use the wrong column name. Use this table:

| Table | Correct column | Do NOT use |
|-------|---------------|------------|
| `games` | `game_pk` | `game_id` |
| `games` | `game_time_utc` | `game_time` |
| `dk_salaries` | `name` | `player_name` |
| `dk_salaries` | `team` (abbreviation) | `team_id`, `game_date` (don't exist) |
| `lineups` | `batting_order != null` | `is_confirmed` (may not exist) |
| `weather` | `precip_pct` | `precip_prob` |

### Multi-Season Queries

- Always add `.order('season', {ascending:false})` and deduplicate in JS for tables with multiple seasons.

### Frontend File Editing

- **Read the relevant section of a file before any Edit call.** The Edit tool fails if it can't find the exact text.
- For large files, read just the offset/lines needed rather than the whole file.

---

## DraftKings API — Known Gotchas

### Showdown Game Type String

- Filter for `c.get('gameType') == 'Showdown Captain Mode'` — NOT `'Showdown'`
- Other known gameType values: `'Classic'`, `'Best Ball'`, `'Snake'`, `'Snake Showdown'`, `'Single Stat - Home Runs'`, `'Tiers'`

### Showdown CPT vs FLEX Detection

- In DK draftables API and salary CSV, both CPT and FLEX rows show the player's real position (e.g. `'SP'`, `'OF'`). Neither ever says `'CPT'`.
- To keep only FLEX rows: each `playerDkId` appears EXACTLY twice — CPT (1.5× salary) and FLEX (base salary). Keep the lower-salary entry.
- Build `flex_draftable_ids` set before the main loop; skip draftables not in it.

### DraftGroups `Games` Array

- `dg.get('Games')` is always `[]` from the lobby API — don't try to extract team names from it.
- Instead: collect unique `teamAbbreviation` values from the draftables list.

---

## Salary Pipeline — Recurring Issues

### Entire Teams Missing from `dk_salaries`

Three distinct root causes — don't confuse them:

**Cause 1: DK slate not open yet**
- DK hasn't published a Classic DG for those games yet (normal, happens 24–48 hrs before first pitch).
- Confirm: run `check_dk_contest_types.py` and check if the DG ID appears in `classic_dg_ids`.
- Fix: re-run `load_dk_salaries.py` once DK opens the DG.

**Cause 2: Wrong player IDs (DK proprietary IDs ≠ MLBAM)**
- DK returns its own internal `playerId`. Name lookup resolves to the wrong MLBAM ID.
- Diagnosis: run `diagnose_salary_mismatch.py` — ID MISMATCH rows show the problem.
- Fix: add `wrong_id → correct_mlbam_id` to `PLAYER_ID_REMAP` in `load_dk_salaries.py`, then re-run.
- `DK_TO_MLBAM` intercepts at the DK API level. `PLAYER_ID_REMAP` intercepts AFTER all resolution.

**Cause 3: Stale WBC / non-MLB team rows**
- Old WBC DFS data (CAN, DR, ISR, ITA, MEX, NED, PR, USA, VEN) pollutes the table.
- Delete manually: `supabase.table('dk_salaries').delete().eq('team', 'CAN').execute()` etc.
- After any bulk pipeline change, verify non-MLB teams aren't in `dk_salaries`.

### After Adding `PLAYER_ID_REMAP` Entries

1. Add entry to `PLAYER_ID_REMAP` in `load_dk_salaries.py`
2. Re-run `load_dk_salaries.py`
3. Re-run `diagnose_salary_mismatch.py` — expect 0 ID mismatches
4. If stale rows with wrong IDs remain, use the auto-generated SQL from the diagnose output

### Diagnostic Scripts Must Paginate

- Any script loading a full table must use paginated `.range()` loop — never a bare `.select()` without `.range()` or explicit `.limit(5000)`.

---

## Frontend (`_allSlates`) Structure

- `_allSlates` is an array of objects: `{ slate, contestType }` — NOT an array of strings.
- Always destructure: `_allSlates.forEach(({ slate: s, contestType }) => ...)`
- Treating elements as strings will silently break slate filtering.

### `renderSlot` vs `renderSlot_SD`

- `renderSlot(key)` — updates the DOM element in-place (Classic lineup builder).
- `renderSlot_SD(slotDef, player)` — returns an HTML string. Caller must do `element.outerHTML = renderSlot_SD(...)`.

---

## Projection Engine (`compute_projections.py`)

**Three-Tier Framework:**

| Tier | Weight | What It Measures |
|------|--------|-----------------|
| Tier 1 — True Talent | 45% | Marcel-weighted 3yr wOBA + xwOBA luck correction |
| Tier 2 — Matchup | 25% | Pitcher quality (xFIP/Stuff+/K%/BB%) + platoon split + lineup position |
| Tier 3 — Game Context | 30% | Vegas implied runs (58%) + park factor (26%) + weather (16%) |

**Key constants (don't change without understanding the math):**
- `LEAGUE_AVG_WOBA = 0.315`
- `LEAGUE_AVG_XFIP = 3.90`
- `LEAGUE_AVG_IMPLIED = 4.5`
- `SP_CALIBRATION = 0.90` (model runs ~10% hot vs benchmarks)
- `SP_SHARE = 0.60` (SP covers ~60% of batter PA; bullpen 40%)

**Run the engine:**
```
py -3.12 compute_projections.py
py -3.12 compute_projections.py --date 2026-03-26
```

---

## Git Disaster Recovery

### The `.git` Folder Disappears Between Sessions

This has happened before. If `git status` shows "not a git repository":

```bash
git init
git remote add origin https://github.com/Steffen568/slatehub.git
git fetch origin
git reset --soft origin/main
```

Then verify with `git status` before doing anything else.

### Files Accidentally Moved to a Subdirectory

```bash
git mv html/filename.html filename.html
# repeat for each file
git commit -m "Move files back to root"
git push
```

---

## File & Folder Map

```
/ (repo root)
├── CLAUDE.md                  ← this file
├── config.py                  ← auto-detects season (Apr–Dec = current year)
├── compute_projections.py     ← DFS projection engine (3-tier)
├── compute_ownership.py       ← ownership projections
├── refresh_all.py             ← runs all load_*.py scripts in sequence
├── load_*.py                  ← individual data pipeline scripts
├── seed_*.py                  ← one-time database seeding scripts
├── check_*.py                 ← diagnostic / debugging scripts
├── validate_*.py              ← data validation scripts
├── diagnose_salary_mismatch.py← salary ID diagnosis tool
├── migrate_*.sql              ← database schema migrations (run in Supabase SQL editor)
├── fix_*.sql                  ← one-off data fixes
├── index.html                 ← main slate hub page
├── hand-builders-hub.html     ← lineup builder (Classic + Showdown)
├── top-pitchers.html          ← pitcher rankings page
├── top-stacks.html            ← batting stack analysis
├── debug_*.html               ← debugging views (not production)
├── *.png / *.webp             ← stadium images (served at root for GitHub Pages)
├── splits_*.csv               ← local splits data cache
└── tasks/
    └── lessons.md             ← session-by-session lessons log (also update CLAUDE.md)
```

---

## Where to Log New Lessons

**Claude does this automatically after every fix** — you don't need to ask.

After any correction or bug fix, Claude will:
1. Add the lesson to `tasks/lessons.md` immediately (before moving on).
2. Add a permanent rule to the relevant section of `CLAUDE.md` if it's a pattern worth keeping.

**Your only job:** commit and push the updated files after the session so the lessons survive to the next one.

```bash
git add CLAUDE.md tasks/lessons.md
git commit -m "Update lessons from session"
git push
```

The goal is that any future session can read this file and never repeat a past mistake.

---

## Environment Setup (if starting fresh)

```bash
# Install dependencies (Python 3.12 only)
py -3.12 -m pip install supabase python-dotenv pybaseball requests

# .env file (never commit this)
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_KEY=your-anon-or-service-key
```

---

*Last updated: Session 24 — March 2026 (added Plan Mode, Verification, Self-Improvement Loop)*
