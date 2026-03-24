# SlateHub — Lessons Learned

Recurring issues that have burned us. When a new solution is found, add it here immediately.

---

## Git / Deployment

### Files must live at repo root — never in subdirectories
**What happened:** In Session 21, a fresh `git init` picked up the local `html/` directory structure. All HTML and image files moved to `html/` in the commit, breaking GitHub Pages with a 404.
**Rule:** Every HTML file and image asset must be committed at the repo root (`hand-builders-hub.html`, `index.html`, etc. — NOT `html/hand-builders-hub.html`). GitHub Pages serves from root.
**Fix if it happens:** `git mv html/filename.html filename.html` for each file, commit and push.

### The git repo disappears between sessions
**What happened:** The `.git` folder was missing at the start of Session 21 despite being present in prior sessions. Caused a fresh `git init` which orphaned the remote history.
**Rule:** At the start of each session, verify with `git status` before doing anything. If the repo is missing, `git init` + `git remote add origin https://github.com/Steffen568/slatehub.git` + `git fetch origin` + `git reset --soft origin/main`.

---

## DraftKings API

### Showdown gameType is `'Showdown Captain Mode'` — not `'Showdown'`
**What happened:** The pipeline filtered for `gameType == 'Showdown'` and found zero SD contests. The actual value returned by the DK lobby API is `'Showdown Captain Mode'`.
**Rule:** Always filter Showdown contests with `c.get('gameType') == 'Showdown Captain Mode'`.
**Other gameType values seen:** `'Classic'`, `'Best Ball'`, `'Snake'`, `'Snake Showdown'`, `'Single Stat - Home Runs'`, `'Tiers'`.

### Showdown CPT rows cannot be detected by position field
**What happened:** We tried `position == 'CPT'` to skip captain-slot draftables. But in the DK draftables API and salary CSV, both CPT and FLEX entries show the player's real position (`'SP'`, `'OF'`, etc.) — neither ever says `'CPT'`. `cpt_skipped` was always 0.
**Rule:** For Showdown DGs, each `playerDkId` appears EXACTLY twice — once at 1.5× salary (CPT) and once at base salary (FLEX). To keep only FLEX: group by `playerDkId`, keep the entry with the **lower salary**.
**Implementation:** Build `flex_draftable_ids` set before the main loop, then skip any draftable whose `draftableId` is not in that set.

### DK DraftGroups `Games` array is always empty from the lobby API
**What happened:** Tried to extract home/away team names from `dg.get('Games')` to build SD slate labels. It's always `[]`.
**Rule:** Infer team names from the draftables themselves — collect unique `teamAbbreviation` values from the draftable list after fetching them.

---

## Supabase / Database

### Column name gotchas (wrong column = silent null, not an error)
| Table | Use this | NOT this |
|-------|----------|----------|
| `games` | `game_pk` | `game_id` |
| `games` | `game_time_utc` | `game_time` |
| `dk_salaries` | `name` | `player_name` |
| `dk_salaries` | `team` (abbreviation only) | `team_id` / `game_date` (don't exist) |
| `lineups` | `batting_order != null` | `is_confirmed` (may not exist) |
| `weather` | `precip_pct` | `precip_prob` |

### Default row limit is 1000 — always add `.limit(5000)`
**What happened:** Queries silently truncate at 1000 rows. Large tables (dk_salaries, player_projections, lineups) need explicit limit.
**Rule:** Every Supabase `.select()` on a potentially large table gets `.limit(5000)`.

### `.in()` with more than ~150 IDs breaks (URL too long)
**What happened:** Supabase `.in('player_id', bigArray)` fails silently or errors when the array is large.
**Rule:** Use the `batchIn()` helper (defined in each HTML file) to chunk arrays into ≤150-ID batches.

### Multi-season tables return stale data without ordering
**Rule:** `.order('season', {ascending:false})` + deduplicate in JS for any table that stores multiple seasons.

### New columns must be migrated before running the pipeline
**What happened:** Added `contest_type` column to `dk_salaries` in `load_dk_salaries.py` but ran the pipeline before running the SQL migration. The delete step wiped old data, the insert failed silently, and all pricing was lost.
**Rule:** Any time `load_*.py` adds a new field to an upsert dict, run the `ALTER TABLE` migration in Supabase SQL editor FIRST. Keep migration SQL in `migrate_*.sql` files in the repo.

---

## Python Environment

### Must use `py -3.12` — not `python` or `py` alone
**What happened:** Running `python load_dk_salaries.py` uses Python 3.14 which doesn't have `supabase` installed. The supabase package is installed under Python 3.12.
**Rule:** Always run scripts as `py -3.12 script.py`. If adding new packages, install them with `py -3.12 -m pip install <package>`.

---

## Frontend

### Edit tool requires reading the file first
**What happened:** Attempted to edit a file without reading it, causing the Edit tool to fail.
**Rule:** Always `Read` the relevant section of a file before making any `Edit` call. For large files, read just the offset/lines needed.

### `_allSlates` structure changed in Session 22 — now objects, not strings
**What happened:** Originally `_allSlates` was an array of slate label strings. After adding `contest_type`, it became an array of `{slate, contestType}` objects. Any code that treats elements as strings will break.
**Rule:** Always destructure: `_allSlates.forEach(({ slate: s, contestType }) => ...)`.

### `renderSlot_SD` returns HTML string — must use `.outerHTML =` not inner update
**Rule:** Classic `renderSlot(key)` updates the element in-place. SD version `renderSlot_SD(slotDef, player)` returns an HTML string and the caller must do `element.outerHTML = renderSlot_SD(...)`.

---

## Salary Pipeline Recurring Issues

### `diagnose_salary_mismatch.py` does not catch all missing salaries
**Status:** Open as of Session 22. Some players (e.g. Orlando Arcia) and entire teams (ATL, KC) have null pricing even after running the diagnose script. Root cause not yet identified — may be a DK→MLBAM ID issue in `load_dk_salaries.py` that bypasses the override map.
**Next step:** Cross-reference players missing from `dk_salaries` against what DK returns for their draftgroup, check if their `playerDkId` is being resolved to the wrong `player_id`.
