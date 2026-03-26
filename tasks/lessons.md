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

### Multi-position eligibility lost when CSV fetch fails
**What happened:** DK CSV (`getavailableplayerscsv`) has correct multi-position eligibility (e.g. "2B/SS"), but the URL needs auth and often fails silently. Fallback to the draftables API only gives the single primary position. Additionally, the dedup logic was skipping duplicate entries for the same player instead of merging their positions.
**Rule:** DK lists multi-eligible players as separate draftable entries (same `playerDkId`, different `position`). When deduplicating, merge positions instead of discarding duplicates. Use `merge_positions()` helper at both dedup stages (in-loop and post-loop).

### Salary ID mismatches now auto-fixed by the pipeline
**What happened:** ID mismatches between `lineups.player_id` and `dk_salaries.player_id` blocked the pipeline from reaching phase 3 (projections). Required manual `PLAYER_ID_REMAP` edits.
**Rule:** The validation gate in `agent_lineups_dk.py` now auto-fixes mismatches: (1) updates `dk_salaries` rows in Supabase, (2) adds entries to `PLAYER_ID_REMAP` in `load_dk_salaries.py`, (3) re-validates. Only blocks if auto-fix fails.

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

### Late-Swap: `lockedPlayers` vs game-lock are different concepts
**What happened:** The existing `lockedPlayers` Set is used to force-include a player in the optimizer (100% exposure). Game-locking for late swap is different — it freezes a player in their slot because their game has started.
**Rule:** Use `lateSwapMode` + `isPlayerGameLocked(player)` for game-lock detection. Never conflate with the `lockedPlayers` Set which is the user's force-include feature.

### Late-Swap LP must use remaining budget, not full salary cap
**Rule:** When building the LP for unlocked slots, the salary constraint must be `remaining_salary = optoSalaryCap - lockedSalary`, and position constraints must only include positions needed for the unlocked slots. Stack constraints must account for locked players already contributing to the stack.

### DK entries CSV format — mixed entry + pool rows
**What happened:** The DK entries CSV has entry data in columns 0-13 AND player pool data in columns 14+ on the SAME rows. Some rows have both, some have only pool data.
**Rule:** When exporting late-swapped lineups, walk the original CSV line-by-line and only replace columns 4-13 (the player cells) on entry rows. Preserve everything else exactly as-is. Never rebuild the CSV from scratch — modify in-place.

### Late swap export must preserve original draftable IDs for locked players
**Rule:** When a player is locked (game started), their original draftable ID from the CSV import must be used in the export. Store `_dkDraftableId` on each player object during import. For swapped players, look up the ID from `dkDraftgroupMap` (populated from the CSV pool section).

---

## Salary Pipeline Recurring Issues

### Entire teams missing from dk_salaries — root causes (Session 23)
Three distinct causes. Don't conflate them.

**Cause 1 — DK slate not open yet.**
ATL, KC, MIA, OAK, TOR had zero rows because DK hadn't opened a classic DG for their Opening Day games yet. Not a bug. Re-run `load_dk_salaries.py` once DK opens the DG (typically 24–48 hrs before first pitch) and they appear automatically.
**How to confirm:** Run `check_dk_contest_types.py` and verify the missing team's DG ID appears in `classic_dg_ids`.

**Cause 2 — Wrong player IDs stored (DK proprietary IDs ≠ MLBAM).**
DK returns its own internal `playerId` for many players. Our name lookup resolves to the wrong MLBAM ID and stores that. The `diagnose_salary_mismatch.py` script surfaces these as ID MISMATCH rows.
**Fix:** Add the wrong ID → correct MLBAM ID mapping to `PLAYER_ID_REMAP` in `load_dk_salaries.py`, then re-run the pipeline.
**Rule:** `DK_TO_MLBAM` intercepts at the DK API `playerId` level. `PLAYER_ID_REMAP` intercepts AFTER all resolution (name lookup, DK_TO_MLBAM, fallback) — use this when the wrong ID comes from the players table name lookup.

**Cause 3 — Stale WBC / non-MLB team rows polluting dk_salaries.**
Found CAN, DR, ISR, ITA, MEX, NED, PR, USA, VEN rows from old WBC DFS data. These are never cleared by the pipeline's slate-label delete step (which only clears current slate labels). Fixed in Session 23 by deleting by team abbreviation.
**Rule:** After any bulk pipeline change, verify non-MLB teams aren't in dk_salaries. Delete with: `supabase.table('dk_salaries').delete().eq('team', 'CAN').execute()` etc.

### `diagnose_salary_mismatch.py` silently truncated at 1000 rows (fixed Session 23)
**What happened:** The `all_salary_rows` name-lookup query had no `.limit()`, so Supabase returned only the first 1000 rows. Players in rows 1001+ were misclassified as "truly missing" when they were actually ID mismatches.
**Fix:** Use paginated `.range()` loop to load all rows. Already fixed in the script.
**Rule:** Any diagnostic script that loads a full table must paginate — never use a bare `.select()` without `.range()` or explicit `.limit(5000)`.

### Workflow after adding new PLAYER_ID_REMAP entries
1. Add entry to `PLAYER_ID_REMAP` in `load_dk_salaries.py`
2. Re-run `load_dk_salaries.py` (pipeline re-uploads all current slates with correct IDs)
3. Re-run `diagnose_salary_mismatch.py` to verify — expect 0 ID mismatches
4. If any stale rows with wrong IDs remain from old slate labels not in the current run, fix with the auto-generated SQL from the diagnose output

### load_rosters.py failed during --stats run
**What happened:**     return codecs.charmap_encode(input,self.errors,encoding_table)[0]
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
UnicodeEncodeError: 'charmap' codec can't encode character '\u2713' in position 2: character maps to <undefined>
**Rule:** Check that py -3.12 and all dependencies are installed. Check API availability.

### Projection row count too low after compute_projections.py
**What happened:** Only 20 rows in player_projections for 2026-03-25
**Rule:** Check that load_schedule.py ran successfully and lineups loaded before projections.

### SD optimizer repeated same players in every lineup (Session 25)
**What happened:** Five bugs combined: (1) `Object.entries` produces string keys but `player_id` is numeric — `capExcluded.has(p.player_id)` never matched. (2) Exposure formula divided by `generated.length` (oscillates) instead of using absolute cap like classic (`Math.ceil(N * cap / 100)`). (3) Per-player `expTargets` not checked. (4) No projection randomization (`randPct`) — LP returned identical solution each time. (5) Referenced `optoSettings.maxExp` but the field is `optoSettings.exposureMax`.
**Rule:** When porting logic between classic and SD optimizers, verify: type consistency (string vs number), correct property names, and all diversity mechanisms (noise, exposure caps, per-player caps).

### SD lineup cards were unstyled — wrong CSS class structure (Session 25)
**What happened:** `renderLineupCard_SD` used classes (`lc-row`, `lc-pos`, `lc-name`, etc.) with zero CSS definitions. Classic cards use `lc-chip` / `lc-slot-*` classes which have full styling.
**Rule:** SD lineup cards must use the same `lc-chip` container structure as classic, with SD-specific grid layout (`lc-slot-sd`) for the player rows.

### SD reliever baseline projections were absurdly high (Session 25)
**What happened:** All 52 pitchers in dk_salaries got baseline projections of 2.5 pts/$1K. Only 2 are actual starters with real projections; the other 50 are relievers. A reliever at $10K got 25 pts baseline — higher than Aaron Judge's real 10.7 pts. The LP stuffed lineups with relievers, and solving 180 binary variables made it slow (~1 minute).
**Rule:** In SD mode, pitchers WITHOUT real projections from `player_projections` are relievers. They should get a flat low baseline (~3 pts) reflecting ~1 IP of relief work, not a salary-scaled baseline. Only starters have real projections and should drive lineup construction.

### SD optimizer was MINIMIZING instead of maximizing — wrong jsLPSolver property (Session 25)
**What happened:** `optoBuildLP_Showdown` used `optiType: 'max'` but jsLPSolver expects `opType: 'max'`. With the wrong property name, the solver saw no optimization direction and defaulted to minimization. This produced ~36-point lineups (the WORST possible) with zero pitchers, because the solver actively avoided high-projection players.
**Rule:** jsLPSolver model properties are: `optimize` (string — attribute name), `opType` (string — `'max'` or `'min'`), `constraints`, `variables`, `ints`. NOT `optiType`. Always cross-reference with the classic optimizer model setup when building new optimizer modes.

### optoSolve_Showdown matched wrong keys from solver result (Session 25)
**What happened:** `key.startsWith('c')` matched solver metadata like `cpt_count`. `key.startsWith('f')` matched `flex_count`, `feasible`. Also `byId[stringPid]` failed because map keys were numeric `player_id`.
**Rule:** Use regex `/^([cf])(\d+)$/` to extract PIDs from solver result. Use `String(player_id)` for lookup maps when PIDs come from string sources.

### Odds loader matched zero games — UTC date offset + team name format (Session 26)
**What happened:** `load_odds.py` loaded DB games for `today` only, but late-night US games have UTC commence times one day ahead (e.g. NYY@SF on Mar 25 local = `2026-03-26T00:05:00Z`). Also, the DB stores short team names ("Yankees") but the lookup was comparing Odds API full names ("New York Yankees") against mapped short names.
**Rule:** Always load games for today AND tomorrow when matching odds. Build `db_lookup` indexed by both full and short team name formats.

### SD projections leaked across games — no game_pk scoping (Session 26)
**What happened:** SD projection query filtered by date only, returning all 198 hitter projections from 11 games on the date. Hitters from unrelated games got full projections in the SD pool.
**Rule:** In SD mode, extract `game_pk` from the slate label (`sd_AWAY@HOME_YYYY-MM-DD`) and scope projection queries with `.eq('game_pk', sdGamePk)`.

### SD exposure settings not binding to settings drawer (Session 26)
**What happened:** SD optimizer used `optoSettings.exposureMax` (hardcoded 100, no UI binding) instead of `expHitterMax`/`expPitcherMax` which are bound to the settings drawer inputs.
**Rule:** SD optimizer must use `expHitterMax` and `expPitcherMax` for exposure caps, not `optoSettings.exposureMax`. Check that variable names match between settings drawer `oninput` handlers and optimizer code.

### SD CPT/FLEX exclusion was shared — needed independent sets (Session 26)
**What happened:** A single `excludedPlayers` Set was used for both CPT and FLEX. Excluding a player in the Utility tab also excluded them from Captain.
**Rule:** SD mode needs separate `excludedCpt` and `excludedFlex` Sets with a `toggleExcludeSD(e, pid, role)` function. LP builder checks role-specific exclusions independently.

### DK ID mismatch: J.T. Realmuto 592663
**What happened:** diagnose_salary_mismatch.py found: J.T. Realmuto                      592663  ⚠ ID MISMATCH — DK has id(s): 548255, 548255  salary: $3,700
**Rule:** Add the wrong_id → correct_mlbam_id mapping to PLAYER_ID_REMAP in load_dk_salaries.py, then re-run load_dk_salaries.py.

### DK ID mismatch: Yohel Pozo 650968
**What happened:** diagnose_salary_mismatch.py found: Yohel Pozo                         650968  ⚠ ID MISMATCH — DK has id(s): 828445, 828445  salary: $2,200
**Rule:** Add the wrong_id → correct_mlbam_id mapping to PLAYER_ID_REMAP in load_dk_salaries.py, then re-run load_dk_salaries.py.

### DK ID mismatch: Fernando Tatis Jr. 665487
**What happened:** diagnose_salary_mismatch.py found: Fernando Tatis Jr.                 665487  ⚠ ID MISMATCH — DK has id(s): 919910, 919910, 919910  salary: $5,600
**Rule:** Add the wrong_id → correct_mlbam_id mapping to PLAYER_ID_REMAP in load_dk_salaries.py, then re-run load_dk_salaries.py.

### DK ID mismatch: Luis Garcia Jr. 671277
**What happened:** diagnose_salary_mismatch.py found: Luis Garcia Jr.                    671277  ⚠ ID MISMATCH — DK has id(s): 962605  salary: $3,300
**Rule:** Add the wrong_id → correct_mlbam_id mapping to PLAYER_ID_REMAP in load_dk_salaries.py, then re-run load_dk_salaries.py.

### DK ID mismatch: Jose Tena 677588
**What happened:** diagnose_salary_mismatch.py found: Jose Tena                          677588  ⚠ ID MISMATCH — DK has id(s): 1118063  salary: $2,200
**Rule:** Add the wrong_id → correct_mlbam_id mapping to PLAYER_ID_REMAP in load_dk_salaries.py, then re-run load_dk_salaries.py.

### DK ID mismatch: Miguel Vargas 678246
**What happened:** diagnose_salary_mismatch.py found: Miguel Vargas                      678246  ⚠ ID MISMATCH — DK has id(s): 1120962  salary: $2,900
**Rule:** Add the wrong_id → correct_mlbam_id mapping to PLAYER_ID_REMAP in load_dk_salaries.py, then re-run load_dk_salaries.py.

### DK ID mismatch: David Fry 681807
**What happened:** diagnose_salary_mismatch.py found: David Fry                          681807  ⚠ ID MISMATCH — DK has id(s): 1118963, 1118963  salary: $3,000
**Rule:** Add the wrong_id → correct_mlbam_id mapping to PLAYER_ID_REMAP in load_dk_salaries.py, then re-run load_dk_salaries.py.

### DK ID mismatch: Angel Martinez 682657
**What happened:** diagnose_salary_mismatch.py found: Angel Martinez                     682657  ⚠ ID MISMATCH — DK has id(s): 1284664, 1284664  salary: $3,100
**Rule:** Add the wrong_id → correct_mlbam_id mapping to PLAYER_ID_REMAP in load_dk_salaries.py, then re-run load_dk_salaries.py.

### DK ID mismatch: Pedro Pages 686780
**What happened:** diagnose_salary_mismatch.py found: Pedro Pages                        686780  ⚠ ID MISMATCH — DK has id(s): 1115760, 1115760  salary: $2,800
**Rule:** Add the wrong_id → correct_mlbam_id mapping to PLAYER_ID_REMAP in load_dk_salaries.py, then re-run load_dk_salaries.py.

### DK ID mismatch: James Wood 695578
**What happened:** diagnose_salary_mismatch.py found: James Wood                         695578  ⚠ ID MISMATCH — DK has id(s): 1316803  salary: $5,300
**Rule:** Add the wrong_id → correct_mlbam_id mapping to PLAYER_ID_REMAP in load_dk_salaries.py, then re-run load_dk_salaries.py.

### DK ID mismatch: Jacob Young 696285
**What happened:** diagnose_salary_mismatch.py found: Jacob Young                        696285  ⚠ ID MISMATCH — DK has id(s): 1318244  salary: $2,500
**Rule:** Add the wrong_id → correct_mlbam_id mapping to PLAYER_ID_REMAP in load_dk_salaries.py, then re-run load_dk_salaries.py.

### DK ID mismatch: ── ID MISMATCHES (these need a fix) ──
**What happened:** diagnose_salary_mismatch.py found: ── ID MISMATCHES (these need a fix) ──
**Rule:** Add the wrong_id → correct_mlbam_id mapping to PLAYER_ID_REMAP in load_dk_salaries.py, then re-run load_dk_salaries.py.

### Auto-fixed DK ID mismatches: Angel Martinez, David Fry, Fernando Tatis Jr., J.T. Realmuto, Jacob Young, James Wood, Jose Tena, Luis Garcia Jr., Miguel Vargas, Pedro Pages, Yohel Pozo
**What happened:** Pipeline auto-fixed 11 salary ID mismatch(es) in dk_salaries and added 11 PLAYER_ID_REMAP entry/entries.
**Rule:** Auto-fix handled it. If the same player keeps appearing, investigate the root cause in the players table.
