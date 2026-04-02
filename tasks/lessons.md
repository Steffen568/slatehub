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

### Auto-fixed DK ID mismatches: Ivan Herrera
**What happened:** Pipeline auto-fixed 1 salary ID mismatch(es) in dk_salaries and added 1 PLAYER_ID_REMAP entry/entries.
**Rule:** Auto-fix handled it. If the same player keeps appearing, investigate the root cause in the players table.

### Auto-fixed DK ID mismatches: Carlos Santana, Cole Young, Gabriel Arias, Jacob Wilson, Jesus Sanchez, Jose Caballero, Jose Ramirez, Julio Rodriguez, Lane Thomas, Matt Olson, Max Muncy, Miguel Rojas, Vladimir Guerrero Jr., Will Smith
**What happened:** Pipeline auto-fixed 14 salary ID mismatch(es) in dk_salaries and added 14 PLAYER_ID_REMAP entry/entries.
**Rule:** Auto-fix handled it. If the same player keeps appearing, investigate the root cause in the players table.

### Auto-fixed DK ID mismatches: Lane Thomas
**What happened:** Pipeline auto-fixed 1 salary ID mismatch(es) in dk_salaries and added 0 PLAYER_ID_REMAP entry/entries.
**Rule:** Auto-fix handled it. If the same player keeps appearing, investigate the root cause in the players table.

### DK ID mismatch (auto-fix failed): Max Muncy
**What happened:** Auto-fix could not resolve: lineup_id=691777 dk_id=571970
**Rule:** Manual investigation needed — check players table or DK API for this player.

### Auto-fixed DK ID mismatches: Cole Young, Gabriel Arias, Jacob Wilson, Jose Caballero, Jose Ramirez, Julio Rodriguez, Lane Thomas, Luis Campusano
**What happened:** Pipeline auto-fixed 8 salary ID mismatch(es) in dk_salaries and added 3 PLAYER_ID_REMAP entry/entries.
**Rule:** Auto-fix handled it. If the same player keeps appearing, investigate the root cause in the players table.

---

## Session 28 Lessons (2026-03-28)

### Stale DG data from previous days pollutes current slate pools
**What happened:** Yesterday's DG 144171 (afternoon slate with LAA/HOU/DET/SD) persisted in dk_salaries because it wasn't in the current API response and never got deleted. Users saw non-slate players in the pool.
**Rule:** `load_dk_salaries.py` must clean up stale DGs — any dg_id in the DB but NOT in the current API response should be deleted. Added stale DG cleanup step before the main upload.

### PvH grouped LP constraints destroy stacks
**What happened:** PvH used one constraint per SP: `SP + sum(all_opposing_hitters) <= 1`. When the SP wasn't selected (=0), opposing hitters were still limited to 1 total — killing all stacks.
**Rule:** Use PAIRWISE constraints: one per (SP, hitter) pair. `SP + hitter <= 1` means when SP=0, the hitter is unconstrained. Only when SP=1 is the hitter blocked.

### DK CSV marks locked players with (LOCKED) suffix
**What happened:** DK entries CSV has `Kevin Gausman (42413638) (LOCKED)` for locked players. The parsing regex expected cells to end with `(digits)`, so all locked players failed to parse and their entry slots went blank.
**Rule:** Strip `(LOCKED)` suffix before parsing the player cell regex. Track the locked flag per cell.

### Late-swap mode race condition — loadPlayerPool overwrites CSV pool
**What happened:** `enterLateSwapMode()` → `setContestMode` → `loadSlates` → `loadPlayerPool` ran concurrently with CSV import. `loadPlayerPool` loaded the main slate (with NYY/SF), overwriting the CSV-built pool.
**Rule:** Skip `loadPlayerPool` when `lateSwapMode=true` — both in `loadSlates` initial load AND the dropdown change listener. `buildPoolFromCSV` handles the pool in late-swap mode.

### Classic mode pitchers leak from all-date projections
**What happened:** Classic mode built pitchers from `player_projections` (all games on the date), not from dk_salaries. Pitchers from non-slate games appeared with no salary.
**Rule:** In Classic mode, skip pitchers that don't have a salary row on the current slate: `if (!salRow) return;`

### Same-name players resolve to wrong MLBAM ID
**What happened:** Two Max Muncys (LAD pid=571970, ATH pid=691777) both resolved to 571970. Name lookup was ambiguous (excluded), and DK's playerId fell through to the wrong MLBAM.
**Rule:** After ID resolution, check if the resolved player_id's lineup team matches the DK team abbreviation. If multiple MLBAM IDs share the name, pick the one matching the DK team. Added `name_to_all_mlbam` map + team-based disambiguation in `load_dk_salaries.py`.

### VENUE_COORDS must use MLB API venue_ids
**What happened:** Radar showed wrong locations because VENUE_COORDS used sequential IDs (1-30) that didn't match MLB API venue_ids (1, 2, 680, 2392, etc.).
**Rule:** All venue_id lookups (park_factors, VENUE_COORDS) must use MLB API venue_ids from the games table. These were remapped in Session 28.

### buildPoolFromCSV projection query must use game_date directly
**What happened:** Projections were fetched via a games table lookup by game_pk, which returned empty when date was 'all'. All players got proj_dk_pts: 0.
**Rule:** Query `player_projections` directly by `.eq('game_date', date)` with `.limit(5000)`. Fall back to today's date if date picker is 'all'.

### Auto-fixed DK ID mismatches: Brandon Lowe, David Hamilton, Gabriel Arias, Ivan Herrera, Jacob Wilson, Jose Ramirez, Josh Smith, Julio Rodriguez, Miguel Rojas, Miguel Vargas, Mike Yastrzemski, Tyler O'Neill
**What happened:** Pipeline auto-fixed 12 salary ID mismatch(es) in dk_salaries and added 3 PLAYER_ID_REMAP entry/entries.
**Rule:** Auto-fix handled it. If the same player keeps appearing, investigate the root cause in the players table.

### Auto-fixed DK ID mismatches: Gabriel Arias, Jose Ramirez, Vladimir Guerrero Jr.
**What happened:** Pipeline auto-fixed 3 salary ID mismatch(es) in dk_salaries and added 0 PLAYER_ID_REMAP entry/entries.
**Rule:** Auto-fix handled it. If the same player keeps appearing, investigate the root cause in the players table.

---

## Session 29 — Late-Swap, Ownership, Weather (2026-03-28)

### player_projections query selecting nonexistent 'position' column
**What happened:** `buildPoolFromCSV` queried `player_projections` with `position` in the select list, but that column doesn't exist. Supabase returned 400, so projections=0 for ALL players. Salaries, teams, and game mappings all cascaded from this failure.
**Rule:** Before adding a column to a `.select()`, verify it exists in the table. Supabase returns 400 (not empty results) for nonexistent columns.

### CSV pool section may be incomplete in SlateHub-exported CSVs
**What happened:** The DK entries CSV has pool data (salary, team, position) in columns 15-23 alongside entry rows. But when the user exports lineups from SlateHub and pastes them into the DK template, the first ~150 entry rows have filled lineups but EMPTY pool columns. Pool data only appears after the entries end (~260 of ~590 players). Star players like Freeman, Betts, Trout had no salary.
**Rule:** Always backfill `_csvPoolData` from `dk_salaries` in Supabase before building the pool. This covers any players missing from the CSV pool section.

### Supabase silently truncates at 1000 rows even with .limit(5000)
**What happened:** `dk_salaries` had 2009 classic rows. The query used `.limit(5000)` but Supabase returned only 1000, cutting off DET players entirely. No error thrown.
**Rule:** For any table that might exceed 1000 rows, paginate with `.range(offset, offset+999)` in a loop. This applies to `dk_salaries`, `player_projections`, `batter_stats`, `batter_splits`, `rosters`. The `.limit()` parameter does NOT override Supabase's hard 1000-row cap.

### Slate loading query hit 1000-row cap — Showdown slates disappeared (Session 30)
**What happened:** The `loadDates()` function in hand-builders-hub.html used `.limit(3000)` to fetch dk_salaries for slate discovery. With 2365 rows, only 1000 came back and Showdown slates (CLE@SEA) were excluded by random row order.
**Rule:** The slate-loading query is just another instance of the 1000-row cap. Every `.select()` on dk_salaries must paginate with `.range()`. When fixing a pattern, grep for ALL occurrences of the anti-pattern, not just the one that broke.

### mlbPosToDK returned '--' for DH position
**What happened:** Players listed as DH in lineups showed '--' for position badge. `mlbPosToDK` deliberately returned '--' for DH.
**Rule:** DH is a real lineup position — return 'DH' from `mlbPosToDK`, not '--'.

### loadBatterHands missing String() fallback on player_id lookup
**What happened:** Batting hand showed '--' for players where `player_id` type (number vs string) didn't match between lineups and rosters tables.
**Rule:** Always use both raw key and `String()` fallback when looking up player_id across tables: `map[id] || map[String(id)]`.

### localStorage QuotaExceededError on large optimizer runs
**What happened:** Saving 150 optimized lineups with full player objects exceeded localStorage's ~5MB limit.
**Rule:** Use IndexedDB for large data (50MB+ quota). Slim player objects before persisting — strip stats/grades/arsenal fields, keep only essential 17 fields. Migrated in Session 29.

### _slimBuilds processed lu.slots but lineups use lu.lineup
**What happened:** The `_slimBuilds` function was processing `lu.slots` to strip player objects, but all saved lineups store data in `lu.lineup`. The slim optimization was a complete no-op.
**Rule:** When adding persistence optimizations, verify the property name matches actual data structure. Imported lineups use `lineup`, manually saved use `lineup`. Both need processing.

### Wind effect banner needs ballpark CF bearing data
**What happened:** Added a wind In/Out/Cross indicator to the weather panel using `VENUE_CF_BEARING` map (compass bearing from home plate to center field for each MLB park). Compares wind direction against CF bearing: ±45° = out, ±135-180° = in, else = cross.
**Rule:** Reference data — `VENUE_CF_BEARING` in `index.html` has bearings for all 30 parks. `null` = dome/closed roof.

### DK public API does not expose ownership data
**What happened:** Tried multiple DK API endpoints (draftables, scores, contests, draftgroups) for actual ownership %. None return ownership without authentication.
**Rule:** Actual ownership must come from DK contest standings CSV export. Use `load_actual_ownership.py --csv <file>` to import. The standings CSV has `%Drafted` column with ownership data.

### Auto-fixed DK ID mismatches: Cole Young, Gabriel Arias, Gary Sanchez, Ivan Herrera, Jacob Wilson, Jose Ramirez, Josh Smith, Julio Rodriguez, Miguel Vargas, Mike Yastrzemski
**What happened:** Pipeline auto-fixed 10 salary ID mismatch(es) in dk_salaries and added 1 PLAYER_ID_REMAP entry/entries.
**Rule:** Auto-fix handled it. If the same player keeps appearing, investigate the root cause in the players table.

### Auto-fixed DK ID mismatches: Tyler O'Neill, Vladimir Guerrero Jr.
**What happened:** Pipeline auto-fixed 2 salary ID mismatch(es) in dk_salaries and added 0 PLAYER_ID_REMAP entry/entries.
**Rule:** Auto-fix handled it. If the same player keeps appearing, investigate the root cause in the players table.

### Auto-fixed DK ID mismatches: Carlos Santana, Tyler O'Neill, Vladimir Guerrero Jr., Will Smith
**What happened:** Pipeline auto-fixed 4 salary ID mismatch(es) in dk_salaries and added 0 PLAYER_ID_REMAP entry/entries.
**Rule:** Auto-fix handled it. If the same player keeps appearing, investigate the root cause in the players table.

---

## Session 30 — Sim Engine Build (2026-03-30)

### Pitcher handedness missing from games table breaks platoon splits
**What happened:** 84 game entries had null `home_sp_hand`/`away_sp_hand`. The sim projection engine couldn't look up platoon splits for opposing batters, inflating opp_quality and deflating pitcher projections (Cease projected 13.3 instead of 18+).
**Rule:** After loading schedule, verify SP handedness is populated. Backfill from rosters table (`throws` column) when MLB API doesn't return `pitchHand`.

### Small-sample split wRC+ extremes (469, -100) corrupt opp lineup quality
**What happened:** Early-season platoon splits with <30 PA produced absurd wRC+ values (Langeliers 469 wRC+ vs RHP from 12 PA). This inflated the A's lineup quality to 1.19 when they should be ~0.95.
**Rule:** Cap split wRC+ at [30, 200]. Require minimum 30 PA for splits to be used. Fall back to overall wRC+ (capped [40, 180]) when splits are unreliable.

### Odds matching fails when DB has inconsistent team name formats
**What happened:** DB stored "Athletics" (short) but Odds API sends "Oakland Athletics" (full). The lookup matched on exact team name pairs, failing for mixed formats. 4+ games per day had no odds.
**Rule:** Build flexible lookup indexing each game by ALL name variants (full, short, reverse-mapped). `load_odds.py` now does this.

### DK slate classification by time-only blends turbo into main
**What happened:** A 4-game turbo starting at 6:35 PM got labeled `main` because it fell in the main time window (5-7:30 PM). It blended with the actual 10-game main slate.
**Rule:** After fetching draftables, refine slate label using game count: ≤5 games in main window → `turbo`, ≤2 games in late window → `late_night`.

### Ownership null for players with DK/MLB ID mismatches
**What happened:** DK salaries use proprietary player IDs that differ from MLBAM IDs for some players (Castillo, Martinez, McCullers, Johnson). The ownership sim joined only on player_id, skipping these players.
**Rule:** Always add name-based fallback matching when joining DK salary data to projection data. Normalize names (lowercase, strip Jr/Sr/III suffixes).

### Pool generator no-stack fallback produces unstacked lineups
**What happened:** When the LP solver failed for the top 2 stacked team candidates, it fell back to a no-stack solve, producing unstacked lineups that made it into the pool and portfolio.
**Rule:** Never fall back to no-stack solves in the pool generator. If stacked solve fails, return null and move on. Use weighted random team selection to spread across all eligible teams.

### Calibrating sim projections against the analytical engine is circular
**What happened:** Initially tried tuning the sim to match the analytical engine's output. But the whole point of the sim is to be BETTER than the analytical engine.
**Rule:** Calibrate against ACTUAL results (from `actual_results` table), not against the old model. Use `load_actuals.py` to populate actual DK points, then backtest sim vs reality.

### Auto-fixed DK ID mismatches: Carson Kelly, Cole Young, David Hamilton, Gabriel Arias, Ivan Herrera, Jacob Wilson, Jose Ramirez, Josh Smith, Julio Rodriguez, Miguel Rojas, Miguel Vargas
**What happened:** Pipeline auto-fixed 11 salary ID mismatch(es) in dk_salaries and added 1 PLAYER_ID_REMAP entry/entries.
**Rule:** Auto-fix handled it. If the same player keeps appearing, investigate the root cause in the players table.

---

## Session 31 — Ownership & Pitcher Calibration (2026-03-30)

### Ownership floor/cap clamp compresses the distribution
**What happened:** `clip(calibrated[pid], 0.1, 75.0)` in sim_ownership.py gave every player a 0.1% floor and 75% cap. P10 pred was 3.2% vs actual 0.1%.
**Rule:** Don't apply artificial floor/cap. Let the sim and calibration determine the natural range. Clamp to [0, 100] only.

### Linear calibration with intercept lifts the floor
**What happened:** Actual ownership calibration used `actual = slope * pred + intercept`. The intercept (+3.86) lifted every player's ownership, destroying the low end of the distribution.
**Rule:** Use multiplicative-only calibration (`pred * scale`) to preserve distribution shape. No intercept.

### Additive noise can't overcome large base_score gaps
**What happened:** Pitcher ownership was winner-take-all because additive noise (SD ~10) couldn't overcome base_score gaps of 200+. Top pitcher absorbed all SP1 picks.
**Rule:** Use multiplicative noise (`score * (1 + N(0, sd))`) so noise scales with the score. Pitchers need SD ~0.50 (50%) for the top 3-4 SPs to compete for 2 slots.

### Value (pts/$) has near-zero correlation with actual ownership
**What happened:** Weighted value at 35% in ownership sim. Analysis showed r=-0.055 with actual ownership. Projection (r=0.557) and salary (r=0.536) are the real drivers.
**Rule:** Weight projection and salary highest. Value weight should be minimal (~5%).

### SP calibration was compressing projections, not widening them
**What happened:** `sp_cal = 0.87 + 0.03 * era_ratio` gave aces 0.894 and scrubs 0.910 — barely different, and aces got a BIGGER haircut. SD ratio was 0.39 (projections 61% too flat).
**Rule:** Replace compression calibration with spread amplification: `spread_mult = 2.0 - era_ratio` clamped to [0.78, 1.25]. Aces get boosted, bad arms get penalized. SD ratio improved to 0.68.

### Per-slate ownership requires separate table
**What happened:** player_projections PK is (player_id, game_pk) — can't store different ownership per slate. Same player got same ownership on main and late slates.
**Rule:** Use dedicated `slate_ownership` table with PK (player_id, game_pk, dk_slate). Frontend overlays slate-specific ownership from this table.

---

## Session 32 — Pool Generator + Contest Sim Overhaul (2026-03-31)

### LP solver produces duplicates because it's deterministic
**What happened:** jsLPSolver returns the same optimal lineup for similar inputs. With correlated noise (game+team = 0.834 SD), the top 10 players barely change between sims. Result: only ~1500 unique from 2000 attempts.
**Rule:** Use greedy randomized builder (top-3 weighted random per slot) instead of LP. Near-zero dupe rate, 10-30x faster.

### Weighted random team selection concentrates on top 3-4 teams
**What happened:** Pool only had 3-4 different main stacks because weighted random heavily favored top-scored teams.
**Rule:** Round-robin team selection ensures ALL viable teams get coverage (~5% each for 21 teams).

### DK lobby includes tomorrow's slates with different salaries
**What happened:** Two DGs labeled "main" — today's and tomorrow's — with different prices. Frontend picked wrong salary, lineups exceeded cap on DK upload.
**Rule:** Skip DGs with future start dates in load_dk_salaries.py. Also `discard()` from classic_dg_ids/showdown_dg_ids.

### Supabase delete has 1000-row limit
**What happened:** `delete().eq('season', 2026)` only removed 1000 rows, leaving stale data.
**Rule:** Loop deletes until 0 returned: `while True: res = delete()...; if len(res.data)==0: break`

### Old season data persists across season transitions
**What happened:** Spring training data had season=2025 but config switched to 2026. Delete only cleared 2026, leaving 2025 rows.
**Rule:** Clear both current and prior season in load_dk_salaries.py.

### Pool builder needs multiple stack configurations
**What happened:** Fixed 4-2 stacks limited diversity and missed realistic constructions.
**Rule:** 6 configs: 5-3, 5-2, 5-naked, 4-3-1 (bring-back), 4-4, 4-2-2. Round-robin through configs × teams.

### Move pool generation to Python for scale
**What happened:** Browser-based LP solver capped at ~1500-2000 lineups due to JS performance and deterministic solver. Need 5K-25K per slate.
**Rule:** Python greedy builder (`generate_pool.py`) stores in `sim_pool` table. Frontend fetches from DB. No browser generation limit.

### Portfolio builder over-diversifies
**What happened:** Portfolio selection strategies (Balanced, First Place, Contrarian, etc.) spread lineups too thin. User couldn't hone in on specific exposure targets.
**Rule:** Removed portfolio builder entirely. Active exposure filtering on the pool replaces it — max targets filter lineups directly, min targets highlight in exposure panel.

### Auto-fixed DK ID mismatches: Carson Kelly, Cole Young, David Hamilton, Gabriel Arias, Jacob Wilson, Jose Ramirez, Josh Smith, Julio Rodriguez, Miguel Rojas, Vladimir Guerrero Jr.
**What happened:** Pipeline auto-fixed 10 salary ID mismatch(es) in dk_salaries and added 0 PLAYER_ID_REMAP entry/entries.
**Rule:** Auto-fix handled it. If the same player keeps appearing, investigate the root cause in the players table.

### Auto-fixed DK ID mismatches: Gabriel Arias, Jose Caballero, Jose Ramirez, Miguel Rojas, Tyler O'Neill, Vladimir Guerrero Jr.
**What happened:** Pipeline auto-fixed 6 salary ID mismatch(es) in dk_salaries and added 0 PLAYER_ID_REMAP entry/entries.
**Rule:** Auto-fix handled it. If the same player keeps appearing, investigate the root cause in the players table.

### Auto-fixed DK ID mismatches: Brandon Lowe, Carson Kelly, Cole Young, Gabriel Arias, Gary Sanchez, Ivan Herrera, Jacob Wilson, Jose Caballero, Jose Ramirez, Josh Smith, Julio Rodriguez, Miguel Vargas, Mike Yastrzemski
**What happened:** Pipeline auto-fixed 13 salary ID mismatch(es) in dk_salaries and added 0 PLAYER_ID_REMAP entry/entries.
**Rule:** Auto-fix handled it. If the same player keeps appearing, investigate the root cause in the players table.

### Pitcher missing from projections after TJ surgery (McClanahan)
**What happened:** Shane McClanahan only had 2023 pitcher_stats (missed 2024-2025 with Tommy John). The engine queried SEASON, SEASON-1, SEASON-2 (2024-2026) so found nothing and skipped him.
**Rule:** Pitcher stats lookback must include SEASON-3 (4 years) to cover pitchers returning from long injuries.

### Sim pool: 4 bugs — blank cards, 6/7-man stacks, limited pitchers/teams
**What happened:** (1) loadPoolFromDB had `|| sl.pos === 'OF'` that matched any hitter to OF slots, causing position mis-assignment. (2) Stack counting looped `for s=2..cnt`, recording every sub-size (a 5-man stack also counted as 2/3/4-man). (3) Sub-team candidates were hard-capped at `.slice(0,8)` in two places. (4) Diversity nudge was too weak (>25% threshold, -15% penalty).
**Rule:** Stack exposure must only count the actual size. Sub-team candidates must not be capped. Position assignment must use constraint ordering (most restricted first).

### Session 33 — Sim pool position export failures
**What happened:** generate_pool.py returned player_ids in random greedy-fill order. loadPoolFromDB tried to reconstruct positions but got it wrong (Pass 2 had no constraints). DK rejected lineups with players in wrong slots.
**Rule:** player_ids MUST be stored in DK slot order [SP,SP,C,1B,2B,3B,SS,OF,OF,OF]. Frontend assigns by index — zero guessing.

### Session 33 — Team abbreviation mismatch broke 5-hitter cap
**What happened:** player_projections.team had mixed formats ("ATL" and "Atlanta Braves"). The hitter cap counted them as separate teams, allowing 6+ from one team.
**Rule:** Always use dk_salaries.team (consistent DK abbreviations). Never use player_projections.team as primary.

### Session 33 — Salary player_id mismatch
**What happened:** 18 players had different IDs in player_projections vs dk_salaries. Pool stored projection IDs but frontend fetched from dk_salaries — lookups failed, showing null team/salary.
**Rule:** Always use dk_salaries.player_id in sim_pool. The frontend fetches from dk_salaries.

### Session 33 — Staleness regression for returning injured players
**What happened:** Woodruff had only 2023 stats (TJ surgery). His elite K% drove inflated projections with no recent data to temper them.
**Rule:** When most recent stats are 2+ years old, add extra regression toward league average. Don't penalize by rotation position — penalize by data freshness.

### Session 33 — O(n*m) contest sim bottleneck
**What happened:** Contest sim compared every user lineup against every contest pool lineup per sim iteration. With 15K × 25K × 5000 sims = frozen browser.
**Rule:** Sort contest scores once, binary search for each lineup. O(n*log(m)) not O(n*m).

### Session 33 — Min exposure filter didn't work
**What happened:** Min enforcement tried to pull from "skipped" lineups, but when no max caps were set nothing was skipped. All lineups already in result.
**Rule:** Min exposure needs priority sorting — put lineups matching min targets at front BEFORE the filter loop, not after.

### Auto-fixed DK ID mismatches: Brandon Lowe, Carson Kelly, Christian Vazquez, Cole Young, David Hamilton, Dylan Carlson, Gabriel Arias, Jose Caballero, Jose Fernandez, Jose Ramirez, Julio Rodriguez, Luis Campusano, Miguel Vargas
**What happened:** Pipeline auto-fixed 13 salary ID mismatch(es) in dk_salaries and added 3 PLAYER_ID_REMAP entry/entries.
**Rule:** Auto-fix handled it. If the same player keeps appearing, investigate the root cause in the players table.

### Low salary match rate in sanity check
**What happened:** Only 31% of projected batters have a dk_salaries row for 2026-04-01
**Rule:** Check that load_dk_salaries.py ran successfully and the validation gate passed.

## Session 34 — Slate Grouping, SP Inflation, Diagnostics

### DK slate grouping by start time is unreliable
**What happened:** Main slate starting at 12:15 PM was labeled "early" because the code used time buckets (<13h = early). DK's actual main slate is the one with the most games regardless of time.
**Rule:** Use DK API `GameCount` field to identify main slate (highest game count >= 6). Only label "turbo" if small slate starts within 2 hours of main.

### sim_projections spread_mult was inflating ace pitchers by 25%
**What happened:** Sale projected at 33.9 (actual ~22). The spread_mult formula `2.0 - era_ratio` boosted aces by up to 1.25x with no calibration haircut.
**Rule:** sim_projections must apply SP calibration haircut (0.87-0.95) matching compute_projections, not an ace boost.

### Stale locked DGs bleed games into active slates
**What happened:** DK removes locked slates from API, but our dk_slate_games table kept those rows. Frontend showed extra games on active slates.
**Rule:** Purge ALL slate_games for the season before upserting current data. Don't just clean DGs in the current API response.

### Correlation with error does not mean a stat improves projections
**What happened:** barrel_pct showed r=+0.18 correlation with error, but cross-validated incremental test showed it HURTS MAE when added. Same for avg_ev, hr, rbi.
**Rule:** Always use train/test split incremental value test to validate new predictors. Correlation alone is misleading — stats already captured by the model show zero marginal value.

### 4 dates is too thin for parameter weight tuning
**What happened:** Park dampening (26%→5%) and platoon dampen (70% passthrough) both improved the backtest weight sweep but made live MAE marginally worse (+0.03).
**Rule:** Need 2+ weeks (14+ dates, 3000+ matched players) before confidently tuning weights. Keep conservative changes only until sample grows.

### sim_projections is the production engine, not compute_projections
**What happened:** Fixed multipliers in compute_projections but they kept getting overwritten because sim_projections runs after and replaces all data.
**Rule:** sim_projections.py is what the pipeline (agent_projections.py) actually calls. All projection changes must go in sim_projections.py.
