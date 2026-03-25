# SlateHub — Agent Pipeline Build Spec
## For Claude Code

> This document is a complete specification for building the SlateHub agent pipeline.
> Read it entirely before writing any code. Follow the rules in CLAUDE.md throughout.
> All scripts run as `py -3.12`. Never use `python` or bare `py`.

---

## What We Are Building

A three-agent pipeline that replaces the current sequential `refresh_all.py` with a smarter,
faster system that:

1. Runs independent stats scripts **in parallel** (cuts ~15 min stats run to ~4 min)
2. Validates DraftKings salary data **before** allowing projections to run
3. Auto-detects and logs new DK player ID mismatches to `tasks/lessons.md`
4. Tracks lineup confirmation status between runs (stops polling confirmed games)
5. Produces a clean **summary log** after every run
6. Writes new lessons to `tasks/lessons.md` automatically when errors occur

The existing scripts (`load_stats.py`, `load_dk_salaries.py`, etc.) are **not modified**.
The agents are wrappers that call those scripts smarter.

---

## Files To Create

| File | Purpose |
|------|---------|
| `refresh_all.py` | Replace existing — the Orchestrator |
| `agents/agent_stats.py` | Agent 1 — parallel stats runner |
| `agents/agent_lineups_dk.py` | Agent 2 — lineups + DK with validation gate |
| `agents/agent_projections.py` | Agent 3 — projections, gated on Agent 2 |
| `agents/logger.py` | Shared logging + lessons.md writer |
| `agents/lineup_state.json` | Persistent lineup confirmation state (auto-created) |

Create the `agents/` directory. All agent files live there.
The `agents/` directory must have an `__init__.py` (empty file) so it's importable.

---

## Architecture Rules (Read Before Coding)

### Dependency Chain

```
Orchestrator
├── Agent 1 (Stats)          — runs in parallel, independent of everything
│   ├── load_rosters.py
│   ├── load_stats.py
│   ├── load_reliever_stats.py
│   ├── load_arsenal.py
│   ├── load_bat_tracking.py
│   └── load_savant_splits.py
│
├── Agent 2 (Lineups & DK)   — runs sequentially, order matters
│   ├── load_schedule.py      (must run first — lineups needed for projections)
│   ├── load_dk_slates.py
│   ├── load_dk_salaries.py
│   ├── validate_dk_salaries() (new — runs diagnose_salary_mismatch.py internally)
│   ├── load_odds.py
│   └── load_weather.py
│   └── ── VALIDATION GATE ── (stops here if salary issues found)
│
└── Agent 3 (Projections)    — only runs if Agent 2 passes validation gate
    ├── compute_projections.py
    ├── compute_ownership.py
    └── sanity_check()        (new — verifies output makes sense)
```

### Critical Constraint: Agent 3 Must Block On Agent 2

Agent 3 must not start until Agent 2 returns a success status AND passes the
validation gate. If Agent 2 fails or the validation gate fails, Agent 3 must
log the failure and exit cleanly — never run projections on bad data.

### Agent 1 and Agent 2 Can Run Simultaneously

Agent 1 (stats) and Agent 2 (lineups/DK) can start at the same time because
they don't depend on each other. Agent 3 waits for both to complete.

---

## File 1: `agents/logger.py`

This is the shared utility used by all agents. Build this first.

### What it does

- Tracks pass/fail for every script that runs, with elapsed time
- Writes a summary table at the end of each run
- Appends new lessons to `tasks/lessons.md` in the correct format
- Stores the session log in memory so the orchestrator can read it

### Class: `RunLogger`

```python
class RunLogger:
    def __init__(self, mode: str)
    def record(self, label: str, success: bool, elapsed: float, notes: str = "")
    def add_lesson(self, title: str, what_happened: str, rule: str)
    def print_summary(self)
    def write_lessons_to_file(self)
    def all_passed(self) -> bool
    def any_failed(self) -> bool
    def get_failures(self) -> list[dict]
```

### `add_lesson` behavior

When called, it appends to `tasks/lessons.md` immediately (not at the end of the run).
Format must match the existing lessons.md style exactly:

```markdown
### [title]
**What happened:** [what_happened]
**Rule:** [rule]
```

Before appending, check if a lesson with the same title already exists in the file.
If it does, skip — don't create duplicates.

### `print_summary` output format

```
=======================================================
 DONE — MORNING [09:14:32]
=======================================================
 ✓ Rosters                 4.2s
 ✓ Schedule & Lineups      8.1s
 ✓ DK Slates               2.3s
 ✓ DK Salaries            11.4s
 ✓ Salary Validation       0.8s
 ✓ Odds                    3.1s
 ✓ Weather                 2.9s
 ✓ Projections            22.7s
 ✓ Ownership               9.4s
 ✗ Pitch Arsenal      FAILED  (see output above)

All phases OK.   ← or: 1 phase FAILED — check output above.
```

---

## File 2: `agents/agent_stats.py`

### What it does

Runs the 6 stats scripts simultaneously using `concurrent.futures.ThreadPoolExecutor`.
Each script runs in its own thread via `subprocess.run(['py', '-3.12', script])`.

### Scripts to run in parallel

```python
STATS_SCRIPTS = [
    ('load_rosters.py',       'Rosters'),
    ('load_stats.py',         'Batter & Pitcher Stats'),
    ('load_reliever_stats.py','Reliever Stats'),
    ('load_arsenal.py',       'Pitch Arsenal'),
    ('load_bat_tracking.py',  'Bat Tracking'),
    ('load_savant_splits.py', 'Savant Splits'),
]
```

### Implementation rules

- Use `ThreadPoolExecutor(max_workers=6)` — one thread per script
- Each thread captures stdout and stderr via `subprocess.run(..., capture_output=True, text=True)`
- Print each script's output AFTER it finishes (not interleaved during) — prefix each line
  with the script label so output is readable: `[Pitch Arsenal] Fetching...`
- Record result in the shared `RunLogger`
- If a script fails (non-zero return code), log it but do NOT stop other scripts —
  let all 6 finish and report all failures at the end
- Return the logger so the orchestrator can read pass/fail

### Function signature

```python
def run(logger: RunLogger) -> RunLogger:
    """Run all stats scripts in parallel. Returns logger with results."""
```

### Lesson auto-capture

If any script fails, call `logger.add_lesson()` with:
- title: `[Script name] failed during --stats run`
- what_happened: the last 3 lines of stderr from that script
- rule: `Check that py -3.12 and all dependencies are installed. Check API availability.`

---

## File 3: `agents/agent_lineups_dk.py`

### What it does

Runs the morning data pipeline sequentially, with a validation gate before returning.

### Scripts to run in order

```python
LINEUP_DK_SCRIPTS = [
    ('load_schedule.py',    'Schedule & Lineups'),
    ('load_dk_slates.py',   'DK Slates'),
    ('load_dk_salaries.py', 'DK Salaries'),
    # --- VALIDATION GATE runs here ---
    ('load_odds.py',        'Odds'),
    ('load_weather.py',     'Weather'),
]
```

### Validation gate (runs between load_dk_salaries and load_odds)

After `load_dk_salaries.py` completes successfully, run a validation check before continuing:

```python
def validate_dk_salaries(logger: RunLogger) -> bool:
    """
    Run diagnose_salary_mismatch.py and parse its output.
    Returns True if validation passes (0 ID mismatches).
    Returns False if new mismatches are found — logs lesson and returns failure.
    """
```

How to run the diagnosis:
- Call `subprocess.run(['py', '-3.12', 'diagnose_salary_mismatch.py'], capture_output=True, text=True)`
- Parse stdout for lines containing "ID MISMATCH"
- If any found, extract player name and ID info from each line
- Call `logger.add_lesson()` for each new mismatch found (check if already in lessons.md first)
- Print a clear warning showing which players have mismatched IDs
- Return `False` — Agent 3 must not run

If validation passes (0 mismatches), print a green checkmark and return `True`.

### Lineup confirmation tracking

The `agents/lineup_state.json` file stores which game_pks already have confirmed lineups.
Structure:

```json
{
  "date": "2026-04-01",
  "confirmed_game_pks": [748230, 748231]
}
```

On `--quick` mode runs:
1. Load the state file. If the date doesn't match today, reset it.
2. After `load_schedule.py` runs, query Supabase for today's lineups where `status = 'confirmed'`
3. Any game_pks already in `confirmed_game_pks` are noted as "already confirmed — skipped re-poll"
4. Update the state file with any newly confirmed game_pks
5. If ALL games for today are confirmed, print "All lineups confirmed — quick poll skipped next run"

The lineup state tracking is informational only in this build — `load_schedule.py` still runs
for all games. In a future build this can be used to skip API calls for confirmed games.

### Validation gate failure behavior

If the validation gate returns `False`:
- Log the failure in the RunLogger
- Print a clear error message listing the mismatched players
- Return the logger immediately — do NOT run `load_odds.py` or `load_weather.py`
- The orchestrator sees the failure and does not start Agent 3

### Function signature

```python
def run(logger: RunLogger, mode: str, quick: bool = False) -> tuple[RunLogger, bool]:
    """
    Run lineup + DK pipeline.
    Returns (logger, passed_validation_gate).
    """
```

---

## File 4: `agents/agent_projections.py`

### What it does

Runs projections and ownership, then performs a sanity check on the output.
Only runs if called by the orchestrator after Agent 2 passes its validation gate.

### Scripts to run in order

```python
PROJECTION_SCRIPTS = [
    ('compute_projections.py', 'DFS Projections'),
    ('compute_ownership.py',   'Ownership Projections'),
]
```

### Sanity check (runs after both scripts complete)

Query Supabase after projections complete to verify the output is sensible.
Connect using `python-dotenv` and the `supabase` Python client (same pattern as existing scripts).

Checks to perform:
1. **Row count** — `player_projections` for today must have at least 100 rows.
   If fewer than 100, something went wrong (likely no lineups loaded).
2. **Pitcher count** — must have at least 2 pitchers projected for today (`is_pitcher = True`).
   If 0 or 1, lineups or schedule likely failed.
3. **Null projection check** — no more than 20% of rows should have `proj_dk_pts = null`.
   Higher than 20% means stats data is missing or the projection engine failed mid-run.
4. **Salary match check** — at least 50% of projected batters must have a corresponding
   row in `dk_salaries` for today's season. If lower, DK salaries failed to load.

If any check fails:
- Log it via `logger.record()` with success=False and the check name
- Call `logger.add_lesson()` with what failed and what it usually means
- Still complete the other checks — report all failures, not just the first

If all checks pass, print a summary:
```
 Sanity check passed:
   ✓ 234 projections for today
   ✓ 14 pitchers projected
   ✓ 98% of players have proj_dk_pts
   ✓ 89% of batters matched to DK salaries
```

### Function signature

```python
def run(logger: RunLogger, target_date: str = None) -> RunLogger:
    """Run projections + ownership + sanity check. Returns logger with results."""
```

---

## File 5: `refresh_all.py` (replacement)

This replaces the existing `refresh_all.py`. The mode flags stay identical so
Windows Task Scheduler doesn't need to be reconfigured.

### Mode flags (unchanged from existing)

```
--quick     Every 15 min. Schedule + lineups + weather only.
--morning   9:00 AM. Full morning pull.
--postgame  11:30 PM. Bullpen + game logs.
--stats     7:00 AM. Season stats refresh (now parallel).
--splits    7:30 AM. Excel splits refresh.
--full      Runs everything.
```

### Orchestrator logic

```python
import sys, time, subprocess
from datetime import datetime
from agents.logger import RunLogger
from agents.agent_stats import run as run_stats
from agents.agent_lineups_dk import run as run_lineups_dk
from agents.agent_projections import run as run_projections

QUICK    = '--quick'    in sys.argv
MORNING  = '--morning'  in sys.argv
POSTGAME = '--postgame' in sys.argv
STATS    = '--stats'    in sys.argv
SPLITS   = '--splits'   in sys.argv
FULL     = '--full' in sys.argv or not any([QUICK, MORNING, POSTGAME, STATS, SPLITS])

mode = ('FULL' if FULL else 'QUICK' if QUICK else 'MORNING' if MORNING
        else 'POST-GAME' if POSTGAME else 'STATS' if STATS else 'SPLITS')

logger = RunLogger(mode)
print(f"\nSlateHub Refresh — {datetime.now().strftime('%Y-%m-%d %H:%M')} [{mode}]")
```

### Per-mode execution

**`--quick` mode:**
```python
# Just lineups + weather, no DK salary pull, no projections
run_lineups_dk(logger, mode='quick', quick=True)
```
For quick mode, `agent_lineups_dk` only runs `load_schedule.py` and `load_weather.py`.
Skip `load_dk_slates`, `load_dk_salaries`, `load_odds`, and the validation gate entirely.

**`--stats` mode:**
```python
run_stats(logger)
# Also run splits if --stats
if STATS or FULL:
    # run refresh_excel_splits.py and sync_excel_splits.py sequentially
    run_splits(logger)
```

**`--morning` mode:**
```python
import concurrent.futures

# Agent 1 and Agent 2 can start simultaneously
with concurrent.futures.ThreadPoolExecutor(max_workers=2) as ex:
    stats_future   = ex.submit(run_stats, logger)
    lineups_future = ex.submit(run_lineups_dk, logger, 'morning')

stats_logger, _           = stats_future.result()
lineups_logger, dk_passed = lineups_future.result()

# Agent 3 only runs if Agent 2 passed its validation gate
if dk_passed:
    run_projections(logger)
else:
    print("\n PROJECTIONS SKIPPED — DK validation gate failed.")
    print(" Fix PLAYER_ID_REMAP issues first, then re-run --morning.")
```

**`--postgame` mode:**
```python
# Unchanged from original — just bullpen and game logs
run_script('load_bullpen.py',          'Bullpen — final pitch counts', logger)
run_script('load_game_logs.py --days 3','Game Logs — today\'s results', logger)
```

**`--full` mode:**
```python
# Run stats + morning simultaneously, then projections
# Same as --morning but also include splits after stats
```

### Helper function

```python
def run_script(script: str, label: str, logger: RunLogger, continue_on_fail=True) -> bool:
    """Run a single script, record result, return success bool."""
    start = time.time()
    result = subprocess.run(['py', '-3.12'] + script.split(), capture_output=False)
    elapsed = time.time() - start
    success = result.returncode == 0
    logger.record(label, success, elapsed)
    return success
```

### End of orchestrator

```python
logger.print_summary()
logger.write_lessons_to_file()
```

---

## DK Lineup CSV — Player IDs Explained

This section explains the player ID situation for building lineup CSVs to upload to DraftKings.
This is **informational for now** — the CSV generator is a future build item.

### Regular Season IDs (the normal case)

During the regular season, the DK salary CSV (downloadable from the DK lineup page)
contains a column called `ID` which is the **DraftKings draftableId** for each player.
This is stored in the `dk_salaries` table as `dk_player_id` (via `load_dk_salaries.py`).

The lineup upload CSV format DK expects:

```
QB,RB,RB,WR,WR,WR,TE,FLEX,DST  ← NFL example
P,P,C,1B,2B,3B,SS,OF,OF,OF     ← MLB Classic
CPT,FLEX,FLEX,FLEX,FLEX,FLEX    ← MLB Showdown
```

For MLB, each cell needs the player's **draftableId** from the DK CSV.
This IS stored in our database — it's the `dk_player_id` field in `dk_salaries`.

### Spring Training IDs (the problem)

During spring training, DK uses **different draftableIds** than they use for those same
players during the regular season. The IDs reset when the regular season starts.
This is why you had to download the CSV manually — the spring training IDs were different
from the regular season IDs we store in `DK_TO_MLBAM`.

### Regular Season Solution (future build)

When building the lineup CSV generator, use this approach:
1. Pull the `dk_player_id` from `dk_salaries` for the selected players
2. That value IS the correct DK draftableId for lineup upload
3. Build the CSV in DK's required format using those IDs

The `fetch_dk_csv_positions()` function in `load_dk_salaries.py` already fetches
the DK CSV and maps `draftableId → position`. The same endpoint provides the IDs needed.

**Key rule:** Never use MLBAM IDs in a lineup upload CSV. DK only accepts its own
draftableId. Our `dk_player_id` column in `dk_salaries` stores exactly this value.

---

## Implementation Order

Build in this exact order. Each step depends on the previous.

### Step 1: Create directory structure

```bash
mkdir agents
touch agents/__init__.py
```

### Step 2: Build `agents/logger.py`

Test it standalone:
```python
from agents.logger import RunLogger
log = RunLogger('TEST')
log.record('Test Script', True, 2.3)
log.record('Failing Script', False, 0.5, 'timeout')
log.print_summary()
```
Expected: summary table with one ✓ and one ✗.

### Step 3: Build `agents/agent_stats.py`

Test it standalone with `--stats` mode:
```bash
py -3.12 -c "from agents.logger import RunLogger; from agents.agent_stats import run; run(RunLogger('STATS'))"
```
Watch that all 6 scripts launch and output appears prefixed with script name.
Verify all 6 complete before summary prints.

### Step 4: Build `agents/agent_lineups_dk.py`

Test the validation gate in isolation before wiring to the orchestrator:
```bash
py -3.12 -c "
from agents.logger import RunLogger
from agents.agent_lineups_dk import validate_dk_salaries
log = RunLogger('TEST')
result = validate_dk_salaries(log)
print('Gate passed:', result)
"
```

### Step 5: Build `agents/agent_projections.py`

Test sanity check in isolation (projections must have already run today):
```bash
py -3.12 -c "
from agents.logger import RunLogger
from agents.agent_projections import sanity_check
log = RunLogger('TEST')
sanity_check(log)
log.print_summary()
"
```

### Step 6: Build new `refresh_all.py`

Before replacing the existing file, rename it as a backup:
```bash
copy refresh_all.py refresh_all_backup.py
```
Then build the new version.

Test each mode individually:
```bash
py -3.12 refresh_all.py --stats
py -3.12 refresh_all.py --quick
py -3.12 refresh_all.py --morning
```

---

## Error Handling Rules

These apply throughout all agent code:

1. **Never let an exception in one agent crash the orchestrator.** Wrap each agent call
   in try/except. If an agent throws an unhandled exception, log it and continue.

2. **Never silently swallow errors.** If a script fails, print its stderr output before
   moving on — even in parallel mode where output is buffered.

3. **Respect the validation gate.** If `validate_dk_salaries()` returns False, Agent 3
   must not run under any circumstances — not even if the user runs `--full` mode.
   The gate exists to prevent bad data from reaching the frontend.

4. **Supabase connection errors are fatal.** If the sanity check can't connect to Supabase,
   log it as a failure but do not crash — the projection scripts already ran successfully.

5. **lessons.md writes must not fail silently.** If the file can't be written (permissions,
   missing directory), print a warning and the lesson content to stdout so it's not lost.

---

## What NOT To Build (Out Of Scope For This Task)

- Do not modify any existing `load_*.py` or `compute_*.py` scripts
- Do not build the lineup CSV generator (future task)
- Do not add any new Supabase tables
- Do not change Windows Task Scheduler bat/ps1 files
- Do not modify `CLAUDE.md` or `tasks/lessons.md` (the logger writes to lessons.md automatically)
- Do not build a web dashboard or UI for the agent output

---

## Testing Checklist

Before considering the build complete, verify all of these:

- [ ] `py -3.12 refresh_all.py --stats` launches all 6 scripts simultaneously (check timestamps in output)
- [ ] `py -3.12 refresh_all.py --stats` finishes in under 6 minutes (vs ~15 min before)
- [ ] `py -3.12 refresh_all.py --morning` blocks Agent 3 when validation gate fails
- [ ] `py -3.12 refresh_all.py --morning` runs Agent 3 when validation gate passes
- [ ] `py -3.12 refresh_all.py --quick` only runs schedule + weather (nothing else)
- [ ] `py -3.12 refresh_all.py --postgame` only runs bullpen + game logs
- [ ] A new player ID mismatch causes a lesson to appear in `tasks/lessons.md`
- [ ] `agents/lineup_state.json` is created and updated after a `--quick` run
- [ ] Summary log prints at the end of every mode
- [ ] `refresh_all_backup.py` exists before the new file is written

---

## Known Issues To Watch For

These are from `tasks/lessons.md` — review before touching related code:

- **Python version**: Always `py -3.12`. Never `python` or bare `py`.
- **Supabase `.limit()`**: Any `.select()` on a large table needs `.limit(5000)`.
- **`.in()` with 150+ IDs**: Use chunked queries — never pass a large array directly.
- **Multi-season tables**: Always `.order('season', {ascending:false})` + deduplicate.
- **Git repo disappearing**: Run `git status` before anything. See CLAUDE.md for recovery steps.
- **DK gameType string**: `'Showdown Captain Mode'` not `'Showdown'`.
- **Column name gotchas**: `game_pk` not `game_id`. `precip_pct` not `precip_prob`. See CLAUDE.md table.

---

## Definition Of Done

The build is complete when:
1. All files listed in "Files To Create" exist
2. All items in the Testing Checklist are passing
3. The stats run is measurably faster than before
4. The validation gate correctly blocks projections when DK data is bad
5. `tasks/lessons.md` is automatically updated when new ID mismatches are found
6. `refresh_all_backup.py` exists as a safety fallback
7. The new `refresh_all.py` accepts all the same mode flags as before

---

*Build spec version: 1.0 — April 2026*
*Companion file: CLAUDE.md (read that too — it has the project rules)*
