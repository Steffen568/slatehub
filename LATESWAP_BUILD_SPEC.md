# SlateHub — Late-Swap Optimizer Build Spec

**For Claude Code — Read CLAUDE.md first, then this file entirely before writing any code.**

---

## What We Are Building

A **Late-Swap Optimizer** for the Classic MLB DraftKings lineup builder in `hand-builders-hub.html`. This feature allows users to re-optimize lineups **after some games have already locked**, swapping only the unlocked roster spots while keeping locked players frozen in place.

MLB is unique: games lock individually (not all at once like NFL). A 7:05 PM ET game locks before a 10:10 PM ET game. Late swap lets you update your lineup with new information (weather delays, late scratches, updated projections) for games that haven't started yet.

### Core Behavior

1. **Locked players stay frozen** — any player whose game has already started (or is past lock time) cannot be moved, removed, or swapped. They remain in their exact roster slot.
2. **Unlocked slots get re-optimized** — the optimizer fills only the remaining open slots using the same jsLPSolver engine, respecting salary cap (minus locked players' salaries), roster rules, and updated projections.
3. **Single lineup mode** — re-optimize one lineup at a time.
4. **Multi-lineup mode** — re-optimize a batch of lineups (e.g., 20 lineups) simultaneously, each respecting its own locked players.

---

## Technical Context — Read Before Coding

### Existing Stack
- **Frontend**: Vanilla HTML + JavaScript (no framework), GitHub Pages
- **Database**: Supabase (Postgres)
- **Solver**: `jsLPSolver` (already used in Showdown optimizer in `hand-builders-hub.html`)
- **Player data**: `dk_salaries` table (DK draftableIds, salaries, positions, teams) + `player_projections` table (proj_dk_pts)
- **Salary CSV**: Users upload DraftKings salary CSV which gets loaded into Supabase via `load_dk_salaries.py`

### Existing Optimizer Location
The Classic and Showdown optimizers both live in `hand-builders-hub.html`. The Showdown optimizer already uses jsLPSolver with these patterns:
- `opType: 'max'` (NOT `optiType` — this is a known bug from Session 25)
- Constraints for salary cap, roster size, positional requirements
- Exposure caps per player
- Exclude lists (per slot type)

**READ the existing Classic optimizer code in `hand-builders-hub.html` before writing anything.** Understand the current model structure, variable naming, how player data flows from Supabase into the solver, and how results are rendered into the lineup slots.

### DraftKings MLB Classic Roster Structure
| Slot | Position(s) | Count |
|------|-------------|-------|
| P    | SP          | 2     |
| C    | C           | 1     |
| 1B   | 1B          | 1     |
| 2B   | 2B          | 1     |
| 3B   | 3B          | 1     |
| SS   | SS          | 1     |
| OF   | OF          | 3     |

**Total: 10 players, $50,000 salary cap (configurable via `optoSalaryCap`)**

---

## How Late Swap Should Work — Step by Step

### Step 1: Determine Lock Status

Each player in a lineup is associated with a game (`game_pk`) which has a `game_time_utc`. A player is **locked** if:

```
current_time_utc >= game_time_utc
```

Query the `games` table for today's games. Each player's `game_pk` maps to a `game_time_utc`. Compare against `new Date().toISOString()` to determine lock status.

**Important**: Use `game_pk` (NOT `game_id` — see CLAUDE.md column name rules). Use `game_time_utc` (NOT `game_time`).

### Step 2: Partition the Lineup

Given an existing lineup of 10 players, split into:
- **Locked set**: Players whose games have started. These are frozen — their slots, salaries, and positions are fixed.
- **Unlocked set**: Players whose games haven't started yet. These slots are available for re-optimization.

### Step 3: Calculate Remaining Budget

```
remaining_salary = optoSalaryCap - sum(locked_player_salaries)
```

### Step 4: Build the Late-Swap LP Model

Build a jsLPSolver model that:
- **Maximizes** total projected points for unlocked slots only
- **Salary constraint**: total unlocked player salaries ≤ `remaining_salary`
- **Position constraints**: only the positions of the unlocked slots need to be filled (e.g., if 1B and 2 OF are unlocked, the model needs exactly 1 1B-eligible player and 2 OF-eligible players)
- **Roster constraints**: no player can appear in both locked and unlocked sets (a locked player cannot also fill an unlocked slot)
- **Team stacking rules**: if stacking rules exist in the current optimizer, respect them across both locked + unlocked players
- **Exclude list**: the existing exclude functionality should still work for unlocked slots
- **Exposure caps**: in multi-lineup mode, exposure caps apply across all lineups being re-optimized

### Step 5: Solve and Render

Run jsLPSolver, extract results using the same regex pattern as the existing optimizer (`/^([cf])(\d+)$/` or whatever pattern the Classic optimizer uses — **read the code first**). Place solved players into the unlocked slots. Locked players remain untouched in their slots.

---

## UI Design — Integrate Into Existing Flow

**Most efficient approach: add a "Late Swap" toggle/mode within the existing Classic optimizer section.** Do NOT create a separate page — this keeps it lightweight and reuses existing code.

### UI Elements to Add

1. **"Late Swap Mode" toggle** — a switch/checkbox near the existing optimizer controls. When enabled:
   - The optimizer switches to late-swap behavior
   - Locked players are visually indicated (e.g., a lock icon 🔒, greyed-out background, or a colored border)
   - Unlocked slots are visually highlighted as "swappable"
   - The salary display updates to show: `Remaining: $X,XXX / $50,000 (Locked: $XX,XXX)`

2. **Lock time display** — next to each player in the lineup, show their game time. If locked, show "🔒 LOCKED". If unlocked, show time until lock (e.g., "Locks in 2h 15m").

3. **"Re-Optimize" button** — when Late Swap Mode is on, the main optimize button text changes to "Re-Optimize Unlocked" to make the action clear.

4. **Multi-lineup late swap** — in the lineup list/queue (if multi-lineup is already built), add a "Late Swap All" button that re-optimizes all queued lineups, each respecting its own locked players.

### Visual Lock Indicators

```
LOCKED slots:   🔒 grey background, non-interactive, salary dimmed
UNLOCKED slots: normal appearance, fully interactive, green subtle border or highlight
```

### Settings Drawer Integration

Add these settings to the existing optimizer settings drawer:
- **Auto-detect locks**: ON by default. When ON, the optimizer automatically checks game times to determine locked vs unlocked. When OFF, the user can manually toggle which players are locked (useful for testing or pre-lock planning).

---

## Data Flow

### Where Player Game Times Come From

The `games` table has `game_pk` and `game_time_utc` for each game today.

The `player_projections` table has `game_pk` for each player.

Join path: `player in lineup → player_projections.game_pk → games.game_time_utc`

Alternatively, the `dk_salaries` table may have game info. **Check both tables in the codebase to see which is more reliable and already loaded in the frontend.**

### Supabase Queries (respect existing patterns)

- Always use `.limit(5000)` on large tables
- Never use `.in()` with more than 150 IDs — use `batchIn()` helper
- Use `.order('season', {ascending:false})` for multi-season tables + deduplicate

### Refresh Lock Status

Game times don't change often, but late scratches and lineup changes do. Add a **"Refresh Status"** button that:
1. Re-queries the `games` table for current `game_time_utc` values
2. Re-checks `player_projections` for any updated projections
3. Recalculates which players are locked vs unlocked
4. Updates the UI indicators

---

## Multi-Lineup Late Swap

### How It Works

If the user has built or generated multiple lineups (N lineups):

1. For each lineup, determine its locked/unlocked partition independently (different lineups may have different players, some locked, some not)
2. Run late-swap optimization on each lineup individually
3. **Exposure caps apply globally** across all N lineups — if a player is capped at 30% and there are 20 lineups, that player can appear in at most 6 lineups (across both locked and unlocked appearances)
4. **Locked appearances count toward exposure** — if a player is locked in 4 lineups, they can only appear in 2 more unlocked slots (at 30% of 20)

### Implementation Approach

Loop through lineups, solving one at a time. After each solve, update the exposure tracking counts. This is the same sequential approach the Showdown multi-lineup optimizer uses — **read that code for the pattern.**

---

## Edge Cases to Handle

1. **All players locked** — if every slot is locked (all games started), show a message: "All games have started. No slots available for late swap." Disable the re-optimize button.

2. **No players locked** — if no games have started, late swap behaves identically to the regular optimizer (all slots are open). Show a note: "No games locked yet — running full optimization."

3. **Only one slot unlocked** — the optimizer should still run, but it's effectively just picking the best available player for that one slot within the remaining budget.

4. **Salary impossible** — if locked players consume so much salary that no valid lineup exists for the remaining slots, show an error: "Cannot build valid lineup — locked players use $XX,XXX, leaving only $X,XXX for Y remaining slots."

5. **Player in locked slot also eligible for unlocked slot** — this shouldn't happen (a player can only be in one slot), but guard against it. The locked player's ID should be excluded from the unlocked player pool.

6. **Game time changes** — rare but possible (rain delays, postponements). The refresh button handles this. If a previously-locked game gets postponed, that player becomes unlocked again on refresh.

7. **Lineup not yet built** — late swap mode should be disabled if there's no existing lineup to swap. Show a tooltip: "Build or import a lineup first."

---

## What NOT to Build

- Do NOT modify any Python pipeline scripts (`load_*.py`, `compute_*.py`)
- Do NOT create new Supabase tables — use existing tables only
- Do NOT build a DK CSV export for late-swapped lineups (future task)
- Do NOT add real-time game score tracking
- Do NOT modify the Showdown optimizer — this is Classic only
- Do NOT add a separate HTML page — everything goes in `hand-builders-hub.html`

---

## Implementation Order

Build in this exact order. Test each step before moving to the next.

### Step 1: Read and Understand Existing Code
- Read `hand-builders-hub.html` thoroughly — understand the Classic optimizer model, how player data loads from Supabase, how lineups render, how the settings drawer works
- Read the Showdown optimizer for jsLPSolver patterns and multi-lineup exposure logic
- Identify where game time data is available in the frontend (check what's already fetched from `games` or `player_projections`)

### Step 2: Build Lock Detection Logic
- Write a function `getPlayerLockStatus(player)` that checks `game_time_utc` against current time
- Write `partitionLineup(lineup)` that returns `{ locked: [...], unlocked: [...], lockedSalary: N, remainingBudget: N }`
- Test with mock data first (hardcode a time where some games are locked and some aren't)

### Step 3: Build the Late-Swap Solver
- Write `lateSwapOptimize(lineup, playerPool, settings)` that:
  - Calls `partitionLineup()` to get locked/unlocked split
  - Builds a jsLPSolver model for only the unlocked positions
  - Adds salary constraint using remaining budget
  - Excludes locked players from the pool
  - Returns the re-optimized lineup
- Test with a single lineup first

### Step 4: Add UI Toggle and Visual Indicators
- Add the Late Swap Mode toggle to the optimizer controls
- Add lock icons and status displays to lineup slots
- Update salary display to show locked/remaining breakdown
- Change optimize button text when in late-swap mode

### Step 5: Wire Up Single-Lineup Late Swap
- Connect the re-optimize button to `lateSwapOptimize()`
- Render results back into lineup slots
- Add the Refresh Status button

### Step 6: Add Multi-Lineup Late Swap
- Add "Late Swap All" button for batch re-optimization
- Implement exposure tracking across locked + unlocked appearances
- Test with multiple lineups that have different lock states

---

## Testing Checklist

Before considering the build complete, verify ALL of these:

- [ ] Late swap correctly identifies locked vs unlocked players based on game times
- [ ] Locked players remain in their slots after re-optimization (they never move)
- [ ] Remaining salary is correctly calculated (cap minus locked salaries)
- [ ] The solver only fills unlocked position slots (not all positions)
- [ ] A locked player cannot appear in an unlocked slot in the same lineup
- [ ] "All locked" edge case shows the correct message and disables the button
- [ ] "No locked" edge case runs a full optimization
- [ ] "Salary impossible" edge case shows a clear error message
- [ ] Multi-lineup late swap respects per-player exposure caps globally
- [ ] Locked appearances count toward exposure caps in multi-lineup mode
- [ ] Refresh Status button correctly updates lock indicators
- [ ] The existing Classic optimizer still works normally when Late Swap Mode is OFF
- [ ] The existing Showdown optimizer is completely untouched
- [ ] UI lock indicators (🔒 icons, highlighting) display correctly
- [ ] Settings drawer auto-detect toggle works

---

## Lessons from Past Sessions (Apply These)

These are from CLAUDE.md and must be followed:

- **jsLPSolver model property is `opType`** — NOT `optiType`. Wrong name silently defaults to minimization.
- **Always read the file before editing** — the Edit tool fails if it can't find the exact text.
- **Supabase `.limit(5000)`** on all large table queries.
- **Never use `.in()` with 150+ IDs** — use `batchIn()` helper.
- **Column names**: `game_pk` not `game_id`, `game_time_utc` not `game_time`, `precip_pct` not `precip_prob`.
- **Plan before acting** — write out what files will change and why before writing code. Wait for approval.
- **Verify before calling it done** — show evidence that each piece works.
- **Log lessons** — after any bug fix, update `tasks/lessons.md` and `CLAUDE.md`.

---

*Build spec version: 1.0 — March 2026*
*Companion files: CLAUDE.md, AGENT_BUILD_SPEC.md (for reference on spec format)*
