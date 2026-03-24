#!/usr/bin/env python3
"""
Phase 9 — 40-Man Rosters
Pulls current 40-man roster for all 30 MLB teams from the MLB API
Stores in a `rosters` table used to filter player pools across the app
"""

import os, requests, time
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()
sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])

MLB_BASE = "https://statsapi.mlb.com/api/v1"

# All 30 MLB team IDs
TEAM_IDS = [
    108, 109, 110, 111, 112, 113, 114, 115, 116, 117,
    118, 119, 120, 121, 133, 134, 135, 136, 137, 138,
    139, 140, 141, 142, 143, 144, 145, 146, 147, 158
]

ROSTER_TYPES = [
    ("40Man",              True),   # (rosterType, on_40_man)
    ("nonRosterInvitees",  False),  # Spring training NRIs
]

def fetch_roster(team_id, roster_type):
    url = f"{MLB_BASE}/teams/{team_id}/roster?rosterType={roster_type}&hydrate=person(pitchHand,batSide)"
    r = requests.get(url, timeout=15)
    r.raise_for_status()
    return r.json()

def run():
    print("\nPhase 9 — Rosters (40-Man + NRIs)")
    print("=" * 40)

    # Use dict keyed by player_id so 40-man always wins over NRI if same player appears in both
    records_map = {}

    for team_id in TEAM_IDS:
        team_count = 0
        for roster_type, on_40_man in ROSTER_TYPES:
            try:
                data   = fetch_roster(team_id, roster_type)
                roster = data.get("roster", [])

                for p in roster:
                    person = p.get("person", {})
                    status = p.get("status", {})
                    pos    = p.get("position", {})
                    pid    = person.get("id")
                    if not pid:
                        continue
                    # Only write if not already present (40-man takes priority)
                    if pid not in records_map:
                        records_map[pid] = {
                            "player_id"    : pid,
                            "player_name"  : person.get("fullName"),
                            "team_id"      : team_id,
                            "position"     : pos.get("abbreviation"),
                            "position_type": pos.get("type"),
                            "roster_status": status.get("description", "NRI" if not on_40_man else "Active"),
                            "on_40_man"    : on_40_man,
                            "throws"       : person.get("pitchHand", {}).get("code"),
                            "bats"         : person.get("batSide",   {}).get("code"),
                        }
                    team_count += len(roster)
                time.sleep(0.10)

            except Exception as e:
                # NRI endpoint may 404 for some teams — not an error
                if roster_type == "nonRosterInvitees" and "404" in str(e):
                    pass
                else:
                    print(f"  ERROR team {team_id} ({roster_type}): {e}")
                continue

        print(f"  ✓ Team {team_id}: {team_count} players (40-man + NRIs)")

    all_records = list(records_map.values())
    print(f"\n  Total: {len(all_records)} unique players (40-man + NRIs)")

    # Upload in batches
    print("\nUploading...")
    BATCH = 500
    uploaded = 0
    for i in range(0, len(all_records), BATCH):
        batch = all_records[i:i+BATCH]
        (sb.table("rosters")
           .upsert(batch, on_conflict="player_id,team_id", ignore_duplicates=False)
           .execute())
        uploaded += len(batch)
        print(f"  ✓ {uploaded}/{len(all_records)}")

    print(f"\nPhase 9 complete. {uploaded} roster entries uploaded.")

    # Sanity check
    total = sb.table("rosters").select("player_id", count="exact").execute()
    print(f"Total players in rosters table: {total.count}")

    # Check a known reliever
    check = sb.table("rosters").select("*").eq("player_id", 552640).execute()
    print(f"\nAndrew Kittredge on 40-man: {len(check.data) > 0}")

if __name__ == "__main__":
    run()
    