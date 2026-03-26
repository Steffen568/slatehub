-- Migration: create dk_slate_games table
-- Maps which games (competitions) belong to each DK draft group / slate.
-- Preserves game-to-slate mapping even after DK removes locked DGs from their API.
--
-- Run this in the Supabase SQL editor BEFORE deploying the Python changes.

CREATE TABLE IF NOT EXISTS dk_slate_games (
  dg_id          INTEGER NOT NULL,
  competition_id INTEGER NOT NULL,
  dk_slate       TEXT    NOT NULL,
  away_team      TEXT    NOT NULL,
  home_team      TEXT    NOT NULL,
  start_time     TEXT,
  season         INTEGER NOT NULL,
  PRIMARY KEY (dg_id, competition_id)
);

ALTER TABLE dk_slate_games ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Allow anon read" ON dk_slate_games FOR SELECT USING (true);
CREATE POLICY "Allow service write" ON dk_slate_games FOR ALL USING (true);
