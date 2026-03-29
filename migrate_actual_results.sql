-- Create actual_results table for backtesting projections
-- Run in Supabase SQL editor

CREATE TABLE IF NOT EXISTS actual_results (
  player_id   INTEGER NOT NULL,
  game_pk     INTEGER NOT NULL,
  game_date   DATE    NOT NULL,
  full_name   TEXT,
  team        TEXT,
  is_pitcher  BOOLEAN DEFAULT FALSE,
  -- Batter stats
  pa  INTEGER, ab INTEGER, h INTEGER,
  singles INTEGER, doubles INTEGER, triples INTEGER, hr INTEGER,
  r INTEGER, rbi INTEGER, bb INTEGER, hbp INTEGER, sb INTEGER, cs INTEGER, k INTEGER,
  -- Pitcher stats
  ip FLOAT, p_k INTEGER, p_er INTEGER, p_h INTEGER, p_bb INTEGER,
  p_hr INTEGER, win BOOLEAN, loss BOOLEAN, cg BOOLEAN, sho BOOLEAN,
  -- DK scoring
  actual_dk_pts FLOAT,
  PRIMARY KEY (player_id, game_pk)
);
