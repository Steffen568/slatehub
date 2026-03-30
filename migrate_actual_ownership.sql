-- migrate_actual_ownership.sql
-- Run in Supabase SQL Editor BEFORE running load_actual_ownership.py

CREATE TABLE IF NOT EXISTS actual_ownership (
  player_id    BIGINT       NOT NULL,
  dg_id        BIGINT       NOT NULL,
  game_date    DATE         NOT NULL,
  dk_name      TEXT,
  team         TEXT,
  position     TEXT,
  salary       INT,
  ownership_pct NUMERIC(5,2),        -- actual ownership from DK (0-100)
  dk_slate     TEXT,                  -- slate label (early/main/late etc.)
  contest_type TEXT DEFAULT 'classic',
  fetched_at   TIMESTAMPTZ  DEFAULT NOW(),
  PRIMARY KEY (player_id, dg_id)
);

-- Index for date-based queries and comparison joins
CREATE INDEX IF NOT EXISTS idx_actual_ownership_date ON actual_ownership (game_date);
CREATE INDEX IF NOT EXISTS idx_actual_ownership_player ON actual_ownership (player_id, game_date);
