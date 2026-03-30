-- migrate_slate_ownership.sql
-- Per-slate ownership projections table
-- Run in Supabase SQL editor BEFORE running sim_ownership.py

CREATE TABLE IF NOT EXISTS slate_ownership (
    player_id   BIGINT       NOT NULL,
    game_pk     BIGINT       NOT NULL,
    game_date   DATE         NOT NULL,
    dk_slate    TEXT         NOT NULL,
    proj_ownership NUMERIC(5,2) NOT NULL DEFAULT 0,
    computed_at TIMESTAMPTZ  NOT NULL DEFAULT now(),

    PRIMARY KEY (player_id, game_pk, dk_slate)
);

-- Fast lookups by date + slate (main frontend query)
CREATE INDEX IF NOT EXISTS idx_slate_own_date_slate
    ON slate_ownership (game_date, dk_slate);

-- Fast lookups by player across dates (for historical analysis)
CREATE INDEX IF NOT EXISTS idx_slate_own_player_date
    ON slate_ownership (player_id, game_date);

-- Enable RLS but allow anon read (matches other tables)
ALTER TABLE slate_ownership ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Allow public read" ON slate_ownership
    FOR SELECT USING (true);

CREATE POLICY "Allow service write" ON slate_ownership
    FOR ALL USING (true) WITH CHECK (true);
