-- migrate_pitcher_props.sql
-- Stores Vegas pitcher prop lines (IP outs, strikeouts) from The Odds API
-- Used by sim_projections.py to anchor IP and K projections to market lines

CREATE TABLE IF NOT EXISTS pitcher_props (
    game_pk      BIGINT NOT NULL,
    game_date    DATE NOT NULL,
    player_id    BIGINT NOT NULL,
    player_name  TEXT,
    team         TEXT,
    outs_line       NUMERIC,   -- pitcher_outs O/U point (e.g. 16.5 = 5.5 IP)
    strikeouts_line NUMERIC,   -- pitcher_strikeouts O/U point (e.g. 5.5)
    implied_ip      NUMERIC,   -- outs_line / 3 (convenience column)
    implied_ks      NUMERIC,   -- same as strikeouts_line (convenience column)
    fetched_at   TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (game_pk, player_id)
);

-- Index for date-based lookups
CREATE INDEX IF NOT EXISTS idx_pitcher_props_date ON pitcher_props (game_date);

-- Enable RLS (match other tables)
ALTER TABLE pitcher_props ENABLE ROW LEVEL SECURITY;

-- Allow anon reads
CREATE POLICY "Allow anon read" ON pitcher_props FOR SELECT USING (true);

-- Allow service role writes
CREATE POLICY "Allow service write" ON pitcher_props FOR ALL USING (true) WITH CHECK (true);
