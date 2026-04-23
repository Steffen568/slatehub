-- Create projection_history table to archive all sim projections
-- Run this in Supabase SQL Editor before backfilling

CREATE TABLE IF NOT EXISTS projection_history (
    id BIGSERIAL PRIMARY KEY,
    player_id INTEGER NOT NULL,
    game_pk INTEGER,
    game_date DATE NOT NULL,
    full_name TEXT,
    team TEXT,
    batting_order INTEGER,
    is_pitcher BOOLEAN DEFAULT FALSE,
    proj_dk_pts REAL,
    proj_floor REAL,
    proj_ceiling REAL,
    proj_ip REAL,
    proj_ks REAL,
    proj_er REAL,
    win_prob REAL,
    computed_at TIMESTAMPTZ DEFAULT NOW(),
    model_version TEXT DEFAULT 'bayesian_v1'
);

-- Index for fast lookups by date and player
CREATE INDEX IF NOT EXISTS idx_proj_hist_date ON projection_history(game_date);
CREATE INDEX IF NOT EXISTS idx_proj_hist_player_date ON projection_history(player_id, game_date);

-- Enable RLS but allow service role full access
ALTER TABLE projection_history ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Allow all for service role" ON projection_history
    FOR ALL USING (true) WITH CHECK (true);
