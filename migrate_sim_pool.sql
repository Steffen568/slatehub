-- migrate_sim_pool.sql
-- Pre-computed lineup pools for contest simulation
-- Run in Supabase SQL editor BEFORE running generate_pool.py

CREATE TABLE IF NOT EXISTS sim_pool (
    pool_id     BIGSERIAL    PRIMARY KEY,
    game_date   DATE         NOT NULL,
    dk_slate    TEXT         NOT NULL,
    pool_type   TEXT         NOT NULL DEFAULT 'user',  -- 'user' or 'contest'
    player_ids  BIGINT[]     NOT NULL,                 -- array of 10 player IDs
    salary      INT          NOT NULL DEFAULT 0,
    proj        NUMERIC(6,2) NOT NULL DEFAULT 0,
    stack_team  TEXT,                                   -- main stack team abbreviation
    stack_size  INT          NOT NULL DEFAULT 0,        -- main stack size (3,4,5)
    sub_team    TEXT,                                   -- sub stack team
    sub_size    INT          NOT NULL DEFAULT 0,
    computed_at TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- Primary lookup: fetch pools by date + slate + type
CREATE INDEX IF NOT EXISTS idx_sim_pool_lookup
    ON sim_pool (game_date, dk_slate, pool_type);

-- Enable RLS
ALTER TABLE sim_pool ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Allow public read" ON sim_pool
    FOR SELECT USING (true);

CREATE POLICY "Allow service write" ON sim_pool
    FOR ALL USING (true) WITH CHECK (true);
