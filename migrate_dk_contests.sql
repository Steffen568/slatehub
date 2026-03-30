-- migrate_dk_contests.sql
-- DK contest metadata for contest simulation
-- Run in Supabase SQL editor BEFORE running load_contest_data.py

CREATE TABLE IF NOT EXISTS dk_contests (
    contest_id    BIGINT       NOT NULL PRIMARY KEY,
    dg_id         BIGINT       NOT NULL,
    name          TEXT         NOT NULL,
    entry_fee     NUMERIC(10,2) NOT NULL DEFAULT 0,
    prize_pool    NUMERIC(12,2) NOT NULL DEFAULT 0,
    max_entries   INT          NOT NULL DEFAULT 0,
    entry_count   INT          NOT NULL DEFAULT 0,
    max_per_user  INT          NOT NULL DEFAULT 1,
    positions_paid INT         NOT NULL DEFAULT 0,
    payout_pct    NUMERIC(5,2) NOT NULL DEFAULT 0,  -- % of field that cashes
    first_place   NUMERIC(12,2) NOT NULL DEFAULT 0,
    min_cash      NUMERIC(10,2) NOT NULL DEFAULT 0,  -- min payout amount
    dk_slate      TEXT,
    contest_type  TEXT         NOT NULL DEFAULT 'classic',
    game_date     DATE,
    payout_json   JSONB,        -- full payout structure for detailed sim
    fetched_at    TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- Lookups by date + slate
CREATE INDEX IF NOT EXISTS idx_dk_contests_date_slate
    ON dk_contests (game_date, dk_slate);

-- Lookups by draft group
CREATE INDEX IF NOT EXISTS idx_dk_contests_dg
    ON dk_contests (dg_id);

-- RLS
ALTER TABLE dk_contests ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Allow public read" ON dk_contests
    FOR SELECT USING (true);

CREATE POLICY "Allow service write" ON dk_contests
    FOR ALL USING (true) WITH CHECK (true);
