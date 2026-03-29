-- Add simulation distribution columns to player_projections
-- Run in Supabase SQL editor BEFORE running sim_projections.py

ALTER TABLE player_projections
  ADD COLUMN IF NOT EXISTS sim_mean    FLOAT,
  ADD COLUMN IF NOT EXISTS sim_median  FLOAT,
  ADD COLUMN IF NOT EXISTS sim_floor   FLOAT,
  ADD COLUMN IF NOT EXISTS sim_ceiling FLOAT,
  ADD COLUMN IF NOT EXISTS sim_sd      FLOAT,
  ADD COLUMN IF NOT EXISTS sim_p25     FLOAT,
  ADD COLUMN IF NOT EXISTS sim_p75     FLOAT,
  ADD COLUMN IF NOT EXISTS sim_count   INT;
