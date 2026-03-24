-- Migration: Add physics columns to pitch_arsenal
-- Run this once in the Supabase SQL Editor before re-running load_arsenal.py

ALTER TABLE pitch_arsenal ADD COLUMN IF NOT EXISTS spin_rate     FLOAT;
ALTER TABLE pitch_arsenal ADD COLUMN IF NOT EXISTS release_height FLOAT;
ALTER TABLE pitch_arsenal ADD COLUMN IF NOT EXISTS extension      FLOAT;
ALTER TABLE pitch_arsenal ADD COLUMN IF NOT EXISTS arm_angle      FLOAT;

-- ivb and hb columns already exist (added previously) — no action needed for those
