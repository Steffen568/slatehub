-- Migration: Create bat_tracking table
-- Run in Supabase SQL Editor before running load_bat_tracking.py

CREATE TABLE IF NOT EXISTS bat_tracking (
    player_id               BIGINT  NOT NULL,
    season                  INT     NOT NULL,
    bat_speed               FLOAT,
    swing_length            FLOAT,
    squared_up_pct          FLOAT,   -- per bat contact, 0-1 decimal
    blast_pct               FLOAT,   -- per bat contact, 0-1 decimal
    attack_angle            FLOAT,   -- degrees
    swing_path_tilt         FLOAT,   -- degrees
    ideal_attack_angle_pct  FLOAT,   -- 0-1 decimal
    PRIMARY KEY (player_id, season)
);
