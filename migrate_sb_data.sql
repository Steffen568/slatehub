-- Add stolen base vulnerability columns to pitcher_stats
ALTER TABLE pitcher_stats ADD COLUMN IF NOT EXISTS sb_allowed integer;
ALTER TABLE pitcher_stats ADD COLUMN IF NOT EXISTS cs_allowed integer;
ALTER TABLE pitcher_stats ADD COLUMN IF NOT EXISTS sb_pct real;
ALTER TABLE pitcher_stats ADD COLUMN IF NOT EXISTS wild_pitches integer;
ALTER TABLE pitcher_stats ADD COLUMN IF NOT EXISTS pickoffs integer;
ALTER TABLE pitcher_stats ADD COLUMN IF NOT EXISTS sb_per_9 real;

-- Create catcher pop time table
CREATE TABLE IF NOT EXISTS catcher_poptime (
  player_id bigint NOT NULL,
  season integer NOT NULL,
  full_name text,
  team_id integer,
  pop_2b real,           -- avg pop time to 2nd on steal attempts (seconds)
  pop_2b_cs real,        -- pop time on caught stealing
  pop_2b_sb real,        -- pop time on successful steals
  pop_2b_attempts integer, -- number of steal attempts against
  exchange real,         -- glove-to-release time
  arm_strength real,     -- max effort arm strength (mph)
  updated_at timestamptz DEFAULT now(),
  PRIMARY KEY (player_id, season)
);
