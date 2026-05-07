-- Add data_type column to distinguish blend (3-yr rolling) from single-year data
ALTER TABLE park_factors ADD COLUMN IF NOT EXISTS data_type TEXT NOT NULL DEFAULT 'blend';

-- Drop old unique constraint (was on venue_id, season only)
ALTER TABLE park_factors DROP CONSTRAINT IF EXISTS park_factors_venue_id_season_key;

-- New unique constraint includes data_type
ALTER TABLE park_factors ADD CONSTRAINT park_factors_venue_id_season_data_type_key
  UNIQUE (venue_id, season, data_type);
