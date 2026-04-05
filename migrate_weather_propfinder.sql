-- Add PropFinder-specific columns to weather table
ALTER TABLE weather ADD COLUMN IF NOT EXISTS delay_risk text;
ALTER TABLE weather ADD COLUMN IF NOT EXISTS forecaster_notes text;
