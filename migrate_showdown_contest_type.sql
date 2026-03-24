-- Add contest_type column to dk_salaries to distinguish Classic vs Showdown slates
ALTER TABLE dk_salaries ADD COLUMN IF NOT EXISTS contest_type TEXT DEFAULT 'classic';

-- Backfill existing rows (all previously loaded data is Classic)
UPDATE dk_salaries SET contest_type = 'classic' WHERE contest_type IS NULL;
