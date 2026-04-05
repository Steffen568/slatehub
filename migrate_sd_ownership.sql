-- Add CPT/FLEX ownership columns for Showdown slates
ALTER TABLE slate_ownership ADD COLUMN IF NOT EXISTS cpt_ownership FLOAT;
ALTER TABLE slate_ownership ADD COLUMN IF NOT EXISTS flex_ownership FLOAT;
