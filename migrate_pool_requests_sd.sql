-- Extend pool_requests for Showdown mode
ALTER TABLE pool_requests ADD COLUMN IF NOT EXISTS is_showdown BOOLEAN DEFAULT false;
ALTER TABLE pool_requests ADD COLUMN IF NOT EXISTS excluded_cpt BIGINT[] DEFAULT '{}';
ALTER TABLE pool_requests ADD COLUMN IF NOT EXISTS excluded_flex BIGINT[] DEFAULT '{}';
ALTER TABLE pool_requests ADD COLUMN IF NOT EXISTS cpt_exp_max INT DEFAULT 35;
