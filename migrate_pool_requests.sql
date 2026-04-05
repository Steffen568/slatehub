-- On-demand pool generation relay table
-- Frontend writes requests, backend polls and processes them
CREATE TABLE pool_requests (
  id                    BIGSERIAL PRIMARY KEY,
  created_at            TIMESTAMPTZ DEFAULT now(),
  status                TEXT NOT NULL DEFAULT 'pending',  -- pending | processing | complete | error
  game_date             DATE NOT NULL,
  dk_slate              TEXT NOT NULL,
  contest_type          TEXT NOT NULL DEFAULT 'gpp',      -- gpp | small
  user_pool_size        INT DEFAULT 10000,
  contest_pool_size     INT DEFAULT 15000,
  -- User customizations (from player chips)
  excluded_players      BIGINT[] DEFAULT '{}',
  locked_players        BIGINT[] DEFAULT '{}',
  proj_overrides        JSONB DEFAULT '{}',
  exclude_teams         TEXT[] DEFAULT '{}',
  -- User customizations (from settings drawer)
  exposure_caps         JSONB DEFAULT '{}',
  hitter_exp_max        INT DEFAULT 100,
  pitcher_exp_max       INT DEFAULT 100,
  stack_rules           JSONB DEFAULT '[]',
  salary_cap            INT DEFAULT 50000,
  min_salary            INT DEFAULT 48500,
  -- Contest pool tuning
  contest_discount_teams JSONB DEFAULT '{}',
  -- Result metadata
  user_pool_count       INT,
  contest_pool_count    INT,
  error_message         TEXT,
  completed_at          TIMESTAMPTZ
);

-- Index for polling
CREATE INDEX idx_pool_requests_status ON pool_requests (status, created_at);
