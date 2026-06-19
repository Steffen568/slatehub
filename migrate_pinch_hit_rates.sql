-- Per-game record: did this player start, and were they pinch hit for?
-- Primary key (player_id, game_pk) makes daily upserts idempotent.
CREATE TABLE IF NOT EXISTS pinch_hit_game_log (
  player_id   INTEGER  NOT NULL,
  game_pk     INTEGER  NOT NULL,
  season      INTEGER  NOT NULL,
  player_name TEXT,
  started     SMALLINT NOT NULL DEFAULT 0,  -- 1 if player was in starting lineup
  ph_for      SMALLINT NOT NULL DEFAULT 0,  -- 1 if a substitute entered their slot
  PRIMARY KEY (player_id, game_pk)
);

-- Season-level aggregate: rebuilt from game_log after each run.
CREATE TABLE IF NOT EXISTS pinch_hit_rates (
  player_id     INTEGER NOT NULL,
  player_name   TEXT,
  season        INTEGER NOT NULL,
  games_started INTEGER NOT NULL DEFAULT 0,
  ph_for_count  INTEGER NOT NULL DEFAULT 0,
  ph_for_rate   FLOAT,
  PRIMARY KEY (player_id, season)
);
