-- sql/create_tables.sql
-- CHANGED: Minimal normalized schema + boolean sprints + edited flag.

CREATE TABLE IF NOT EXISTS quarters (
  id SERIAL PRIMARY KEY,
  label TEXT UNIQUE NOT NULL,      -- e.g., '2025Q3'
  is_current BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_quarters_current ON quarters (is_current) WHERE is_current = TRUE;

CREATE TABLE IF NOT EXISTS tribes (
  id SERIAL PRIMARY KEY,
  name TEXT UNIQUE NOT NULL
);


CREATE TABLE IF NOT EXISTS apps (
  id SERIAL PRIMARY KEY,
  name TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS temp_assignments (
  id SERIAL PRIMARY KEY,
  quarter_id INT NOT NULL REFERENCES quarters(id) ON DELETE CASCADE,
  tribe_id INT NOT NULL REFERENCES tribes(id) ON DELETE CASCADE,
  app_id INT NOT NULL REFERENCES apps(id) ON DELETE CASCADE,
  resource_name TEXT NOT NULL,
  role TEXT NOT NULL,
  assign_type TEXT NOT NULL CHECK (assign_type IN ('Dedicated','Shared'))
);

-- master: what users actually book
CREATE TABLE IF NOT EXISTS master_assignments (
  id SERIAL PRIMARY KEY,
  quarter_id INT NOT NULL REFERENCES quarters(id) ON DELETE CASCADE,
  tribe_name TEXT NOT NULL,
  app_name TEXT NOT NULL,
  resource_name TEXT NOT NULL,
  role TEXT NOT NULL,
  assign_type TEXT NOT NULL CHECK (assign_type IN ('Dedicated','Shared')),
  s1 SMALLINT NOT NULL DEFAULT 0,
  s2 SMALLINT NOT NULL DEFAULT 0,
  s3 SMALLINT NOT NULL DEFAULT 0,
  s4 SMALLINT NOT NULL DEFAULT 0,
  s5 SMALLINT NOT NULL DEFAULT 0,
  s6 SMALLINT NOT NULL DEFAULT 0,
  edited BOOLEAN NOT NULL DEFAULT FALSE,
  updated_at TIMESTAMP NULL
);

-- natural upsert identity used by API
CREATE UNIQUE INDEX IF NOT EXISTS uq_master_identity
ON master_assignments(quarter_id, tribe_name, app_name, resource_name, role);

-- natural upsert identity used by API
CREATE UNIQUE INDEX IF NOT EXISTS uq_master_identity
ON master_assignments(quarter_id, tribe_name, app_name, resource_name, role);

-- Performance indexes for fast availability checks and lookups
CREATE INDEX IF NOT EXISTS idx_ma_quarter_resource
  ON master_assignments (quarter_id, resource_name);

CREATE INDEX IF NOT EXISTS idx_ma_quarter_resource_tribe
  ON master_assignments (quarter_id, resource_name, tribe_name);

CREATE INDEX IF NOT EXISTS idx_ta_quarter_res
  ON temp_assignments (quarter_id, resource_id);

CREATE INDEX IF NOT EXISTS idx_ta_quarter_res_tribe
  ON temp_assignments (quarter_id, resource_id, tribe_id);