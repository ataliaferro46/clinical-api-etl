-- Clinical Data ETL Pipeline Database Schema
BEGIN;

-- (Optional) enable useful extensions
-- CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
-- CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- ---------------------------------------------------------------------
-- Drop legacy/staging tables if they exist
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS clinical_measurements CASCADE;
DROP TABLE IF EXISTS measurements CASCADE;

-- Also drop our tables to allow idempotent re-runs in dev
DROP TABLE IF EXISTS fact_measurement CASCADE;
DROP TABLE IF EXISTS dim_participant CASCADE;
DROP TABLE IF EXISTS dim_study CASCADE;
DROP TABLE IF EXISTS dim_site CASCADE;
DROP TABLE IF EXISTS dim_measurement_type CASCADE;
DROP TABLE IF EXISTS dim_unit CASCADE;
DROP TABLE IF EXISTS etl_jobs CASCADE;

-- ---------------------------------------------------------------------
-- Dimension tables
-- ---------------------------------------------------------------------

-- Studies
CREATE TABLE dim_study (
  study_id TEXT PRIMARY KEY
);

-- Sites
CREATE TABLE dim_site (
  site_id TEXT PRIMARY KEY
);

-- Participants (composite PK to keep participant IDs scoped by study)
CREATE TABLE dim_participant (
  study_id      TEXT NOT NULL REFERENCES dim_study(study_id) ON UPDATE CASCADE ON DELETE RESTRICT,
  participant_id TEXT NOT NULL,
  PRIMARY KEY (study_id, participant_id)
);

-- Measurement Types (surrogate key)
CREATE TABLE dim_measurement_type (
  id   SERIAL PRIMARY KEY,
  name TEXT NOT NULL UNIQUE
);

-- Units (surrogate key)
CREATE TABLE dim_unit (
  id   SERIAL PRIMARY KEY,
  name TEXT NOT NULL UNIQUE
);

-- ---------------------------------------------------------------------
-- Fact table
-- ---------------------------------------------------------------------
CREATE TABLE fact_measurement (
  id                 BIGSERIAL PRIMARY KEY,

  -- Natural keys
  study_id           TEXT NOT NULL REFERENCES dim_study(study_id) ON UPDATE CASCADE ON DELETE RESTRICT,
  participant_id     TEXT NOT NULL,
  site_id            TEXT REFERENCES dim_site(site_id) ON UPDATE CASCADE ON DELETE SET NULL,

  -- Foreign keys to dimensions
  measurement_type_id INT  NOT NULL REFERENCES dim_measurement_type(id) ON UPDATE CASCADE ON DELETE RESTRICT,
  unit_id             INT  NOT NULL REFERENCES dim_unit(id)             ON UPDATE CASCADE ON DELETE RESTRICT,

  -- Values
  value_numeric      NUMERIC(12,4),
  systolic           SMALLINT,
  diastolic          SMALLINT,

  -- Quality
  quality_score      NUMERIC(4,3) NOT NULL DEFAULT 0 CHECK (quality_score >= 0 AND quality_score <= 1),
  is_valid           BOOLEAN NOT NULL DEFAULT TRUE,
  quality_flags      TEXT[] NOT NULL DEFAULT '{}',

  -- Time
  ts                 TIMESTAMPTZ NOT NULL,

  -- Lineage
  source_file        TEXT,

  -- FK to participant composite
  CONSTRAINT fk_fact_participant
    FOREIGN KEY (study_id, participant_id)
    REFERENCES dim_participant(study_id, participant_id)
    ON UPDATE CASCADE ON DELETE RESTRICT
);

-- ---------------------------------------------------------------------
-- Job tracking (optional but useful for API/E2E tests)
-- ---------------------------------------------------------------------
CREATE TABLE etl_jobs (
  id          UUID PRIMARY KEY,
  filename    TEXT NOT NULL,
  study_id    TEXT,
  status      TEXT NOT NULL, -- queued|running|completed|failed
  created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ---------------------------------------------------------------------
-- Indexes tuned for analytics
-- ---------------------------------------------------------------------

-- Common filter pattern: participant time series per study
CREATE INDEX idx_fact_study_part_ts
  ON fact_measurement (study_id, participant_id, ts DESC);

-- Fast filtering/aggregates by measurement type and time
CREATE INDEX idx_fact_meastype_ts
  ON fact_measurement (measurement_type_id, ts DESC);

-- Site-level rollups
CREATE INDEX idx_fact_site
  ON fact_measurement (site_id);

-- Quality filters
CREATE INDEX idx_fact_quality_score
  ON fact_measurement (quality_score);

-- Only the invalids (sparse)
CREATE INDEX idx_fact_invalid_only
  ON fact_measurement (ts)
  WHERE NOT is_valid;

-- Searchable flags (array)
CREATE INDEX idx_fact_quality_flags_gin
  ON fact_measurement USING GIN (quality_flags);

COMMIT;

-- ---------------------------------------------------------------------
-- Notes:
-- - The ETL must upsert into dim_* and then insert into fact_measurement
--   with the surrogate IDs (measurement_type_id, unit_id).
-- - No free-text measurement type or unit columns exist in the fact table.
-- - If you previously wrote to legacy tables, those are intentionally dropped.
-- ---------------------------------------------------------------------
