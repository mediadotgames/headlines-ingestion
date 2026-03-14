-- initdb/001_schema.sql
-- -----------------------------------------------------------------------------
-- Purpose:
--   Baseline schema for the DB with "documents" and the new "anomalies"
--   table (no "comments"). Designed to mimic Aurora PG features locally.
--
-- Conventions:
--   - UTC everywhere (timestamptz)
--   - ENUMs for controlled taxonomies
--   - Arrays for tags/outlets with GIN indexes for fast membership queries
--   - UUID PKs via pgcrypto
-- -----------------------------------------------------------------------------


-- Extensions commonly available on Aurora PG
CREATE EXTENSION IF NOT EXISTS pgcrypto;           -- gen_random_uuid()
CREATE EXTENSION IF NOT EXISTS pg_stat_statements; -- query telemetry

-- -----------------------------
-- Anomalies
-- -----------------------------

-- Controlled vocabularies (ENUMs)
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'anomaly_severity') THEN
    CREATE TYPE anomaly_severity AS ENUM ('Low','Medium','High','Critical');
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'anomaly_category') THEN
    CREATE TYPE anomaly_category AS ENUM (
      'Saturation Event',
      'Ignored Policy Proposals',
      'Ignored Human Rights Violation'
    );
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'anomaly_resolution_status') THEN
    CREATE TYPE anomaly_resolution_status AS ENUM ('Pending','Validated','Invalidated');
  END IF;
END$$;

-- Main table
CREATE TABLE IF NOT EXISTS anomalies (
  anomaly_id         uuid PRIMARY KEY DEFAULT gen_random_uuid(),

  -- Required descriptive fields
  summary            text        NOT NULL,   -- human-readable description
  report_time        timestamptz NOT NULL,   -- when reported (UTC)
  category           anomaly_category NOT NULL,
  severity           anomaly_severity  NOT NULL,

  -- Reporter & workflow
  reporter           text        NOT NULL,   -- "bots@..." or username
  tags               text[]      NOT NULL DEFAULT '{}',  -- String Array
  outlets            text[]      NOT NULL DEFAULT '{}',  -- String Array
  resolution_status  anomaly_resolution_status NOT NULL DEFAULT 'Pending',
  investigator       text,                   -- optional
  added_context      text,                   -- optional (community notes)

  -- Audit
  created_at         timestamptz NOT NULL DEFAULT now(),
  updated_at         timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE anomalies IS 'Operational anomalies detected by bots/users';
COMMENT ON COLUMN anomalies.summary           IS 'Short human-readable description';
COMMENT ON COLUMN anomalies.report_time       IS 'UTC timestamp the anomaly was reported';
COMMENT ON COLUMN anomalies.category          IS 'Controlled taxonomy (ENUM)';
COMMENT ON COLUMN anomalies.severity          IS 'Controlled severity (ENUM)';
COMMENT ON COLUMN anomalies.reporter          IS 'bots@<name> or username';
COMMENT ON COLUMN anomalies.tags              IS 'Freeform tags (text[])';
COMMENT ON COLUMN anomalies.outlets           IS 'Impacted systems or sources (text[])';
COMMENT ON COLUMN anomalies.resolution_status IS 'Pending/Validated/Invalidated';
COMMENT ON COLUMN anomalies.investigator      IS 'Assigned investigator';
COMMENT ON COLUMN anomalies.added_context     IS 'Extra context or community notes';

-- Indexes: time, status, and array membership
CREATE INDEX IF NOT EXISTS idx_anomalies_report_time
  ON anomalies (report_time);

CREATE INDEX IF NOT EXISTS idx_anomalies_resolution_status
  ON anomalies (resolution_status);

CREATE INDEX IF NOT EXISTS idx_anomalies_tags_gin
  ON anomalies USING GIN (tags);

CREATE INDEX IF NOT EXISTS idx_anomalies_outlets_gin
  ON anomalies USING GIN (outlets);

-- Auto-update updated_at on UPDATE
CREATE OR REPLACE FUNCTION tg_set_updated_at()
RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  NEW.updated_at := now();
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS set_updated_at ON anomalies;
CREATE TRIGGER set_updated_at
BEFORE UPDATE ON anomalies
FOR EACH ROW EXECUTE FUNCTION tg_set_updated_at();

