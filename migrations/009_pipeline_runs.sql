-- 009_pipeline_runs.sql
-- Tracks ingestion pipeline execution for Grafana / health monitoring

CREATE TABLE IF NOT EXISTS public.pipeline_runs (
    -- Deterministic run identifier (we use the scheduled run time in UTC)
    run_id timestamptz NOT NULL,

    -- Which pipeline produced this run (e.g. newsapi-org, rss, scraper)
    ingestion_source text NOT NULL,

    -- Timing
    window_from timestamptz NOT NULL,
    window_to   timestamptz NOT NULL,

    collected_at timestamptz,                 -- when collector actually fetched data
    load_started_at timestamptz,              -- when loader began
    load_completed_at timestamptz,            -- when loader finished

    -- Counts
    articles_fetched int NOT NULL DEFAULT 0,  -- total fetched from upstream (may include duplicates)
    articles_deduped int NOT NULL DEFAULT 0,  -- unique by url (or your chosen identity)
    rows_loaded      int NOT NULL DEFAULT 0,  -- rows successfully upserted into headlines

    -- Status + error
    status text NOT NULL DEFAULT 'started',   -- started | collected | loaded | failed
    error_code text,
    error_message text,

    -- Auditing
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),

    -- One row per (run_id, ingestion_source)
    CONSTRAINT pipeline_runs_pk PRIMARY KEY (run_id, ingestion_source),

    -- Light validation to avoid accidental nonsense
    CONSTRAINT pipeline_runs_window_chk CHECK (window_from < window_to),
    CONSTRAINT pipeline_runs_status_chk CHECK (status IN ('started','collected','loaded','failed'))
);

-- Helpful indexes for Grafana queries
CREATE INDEX IF NOT EXISTS pipeline_runs_ingestion_source_idx
ON public.pipeline_runs (ingestion_source);

CREATE INDEX IF NOT EXISTS pipeline_runs_run_id_desc_idx
ON public.pipeline_runs (run_id DESC);

CREATE INDEX IF NOT EXISTS pipeline_runs_status_idx
ON public.pipeline_runs (status);

-- Keep updated_at current (assumes tg_set_updated_at() already exists in your DB)
DROP TRIGGER IF EXISTS set_updated_at ON public.pipeline_runs;
CREATE TRIGGER set_updated_at
BEFORE UPDATE ON public.pipeline_runs
FOR EACH ROW
EXECUTE FUNCTION tg_set_updated_at();