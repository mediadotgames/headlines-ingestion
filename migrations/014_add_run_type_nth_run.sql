BEGIN;

-- 1) Add columns if missing

ALTER TABLE public.pipeline_runs
  ADD COLUMN IF NOT EXISTS run_type text,
  ADD COLUMN IF NOT EXISTS nth_run integer;

ALTER TABLE public.newsapi_articles
  ADD COLUMN IF NOT EXISTS run_type text,
  ADD COLUMN IF NOT EXISTS nth_run integer;

-- 2) Backfill pipeline_runs.run_type

UPDATE public.pipeline_runs
SET run_type = CASE
  WHEN run_id = '1900-01-01T00:00:00.000Z'::timestamptz THEN 'seed'
  ELSE 'scheduled'
END
WHERE run_type IS NULL;

-- 3) Backfill pipeline_runs.nth_run

WITH ranked AS (
  SELECT
    ctid,
    ROW_NUMBER() OVER (
      PARTITION BY run_id, ingestion_source, run_type
      ORDER BY
        created_at NULLS LAST,
        collected_at NULLS LAST,
        load_started_at NULLS LAST,
        load_completed_at NULLS LAST,
        updated_at NULLS LAST,
        ctid
    ) AS rn
  FROM public.pipeline_runs
)
UPDATE public.pipeline_runs p
SET nth_run = ranked.rn
FROM ranked
WHERE p.ctid = ranked.ctid
  AND p.nth_run IS NULL;

-- 4) Backfill newsapi_articles
-- Since pipeline_runs may be empty historically, fall back directly.

WITH chosen_runs AS (
  SELECT
    pr.run_id,
    pr.ingestion_source,
    pr.run_type,
    pr.nth_run,
    ROW_NUMBER() OVER (
      PARTITION BY pr.run_id, pr.ingestion_source
      ORDER BY
        CASE pr.run_type
          WHEN 'seed' THEN 1
          WHEN 'scheduled' THEN 2
          WHEN 'backfill' THEN 3
          ELSE 99
        END,
        pr.nth_run
    ) AS pick_one
  FROM public.pipeline_runs pr
)
UPDATE public.newsapi_articles a
SET
  run_type = cr.run_type,
  nth_run = cr.nth_run
FROM chosen_runs cr
WHERE a.run_id = cr.run_id
  AND a.ingestion_source = cr.ingestion_source
  AND cr.pick_one = 1
  AND (a.run_type IS NULL OR a.nth_run IS NULL);

UPDATE public.newsapi_articles
SET
  run_type = COALESCE(
    run_type,
    CASE
      WHEN run_id = '1900-01-01T00:00:00.000Z'::timestamptz THEN 'seed'
      ELSE 'scheduled'
    END
  ),
  nth_run = COALESCE(nth_run, 1)
WHERE run_type IS NULL OR nth_run IS NULL;

-- 5) Constraints

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'pipeline_runs_run_type_check'
  ) THEN
    ALTER TABLE public.pipeline_runs
      ADD CONSTRAINT pipeline_runs_run_type_check
      CHECK (run_type IN ('scheduled', 'backfill', 'seed'));
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'newsapi_articles_run_type_check'
  ) THEN
    ALTER TABLE public.newsapi_articles
      ADD CONSTRAINT newsapi_articles_run_type_check
      CHECK (run_type IN ('scheduled', 'backfill', 'seed'));
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'pipeline_runs_nth_run_check'
  ) THEN
    ALTER TABLE public.pipeline_runs
      ADD CONSTRAINT pipeline_runs_nth_run_check
      CHECK (nth_run >= 1);
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'newsapi_articles_nth_run_check'
  ) THEN
    ALTER TABLE public.newsapi_articles
      ADD CONSTRAINT newsapi_articles_nth_run_check
      CHECK (nth_run >= 1);
  END IF;
END $$;

-- 6) Make columns NOT NULL

ALTER TABLE public.pipeline_runs
  ALTER COLUMN run_type SET NOT NULL,
  ALTER COLUMN nth_run SET NOT NULL;

ALTER TABLE public.newsapi_articles
  ALTER COLUMN run_type SET NOT NULL,
  ALTER COLUMN nth_run SET NOT NULL;

-- 7) Drop FK if it exists, but do NOT recreate it now

ALTER TABLE public.newsapi_articles
  DROP CONSTRAINT IF EXISTS fk_newsapi_articles_pipeline_run;

-- 8) Replace pipeline_runs PK

ALTER TABLE public.pipeline_runs
  DROP CONSTRAINT IF EXISTS pipeline_runs_pk;

ALTER TABLE public.pipeline_runs
  DROP CONSTRAINT IF EXISTS pipeline_runs_pkey;

ALTER TABLE public.pipeline_runs
  ADD CONSTRAINT pipeline_runs_pk
  PRIMARY KEY (run_id, ingestion_source, run_type, nth_run);

-- 9) Helpful indexes

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_lookup
  ON public.pipeline_runs (run_id, ingestion_source, run_type, nth_run DESC);

CREATE INDEX IF NOT EXISTS idx_newsapi_articles_run_lookup
  ON public.newsapi_articles (run_id, ingestion_source, run_type, nth_run);

COMMIT;