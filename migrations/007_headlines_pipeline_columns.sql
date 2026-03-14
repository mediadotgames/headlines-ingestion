-- 007_headlines_pipeline_columns.sql
-- Adds ingestion pipeline observability columns + indexes (idempotent)

ALTER TABLE IF EXISTS public.headlines
  ADD COLUMN IF NOT EXISTS run_id timestamptz,
  ADD COLUMN IF NOT EXISTS collected_at timestamptz,
  ADD COLUMN IF NOT EXISTS ingested_at timestamptz NOT NULL DEFAULT now();

CREATE INDEX IF NOT EXISTS headlines_run_id_idx
ON public.headlines (run_id DESC);

CREATE INDEX IF NOT EXISTS headlines_collected_at_idx
ON public.headlines (collected_at DESC);

CREATE INDEX IF NOT EXISTS headlines_ingested_at_idx
ON public.headlines (ingested_at DESC);