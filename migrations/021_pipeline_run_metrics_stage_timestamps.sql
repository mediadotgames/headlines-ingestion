-- Add stage_started_at and stage_completed_at to pipeline_run_metrics
-- for precise per-stage duration tracking in Grafana.
-- Both columns are nullable so existing rows and writers are unaffected.

ALTER TABLE public.pipeline_run_metrics
  ADD COLUMN IF NOT EXISTS stage_started_at  timestamptz;

ALTER TABLE public.pipeline_run_metrics
  ADD COLUMN IF NOT EXISTS stage_completed_at timestamptz;
