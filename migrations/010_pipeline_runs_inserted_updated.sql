-- 010_pipeline_runs_inserted_updated.sql
-- Add inserted/updated breakdown for loader metrics
ALTER TABLE public.pipeline_runs
ADD COLUMN IF NOT EXISTS db_rows_inserted int NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS db_rows_updated int NOT NULL DEFAULT 0;
-- Optional sanity check: loaded should equal inserted+updated when everything attempts an upsert
-- (you can skip this constraint if you plan to allow "skipped" rows later)
-- ALTER TABLE public.pipeline_runs
--   ADD CONSTRAINT pipeline_runs_loaded_breakdown_chk
--   CHECK (rows_loaded = db_rows_inserted + db_rows_updated);
ALTER TABLE public.pipeline_runs DROP CONSTRAINT IF EXISTS pipeline_runs_rows_consistency;
ALTER TABLE public.pipeline_runs
ADD CONSTRAINT pipeline_runs_rows_consistency CHECK (rows_loaded = db_rows_inserted + db_rows_updated);