BEGIN;

-- -------------------------------------------------------------------
-- Add stage column to existing pipeline_run_metrics table
-- -------------------------------------------------------------------
ALTER TABLE public.pipeline_run_metrics
  ADD COLUMN IF NOT EXISTS stage text;

-- Backfill existing rows to article_load
UPDATE public.pipeline_run_metrics
SET stage = 'article_load'
WHERE stage IS NULL;

ALTER TABLE public.pipeline_run_metrics
  ALTER COLUMN stage SET DEFAULT 'article_load';

ALTER TABLE public.pipeline_run_metrics
  ALTER COLUMN stage SET NOT NULL;

-- -------------------------------------------------------------------
-- Add / update constraints
-- -------------------------------------------------------------------
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'pipeline_run_metrics_stage_check'
  ) THEN
    ALTER TABLE public.pipeline_run_metrics
      ADD CONSTRAINT pipeline_run_metrics_stage_check
      CHECK (stage IN (
        'article_collect',
        'article_load',
        'event_collect',
        'event_load'
      ));
  END IF;
END $$;

-- -------------------------------------------------------------------
-- Replace old primary key with new primary key including stage
-- -------------------------------------------------------------------
ALTER TABLE public.pipeline_run_metrics
  DROP CONSTRAINT IF EXISTS pipeline_run_metrics_pkey;

ALTER TABLE public.pipeline_run_metrics
  ADD CONSTRAINT pipeline_run_metrics_pkey
  PRIMARY KEY (run_id, ingestion_source, run_type, nth_run, stage, metric_name);

-- -------------------------------------------------------------------
-- Replace / add indexes
-- -------------------------------------------------------------------
DROP INDEX IF EXISTS idx_pipeline_run_metrics_run_lookup;

CREATE INDEX IF NOT EXISTS idx_pipeline_run_metrics_run_lookup
  ON public.pipeline_run_metrics (run_id DESC, ingestion_source, run_type, nth_run, stage);

CREATE INDEX IF NOT EXISTS idx_pipeline_run_metrics_stage_metric
  ON public.pipeline_run_metrics (stage, metric_name);

CREATE INDEX IF NOT EXISTS idx_pipeline_run_metrics_metric_name
  ON public.pipeline_run_metrics (metric_name);

COMMIT;