BEGIN;

CREATE TABLE IF NOT EXISTS public.pipeline_run_metrics (
    run_id timestamptz NOT NULL,
    ingestion_source text NOT NULL,
    run_type text NOT NULL,
    nth_run integer NOT NULL,
    metric_name text NOT NULL,
    metric_value bigint NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),

    PRIMARY KEY (run_id, ingestion_source, run_type, nth_run, metric_name)
);

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'pipeline_run_metrics_run_type_check'
  ) THEN
    ALTER TABLE public.pipeline_run_metrics
      ADD CONSTRAINT pipeline_run_metrics_run_type_check
      CHECK (run_type IN ('scheduled', 'backfill', 'seed'));
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'pipeline_run_metrics_nth_run_check'
  ) THEN
    ALTER TABLE public.pipeline_run_metrics
      ADD CONSTRAINT pipeline_run_metrics_nth_run_check
      CHECK (nth_run >= 1);
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_pipeline_run_metrics_run_lookup
  ON public.pipeline_run_metrics (run_id DESC, ingestion_source, run_type, nth_run);

CREATE INDEX IF NOT EXISTS idx_pipeline_run_metrics_metric_name
  ON public.pipeline_run_metrics (metric_name);

COMMIT;