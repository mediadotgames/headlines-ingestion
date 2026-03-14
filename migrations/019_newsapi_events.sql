BEGIN;

CREATE TABLE IF NOT EXISTS public.events (
    uri text PRIMARY KEY,

    -- promoted scalar / commonly queried fields
    total_article_count integer,
    relevance integer,
    event_date date,
    sentiment double precision,
    social_score double precision,

    -- promoted structured fields preserved as jsonb
    article_counts jsonb,
    title jsonb,
    summary jsonb,
    concepts jsonb,
    categories jsonb,
    common_dates jsonb,
    location jsonb,
    stories jsonb,
    images jsonb,

    -- internal upstream sort field; preserve, but do not use analytically
    wgt bigint,

    -- full upstream payload for audit/debug/reprocessing
    raw_event jsonb NOT NULL,

    -- freshness / lifecycle
    first_collected_at timestamptz NOT NULL DEFAULT now(),
    last_collected_at timestamptz NOT NULL DEFAULT now(),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema = 'public'
      AND table_name = 'events'
  )
  AND NOT EXISTS (
    SELECT 1
    FROM pg_trigger
    WHERE tgrelid = 'public.events'::regclass
      AND tgname = 'set_updated_at'
  ) THEN
    CREATE TRIGGER set_updated_at
    BEFORE UPDATE ON public.events
    FOR EACH ROW
    EXECUTE FUNCTION tg_set_updated_at();
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_events_event_date
  ON public.events (event_date DESC);

CREATE INDEX IF NOT EXISTS idx_events_last_collected_at
  ON public.events (last_collected_at DESC);

CREATE INDEX IF NOT EXISTS idx_events_total_article_count
  ON public.events (total_article_count DESC);

CREATE INDEX IF NOT EXISTS idx_events_social_score
  ON public.events (social_score DESC);

CREATE INDEX IF NOT EXISTS idx_events_relevance
  ON public.events (relevance DESC);

COMMIT;