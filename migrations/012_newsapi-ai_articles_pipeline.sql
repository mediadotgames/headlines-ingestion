BEGIN;
ALTER TABLE public.newsapi_articles
ADD COLUMN IF NOT EXISTS run_id timestamptz,
    ADD COLUMN IF NOT EXISTS collected_at timestamptz,
    ADD COLUMN IF NOT EXISTS ingested_at timestamptz NOT NULL DEFAULT now();
ALTER TABLE public.newsapi_articles
ADD COLUMN IF NOT EXISTS ingestion_source text NOT NULL DEFAULT 'newsapi-ai';
CREATE INDEX IF NOT EXISTS idx_newsapi_articles_run_id ON public.newsapi_articles (run_id);
CREATE INDEX IF NOT EXISTS idx_newsapi_articles_collected_at ON public.newsapi_articles (collected_at);
CREATE INDEX IF NOT EXISTS idx_newsapi_articles_ingestion_source ON public.newsapi_articles (ingestion_source);
DO $$ BEGIN IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'newsapi_articles_ingestion_source_check'
) THEN
ALTER TABLE public.newsapi_articles
ADD CONSTRAINT newsapi_articles_ingestion_source_check CHECK (ingestion_source IN ('newsapi-ai'));
END IF;
END $$;
COMMIT;