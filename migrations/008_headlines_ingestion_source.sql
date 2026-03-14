ALTER TABLE public.headlines
  ADD COLUMN IF NOT EXISTS ingestion_source text NOT NULL DEFAULT 'newsapi-org';

ALTER TABLE public.headlines
  ADD CONSTRAINT headlines_ingestion_source_chk
  CHECK (ingestion_source IN ('newsapi-org', 'rss', 'scraper'));