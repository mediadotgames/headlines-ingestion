BEGIN;

ALTER TABLE public.newsapi_articles
  ADD COLUMN IF NOT EXISTS categories jsonb,
  ADD COLUMN IF NOT EXISTS concepts jsonb,
  ADD COLUMN IF NOT EXISTS links jsonb,
  ADD COLUMN IF NOT EXISTS videos jsonb,
  ADD COLUMN IF NOT EXISTS shares jsonb,
  ADD COLUMN IF NOT EXISTS duplicate_list jsonb,
  ADD COLUMN IF NOT EXISTS extracted_dates jsonb,
  ADD COLUMN IF NOT EXISTS location jsonb,
  ADD COLUMN IF NOT EXISTS original_article jsonb,
  ADD COLUMN IF NOT EXISTS raw_article jsonb;

CREATE INDEX IF NOT EXISTS idx_newsapi_articles_categories_gin
  ON public.newsapi_articles USING gin (categories);

CREATE INDEX IF NOT EXISTS idx_newsapi_articles_concepts_gin
  ON public.newsapi_articles USING gin (concepts);

CREATE INDEX IF NOT EXISTS idx_newsapi_articles_links_gin
  ON public.newsapi_articles USING gin (links);

CREATE INDEX IF NOT EXISTS idx_newsapi_articles_duplicate_list_gin
  ON public.newsapi_articles USING gin (duplicate_list);

-- Optional: defer this until raw_article is actually queried enough to justify it
-- CREATE INDEX IF NOT EXISTS idx_newsapi_articles_raw_article_gin
--   ON public.newsapi_articles USING gin (raw_article);

COMMIT;