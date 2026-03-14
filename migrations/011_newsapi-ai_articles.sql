-- Migration: create newsapi-ai_articles table

CREATE TABLE IF NOT EXISTS public.newsapi_articles (
    uri text PRIMARY KEY,
    url text,
    title text,
    body text,
    date date,
    time time,
    date_time timestamptz,
    date_time_published timestamptz,
    lang text,
    is_duplicate boolean,
    data_type text,
    sentiment double precision,
    event_uri text,
    relevance integer,
    story_uri text,
    image text,
    source jsonb,
    authors jsonb,
    sim double precision,
    wgt bigint,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT newsapi_articles_data_type_check
        CHECK (data_type IS NULL OR data_type IN ('news', 'blog', 'pr'))
);

CREATE INDEX IF NOT EXISTS idx_newsapi_articles_date_time
    ON public.newsapi_articles (date_time DESC);

CREATE INDEX IF NOT EXISTS idx_newsapi_articles_date_time_published
    ON public.newsapi_articles (date_time_published DESC);

CREATE INDEX IF NOT EXISTS idx_newsapi_articles_event_uri
    ON public.newsapi_articles (event_uri);

CREATE INDEX IF NOT EXISTS idx_newsapi_articles_story_uri
    ON public.newsapi_articles (story_uri);

CREATE INDEX IF NOT EXISTS idx_newsapi_articles_lang
    ON public.newsapi_articles (lang);

CREATE INDEX IF NOT EXISTS idx_newsapi_articles_data_type
    ON public.newsapi_articles (data_type);

CREATE INDEX IF NOT EXISTS idx_newsapi_articles_is_duplicate
    ON public.newsapi_articles (is_duplicate);

CREATE INDEX IF NOT EXISTS idx_newsapi_articles_relevance
    ON public.newsapi_articles (relevance DESC);

CREATE INDEX IF NOT EXISTS idx_newsapi_articles_source_gin
    ON public.newsapi_articles
    USING gin (source);

CREATE INDEX IF NOT EXISTS idx_newsapi_articles_authors_gin
    ON public.newsapi_articles
    USING gin (authors);