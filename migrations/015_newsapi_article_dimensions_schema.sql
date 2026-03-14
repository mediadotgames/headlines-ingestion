BEGIN;

-- -------------------------------------------------------------------
-- Raw upstream source dimension (separate from curated public.outlets)
-- -------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.newsapi_sources (
    uri text PRIMARY KEY,
    title text,
    description text,
    social_media jsonb,
    ranking jsonb,
    location jsonb,
    image text,
    thumb_image text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

DROP TRIGGER IF EXISTS set_updated_at ON public.newsapi_sources;
CREATE TRIGGER set_updated_at
BEFORE UPDATE ON public.newsapi_sources
FOR EACH ROW
EXECUTE FUNCTION tg_set_updated_at();

-- -------------------------------------------------------------------
-- Canonical concepts
-- -------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.newsapi_concepts (
    uri text PRIMARY KEY,
    type text,
    image text,
    label jsonb,
    location jsonb,
    synonyms jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT newsapi_concepts_type_check
        CHECK (type IS NULL OR type IN ('person', 'loc', 'org', 'wiki'))
);

DROP TRIGGER IF EXISTS set_updated_at ON public.newsapi_concepts;
CREATE TRIGGER set_updated_at
BEFORE UPDATE ON public.newsapi_concepts
FOR EACH ROW
EXECUTE FUNCTION tg_set_updated_at();

-- -------------------------------------------------------------------
-- Canonical categories
-- -------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.newsapi_categories (
    uri text PRIMARY KEY,
    parent_uri text,
    label text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

DROP TRIGGER IF EXISTS set_updated_at ON public.newsapi_categories;
CREATE TRIGGER set_updated_at
BEFORE UPDATE ON public.newsapi_categories
FOR EACH ROW
EXECUTE FUNCTION tg_set_updated_at();

-- -------------------------------------------------------------------
-- Article -> concept relationship table
-- -------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.newsapi_article_concepts (
    article_uri text NOT NULL,
    concept_uri text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (article_uri, concept_uri)
);

-- -------------------------------------------------------------------
-- Article -> category relationship table
-- -------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.newsapi_article_categories (
    article_uri text NOT NULL,
    category_uri text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (article_uri, category_uri)
);

-- -------------------------------------------------------------------
-- Add source foreign-key column onto articles
-- -------------------------------------------------------------------
ALTER TABLE public.newsapi_articles
  ADD COLUMN IF NOT EXISTS source_uri text;

COMMIT;