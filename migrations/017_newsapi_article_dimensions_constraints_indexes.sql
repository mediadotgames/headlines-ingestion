BEGIN;

-- -------------------------------------------------------------------
-- Foreign keys
-- -------------------------------------------------------------------
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'newsapi_articles_source_fk'
  ) THEN
    ALTER TABLE public.newsapi_articles
      ADD CONSTRAINT newsapi_articles_source_fk
      FOREIGN KEY (source_uri)
      REFERENCES public.newsapi_sources(uri);
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'newsapi_article_concepts_article_fk'
  ) THEN
    ALTER TABLE public.newsapi_article_concepts
      ADD CONSTRAINT newsapi_article_concepts_article_fk
      FOREIGN KEY (article_uri)
      REFERENCES public.newsapi_articles(uri)
      ON DELETE CASCADE;
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'newsapi_article_concepts_concept_fk'
  ) THEN
    ALTER TABLE public.newsapi_article_concepts
      ADD CONSTRAINT newsapi_article_concepts_concept_fk
      FOREIGN KEY (concept_uri)
      REFERENCES public.newsapi_concepts(uri)
      ON DELETE CASCADE;
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'newsapi_article_categories_article_fk'
  ) THEN
    ALTER TABLE public.newsapi_article_categories
      ADD CONSTRAINT newsapi_article_categories_article_fk
      FOREIGN KEY (article_uri)
      REFERENCES public.newsapi_articles(uri)
      ON DELETE CASCADE;
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'newsapi_article_categories_category_fk'
  ) THEN
    ALTER TABLE public.newsapi_article_categories
      ADD CONSTRAINT newsapi_article_categories_category_fk
      FOREIGN KEY (category_uri)
      REFERENCES public.newsapi_categories(uri)
      ON DELETE CASCADE;
  END IF;
END $$;

-- -------------------------------------------------------------------
-- Indexes
-- -------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_newsapi_articles_source_uri
  ON public.newsapi_articles (source_uri);

CREATE INDEX IF NOT EXISTS idx_newsapi_article_concepts_article_uri
  ON public.newsapi_article_concepts (article_uri);

CREATE INDEX IF NOT EXISTS idx_newsapi_article_concepts_concept_uri
  ON public.newsapi_article_concepts (concept_uri);

CREATE INDEX IF NOT EXISTS idx_newsapi_article_categories_article_uri
  ON public.newsapi_article_categories (article_uri);

CREATE INDEX IF NOT EXISTS idx_newsapi_article_categories_category_uri
  ON public.newsapi_article_categories (category_uri);

CREATE INDEX IF NOT EXISTS idx_newsapi_concepts_type
  ON public.newsapi_concepts (type);

CREATE INDEX IF NOT EXISTS idx_newsapi_categories_parent_uri
  ON public.newsapi_categories (parent_uri);

CREATE INDEX IF NOT EXISTS idx_newsapi_concepts_label_eng
  ON public.newsapi_concepts ((label->>'eng'));

COMMIT;