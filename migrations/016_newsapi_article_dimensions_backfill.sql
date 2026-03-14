BEGIN;

-- -------------------------------------------------------------------
-- Build a deterministic worklist for batching
-- -------------------------------------------------------------------
CREATE TEMP TABLE tmp_newsapi_article_backfill_worklist AS
SELECT
    uri,
    ROW_NUMBER() OVER (ORDER BY uri) AS rn
FROM public.newsapi_articles;

CREATE INDEX ON tmp_newsapi_article_backfill_worklist (rn);

DO $$
DECLARE
    v_batch_size integer := 5000;
    v_min_rn integer := 1;
    v_max_rn integer;
BEGIN
    SELECT MAX(rn) INTO v_max_rn
    FROM tmp_newsapi_article_backfill_worklist;

    WHILE v_min_rn <= COALESCE(v_max_rn, 0) LOOP

        -- -----------------------------------------------------------
        -- Sources
        -- Pick exactly one best row per source URI in this batch.
        -- Preference order:
        --   1) non-empty description
        --   2) non-empty image
        --   3) non-empty thumb_image
        --   4) non-null social_media
        --   5) non-null ranking
        --   6) non-null location
        -- -----------------------------------------------------------
        INSERT INTO public.newsapi_sources (
            uri,
            title,
            description,
            social_media,
            ranking,
            location,
            image,
            thumb_image
        )
        SELECT DISTINCT ON (src.uri)
            src.uri,
            src.title,
            src.description,
            src.social_media,
            src.ranking,
            src.location,
            src.image,
            src.thumb_image
        FROM (
            SELECT
                a.source->>'uri'         AS uri,
                a.source->>'title'       AS title,
                a.source->>'description' AS description,
                a.source->'socialMedia'  AS social_media,
                a.source->'ranking'      AS ranking,
                a.source->'location'     AS location,
                a.source->>'image'       AS image,
                a.source->>'thumbImage'  AS thumb_image
            FROM public.newsapi_articles a
            JOIN tmp_newsapi_article_backfill_worklist w
              ON w.uri = a.uri
            WHERE w.rn BETWEEN v_min_rn AND v_min_rn + v_batch_size - 1
              AND a.source IS NOT NULL
              AND jsonb_typeof(a.source) = 'object'
              AND a.source ? 'uri'
              AND NULLIF(a.source->>'uri', '') IS NOT NULL
        ) src
        ORDER BY
            src.uri,
            (NULLIF(src.description, '') IS NOT NULL) DESC,
            (NULLIF(src.image, '') IS NOT NULL) DESC,
            (NULLIF(src.thumb_image, '') IS NOT NULL) DESC,
            (src.social_media IS NOT NULL) DESC,
            (src.ranking IS NOT NULL) DESC,
            (src.location IS NOT NULL) DESC,
            src.title DESC NULLS LAST
        ON CONFLICT (uri) DO UPDATE
        SET
            title = COALESCE(EXCLUDED.title, public.newsapi_sources.title),
            description = COALESCE(EXCLUDED.description, public.newsapi_sources.description),
            social_media = COALESCE(EXCLUDED.social_media, public.newsapi_sources.social_media),
            ranking = COALESCE(EXCLUDED.ranking, public.newsapi_sources.ranking),
            location = COALESCE(EXCLUDED.location, public.newsapi_sources.location),
            image = COALESCE(EXCLUDED.image, public.newsapi_sources.image),
            thumb_image = COALESCE(EXCLUDED.thumb_image, public.newsapi_sources.thumb_image),
            updated_at = now();

        -- -----------------------------------------------------------
        -- source_uri on articles
        -- -----------------------------------------------------------
        UPDATE public.newsapi_articles a
        SET source_uri = a.source->>'uri'
        FROM tmp_newsapi_article_backfill_worklist w
        WHERE w.uri = a.uri
          AND w.rn BETWEEN v_min_rn AND v_min_rn + v_batch_size - 1
          AND a.source IS NOT NULL
          AND jsonb_typeof(a.source) = 'object'
          AND a.source ? 'uri'
          AND NULLIF(a.source->>'uri', '') IS NOT NULL
          AND a.source_uri IS DISTINCT FROM a.source->>'uri';

        -- -----------------------------------------------------------
        -- Concepts
        -- Pick exactly one best row per concept URI in this batch.
        -- Preference order:
        --   1) non-null type
        --   2) non-empty image
        --   3) non-null label
        --   4) non-null location
        --   5) non-null synonyms
        -- -----------------------------------------------------------
        INSERT INTO public.newsapi_concepts (
            uri,
            type,
            image,
            label,
            location,
            synonyms
        )
        SELECT DISTINCT ON (c.uri)
            c.uri,
            c.type,
            c.image,
            c.label,
            c.location,
            c.synonyms
        FROM (
            SELECT
                concept->>'uri'      AS uri,
                concept->>'type'     AS type,
                concept->>'image'    AS image,
                concept->'label'     AS label,
                concept->'location'  AS location,
                concept->'synonyms'  AS synonyms
            FROM public.newsapi_articles a
            JOIN tmp_newsapi_article_backfill_worklist w
              ON w.uri = a.uri
            CROSS JOIN LATERAL jsonb_array_elements(
                CASE
                    WHEN a.concepts IS NOT NULL AND jsonb_typeof(a.concepts) = 'array'
                        THEN a.concepts
                    ELSE '[]'::jsonb
                END
            ) AS concept
            WHERE w.rn BETWEEN v_min_rn AND v_min_rn + v_batch_size - 1
              AND concept ? 'uri'
              AND NULLIF(concept->>'uri', '') IS NOT NULL
        ) c
        ORDER BY
            c.uri,
            (NULLIF(c.type, '') IS NOT NULL) DESC,
            (NULLIF(c.image, '') IS NOT NULL) DESC,
            (c.label IS NOT NULL) DESC,
            (c.location IS NOT NULL) DESC,
            (c.synonyms IS NOT NULL) DESC
        ON CONFLICT (uri) DO UPDATE
        SET
            type = COALESCE(EXCLUDED.type, public.newsapi_concepts.type),
            image = COALESCE(EXCLUDED.image, public.newsapi_concepts.image),
            label = COALESCE(EXCLUDED.label, public.newsapi_concepts.label),
            location = COALESCE(EXCLUDED.location, public.newsapi_concepts.location),
            synonyms = COALESCE(EXCLUDED.synonyms, public.newsapi_concepts.synonyms),
            updated_at = now();

        -- -----------------------------------------------------------
        -- Article <-> concept joins
        -- -----------------------------------------------------------
        INSERT INTO public.newsapi_article_concepts (
            article_uri,
            concept_uri
        )
        SELECT DISTINCT
            a.uri,
            concept->>'uri'
        FROM public.newsapi_articles a
        JOIN tmp_newsapi_article_backfill_worklist w
          ON w.uri = a.uri
        CROSS JOIN LATERAL jsonb_array_elements(
            CASE
                WHEN a.concepts IS NOT NULL AND jsonb_typeof(a.concepts) = 'array'
                    THEN a.concepts
                ELSE '[]'::jsonb
            END
        ) AS concept
        WHERE w.rn BETWEEN v_min_rn AND v_min_rn + v_batch_size - 1
          AND concept ? 'uri'
          AND NULLIF(concept->>'uri', '') IS NOT NULL
        ON CONFLICT DO NOTHING;

        -- -----------------------------------------------------------
        -- Categories
        -- Pick exactly one best row per category URI in this batch.
        -- Preference order:
        --   1) non-empty parent_uri
        --   2) non-empty label
        -- -----------------------------------------------------------
        INSERT INTO public.newsapi_categories (
            uri,
            parent_uri,
            label
        )
        SELECT DISTINCT ON (c.uri)
            c.uri,
            c.parent_uri,
            c.label
        FROM (
            SELECT
                category->>'uri'       AS uri,
                category->>'parentUri' AS parent_uri,
                category->>'label'     AS label
            FROM public.newsapi_articles a
            JOIN tmp_newsapi_article_backfill_worklist w
              ON w.uri = a.uri
            CROSS JOIN LATERAL jsonb_array_elements(
                CASE
                    WHEN a.categories IS NOT NULL AND jsonb_typeof(a.categories) = 'array'
                        THEN a.categories
                    ELSE '[]'::jsonb
                END
            ) AS category
            WHERE w.rn BETWEEN v_min_rn AND v_min_rn + v_batch_size - 1
              AND category ? 'uri'
              AND NULLIF(category->>'uri', '') IS NOT NULL
        ) c
        ORDER BY
            c.uri,
            (NULLIF(c.parent_uri, '') IS NOT NULL) DESC,
            (NULLIF(c.label, '') IS NOT NULL) DESC
        ON CONFLICT (uri) DO UPDATE
        SET
            parent_uri = COALESCE(EXCLUDED.parent_uri, public.newsapi_categories.parent_uri),
            label = COALESCE(EXCLUDED.label, public.newsapi_categories.label),
            updated_at = now();

        -- -----------------------------------------------------------
        -- Article <-> category joins
        -- -----------------------------------------------------------
        INSERT INTO public.newsapi_article_categories (
            article_uri,
            category_uri
        )
        SELECT DISTINCT
            a.uri,
            category->>'uri'
        FROM public.newsapi_articles a
        JOIN tmp_newsapi_article_backfill_worklist w
          ON w.uri = a.uri
        CROSS JOIN LATERAL jsonb_array_elements(
            CASE
                WHEN a.categories IS NOT NULL AND jsonb_typeof(a.categories) = 'array'
                    THEN a.categories
                ELSE '[]'::jsonb
            END
        ) AS category
        WHERE w.rn BETWEEN v_min_rn AND v_min_rn + v_batch_size - 1
          AND category ? 'uri'
          AND NULLIF(category->>'uri', '') IS NOT NULL
        ON CONFLICT DO NOTHING;

        v_min_rn := v_min_rn + v_batch_size;
    END LOOP;
END $$;

COMMIT;