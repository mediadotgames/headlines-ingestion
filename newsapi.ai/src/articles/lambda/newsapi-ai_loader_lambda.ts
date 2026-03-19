import "dotenv/config";
import * as fs from "node:fs";
import * as path from "node:path";
import { pipeline } from "node:stream/promises";
import { Client } from "pg";
import { from as copyFrom } from "pg-copy-streams";
import {
  S3Client,
  GetObjectCommand,
  PutObjectCommand,
} from "@aws-sdk/client-s3";
import type { S3Event } from "aws-lambda";
import {
  csvHeader,
  ARTIFACT_CONTRACT_VERSION,
} from "../shared/newsapi-aiArtifactSchema";

const _log = console.log;
const _err = console.error;
const _warn = console.warn;
console.log = (...a: unknown[]) => _log("\n", ...a);
console.error = (...a: unknown[]) => _err("\n", ...a);
console.warn = (...a: unknown[]) => _warn("\n", ...a);

const DATABASE_URL = process.env.DATABASE_URL!;
if (!DATABASE_URL) throw new Error("Missing DATABASE_URL");

const s3 = new S3Client({});

type Manifest = {
  artifact_contract_version?: string;
  ingestion_source: string;
  run_id: string;
  run_type: string;
  nth_run: number;
  collected_at: string;
  window_from?: string;
  window_to?: string;
};

type ArtifactDiagnostics = {
  rowsInArtifact: number;
  minDateTimePublished: string | null;
  maxDateTimePublished: string | null;
  populatedCounts: Record<string, number>;
  sampleUris: string[];
};

function truncateErrorMessage(msg: string, max = 2000): string {
  return msg.length <= max ? msg : msg.slice(0, max);
}

function parseS3RecordKey(rawKey: string): string {
  return decodeURIComponent(rawKey.replace(/\+/g, " "));
}

async function s3GetText(bucket: string, key: string): Promise<string> {
  const resp = await s3.send(
    new GetObjectCommand({ Bucket: bucket, Key: key }),
  );
  if (!resp.Body) {
    throw new Error(`S3 GetObject returned empty body: s3://${bucket}/${key}`);
  }
  return await new Response(resp.Body as any).text();
}

async function s3DownloadToFile(
  bucket: string,
  key: string,
  destPath: string,
): Promise<void> {
  const resp = await s3.send(
    new GetObjectCommand({ Bucket: bucket, Key: key }),
  );
  if (!resp.Body) {
    throw new Error(`S3 GetObject returned empty body: s3://${bucket}/${key}`);
  }

  await fs.promises.mkdir(path.dirname(destPath), { recursive: true });
  const writeStream = fs.createWriteStream(destPath);
  await pipeline(resp.Body as any, writeStream);
}

async function s3PutJson(
  bucket: string,
  key: string,
  obj: unknown,
): Promise<void> {
  const body = JSON.stringify(obj, null, 2);
  await s3.send(
    new PutObjectCommand({
      Bucket: bucket,
      Key: key,
      Body: body,
      ContentType: "application/json",
    }),
  );
}

async function s3PutInvocationJson(
  bucket: string,
  runPrefix: string,
  requestId: string,
  obj: unknown,
): Promise<string> {
  const key = `${runPrefix}load_reports/${requestId}.json`;
  await s3PutJson(bucket, key, obj);
  return key;
}

async function setLoadStarted(
  db: Client,
  runId: string,
  ingestionSource: string,
  runType: string,
  nthRun: number,
  whenIso: string,
) {
  await db.query(
    `
    UPDATE public.pipeline_runs
    SET
      load_started_at = COALESCE(load_started_at, $5),
      updated_at = now()
    WHERE run_id = $1
      AND ingestion_source = $2
      AND run_type = $3
      AND nth_run = $4
    `,
    [runId, ingestionSource, runType, nthRun, whenIso],
  );
}

async function setLoadCompleted(
  db: Client,
  runId: string,
  ingestionSource: string,
  runType: string,
  nthRun: number,
  whenIso: string,
  rowsLoaded: number,
  dbRowsInserted: number,
  dbRowsUpdated: number,
) {
  await db.query(
    `
    UPDATE public.pipeline_runs
    SET
      load_completed_at = $5,
      rows_loaded = $6,
      db_rows_inserted = $7,
      db_rows_updated = $8,
      status = 'loaded',
      error_code = NULL,
      error_message = NULL,
      updated_at = now()
    WHERE run_id = $1
      AND ingestion_source = $2
      AND run_type = $3
      AND nth_run = $4
    `,
    [
      runId,
      ingestionSource,
      runType,
      nthRun,
      whenIso,
      rowsLoaded,
      dbRowsInserted,
      dbRowsUpdated,
    ],
  );
}

async function upsertPipelineRunMetrics(
  db: Client,
  runId: string,
  ingestionSource: string,
  runType: string,
  nthRun: number,
  stageStartedAt: string,
  stageCompletedAt: string,
) {
  const res = await db.query(
    `
    WITH article_scope AS (
      SELECT
        a.uri,
        a.source_uri
      FROM public.newsapi_articles a
      WHERE a.run_id = $1
        AND a.ingestion_source = $2
        AND a.run_type = $3
        AND a.nth_run = $4
    ),
    metrics AS (
      SELECT 'articles_upserted'::text AS metric_name, COUNT(*)::bigint AS metric_value
      FROM article_scope

      UNION ALL

      SELECT 'articles_with_source_uri'::text, COUNT(*)::bigint
      FROM article_scope
      WHERE source_uri IS NOT NULL

      UNION ALL

      SELECT 'distinct_sources_in_articles'::text, COUNT(DISTINCT source_uri)::bigint
      FROM article_scope
      WHERE source_uri IS NOT NULL

      UNION ALL

      SELECT 'article_concept_links'::text, COUNT(*)::bigint
      FROM public.newsapi_article_concepts ac
      JOIN article_scope a
        ON a.uri = ac.article_uri

      UNION ALL

      SELECT 'distinct_concepts_linked'::text, COUNT(DISTINCT ac.concept_uri)::bigint
      FROM public.newsapi_article_concepts ac
      JOIN article_scope a
        ON a.uri = ac.article_uri

      UNION ALL

      SELECT 'article_category_links'::text, COUNT(*)::bigint
      FROM public.newsapi_article_categories ac
      JOIN article_scope a
        ON a.uri = ac.article_uri

      UNION ALL

      SELECT 'distinct_categories_linked'::text, COUNT(DISTINCT ac.category_uri)::bigint
      FROM public.newsapi_article_categories ac
      JOIN article_scope a
        ON a.uri = ac.article_uri
    )
    INSERT INTO public.pipeline_run_metrics (
      run_id,
      ingestion_source,
      run_type,
      nth_run,
      stage,
      metric_name,
      metric_value,
      stage_started_at,
      stage_completed_at
    )
    SELECT
      $1::timestamptz,
      $2::text,
      $3::text,
      $4::integer,
      'article_load'::text,
      metric_name,
      metric_value,
      $5::timestamptz,
      $6::timestamptz
    FROM metrics
    ON CONFLICT (run_id, ingestion_source, run_type, nth_run, stage, metric_name)
    DO UPDATE SET
      metric_value = EXCLUDED.metric_value,
      stage_started_at = EXCLUDED.stage_started_at,
      stage_completed_at = EXCLUDED.stage_completed_at
    RETURNING metric_name, metric_value
    `,
    [runId, ingestionSource, runType, nthRun, stageStartedAt, stageCompletedAt],
  );

  return res.rows as Array<{
    metric_name: string;
    metric_value: string | number;
  }>;
}

async function markRunFailed(
  db: Client,
  runId: string,
  ingestionSource: string,
  runType: string,
  nthRun: number,
  code: string,
  message: string,
) {
  await db.query(
    `
    UPDATE public.pipeline_runs
    SET
      status = 'failed',
      error_code = $5,
      error_message = $6,
      updated_at = now()
    WHERE run_id = $1
      AND ingestion_source = $2
      AND run_type = $3
      AND nth_run = $4
    `,
    [
      runId,
      ingestionSource,
      runType,
      nthRun,
      code,
      truncateErrorMessage(message),
    ],
  );
}

type PipelineRunLoadState = {
  status: string | null;
  loadStartedAt: string | null;
  loadCompletedAt: string | null;
};

async function getPipelineRunLoadState(
  db: Client,
  runId: string,
  ingestionSource: string,
  runType: string,
  nthRun: number,
): Promise<PipelineRunLoadState> {
  const res = await db.query(
    `
    SELECT
      status,
      load_started_at,
      load_completed_at
    FROM public.pipeline_runs
    WHERE run_id = $1
      AND ingestion_source = $2
      AND run_type = $3
      AND nth_run = $4
    `,
    [runId, ingestionSource, runType, nthRun],
  );

  const row = res.rows[0];
  return {
    status: row?.status ?? null,
    loadStartedAt: row?.load_started_at
      ? new Date(row.load_started_at).toISOString()
      : null,
    loadCompletedAt: row?.load_completed_at
      ? new Date(row.load_completed_at).toISOString()
      : null,
  };
}

async function copyCsvIntoTempTable(db: Client, csvPath: string) {
  await db.query(`
    CREATE TEMP TABLE tmp_newsapi_ai_load (
      uri                 text,
      run_type            text,
      nth_run             text,
      url                 text,
      title               text,
      body                text,
      date                text,
      time                text,
      date_time           text,
      date_time_published text,
      lang                text,
      is_duplicate        text,
      data_type           text,
      sentiment           text,
      event_uri           text,
      relevance           text,
      story_uri           text,
      image               text,
      source              text,
      authors             text,
      sim                 text,
      wgt                 text,
      categories          text,
      concepts            text,
      links               text,
      videos              text,
      shares              text,
      duplicate_list      text,
      extracted_dates     text,
      location            text,
      original_article    text,
      raw_article         text
    ) ON COMMIT DROP
  `);

  const copySql = `
    COPY tmp_newsapi_ai_load (
      uri,
      run_type,
      nth_run,
      url,
      title,
      body,
      date,
      time,
      date_time,
      date_time_published,
      lang,
      is_duplicate,
      data_type,
      sentiment,
      event_uri,
      relevance,
      story_uri,
      image,
      source,
      authors,
      sim,
      wgt,
      categories,
      concepts,
      links,
      videos,
      shares,
      duplicate_list,
      extracted_dates,
      location,
      original_article,
      raw_article
    )
    FROM STDIN WITH (FORMAT csv, HEADER true)
  `;

  const stream = db.query(copyFrom(copySql));
  await pipeline(fs.createReadStream(csvPath), stream as any);
}

async function countArtifactRows(db: Client) {
  const totalRes = await db.query(`
    SELECT COUNT(*)::int AS n
    FROM tmp_newsapi_ai_load
  `);

  const attemptedRes = await db.query(`
    SELECT COUNT(*)::int AS n
    FROM tmp_newsapi_ai_load
    WHERE NULLIF(uri, '') IS NOT NULL
  `);

  return {
    rowsInArtifact: Number(totalRes.rows[0].n),
    rowsAttempted: Number(attemptedRes.rows[0].n),
  };
}

async function getArtifactDiagnostics(
  db: Client,
): Promise<ArtifactDiagnostics> {
  const summaryRes = await db.query(`
    SELECT
      COUNT(*)::int AS rows_in_artifact,
      MIN(NULLIF(date_time_published, '')::timestamptz) AS min_date_time_published,
      MAX(NULLIF(date_time_published, '')::timestamptz) AS max_date_time_published,
      COUNT(*) FILTER (WHERE NULLIF(source, '') IS NOT NULL AND NULLIF(source, '') <> 'null')::int AS source_count,
      COUNT(*) FILTER (WHERE NULLIF(authors, '') IS NOT NULL AND NULLIF(authors, '') <> 'null')::int AS authors_count,
      COUNT(*) FILTER (WHERE NULLIF(categories, '') IS NOT NULL AND NULLIF(categories, '') <> 'null')::int AS categories_count,
      COUNT(*) FILTER (WHERE NULLIF(concepts, '') IS NOT NULL AND NULLIF(concepts, '') <> 'null')::int AS concepts_count,
      COUNT(*) FILTER (WHERE NULLIF(links, '') IS NOT NULL AND NULLIF(links, '') <> 'null')::int AS links_count,
      COUNT(*) FILTER (WHERE NULLIF(videos, '') IS NOT NULL AND NULLIF(videos, '') <> 'null')::int AS videos_count,
      COUNT(*) FILTER (WHERE NULLIF(shares, '') IS NOT NULL AND NULLIF(shares, '') <> 'null')::int AS shares_count,
      COUNT(*) FILTER (WHERE NULLIF(duplicate_list, '') IS NOT NULL AND NULLIF(duplicate_list, '') <> 'null')::int AS duplicate_list_count,
      COUNT(*) FILTER (WHERE NULLIF(extracted_dates, '') IS NOT NULL AND NULLIF(extracted_dates, '') <> 'null')::int AS extracted_dates_count,
      COUNT(*) FILTER (WHERE NULLIF(location, '') IS NOT NULL AND NULLIF(location, '') <> 'null')::int AS location_count,
      COUNT(*) FILTER (WHERE NULLIF(original_article, '') IS NOT NULL AND NULLIF(original_article, '') <> 'null')::int AS original_article_count,
      COUNT(*) FILTER (WHERE NULLIF(raw_article, '') IS NOT NULL AND NULLIF(raw_article, '') <> 'null')::int AS raw_article_count
    FROM tmp_newsapi_ai_load
  `);

  const sampleRes = await db.query(`
    SELECT uri
    FROM tmp_newsapi_ai_load
    WHERE NULLIF(uri, '') IS NOT NULL
    ORDER BY uri
    LIMIT 3
  `);

  const row = summaryRes.rows[0];

  return {
    rowsInArtifact: Number(row.rows_in_artifact),
    minDateTimePublished: row.min_date_time_published
      ? new Date(row.min_date_time_published).toISOString()
      : null,
    maxDateTimePublished: row.max_date_time_published
      ? new Date(row.max_date_time_published).toISOString()
      : null,
    populatedCounts: {
      source: Number(row.source_count),
      authors: Number(row.authors_count),
      categories: Number(row.categories_count),
      concepts: Number(row.concepts_count),
      links: Number(row.links_count),
      videos: Number(row.videos_count),
      shares: Number(row.shares_count),
      duplicate_list: Number(row.duplicate_list_count),
      extracted_dates: Number(row.extracted_dates_count),
      location: Number(row.location_count),
      original_article: Number(row.original_article_count),
      raw_article: Number(row.raw_article_count),
    },
    sampleUris: sampleRes.rows.map((r) => String(r.uri)),
  };
}

async function bulkUpsertFromTemp(
  db: Client,
  ingestionSource: string,
  runId: string,
  runType: string,
  nthRun: number,
  collectedAt: string,
) {
  const res = await db.query(
    `
    WITH upserted AS (
      INSERT INTO public.newsapi_articles (
        uri,
        url,
        title,
        body,
        date,
        time,
        date_time,
        date_time_published,
        lang,
        is_duplicate,
        data_type,
        sentiment,
        event_uri,
        relevance,
        story_uri,
        image,
        source,
        authors,
        sim,
        wgt,
        categories,
        concepts,
        links,
        videos,
        shares,
        duplicate_list,
        extracted_dates,
        location,
        original_article,
        raw_article,
        ingestion_source,
        run_id,
        run_type,
        nth_run,
        collected_at,
        ingested_at
      )
      SELECT
        NULLIF(uri, '') AS uri,
        NULLIF(url, '') AS url,
        NULLIF(title, '') AS title,
        NULLIF(body, '') AS body,
        NULLIF(date, '')::date AS date,
        NULLIF(time, '')::time AS time,
        NULLIF(date_time, '')::timestamptz AS date_time,
        NULLIF(date_time_published, '')::timestamptz AS date_time_published,
        NULLIF(lang, '') AS lang,
        CASE
          WHEN lower(NULLIF(is_duplicate, '')) = 'true' THEN true
          WHEN lower(NULLIF(is_duplicate, '')) = 'false' THEN false
          ELSE NULL
        END AS is_duplicate,
        NULLIF(data_type, '') AS data_type,
        NULLIF(sentiment, '')::double precision AS sentiment,
        NULLIF(event_uri, '') AS event_uri,
        NULLIF(relevance, '')::integer AS relevance,
        NULLIF(story_uri, '') AS story_uri,
        NULLIF(image, '') AS image,
        NULLIF(source, '')::jsonb AS source,
        NULLIF(authors, '')::jsonb AS authors,
        NULLIF(sim, '')::double precision AS sim,
        NULLIF(wgt, '')::bigint AS wgt,
        NULLIF(categories, '')::jsonb AS categories,
        NULLIF(concepts, '')::jsonb AS concepts,
        NULLIF(links, '')::jsonb AS links,
        NULLIF(videos, '')::jsonb AS videos,
        NULLIF(shares, '')::jsonb AS shares,
        NULLIF(duplicate_list, '')::jsonb AS duplicate_list,
        NULLIF(extracted_dates, '')::jsonb AS extracted_dates,
        NULLIF(location, '')::jsonb AS location,
        NULLIF(original_article, '')::jsonb AS original_article,
        NULLIF(raw_article, '')::jsonb AS raw_article,
        $1 AS ingestion_source,
        $2::timestamptz AS run_id,
        COALESCE(NULLIF(run_type, ''), $3) AS run_type,
        COALESCE(NULLIF(nth_run, '')::integer, $4::integer) AS nth_run,
        $5::timestamptz AS collected_at,
        now() AS ingested_at
      FROM tmp_newsapi_ai_load
      WHERE NULLIF(uri, '') IS NOT NULL
      ON CONFLICT (uri) DO UPDATE SET
        url = EXCLUDED.url,
        title = EXCLUDED.title,
        body = EXCLUDED.body,
        date = EXCLUDED.date,
        time = EXCLUDED.time,
        date_time = EXCLUDED.date_time,
        date_time_published = EXCLUDED.date_time_published,
        lang = EXCLUDED.lang,
        is_duplicate = EXCLUDED.is_duplicate,
        data_type = EXCLUDED.data_type,
        sentiment = EXCLUDED.sentiment,
        event_uri = EXCLUDED.event_uri,
        relevance = EXCLUDED.relevance,
        story_uri = EXCLUDED.story_uri,
        image = EXCLUDED.image,
        source = EXCLUDED.source,
        authors = EXCLUDED.authors,
        sim = EXCLUDED.sim,
        wgt = EXCLUDED.wgt,
        categories = EXCLUDED.categories,
        concepts = EXCLUDED.concepts,
        links = EXCLUDED.links,
        videos = EXCLUDED.videos,
        shares = EXCLUDED.shares,
        duplicate_list = EXCLUDED.duplicate_list,
        extracted_dates = EXCLUDED.extracted_dates,
        location = EXCLUDED.location,
        original_article = EXCLUDED.original_article,
        raw_article = EXCLUDED.raw_article,
        ingestion_source = EXCLUDED.ingestion_source,
        run_id = EXCLUDED.run_id,
        run_type = EXCLUDED.run_type,
        nth_run = EXCLUDED.nth_run,
        collected_at = EXCLUDED.collected_at,
        updated_at = now()
      WHERE
        public.newsapi_articles.url IS DISTINCT FROM EXCLUDED.url OR
        public.newsapi_articles.title IS DISTINCT FROM EXCLUDED.title OR
        public.newsapi_articles.body IS DISTINCT FROM EXCLUDED.body OR
        public.newsapi_articles.date IS DISTINCT FROM EXCLUDED.date OR
        public.newsapi_articles.time IS DISTINCT FROM EXCLUDED.time OR
        public.newsapi_articles.date_time IS DISTINCT FROM EXCLUDED.date_time OR
        public.newsapi_articles.date_time_published IS DISTINCT FROM EXCLUDED.date_time_published OR
        public.newsapi_articles.lang IS DISTINCT FROM EXCLUDED.lang OR
        public.newsapi_articles.is_duplicate IS DISTINCT FROM EXCLUDED.is_duplicate OR
        public.newsapi_articles.data_type IS DISTINCT FROM EXCLUDED.data_type OR
        public.newsapi_articles.sentiment IS DISTINCT FROM EXCLUDED.sentiment OR
        public.newsapi_articles.event_uri IS DISTINCT FROM EXCLUDED.event_uri OR
        public.newsapi_articles.relevance IS DISTINCT FROM EXCLUDED.relevance OR
        public.newsapi_articles.story_uri IS DISTINCT FROM EXCLUDED.story_uri OR
        public.newsapi_articles.image IS DISTINCT FROM EXCLUDED.image OR
        public.newsapi_articles.source IS DISTINCT FROM EXCLUDED.source OR
        public.newsapi_articles.authors IS DISTINCT FROM EXCLUDED.authors OR
        public.newsapi_articles.sim IS DISTINCT FROM EXCLUDED.sim OR
        public.newsapi_articles.wgt IS DISTINCT FROM EXCLUDED.wgt OR
        public.newsapi_articles.categories IS DISTINCT FROM EXCLUDED.categories OR
        public.newsapi_articles.concepts IS DISTINCT FROM EXCLUDED.concepts OR
        public.newsapi_articles.links IS DISTINCT FROM EXCLUDED.links OR
        public.newsapi_articles.videos IS DISTINCT FROM EXCLUDED.videos OR
        public.newsapi_articles.shares IS DISTINCT FROM EXCLUDED.shares OR
        public.newsapi_articles.duplicate_list IS DISTINCT FROM EXCLUDED.duplicate_list OR
        public.newsapi_articles.extracted_dates IS DISTINCT FROM EXCLUDED.extracted_dates OR
        public.newsapi_articles.location IS DISTINCT FROM EXCLUDED.location OR
        public.newsapi_articles.original_article IS DISTINCT FROM EXCLUDED.original_article OR
        public.newsapi_articles.raw_article IS DISTINCT FROM EXCLUDED.raw_article OR
        public.newsapi_articles.ingestion_source IS DISTINCT FROM EXCLUDED.ingestion_source OR
        public.newsapi_articles.run_id IS DISTINCT FROM EXCLUDED.run_id OR
        public.newsapi_articles.run_type IS DISTINCT FROM EXCLUDED.run_type OR
        public.newsapi_articles.nth_run IS DISTINCT FROM EXCLUDED.nth_run OR
        public.newsapi_articles.collected_at IS DISTINCT FROM EXCLUDED.collected_at
      RETURNING (xmax = 0) AS inserted
    )
    SELECT
      COUNT(*)::int AS rows_loaded,
      COUNT(*) FILTER (WHERE inserted)::int AS db_rows_inserted,
      COUNT(*) FILTER (WHERE NOT inserted)::int AS db_rows_updated
    FROM upserted
    `,
    [ingestionSource, runId, runType, nthRun, collectedAt],
  );

  return {
    rowsLoaded: Number(res.rows[0].rows_loaded),
    dbRowsInserted: Number(res.rows[0].db_rows_inserted),
    dbRowsUpdated: Number(res.rows[0].db_rows_updated),
  };
}

async function syncNormalizedArticleDimensionsFromTemp(db: Client) {
  await db.query(
    `
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
        NULLIF((NULLIF(t.source, '')::jsonb)->>'uri', '') AS uri,
        NULLIF((NULLIF(t.source, '')::jsonb)->>'title', '') AS title,
        NULLIF((NULLIF(t.source, '')::jsonb)->>'description', '') AS description,
        (NULLIF(t.source, '')::jsonb)->'socialMedia' AS social_media,
        (NULLIF(t.source, '')::jsonb)->'ranking' AS ranking,
        (NULLIF(t.source, '')::jsonb)->'location' AS location,
        NULLIF((NULLIF(t.source, '')::jsonb)->>'image', '') AS image,
        NULLIF((NULLIF(t.source, '')::jsonb)->>'thumbImage', '') AS thumb_image
      FROM tmp_newsapi_ai_load t
      WHERE NULLIF(t.uri, '') IS NOT NULL
        AND NULLIF(t.source, '') IS NOT NULL
        AND NULLIF(t.source, '') <> 'null'
        AND jsonb_typeof(NULLIF(t.source, '')::jsonb) = 'object'
        AND (NULLIF(t.source, '')::jsonb ? 'uri')
    ) src
    WHERE src.uri IS NOT NULL
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
      updated_at = now()
    `,
  );

  await db.query(
    `
    WITH normalized_sources AS (
      SELECT
        NULLIF(t.uri, '') AS article_uri,
        CASE
          WHEN NULLIF(t.source, '') IS NOT NULL
            AND NULLIF(t.source, '') <> 'null'
            AND jsonb_typeof(NULLIF(t.source, '')::jsonb) = 'object'
            AND (NULLIF(t.source, '')::jsonb ? 'uri')
          THEN NULLIF((NULLIF(t.source, '')::jsonb)->>'uri', '')
          ELSE NULL
        END AS source_uri
      FROM tmp_newsapi_ai_load t
      WHERE NULLIF(t.uri, '') IS NOT NULL
    )
    UPDATE public.newsapi_articles a
    SET
      source_uri = s.source_uri,
      updated_at = now()
    FROM normalized_sources s
    WHERE a.uri = s.article_uri
      AND a.source_uri IS DISTINCT FROM s.source_uri
    `,
  );

  await db.query(
    `
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
        NULLIF(concept->>'uri', '') AS uri,
        NULLIF(concept->>'type', '') AS type,
        NULLIF(concept->>'image', '') AS image,
        concept->'label' AS label,
        concept->'location' AS location,
        concept->'synonyms' AS synonyms
      FROM tmp_newsapi_ai_load t
      CROSS JOIN LATERAL jsonb_array_elements(
        CASE
          WHEN NULLIF(t.concepts, '') IS NOT NULL
            AND NULLIF(t.concepts, '') <> 'null'
            AND jsonb_typeof(NULLIF(t.concepts, '')::jsonb) = 'array'
          THEN NULLIF(t.concepts, '')::jsonb
          ELSE '[]'::jsonb
        END
      ) AS concept
      WHERE NULLIF(t.uri, '') IS NOT NULL
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
      updated_at = now()
    `,
  );

  await db.query(
    `
    DELETE FROM public.newsapi_article_concepts ac
    USING (
      SELECT DISTINCT NULLIF(uri, '') AS article_uri
      FROM tmp_newsapi_ai_load
      WHERE NULLIF(uri, '') IS NOT NULL
    ) t
    WHERE ac.article_uri = t.article_uri
    `,
  );

  await db.query(
    `
    INSERT INTO public.newsapi_article_concepts (
      article_uri,
      concept_uri
    )
    SELECT DISTINCT
      NULLIF(t.uri, '') AS article_uri,
      NULLIF(concept->>'uri', '') AS concept_uri
    FROM tmp_newsapi_ai_load t
    CROSS JOIN LATERAL jsonb_array_elements(
      CASE
        WHEN NULLIF(t.concepts, '') IS NOT NULL
          AND NULLIF(t.concepts, '') <> 'null'
          AND jsonb_typeof(NULLIF(t.concepts, '')::jsonb) = 'array'
        THEN NULLIF(t.concepts, '')::jsonb
        ELSE '[]'::jsonb
      END
    ) AS concept
    WHERE NULLIF(t.uri, '') IS NOT NULL
      AND concept ? 'uri'
      AND NULLIF(concept->>'uri', '') IS NOT NULL
    ON CONFLICT DO NOTHING
    `,
  );

  await db.query(
    `
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
        NULLIF(category->>'uri', '') AS uri,
        NULLIF(category->>'parentUri', '') AS parent_uri,
        NULLIF(category->>'label', '') AS label
      FROM tmp_newsapi_ai_load t
      CROSS JOIN LATERAL jsonb_array_elements(
        CASE
          WHEN NULLIF(t.categories, '') IS NOT NULL
            AND NULLIF(t.categories, '') <> 'null'
            AND jsonb_typeof(NULLIF(t.categories, '')::jsonb) = 'array'
          THEN NULLIF(t.categories, '')::jsonb
          ELSE '[]'::jsonb
        END
      ) AS category
      WHERE NULLIF(t.uri, '') IS NOT NULL
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
      updated_at = now()
    `,
  );

  await db.query(
    `
    DELETE FROM public.newsapi_article_categories ac
    USING (
      SELECT DISTINCT NULLIF(uri, '') AS article_uri
      FROM tmp_newsapi_ai_load
      WHERE NULLIF(uri, '') IS NOT NULL
    ) t
    WHERE ac.article_uri = t.article_uri
    `,
  );

  await db.query(
    `
    INSERT INTO public.newsapi_article_categories (
      article_uri,
      category_uri
    )
    SELECT DISTINCT
      NULLIF(t.uri, '') AS article_uri,
      NULLIF(category->>'uri', '') AS category_uri
    FROM tmp_newsapi_ai_load t
    CROSS JOIN LATERAL jsonb_array_elements(
      CASE
        WHEN NULLIF(t.categories, '') IS NOT NULL
          AND NULLIF(t.categories, '') <> 'null'
          AND jsonb_typeof(NULLIF(t.categories, '')::jsonb) = 'array'
        THEN NULLIF(t.categories, '')::jsonb
        ELSE '[]'::jsonb
      END
    ) AS category
    WHERE NULLIF(t.uri, '') IS NOT NULL
      AND category ? 'uri'
      AND NULLIF(category->>'uri', '') IS NOT NULL
    ON CONFLICT DO NOTHING
    `,
  );
}

export const handler = async (
  event: S3Event,
  context: { awsRequestId?: string } = {},
) => {
  const rec = event.Records?.[0];
  const requestId = context.awsRequestId ?? `manual-${Date.now()}`;
  if (!rec) throw new Error("No S3 records in event");

  const bucket = rec.s3.bucket.name;
  const manifestKey = parseS3RecordKey(rec.s3.object.key);

  if (!manifestKey.endsWith("manifest.json")) {
    console.log(`Ignoring non-manifest key: s3://${bucket}/${manifestKey}`);
    return;
  }

  console.log("══ ARTICLE LOADER START ══");

  const manifestText = await s3GetText(bucket, manifestKey);
  const manifest = JSON.parse(manifestText) as Manifest;

  if (manifest.artifact_contract_version !== ARTIFACT_CONTRACT_VERSION) {
    throw new Error(
      `Artifact contract mismatch. Expected ${ARTIFACT_CONTRACT_VERSION}, got ${manifest.artifact_contract_version}`,
    );
  }

  if (!manifest.collected_at) {
    throw new Error("Manifest missing collected_at — collector incomplete");
  }

  const runPrefix = manifestKey.replace(/manifest\.json$/, "");
  const csvKey = `${runPrefix}articles.csv`;
  const reportKey = `${runPrefix}load_report.json`;

  console.log(`── LOAD CSV ──\nrun=${manifest.run_id} nth=${manifest.nth_run} ${manifest.run_type}`);

  const csvPath = path.join("/tmp", "articles.csv");
  await s3DownloadToFile(bucket, csvKey, csvPath);

  const expectedHeader = csvHeader();
  const actualHeader = fs.readFileSync(csvPath, "utf8").split(/\r?\n/, 1)[0];

  if (actualHeader !== expectedHeader) {
    throw new Error(
      `Unexpected CSV header.\nExpected: ${expectedHeader}\nActual:   ${actualHeader}`,
    );
  }

  const useSSL = !DATABASE_URL.includes("localhost");
  const db = new Client({
    connectionString: DATABASE_URL,
    ssl: useSSL ? { rejectUnauthorized: false } : false,
    connectionTimeoutMillis: 5000,
  });

  await db.connect();
  const loadStartedAtIso = new Date().toISOString();
  const startedMs = Date.now();

  try {
    await db.query("BEGIN");

    const existingState = await getPipelineRunLoadState(
      db,
      manifest.run_id,
      manifest.ingestion_source,
      manifest.run_type,
      manifest.nth_run,
    );

    if (existingState.status === "loaded" && existingState.loadCompletedAt) {
      const skippedAtIso = new Date().toISOString();
      const skipReport = {
        artifact_contract_version: ARTIFACT_CONTRACT_VERSION,
        ingestion_source: manifest.ingestion_source,
        run_id: manifest.run_id,
        run_type: manifest.run_type,
        nth_run: manifest.nth_run,
        s3_bucket: bucket,
        s3_prefix: runPrefix,
        load_started_at: loadStartedAtIso,
        load_completed_at: skippedAtIso,
        duration_ms: Date.now() - startedMs,
        pipeline_status: "skipped_duplicate",
        skip_reason: "pipeline_run_already_loaded",
        existing_pipeline_run_state: existingState,
        request_id: requestId,
      };
      await db.query("ROLLBACK");
      console.log("transaction_rolled_back_already_loaded");
      const invocationReportKey = await s3PutInvocationJson(
        bucket,
        runPrefix,
        requestId,
        skipReport,
      );
      console.log("duplicate_invocation_report_written:", invocationReportKey);
      console.log("loader_skipped_already_loaded_run");
      return;
    }

    await setLoadStarted(
      db,
      manifest.run_id,
      manifest.ingestion_source,
      manifest.run_type,
      manifest.nth_run,
      loadStartedAtIso,
    );
    await copyCsvIntoTempTable(db, csvPath);

    const { rowsInArtifact, rowsAttempted } = await countArtifactRows(db);
    const diagnostics = await getArtifactDiagnostics(db);

    const { rowsLoaded, dbRowsInserted, dbRowsUpdated } =
      await bulkUpsertFromTemp(
        db,
        manifest.ingestion_source,
        manifest.run_id,
        manifest.run_type,
        manifest.nth_run,
        manifest.collected_at,
      );
    await syncNormalizedArticleDimensionsFromTemp(db);

    const dbRowsUnchanged = rowsAttempted - rowsLoaded;
    const loadCompletedAtIso = new Date().toISOString();

    console.log(`── UPSERT ──\nrows=${rowsInArtifact} sources=${diagnostics.populatedCounts?.source ?? "?"}\nloaded=${rowsLoaded} ins=${dbRowsInserted} upd=${dbRowsUpdated} unch=${dbRowsUnchanged}`);

    const pipelineRunMetrics = await upsertPipelineRunMetrics(
      db,
      manifest.run_id,
      manifest.ingestion_source,
      manifest.run_type,
      manifest.nth_run,
      loadStartedAtIso,
      loadCompletedAtIso,
    );
    console.log("── COMMIT + REPORT ──");
    await setLoadCompleted(
      db,
      manifest.run_id,
      manifest.ingestion_source,
      manifest.run_type,
      manifest.nth_run,
      loadCompletedAtIso,
      rowsLoaded,
      dbRowsInserted,
      dbRowsUpdated,
    );
    await db.query("COMMIT");

    const loadReport = {
      artifact_contract_version: ARTIFACT_CONTRACT_VERSION,
      ingestion_source: manifest.ingestion_source,
      run_id: manifest.run_id,
      run_type: manifest.run_type,
      nth_run: manifest.nth_run,
      s3_bucket: bucket,
      s3_prefix: runPrefix,
      csv_header: expectedHeader,
      load_started_at: loadStartedAtIso,
      rows_in_artifact: rowsInArtifact,
      rows_attempted: rowsAttempted,
      rows_loaded: rowsLoaded,
      db_rows_inserted: dbRowsInserted,
      db_rows_updated: dbRowsUpdated,
      db_rows_unchanged: dbRowsUnchanged,
      min_date_time_published: diagnostics.minDateTimePublished,
      max_date_time_published: diagnostics.maxDateTimePublished,
      sample_uris: diagnostics.sampleUris,
      populated_counts: diagnostics.populatedCounts,
      pipeline_run_metrics: pipelineRunMetrics,
      load_completed_at: loadCompletedAtIso,
      duration_ms: Date.now() - startedMs,
      pipeline_status: "loaded",
    };

    await s3PutJson(bucket, reportKey, loadReport);
    const invocationReportKey = await s3PutInvocationJson(
      bucket,
      runPrefix,
      requestId,
      {
        ...loadReport,
        request_id: requestId,
      },
    );
    console.log("══ ARTICLE LOADER COMPLETE ══");
  } catch (e: any) {
    console.error(`══ ARTICLE LOADER FAILED ══\n${e?.message ? String(e.message) : String(e)}`);

    try {
      await db.query("ROLLBACK");
      console.log("transaction_rolled_back");
    } catch {
      // ignore rollback failure
    }

    try {
      await markRunFailed(
        db,
        manifest.run_id,
        manifest.ingestion_source,
        manifest.run_type,
        manifest.nth_run,
        "loader_error",
        e?.message ? String(e.message) : String(e),
      );
      console.log("pipeline_run_marked_failed");
    } catch (inner) {
      console.error("also_failed_to_mark_pipeline_run_failed:", inner);
    }

    throw e;
  } finally {
    try {
      await db.query(`DROP TABLE IF EXISTS tmp_newsapi_ai_load`);
    } catch {
      // ignore cleanup failure
    }

    await db.end().catch(() => {});
  }
};
