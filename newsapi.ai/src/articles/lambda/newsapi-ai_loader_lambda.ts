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
  const resp = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
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
  const resp = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
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
    [runId, ingestionSource, runType, nthRun, code, truncateErrorMessage(message)],
  );
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

async function getArtifactDiagnostics(db: Client): Promise<ArtifactDiagnostics> {
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

export const handler = async (event: S3Event) => {
  const rec = event.Records?.[0];
  if (!rec) throw new Error("No S3 records in event");

  const bucket = rec.s3.bucket.name;
  const manifestKey = parseS3RecordKey(rec.s3.object.key);

  if (!manifestKey.endsWith("manifest.json")) {
    console.log(`Ignoring non-manifest key: s3://${bucket}/${manifestKey}`);
    return;
  }

  console.log("loader starting");
  console.log("bucket:", bucket);
  console.log("manifest_key:", manifestKey);

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

  console.log("run_id:", manifest.run_id);
  console.log("run_type:", manifest.run_type);
  console.log("nth_run:", manifest.nth_run);
  console.log("ingestion_source:", manifest.ingestion_source);
  console.log("csv_key:", csvKey);
  console.log("report_key:", reportKey);

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
  console.log("database_connected");

  const loadStartedAtIso = new Date().toISOString();
  const startedMs = Date.now();

  try {
    await db.query("BEGIN");
    console.log("transaction_started");

    await setLoadStarted(
      db,
      manifest.run_id,
      manifest.ingestion_source,
      manifest.run_type,
      manifest.nth_run,
      loadStartedAtIso,
    );
    console.log("pipeline_run_marked_load_started");

    await copyCsvIntoTempTable(db, csvPath);
    console.log("csv_copied_to_temp_table");

    const { rowsInArtifact, rowsAttempted } = await countArtifactRows(db);
    console.log("artifact_row_counts:", { rowsInArtifact, rowsAttempted });

    const diagnostics = await getArtifactDiagnostics(db);
    console.log("artifact_diagnostics:", diagnostics);

    const { rowsLoaded, dbRowsInserted, dbRowsUpdated } =
      await bulkUpsertFromTemp(
        db,
        manifest.ingestion_source,
        manifest.run_id,
        manifest.run_type,
        manifest.nth_run,
        manifest.collected_at,
      );

    const dbRowsUnchanged = rowsAttempted - rowsLoaded;
    const loadCompletedAtIso = new Date().toISOString();

    console.log("db_write_results:", {
      rowsLoaded,
      dbRowsInserted,
      dbRowsUpdated,
      dbRowsUnchanged,
    });

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
    console.log("pipeline_run_marked_loaded");

    await db.query("COMMIT");
    console.log("transaction_committed");

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
      load_completed_at: loadCompletedAtIso,
      duration_ms: Date.now() - startedMs,
      pipeline_status: "loaded",
    };

    await s3PutJson(bucket, reportKey, loadReport);
    console.log("load_report_written:", reportKey);
    console.log("loader_completed_successfully");
  } catch (e: any) {
    console.error("loader_failed:", e);

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
    console.log("database_connection_closed");
  }
};