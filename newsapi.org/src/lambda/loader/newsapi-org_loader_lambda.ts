import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { pipeline } from "node:stream/promises";
import { Client } from "pg";
import { from as copyFrom } from "pg-copy-streams";
import {
  S3Client,
  GetObjectCommand,
  PutObjectCommand,
} from "@aws-sdk/client-s3";
import type { S3Event } from "aws-lambda";
import { csvHeader } from "../../shared/newsapi-orgArtifactSchema";

const DATABASE_URL = process.env.DATABASE_URL!;
if (!DATABASE_URL) throw new Error("Missing DATABASE_URL");

const s3 = new S3Client({});

type Manifest = {
  ingestion_source: string;
  run_id: string;
  collected_at: string;
  window_from?: string;
  window_to?: string;
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

async function s3PutJson(bucket: string, key: string, obj: unknown): Promise<void> {
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
  whenIso: string,
) {
  await db.query(
    `
    UPDATE public.pipeline_runs
    SET
      load_started_at = COALESCE(load_started_at, $3),
      updated_at = now()
    WHERE run_id = $1 AND ingestion_source = $2
    `,
    [runId, ingestionSource, whenIso],
  );
}

async function setLoadCompleted(
  db: Client,
  runId: string,
  ingestionSource: string,
  whenIso: string,
  rowsLoaded: number,
  dbRowsInserted: number,
  dbRowsUpdated: number,
) {
  await db.query(
    `
    UPDATE public.pipeline_runs
    SET
      load_completed_at = $3,
      rows_loaded = $4,
      db_rows_inserted = $5,
      db_rows_updated = $6,
      status = 'loaded',
      error_code = NULL,
      error_message = NULL,
      updated_at = now()
    WHERE run_id = $1 AND ingestion_source = $2
    `,
    [
      runId,
      ingestionSource,
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
  code: string,
  message: string,
) {
  await db.query(
    `
    UPDATE public.pipeline_runs
    SET
      status = 'failed',
      error_code = $3,
      error_message = $4,
      updated_at = now()
    WHERE run_id = $1 AND ingestion_source = $2
    `,
    [runId, ingestionSource, code, truncateErrorMessage(message)],
  );
}

async function copyCsvIntoTempTable(db: Client, csvPath: string) {
  await db.query(`
    CREATE TEMP TABLE tmp_headlines_load (
      source_id    text,
      source_name  text,
      author       text,
      title        text,
      description  text,
      url          text,
      url_to_image text,
      published_at text,
      content      text
    )
  `);

  const copySql = `
    COPY tmp_headlines_load (
      source_id,
      source_name,
      author,
      title,
      description,
      url,
      url_to_image,
      published_at,
      content
    )
    FROM STDIN WITH (FORMAT csv, HEADER true)
  `;

  const stream = db.query(copyFrom(copySql));
  await pipeline(fs.createReadStream(csvPath), stream as any);
}

async function countArtifactRows(db: Client) {
  const totalRes = await db.query(`
    SELECT COUNT(*)::int AS n
    FROM tmp_headlines_load
  `);

  const attemptedRes = await db.query(`
    SELECT COUNT(*)::int AS n
    FROM tmp_headlines_load
    WHERE NULLIF(url, '') IS NOT NULL
  `);

  return {
    rowsInArtifact: Number(totalRes.rows[0].n),
    rowsAttempted: Number(attemptedRes.rows[0].n),
  };
}

async function bulkUpsertFromTemp(
  db: Client,
  ingestionSource: string,
  runId: string,
  collectedAt: string,
) {
  const res = await db.query(
    `
    WITH upserted AS (
      INSERT INTO public.headlines (
        ingestion_source,
        source_id,
        source_name,
        author,
        title,
        description,
        url,
        url_to_image,
        published_at,
        content,
        run_id,
        collected_at,
        ingested_at
      )
      SELECT
        $1 AS ingestion_source,
        NULLIF(source_id, '') AS source_id,
        NULLIF(source_name, '') AS source_name,
        NULLIF(author, '') AS author,
        NULLIF(title, '') AS title,
        NULLIF(description, '') AS description,
        NULLIF(url, '') AS url,
        NULLIF(url_to_image, '') AS url_to_image,
        NULLIF(published_at, '')::timestamptz AS published_at,
        NULLIF(content, '') AS content,
        $2::timestamptz AS run_id,
        $3::timestamptz AS collected_at,
        now() AS ingested_at
      FROM tmp_headlines_load
      WHERE NULLIF(url, '') IS NOT NULL
      ON CONFLICT (url) DO UPDATE SET
        ingestion_source = EXCLUDED.ingestion_source,
        source_id = EXCLUDED.source_id,
        source_name = EXCLUDED.source_name,
        author = EXCLUDED.author,
        title = EXCLUDED.title,
        description = EXCLUDED.description,
        url_to_image = EXCLUDED.url_to_image,
        published_at = EXCLUDED.published_at,
        content = EXCLUDED.content,
        run_id = EXCLUDED.run_id,
        collected_at = EXCLUDED.collected_at,
        updated_at = now()
      WHERE
        public.headlines.ingestion_source IS DISTINCT FROM EXCLUDED.ingestion_source OR
        public.headlines.source_id IS DISTINCT FROM EXCLUDED.source_id OR
        public.headlines.source_name IS DISTINCT FROM EXCLUDED.source_name OR
        public.headlines.author IS DISTINCT FROM EXCLUDED.author OR
        public.headlines.title IS DISTINCT FROM EXCLUDED.title OR
        public.headlines.description IS DISTINCT FROM EXCLUDED.description OR
        public.headlines.url_to_image IS DISTINCT FROM EXCLUDED.url_to_image OR
        public.headlines.published_at IS DISTINCT FROM EXCLUDED.published_at OR
        public.headlines.content IS DISTINCT FROM EXCLUDED.content OR
        public.headlines.run_id IS DISTINCT FROM EXCLUDED.run_id OR
        public.headlines.collected_at IS DISTINCT FROM EXCLUDED.collected_at
      RETURNING (xmax = 0) AS inserted
    )
    SELECT
      COUNT(*)::int AS rows_loaded,
      COUNT(*) FILTER (WHERE inserted)::int AS db_rows_inserted,
      COUNT(*) FILTER (WHERE NOT inserted)::int AS db_rows_updated
    FROM upserted
    `,
    [ingestionSource, runId, collectedAt],
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

  const manifestText = await s3GetText(bucket, manifestKey);
  const manifest = JSON.parse(manifestText) as Manifest;

  const runPrefix = manifestKey.replace(/manifest\.json$/, "");
  const csvKey = `${runPrefix}articles.csv`;
  const reportKey = `${runPrefix}load_report.json`;

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
  });
  await db.connect();

  const loadStartedAtIso = new Date().toISOString();
  const startedMs = Date.now();

  try {
    await setLoadStarted(
      db,
      manifest.run_id,
      manifest.ingestion_source,
      loadStartedAtIso,
    );

    await copyCsvIntoTempTable(db, csvPath);

    const { rowsInArtifact, rowsAttempted } = await countArtifactRows(db);

    const { rowsLoaded, dbRowsInserted, dbRowsUpdated } =
      await bulkUpsertFromTemp(
        db,
        manifest.ingestion_source,
        manifest.run_id,
        manifest.collected_at,
      );

    const dbRowsUnchanged = rowsAttempted - rowsLoaded;
    const loadCompletedAtIso = new Date().toISOString();

    await setLoadCompleted(
      db,
      manifest.run_id,
      manifest.ingestion_source,
      loadCompletedAtIso,
      rowsLoaded,
      dbRowsInserted,
      dbRowsUpdated,
    );

    const loadReport = {
      ingestion_source: manifest.ingestion_source,
      run_id: manifest.run_id,
      s3_bucket: bucket,
      s3_prefix: runPrefix,
      load_started_at: loadStartedAtIso,
      rows_in_artifact: rowsInArtifact,
      rows_attempted: rowsAttempted,
      rows_loaded: rowsLoaded,
      db_rows_inserted: dbRowsInserted,
      db_rows_updated: dbRowsUpdated,
      db_rows_unchanged: dbRowsUnchanged,
      load_completed_at: loadCompletedAtIso,
      duration_ms: Date.now() - startedMs,
      pipeline_status: "loaded",
    };

    await s3PutJson(bucket, reportKey, loadReport);
    console.log("Load complete:", reportKey);
  } catch (e: any) {
    console.error("Load failed:", e);

    try {
      await markRunFailed(
        db,
        manifest.run_id,
        manifest.ingestion_source,
        "loader_error",
        e?.message ? String(e.message) : String(e),
      );
    } catch (inner) {
      console.error("Also failed to mark pipeline_runs as failed:", inner);
    }

    throw e;
  } finally {
    try {
      await db.query(`DROP TABLE IF EXISTS tmp_headlines_load`);
    } catch {
      // ignore cleanup failure
    }

    await db.end().catch(() => {});
  }
};