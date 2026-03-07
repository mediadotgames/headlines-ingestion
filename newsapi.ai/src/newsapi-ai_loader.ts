import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { Client } from "pg";
import { from as copyFrom } from "pg-copy-streams";
import { pipeline } from "node:stream/promises";
import { csvHeader } from "./shared/newsapi-aiArtifactSchema";

const DATABASE_URL = process.env.DATABASE_URL!;
if (!DATABASE_URL) throw new Error("Missing DATABASE_URL");

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

function pickLatestRunDir(baseOutDir: string): string {
  const runsRoot = path.join(baseOutDir, "ingestion_source=newsapi-ai");
  const names = fs
    .readdirSync(runsRoot, { withFileTypes: true })
    .filter((d) => d.isDirectory() && d.name.startsWith("run_id="))
    .map((d) => d.name)
    .sort();

  if (names.length === 0) {
    throw new Error(`No run directories found under ${runsRoot}`);
  }

  return path.join(runsRoot, names[names.length - 1]);
}

async function copyCsvIntoTempTable(db: Client, csvPath: string) {
  await db.query(`
    CREATE TEMP TABLE tmp_newsapi_ai_load (
      uri                 text,
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
      wgt                 text
    )
  `);

  const copySql = `
    COPY tmp_newsapi_ai_load (
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
      wgt
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

async function bulkUpsertFromTemp(db: Client) {
  const res = await db.query(`
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
        wgt
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
        NULLIF(wgt, '')::bigint AS wgt
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
        public.newsapi_articles.wgt IS DISTINCT FROM EXCLUDED.wgt
      RETURNING (xmax = 0) AS inserted
    )
    SELECT
      COUNT(*)::int AS rows_loaded,
      COUNT(*) FILTER (WHERE inserted)::int AS db_rows_inserted,
      COUNT(*) FILTER (WHERE NOT inserted)::int AS db_rows_updated
    FROM upserted
  `);

  return {
    rowsLoaded: Number(res.rows[0].rows_loaded),
    dbRowsInserted: Number(res.rows[0].db_rows_inserted),
    dbRowsUpdated: Number(res.rows[0].db_rows_updated),
  };
}

async function main() {
  const baseOutDir = path.join(process.cwd(), "out");
  const runDir = pickLatestRunDir(baseOutDir);

  const manifestPath = path.join(runDir, "manifest.json");
  const csvPath = path.join(runDir, "articles.csv");
  const loadReportPath = path.join(runDir, "load_report.json");

  console.log("Loading artifacts from:", runDir);

  const manifest = JSON.parse(
    fs.readFileSync(manifestPath, "utf8"),
  ) as Manifest;

  if (!manifest.collected_at) {
    throw new Error("Manifest missing collected_at — collector incomplete");
  }

  console.log("manifest.run_id:", manifest.run_id);
  console.log("manifest.ingestion_source:", manifest.ingestion_source);

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
      await bulkUpsertFromTemp(db);

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
      artifacts_dir: runDir,
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

    fs.writeFileSync(
      loadReportPath,
      JSON.stringify(loadReport, null, 2),
      "utf8",
    );
    console.log("Load complete:", loadReportPath);
  } catch (e: any) {
    console.error(e);

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

    process.exitCode = 1;
  } finally {
    try {
      await db.query(`DROP TABLE IF EXISTS tmp_newsapi_ai_load`);
    } catch {
      // ignore cleanup failure
    }

    await db.end().catch(() => {});
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
