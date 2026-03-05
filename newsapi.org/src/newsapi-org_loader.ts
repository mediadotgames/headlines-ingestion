import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { Client } from "pg";
import { from as copyFrom } from "pg-copy-streams";

const DATABASE_URL = process.env.DATABASE_URL!;
if (!DATABASE_URL) throw new Error("Missing DATABASE_URL");

const INGESTION_SOURCE = "newsapi-org";

type LoadReport = {
  ingestion_source: string;
  run_id: string;
  artifacts_dir: string;

  load_started_at: string;
  load_completed_at?: string;
  duration_ms?: number;

  // counts
  rows_in_artifact: number; // actual parsed rows in tmp table
  rows_attempted: number;   // rows with non-empty url
  rows_loaded: number;      // inserted + updated (no no-ops)
  db_rows_inserted: number;
  db_rows_updated: number;

  // optional but very useful: conflicts where nothing changed
  db_rows_unchanged: number;

  pipeline_status?: "loaded" | "failed";
  error_code?: string;
  error_message?: string;
};

function pickLatestRunDir(baseDir: string): string {
  const runDirs = fs
    .readdirSync(baseDir, { withFileTypes: true })
    .filter((d) => d.isDirectory() && d.name.startsWith("run_id="))
    .map((d) => d.name)
    .sort();

  if (runDirs.length === 0) {
    throw new Error(`No run_id directories found in: ${baseDir}`);
  }

  return path.join(baseDir, runDirs[runDirs.length - 1]);
}

function writeLoadReport(runDir: string, report: LoadReport) {
  const reportPath = path.join(runDir, "load_report.json");
  fs.writeFileSync(reportPath, JSON.stringify(report, null, 2), "utf8");
}

async function setLoadStarted(db: Client, args: {
  runId: Date;
  ingestionSource: string;
  loadStartedAt: Date;
}) {
  const sql = `
    UPDATE public.pipeline_runs
    SET
      load_started_at = COALESCE(load_started_at, $3),
      updated_at = now()
    WHERE run_id = $1 AND ingestion_source = $2
  `;
  await db.query(sql, [args.runId, args.ingestionSource, args.loadStartedAt]);
}

async function setLoadCompleted(db: Client, args: {
  runId: Date;
  ingestionSource: string;
  loadCompletedAt: Date;
  rowsLoaded: number;
  dbRowsInserted: number;
  dbRowsUpdated: number;
}) {
  const sql = `
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
  `;
  await db.query(sql, [
    args.runId,
    args.ingestionSource,
    args.loadCompletedAt,
    args.rowsLoaded,
    args.dbRowsInserted,
    args.dbRowsUpdated,
  ]);
}

async function markRunFailed(db: Client, args: {
  runId: Date;
  ingestionSource: string;
  errorCode?: string;
  errorMessage: string;
}) {
  const sql = `
    UPDATE public.pipeline_runs
    SET
      status = 'failed',
      error_code = $3,
      error_message = $4,
      updated_at = now()
    WHERE run_id = $1 AND ingestion_source = $2
  `;
  await db.query(sql, [args.runId, args.ingestionSource, args.errorCode ?? null, args.errorMessage]);
}

async function copyCsvIntoTempTable(db: Client, csvPath: string) {
  // IMPORTANT: do NOT use ON COMMIT DROP unless you wrap everything in one explicit transaction.
  await db.query(`
    CREATE TEMP TABLE tmp_headlines_load (
      ingestion_source text,
      run_id text,
      collected_at text,
      window_from text,
      window_to text,
      source_id text,
      source_name text,
      author text,
      title text,
      description text,
      url text,
      url_to_image text,
      published_at text,
      content text
    )
  `);

  const copySql = `
    COPY tmp_headlines_load (
      ingestion_source,
      run_id,
      collected_at,
      window_from,
      window_to,
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

  await new Promise<void>((resolve, reject) => {
    const dbStream = db.query(copyFrom(copySql));
    const fileStream = fs.createReadStream(csvPath);

    dbStream.on("error", reject);
    fileStream.on("error", reject);
    dbStream.on("finish", () => resolve());

    fileStream.pipe(dbStream);
  });
}

async function getTempCounts(db: Client): Promise<{
  rowsInArtifact: number;
  rowsAttempted: number;
}> {
  const totalRes = await db.query<{ c: string }>(`SELECT COUNT(*)::text AS c FROM tmp_headlines_load`);
  const rowsInArtifact = Number(totalRes.rows[0]?.c ?? "0");

  const attemptedRes = await db.query<{ c: string }>(`
    SELECT COUNT(*)::text AS c
    FROM tmp_headlines_load
    WHERE NULLIF(url, '') IS NOT NULL
  `);
  const rowsAttempted = Number(attemptedRes.rows[0]?.c ?? "0");

  return { rowsInArtifact, rowsAttempted };
}

async function bulkUpsertFromTemp(db: Client): Promise<{
  rowsLoaded: number;
  inserted: number;
  updated: number;
}> {
  /**
   * We skip no-op updates:
   * - DO UPDATE runs ONLY when at least one meaningful column differs.
   *
   * That makes:
   * - "updated" == rows where something actually changed
   * - rowsLoaded == inserted + updated
   * - attempted - rowsLoaded == unchanged conflicts
   */
  const upsertAgg = await db.query<{
    rows_loaded: number;
    inserted: number;
    updated: number;
  }>(`
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
        collected_at
      )
      SELECT
        NULLIF(ingestion_source, '')                                 AS ingestion_source,
        NULLIF(source_id, '')                                        AS source_id,
        NULLIF(source_name, '')                                      AS source_name,
        NULLIF(author, '')                                           AS author,
        NULLIF(title, '')                                            AS title,
        NULLIF(description, '')                                      AS description,
        NULLIF(url, '')                                              AS url,
        NULLIF(url_to_image, '')                                     AS url_to_image,
        NULLIF(published_at, '')::timestamptz                        AS published_at,
        NULLIF(content, '')                                          AS content,
        NULLIF(run_id, '')::timestamptz                              AS run_id,
        NULLIF(collected_at, '')::timestamptz                        AS collected_at
      FROM tmp_headlines_load
      WHERE NULLIF(url, '') IS NOT NULL

      ON CONFLICT (url) DO UPDATE SET
        ingestion_source = EXCLUDED.ingestion_source,
        source_id    = EXCLUDED.source_id,
        source_name  = EXCLUDED.source_name,
        author       = EXCLUDED.author,
        title        = EXCLUDED.title,
        description  = EXCLUDED.description,
        url_to_image = EXCLUDED.url_to_image,
        published_at = EXCLUDED.published_at,
        content      = EXCLUDED.content,
        run_id       = EXCLUDED.run_id,
        collected_at = EXCLUDED.collected_at,
        updated_at   = now()

      WHERE
        public.headlines.ingestion_source IS DISTINCT FROM EXCLUDED.ingestion_source OR
        public.headlines.source_id        IS DISTINCT FROM EXCLUDED.source_id OR
        public.headlines.source_name      IS DISTINCT FROM EXCLUDED.source_name OR
        public.headlines.author           IS DISTINCT FROM EXCLUDED.author OR
        public.headlines.title            IS DISTINCT FROM EXCLUDED.title OR
        public.headlines.description      IS DISTINCT FROM EXCLUDED.description OR
        public.headlines.url_to_image     IS DISTINCT FROM EXCLUDED.url_to_image OR
        public.headlines.published_at     IS DISTINCT FROM EXCLUDED.published_at OR
        public.headlines.content          IS DISTINCT FROM EXCLUDED.content OR
        public.headlines.run_id           IS DISTINCT FROM EXCLUDED.run_id OR
        public.headlines.collected_at     IS DISTINCT FROM EXCLUDED.collected_at

      RETURNING (xmax = 0) AS inserted
    )
    SELECT
      COUNT(*)::int AS rows_loaded,
      COUNT(*) FILTER (WHERE inserted)::int AS inserted,
      COUNT(*) FILTER (WHERE NOT inserted)::int AS updated
    FROM upserted
  `);

  const agg = upsertAgg.rows[0] ?? { rows_loaded: 0, inserted: 0, updated: 0 };
  return { rowsLoaded: agg.rows_loaded, inserted: agg.inserted, updated: agg.updated };
}

async function main() {
  const outBase = path.join(process.cwd(), "out", `ingestion_source=${INGESTION_SOURCE}`);
  if (!fs.existsSync(outBase)) throw new Error(`Artifacts folder not found: ${outBase}`);

  const runDir = pickLatestRunDir(outBase);
  const manifestPath = path.join(runDir, "manifest.json");
  const csvPath = path.join(runDir, "articles.csv");

  if (!fs.existsSync(manifestPath)) throw new Error(`Missing manifest: ${manifestPath}`);
  if (!fs.existsSync(csvPath)) throw new Error(`Missing CSV: ${csvPath}`);

  const manifest = JSON.parse(fs.readFileSync(manifestPath, "utf8"));
  const manifestRunId: string = manifest.run_id;
  const runId = new Date(manifestRunId);

  console.log("Loading artifacts from:", runDir);
  console.log("manifest.run_id:", manifestRunId);
  console.log("manifest.ingestion_source:", manifest.ingestion_source);

  const loadStartedAt = new Date();
  const report: LoadReport = {
    ingestion_source: INGESTION_SOURCE,
    run_id: manifestRunId,
    artifacts_dir: runDir,
    load_started_at: loadStartedAt.toISOString(),

    rows_in_artifact: 0,
    rows_attempted: 0,
    rows_loaded: 0,
    db_rows_inserted: 0,
    db_rows_updated: 0,
    db_rows_unchanged: 0,
  };

  // Write early report (helps if we crash)
  writeLoadReport(runDir, report);

  const db = new Client({ connectionString: DATABASE_URL });
  await db.connect();

  try {
    await setLoadStarted(db, { runId, ingestionSource: INGESTION_SOURCE, loadStartedAt });

    // 1) COPY CSV into temp table
    await copyCsvIntoTempTable(db, csvPath);

    // 2) Accurate artifact counts from the temp table (handles embedded newlines in CSV)
    const { rowsInArtifact, rowsAttempted } = await getTempCounts(db);
    report.rows_in_artifact = rowsInArtifact;
    report.rows_attempted = rowsAttempted;

    // 3) Bulk upsert (skipping no-op updates)
    const { rowsLoaded, inserted, updated } = await bulkUpsertFromTemp(db);

    const loadCompletedAt = new Date();

    // Update pipeline_runs
    await setLoadCompleted(db, {
      runId,
      ingestionSource: INGESTION_SOURCE,
      loadCompletedAt,
      rowsLoaded,
      dbRowsInserted: inserted,
      dbRowsUpdated: updated,
    });

    // Update load report
    report.rows_loaded = rowsLoaded;
    report.db_rows_inserted = inserted;
    report.db_rows_updated = updated;
    report.db_rows_unchanged = Math.max(0, rowsAttempted - rowsLoaded);

    report.load_completed_at = loadCompletedAt.toISOString();
    report.duration_ms = loadCompletedAt.getTime() - loadStartedAt.getTime();
    report.pipeline_status = "loaded";

    // Final report write (this is the one you care about)
    writeLoadReport(runDir, report);

    console.log(
      `done. rows_in_artifact=${rowsInArtifact} attempted=${rowsAttempted} loaded=${rowsLoaded} inserted=${inserted} updated=${updated} unchanged=${report.db_rows_unchanged}`
    );
  } catch (e: any) {
    const msg = e?.message ? String(e.message) : String(e);
    console.error(e);

    try {
      await markRunFailed(db, {
        runId,
        ingestionSource: INGESTION_SOURCE,
        errorCode: "loader_error",
        errorMessage: msg.slice(0, 2000),
      });
    } catch (inner) {
      console.error("Also failed to mark pipeline_runs as failed:", inner);
    }

    const failedAt = new Date();
    report.load_completed_at = failedAt.toISOString();
    report.duration_ms = failedAt.getTime() - loadStartedAt.getTime();
    report.pipeline_status = "failed";
    report.error_code = "loader_error";
    report.error_message = msg.slice(0, 4000);

    writeLoadReport(runDir, report);
    process.exitCode = 1;
  } finally {
    try {
      await db.query(`DROP TABLE IF EXISTS tmp_headlines_load`);
    } catch {
      // ignore
    }
    await db.end().catch(() => {});
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});