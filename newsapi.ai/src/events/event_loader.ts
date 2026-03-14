import "dotenv/config";
import * as fs from "node:fs";
import * as path from "node:path";
import { Client } from "pg";
import { from as copyFrom } from "pg-copy-streams";
import { pipeline } from "node:stream/promises";
import {
  EVENT_ARTIFACT_CONTRACT_VERSION,
  EVENT_DATA_FILE,
  EVENT_MANIFEST_FILE,
  EVENT_LOAD_REPORT_FILE,
  EVENT_CSV_HEADERS,
} from "./shared/eventArtifactSchema";

const DATABASE_URL = process.env.DATABASE_URL!;
if (!DATABASE_URL) throw new Error("Missing DATABASE_URL");

type EventManifest = {
  artifact_contract_version: string;
  ingestion_source: string;
  parent_ingestion_source: string | null;
  run_id: string;
  run_type: string;
  nth_run: number;
  collected_at: string;
  parent_run_id: string | null;
  parent_run_type: string | null;
  parent_nth_run: number | null;
  discovery_scope: string;
  discovery_time_column: string | null;
  discovery_start: string | null;
  discovery_end: string | null;
  discovery_source_uris: string[];
  only_missing: boolean;
  include_stale: boolean;
  stale_after_hours: number | null;
  candidate_articles_scanned: number;
  distinct_event_uris_discovered: number;
  distinct_event_uris_selected: number;
  events_fetched: number;
  csv_file: string;
  window_from?: string;
  window_to?: string;
};

type ArtifactDiagnostics = {
  rowsInArtifact: number;
  minEventDate: string | null;
  maxEventDate: string | null;
  populatedCounts: {
    article_counts: number;
    title: number;
    summary: number;
    concepts: number;
    categories: number;
    common_dates: number;
    location: number;
    stories: number;
    images: number;
    raw_event: number;
  };
  sampleUris: string[];
};

type PipelineRunLoadState = {
  status: string | null;
  loadStartedAt: string | null;
  loadCompletedAt: string | null;
};

function truncateErrorMessage(msg: string, max = 2000): string {
  return msg.length <= max ? msg : msg.slice(0, max);
}

function csvHeader(): string {
  return EVENT_CSV_HEADERS.join(",");
}

function listManifestPaths(baseDir: string): string[] {
  const found: string[] = [];

  function walk(dir: string) {
    for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
      const full = path.join(dir, entry.name);
      if (entry.isDirectory()) {
        walk(full);
      } else if (entry.isFile() && entry.name === EVENT_MANIFEST_FILE) {
        found.push(full);
      }
    }
  }

  if (!fs.existsSync(baseDir)) {
    return found;
  }

  walk(baseDir);
  return found;
}

function pickLatestManifestPath(baseOutDir: string): string {
  const root = path.join(
    baseOutDir,
    "out",
    "ingestion_source=newsapi-ai-events",
  );
  const manifests = listManifestPaths(root);

  if (manifests.length === 0) {
    throw new Error(`No ${EVENT_MANIFEST_FILE} files found under ${root}`);
  }

  manifests.sort((a, b) => fs.statSync(b).mtimeMs - fs.statSync(a).mtimeMs);
  return manifests[0];
}

function safeReportTimestamp(iso: string): string {
  return iso.replace(/[:]/g, "-");
}

function writeLocalInvocationReport(
  runDir: string,
  report: unknown,
  whenIso: string,
) {
  const reportsDir = path.join(runDir, "load_reports");
  fs.mkdirSync(reportsDir, { recursive: true });
  const reportPath = path.join(
    reportsDir,
    `${safeReportTimestamp(whenIso)}.json`,
  );
  fs.writeFileSync(reportPath, JSON.stringify(report, null, 2), "utf8");
  return reportPath;
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

async function tryAcquireRunLoadLock(
  db: Client,
  runId: string,
  ingestionSource: string,
  runType: string,
  nthRun: number,
): Promise<boolean> {
  const lockNamespace = "newsapi-events_loader";
  const lockKey = `${runId}|${ingestionSource}|${runType}|${nthRun}`;

  const res = await db.query(
    `
    SELECT pg_try_advisory_xact_lock(hashtext($1), hashtext($2)) AS acquired
    `,
    [lockNamespace, lockKey],
  );

  return Boolean(res.rows[0]?.acquired);
}

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
    CREATE TEMP TABLE tmp_newsapi_events_load (
      uri                 text,
      run_type            text,
      nth_run             text,
      total_article_count text,
      relevance           text,
      event_date          text,
      sentiment           text,
      social_score        text,
      article_counts      text,
      title               text,
      summary             text,
      concepts            text,
      categories          text,
      common_dates        text,
      location            text,
      stories             text,
      images              text,
      wgt                 text,
      raw_event           text
    ) ON COMMIT DROP
  `);

  const copySql = `
    COPY tmp_newsapi_events_load (
      uri,
      run_type,
      nth_run,
      total_article_count,
      relevance,
      event_date,
      sentiment,
      social_score,
      article_counts,
      title,
      summary,
      concepts,
      categories,
      common_dates,
      location,
      stories,
      images,
      wgt,
      raw_event
    )
    FROM STDIN WITH (FORMAT csv, HEADER true)
  `;

  const stream = db.query(copyFrom(copySql));
  await pipeline(fs.createReadStream(csvPath), stream as any);
}

async function countArtifactRows(db: Client) {
  const totalRes = await db.query(`
    SELECT COUNT(*)::int AS n
    FROM tmp_newsapi_events_load
  `);

  const attemptedRes = await db.query(`
    SELECT COUNT(*)::int AS n
    FROM tmp_newsapi_events_load
    WHERE NULLIF(uri, '') IS NOT NULL
  `);

  return {
    rowsInArtifact: Number(totalRes.rows[0].n),
    rowsAttempted: Number(attemptedRes.rows[0].n),
  };
}

async function collectArtifactDiagnostics(
  db: Client,
): Promise<ArtifactDiagnostics> {
  const statsRes = await db.query(`
    SELECT
      COUNT(*)::int AS rows_in_artifact,
      MIN(NULLIF(event_date, '')::date) AS min_event_date,
      MAX(NULLIF(event_date, '')::date) AS max_event_date,

      COUNT(*) FILTER (WHERE NULLIF(article_counts, '') IS NOT NULL AND NULLIF(article_counts, '') <> 'null')::int AS article_counts_count,
      COUNT(*) FILTER (WHERE NULLIF(title, '') IS NOT NULL AND NULLIF(title, '') <> 'null')::int AS title_count,
      COUNT(*) FILTER (WHERE NULLIF(summary, '') IS NOT NULL AND NULLIF(summary, '') <> 'null')::int AS summary_count,
      COUNT(*) FILTER (WHERE NULLIF(concepts, '') IS NOT NULL AND NULLIF(concepts, '') <> 'null')::int AS concepts_count,
      COUNT(*) FILTER (WHERE NULLIF(categories, '') IS NOT NULL AND NULLIF(categories, '') <> 'null')::int AS categories_count,
      COUNT(*) FILTER (WHERE NULLIF(common_dates, '') IS NOT NULL AND NULLIF(common_dates, '') <> 'null')::int AS common_dates_count,
      COUNT(*) FILTER (WHERE NULLIF(location, '') IS NOT NULL AND NULLIF(location, '') <> 'null')::int AS location_count,
      COUNT(*) FILTER (WHERE NULLIF(stories, '') IS NOT NULL AND NULLIF(stories, '') <> 'null')::int AS stories_count,
      COUNT(*) FILTER (WHERE NULLIF(images, '') IS NOT NULL AND NULLIF(images, '') <> 'null')::int AS images_count,
      COUNT(*) FILTER (WHERE NULLIF(raw_event, '') IS NOT NULL AND NULLIF(raw_event, '') <> 'null')::int AS raw_event_count
    FROM tmp_newsapi_events_load
  `);

  const sampleUrisRes = await db.query(`
    SELECT uri
    FROM tmp_newsapi_events_load
    WHERE NULLIF(uri, '') IS NOT NULL
    ORDER BY uri
    LIMIT 3
  `);

  const row = statsRes.rows[0];

  return {
    rowsInArtifact: Number(row.rows_in_artifact),
    minEventDate: row.min_event_date
      ? new Date(row.min_event_date).toISOString().slice(0, 10)
      : null,
    maxEventDate: row.max_event_date
      ? new Date(row.max_event_date).toISOString().slice(0, 10)
      : null,
    populatedCounts: {
      article_counts: Number(row.article_counts_count),
      title: Number(row.title_count),
      summary: Number(row.summary_count),
      concepts: Number(row.concepts_count),
      categories: Number(row.categories_count),
      common_dates: Number(row.common_dates_count),
      location: Number(row.location_count),
      stories: Number(row.stories_count),
      images: Number(row.images_count),
      raw_event: Number(row.raw_event_count),
    },
    sampleUris: sampleUrisRes.rows.map((r) => String(r.uri)),
  };
}

async function upsertPipelineRunMetrics(
  db: Client,
  runId: string,
  ingestionSource: string,
  runType: string,
  nthRun: number,
  stage: "event_load",
  metrics: Array<{ metric_name: string; metric_value: number }>,
) {
  if (metrics.length === 0) return;

  const valuesSql = metrics
    .map(
      (_, idx) =>
        `($1::timestamptz, $2::text, $3::text, $4::integer, $5::text, $${idx * 2 + 6}::text, $${idx * 2 + 7}::bigint)`,
    )
    .join(", ");

  const params: Array<string | number> = [
    runId,
    ingestionSource,
    runType,
    nthRun,
    stage,
  ];
  for (const m of metrics) {
    params.push(m.metric_name, m.metric_value);
  }

  await db.query(
    `
    INSERT INTO public.pipeline_run_metrics (
      run_id, ingestion_source, run_type, nth_run, stage, metric_name, metric_value
    )
    VALUES ${valuesSql}
    ON CONFLICT (run_id, ingestion_source, run_type, nth_run, stage, metric_name)
    DO UPDATE SET metric_value = EXCLUDED.metric_value
    `,
    params,
  );
}

async function bulkUpsertFromTemp(db: Client) {
  const res = await db.query(
    `
    WITH upserted AS (
      INSERT INTO public.events (
        uri,
        total_article_count,
        relevance,
        event_date,
        sentiment,
        social_score,
        article_counts,
        title,
        summary,
        concepts,
        categories,
        common_dates,
        location,
        stories,
        images,
        wgt,
        raw_event,
        first_collected_at,
        last_collected_at
      )
      SELECT
        NULLIF(uri, '') AS uri,
        NULLIF(total_article_count, '')::integer AS total_article_count,
        NULLIF(relevance, '')::integer AS relevance,
        NULLIF(event_date, '')::date AS event_date,
        NULLIF(sentiment, '')::double precision AS sentiment,
        NULLIF(social_score, '')::double precision AS social_score,
        NULLIF(article_counts, '')::jsonb AS article_counts,
        NULLIF(title, '')::jsonb AS title,
        NULLIF(summary, '')::jsonb AS summary,
        NULLIF(concepts, '')::jsonb AS concepts,
        NULLIF(categories, '')::jsonb AS categories,
        NULLIF(common_dates, '')::jsonb AS common_dates,
        NULLIF(location, '')::jsonb AS location,
        NULLIF(stories, '')::jsonb AS stories,
        NULLIF(images, '')::jsonb AS images,
        NULLIF(wgt, '')::bigint AS wgt,
        NULLIF(raw_event, '')::jsonb AS raw_event,
        now() AS first_collected_at,
        now() AS last_collected_at
      FROM tmp_newsapi_events_load
      WHERE NULLIF(uri, '') IS NOT NULL
      ON CONFLICT (uri) DO UPDATE SET
        total_article_count = EXCLUDED.total_article_count,
        relevance = EXCLUDED.relevance,
        event_date = EXCLUDED.event_date,
        sentiment = EXCLUDED.sentiment,
        social_score = EXCLUDED.social_score,
        article_counts = EXCLUDED.article_counts,
        title = EXCLUDED.title,
        summary = EXCLUDED.summary,
        concepts = EXCLUDED.concepts,
        categories = EXCLUDED.categories,
        common_dates = EXCLUDED.common_dates,
        location = EXCLUDED.location,
        stories = EXCLUDED.stories,
        images = EXCLUDED.images,
        wgt = EXCLUDED.wgt,
        raw_event = EXCLUDED.raw_event,
        last_collected_at = now(),
        updated_at = now()
      WHERE
        public.events.total_article_count IS DISTINCT FROM EXCLUDED.total_article_count OR
        public.events.relevance IS DISTINCT FROM EXCLUDED.relevance OR
        public.events.event_date IS DISTINCT FROM EXCLUDED.event_date OR
        public.events.sentiment IS DISTINCT FROM EXCLUDED.sentiment OR
        public.events.social_score IS DISTINCT FROM EXCLUDED.social_score OR
        public.events.article_counts IS DISTINCT FROM EXCLUDED.article_counts OR
        public.events.title IS DISTINCT FROM EXCLUDED.title OR
        public.events.summary IS DISTINCT FROM EXCLUDED.summary OR
        public.events.concepts IS DISTINCT FROM EXCLUDED.concepts OR
        public.events.categories IS DISTINCT FROM EXCLUDED.categories OR
        public.events.common_dates IS DISTINCT FROM EXCLUDED.common_dates OR
        public.events.location IS DISTINCT FROM EXCLUDED.location OR
        public.events.stories IS DISTINCT FROM EXCLUDED.stories OR
        public.events.images IS DISTINCT FROM EXCLUDED.images OR
        public.events.wgt IS DISTINCT FROM EXCLUDED.wgt OR
        public.events.raw_event IS DISTINCT FROM EXCLUDED.raw_event
      RETURNING (xmax = 0) AS inserted
    )
    SELECT
      COUNT(*)::int AS rows_loaded,
      COUNT(*) FILTER (WHERE inserted)::int AS db_rows_inserted,
      COUNT(*) FILTER (WHERE NOT inserted)::int AS db_rows_updated
    FROM upserted
    `,
  );

  return {
    rowsLoaded: Number(res.rows[0].rows_loaded),
    dbRowsInserted: Number(res.rows[0].db_rows_inserted),
    dbRowsUpdated: Number(res.rows[0].db_rows_updated),
  };
}

async function main() {
  const baseOutDir = process.cwd();
  const manifestPath = pickLatestManifestPath(baseOutDir);
  const runDir = path.dirname(manifestPath);

  const csvPath = path.join(runDir, EVENT_DATA_FILE);
  const loadReportPath = path.join(runDir, EVENT_LOAD_REPORT_FILE);

  console.log("Loading event artifacts from:", runDir);

  const manifest = JSON.parse(
    fs.readFileSync(manifestPath, "utf8"),
  ) as EventManifest;

  if (manifest.artifact_contract_version !== EVENT_ARTIFACT_CONTRACT_VERSION) {
    throw new Error(
      `Artifact contract mismatch. Expected ${EVENT_ARTIFACT_CONTRACT_VERSION}, got ${manifest.artifact_contract_version}`,
    );
  }

  if (manifest.csv_file !== EVENT_DATA_FILE) {
    throw new Error(
      `Manifest csv_file mismatch. Expected ${EVENT_DATA_FILE}, got ${manifest.csv_file}`,
    );
  }

  if (!manifest.collected_at) {
    throw new Error(
      "Event manifest missing collected_at — collector incomplete",
    );
  }

  console.log(
    `run: ${manifest.run_id} | ${manifest.run_type} | nth_run=${manifest.nth_run} | collected_at=${manifest.collected_at}`,
  );
  console.log(
    `parent: run_id=${manifest.parent_run_id ?? "none"} | ${manifest.parent_run_type ?? ""} | nth_run=${manifest.parent_nth_run ?? ""}`,
  );
  console.log(`events in manifest: ${manifest.events_fetched}`);

  if (manifest.events_fetched === 0) {
    console.log("manifest reports 0 events fetched — nothing to load");
    console.log("loader_completed_with_zero_events");
    return;
  }

  const expectedHeader = csvHeader();
  const actualHeader = fs.readFileSync(csvPath, "utf8").split(/\r?\n/, 1)[0];

  if (actualHeader !== expectedHeader) {
    throw new Error(
      `Unexpected events CSV header.\nExpected: ${expectedHeader}\nActual:   ${actualHeader}`,
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
    await db.query("BEGIN");

    const lockAcquired = await tryAcquireRunLoadLock(
      db,
      manifest.run_id,
      manifest.ingestion_source,
      manifest.run_type,
      manifest.nth_run,
    );

    if (!lockAcquired) {
      const skippedAtIso = new Date().toISOString();
      const skipReport = {
        artifact_contract_version: manifest.artifact_contract_version,
        ingestion_source: manifest.ingestion_source,
        run_id: manifest.run_id,
        run_type: manifest.run_type,
        nth_run: manifest.nth_run,
        parent_run_id: manifest.parent_run_id,
        parent_run_type: manifest.parent_run_type,
        parent_nth_run: manifest.parent_nth_run,
        artifacts_dir: runDir,
        load_started_at: loadStartedAtIso,
        load_completed_at: skippedAtIso,
        duration_ms: Date.now() - startedMs,
        pipeline_status: "skipped_duplicate",
        skip_reason: "duplicate_loader_execution_in_progress",
      };
      await db.query("ROLLBACK");
      const skipReportPath = writeLocalInvocationReport(
        runDir,
        skipReport,
        skippedAtIso,
      );
      console.log(
        "Skipped duplicate event loader execution. Invocation report:",
        skipReportPath,
      );
      return;
    }

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
        artifact_contract_version: manifest.artifact_contract_version,
        ingestion_source: manifest.ingestion_source,
        run_id: manifest.run_id,
        run_type: manifest.run_type,
        nth_run: manifest.nth_run,
        parent_run_id: manifest.parent_run_id,
        parent_run_type: manifest.parent_run_type,
        parent_nth_run: manifest.parent_nth_run,
        artifacts_dir: runDir,
        load_started_at: loadStartedAtIso,
        load_completed_at: skippedAtIso,
        duration_ms: Date.now() - startedMs,
        pipeline_status: "skipped_duplicate",
        skip_reason: "pipeline_run_already_loaded",
        existing_pipeline_run_state: existingState,
      };
      await db.query("ROLLBACK");
      const skipReportPath = writeLocalInvocationReport(
        runDir,
        skipReport,
        skippedAtIso,
      );
      console.log(
        "Skipped already-loaded event run. Invocation report:",
        skipReportPath,
      );
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

    const diagnostics = await collectArtifactDiagnostics(db);
    const { rowsInArtifact, rowsAttempted } = await countArtifactRows(db);

    console.log(`rows in artifact: ${rowsInArtifact} total, ${rowsAttempted} with valid uri`);
    console.log(`event date range: ${diagnostics.minEventDate ?? "n/a"} → ${diagnostics.maxEventDate ?? "n/a"}`);

    const { rowsLoaded, dbRowsInserted, dbRowsUpdated } =
      await bulkUpsertFromTemp(db);

    const dbRowsUnchanged = rowsAttempted - rowsLoaded;
    const loadCompletedAtIso = new Date().toISOString();

    console.log(`upsert: inserted=${dbRowsInserted} updated=${dbRowsUpdated} unchanged=${dbRowsUnchanged}`);

    await upsertPipelineRunMetrics(
      db,
      manifest.run_id,
      manifest.ingestion_source,
      manifest.run_type,
      manifest.nth_run,
      "event_load",
      [
        { metric_name: "events_upserted", metric_value: rowsLoaded },
        { metric_name: "events_inserted", metric_value: dbRowsInserted },
        { metric_name: "events_updated", metric_value: dbRowsUpdated },
      ],
    );

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
      artifact_contract_version: manifest.artifact_contract_version,
      ingestion_source: manifest.ingestion_source,
      run_id: manifest.run_id,
      run_type: manifest.run_type,
      nth_run: manifest.nth_run,
      parent_run_id: manifest.parent_run_id,
      parent_run_type: manifest.parent_run_type,
      parent_nth_run: manifest.parent_nth_run,
      artifacts_dir: runDir,
      csv_header: expectedHeader,
      load_started_at: loadStartedAtIso,
      rows_in_artifact: rowsInArtifact,
      rows_attempted: rowsAttempted,
      rows_loaded: rowsLoaded,
      events_upserted: rowsLoaded,
      events_inserted: dbRowsInserted,
      events_updated: dbRowsUpdated,
      db_rows_unchanged: dbRowsUnchanged,
      min_event_date: diagnostics.minEventDate,
      max_event_date: diagnostics.maxEventDate,
      sample_uris: diagnostics.sampleUris,
      populated_counts: diagnostics.populatedCounts,
      pipeline_run_metrics: [
        {
          stage: "event_load",
          metric_name: "events_upserted",
          metric_value: rowsLoaded,
        },
        {
          stage: "event_load",
          metric_name: "events_inserted",
          metric_value: dbRowsInserted,
        },
        {
          stage: "event_load",
          metric_name: "events_updated",
          metric_value: dbRowsUpdated,
        },
      ],
      load_completed_at: loadCompletedAtIso,
      duration_ms: Date.now() - startedMs,
      pipeline_status: "loaded",
    };

    fs.writeFileSync(
      loadReportPath,
      JSON.stringify(loadReport, null, 2),
      "utf8",
    );
    const invocationReportPath = writeLocalInvocationReport(
      runDir,
      loadReport,
      loadCompletedAtIso,
    );
    console.log(
      `event load complete: ${rowsLoaded} upserted (${dbRowsInserted} inserted, ${dbRowsUpdated} updated, ${dbRowsUnchanged} unchanged) in ${Date.now() - startedMs}ms`,
    );
    console.log("load report:", loadReportPath);
  } catch (e: any) {
    console.error(e);

    try {
      await db.query("ROLLBACK");
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
        "event_loader_error",
        e?.message ? String(e.message) : String(e),
      );
    } catch (inner) {
      console.error("Also failed to mark pipeline_runs as failed:", inner);
    }

    throw e;
  } finally {
    try {
      await db.query(`DROP TABLE IF EXISTS tmp_newsapi_events_load`);
    } catch {
      // ignore cleanup failure
    }

    await db.end().catch(() => {});
  }
}

export async function handler() {
  await main();
}

if (require.main === module) {
  handler().catch((e) => {
    console.error(e);
    process.exit(1);
  });
}