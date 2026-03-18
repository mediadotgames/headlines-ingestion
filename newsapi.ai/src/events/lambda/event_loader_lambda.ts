import "dotenv/config";
import * as fs from "node:fs";
import * as path from "node:path";
import { Client } from "pg";
import { from as copyFrom } from "pg-copy-streams";
import { pipeline } from "node:stream/promises";
import {
  S3Client,
  GetObjectCommand,
  PutObjectCommand,
} from "@aws-sdk/client-s3";
import {
  EVENT_ARTIFACT_CONTRACT_VERSION,
  EVENT_DATA_FILE,
  EVENT_MANIFEST_FILE,
  EVENT_LOAD_REPORT_FILE,
  EVENT_CSV_HEADERS,
} from "../shared/eventArtifactSchema";

const DATABASE_URL = process.env.DATABASE_URL!;
if (!DATABASE_URL) throw new Error("Missing DATABASE_URL");

const s3 = new S3Client({});

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
  stageStartedAt: string,
  stageCompletedAt: string,
) {
  if (metrics.length === 0) return;

  const startIdx = metrics.length * 2 + 6;
  const endIdx = startIdx + 1;

  const valuesSql = metrics
    .map(
      (_, idx) =>
        `($1::timestamptz, $2::text, $3::text, $4::integer, $5::text, $${idx * 2 + 6}::text, $${idx * 2 + 7}::bigint, $${startIdx}::timestamptz, $${endIdx}::timestamptz)`,
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
  params.push(stageStartedAt, stageCompletedAt);

  await db.query(
    `
    INSERT INTO public.pipeline_run_metrics (
      run_id, ingestion_source, run_type, nth_run, stage, metric_name, metric_value, stage_started_at, stage_completed_at
    )
    VALUES ${valuesSql}
    ON CONFLICT (run_id, ingestion_source, run_type, nth_run, stage, metric_name)
    DO UPDATE SET
      metric_value = EXCLUDED.metric_value,
      stage_started_at = EXCLUDED.stage_started_at,
      stage_completed_at = EXCLUDED.stage_completed_at
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

export const handler = async (event: any, context: { awsRequestId?: string } = {}) => {
  console.log("event_loader_lambda triggered");
  console.log(JSON.stringify(event, null, 2));

  const requestId = context.awsRequestId ?? `manual-${Date.now()}`;

  // ── Resolve manifest from S3 trigger ────────────────────────────
  const record = event?.Records?.[0];
  if (!record) {
    throw new Error("No S3 record provided to event loader lambda");
  }

  const bucket = record.s3.bucket.name;
  const manifestKey = parseS3RecordKey(record.s3.object.key);

  if (!manifestKey.endsWith(EVENT_MANIFEST_FILE)) {
    console.log(`Ignoring non event-manifest key: s3://${bucket}/${manifestKey}`);
    return;
  }

  console.log("loader starting");
  console.log("bucket:", bucket);
  console.log("manifest_key:", manifestKey);

  // ── Read manifest from S3 ───────────────────────────────────────
  const manifestText = await s3GetText(bucket, manifestKey);
  const manifest = JSON.parse(manifestText) as EventManifest;

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

  const runPrefix = manifestKey.replace(new RegExp(`${EVENT_MANIFEST_FILE}$`), "");
  const csvKey = `${runPrefix}${EVENT_DATA_FILE}`;
  const reportKey = `${runPrefix}${EVENT_LOAD_REPORT_FILE}`;

  console.log("run_id:", manifest.run_id);
  console.log("run_type:", manifest.run_type);
  console.log("nth_run:", manifest.nth_run);
  console.log("ingestion_source:", manifest.ingestion_source);
  console.log("parent_run_id:", manifest.parent_run_id ?? "none");
  console.log("parent_run_type:", manifest.parent_run_type ?? "");
  console.log("parent_nth_run:", manifest.parent_nth_run ?? "");
  console.log("events_in_manifest:", manifest.events_fetched);
  console.log("csv_key:", csvKey);
  console.log("report_key:", reportKey);

  // ── Early exit if collector produced zero events ────────────────
  if (manifest.events_fetched === 0) {
    console.log("manifest reports 0 events fetched — nothing to load");
    const emptyReport = {
      pipeline_status: "skipped",
      skip_reason: "zero_events_fetched",
      run_id: manifest.run_id,
      ingestion_source: manifest.ingestion_source,
      run_type: manifest.run_type,
      nth_run: manifest.nth_run,
      parent_run_id: manifest.parent_run_id,
      parent_run_type: manifest.parent_run_type,
      parent_nth_run: manifest.parent_nth_run,
      s3_bucket: bucket,
      s3_prefix: runPrefix,
      events_in_manifest: 0,
      rows_loaded: 0,
      db_rows_inserted: 0,
      db_rows_updated: 0,
    };
    await s3PutJson(bucket, reportKey, emptyReport);
    console.log("empty_load_report_written:", reportKey);
    console.log("loader_completed_with_zero_events");
    return;
  }

  // ── Download CSV from S3 to /tmp ────────────────────────────────
  const csvPath = path.join("/tmp", EVENT_DATA_FILE);
  await s3DownloadToFile(bucket, csvKey, csvPath);

  const expectedHeader = csvHeader();
  const actualHeader = fs.readFileSync(csvPath, "utf8").split(/\r?\n/, 1)[0];

  if (actualHeader !== expectedHeader) {
    throw new Error(
      `Unexpected events CSV header.\nExpected: ${expectedHeader}\nActual:   ${actualHeader}`,
    );
  }

  // ── Database connection ─────────────────────────────────────────
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

    // ── Duplicate detection ───────────────────────────────────────
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
      const invocationReportKey = await s3PutInvocationJson(bucket, runPrefix, requestId, skipReport);
      console.log("duplicate_invocation_report_written:", invocationReportKey);
      console.log("loader_skipped_already_loaded_run");
      return;
    }

    // ── Load pipeline ─────────────────────────────────────────────
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

    const diagnostics = await collectArtifactDiagnostics(db);
    const { rowsInArtifact, rowsAttempted } = await countArtifactRows(db);

    console.log("artifact_row_counts:", { rowsInArtifact, rowsAttempted });
    console.log(`event date range: ${diagnostics.minEventDate ?? "n/a"} → ${diagnostics.maxEventDate ?? "n/a"}`);

    const { rowsLoaded, dbRowsInserted, dbRowsUpdated } =
      await bulkUpsertFromTemp(db);

    const dbRowsUnchanged = rowsAttempted - rowsLoaded;
    const loadCompletedAtIso = new Date().toISOString();

    console.log("db_write_results:", {
      rowsLoaded,
      dbRowsInserted,
      dbRowsUpdated,
      dbRowsUnchanged,
    });

    // ── Pipeline run metrics ──────────────────────────────────────
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
      loadStartedAtIso,
      loadCompletedAtIso,
    );
    console.log("pipeline_run_metrics_written");

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

    // ── Write load report to S3 ───────────────────────────────────
    const loadReport = {
      artifact_contract_version: manifest.artifact_contract_version,
      ingestion_source: manifest.ingestion_source,
      run_id: manifest.run_id,
      run_type: manifest.run_type,
      nth_run: manifest.nth_run,
      parent_run_id: manifest.parent_run_id,
      parent_run_type: manifest.parent_run_type,
      parent_nth_run: manifest.parent_nth_run,
      s3_bucket: bucket,
      s3_prefix: runPrefix,
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

    await s3PutJson(bucket, reportKey, loadReport);
    console.log("load_report_written:", reportKey);

    const invocationReportKey = await s3PutInvocationJson(bucket, runPrefix, requestId, {
      ...loadReport,
      request_id: requestId,
    });
    console.log("invocation_report_written:", invocationReportKey);
    console.log("event_loader_completed_successfully");
  } catch (e: any) {
    console.error("event_loader_failed:", e);

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
        "event_loader_error",
        e?.message ? String(e.message) : String(e),
      );
      console.log("pipeline_run_marked_failed");
    } catch (inner) {
      console.error("also_failed_to_mark_pipeline_run_failed:", inner);
    }

    throw e;
  } finally {
    try {
      await db.query(`DROP TABLE IF EXISTS tmp_newsapi_events_load`);
    } catch {
      // ignore cleanup failure
    }

    await db.end().catch(() => {});
    console.log("database_connection_closed");
  }
};
