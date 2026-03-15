import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { Client } from "pg";
import {
  EVENT_ARTIFACT_CONTRACT_VERSION,
  EVENT_INGESTION_SOURCE,
  EVENT_DATA_FILE,
  EVENT_MANIFEST_FILE,
  csvHeader,
  toCsvRow,
} from "./shared/eventArtifactSchema";
import {
  sleep,
  isRetryableStatus,
  isRetryableError,
  backoffDelayMs,
} from "../shared/retry";

const EVENTREGISTRY_API_KEY = process.env.EVENTREGISTRY_API_KEY!;
const DATABASE_URL = process.env.DATABASE_URL!;

// Collector execution mode, not the parent article run_type.
const RUN_TYPE = (process.env.RUN_TYPE ?? "scheduled").trim().toLowerCase();

const PARENT_RUN_ID = (process.env.PARENT_RUN_ID ?? "").trim();
const PARENT_INGESTION_SOURCE = (process.env.PARENT_INGESTION_SOURCE ?? "newsapi-ai").trim();
const PARENT_RUN_TYPE = (process.env.PARENT_RUN_TYPE ?? "").trim().toLowerCase();
const PARENT_NTH_RUN = (process.env.PARENT_NTH_RUN ?? "").trim();

const EVENT_DISCOVERY_SCOPE = (process.env.EVENT_DISCOVERY_SCOPE ?? "parent_run").trim().toLowerCase();
const EVENT_DISCOVERY_TIME_COLUMN = (process.env.EVENT_DISCOVERY_TIME_COLUMN ?? "ingested_at").trim();
const EVENT_DISCOVERY_START = (process.env.EVENT_DISCOVERY_START ?? "").trim();
const EVENT_DISCOVERY_END = (process.env.EVENT_DISCOVERY_END ?? "").trim();
const EVENT_DISCOVERY_SOURCE_URIS = (process.env.EVENT_DISCOVERY_SOURCE_URIS ?? "").trim();
const EVENT_DISCOVERY_ONLY_MISSING =
  (process.env.EVENT_DISCOVERY_ONLY_MISSING ?? "false").trim().toLowerCase() === "true";
const EVENT_DISCOVERY_INCLUDE_STALE =
  (process.env.EVENT_DISCOVERY_INCLUDE_STALE ?? "true").trim().toLowerCase() === "true";
const EVENT_STALE_AFTER_HOURS = Number(process.env.EVENT_STALE_AFTER_HOURS ?? "24");
const EVENT_DISCOVERY_MAX_ARTICLES = Number(process.env.EVENT_DISCOVERY_MAX_ARTICLES ?? "50000");
const EVENT_DISCOVERY_MAX_EVENT_URIS = Number(process.env.EVENT_DISCOVERY_MAX_EVENT_URIS ?? "10000");
const FETCH_MAX_ATTEMPTS = Number(process.env.FETCH_MAX_ATTEMPTS ?? "5");
const EVENT_FETCH_BATCH_SIZE = Number(process.env.EVENT_FETCH_BATCH_SIZE ?? "50");

if (!EVENTREGISTRY_API_KEY) throw new Error("Missing EVENTREGISTRY_API_KEY");
if (!DATABASE_URL) throw new Error("Missing DATABASE_URL");
if (!["scheduled", "backfill", "seed"].includes(RUN_TYPE)) {
  throw new Error(`Invalid RUN_TYPE: ${RUN_TYPE}`);
}
if (!["parent_run", "time_window", "full_corpus", "custom_filter"].includes(EVENT_DISCOVERY_SCOPE)) {
  throw new Error(`Invalid EVENT_DISCOVERY_SCOPE: ${EVENT_DISCOVERY_SCOPE}`);
}
if (!["ingested_at", "date_time_published", "created_at"].includes(EVENT_DISCOVERY_TIME_COLUMN)) {
  throw new Error(`Invalid EVENT_DISCOVERY_TIME_COLUMN: ${EVENT_DISCOVERY_TIME_COLUMN}`);
}
if (EVENT_DISCOVERY_SCOPE === "time_window" && (!EVENT_DISCOVERY_START || !EVENT_DISCOVERY_END)) {
  throw new Error("time_window scope requires EVENT_DISCOVERY_START and EVENT_DISCOVERY_END");
}

type EventRegistryEvent = {
  uri?: string | null;
  totalArticleCount?: number | null;
  articleCounts?: Record<string, number> | null;
  relevance?: number | null;
  eventDate?: string | null;
  sentiment?: number | null;
  socialScore?: number | null;
  images?: unknown[] | null;
  title?: Record<string, string> | null;
  summary?: Record<string, string> | null;
  concepts?: unknown[] | null;
  categories?: unknown[] | null;
  commonDates?: unknown[] | null;
  location?: Record<string, unknown> | null;
  stories?: unknown[] | null;
  wgt?: number | null;
  [key: string]: unknown;
};

type EventRegistryPayloadValue = {
  info?: EventRegistryEvent | null;
  stories?: unknown[] | null;
  eventDate?: string | null;
  totalArticleCount?: number | null;
  title?: Record<string, string> | null;
  summary?: Record<string, string> | null;
  images?: unknown[] | null;
  location?: Record<string, unknown> | null;
  commonDates?: unknown[] | null;
  articleCounts?: Record<string, number> | null;
  categories?: unknown[] | null;
  concepts?: unknown[] | null;
  relevance?: number | null;
  sentiment?: number | null;
  socialScore?: number | null;
  wgt?: number | null;
  [key: string]: unknown;
};

type EventRegistryResponse = {
  event?: EventRegistryEvent | null;
  events?: {
    results?: EventRegistryEvent[];
  } | EventRegistryEvent[];
  results?: EventRegistryEvent[];
  error?: unknown;
  [key: string]: unknown;
};

type ArticleSelectionRow = {
  event_uri: string;
};

type ParentRunRef = {
  run_id: string;
  run_type: string;
  nth_run: number;
  ingestion_source: string;
  load_report_path?: string | null;
};

function jsonString(value: unknown): string {
  return JSON.stringify(value ?? null);
}

function safePathComponent(value: string): string {
  return value.replace(/[:]/g, "-");
}

function listArticleLoadReports(baseDir: string): string[] {
  const found: string[] = [];

  function walk(dir: string) {
    for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
      const full = path.join(dir, entry.name);
      if (entry.isDirectory()) {
        walk(full);
      } else if (entry.isFile() && entry.name === "load_report.json") {
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

function pickLatestArticleLoadReport(baseOutDir: string): string {
  const root = path.join(baseOutDir, "out", "ingestion_source=newsapi-ai");
  const reports = listArticleLoadReports(root);

  if (reports.length === 0) {
    throw new Error(`No article load_report.json files found under ${root}`);
  }

  reports.sort((a, b) => fs.statSync(b).mtimeMs - fs.statSync(a).mtimeMs);
  return reports[0];
}

function resolveParentRunRef(baseOutDir: string): ParentRunRef {
  const hasFullEnvOverride =
    PARENT_RUN_ID !== "" &&
    PARENT_RUN_TYPE !== "" &&
    PARENT_NTH_RUN !== "" &&
    PARENT_INGESTION_SOURCE !== "";

  if (hasFullEnvOverride) {
    const nth = Number(PARENT_NTH_RUN);
    if (!Number.isInteger(nth) || nth < 1) {
      throw new Error(`Invalid PARENT_NTH_RUN: ${PARENT_NTH_RUN}`);
    }

    return {
      run_id: PARENT_RUN_ID,
      run_type: PARENT_RUN_TYPE,
      nth_run: nth,
      ingestion_source: PARENT_INGESTION_SOURCE,
      load_report_path: null,
    };
  }

  const reportPath = pickLatestArticleLoadReport(baseOutDir);
  const report = JSON.parse(fs.readFileSync(reportPath, "utf8"));

  if (
    !report.run_id ||
    !report.run_type ||
    !Number.isInteger(report.nth_run) ||
    !report.ingestion_source
  ) {
    throw new Error(`Invalid article load report at ${reportPath}`);
  }

  if (report.pipeline_status !== "loaded") {
    throw new Error(`Latest article load report is not loaded: ${reportPath}`);
  }

  return {
    run_id: String(report.run_id),
    run_type: String(report.run_type),
    nth_run: Number(report.nth_run),
    ingestion_source: String(report.ingestion_source),
    load_report_path: reportPath,
  };
}

async function postEventRegistry(
  body: Record<string, unknown>,
  contextLabel: string,
): Promise<EventRegistryResponse> {
  const url = "https://eventregistry.org/api/v1/event/getEvent";

  for (let attempt = 1; attempt <= FETCH_MAX_ATTEMPTS; attempt += 1) {
    try {
      const res = await fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });

      const json = (await res.json().catch(() => ({}))) as EventRegistryResponse;

      if (!res.ok) {
        const message = `Event Registry HTTP ${res.status}: ${JSON.stringify(json)}`;
        if (isRetryableStatus(res.status) && attempt < FETCH_MAX_ATTEMPTS) {
          const delayMs = backoffDelayMs(attempt);
          console.warn(
            `${contextLabel}: retryable API error on attempt ${attempt}/${FETCH_MAX_ATTEMPTS}; sleeping ${delayMs}ms`,
          );
          await sleep(delayMs);
          continue;
        }
        throw new Error(message);
      }

      if (json.error) {
        throw new Error(`Event Registry error: ${JSON.stringify(json.error)}`);
      }

      return json;
    } catch (err) {
      if (attempt < FETCH_MAX_ATTEMPTS && isRetryableError(err)) {
        const delayMs = backoffDelayMs(attempt);
        console.warn(
          `${contextLabel}: retryable fetch failure on attempt ${attempt}/${FETCH_MAX_ATTEMPTS}; sleeping ${delayMs}ms`,
        );
        await sleep(delayMs);
        continue;
      }
      throw err;
    }
  }

  throw new Error(`${contextLabel}: exhausted retries`);
}

function normalizeKeyedEvent(uriKey: string, value: EventRegistryPayloadValue): EventRegistryEvent | null {
  if (!value || typeof value !== "object") return null;

  const info = value.info && typeof value.info === "object" ? value.info : {};

  const merged: EventRegistryEvent = {
    ...(info as EventRegistryEvent),
    uri:
      typeof (info as EventRegistryEvent).uri === "string" && (info as EventRegistryEvent).uri
        ? (info as EventRegistryEvent).uri
        : uriKey,
    concepts: value.concepts ?? (info as EventRegistryEvent).concepts ?? null,
    categories: value.categories ?? (info as EventRegistryEvent).categories ?? null,
    eventDate: value.eventDate ?? (info as EventRegistryEvent).eventDate ?? null,
    totalArticleCount:
      value.totalArticleCount ?? (info as EventRegistryEvent).totalArticleCount ?? null,
    title: value.title ?? (info as EventRegistryEvent).title ?? null,
    summary: value.summary ?? (info as EventRegistryEvent).summary ?? null,
    images: value.images ?? (info as EventRegistryEvent).images ?? null,
    location: value.location ?? (info as EventRegistryEvent).location ?? null,
    commonDates: value.commonDates ?? (info as EventRegistryEvent).commonDates ?? null,
    articleCounts: value.articleCounts ?? (info as EventRegistryEvent).articleCounts ?? null,
    stories: value.stories ?? (info as EventRegistryEvent).stories ?? null,
    relevance: value.relevance ?? (info as EventRegistryEvent).relevance ?? null,
    sentiment: value.sentiment ?? (info as EventRegistryEvent).sentiment ?? null,
    socialScore: value.socialScore ?? (info as EventRegistryEvent).socialScore ?? null,
    wgt: value.wgt ?? (info as EventRegistryEvent).wgt ?? null,
  };

  return merged;
}

function extractEvents(payload: EventRegistryResponse): EventRegistryEvent[] {
  const keys = payload && typeof payload === "object" ? Object.keys(payload) : [];

  if (payload.event) return [payload.event];
  if (Array.isArray(payload.events)) return payload.events;
  if (Array.isArray(payload.events?.results)) return payload.events.results;
  if (Array.isArray(payload.results)) return payload.results;

  const keyedEvents: EventRegistryEvent[] = [];
  for (const [key, value] of Object.entries(payload ?? {})) {
    if (!key.includes("-")) continue;
    if (!value || typeof value !== "object") continue;

    const normalized = normalizeKeyedEvent(key, value as EventRegistryPayloadValue);
    if (normalized) keyedEvents.push(normalized);
  }

  if (keyedEvents.length > 0) {
    return keyedEvents;
  }

  return [];
}

async function upsertPipelineRunMetrics(
  db: Client,
  runId: string,
  ingestionSource: string,
  runType: string,
  nthRun: number,
  stage: "event_collect",
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

  const params: Array<string | number> = [runId, ingestionSource, runType, nthRun, stage];
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

function buildEventDiscoveryQuery(): string {
  if (EVENT_DISCOVERY_SCOPE === "parent_run") {
    return `
      SELECT DISTINCT a.event_uri
      FROM public.newsapi_articles a
      WHERE a.run_id = $1
        AND a.ingestion_source = $2
        AND a.run_type = $3
        AND a.nth_run = $4
        AND a.event_uri IS NOT NULL
        AND a.event_uri <> ''
      LIMIT $5
    `;
  }

  if (EVENT_DISCOVERY_SCOPE === "time_window") {
    return `
      SELECT DISTINCT a.event_uri
      FROM public.newsapi_articles a
      WHERE a.${EVENT_DISCOVERY_TIME_COLUMN} >= $1
        AND a.${EVENT_DISCOVERY_TIME_COLUMN} < $2
        AND a.event_uri IS NOT NULL
        AND a.event_uri <> ''
      LIMIT $3
    `;
  }

  return `
    SELECT DISTINCT a.event_uri
    FROM public.newsapi_articles a
    WHERE a.event_uri IS NOT NULL
      AND a.event_uri <> ''
    LIMIT $1
  `;
}

async function main() {
  const stageStartedAtIso = new Date().toISOString();
  const baseOutDir = process.cwd();
  const parentRun =
    EVENT_DISCOVERY_SCOPE === "parent_run"
      ? resolveParentRunRef(baseOutDir)
      : null;

  const useSSL = !DATABASE_URL.includes("localhost");
  const db = new Client({
    connectionString: DATABASE_URL,
    ssl: useSSL ? { rejectUnauthorized: false } : false,
  });
  await db.connect();

  try {
    const discoverySql = buildEventDiscoveryQuery();
    const discoveryParams =
      EVENT_DISCOVERY_SCOPE === "parent_run"
        ? [
            parentRun!.run_id,
            parentRun!.ingestion_source,
            parentRun!.run_type,
            parentRun!.nth_run,
            EVENT_DISCOVERY_MAX_EVENT_URIS,
          ]
        : EVENT_DISCOVERY_SCOPE === "time_window"
          ? [EVENT_DISCOVERY_START, EVENT_DISCOVERY_END, EVENT_DISCOVERY_MAX_EVENT_URIS]
          : [EVENT_DISCOVERY_MAX_EVENT_URIS];

    const candidateCountRes = await db.query(
      EVENT_DISCOVERY_SCOPE === "parent_run"
        ? `
          SELECT COUNT(*)::bigint AS candidate_articles_scanned
          FROM public.newsapi_articles a
          WHERE a.run_id = $1
            AND a.ingestion_source = $2
            AND a.run_type = $3
            AND a.nth_run = $4
        `
        : EVENT_DISCOVERY_SCOPE === "time_window"
          ? `
          SELECT COUNT(*)::bigint AS candidate_articles_scanned
          FROM public.newsapi_articles a
          WHERE a.${EVENT_DISCOVERY_TIME_COLUMN} >= $1
            AND a.${EVENT_DISCOVERY_TIME_COLUMN} < $2
        `
          : `
          SELECT LEAST(COUNT(*), $1)::bigint AS candidate_articles_scanned
          FROM public.newsapi_articles
        `,
      EVENT_DISCOVERY_SCOPE === "parent_run"
        ? [parentRun!.run_id, parentRun!.ingestion_source, parentRun!.run_type, parentRun!.nth_run]
        : EVENT_DISCOVERY_SCOPE === "time_window"
          ? [EVENT_DISCOVERY_START, EVENT_DISCOVERY_END]
          : [EVENT_DISCOVERY_MAX_ARTICLES],
    );

    const selectedUrisRes = await db.query<ArticleSelectionRow>(discoverySql, discoveryParams);
    const allDiscoveredUris = selectedUrisRes.rows.map((r) => r.event_uri).filter(Boolean);

    console.log("candidate_articles_scanned:", Number(candidateCountRes.rows[0].candidate_articles_scanned ?? 0));
    console.log("distinct_event_uris_discovered:", allDiscoveredUris.length);

    const freshnessFilteredRes = await db.query<{ event_uri: string }>(
      `
      SELECT a.event_uri
      FROM unnest($1::text[]) AS a(event_uri)
      LEFT JOIN public.events e
        ON e.uri = a.event_uri
      WHERE (
        e.uri IS NULL
        OR
        ($3::boolean = true AND e.last_collected_at < NOW() - ($4::text || ' hours')::interval)
        OR
        ($2::boolean = false AND $3::boolean = false)
      )
      `,
      [
        allDiscoveredUris,
        EVENT_DISCOVERY_ONLY_MISSING,
        EVENT_DISCOVERY_INCLUDE_STALE,
        String(EVENT_STALE_AFTER_HOURS),
      ],
    );

    const selectedEventUris = freshnessFilteredRes.rows.map((r) => r.event_uri);

    console.log("distinct_event_uris_selected:", selectedEventUris.length);

    const byUri = new Map<string, EventRegistryEvent>();

    const totalBatches = Math.ceil(selectedEventUris.length / EVENT_FETCH_BATCH_SIZE);
    console.log(
      `fetching ${selectedEventUris.length} events in ${totalBatches} batch(es) of up to ${EVENT_FETCH_BATCH_SIZE}`,
    );

    for (let batchIdx = 0; batchIdx < totalBatches; batchIdx += 1) {
      const batchStart = batchIdx * EVENT_FETCH_BATCH_SIZE;
      const batchUris = selectedEventUris.slice(batchStart, batchStart + EVENT_FETCH_BATCH_SIZE);
      const contextLabel = `batch ${batchIdx + 1}/${totalBatches}`;

      const payload = await postEventRegistry(
        {
          apiKey: EVENTREGISTRY_API_KEY,
          eventUri: batchUris,
          includeEventConcepts: true,
          includeEventCategories: true,
          includeEventLocation: true,
          includeEventImages: true,
          includeEventStories: true,
        },
        contextLabel,
      );

      const extracted = extractEvents(payload);
      let batchMissingUri = 0;

      for (const event of extracted) {
        const uri = event.uri == null ? "" : String(event.uri).trim();
        if (!uri) {
          batchMissingUri += 1;
          continue;
        }
        byUri.set(uri, event);
      }

      console.log(
        `${contextLabel}: requested=${batchUris.length} extracted=${extracted.length} missing_uri=${batchMissingUri} running_total=${byUri.size}`,
      );

      if (extracted.length === 0) {
        console.warn(
          `${contextLabel}: no events extracted; payload preview=`,
          JSON.stringify(payload).slice(0, 500),
        );
      }
    }

    const deduped = Array.from(byUri.values());
    const collectedAt = new Date().toISOString();

    console.log("final fetched/deduped events:", deduped.length);

    if (parentRun) {
      const stageCompletedAtIso = new Date().toISOString();
      await upsertPipelineRunMetrics(
        db,
        parentRun.run_id,
        parentRun.ingestion_source,
        parentRun.run_type,
        parentRun.nth_run,
        "event_collect",
        [
          {
            metric_name: "candidate_articles_scanned",
            metric_value: Number(candidateCountRes.rows[0].candidate_articles_scanned ?? 0),
          },
          {
            metric_name: "distinct_event_uris_discovered",
            metric_value: allDiscoveredUris.length,
          },
          {
            metric_name: "distinct_event_uris_selected",
            metric_value: selectedEventUris.length,
          },
          {
            metric_name: "events_fetched",
            metric_value: deduped.length,
          },
        ],
        stageStartedAtIso,
        stageCompletedAtIso,
      );
    }

    const artifactRunId = parentRun ? parentRun.run_id : collectedAt;
    const artifactRunType = parentRun ? parentRun.run_type : RUN_TYPE;
    const artifactNthRun = parentRun ? parentRun.nth_run : 1;
    const artifactCollectedAt = safePathComponent(collectedAt);

    const outDir = path.join(
      process.cwd(),
      "out",
      `ingestion_source=${EVENT_INGESTION_SOURCE}`,
      `parent_run_id=${safePathComponent(artifactRunId)}`,
      `parent_run_type=${artifactRunType}`,
      `parent_nth_run=${artifactNthRun}`,
      `collected_at=${artifactCollectedAt}`,
    );
    fs.mkdirSync(outDir, { recursive: true });

    const csvPath = path.join(outDir, EVENT_DATA_FILE);
    const manifestPath = path.join(outDir, EVENT_MANIFEST_FILE);

    const rows = deduped.map((e) =>
      toCsvRow({
        uri: e.uri == null ? "" : String(e.uri),
        run_type: artifactRunType,
        nth_run: String(artifactNthRun),
        total_article_count: e.totalArticleCount == null ? "" : String(e.totalArticleCount),
        relevance: e.relevance == null ? "" : String(e.relevance),
        event_date: e.eventDate ?? "",
        sentiment: e.sentiment == null ? "" : String(e.sentiment),
        social_score: e.socialScore == null ? "" : String(e.socialScore),
        article_counts: JSON.stringify(e.articleCounts ?? null),
        title: JSON.stringify(e.title ?? null),
        summary: JSON.stringify(e.summary ?? null),
        concepts: JSON.stringify(e.concepts ?? null),
        categories: JSON.stringify(e.categories ?? null),
        common_dates: JSON.stringify(e.commonDates ?? null),
        location: JSON.stringify(e.location ?? null),
        stories: JSON.stringify(e.stories ?? null),
        images: JSON.stringify(e.images ?? null),
        wgt: e.wgt == null ? "" : String(e.wgt),
        raw_event: JSON.stringify(e),
      }),
    );

    fs.writeFileSync(csvPath, csvHeader() + "\n" + rows.join("\n") + "\n", "utf8");

    const manifest = {
      artifact_contract_version: EVENT_ARTIFACT_CONTRACT_VERSION,
      ingestion_source: EVENT_INGESTION_SOURCE,

      run_id: artifactRunId,
      run_type: artifactRunType,
      nth_run: artifactNthRun,

      collected_at: collectedAt,

      parent_run_id: parentRun ? parentRun.run_id : null,
      parent_run_type: parentRun ? parentRun.run_type : null,
      parent_nth_run: parentRun ? parentRun.nth_run : null,
      parent_ingestion_source: parentRun ? parentRun.ingestion_source : null,
      parent_load_report_path: parentRun?.load_report_path ?? null,

      artifact_dir: outDir,

      discovery_scope: EVENT_DISCOVERY_SCOPE,
      discovery_time_column:
        EVENT_DISCOVERY_SCOPE === "time_window" ? EVENT_DISCOVERY_TIME_COLUMN : null,
      discovery_start:
        EVENT_DISCOVERY_SCOPE === "time_window" ? EVENT_DISCOVERY_START : null,
      discovery_end:
        EVENT_DISCOVERY_SCOPE === "time_window" ? EVENT_DISCOVERY_END : null,
      discovery_source_uris: EVENT_DISCOVERY_SOURCE_URIS
        ? EVENT_DISCOVERY_SOURCE_URIS.split(",").map((s) => s.trim()).filter(Boolean)
        : [],
      only_missing: EVENT_DISCOVERY_ONLY_MISSING,
      include_stale: EVENT_DISCOVERY_INCLUDE_STALE,
      stale_after_hours: EVENT_DISCOVERY_INCLUDE_STALE ? EVENT_STALE_AFTER_HOURS : null,
      candidate_articles_scanned: Number(candidateCountRes.rows[0].candidate_articles_scanned ?? 0),
      distinct_event_uris_discovered: allDiscoveredUris.length,
      distinct_event_uris_selected: selectedEventUris.length,
      events_fetched: deduped.length,
      csv_file: EVENT_DATA_FILE,
    };

    fs.writeFileSync(manifestPath, JSON.stringify(manifest, null, 2), "utf8");
    console.log("wrote:", outDir);
  } finally {
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