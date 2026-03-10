import "dotenv/config";
import * as fs from "node:fs";
import * as path from "node:path";
import { Client } from "pg";
import { DateTime } from "luxon";
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import { csvHeader, toCsvRow } from "../shared/newsapi-aiArtifactSchema";

const EVENTREGISTRY_API_KEY = process.env.EVENTREGISTRY_API_KEY!;
const EVENTREGISTRY_SOURCE_URIS = process.env.EVENTREGISTRY_SOURCE_URIS!;
const DATABASE_URL = process.env.DATABASE_URL!;
const ARTIFACT_BUCKET = process.env.ARTIFACT_BUCKET!;
const ARTIFACT_PREFIX = process.env.ARTIFACT_PREFIX!;
const LOOKBACK_DAYS = Number(process.env.LOOKBACK_DAYS ?? 2);
const WINDOW_END_DAYS_AGO = Number(process.env.WINDOW_END_DAYS_AGO ?? 0);
const SEED_RUN = process.env.SEED_RUN === "true";
const SEED_RUN_ID = "1900-01-01T00:00:00.000Z";
const ARTIFACT_CONTRACT_VERSION = "newsapi-ai/v2";

if (!EVENTREGISTRY_API_KEY) throw new Error("Missing EVENTREGISTRY_API_KEY");
if (!EVENTREGISTRY_SOURCE_URIS)
  throw new Error("Missing EVENTREGISTRY_SOURCE_URIS");
if (!DATABASE_URL) throw new Error("Missing DATABASE_URL");
if (!ARTIFACT_BUCKET) throw new Error("Missing ARTIFACT_BUCKET");
if (!ARTIFACT_PREFIX) throw new Error("Missing ARTIFACT_PREFIX");

const INGESTION_SOURCE = "newsapi-ai";
const CANON_TZ = "Pacific/Honolulu";
const PAGE_SIZE = 100;
const MAX_PAGES = 500;

const sourceUris = EVENTREGISTRY_SOURCE_URIS.split(",")
  .map((s) => s.trim())
  .filter(Boolean);
if (sourceUris.length === 0)
  throw new Error("EVENTREGISTRY_SOURCE_URIS resolved to an empty list");

const s3 = new S3Client({});

type Source = Record<string, unknown> | null;
type Author = Record<string, unknown>;
type Category = Record<string, unknown>;
type Concept = Record<string, unknown>;
type Video = Record<string, unknown>;
type Shares = Record<string, unknown> | number | null;
type Location = Record<string, unknown> | null;
type OriginalArticle = Record<string, unknown> | null;
type ExtractedDate = Record<string, unknown>;

type NewsApiAiArticle = {
  uri?: string | number | null;
  url?: string | null;
  title?: string | null;
  body?: string | null;
  date?: string | null;
  time?: string | null;
  dateTime?: string | null;
  dateTimePub?: string | null;
  lang?: string | null;
  isDuplicate?: boolean | null;
  dataType?: string | null;
  sentiment?: number | null;
  eventUri?: string | null;
  relevance?: number | null;
  storyUri?: string | null;
  image?: string | null;
  source?: Source;
  authors?: Author[] | null;
  categories?: Category[] | null;
  concepts?: Concept[] | null;
  links?: string[] | null;
  videos?: Video[] | null;
  shares?: Shares;
  socialScore?: Shares;
  duplicateList?: string[] | null;
  extractedDates?: ExtractedDate[] | null;
  location?: Location;
  originalArticle?: OriginalArticle;
  sim?: number | null;
  wgt?: number | null;
};

type EventRegistryResponse = {
  articles?:
    | { results?: NewsApiAiArticle[]; pages?: number; totalResults?: number }
    | NewsApiAiArticle[];
  results?: NewsApiAiArticle[];
  error?: unknown;
};

function isoOrThrow(dt: DateTime, label: string): string {
  const s = dt.toISO();
  if (!s) throw new Error(`Luxon produced null ISO for ${label}`);
  return s;
}

function computeHonoluluWindowUtc() {
  const windowToLocal = DateTime.now()
    .setZone(CANON_TZ)
    .startOf("day")
    .minus({ days: WINDOW_END_DAYS_AGO });
  const windowFromLocal = windowToLocal.minus({ days: LOOKBACK_DAYS });
  const windowToUtc = windowToLocal.toUTC();
  const windowFromUtc = windowFromLocal.toUTC();
  const runIdUtc = windowToUtc;
  return {
    runIdUtc,
    windowFromUtc,
    windowToUtc,
    windowFromLocal,
    windowToLocal,
  };
}

function jsonString(value: unknown): string {
  return JSON.stringify(value ?? null);
}

function parseArticleDateMs(article: NewsApiAiArticle): number | null {
  const raw = article.dateTimePub ?? article.dateTime ?? null;
  if (!raw) return null;
  const ms = new Date(raw).getTime();
  return Number.isFinite(ms) ? ms : null;
}

function extractArticles(payload: EventRegistryResponse): NewsApiAiArticle[] {
  if (Array.isArray(payload.articles)) return payload.articles;
  if (Array.isArray(payload.articles?.results)) return payload.articles.results;
  if (Array.isArray(payload.results)) return payload.results;
  return [];
}

function firstErrorMessage(payload: EventRegistryResponse): string | null {
  if (!payload.error) return null;
  if (typeof payload.error === "string") return payload.error;
  try {
    return JSON.stringify(payload.error);
  } catch {
    return String(payload.error);
  }
}

async function fetchPage(args: {
  page: number;
  dateStart: string;
  dateEnd: string;
}): Promise<{ articles: NewsApiAiArticle[]; raw: EventRegistryResponse }> {
  const url = "https://eventregistry.org/api/v1/article/getArticles";
  const body = {
    apiKey: EVENTREGISTRY_API_KEY,
    resultType: "articles",
    lang: "eng",
    sourceUri: sourceUris,
    dateStart: args.dateStart,
    dateEnd: args.dateEnd,
    articlesPage: args.page,
    articlesCount: PAGE_SIZE,
    articlesSortBy: "date",
    articlesSortByAsc: false,
    includeArticleSocialScore: true,
    includeArticleConcepts: true,
    includeArticleCategories: true,
    includeArticleLocation: true,
    includeArticleVideos: true,
    includeArticleLinks: true,
    includeArticleExtractedDates: true,
    includeArticleDuplicateList: true,
    includeArticleOriginalArticle: true,
    includeSourceDescription: true,
    includeSourceLocation: true,
    includeSourceRanking: true,
    includeConceptImage: true,
    includeConceptSynonyms: true,
    includeCategoryParentUri: true,
    includeLocationGeoLocation: true,
    includeLocationPopulation: true,
    includeLocationGeoNamesId: true,
    includeLocationCountryArea: true,
    includeLocationCountryContinent: true,
  };

  const res = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });

  const json = (await res.json().catch(() => ({}))) as EventRegistryResponse;
  if (!res.ok)
    throw new Error(
      `Event Registry HTTP ${res.status}: ${JSON.stringify(json)}`,
    );
  const errMsg = firstErrorMessage(json);
  if (errMsg) throw new Error(`Event Registry error: ${errMsg}`);
  return { articles: extractArticles(json), raw: json };
}

async function upsertPipelineRunStarted(
  db: Client,
  args: {
    runId: Date;
    ingestionSource: string;
    windowFrom: Date;
    windowTo: Date;
  },
) {
  await db.query(
    `
    INSERT INTO public.pipeline_runs (run_id, ingestion_source, window_from, window_to, status, created_at, updated_at)
    VALUES ($1, $2, $3, $4, 'started', now(), now())
    ON CONFLICT (run_id, ingestion_source) DO UPDATE SET
      window_from = EXCLUDED.window_from,
      window_to = EXCLUDED.window_to,
      status = 'started',
      error_code = NULL,
      error_message = NULL,
      updated_at = now()
  `,
    [args.runId, args.ingestionSource, args.windowFrom, args.windowTo],
  );
}

async function updatePipelineRunCollected(
  db: Client,
  args: {
    runId: Date;
    ingestionSource: string;
    collectedAt: Date;
    articlesFetched: number;
    articlesDeduped: number;
  },
) {
  await db.query(
    `
    UPDATE public.pipeline_runs
    SET collected_at = $3, articles_fetched = $4, articles_deduped = $5, status = 'collected', error_code = NULL, error_message = NULL, updated_at = now()
    WHERE run_id = $1 AND ingestion_source = $2
  `,
    [
      args.runId,
      args.ingestionSource,
      args.collectedAt,
      args.articlesFetched,
      args.articlesDeduped,
    ],
  );
}

async function markPipelineRunFailed(
  db: Client,
  args: {
    runId: Date;
    ingestionSource: string;
    errorCode?: string;
    errorMessage: string;
  },
) {
  await db.query(
    `
    UPDATE public.pipeline_runs
    SET status = 'failed', error_code = $3, error_message = $4, updated_at = now()
    WHERE run_id = $1 AND ingestion_source = $2
  `,
    [
      args.runId,
      args.ingestionSource,
      args.errorCode ?? null,
      args.errorMessage,
    ],
  );
}

async function uploadFile(
  bucket: string,
  key: string,
  filePath: string,
  contentType: string,
) {
  const body = await fs.promises.readFile(filePath);
  await s3.send(
    new PutObjectCommand({
      Bucket: bucket,
      Key: key,
      Body: body,
      ContentType: contentType,
    }),
  );
}

export const handler = async () => {
  console.log("collector starting");
  console.log("artifact_contract_version:", ARTIFACT_CONTRACT_VERSION);
  console.log("ingestion_source:", INGESTION_SOURCE);
  console.log("seed_run:", SEED_RUN);
  console.log("lookback_days:", LOOKBACK_DAYS);
  console.log("window_end_days_ago:", WINDOW_END_DAYS_AGO);
  console.log("page_size:", PAGE_SIZE);
  console.log("max_pages:", MAX_PAGES);
  console.log("source_uris_count:", sourceUris.length);
  console.log("source_uris:", sourceUris);

  const {
    runIdUtc,
    windowFromUtc,
    windowToUtc,
    windowFromLocal,
    windowToLocal,
  } = computeHonoluluWindowUtc();
  const run_id = SEED_RUN ? SEED_RUN_ID : isoOrThrow(runIdUtc, "run_id");
  const window_from = isoOrThrow(windowFromUtc, "window_from");
  const window_to = isoOrThrow(windowToUtc, "window_to");
  const window_from_local = isoOrThrow(windowFromLocal, "window_from_local");
  const window_to_local = isoOrThrow(windowToLocal, "window_to_local");
  const date_start = windowFromUtc.toISODate();
  const date_end = windowToUtc.toISODate();

  if (!date_start || !date_end) {
    throw new Error("Failed to compute dateStart/dateEnd in YYYY-MM-DD format");
  }

  console.log("run_id:", run_id);
  console.log("window_utc:", { window_from, window_to });
  console.log("window_local:", { window_from_local, window_to_local });
  console.log("date_range:", { date_start, date_end });

  const useSSL = !DATABASE_URL.includes("localhost");
  const db = new Client({
    connectionString: DATABASE_URL,
    ssl: useSSL ? { rejectUnauthorized: false } : false,
    connectionTimeoutMillis: 5000,
  });
  await db.connect();
  console.log("database_connected");

  const safeRunId = run_id.replace(/:/g, "-");
  const tmpDir = path.join(
    "/tmp",
    "out",
    `ingestion_source=${INGESTION_SOURCE}`,
    `run_id=${safeRunId}`,
  );
  await fs.promises.mkdir(tmpDir, { recursive: true });

  const jsonlPath = path.join(tmpDir, "articles.jsonl");
  const csvPath = path.join(tmpDir, "articles.csv");
  const manifestPath = path.join(tmpDir, "manifest.json");
  const s3RunPrefix = `${ARTIFACT_PREFIX.replace(/\/$/, "")}/ingestion_source=${INGESTION_SOURCE}/run_id=${safeRunId}`;

  console.log("tmp_dir:", tmpDir);
  console.log("s3_run_prefix:", `s3://${ARTIFACT_BUCKET}/${s3RunPrefix}/`);

  try {
    await upsertPipelineRunStarted(db, {
      runId: runIdUtc.toJSDate(),
      ingestionSource: INGESTION_SOURCE,
      windowFrom: windowFromUtc.toJSDate(),
      windowTo: windowToUtc.toJSDate(),
    });
    console.log("pipeline_run_marked_started");

    const hardFromMs = windowFromUtc.toMillis();
    const hardToMs = windowToUtc.toMillis();
    const byUri = new Map<string, NewsApiAiArticle>();
    let totalFetched = 0;

    for (let page = 1; page <= MAX_PAGES; page++) {
      const { articles } = await fetchPage({
        page,
        dateStart: date_start,
        dateEnd: date_end,
      });
      console.log(`page ${page}: batch=${articles.length}`);

      if (articles.length === 0) {
        console.log(`page ${page}: empty batch, stopping`);
        break;
      }

      totalFetched += articles.length;
      let oldestMsOnPage: number | null = null;

      for (const article of articles) {
        const uri = article.uri == null ? "" : String(article.uri).trim();
        if (!uri) continue;

        const articleMs = parseArticleDateMs(article);
        if (articleMs != null) {
          if (oldestMsOnPage == null || articleMs < oldestMsOnPage) {
            oldestMsOnPage = articleMs;
          }
          if (articleMs < hardFromMs || articleMs >= hardToMs) {
            continue;
          }
        }

        byUri.set(uri, article);
      }

      if (articles.length < PAGE_SIZE) {
        console.log(`page ${page}: batch smaller than page_size, stopping`);
        break;
      }

      if (oldestMsOnPage != null && oldestMsOnPage < hardFromMs) {
        console.log(
          `page ${page}: stopping early because oldest article crossed window_from`,
        );
        break;
      }
    }

    if (byUri.size === 0)
      throw new Error(
        "Collector fetched zero articles — aborting artifact write",
      );

    const deduped = Array.from(byUri.values()).sort(
      (a, b) => (parseArticleDateMs(b) ?? 0) - (parseArticleDateMs(a) ?? 0),
    );
    const collectedAt = new Date();
    const collected_at = collectedAt.toISOString();

    console.log("fetch_summary:", {
      total_fetched: totalFetched,
      deduped_by_uri: deduped.length,
      collected_at,
    });

    await updatePipelineRunCollected(db, {
      runId: runIdUtc.toJSDate(),
      ingestionSource: INGESTION_SOURCE,
      collectedAt,
      articlesFetched: totalFetched,
      articlesDeduped: deduped.length,
    });
    console.log("pipeline_run_marked_collected");

    const jsonl =
      deduped
        .map((a) =>
          JSON.stringify({
            ingestion_source: INGESTION_SOURCE,
            run_id,
            collected_at,
            window_from,
            window_to,
            uri: a.uri == null ? null : String(a.uri),
            url: a.url ?? null,
            title: a.title ?? null,
            body: a.body ?? null,
            date: a.date ?? null,
            time: a.time ?? null,
            date_time: a.dateTime ?? null,
            date_time_published: a.dateTimePub ?? null,
            lang: a.lang ?? null,
            is_duplicate: a.isDuplicate ?? null,
            data_type: a.dataType ?? null,
            sentiment: a.sentiment ?? null,
            event_uri: a.eventUri ?? null,
            relevance: a.relevance ?? null,
            story_uri: a.storyUri ?? null,
            image: a.image ?? null,
            source: a.source ?? null,
            authors: a.authors ?? [],
            sim: a.sim ?? null,
            wgt: a.wgt ?? null,
            categories: a.categories ?? null,
            concepts: a.concepts ?? null,
            links: a.links ?? null,
            videos: a.videos ?? null,
            shares: a.shares ?? a.socialScore ?? null,
            duplicate_list: a.duplicateList ?? null,
            extracted_dates: a.extractedDates ?? null,
            location: a.location ?? null,
            original_article: a.originalArticle ?? null,
            raw_article: a,
          }),
        )
        .join("\n") + "\n";
    await fs.promises.writeFile(jsonlPath, jsonl, "utf8");

    const rows = deduped.map((a) =>
      toCsvRow({
        uri: a.uri == null ? "" : String(a.uri),
        url: a.url ?? "",
        title: a.title ?? "",
        body: a.body ?? "",
        date: a.date ?? "",
        time: a.time ?? "",
        date_time: a.dateTime ?? "",
        date_time_published: a.dateTimePub ?? "",
        lang: a.lang ?? "",
        is_duplicate: a.isDuplicate == null ? "" : String(a.isDuplicate),
        data_type: a.dataType ?? "",
        sentiment: a.sentiment == null ? "" : String(a.sentiment),
        event_uri: a.eventUri ?? "",
        relevance: a.relevance == null ? "" : String(a.relevance),
        story_uri: a.storyUri ?? "",
        image: a.image ?? "",
        source: jsonString(a.source),
        authors: JSON.stringify(a.authors ?? []),
        sim: a.sim == null ? "" : String(a.sim),
        wgt: a.wgt == null ? "" : String(a.wgt),
        categories: jsonString(a.categories),
        concepts: jsonString(a.concepts),
        links: jsonString(a.links),
        videos: jsonString(a.videos),
        shares: jsonString(a.shares ?? a.socialScore),
        duplicate_list: jsonString(a.duplicateList),
        extracted_dates: jsonString(a.extractedDates),
        location: jsonString(a.location),
        original_article: jsonString(a.originalArticle),
        raw_article: jsonString(a),
      }),
    );

    await fs.promises.writeFile(
      csvPath,
      csvHeader() + "\n" + rows.join("\n") + "\n",
      "utf8",
    );

    const manifest = {
      artifact_contract_version: ARTIFACT_CONTRACT_VERSION,
      ingestion_source: INGESTION_SOURCE,
      canonical_tz: CANON_TZ,
      run_id,
      window_from,
      window_to,
      collected_at,
      window_from_local,
      window_to_local,
      source_uris: sourceUris,
      lang: "eng",
      articles_fetched: totalFetched,
      articles_deduped: deduped.length,
      page_size: PAGE_SIZE,
      max_pages: MAX_PAGES,
      pagination_optimization:
        "stop once oldest article on a page is older than window_from",
      schedule_recommendation: {
        service: "EventBridge Scheduler",
        timezone: CANON_TZ,
        time: "00:05",
        note: "Runs shortly after Honolulu midnight so the full USA day including Hawaii is complete.",
      },
    };

    await fs.promises.writeFile(
      manifestPath,
      JSON.stringify(manifest, null, 2),
      "utf8",
    );

    console.log("artifact_files_written:", {
      jsonlPath,
      csvPath,
      manifestPath,
    });

    await uploadFile(
      ARTIFACT_BUCKET,
      `${s3RunPrefix}/articles.jsonl`,
      jsonlPath,
      "application/x-ndjson",
    );
    console.log("uploaded:", `${s3RunPrefix}/articles.jsonl`);

    await uploadFile(
      ARTIFACT_BUCKET,
      `${s3RunPrefix}/articles.csv`,
      csvPath,
      "text/csv",
    );
    console.log("uploaded:", `${s3RunPrefix}/articles.csv`);

    await uploadFile(
      ARTIFACT_BUCKET,
      `${s3RunPrefix}/manifest.json`,
      manifestPath,
      "application/json",
    );
    console.log("uploaded:", `${s3RunPrefix}/manifest.json`);

    console.log("collector_completed_successfully");
  } catch (e: any) {
    const msg = e?.message ? String(e.message) : String(e);
    console.error("collector_failed:", {
      message: msg,
      run_id,
      ingestion_source: INGESTION_SOURCE,
    });

    try {
      await markPipelineRunFailed(db, {
        runId: runIdUtc.toJSDate(),
        ingestionSource: INGESTION_SOURCE,
        errorCode: "collector_error",
        errorMessage: msg.slice(0, 2000),
      });
      console.log("pipeline_run_marked_failed");
    } catch (inner) {
      console.error("failed_to_mark_pipeline_run_failed:", inner);
    }

    throw e;
  } finally {
    await db.end().catch(() => {});
    console.log("database_connection_closed");
  }
};
