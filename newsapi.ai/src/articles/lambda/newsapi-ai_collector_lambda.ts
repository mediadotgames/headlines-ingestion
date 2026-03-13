import "dotenv/config";
import * as fs from "node:fs";
import * as path from "node:path";
import { Client } from "pg";
import { DateTime } from "luxon";
import {
  S3Client,
  GetObjectCommand,
  PutObjectCommand,
} from "@aws-sdk/client-s3";
import {
  csvHeader,
  toCsvRow,
  ARTIFACT_CONTRACT_VERSION,
} from "../shared/newsapi-aiArtifactSchema";
import {
  sleep,
  isRetryableStatus,
  isRetryableError,
  backoffDelayMs,
} from "../shared/retry";

const EVENTREGISTRY_API_KEY = process.env.EVENTREGISTRY_API_KEY!;
const EVENTREGISTRY_SOURCE_URIS = (
  process.env.EVENTREGISTRY_SOURCE_URIS ?? ""
).trim();
const DATABASE_URL = process.env.DATABASE_URL!;
const ARTIFACT_BUCKET = process.env.ARTIFACT_BUCKET!;
const ARTIFACT_PREFIX = process.env.ARTIFACT_PREFIX!;
const LOOKBACK_DAYS = Number(process.env.LOOKBACK_DAYS ?? 2);
const WINDOW_END_DAYS_AGO = Number(process.env.WINDOW_END_DAYS_AGO ?? 0);
const RUN_TYPE = (process.env.RUN_TYPE ?? "scheduled").trim().toLowerCase();
const BACKFILL_LOCAL_DATE = (process.env.BACKFILL_LOCAL_DATE ?? "").trim();
const COLLECTOR_MODE = (process.env.COLLECTOR_MODE ?? "date_window")
  .trim()
  .toLowerCase();
const ARTICLE_URI_LIST_JSON = (process.env.ARTICLE_URI_LIST_JSON ?? "").trim();
const ARTICLE_URI_LIST_S3_URI = (
  process.env.ARTICLE_URI_LIST_S3_URI ?? ""
).trim();
const ARTICLE_URI_BATCH_SIZE = Number(
  process.env.ARTICLE_URI_BATCH_SIZE ?? 100,
);

if (!EVENTREGISTRY_API_KEY) throw new Error("Missing EVENTREGISTRY_API_KEY");
if (!DATABASE_URL) throw new Error("Missing DATABASE_URL");
if (!ARTIFACT_BUCKET) throw new Error("Missing ARTIFACT_BUCKET");
if (!ARTIFACT_PREFIX) throw new Error("Missing ARTIFACT_PREFIX");
if (!["scheduled", "backfill", "seed"].includes(RUN_TYPE)) {
  throw new Error(`Invalid RUN_TYPE: ${RUN_TYPE}`);
}
if (!["date_window", "article_uri_list"].includes(COLLECTOR_MODE)) {
  throw new Error(`Invalid COLLECTOR_MODE: ${COLLECTOR_MODE}`);
}
if (
  !Number.isInteger(ARTICLE_URI_BATCH_SIZE) ||
  ARTICLE_URI_BATCH_SIZE < 1 ||
  ARTICLE_URI_BATCH_SIZE > 100
) {
  throw new Error(
    "ARTICLE_URI_BATCH_SIZE must be an integer between 1 and 100",
  );
}

const INGESTION_SOURCE = "newsapi-ai";
const CANON_TZ = "Pacific/Honolulu";
const PAGE_SIZE = 100;
const MAX_PAGES = 500;
const FETCH_MAX_ATTEMPTS = 5;
const MAX_CONSECUTIVE_REPEATED_PAGES = 3;

const sourceUris = EVENTREGISTRY_SOURCE_URIS.split(",")
  .map((s) => s.trim())
  .filter(Boolean);

if (COLLECTOR_MODE === "date_window" && sourceUris.length === 0) {
  throw new Error(
    "EVENTREGISTRY_SOURCE_URIS resolved to an empty list in date_window mode",
  );
}

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
  article?: NewsApiAiArticle | null;
  articles?:
    | {
        results?: NewsApiAiArticle[];
        pages?: number;
        totalResults?: number;
      }
    | NewsApiAiArticle[];
  results?: NewsApiAiArticle[];
  error?: unknown;
};

type GetArticleEntry = {
  info?: NewsApiAiArticle;
  error?: string;
};

type GetArticleResponse = Record<string, GetArticleEntry | undefined>;

function isoOrThrow(dt: DateTime, label: string): string {
  const s = dt.toISO();
  if (!s) {
    throw new Error(
      `Luxon produced null ISO for ${label}. isValid=${dt.isValid} reason=${dt.invalidReason ?? ""}`,
    );
  }
  return s;
}

function computeWindow(): {
  runIdUtc: DateTime;
  windowFromUtc: DateTime;
  windowToUtc: DateTime;
  windowFromLocal: DateTime;
  windowToLocal: DateTime;
  dateStart: string;
  dateEnd: string;
  mode: "explicit_local_date" | "rolling_window";
} {
  let windowFromLocal: DateTime;
  let windowToLocal: DateTime;
  let mode: "explicit_local_date" | "rolling_window";

  if (BACKFILL_LOCAL_DATE) {
    const parsed = DateTime.fromISO(BACKFILL_LOCAL_DATE, {
      zone: CANON_TZ,
    }).startOf("day");
    if (!parsed.isValid) {
      throw new Error(`Invalid BACKFILL_LOCAL_DATE: ${BACKFILL_LOCAL_DATE}`);
    }
    windowFromLocal = parsed;
    windowToLocal = parsed.plus({ days: 1 });
    mode = "explicit_local_date";
  } else {
    windowToLocal = DateTime.now()
      .setZone(CANON_TZ)
      .startOf("day")
      .minus({ days: WINDOW_END_DAYS_AGO });

    windowFromLocal = windowToLocal.minus({ days: LOOKBACK_DAYS });
    mode = "rolling_window";
  }

  const windowToUtc = windowToLocal.toUTC();
  const windowFromUtc = windowFromLocal.toUTC();
  const runIdUtc = windowToUtc;

  const dateStart = windowFromUtc.toISODate();
  const dateEnd = windowToUtc.toISODate();

  if (!dateStart || !dateEnd) {
    throw new Error("Failed to compute dateStart/dateEnd in YYYY-MM-DD format");
  }

  return {
    runIdUtc,
    windowFromUtc,
    windowToUtc,
    windowFromLocal,
    windowToLocal,
    dateStart,
    dateEnd,
    mode,
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
  if (payload.article) return [payload.article];
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

function parseGetArticleResponse(payload: unknown): {
  articles: NewsApiAiArticle[];
  errors: Array<{ uri: string; error: string }>;
} {
  const articles: NewsApiAiArticle[] = [];
  const errors: Array<{ uri: string; error: string }> = [];

  for (const [uri, entry] of Object.entries(
    (payload ?? {}) as GetArticleResponse,
  )) {
    if (entry?.info) {
      articles.push(entry.info);
      continue;
    }
    if (entry?.error) {
      errors.push({ uri, error: entry.error });
    }
  }

  return { articles, errors };
}

function normalizeUriList(values: unknown[]): string[] {
  return Array.from(
    new Set(
      values
        .map((value) => (value == null ? "" : String(value).trim()))
        .filter(Boolean),
    ),
  );
}

function parseS3Uri(s3Uri: string): { bucket: string; key: string } {
  if (!s3Uri.startsWith("s3://")) {
    throw new Error(`Invalid S3 URI: ${s3Uri}`);
  }
  const withoutScheme = s3Uri.slice(5);
  const slashIndex = withoutScheme.indexOf("/");
  if (slashIndex === -1) {
    throw new Error(`Invalid S3 URI (missing key): ${s3Uri}`);
  }
  return {
    bucket: withoutScheme.slice(0, slashIndex),
    key: withoutScheme.slice(slashIndex + 1),
  };
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

async function loadRequestedArticleUris(): Promise<string[]> {
  const inputsUsed = [
    Boolean(ARTICLE_URI_LIST_JSON),
    Boolean(ARTICLE_URI_LIST_S3_URI),
  ].filter(Boolean).length;
  if (inputsUsed === 0) {
    throw new Error(
      "article_uri_list mode requires ARTICLE_URI_LIST_JSON or ARTICLE_URI_LIST_S3_URI",
    );
  }
  if (inputsUsed > 1) {
    throw new Error(
      "Provide only one of ARTICLE_URI_LIST_JSON or ARTICLE_URI_LIST_S3_URI",
    );
  }

  let parsed: unknown;
  if (ARTICLE_URI_LIST_JSON) {
    parsed = JSON.parse(ARTICLE_URI_LIST_JSON);
  } else {
    const { bucket, key } = parseS3Uri(ARTICLE_URI_LIST_S3_URI);
    parsed = JSON.parse(await s3GetText(bucket, key));
  }

  if (!Array.isArray(parsed)) {
    throw new Error("Article URI list input must be a JSON array");
  }

  const uris = normalizeUriList(parsed);
  if (uris.length === 0) {
    throw new Error("Article URI list resolved to an empty list");
  }
  return uris;
}

function chunkArray<T>(values: T[], size: number): T[][] {
  const chunks: T[][] = [];
  for (let i = 0; i < values.length; i += size) {
    chunks.push(values.slice(i, i + size));
  }
  return chunks;
}

async function postEventRegistry(
  body: Record<string, unknown>,
  contextLabel: string,
): Promise<EventRegistryResponse> {
  const url = "https://eventregistry.org/api/v1/article/getArticles";

  for (let attempt = 1; attempt <= FETCH_MAX_ATTEMPTS; attempt++) {
    try {
      const res = await fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });

      const json = (await res
        .json()
        .catch(() => ({}))) as EventRegistryResponse;

      if (!res.ok) {
        const message = `Event Registry HTTP ${res.status}: ${JSON.stringify(json)}`;

        if (isRetryableStatus(res.status) && attempt < FETCH_MAX_ATTEMPTS) {
          const delayMs = backoffDelayMs(attempt);
          console.warn(
            `${contextLabel}: retryable API error on attempt ${attempt}/${FETCH_MAX_ATTEMPTS}: ${message}; sleeping ${delayMs}ms`,
          );
          await sleep(delayMs);
          continue;
        }

        throw new Error(message);
      }

      const errMsg = firstErrorMessage(json);
      if (errMsg) {
        throw new Error(`Event Registry error: ${errMsg}`);
      }

      return json;
    } catch (err) {
      if (attempt < FETCH_MAX_ATTEMPTS && isRetryableError(err)) {
        const delayMs = backoffDelayMs(attempt);
        console.warn(
          `${contextLabel}: retryable fetch failure on attempt ${attempt}/${FETCH_MAX_ATTEMPTS}: ${
            err instanceof Error ? err.message : String(err)
          }; sleeping ${delayMs}ms`,
        );
        await sleep(delayMs);
        continue;
      }

      throw err;
    }
  }

  throw new Error(`${contextLabel}: exhausted retries`);
}

async function getEventRegistryByQuery(
  url: string,
  query: URLSearchParams,
  contextLabel: string,
): Promise<unknown> {
  const requestUrl = `${url}?${query.toString()}`;

  for (let attempt = 1; attempt <= FETCH_MAX_ATTEMPTS; attempt++) {
    try {
      const res = await fetch(requestUrl, { method: "GET" });
      const json = await res.json().catch(() => ({}));

      if (!res.ok) {
        const message = `Event Registry HTTP ${res.status}: ${JSON.stringify(json)}`;

        if (isRetryableStatus(res.status) && attempt < FETCH_MAX_ATTEMPTS) {
          const delayMs = backoffDelayMs(attempt);
          console.warn(
            `${contextLabel}: retryable API error on attempt ${attempt}/${FETCH_MAX_ATTEMPTS}: ${message}; sleeping ${delayMs}ms`,
          );
          await sleep(delayMs);
          continue;
        }

        throw new Error(message);
      }

      return json;
    } catch (err) {
      if (attempt < FETCH_MAX_ATTEMPTS && isRetryableError(err)) {
        const delayMs = backoffDelayMs(attempt);
        console.warn(
          `${contextLabel}: retryable fetch failure on attempt ${attempt}/${FETCH_MAX_ATTEMPTS}: ${
            err instanceof Error ? err.message : String(err)
          }; sleeping ${delayMs}ms`,
        );
        await sleep(delayMs);
        continue;
      }

      throw err;
    }
  }

  throw new Error(`${contextLabel}: exhausted retries`);
}

async function fetchPage(args: {
  page: number;
  dateStart: string;
  dateEnd: string;
}): Promise<{ articles: NewsApiAiArticle[]; raw: EventRegistryResponse }> {
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

  const raw = await postEventRegistry(body, `page ${args.page}`);
  return { articles: extractArticles(raw), raw };
}

async function fetchArticleUriBatch(args: {
  uris: string[];
  batchIndex: number;
  batchCount: number;
}): Promise<{
  articles: NewsApiAiArticle[];
  raw: GetArticleResponse;
  errors: Array<{ uri: string; error: string }>;
}> {
  const query = new URLSearchParams();
  query.set("apiKey", EVENTREGISTRY_API_KEY);
  for (const uri of args.uris) {
    query.append("articleUri", uri);
  }
  query.set("includeArticleSocialScore", "true");
  query.set("includeArticleConcepts", "true");
  query.set("includeArticleCategories", "true");
  query.set("includeArticleLocation", "true");
  query.set("includeArticleVideos", "true");
  query.set("includeArticleLinks", "true");
  query.set("includeArticleExtractedDates", "true");
  query.set("includeArticleDuplicateList", "true");
  query.set("includeArticleOriginalArticle", "true");
  query.set("includeSourceDescription", "true");
  query.set("includeSourceLocation", "true");
  query.set("includeSourceRanking", "true");
  query.set("includeConceptImage", "true");
  query.set("includeConceptSynonyms", "true");
  query.set("includeCategoryParentUri", "true");
  query.set("includeLocationGeoLocation", "true");
  query.set("includeLocationPopulation", "true");
  query.set("includeLocationGeoNamesId", "true");
  query.set("includeLocationCountryArea", "true");
  query.set("includeLocationCountryContinent", "true");

  const raw = (await getEventRegistryByQuery(
    "https://eventregistry.org/api/v1/article/getArticle",
    query,
    `article_uri_batch ${args.batchIndex}/${args.batchCount} size=${args.uris.length}`,
  )) as GetArticleResponse;
  const { articles, errors } = parseGetArticleResponse(raw);
  return { articles, raw, errors };
}

async function reservePipelineRun(
  db: Client,
  args: {
    runId: Date;
    ingestionSource: string;
    runType: string;
    windowFrom: Date;
    windowTo: Date;
  },
): Promise<number> {
  await db.query("BEGIN");
  try {
    await db.query(
      `SELECT pg_advisory_xact_lock(hashtext($1 || '|' || $2 || '|' || $3))`,
      [args.runId.toISOString(), args.ingestionSource, args.runType],
    );

    const nextNthRunRes = await db.query(
      `
      SELECT COALESCE(MAX(nth_run), 0) + 1 AS next_nth_run
      FROM public.pipeline_runs
      WHERE run_id = $1
        AND ingestion_source = $2
        AND run_type = $3
      `,
      [args.runId, args.ingestionSource, args.runType],
    );

    const nthRun = Number(nextNthRunRes.rows[0].next_nth_run);

    await db.query(
      `
      INSERT INTO public.pipeline_runs (
        run_id,
        ingestion_source,
        run_type,
        nth_run,
        window_from,
        window_to,
        status,
        created_at,
        updated_at
      )
      VALUES ($1, $2, $3, $4, $5, $6, 'started', now(), now())
      `,
      [
        args.runId,
        args.ingestionSource,
        args.runType,
        nthRun,
        args.windowFrom,
        args.windowTo,
      ],
    );

    await db.query("COMMIT");
    return nthRun;
  } catch (e) {
    await db.query("ROLLBACK");
    throw e;
  }
}

async function updatePipelineRunCollected(
  db: Client,
  args: {
    runId: Date;
    ingestionSource: string;
    runType: string;
    nthRun: number;
    collectedAt: Date;
    articlesFetched: number;
    articlesDeduped: number;
  },
) {
  await db.query(
    `
    UPDATE public.pipeline_runs
    SET
      collected_at = $5,
      articles_fetched = $6,
      articles_deduped = $7,
      status = 'collected',
      error_code = NULL,
      error_message = NULL,
      updated_at = now()
    WHERE run_id = $1
      AND ingestion_source = $2
      AND run_type = $3
      AND nth_run = $4
    `,
    [
      args.runId,
      args.ingestionSource,
      args.runType,
      args.nthRun,
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
    runType: string;
    nthRun: number;
    errorCode?: string;
    errorMessage: string;
  },
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
      args.runId,
      args.ingestionSource,
      args.runType,
      args.nthRun,
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

function toArtifactJsonRecord(args: {
  article: NewsApiAiArticle;
  runId: string;
  nthRun: number;
  collectedAt: string;
  windowFrom: string;
  windowTo: string;
}) {
  const { article: a, runId, nthRun, collectedAt, windowFrom, windowTo } = args;
  return {
    ingestion_source: INGESTION_SOURCE,
    run_id: runId,
    run_type: RUN_TYPE,
    nth_run: nthRun,
    collected_at: collectedAt,
    window_from: windowFrom,
    window_to: windowTo,
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
  };
}

function toArtifactCsvRow(article: NewsApiAiArticle, nthRun: number): string {
  return toCsvRow({
    uri: article.uri == null ? "" : String(article.uri),
    run_type: RUN_TYPE,
    nth_run: String(nthRun),
    url: article.url ?? "",
    title: article.title ?? "",
    body: article.body ?? "",
    date: article.date ?? "",
    time: article.time ?? "",
    date_time: article.dateTime ?? "",
    date_time_published: article.dateTimePub ?? "",
    lang: article.lang ?? "",
    is_duplicate:
      article.isDuplicate == null ? "" : String(article.isDuplicate),
    data_type: article.dataType ?? "",
    sentiment: article.sentiment == null ? "" : String(article.sentiment),
    event_uri: article.eventUri ?? "",
    relevance: article.relevance == null ? "" : String(article.relevance),
    story_uri: article.storyUri ?? "",
    image: article.image ?? "",
    source: jsonString(article.source),
    authors: jsonString(article.authors ?? []),
    sim: article.sim == null ? "" : String(article.sim),
    wgt: article.wgt == null ? "" : String(article.wgt),
    categories: jsonString(article.categories),
    concepts: jsonString(article.concepts),
    links: jsonString(article.links),
    videos: jsonString(article.videos),
    shares: jsonString(article.shares ?? article.socialScore),
    duplicate_list: jsonString(article.duplicateList),
    extracted_dates: jsonString(article.extractedDates),
    location: jsonString(article.location),
    original_article: jsonString(article.originalArticle),
    raw_article: jsonString(article),
  });
}

export const handler = async () => {
  console.log("collector starting");
  console.log("artifact_contract_version:", ARTIFACT_CONTRACT_VERSION);
  console.log("ingestion_source:", INGESTION_SOURCE);
  console.log("run_type:", RUN_TYPE);
  console.log("collector_mode:", COLLECTOR_MODE);
  console.log("backfill_local_date:", BACKFILL_LOCAL_DATE || null);
  console.log("lookback_days:", LOOKBACK_DAYS);
  console.log("window_end_days_ago:", WINDOW_END_DAYS_AGO);
  console.log("page_size:", PAGE_SIZE);
  console.log("max_pages:", MAX_PAGES);
  console.log("fetch_max_attempts:", FETCH_MAX_ATTEMPTS);
  console.log("source_uris_count:", sourceUris.length);
  if (COLLECTOR_MODE === "date_window") {
    console.log("source_uris:", sourceUris);
  }

  const {
    runIdUtc,
    windowFromUtc,
    windowToUtc,
    windowFromLocal,
    windowToLocal,
    dateStart,
    dateEnd,
    mode,
  } = computeWindow();

  const run_id = isoOrThrow(runIdUtc, "run_id");
  const window_from = isoOrThrow(windowFromUtc, "window_from");
  const window_to = isoOrThrow(windowToUtc, "window_to");
  const window_from_local = isoOrThrow(windowFromLocal, "window_from_local");
  const window_to_local = isoOrThrow(windowToLocal, "window_to_local");

  console.log("mode:", mode);
  console.log("run_id:", run_id);
  console.log("window_utc:", { window_from, window_to });
  console.log("window_local:", { window_from_local, window_to_local });
  console.log("date_range:", { date_start: dateStart, date_end: dateEnd });

  const useSSL = !DATABASE_URL.includes("localhost");
  const db = new Client({
    connectionString: DATABASE_URL,
    ssl: useSSL ? { rejectUnauthorized: false } : false,
    connectionTimeoutMillis: 5000,
  });

  await db.connect();
  console.log("database_connected");

  let nth_run: number | null = null;

  try {
    nth_run = await reservePipelineRun(db, {
      runId: runIdUtc.toJSDate(),
      ingestionSource: INGESTION_SOURCE,
      runType: RUN_TYPE,
      windowFrom: windowFromUtc.toJSDate(),
      windowTo: windowToUtc.toJSDate(),
    });
    console.log("nth_run:", nth_run);

    if (nth_run === null) {
      throw new Error("nth_run must not be null before artifact generation");
    }
    const nthRun = nth_run;

    const safeRunId = run_id.replace(/:/g, "-");
    const tmpDir = path.join(
      "/tmp",
      "out",
      `ingestion_source=${INGESTION_SOURCE}`,
      `run_id=${safeRunId}`,
      `run_type=${RUN_TYPE}`,
      `nth_run=${nthRun}`,
    );
    await fs.promises.mkdir(tmpDir, { recursive: true });

    const jsonlPath = path.join(tmpDir, "articles.jsonl");
    const csvPath = path.join(tmpDir, "articles.csv");
    const manifestPath = path.join(tmpDir, "manifest.json");
    const missingUriPath = path.join(tmpDir, "missing_article_uris.json");

    const s3RunPrefix =
      `${ARTIFACT_PREFIX.replace(/\/$/, "")}` +
      `/ingestion_source=${INGESTION_SOURCE}` +
      `/run_id=${safeRunId}` +
      `/run_type=${RUN_TYPE}` +
      `/nth_run=${nthRun}`;

    console.log("tmp_dir:", tmpDir);
    console.log("s3_run_prefix:", `s3://${ARTIFACT_BUCKET}/${s3RunPrefix}/`);

    const byUri = new Map<string, NewsApiAiArticle>();
    let totalFetched = 0;
    let requestedArticleUriCount = 0;
    let missingArticleUris: string[] = [];
    let articleUriErrors: Array<{ uri: string; error: string }> = [];

    if (COLLECTOR_MODE === "date_window") {
      const hardFromMs = windowFromUtc.toMillis();
      const hardToMs = windowToUtc.toMillis();
      let consecutiveEarlyStopQualifiedPages = 0;

      const seenPageFingerprints = new Set<string>();
      let consecutiveRepeatedPages = 0;

      for (let page = 1; page <= MAX_PAGES; page++) {
        const { articles } = await fetchPage({
          page,
          dateStart,
          dateEnd,
        });

        console.log(`page ${page}: batch=${articles.length}`);

        if (articles.length === 0) {
          console.log(`page ${page}: empty batch, stopping`);
          break;
        }

        totalFetched += articles.length;

        const pageUris = articles
          .map((a) => (a.uri == null ? "" : String(a.uri).trim()))
          .filter(Boolean);
        const uniquePageUris = new Set(pageUris);

        const pageFingerprint = pageUris.slice(0, 10).join("|");
        const pageAlreadySeen = seenPageFingerprints.has(pageFingerprint);

        if (pageAlreadySeen) {
          consecutiveRepeatedPages += 1;
          console.warn(
            `page ${page}: repeated page fingerprint detected (count=${consecutiveRepeatedPages})`,
          );
        } else {
          seenPageFingerprints.add(pageFingerprint);
          consecutiveRepeatedPages = 0;
        }

        if (consecutiveRepeatedPages >= MAX_CONSECUTIVE_REPEATED_PAGES) {
          throw new Error(
            `Collector detected repeated pagination for ${consecutiveRepeatedPages} consecutive pages; aborting to avoid incomplete artifact`,
          );
        }

        let oldestMsOnPage: number | null = null;
        const sizeBefore = byUri.size;

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

        const sizeAfter = byUri.size;
        const newUniqueUrisAdded = sizeAfter - sizeBefore;

        console.log(
          `page ${page}: unique_uris_in_batch=${uniquePageUris.size}`,
        );
        console.log(
          `page ${page}: new_unique_uris_added=${newUniqueUrisAdded}`,
        );
        console.log(
          `page ${page}: sample_uris=${JSON.stringify(pageUris.slice(0, 5))}`,
        );

        if (articles.length < PAGE_SIZE) {
          console.log(`page ${page}: batch smaller than page_size, stopping`);
          break;
        }

        if (oldestMsOnPage != null && oldestMsOnPage < hardFromMs) {
          if (pageAlreadySeen || newUniqueUrisAdded === 0) {
            console.warn(
              `page ${page}: oldest article crossed window_from, but page is suspicious (repeated=${pageAlreadySeen}, new_unique_uris_added=${newUniqueUrisAdded}); ignoring early-stop`,
            );
            consecutiveEarlyStopQualifiedPages = 0;
          } else {
            consecutiveEarlyStopQualifiedPages += 1;
            console.log(
              `page ${page}: early-stop-qualified page count=${consecutiveEarlyStopQualifiedPages} (new_unique_uris_added=${newUniqueUrisAdded})`,
            );

            if (consecutiveEarlyStopQualifiedPages >= 2) {
              console.log(
                `page ${page}: stopping early after ${consecutiveEarlyStopQualifiedPages} consecutive trustworthy early-stop-qualified pages`,
              );
              break;
            }
          }
        } else {
          consecutiveEarlyStopQualifiedPages = 0;
        }
      }
    } else {
      const requestedUris = await loadRequestedArticleUris();
      requestedArticleUriCount = requestedUris.length;
      const requestedUriSet = new Set(requestedUris);
      const batches = chunkArray(requestedUris, ARTICLE_URI_BATCH_SIZE);
      console.log("article_uri_list_count:", requestedUris.length);
      console.log("article_uri_batch_count:", batches.length);
      console.log("article_uri_batch_size:", ARTICLE_URI_BATCH_SIZE);

      for (let i = 0; i < batches.length; i++) {
        const batchUris = batches[i];
        const { articles, errors } = await fetchArticleUriBatch({
          uris: batchUris,
          batchIndex: i + 1,
          batchCount: batches.length,
        });

        console.log(
          `article_uri_batch ${i + 1}/${batches.length}: requested=${batchUris.length} returned=${articles.length} errored=${errors.length}`,
        );
        totalFetched += articles.length;
        articleUriErrors.push(...errors);

        for (const article of articles) {
          const uri = article.uri == null ? "" : String(article.uri).trim();
          if (!uri) continue;
          byUri.set(uri, article);
        }
      }

      missingArticleUris = requestedUris.filter(
        (uri) => !byUri.has(uri) && requestedUriSet.has(uri),
      );
      console.log("article_uri_error_count:", articleUriErrors.length);
      console.log("article_uri_missing_count:", missingArticleUris.length);
      if (articleUriErrors.length > 0) {
        console.log("article_uri_error_sample:", articleUriErrors.slice(0, 10));
      }
      if (missingArticleUris.length > 0) {
        console.log(
          "article_uri_missing_sample:",
          missingArticleUris.slice(0, 10),
        );
      }
    }

    if (COLLECTOR_MODE === "date_window" && byUri.size === 0) {
      throw new Error(
        "Collector fetched zero articles — aborting artifact write",
      );
    }

    const deduped = Array.from(byUri.values()).sort(
      (a, b) => (parseArticleDateMs(b) ?? 0) - (parseArticleDateMs(a) ?? 0),
    );

    const collectedAt = new Date();
    const collected_at = collectedAt.toISOString();

    console.log("fetch_summary:", {
      total_fetched: totalFetched,
      deduped_by_uri: deduped.length,
      requested_article_uri_count: requestedArticleUriCount,
      missing_article_uri_count: missingArticleUris.length,
      collected_at,
    });

    await updatePipelineRunCollected(db, {
      runId: runIdUtc.toJSDate(),
      ingestionSource: INGESTION_SOURCE,
      runType: RUN_TYPE,
      nthRun,
      collectedAt,
      articlesFetched: totalFetched,
      articlesDeduped: deduped.length,
    });
    console.log("pipeline_run_marked_collected");

    const jsonl =
      deduped
        .map((a) =>
          JSON.stringify(
            toArtifactJsonRecord({
              article: a,
              runId: run_id,
              nthRun,
              collectedAt: collected_at,
              windowFrom: window_from,
              windowTo: window_to,
            }),
          ),
        )
        .join("\n") + "\n";
    await fs.promises.writeFile(jsonlPath, jsonl, "utf8");

    const rows = deduped.map((a) => toArtifactCsvRow(a, nthRun));

    await fs.promises.writeFile(
      csvPath,
      csvHeader() + "\n" + rows.join("\n") + "\n",
      "utf8",
    );

    const manifest = {
      artifact_contract_version: ARTIFACT_CONTRACT_VERSION,
      ingestion_source: INGESTION_SOURCE,
      canonical_tz: CANON_TZ,
      collector_mode: COLLECTOR_MODE,
      run_id,
      run_type: RUN_TYPE,
      nth_run,
      window_from,
      window_to,
      collected_at,
      window_from_local,
      window_to_local,
      source_uris: COLLECTOR_MODE === "date_window" ? sourceUris : [],
      lang: "eng",
      articles_fetched: totalFetched,
      articles_deduped: deduped.length,
      requested_article_uri_count: requestedArticleUriCount,
      missing_article_uri_count: missingArticleUris.length,
      article_uri_batch_size:
        COLLECTOR_MODE === "article_uri_list" ? ARTICLE_URI_BATCH_SIZE : null,
      page_size: COLLECTOR_MODE === "date_window" ? PAGE_SIZE : null,
      max_pages: COLLECTOR_MODE === "date_window" ? MAX_PAGES : null,
      pagination_optimization:
        COLLECTOR_MODE === "date_window"
          ? "stop after 2 consecutive trustworthy early-stop-qualified pages; abort after 3 repeated page fingerprints"
          : null,
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

    if (COLLECTOR_MODE === "article_uri_list") {
      await fs.promises.writeFile(
        missingUriPath,
        JSON.stringify(
          {
            run_id,
            run_type: RUN_TYPE,
            nth_run,
            collector_mode: COLLECTOR_MODE,
            requested_article_uri_count: requestedArticleUriCount,
            missing_article_uri_count: missingArticleUris.length,
            missing_article_uris: missingArticleUris,
          },
          null,
          2,
        ),
        "utf8",
      );
    }

    console.log("artifact_files_written:", {
      jsonlPath,
      csvPath,
      manifestPath,
      missingUriPath,
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

    if (COLLECTOR_MODE === "article_uri_list") {
      await uploadFile(
        ARTIFACT_BUCKET,
        `${s3RunPrefix}/missing_article_uris.json`,
        missingUriPath,
        "application/json",
      );
      console.log("uploaded:", `${s3RunPrefix}/missing_article_uris.json`);
    }

    console.log("collector_completed_successfully");
  } catch (e: any) {
    const msg = e?.message ? String(e.message) : String(e);
    console.error("collector_failed:", {
      message: msg,
      run_id,
      run_type: RUN_TYPE,
      nth_run,
      ingestion_source: INGESTION_SOURCE,
    });

    if (nth_run != null) {
      try {
        await markPipelineRunFailed(db, {
          runId: runIdUtc.toJSDate(),
          ingestionSource: INGESTION_SOURCE,
          runType: RUN_TYPE,
          nthRun: nth_run,
          errorCode: "collector_error",
          errorMessage: msg.slice(0, 2000),
        });
        console.log("pipeline_run_marked_failed");
      } catch (inner) {
        console.error("failed_to_mark_pipeline_run_failed:", inner);
      }
    }

    throw e;
  } finally {
    await db.end().catch(() => {});
    console.log("database_connection_closed");
  }
};
