import "dotenv/config";
import * as fs from "node:fs";
import * as path from "node:path";
import { Client } from "pg";
import { DateTime } from "luxon";
import {
  csvHeader,
  toCsvRow,
  ARTIFACT_CONTRACT_VERSION,
} from "./shared/newsapi-aiArtifactSchema";
import {
  sleep,
  isRetryableStatus,
  isRetryableError,
  backoffDelayMs,
} from "./shared/retry";

console.log("collector starting...");

const EVENTREGISTRY_API_KEY = process.env.EVENTREGISTRY_API_KEY!;
const EVENTREGISTRY_SOURCE_URIS = process.env.EVENTREGISTRY_SOURCE_URIS!;
const DATABASE_URL = process.env.DATABASE_URL!;
const LOOKBACK_DAYS = Number(process.env.LOOKBACK_DAYS ?? 2);
const WINDOW_END_DAYS_AGO = Number(process.env.WINDOW_END_DAYS_AGO ?? 0);
const RUN_TYPE = (process.env.RUN_TYPE ?? "scheduled").trim().toLowerCase();
const BACKFILL_LOCAL_DATE = (process.env.BACKFILL_LOCAL_DATE ?? "").trim();

if (!EVENTREGISTRY_API_KEY) throw new Error("Missing EVENTREGISTRY_API_KEY");
if (!EVENTREGISTRY_SOURCE_URIS) throw new Error("Missing EVENTREGISTRY_SOURCE_URIS");
if (!DATABASE_URL) throw new Error("Missing DATABASE_URL");
if (!["scheduled", "backfill", "seed"].includes(RUN_TYPE)) {
  throw new Error(`Invalid RUN_TYPE: ${RUN_TYPE}`);
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

if (sourceUris.length === 0) {
  throw new Error("EVENTREGISTRY_SOURCE_URIS resolved to an empty list");
}

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
    | {
        results?: NewsApiAiArticle[];
        pages?: number;
        totalResults?: number;
      }
    | NewsApiAiArticle[];
  results?: NewsApiAiArticle[];
  error?: unknown;
};

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
    const parsed = DateTime.fromISO(BACKFILL_LOCAL_DATE, { zone: CANON_TZ }).startOf("day");
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

  for (let attempt = 1; attempt <= FETCH_MAX_ATTEMPTS; attempt++) {
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
            `page ${args.page}: retryable API error on attempt ${attempt}/${FETCH_MAX_ATTEMPTS}: ${message}; sleeping ${delayMs}ms`,
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

      return { articles: extractArticles(json), raw: json };
    } catch (err) {
      if (attempt < FETCH_MAX_ATTEMPTS && isRetryableError(err)) {
        const delayMs = backoffDelayMs(attempt);
        console.warn(
          `page ${args.page}: retryable fetch failure on attempt ${attempt}/${FETCH_MAX_ATTEMPTS}: ${
            err instanceof Error ? err.message : String(err)
          }; sleeping ${delayMs}ms`,
        );
        await sleep(delayMs);
        continue;
      }

      throw err;
    }
  }

  throw new Error(`page ${args.page}: exhausted retries`);
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
  const sql = `
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
  `;

  await db.query(sql, [
    args.runId,
    args.ingestionSource,
    args.runType,
    args.nthRun,
    args.collectedAt,
    args.articlesFetched,
    args.articlesDeduped,
  ]);
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
  const sql = `
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
  `;

  await db.query(sql, [
    args.runId,
    args.ingestionSource,
    args.runType,
    args.nthRun,
    args.errorCode ?? null,
    args.errorMessage,
  ]);
}

async function main() {
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
  console.log("backfill_local_date:", BACKFILL_LOCAL_DATE || null);
  console.log("ingestion_source:", INGESTION_SOURCE);
  console.log("run_type:", RUN_TYPE);
  console.log("artifact_contract_version:", ARTIFACT_CONTRACT_VERSION);
  console.log("canonical_tz:", CANON_TZ);
  console.log("run_id:", run_id);
  console.log("window (UTC):", window_from, "->", window_to);
  console.log("window (Honolulu local):", window_from_local, "->", window_to_local);
  console.log("api_date_start:", dateStart);
  console.log("api_date_end:", dateEnd);
  console.log("fetch_max_attempts:", FETCH_MAX_ATTEMPTS);
  console.log("source_uris:", sourceUris);

  const useSSL = !DATABASE_URL.includes("localhost");
  const db = new Client({
    connectionString: DATABASE_URL,
    ssl: useSSL ? { rejectUnauthorized: false } : false,
  });
  await db.connect();

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

    const hardFromMs = windowFromUtc.toMillis();
    const hardToMs = windowToUtc.toMillis();
    const byUri = new Map<string, NewsApiAiArticle>();
    let totalFetched = 0;
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
      if (articles.length === 0) break;

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
          if (articleMs < hardFromMs || articleMs >= hardToMs) {
            if (oldestMsOnPage == null || articleMs < oldestMsOnPage) {
              oldestMsOnPage = articleMs;
            }
            continue;
          }
          if (oldestMsOnPage == null || articleMs < oldestMsOnPage) {
            oldestMsOnPage = articleMs;
          }
        }

        byUri.set(uri, article);
      }

      const sizeAfter = byUri.size;
      const newUniqueUrisAdded = sizeAfter - sizeBefore;

      console.log(`page ${page}: unique_uris_in_batch=${uniquePageUris.size}`);
      console.log(`page ${page}: new_unique_uris_added=${newUniqueUrisAdded}`);
      console.log(`page ${page}: sample_uris=${JSON.stringify(pageUris.slice(0, 5))}`);

      if (articles.length < PAGE_SIZE) break;

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

    if (byUri.size === 0) {
      throw new Error("Collector fetched zero articles — aborting artifact write");
    }

    const deduped = Array.from(byUri.values());
    deduped.sort(
      (a, b) => (parseArticleDateMs(b) ?? 0) - (parseArticleDateMs(a) ?? 0),
    );

    const collectedAt = new Date();
    const collected_at = collectedAt.toISOString();

    console.log(`fetched=${totalFetched} deduped_by_uri=${deduped.length}`);

    await updatePipelineRunCollected(db, {
      runId: runIdUtc.toJSDate(),
      ingestionSource: INGESTION_SOURCE,
      runType: RUN_TYPE,
      nthRun: nth_run,
      collectedAt,
      articlesFetched: totalFetched,
      articlesDeduped: deduped.length,
    });

    const outDir = path.join(
      process.cwd(),
      "out",
      `ingestion_source=${INGESTION_SOURCE}`,
      `run_id=${run_id.replace(/:/g, "-")}`,
      `run_type=${RUN_TYPE}`,
      `nth_run=${nth_run}`,
    );
    fs.mkdirSync(outDir, { recursive: true });

    const jsonlPath = path.join(outDir, "articles.jsonl");
    const csvPath = path.join(outDir, "articles.csv");
    const manifestPath = path.join(outDir, "manifest.json");

    const jsonl =
      deduped
        .map((a) =>
          JSON.stringify({
            ingestion_source: INGESTION_SOURCE,
            run_id,
            run_type: RUN_TYPE,
            nth_run,
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
    fs.writeFileSync(jsonlPath, jsonl, "utf8");

    const rows = deduped.map((a) =>
      toCsvRow({
        uri: a.uri == null ? "" : String(a.uri),
        run_type: RUN_TYPE,
        nth_run: String(nth_run),
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
        authors: jsonString(a.authors ?? []),
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

    fs.writeFileSync(csvPath, csvHeader() + "\n" + rows.join("\n") + "\n", "utf8");

    const manifest = {
      artifact_contract_version: ARTIFACT_CONTRACT_VERSION,
      ingestion_source: INGESTION_SOURCE,
      canonical_tz: CANON_TZ,
      run_id,
      run_type: RUN_TYPE,
      nth_run,
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
        "stop after 2 consecutive trustworthy early-stop-qualified pages; abort after 3 repeated page fingerprints",
      schedule_recommendation: {
        service: "EventBridge Scheduler",
        timezone: CANON_TZ,
        time: "00:05",
        note: "Runs shortly after Honolulu midnight so the full USA day including Hawaii is complete.",
      },
    };

    fs.writeFileSync(manifestPath, JSON.stringify(manifest, null, 2), "utf8");
    console.log("wrote:", outDir);
  } catch (e: any) {
    const msg = e?.message ? String(e.message) : String(e);
    console.error(e);

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
      } catch (inner) {
        console.error("Also failed to mark pipeline_runs as failed:", inner);
      }
    }

    process.exitCode = 1;
  } finally {
    await db.end().catch(() => {});
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});