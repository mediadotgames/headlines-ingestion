import { DateTime } from "luxon";
import { Client } from "pg";
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
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

const s3 = new S3Client({});

const EVENTREGISTRY_API_KEY = process.env.EVENTREGISTRY_API_KEY!;
const EVENTREGISTRY_SOURCE_URIS = process.env.EVENTREGISTRY_SOURCE_URIS!;
const DATABASE_URL = process.env.DATABASE_URL!;
const ARTIFACT_BUCKET = process.env.ARTIFACT_BUCKET!;
const ARTIFACT_PREFIX = process.env.ARTIFACT_PREFIX!;

const LOOKBACK_DAYS = Number(process.env.LOOKBACK_DAYS ?? 2);
const WINDOW_END_DAYS_AGO = Number(process.env.WINDOW_END_DAYS_AGO ?? 0);
const RUN_TYPE = (process.env.RUN_TYPE ?? "scheduled").trim().toLowerCase();
const BACKFILL_LOCAL_DATE = (process.env.BACKFILL_LOCAL_DATE ?? "").trim();

const CANON_TZ = "Pacific/Honolulu";
const INGESTION_SOURCE = "newsapi-ai";

const PAGE_SIZE = 100;
const MAX_PAGES = 500;
const FETCH_MAX_ATTEMPTS = 5;

const sourceUris = EVENTREGISTRY_SOURCE_URIS.split(",")
  .map((s) => s.trim())
  .filter(Boolean);

function computeWindow() {
  let windowFromLocal: DateTime;
  let windowToLocal: DateTime;
  let mode: "explicit_local_date" | "rolling_window";

  if (BACKFILL_LOCAL_DATE) {
    const parsed = DateTime.fromISO(BACKFILL_LOCAL_DATE, { zone: CANON_TZ }).startOf("day");
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

  const windowFromUtc = windowFromLocal.toUTC();
  const windowToUtc = windowToLocal.toUTC();
  const runIdUtc = windowToUtc;

  const dateStart = windowFromUtc.toISODate();
  const dateEnd = windowToUtc.toISODate();

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

function parseArticleDateMs(article: any): number | null {
  const raw = article.dateTimePub ?? article.dateTime ?? null;
  if (!raw) return null;
  const ms = new Date(raw).getTime();
  return Number.isFinite(ms) ? ms : null;
}

async function fetchPage(page: number, dateStart: string, dateEnd: string) {
  const url = "https://eventregistry.org/api/v1/article/getArticles";

  const body = {
    apiKey: EVENTREGISTRY_API_KEY,
    resultType: "articles",
    lang: "eng",
    sourceUri: sourceUris,
    dateStart,
    dateEnd,
    articlesPage: page,
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
  };

  for (let attempt = 1; attempt <= FETCH_MAX_ATTEMPTS; attempt++) {
    try {
      const res = await fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });

      const json = await res.json();

      if (!res.ok) {
        if (isRetryableStatus(res.status) && attempt < FETCH_MAX_ATTEMPTS) {
          const delay = backoffDelayMs(attempt);
          console.warn(`retrying page ${page} in ${delay}ms`);
          await sleep(delay);
          continue;
        }

        throw new Error(`Event Registry HTTP ${res.status}`);
      }

      return json?.articles?.results ?? [];
    } catch (err) {
      if (attempt < FETCH_MAX_ATTEMPTS && isRetryableError(err)) {
        const delay = backoffDelayMs(attempt);
        await sleep(delay);
        continue;
      }
      throw err;
    }
  }

  throw new Error(`page ${page}: exhausted retries`);
}

export const handler = async () => {
  console.log("collector starting");

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

  console.log("mode:", mode);
  console.log("run_type:", RUN_TYPE);
  console.log("window_utc:", {
    window_from: windowFromUtc.toISO(),
    window_to: windowToUtc.toISO(),
  });

  const hardFromMs = windowFromUtc.toMillis();
  const hardToMs = windowToUtc.toMillis();

  const byUri = new Map<string, any>();
  let totalFetched = 0;
  let consecutiveEarlyStopQualifiedPages = 0;

  for (let page = 1; page <= MAX_PAGES; page++) {
    const articles = await fetchPage(page, dateStart!, dateEnd!);

    console.log(`page ${page}: batch=${articles.length}`);

    if (articles.length === 0) break;

    totalFetched += articles.length;

    const sizeBefore = byUri.size;
    let oldestMsOnPage: number | null = null;

    for (const article of articles) {
      const uri = String(article.uri ?? "").trim();
      if (!uri) continue;

      const ms = parseArticleDateMs(article);

      if (ms !== null) {
        if (oldestMsOnPage === null || ms < oldestMsOnPage) {
          oldestMsOnPage = ms;
        }

        if (ms < hardFromMs || ms >= hardToMs) continue;
      }

      byUri.set(uri, article);
    }

    const sizeAfter = byUri.size;
    const newUniqueUrisAdded = sizeAfter - sizeBefore;

    console.log(`page ${page}: new_unique_uris_added=${newUniqueUrisAdded}`);

    if (articles.length < PAGE_SIZE) break;

    if (oldestMsOnPage !== null && oldestMsOnPage < hardFromMs) {
      if (newUniqueUrisAdded === 0) {
        console.warn("possible repeated page, ignoring early stop");
        consecutiveEarlyStopQualifiedPages = 0;
      } else {
        consecutiveEarlyStopQualifiedPages++;

        if (consecutiveEarlyStopQualifiedPages >= 2) {
          console.log("early stop confirmed");
          break;
        }
      }
    } else {
      consecutiveEarlyStopQualifiedPages = 0;
    }
  }

  const deduped = Array.from(byUri.values());

  console.log("fetched:", totalFetched);
  console.log("deduped:", deduped.length);

  const jsonl = deduped.map((a) => JSON.stringify(a)).join("\n");

  const tmpDir = "/tmp/articles";
  await fs.promises.mkdir(tmpDir, { recursive: true });

  const jsonlPath = `${tmpDir}/articles.jsonl`;
  await fs.promises.writeFile(jsonlPath, jsonl);

  const key = `${ARTIFACT_PREFIX}/run_id=${runIdUtc
    .toISO()
    ?.replace(/:/g, "-")}/articles.jsonl`;

  await s3.send(
    new PutObjectCommand({
      Bucket: ARTIFACT_BUCKET,
      Key: key,
      Body: jsonl,
    }),
  );

  console.log("artifact uploaded:", key);

  return {
    fetched: totalFetched,
    deduped: deduped.length,
  };
};