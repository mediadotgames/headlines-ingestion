import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { Client } from "pg";
import { DateTime } from "luxon";

/**
 * newsapi-org_collector.ts
 *
 * Canonical "USA day" policy:
 * - To ensure we don't miss anything published before midnight anywhere in the USA,
 *   we use Hawaii (Pacific/Honolulu) as the canonical timezone boundary.
 * - Each run collects the *previous calendar day* in Pacific/Honolulu:
 *     window_from_local = startOfDay(Honolulu, today) - 1 day
 *     window_to_local   = startOfDay(Honolulu, today)
 * - We store run_id/window_from/window_to as UTC instants (ISO strings),
 *   because timestamptz and APIs are UTC-friendly.
 *
 * Recommended EventBridge Scheduler settings:
 * - Schedule: Daily at 00:05
 * - Timezone: Pacific/Honolulu
 */

const NEWSAPI_KEY = process.env.NEWSAPI_KEY!;
const SOURCES = process.env.NEWSAPI_SOURCES!;
const DATABASE_URL = process.env.DATABASE_URL!;

if (!NEWSAPI_KEY) throw new Error("Missing NEWSAPI_KEY");
if (!SOURCES) throw new Error("Missing NEWSAPI_SOURCES");
if (!DATABASE_URL) throw new Error("Missing DATABASE_URL");

const PAGE_SIZE = 100;
const INGESTION_SOURCE = "newsapi-org";
const CANON_TZ = "Pacific/Honolulu";

type NewsApiArticle = {
  source: { id: string | null; name: string };
  author: string | null;
  title: string;
  description: string | null;
  url: string;
  urlToImage: string | null;
  publishedAt: string; // ISO UTC
  content: string | null;
};

type NewsApiOk = { status: "ok"; totalResults: number; articles: NewsApiArticle[] };
type NewsApiErr = { status: "error"; code: string; message: string };
type NewsApiResponse = NewsApiOk | NewsApiErr;

/**
 * Luxon DateTime.toISO() is typed as string | null.
 * In our use, these should never be null; if they are, we want a hard failure.
 */
function isoOrThrow(dt: DateTime, label: string): string {
  const s = dt.toISO();
  if (!s) throw new Error(`Luxon produced null ISO for ${label}. isValid=${dt.isValid} reason=${dt.invalidReason ?? ""}`);
  return s;
}

function escapeCsv(v: unknown): string {
  if (v === null || v === undefined) return "";
  const s = String(v);
  if (/[",\n\r]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

async function fetchBatch(fromIso: string, toIso: string): Promise<NewsApiOk> {
  const url = new URL("https://newsapi.org/v2/everything");
  url.searchParams.set("sources", SOURCES);
  url.searchParams.set("from", fromIso);
  url.searchParams.set("to", toIso);
  url.searchParams.set("sortBy", "publishedAt"); // newest first
  url.searchParams.set("pageSize", String(PAGE_SIZE));
  url.searchParams.set("page", "1");

  const res = await fetch(url.toString(), { headers: { "X-Api-Key": NEWSAPI_KEY } });
  const json: NewsApiResponse = await res.json().catch(() => ({} as any));

  if (!res.ok) throw new Error(`NewsAPI HTTP ${res.status}: ${JSON.stringify(json)}`);
  if (json.status !== "ok") throw new Error(`NewsAPI error ${json.code}: ${json.message}`);

  return json;
}

async function upsertPipelineRunStarted(
  db: Client,
  args: { runId: Date; ingestionSource: string; windowFrom: Date; windowTo: Date }
) {
  const sql = `
    INSERT INTO public.pipeline_runs (
      run_id, ingestion_source, window_from, window_to,
      status, created_at, updated_at
    )
    VALUES ($1, $2, $3, $4, 'started', now(), now())
    ON CONFLICT (run_id, ingestion_source) DO UPDATE SET
      window_from = EXCLUDED.window_from,
      window_to   = EXCLUDED.window_to,
      status      = 'started',
      error_code  = NULL,
      error_message = NULL,
      updated_at  = now()
  `;
  await db.query(sql, [args.runId, args.ingestionSource, args.windowFrom, args.windowTo]);
}

async function updatePipelineRunCollected(
  db: Client,
  args: { runId: Date; ingestionSource: string; collectedAt: Date; articlesFetched: number; articlesDeduped: number }
) {
  const sql = `
    UPDATE public.pipeline_runs
    SET
      collected_at = $3,
      articles_fetched = $4,
      articles_deduped = $5,
      status = 'collected',
      error_code = NULL,
      error_message = NULL,
      updated_at = now()
    WHERE run_id = $1 AND ingestion_source = $2
  `;
  await db.query(sql, [args.runId, args.ingestionSource, args.collectedAt, args.articlesFetched, args.articlesDeduped]);
}

async function markPipelineRunFailed(
  db: Client,
  args: { runId: Date; ingestionSource: string; errorCode?: string; errorMessage: string }
) {
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

function computeHonoluluWindowUtc(): {
  runIdUtc: DateTime;
  windowFromUtc: DateTime;
  windowToUtc: DateTime;
  windowFromLocal: DateTime;
  windowToLocal: DateTime;
} {
  const windowToLocal = DateTime.now().setZone(CANON_TZ).startOf("day");
  const windowFromLocal = windowToLocal.minus({ days: 1 });

  const windowToUtc = windowToLocal.toUTC();
  const windowFromUtc = windowFromLocal.toUTC();

  // run_id is the UTC instant corresponding to Honolulu midnight ending the window
  const runIdUtc = windowToUtc;

  return { runIdUtc, windowFromUtc, windowToUtc, windowFromLocal, windowToLocal };
}

async function main() {
  const { runIdUtc, windowFromUtc, windowToUtc, windowFromLocal, windowToLocal } =
    computeHonoluluWindowUtc();

  // Force ISO strings (no nulls)
  const run_id = isoOrThrow(runIdUtc, "run_id");
  const window_from = isoOrThrow(windowFromUtc, "window_from");
  const window_to = isoOrThrow(windowToUtc, "window_to");
  const window_from_local = isoOrThrow(windowFromLocal, "window_from_local");
  const window_to_local = isoOrThrow(windowToLocal, "window_to_local");

  console.log("ingestion_source:", INGESTION_SOURCE);
  console.log("canonical_tz:", CANON_TZ);
  console.log("run_id (UTC instant of Honolulu midnight):", run_id);
  console.log("window (UTC):", window_from, "->", window_to);
  console.log("window (Honolulu local):", window_from_local, "->", window_to_local);
  console.log("sources:", SOURCES);

  const db = new Client({ connectionString: DATABASE_URL });
  await db.connect();

  try {
    await upsertPipelineRunStarted(db, {
      runId: runIdUtc.toJSDate(),
      ingestionSource: INGESTION_SOURCE,
      windowFrom: windowFromUtc.toJSDate(),
      windowTo: windowToUtc.toJSDate(),
    });

    // Cursor loop over the UTC window bounds
    let cursorTo = windowToUtc.toJSDate();
    const hardFromMs = windowFromUtc.toMillis();

    const byUrl = new Map<string, NewsApiArticle>();
    let totalFetched = 0;

    for (let iter = 1; iter <= 500; iter++) {
      const fromIso = window_from; // string (not null)
      const toIso = cursorTo.toISOString(); // string

      const data = await fetchBatch(fromIso, toIso);
      const batch = data.articles ?? [];

      console.log(`iter ${iter}: to=${toIso} batch=${batch.length} totalResults=${data.totalResults}`);

      if (batch.length === 0) break;

      totalFetched += batch.length;
      for (const a of batch) {
        if (a?.url) byUrl.set(a.url, a);
      }

      if (batch.length < PAGE_SIZE) break;

      const oldest = batch[batch.length - 1];
      const oldestMs = new Date(oldest.publishedAt).getTime();

      let nextToMs = oldestMs - 1;
      if (nextToMs >= cursorTo.getTime()) nextToMs = cursorTo.getTime() - 1000;
      if (nextToMs <= hardFromMs) break;

      cursorTo = new Date(nextToMs);
    }

    const deduped = Array.from(byUrl.values());
    deduped.sort((a, b) => new Date(b.publishedAt).getTime() - new Date(a.publishedAt).getTime());

    const collectedAt = new Date();
    const collected_at = collectedAt.toISOString();

    console.log(`fetched=${totalFetched} deduped_by_url=${deduped.length}`);

    await updatePipelineRunCollected(db, {
      runId: runIdUtc.toJSDate(),
      ingestionSource: INGESTION_SOURCE,
      collectedAt,
      articlesFetched: totalFetched,
      articlesDeduped: deduped.length,
    });

    // Write artifacts locally
    const outDir = path.join(
      process.cwd(),
      "out",
      `ingestion_source=${INGESTION_SOURCE}`,
      `run_id=${run_id.replace(/:/g, "-")}` // safe for Windows paths
    );
    fs.mkdirSync(outDir, { recursive: true });

    const jsonlPath = path.join(outDir, "articles.jsonl");
    const csvPath = path.join(outDir, "articles.csv");
    const manifestPath = path.join(outDir, "manifest.json");

    // JSONL
    const jsonl =
      deduped
        .map((a) =>
          JSON.stringify({
            ingestion_source: INGESTION_SOURCE,
            run_id,
            collected_at,
            window_from,
            window_to,
            source_id: a.source?.id ?? null,
            source_name: a.source?.name ?? null,
            author: a.author ?? null,
            title: a.title ?? null,
            description: a.description ?? null,
            url: a.url ?? null,
            url_to_image: a.urlToImage ?? null,
            published_at: a.publishedAt ?? null,
            content: a.content ?? null,
          })
        )
        .join("\n") + "\n";
    fs.writeFileSync(jsonlPath, jsonl, "utf8");

    // CSV
    const header = [
      "ingestion_source",
      "run_id",
      "collected_at",
      "window_from",
      "window_to",
      "source_id",
      "source_name",
      "author",
      "title",
      "description",
      "url",
      "url_to_image",
      "published_at",
      "content",
    ];

    const rows = deduped.map((a) =>
      [
        INGESTION_SOURCE,
        run_id,
        collected_at,
        window_from,
        window_to,
        a.source?.id ?? "",
        a.source?.name ?? "",
        a.author ?? "",
        a.title ?? "",
        a.description ?? "",
        a.url ?? "",
        a.urlToImage ?? "",
        a.publishedAt ?? "",
        a.content ?? "",
      ]
        .map(escapeCsv)
        .join(",")
    );
    fs.writeFileSync(csvPath, header.join(",") + "\n" + rows.join("\n") + "\n", "utf8");

    // Manifest
    const manifest = {
      ingestion_source: INGESTION_SOURCE,
      canonical_tz: CANON_TZ,
      run_id,
      window_from,
      window_to,
      collected_at,
      window_from_local,
      window_to_local,
      sources: SOURCES.split(",").map((s) => s.trim()).filter(Boolean),
      articles_fetched: totalFetched,
      articles_deduped: deduped.length,
      page_size: PAGE_SIZE,
      schedule_recommendation: {
        service: "EventBridge Scheduler",
        timezone: CANON_TZ,
        time: "00:05",
        note: "Runs shortly after Honolulu midnight so the full 'USA day' (incl. Hawaii) is complete.",
      },
    };

    fs.writeFileSync(manifestPath, JSON.stringify(manifest, null, 2), "utf8");
    console.log("wrote:", outDir);
  } catch (e: any) {
    const msg = e?.message ? String(e.message) : String(e);
    console.error(e);

    try {
      await markPipelineRunFailed(db, {
        runId: runIdUtc.toJSDate(),
        ingestionSource: INGESTION_SOURCE,
        errorCode: "collector_error",
        errorMessage: msg.slice(0, 2000),
      });
    } catch (inner) {
      console.error("Also failed to mark pipeline_runs as failed:", inner);
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