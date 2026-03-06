import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { Client } from "pg";
import { DateTime } from "luxon";
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import { csvHeader, toCsvRow } from "../../shared/newsapi-orgArtifactSchema";

const NEWSAPI_KEY = process.env.NEWSAPI_KEY!;
const SOURCES = process.env.NEWSAPI_SOURCES!;
const DATABASE_URL = process.env.DATABASE_URL!;
const ARTIFACT_BUCKET = process.env.ARTIFACT_BUCKET!;
const ARTIFACT_PREFIX = process.env.ARTIFACT_PREFIX!; // e.g. newsapi.org/out

if (!NEWSAPI_KEY) throw new Error("Missing NEWSAPI_KEY");
if (!SOURCES) throw new Error("Missing NEWSAPI_SOURCES");
if (!DATABASE_URL) throw new Error("Missing DATABASE_URL");
if (!ARTIFACT_BUCKET) throw new Error("Missing ARTIFACT_BUCKET");
if (!ARTIFACT_PREFIX) throw new Error("Missing ARTIFACT_PREFIX");

const PAGE_SIZE = 100;
const INGESTION_SOURCE = "newsapi-org";
const CANON_TZ = "Pacific/Honolulu";

const s3 = new S3Client({});

type NewsApiArticle = {
  source: { id: string | null; name: string };
  author: string | null;
  title: string;
  description: string | null;
  url: string;
  urlToImage: string | null;
  publishedAt: string;
  content: string | null;
};

type NewsApiOk = {
  status: "ok";
  totalResults: number;
  articles: NewsApiArticle[];
};

type NewsApiErr = {
  status: "error";
  code: string;
  message: string;
};

type NewsApiResponse = NewsApiOk | NewsApiErr;

function isoOrThrow(dt: DateTime, label: string): string {
  const s = dt.toISO();
  if (!s) {
    throw new Error(
      `Luxon produced null ISO for ${label}. isValid=${dt.isValid} reason=${dt.invalidReason ?? ""}`,
    );
  }
  return s;
}

async function fetchBatch(fromIso: string, toIso: string): Promise<NewsApiOk> {
  const url = new URL("https://newsapi.org/v2/everything");
  url.searchParams.set("sources", SOURCES);
  url.searchParams.set("from", fromIso);
  url.searchParams.set("to", toIso);
  url.searchParams.set("sortBy", "publishedAt");
  url.searchParams.set("pageSize", String(PAGE_SIZE));
  url.searchParams.set("page", "1");

  const res = await fetch(url.toString(), {
    headers: { "X-Api-Key": NEWSAPI_KEY },
  });

  const json: NewsApiResponse = await res.json().catch(() => ({} as any));

  if (!res.ok) {
    throw new Error(`NewsAPI HTTP ${res.status}: ${JSON.stringify(json)}`);
  }

  if (json.status !== "ok") {
    throw new Error(`NewsAPI error ${json.code}: ${json.message}`);
  }

  return json;
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
  const sql = `
    INSERT INTO public.pipeline_runs (
      run_id, ingestion_source, window_from, window_to,
      status, created_at, updated_at
    )
    VALUES ($1, $2, $3, $4, 'started', now(), now())
    ON CONFLICT (run_id, ingestion_source) DO UPDATE SET
      window_from = EXCLUDED.window_from,
      window_to = EXCLUDED.window_to,
      status = 'started',
      error_code = NULL,
      error_message = NULL,
      updated_at = now()
  `;

  await db.query(sql, [
    args.runId,
    args.ingestionSource,
    args.windowFrom,
    args.windowTo,
  ]);
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

  await db.query(sql, [
    args.runId,
    args.ingestionSource,
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
    errorCode?: string;
    errorMessage: string;
  },
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

  await db.query(sql, [
    args.runId,
    args.ingestionSource,
    args.errorCode ?? null,
    args.errorMessage,
  ]);
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
  const runIdUtc = windowToUtc;

  return {
    runIdUtc,
    windowFromUtc,
    windowToUtc,
    windowFromLocal,
    windowToLocal,
  };
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
  const {
    runIdUtc,
    windowFromUtc,
    windowToUtc,
    windowFromLocal,
    windowToLocal,
  } = computeHonoluluWindowUtc();

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

  const useSSL = !DATABASE_URL.includes("localhost");

  const db = new Client({
    connectionString: DATABASE_URL,
    ssl: useSSL ? { rejectUnauthorized: false } : false,
  });
  await db.connect();

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

  const s3RunPrefix =
    `${ARTIFACT_PREFIX.replace(/\/$/, "")}` +
    `/ingestion_source=${INGESTION_SOURCE}` +
    `/run_id=${safeRunId}`;

  try {
    await upsertPipelineRunStarted(db, {
      runId: runIdUtc.toJSDate(),
      ingestionSource: INGESTION_SOURCE,
      windowFrom: windowFromUtc.toJSDate(),
      windowTo: windowToUtc.toJSDate(),
    });

    let cursorTo = windowToUtc.toJSDate();
    const hardFromMs = windowFromUtc.toMillis();

    const byUrl = new Map<string, NewsApiArticle>();
    let totalFetched = 0;

    for (let iter = 1; iter <= 500; iter++) {
      const fromIso = window_from;
      const toIso = cursorTo.toISOString();

      const data = await fetchBatch(fromIso, toIso);
      const batch = data.articles ?? [];

      console.log(
        `iter ${iter}: to=${toIso} batch=${batch.length} totalResults=${data.totalResults}`,
      );

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
    deduped.sort(
      (a, b) =>
        new Date(b.publishedAt).getTime() - new Date(a.publishedAt).getTime(),
    );

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
          }),
        )
        .join("\n") + "\n";
    await fs.promises.writeFile(jsonlPath, jsonl, "utf8");

    const rows = deduped.map((a) =>
      toCsvRow({
        source_id: a.source?.id ?? "",
        source_name: a.source?.name ?? "",
        author: a.author ?? "",
        title: a.title ?? "",
        description: a.description ?? "",
        url: a.url ?? "",
        url_to_image: a.urlToImage ?? "",
        published_at: a.publishedAt ?? "",
        content: a.content ?? "",
      }),
    );

    await fs.promises.writeFile(
      csvPath,
      csvHeader() + "\n" + rows.join("\n") + "\n",
      "utf8",
    );

    const manifest = {
      ingestion_source: INGESTION_SOURCE,
      canonical_tz: CANON_TZ,
      run_id,
      window_from,
      window_to,
      collected_at,
      window_from_local,
      window_to_local,
      sources: SOURCES.split(",")
        .map((s) => s.trim())
        .filter(Boolean),
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

    await fs.promises.writeFile(
      manifestPath,
      JSON.stringify(manifest, null, 2),
      "utf8",
    );

    await uploadFile(
      ARTIFACT_BUCKET,
      `${s3RunPrefix}/articles.jsonl`,
      jsonlPath,
      "application/x-ndjson",
    );
    await uploadFile(
      ARTIFACT_BUCKET,
      `${s3RunPrefix}/articles.csv`,
      csvPath,
      "text/csv",
    );
    await uploadFile(
      ARTIFACT_BUCKET,
      `${s3RunPrefix}/manifest.json`,
      manifestPath,
      "application/json",
    );

    console.log("uploaded to:", `s3://${ARTIFACT_BUCKET}/${s3RunPrefix}/`);
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

    throw e;
  } finally {
    await db.end().catch(() => {});
  }
};