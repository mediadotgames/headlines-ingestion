"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __generator = (this && this.__generator) || function (thisArg, body) {
    var _ = { label: 0, sent: function() { if (t[0] & 1) throw t[1]; return t[1]; }, trys: [], ops: [] }, f, y, t, g = Object.create((typeof Iterator === "function" ? Iterator : Object).prototype);
    return g.next = verb(0), g["throw"] = verb(1), g["return"] = verb(2), typeof Symbol === "function" && (g[Symbol.iterator] = function() { return this; }), g;
    function verb(n) { return function (v) { return step([n, v]); }; }
    function step(op) {
        if (f) throw new TypeError("Generator is already executing.");
        while (g && (g = 0, op[0] && (_ = 0)), _) try {
            if (f = 1, y && (t = op[0] & 2 ? y["return"] : op[0] ? y["throw"] || ((t = y["return"]) && t.call(y), 0) : y.next) && !(t = t.call(y, op[1])).done) return t;
            if (y = 0, t) op = [op[0] & 2, t.value];
            switch (op[0]) {
                case 0: case 1: t = op; break;
                case 4: _.label++; return { value: op[1], done: false };
                case 5: _.label++; y = op[1]; op = [0]; continue;
                case 7: op = _.ops.pop(); _.trys.pop(); continue;
                default:
                    if (!(t = _.trys, t = t.length > 0 && t[t.length - 1]) && (op[0] === 6 || op[0] === 2)) { _ = 0; continue; }
                    if (op[0] === 3 && (!t || (op[1] > t[0] && op[1] < t[3]))) { _.label = op[1]; break; }
                    if (op[0] === 6 && _.label < t[1]) { _.label = t[1]; t = op; break; }
                    if (t && _.label < t[2]) { _.label = t[2]; _.ops.push(op); break; }
                    if (t[2]) _.ops.pop();
                    _.trys.pop(); continue;
            }
            op = body.call(thisArg, _);
        } catch (e) { op = [6, e]; y = 0; } finally { f = t = 0; }
        if (op[0] & 5) throw op[1]; return { value: op[0] ? op[1] : void 0, done: true };
    }
};
Object.defineProperty(exports, "__esModule", { value: true });
require("dotenv/config");
var fs = require("node:fs");
var path = require("node:path");
var pg_1 = require("pg");
var pg_copy_streams_1 = require("pg-copy-streams");
var promises_1 = require("node:stream/promises");
var newsapi_aiArtifactSchema_1 = require("./shared/newsapi-aiArtifactSchema");
var DATABASE_URL = process.env.DATABASE_URL;
if (!DATABASE_URL)
    throw new Error("Missing DATABASE_URL");
function truncateErrorMessage(msg, max) {
    if (max === void 0) { max = 2000; }
    return msg.length <= max ? msg : msg.slice(0, max);
}
function setLoadStarted(db, runId, ingestionSource, whenIso) {
    return __awaiter(this, void 0, void 0, function () {
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0: return [4 /*yield*/, db.query("\n    UPDATE public.pipeline_runs\n    SET\n      load_started_at = COALESCE(load_started_at, $3),\n      updated_at = now()\n    WHERE run_id = $1 AND ingestion_source = $2\n    ", [runId, ingestionSource, whenIso])];
                case 1:
                    _a.sent();
                    return [2 /*return*/];
            }
        });
    });
}
function setLoadCompleted(db, runId, ingestionSource, whenIso, rowsLoaded, dbRowsInserted, dbRowsUpdated) {
    return __awaiter(this, void 0, void 0, function () {
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0: return [4 /*yield*/, db.query("\n    UPDATE public.pipeline_runs\n    SET\n      load_completed_at = $3,\n      rows_loaded = $4,\n      db_rows_inserted = $5,\n      db_rows_updated = $6,\n      status = 'loaded',\n      error_code = NULL,\n      error_message = NULL,\n      updated_at = now()\n    WHERE run_id = $1 AND ingestion_source = $2\n    ", [
                        runId,
                        ingestionSource,
                        whenIso,
                        rowsLoaded,
                        dbRowsInserted,
                        dbRowsUpdated,
                    ])];
                case 1:
                    _a.sent();
                    return [2 /*return*/];
            }
        });
    });
}
function markRunFailed(db, runId, ingestionSource, code, message) {
    return __awaiter(this, void 0, void 0, function () {
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0: return [4 /*yield*/, db.query("\n    UPDATE public.pipeline_runs\n    SET\n      status = 'failed',\n      error_code = $3,\n      error_message = $4,\n      updated_at = now()\n    WHERE run_id = $1 AND ingestion_source = $2\n    ", [runId, ingestionSource, code, truncateErrorMessage(message)])];
                case 1:
                    _a.sent();
                    return [2 /*return*/];
            }
        });
    });
}
function pickLatestRunDir(baseOutDir) {
    var runsRoot = path.join(baseOutDir, "ingestion_source=newsapi-ai");
    var names = fs
        .readdirSync(runsRoot, { withFileTypes: true })
        .filter(function (d) { return d.isDirectory() && d.name.startsWith("run_id="); })
        .map(function (d) { return d.name; })
        .sort();
    if (names.length === 0) {
        throw new Error("No run directories found under ".concat(runsRoot));
    }
    return path.join(runsRoot, names[names.length - 1]);
}
function copyCsvIntoTempTable(db, csvPath) {
    return __awaiter(this, void 0, void 0, function () {
        var copySql, stream;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0: return [4 /*yield*/, db.query("\n    CREATE TEMP TABLE tmp_newsapi_ai_load (\n      uri                 text,\n      url                 text,\n      title               text,\n      body                text,\n      date                text,\n      time                text,\n      date_time           text,\n      date_time_published text,\n      lang                text,\n      is_duplicate        text,\n      data_type           text,\n      sentiment           text,\n      event_uri           text,\n      relevance           text,\n      story_uri           text,\n      image               text,\n      source              text,\n      authors             text,\n      sim                 text,\n      wgt                 text,\n      categories          text,\n      concepts            text,\n      links               text,\n      videos              text,\n      shares              text,\n      duplicate_list      text,\n      extracted_dates     text,\n      location            text,\n      original_article    text,\n      raw_article         text\n    ) ON COMMIT DROP\n  ")];
                case 1:
                    _a.sent();
                    copySql = "\n    COPY tmp_newsapi_ai_load (\n      uri,\n      url,\n      title,\n      body,\n      date,\n      time,\n      date_time,\n      date_time_published,\n      lang,\n      is_duplicate,\n      data_type,\n      sentiment,\n      event_uri,\n      relevance,\n      story_uri,\n      image,\n      source,\n      authors,\n      sim,\n      wgt,\n      categories,\n      concepts,\n      links,\n      videos,\n      shares,\n      duplicate_list,\n      extracted_dates,\n      location,\n      original_article,\n      raw_article\n    )\n    FROM STDIN WITH (FORMAT csv, HEADER true)\n  ";
                    stream = db.query((0, pg_copy_streams_1.from)(copySql));
                    return [4 /*yield*/, (0, promises_1.pipeline)(fs.createReadStream(csvPath), stream)];
                case 2:
                    _a.sent();
                    return [2 /*return*/];
            }
        });
    });
}
function countArtifactRows(db) {
    return __awaiter(this, void 0, void 0, function () {
        var totalRes, attemptedRes;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0: return [4 /*yield*/, db.query("\n    SELECT COUNT(*)::int AS n\n    FROM tmp_newsapi_ai_load\n  ")];
                case 1:
                    totalRes = _a.sent();
                    return [4 /*yield*/, db.query("\n    SELECT COUNT(*)::int AS n\n    FROM tmp_newsapi_ai_load\n    WHERE NULLIF(uri, '') IS NOT NULL\n  ")];
                case 2:
                    attemptedRes = _a.sent();
                    return [2 /*return*/, {
                            rowsInArtifact: Number(totalRes.rows[0].n),
                            rowsAttempted: Number(attemptedRes.rows[0].n),
                        }];
            }
        });
    });
}
function collectArtifactDiagnostics(db) {
    return __awaiter(this, void 0, void 0, function () {
        var statsRes, sampleUrisRes, row;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0: return [4 /*yield*/, db.query("\n    SELECT\n      COUNT(*)::int AS rows_in_artifact,\n      MIN(NULLIF(date_time_published, '')::timestamptz) AS min_date_time_published,\n      MAX(NULLIF(date_time_published, '')::timestamptz) AS max_date_time_published,\n\n      COUNT(*) FILTER (WHERE NULLIF(source, '') IS NOT NULL AND NULLIF(source, '') <> 'null')::int AS source_count,\n      COUNT(*) FILTER (WHERE NULLIF(authors, '') IS NOT NULL AND NULLIF(authors, '') <> 'null')::int AS authors_count,\n      COUNT(*) FILTER (WHERE NULLIF(categories, '') IS NOT NULL AND NULLIF(categories, '') <> 'null')::int AS categories_count,\n      COUNT(*) FILTER (WHERE NULLIF(concepts, '') IS NOT NULL AND NULLIF(concepts, '') <> 'null')::int AS concepts_count,\n      COUNT(*) FILTER (WHERE NULLIF(links, '') IS NOT NULL AND NULLIF(links, '') <> 'null')::int AS links_count,\n      COUNT(*) FILTER (WHERE NULLIF(videos, '') IS NOT NULL AND NULLIF(videos, '') <> 'null')::int AS videos_count,\n      COUNT(*) FILTER (WHERE NULLIF(shares, '') IS NOT NULL AND NULLIF(shares, '') <> 'null')::int AS shares_count,\n      COUNT(*) FILTER (WHERE NULLIF(duplicate_list, '') IS NOT NULL AND NULLIF(duplicate_list, '') <> 'null')::int AS duplicate_list_count,\n      COUNT(*) FILTER (WHERE NULLIF(extracted_dates, '') IS NOT NULL AND NULLIF(extracted_dates, '') <> 'null')::int AS extracted_dates_count,\n      COUNT(*) FILTER (WHERE NULLIF(location, '') IS NOT NULL AND NULLIF(location, '') <> 'null')::int AS location_count,\n      COUNT(*) FILTER (WHERE NULLIF(original_article, '') IS NOT NULL AND NULLIF(original_article, '') <> 'null')::int AS original_article_count,\n      COUNT(*) FILTER (WHERE NULLIF(raw_article, '') IS NOT NULL AND NULLIF(raw_article, '') <> 'null')::int AS raw_article_count\n    FROM tmp_newsapi_ai_load\n  ")];
                case 1:
                    statsRes = _a.sent();
                    return [4 /*yield*/, db.query("\n    SELECT uri\n    FROM tmp_newsapi_ai_load\n    WHERE NULLIF(uri, '') IS NOT NULL\n    ORDER BY uri\n    LIMIT 3\n  ")];
                case 2:
                    sampleUrisRes = _a.sent();
                    row = statsRes.rows[0];
                    return [2 /*return*/, {
                            rowsInArtifact: Number(row.rows_in_artifact),
                            minDateTimePublished: row.min_date_time_published
                                ? new Date(row.min_date_time_published).toISOString()
                                : null,
                            maxDateTimePublished: row.max_date_time_published
                                ? new Date(row.max_date_time_published).toISOString()
                                : null,
                            populatedCounts: {
                                source: Number(row.source_count),
                                authors: Number(row.authors_count),
                                categories: Number(row.categories_count),
                                concepts: Number(row.concepts_count),
                                links: Number(row.links_count),
                                videos: Number(row.videos_count),
                                shares: Number(row.shares_count),
                                duplicate_list: Number(row.duplicate_list_count),
                                extracted_dates: Number(row.extracted_dates_count),
                                location: Number(row.location_count),
                                original_article: Number(row.original_article_count),
                                raw_article: Number(row.raw_article_count),
                            },
                            sampleUris: sampleUrisRes.rows.map(function (r) { return String(r.uri); }),
                        }];
            }
        });
    });
}
function bulkUpsertFromTemp(db, ingestionSource, runId, collectedAt) {
    return __awaiter(this, void 0, void 0, function () {
        var res;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0: return [4 /*yield*/, db.query("\n    WITH upserted AS (\n      INSERT INTO public.newsapi_articles (\n        uri,\n        url,\n        title,\n        body,\n        date,\n        time,\n        date_time,\n        date_time_published,\n        lang,\n        is_duplicate,\n        data_type,\n        sentiment,\n        event_uri,\n        relevance,\n        story_uri,\n        image,\n        source,\n        authors,\n        sim,\n        wgt,\n        categories,\n        concepts,\n        links,\n        videos,\n        shares,\n        duplicate_list,\n        extracted_dates,\n        location,\n        original_article,\n        raw_article,\n        ingestion_source,\n        run_id,\n        collected_at,\n        ingested_at\n      )\n      SELECT\n        NULLIF(uri, '') AS uri,\n        NULLIF(url, '') AS url,\n        NULLIF(title, '') AS title,\n        NULLIF(body, '') AS body,\n        NULLIF(date, '')::date AS date,\n        NULLIF(time, '')::time AS time,\n        NULLIF(date_time, '')::timestamptz AS date_time,\n        NULLIF(date_time_published, '')::timestamptz AS date_time_published,\n        NULLIF(lang, '') AS lang,\n        CASE\n          WHEN lower(NULLIF(is_duplicate, '')) = 'true' THEN true\n          WHEN lower(NULLIF(is_duplicate, '')) = 'false' THEN false\n          ELSE NULL\n        END AS is_duplicate,\n        NULLIF(data_type, '') AS data_type,\n        NULLIF(sentiment, '')::double precision AS sentiment,\n        NULLIF(event_uri, '') AS event_uri,\n        NULLIF(relevance, '')::integer AS relevance,\n        NULLIF(story_uri, '') AS story_uri,\n        NULLIF(image, '') AS image,\n        NULLIF(source, '')::jsonb AS source,\n        NULLIF(authors, '')::jsonb AS authors,\n        NULLIF(sim, '')::double precision AS sim,\n        NULLIF(wgt, '')::bigint AS wgt,\n        NULLIF(categories, '')::jsonb AS categories,\n        NULLIF(concepts, '')::jsonb AS concepts,\n        NULLIF(links, '')::jsonb AS links,\n        NULLIF(videos, '')::jsonb AS videos,\n        NULLIF(shares, '')::jsonb AS shares,\n        NULLIF(duplicate_list, '')::jsonb AS duplicate_list,\n        NULLIF(extracted_dates, '')::jsonb AS extracted_dates,\n        NULLIF(location, '')::jsonb AS location,\n        NULLIF(original_article, '')::jsonb AS original_article,\n        NULLIF(raw_article, '')::jsonb AS raw_article,\n        $1 AS ingestion_source,\n        $2::timestamptz AS run_id,\n        $3::timestamptz AS collected_at,\n        now() AS ingested_at\n      FROM tmp_newsapi_ai_load\n      WHERE NULLIF(uri, '') IS NOT NULL\n      ON CONFLICT (uri) DO UPDATE SET\n        url = EXCLUDED.url,\n        title = EXCLUDED.title,\n        body = EXCLUDED.body,\n        date = EXCLUDED.date,\n        time = EXCLUDED.time,\n        date_time = EXCLUDED.date_time,\n        date_time_published = EXCLUDED.date_time_published,\n        lang = EXCLUDED.lang,\n        is_duplicate = EXCLUDED.is_duplicate,\n        data_type = EXCLUDED.data_type,\n        sentiment = EXCLUDED.sentiment,\n        event_uri = EXCLUDED.event_uri,\n        relevance = EXCLUDED.relevance,\n        story_uri = EXCLUDED.story_uri,\n        image = EXCLUDED.image,\n        source = EXCLUDED.source,\n        authors = EXCLUDED.authors,\n        sim = EXCLUDED.sim,\n        wgt = EXCLUDED.wgt,\n        categories = EXCLUDED.categories,\n        concepts = EXCLUDED.concepts,\n        links = EXCLUDED.links,\n        videos = EXCLUDED.videos,\n        shares = EXCLUDED.shares,\n        duplicate_list = EXCLUDED.duplicate_list,\n        extracted_dates = EXCLUDED.extracted_dates,\n        location = EXCLUDED.location,\n        original_article = EXCLUDED.original_article,\n        raw_article = EXCLUDED.raw_article,\n        ingestion_source = EXCLUDED.ingestion_source,\n        run_id = EXCLUDED.run_id,\n        collected_at = EXCLUDED.collected_at,\n        updated_at = now()\n      WHERE\n        public.newsapi_articles.url IS DISTINCT FROM EXCLUDED.url OR\n        public.newsapi_articles.title IS DISTINCT FROM EXCLUDED.title OR\n        public.newsapi_articles.body IS DISTINCT FROM EXCLUDED.body OR\n        public.newsapi_articles.date IS DISTINCT FROM EXCLUDED.date OR\n        public.newsapi_articles.time IS DISTINCT FROM EXCLUDED.time OR\n        public.newsapi_articles.date_time IS DISTINCT FROM EXCLUDED.date_time OR\n        public.newsapi_articles.date_time_published IS DISTINCT FROM EXCLUDED.date_time_published OR\n        public.newsapi_articles.lang IS DISTINCT FROM EXCLUDED.lang OR\n        public.newsapi_articles.is_duplicate IS DISTINCT FROM EXCLUDED.is_duplicate OR\n        public.newsapi_articles.data_type IS DISTINCT FROM EXCLUDED.data_type OR\n        public.newsapi_articles.sentiment IS DISTINCT FROM EXCLUDED.sentiment OR\n        public.newsapi_articles.event_uri IS DISTINCT FROM EXCLUDED.event_uri OR\n        public.newsapi_articles.relevance IS DISTINCT FROM EXCLUDED.relevance OR\n        public.newsapi_articles.story_uri IS DISTINCT FROM EXCLUDED.story_uri OR\n        public.newsapi_articles.image IS DISTINCT FROM EXCLUDED.image OR\n        public.newsapi_articles.source IS DISTINCT FROM EXCLUDED.source OR\n        public.newsapi_articles.authors IS DISTINCT FROM EXCLUDED.authors OR\n        public.newsapi_articles.sim IS DISTINCT FROM EXCLUDED.sim OR\n        public.newsapi_articles.wgt IS DISTINCT FROM EXCLUDED.wgt OR\n        public.newsapi_articles.categories IS DISTINCT FROM EXCLUDED.categories OR\n        public.newsapi_articles.concepts IS DISTINCT FROM EXCLUDED.concepts OR\n        public.newsapi_articles.links IS DISTINCT FROM EXCLUDED.links OR\n        public.newsapi_articles.videos IS DISTINCT FROM EXCLUDED.videos OR\n        public.newsapi_articles.shares IS DISTINCT FROM EXCLUDED.shares OR\n        public.newsapi_articles.duplicate_list IS DISTINCT FROM EXCLUDED.duplicate_list OR\n        public.newsapi_articles.extracted_dates IS DISTINCT FROM EXCLUDED.extracted_dates OR\n        public.newsapi_articles.location IS DISTINCT FROM EXCLUDED.location OR\n        public.newsapi_articles.original_article IS DISTINCT FROM EXCLUDED.original_article OR\n        public.newsapi_articles.raw_article IS DISTINCT FROM EXCLUDED.raw_article OR\n        public.newsapi_articles.ingestion_source IS DISTINCT FROM EXCLUDED.ingestion_source OR\n        public.newsapi_articles.run_id IS DISTINCT FROM EXCLUDED.run_id OR\n        public.newsapi_articles.collected_at IS DISTINCT FROM EXCLUDED.collected_at\n      RETURNING (xmax = 0) AS inserted\n    )\n    SELECT\n      COUNT(*)::int AS rows_loaded,\n      COUNT(*) FILTER (WHERE inserted)::int AS db_rows_inserted,\n      COUNT(*) FILTER (WHERE NOT inserted)::int AS db_rows_updated\n    FROM upserted\n    ", [ingestionSource, runId, collectedAt])];
                case 1:
                    res = _a.sent();
                    return [2 /*return*/, {
                            rowsLoaded: Number(res.rows[0].rows_loaded),
                            dbRowsInserted: Number(res.rows[0].db_rows_inserted),
                            dbRowsUpdated: Number(res.rows[0].db_rows_updated),
                        }];
            }
        });
    });
}
function main() {
    return __awaiter(this, void 0, void 0, function () {
        var baseOutDir, runDir, manifestPath, csvPath, loadReportPath, manifest, expectedHeader, actualHeader, useSSL, db, loadStartedAtIso, startedMs, diagnostics, _a, rowsInArtifact, rowsAttempted, _b, rowsLoaded, dbRowsInserted, dbRowsUpdated, dbRowsUnchanged, loadCompletedAtIso, loadReport, e_1, _c, inner_1, _d;
        return __generator(this, function (_e) {
            switch (_e.label) {
                case 0:
                    baseOutDir = path.join(process.cwd(), "out");
                    runDir = pickLatestRunDir(baseOutDir);
                    manifestPath = path.join(runDir, "manifest.json");
                    csvPath = path.join(runDir, "articles.csv");
                    loadReportPath = path.join(runDir, "load_report.json");
                    console.log("Loading artifacts from:", runDir);
                    manifest = JSON.parse(fs.readFileSync(manifestPath, "utf8"));
                    if (manifest.artifact_contract_version !== newsapi_aiArtifactSchema_1.ARTIFACT_CONTRACT_VERSION) {
                        throw new Error("Artifact contract mismatch. Expected ".concat(newsapi_aiArtifactSchema_1.ARTIFACT_CONTRACT_VERSION, ", got ").concat(manifest.artifact_contract_version));
                    }
                    if (!manifest.collected_at) {
                        throw new Error("Manifest missing collected_at — collector incomplete");
                    }
                    console.log("manifest.run_id:", manifest.run_id);
                    console.log("manifest.ingestion_source:", manifest.ingestion_source);
                    console.log("manifest.artifact_contract_version:", manifest.artifact_contract_version);
                    expectedHeader = (0, newsapi_aiArtifactSchema_1.csvHeader)();
                    actualHeader = fs.readFileSync(csvPath, "utf8").split(/\r?\n/, 1)[0];
                    if (actualHeader !== expectedHeader) {
                        throw new Error("Unexpected CSV header.\nExpected: ".concat(expectedHeader, "\nActual:   ").concat(actualHeader));
                    }
                    useSSL = !DATABASE_URL.includes("localhost");
                    db = new pg_1.Client({
                        connectionString: DATABASE_URL,
                        ssl: useSSL ? { rejectUnauthorized: false } : false,
                    });
                    return [4 /*yield*/, db.connect()];
                case 1:
                    _e.sent();
                    loadStartedAtIso = new Date().toISOString();
                    startedMs = Date.now();
                    _e.label = 2;
                case 2:
                    _e.trys.push([2, 11, 19, 24]);
                    return [4 /*yield*/, db.query("BEGIN")];
                case 3:
                    _e.sent();
                    return [4 /*yield*/, setLoadStarted(db, manifest.run_id, manifest.ingestion_source, loadStartedAtIso)];
                case 4:
                    _e.sent();
                    return [4 /*yield*/, copyCsvIntoTempTable(db, csvPath)];
                case 5:
                    _e.sent();
                    return [4 /*yield*/, collectArtifactDiagnostics(db)];
                case 6:
                    diagnostics = _e.sent();
                    return [4 /*yield*/, countArtifactRows(db)];
                case 7:
                    _a = _e.sent(), rowsInArtifact = _a.rowsInArtifact, rowsAttempted = _a.rowsAttempted;
                    return [4 /*yield*/, bulkUpsertFromTemp(db, manifest.ingestion_source, manifest.run_id, manifest.collected_at)];
                case 8:
                    _b = _e.sent(), rowsLoaded = _b.rowsLoaded, dbRowsInserted = _b.dbRowsInserted, dbRowsUpdated = _b.dbRowsUpdated;
                    dbRowsUnchanged = rowsAttempted - rowsLoaded;
                    loadCompletedAtIso = new Date().toISOString();
                    return [4 /*yield*/, setLoadCompleted(db, manifest.run_id, manifest.ingestion_source, loadCompletedAtIso, rowsLoaded, dbRowsInserted, dbRowsUpdated)];
                case 9:
                    _e.sent();
                    return [4 /*yield*/, db.query("COMMIT")];
                case 10:
                    _e.sent();
                    loadReport = {
                        artifact_contract_version: manifest.artifact_contract_version,
                        ingestion_source: manifest.ingestion_source,
                        run_id: manifest.run_id,
                        artifacts_dir: runDir,
                        csv_header: expectedHeader,
                        load_started_at: loadStartedAtIso,
                        rows_in_artifact: rowsInArtifact,
                        rows_attempted: rowsAttempted,
                        rows_loaded: rowsLoaded,
                        db_rows_inserted: dbRowsInserted,
                        db_rows_updated: dbRowsUpdated,
                        db_rows_unchanged: dbRowsUnchanged,
                        min_date_time_published: diagnostics.minDateTimePublished,
                        max_date_time_published: diagnostics.maxDateTimePublished,
                        sample_uris: diagnostics.sampleUris,
                        populated_counts: diagnostics.populatedCounts,
                        load_completed_at: loadCompletedAtIso,
                        duration_ms: Date.now() - startedMs,
                        pipeline_status: "loaded",
                    };
                    fs.writeFileSync(loadReportPath, JSON.stringify(loadReport, null, 2), "utf8");
                    console.log("Load complete:", loadReportPath);
                    return [3 /*break*/, 24];
                case 11:
                    e_1 = _e.sent();
                    console.error(e_1);
                    _e.label = 12;
                case 12:
                    _e.trys.push([12, 14, , 15]);
                    return [4 /*yield*/, db.query("ROLLBACK")];
                case 13:
                    _e.sent();
                    return [3 /*break*/, 15];
                case 14:
                    _c = _e.sent();
                    return [3 /*break*/, 15];
                case 15:
                    _e.trys.push([15, 17, , 18]);
                    return [4 /*yield*/, markRunFailed(db, manifest.run_id, manifest.ingestion_source, "loader_error", (e_1 === null || e_1 === void 0 ? void 0 : e_1.message) ? String(e_1.message) : String(e_1))];
                case 16:
                    _e.sent();
                    return [3 /*break*/, 18];
                case 17:
                    inner_1 = _e.sent();
                    console.error("Also failed to mark pipeline_runs as failed:", inner_1);
                    return [3 /*break*/, 18];
                case 18:
                    process.exitCode = 1;
                    return [3 /*break*/, 24];
                case 19:
                    _e.trys.push([19, 21, , 22]);
                    return [4 /*yield*/, db.query("DROP TABLE IF EXISTS tmp_newsapi_ai_load")];
                case 20:
                    _e.sent();
                    return [3 /*break*/, 22];
                case 21:
                    _d = _e.sent();
                    return [3 /*break*/, 22];
                case 22: return [4 /*yield*/, db.end().catch(function () { })];
                case 23:
                    _e.sent();
                    return [7 /*endfinally*/];
                case 24: return [2 /*return*/];
            }
        });
    });
}
main().catch(function (e) {
    console.error(e);
    process.exit(1);
});
