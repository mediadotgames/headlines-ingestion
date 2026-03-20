-- ============================================================================
-- Ingestion Cycle Switch Analysis: 24h → 6h
-- Run against your PostgreSQL warehouse to analyze the last 5 days of
-- article ingestion and identify any gaps after switching to 6h cycles.
--
-- Context: Sub-daily cycle code merged 2026-03-19 ~01:17 EDT (PR #20).
--          Lambda deployment with CYCLE_HOURS=6 happened shortly after.
-- ============================================================================

-- ─────────────────────────────────────────────────────────────────────────────
-- 1. PIPELINE RUNS OVERVIEW (last 5 days)
--    Shows every pipeline run, its window, and article counts.
-- ─────────────────────────────────────────────────────────────────────────────
SELECT '=== 1. PIPELINE RUNS (last 5 days) ===' AS section;

SELECT
    run_id,
    run_type,
    nth_run,
    status,
    window_from,
    window_to,
    EXTRACT(EPOCH FROM (window_to - window_from)) / 3600 AS window_hours,
    articles_fetched,
    articles_deduped,
    rows_loaded,
    db_rows_inserted,
    db_rows_updated,
    collected_at,
    load_completed_at
FROM public.pipeline_runs
WHERE ingestion_source = 'newsapi-ai'
  AND run_id >= now() - interval '5 days'
ORDER BY run_id ASC;

-- ─────────────────────────────────────────────────────────────────────────────
-- 2. IDENTIFY THE CYCLE SWITCH POINT
--    The first run with a window < 24 hours is the first 6h cycle run.
-- ─────────────────────────────────────────────────────────────────────────────
SELECT '=== 2. CYCLE SWITCH DETECTION ===' AS section;

SELECT
    run_id,
    window_from,
    window_to,
    EXTRACT(EPOCH FROM (window_to - window_from)) / 3600 AS window_hours,
    articles_deduped,
    CASE
        WHEN EXTRACT(EPOCH FROM (window_to - window_from)) / 3600 <= 14 THEN '6h-cycle (12h window)'
        WHEN EXTRACT(EPOCH FROM (window_to - window_from)) / 3600 <= 26 THEN '24h-cycle'
        ELSE 'backfill/other'
    END AS cycle_type
FROM public.pipeline_runs
WHERE ingestion_source = 'newsapi-ai'
  AND run_type = 'scheduled'
  AND run_id >= now() - interval '5 days'
ORDER BY run_id ASC;

-- ─────────────────────────────────────────────────────────────────────────────
-- 3. DAILY ARTICLE COUNTS (by date_time, the monotonic EventRegistry timestamp)
--    Compare daily volumes before and after the switch.
-- ─────────────────────────────────────────────────────────────────────────────
SELECT '=== 3. DAILY ARTICLE COUNTS (by EventRegistry date_time) ===' AS section;

SELECT
    date_time::date AS er_date,
    COUNT(*) AS total_articles,
    COUNT(DISTINCT uri) AS unique_articles,
    MIN(date_time) AS earliest_article,
    MAX(date_time) AS latest_article
FROM public.newsapi_articles
WHERE date_time >= now() - interval '7 days'
  AND ingestion_source = 'newsapi-ai'
GROUP BY date_time::date
ORDER BY er_date ASC;

-- ─────────────────────────────────────────────────────────────────────────────
-- 4. HOURLY ARTICLE DISTRIBUTION (last 5 days)
--    Helps spot gaps in specific hour blocks.
-- ─────────────────────────────────────────────────────────────────────────────
SELECT '=== 4. HOURLY ARTICLE DISTRIBUTION (last 5 days) ===' AS section;

SELECT
    date_trunc('hour', date_time) AS hour_bucket,
    COUNT(*) AS article_count
FROM public.newsapi_articles
WHERE date_time >= now() - interval '5 days'
  AND ingestion_source = 'newsapi-ai'
GROUP BY date_trunc('hour', date_time)
ORDER BY hour_bucket ASC;

-- ─────────────────────────────────────────────────────────────────────────────
-- 5. 6-HOUR BLOCK ANALYSIS (aligned to cycle boundaries)
--    Groups articles into 6h blocks (00:00, 06:00, 12:00, 18:00 UTC)
--    to match the new cycle schedule.
-- ─────────────────────────────────────────────────────────────────────────────
SELECT '=== 5. SIX-HOUR BLOCK ANALYSIS (UTC cycle boundaries) ===' AS section;

SELECT
    date_trunc('day', date_time) + (FLOOR(EXTRACT(HOUR FROM date_time) / 6) * interval '6 hours') AS cycle_block_utc,
    COUNT(*) AS article_count,
    COUNT(DISTINCT uri) AS unique_articles,
    MIN(date_time) AS earliest,
    MAX(date_time) AS latest
FROM public.newsapi_articles
WHERE date_time >= now() - interval '5 days'
  AND ingestion_source = 'newsapi-ai'
GROUP BY date_trunc('day', date_time) + (FLOOR(EXTRACT(HOUR FROM date_time) / 6) * interval '6 hours')
ORDER BY cycle_block_utc ASC;

-- ─────────────────────────────────────────────────────────────────────────────
-- 6. GAP DETECTION: Check for missing 6h blocks
--    Generates all expected 6h blocks and LEFT JOINs to find empties.
-- ─────────────────────────────────────────────────────────────────────────────
SELECT '=== 6. GAP DETECTION (missing 6h blocks) ===' AS section;

WITH expected_blocks AS (
    SELECT generate_series(
        date_trunc('day', now() - interval '5 days'),
        now(),
        interval '6 hours'
    ) AS block_start
),
actual_blocks AS (
    SELECT
        date_trunc('day', date_time) + (FLOOR(EXTRACT(HOUR FROM date_time) / 6) * interval '6 hours') AS block_start,
        COUNT(*) AS article_count
    FROM public.newsapi_articles
    WHERE date_time >= now() - interval '5 days'
      AND ingestion_source = 'newsapi-ai'
    GROUP BY 1
)
SELECT
    e.block_start,
    COALESCE(a.article_count, 0) AS article_count,
    CASE WHEN a.article_count IS NULL THEN '*** GAP ***' ELSE 'OK' END AS status
FROM expected_blocks e
LEFT JOIN actual_blocks a ON e.block_start = a.block_start
WHERE e.block_start < now()  -- don't flag future blocks
ORDER BY e.block_start ASC;

-- ─────────────────────────────────────────────────────────────────────────────
-- 7. PIPELINE RUNS vs ARTICLES CROSS-CHECK
--    Matches pipeline_runs to actual article counts per run_id.
-- ─────────────────────────────────────────────────────────────────────────────
SELECT '=== 7. PIPELINE RUNS vs ARTICLES CROSS-CHECK ===' AS section;

SELECT
    pr.run_id,
    pr.run_type,
    pr.status,
    EXTRACT(EPOCH FROM (pr.window_to - pr.window_from)) / 3600 AS window_hours,
    pr.articles_deduped AS run_reported_deduped,
    pr.rows_loaded AS run_reported_loaded,
    COALESCE(art.actual_articles, 0) AS actual_articles_in_db,
    COALESCE(art.actual_articles, 0) - pr.rows_loaded AS load_discrepancy
FROM public.pipeline_runs pr
LEFT JOIN (
    SELECT run_id, COUNT(*) AS actual_articles
    FROM public.newsapi_articles
    WHERE ingestion_source = 'newsapi-ai'
      AND run_id >= now() - interval '5 days'
    GROUP BY run_id
) art ON pr.run_id = art.run_id
WHERE pr.ingestion_source = 'newsapi-ai'
  AND pr.run_id >= now() - interval '5 days'
ORDER BY pr.run_id ASC;

-- ─────────────────────────────────────────────────────────────────────────────
-- 8. BEFORE vs AFTER COMPARISON
--    Compares daily averages from the 24h era (prior 7 days) vs 6h era.
-- ─────────────────────────────────────────────────────────────────────────────
SELECT '=== 8. BEFORE vs AFTER DAILY AVERAGES ===' AS section;

WITH switch_point AS (
    -- First run with sub-daily window (< 14h indicates 6h cycle with 12h lookback)
    SELECT MIN(run_id) AS switched_at
    FROM public.pipeline_runs
    WHERE ingestion_source = 'newsapi-ai'
      AND run_type = 'scheduled'
      AND EXTRACT(EPOCH FROM (window_to - window_from)) / 3600 < 14
      AND run_id >= now() - interval '7 days'
),
before_switch AS (
    SELECT
        date_time::date AS d,
        COUNT(*) AS daily_count
    FROM public.newsapi_articles a
    CROSS JOIN switch_point sp
    WHERE a.ingestion_source = 'newsapi-ai'
      AND a.date_time >= (sp.switched_at - interval '7 days')
      AND a.date_time < sp.switched_at
    GROUP BY date_time::date
),
after_switch AS (
    SELECT
        date_time::date AS d,
        COUNT(*) AS daily_count
    FROM public.newsapi_articles a
    CROSS JOIN switch_point sp
    WHERE a.ingestion_source = 'newsapi-ai'
      AND a.date_time >= sp.switched_at
    GROUP BY date_time::date
)
SELECT 'BEFORE (24h cycles)' AS period,
       COUNT(*) AS num_days,
       SUM(daily_count) AS total_articles,
       ROUND(AVG(daily_count)) AS avg_per_day,
       MIN(daily_count) AS min_day,
       MAX(daily_count) AS max_day
FROM before_switch
UNION ALL
SELECT 'AFTER (6h cycles)' AS period,
       COUNT(*) AS num_days,
       SUM(daily_count) AS total_articles,
       ROUND(AVG(daily_count)) AS avg_per_day,
       MIN(daily_count) AS min_day,
       MAX(daily_count) AS max_day
FROM after_switch;

-- Also show the day-by-day breakdown
SELECT '=== 8b. DAY-BY-DAY with cycle era label ===' AS section;

WITH switch_point AS (
    SELECT MIN(run_id) AS switched_at
    FROM public.pipeline_runs
    WHERE ingestion_source = 'newsapi-ai'
      AND run_type = 'scheduled'
      AND EXTRACT(EPOCH FROM (window_to - window_from)) / 3600 < 14
      AND run_id >= now() - interval '7 days'
)
SELECT
    a.date_time::date AS er_date,
    COUNT(*) AS article_count,
    CASE
        WHEN a.date_time < sp.switched_at THEN '24h-cycle'
        ELSE '6h-cycle'
    END AS era
FROM public.newsapi_articles a
CROSS JOIN switch_point sp
WHERE a.ingestion_source = 'newsapi-ai'
  AND a.date_time >= now() - interval '10 days'
GROUP BY a.date_time::date, CASE WHEN a.date_time < sp.switched_at THEN '24h-cycle' ELSE '6h-cycle' END
ORDER BY er_date ASC;

-- ─────────────────────────────────────────────────────────────────────────────
-- 9. WINDOW COVERAGE CONTINUITY
--    Checks that consecutive pipeline runs have overlapping or abutting windows
--    (no temporal gaps between runs).
-- ─────────────────────────────────────────────────────────────────────────────
SELECT '=== 9. WINDOW COVERAGE CONTINUITY ===' AS section;

WITH ordered_runs AS (
    SELECT
        run_id,
        window_from,
        window_to,
        LAG(window_to) OVER (ORDER BY run_id) AS prev_window_to,
        LAG(run_id) OVER (ORDER BY run_id) AS prev_run_id
    FROM public.pipeline_runs
    WHERE ingestion_source = 'newsapi-ai'
      AND run_type = 'scheduled'
      AND status IN ('collected', 'loaded')
      AND run_id >= now() - interval '5 days'
)
SELECT
    prev_run_id,
    run_id,
    prev_window_to,
    window_from,
    CASE
        WHEN window_from <= prev_window_to THEN 'OVERLAP/OK'
        ELSE 'GAP: ' || EXTRACT(EPOCH FROM (window_from - prev_window_to)) / 3600 || ' hours'
    END AS continuity
FROM ordered_runs
WHERE prev_run_id IS NOT NULL
ORDER BY run_id ASC;

-- ─────────────────────────────────────────────────────────────────────────────
-- 10. FAILED/INCOMPLETE RUNS
--     Any runs that didn't complete successfully could explain missing articles.
-- ─────────────────────────────────────────────────────────────────────────────
SELECT '=== 10. FAILED/INCOMPLETE RUNS ===' AS section;

SELECT
    run_id,
    run_type,
    nth_run,
    status,
    error_code,
    error_message,
    window_from,
    window_to,
    articles_fetched,
    articles_deduped,
    rows_loaded
FROM public.pipeline_runs
WHERE ingestion_source = 'newsapi-ai'
  AND run_id >= now() - interval '5 days'
  AND status NOT IN ('loaded')
ORDER BY run_id ASC;
