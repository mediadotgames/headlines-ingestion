# MDG News Pipeline – Architecture & Data Model

## Overview

The MDG News Pipeline ingests news articles from the EventRegistry / NewsAPI.ai platform, enriches them with structured metadata, and stores them in PostgreSQL for analysis, visualization, and clustering.

The system is designed to support:

- Media coverage analysis
- Event and story clustering
- Topic trend analysis
- Public-interest scoring
- Coverage and omission detection across news outlets

The pipeline emphasizes **reproducibility**, **auditability**, and **artifact-based ingestion**.

---

# System Architecture

EventRegistry API ↓ Collector (Lambda or Local) ↓ Artifacts (S3 or Local Disk) ↓ Loader (Lambda or Local) ↓ PostgreSQL Warehouse

The pipeline is intentionally split into **two stages**:

| Stage | Purpose |
|------|------|
| Collector | Fetches articles and produces artifacts |
| Loader | Loads artifacts into PostgreSQL |

This architecture enables:

- deterministic replays
- debugging ingestion runs
- artifact inspection
- backfilling historical data safely

---

# Collector

The collector performs the following tasks:

1. Computes the ingestion window based on **Honolulu midnight**
2. Determines the logical `run_id`
3. Reads the environment variable `RUN_TYPE`
4. Determines the next `nth_run`
5. Inserts a row into `pipeline_runs`
6. Fetches articles from EventRegistry
7. Deduplicates by `uri`
8. Writes ingestion artifacts

Artifacts produced:

articles.jsonl articles.csv manifest.json

These artifacts form the **contract between collector and loader**.

---

# Run Tracking Model

The pipeline supports multiple executions for the same logical run window.

Two fields define run identity:

run_type nth_run

### run_type

Allowed values:

scheduled backfill seed

### nth_run

Represents the execution count for the same logical run.

Example:

run_id = 2026-03-09T10:00:00Z

scheduled nth_run=1 scheduled nth_run=2 backfill nth_run=1

---

# Pipeline Runs Table

Table:

public.pipeline_runs

Primary key:

(run_id, ingestion_source, run_type, nth_run)

Important columns:

run_id ingestion_source run_type nth_run window_from window_to collected_at load_started_at load_completed_at rows_loaded db_rows_inserted db_rows_updated status

This allows the system to:

- track retries
- distinguish backfills
- audit ingestion history
- diagnose failures

---

# Articles Table

Primary table:

public.newsapi_articles

Primary key:

uri

Important metadata columns:

run_id ingestion_source run_type nth_run collected_at ingested_at

These fields allow every article row to be traced to the exact pipeline execution that produced or last updated it.

---

# Artifact Contract

Current contract version:

newsapi-ai/v3

This contract defines the shared schema between collector and loader.

Key metadata fields:

run_id run_type nth_run ingestion_source collected_at

Core article fields include:

uri url title body date_time date_time_published lang sentiment event_uri story_uri source authors

Enrichment fields include:

categories concepts links videos shares duplicate_list extracted_dates location original_article raw_article

These fields are stored as JSON in artifacts and loaded into PostgreSQL JSONB columns.

---

# Artifact Storage Layout

Artifacts are written to deterministic paths.

ingestion_source=newsapi-ai/ run_id=YYYY-MM-DDTHH-MM-SSZ/ run_type=scheduled|backfill|seed/ nth_run=N/ articles.jsonl articles.csv manifest.json load_report.json

Example:

s3://bucket/prefix/ ingestion_source=newsapi-ai/ run_id=2026-03-09T10-00-00Z/ run_type=scheduled/ nth_run=1/ manifest.json

Benefits:

- prevents artifact overwrites
- allows multiple executions
- supports deterministic replay
- enables debugging of individual runs

---

# Loader

The loader is triggered when a `manifest.json` file appears.

Loader responsibilities:

1. Download artifact files
2. Validate artifact contract version
3. Validate CSV header
4. Read `run_id`, `run_type`, and `nth_run`
5. COPY CSV into a temporary staging table
6. Perform transactional UPSERT into `newsapi_articles`
7. Update the exact `pipeline_runs` row
8. Produce `load_report.json`

Loader guarantees:

- transactional safety
- idempotent loads
- deterministic artifact replay
- detailed ingestion diagnostics

---

# Time Window Model

The pipeline uses **Honolulu midnight** as the canonical boundary.

This ensures the full U.S. news cycle is complete before ingestion runs.

Example schedule:

00:05 Pacific/Honolulu

Environment variables:

LOOKBACK_DAYS WINDOW_END_DAYS_AGO RUN_TYPE

These variables allow:

- historical backfills
- seed runs
- scheduled ingestion

---

# Enrichment Data Model

Articles are enriched with structured metadata.

## Concepts

Concepts represent normalized entities such as:

people organizations locations topics events

Examples:

Donald Trump Pentagon Artificial Intelligence United States Congress

Concepts enable semantic clustering.

---

## Categories

Categories represent hierarchical topics.

Examples:

news/Politics news/Technology dmoz/Society/Law

These enable topic-level aggregation.

---

## Location

Location metadata represents geographic context.

Used for:

- regional news coverage analysis
- geographic clustering
- international media comparison

---

# Clustering Foundations

The enrichment data enables article clustering using signals such as:

shared concepts shared categories event_uri story_uri publication timestamps source relationships

Concept overlap is particularly useful for detecting stories covered by multiple outlets.

---

# Future Extensions

Planned enhancements include:

- normalized concept tables
- article similarity indexing
- event-level clustering
- public-interest scoring
- media coverage heatmaps

These capabilities build on the structured enrichment model already present in the pipeline.

