# MDG News Pipeline – Architecture & Data Model

## Overview

The MDG News Pipeline ingests articles from EventRegistry / NewsAPI.ai, writes deterministic artifacts, and loads those artifacts into PostgreSQL for downstream analysis, clustering, and dashboarding.

The design goals are:

- reproducibility
- auditability
- deterministic replay
- batch safety
- analytical query performance

The warehouse keeps the raw upstream JSON payloads on article rows while also projecting selected dimensions into normalized relational tables.

---

# System Architecture

```text
EventRegistry API
  ↓
Collector (Lambda or Local)
  ↓
Artifacts (S3 or Local Disk)
  ↓
Loader (Lambda or Local)
  ↓
PostgreSQL Warehouse
```

The pipeline is intentionally split into two stages:

| Stage | Purpose |
| --- | --- |
| Collector | Fetches upstream data and produces immutable artifacts |
| Loader | Validates artifacts and writes warehouse tables transactionally |

This split enables:
- deterministic replays
- backfills without re-calling the API
- artifact inspection
- cleaner operational debugging

---

# Collector

The collector performs the following tasks:

1. Computes the ingestion window
2. Determines the logical `run_id`
3. Reads `RUN_TYPE`
4. Determines the next `nth_run`
5. Inserts a row into `public.pipeline_runs`
6. Fetches articles from EventRegistry / NewsAPI.ai
7. Deduplicates by `uri`
8. Writes artifacts

Artifacts produced per run:

```text
articles.jsonl
articles.csv
manifest.json
```

These artifacts form the contract between collector and loader.

---

# Run Tracking Model

The pipeline supports multiple executions for the same logical run window.

Two fields define run identity in addition to `run_id` and `ingestion_source`:

- `run_type`
- `nth_run`

## run_type

Allowed values:

```text
scheduled
backfill
seed
```

## nth_run

Represents the execution count for the same logical run.

Example:

```text
run_id = 2026-03-09T10:00:00Z

scheduled / nth_run=1
scheduled / nth_run=2
backfill  / nth_run=1
```

---

# Warehouse Tables

## 1. `public.pipeline_runs`

Tracks collector and loader lifecycle for each exact pipeline execution.

Primary key:

```sql
(run_id, ingestion_source, run_type, nth_run)
```

Important columns include:

```text
run_id
ingestion_source
run_type
nth_run
window_from
window_to
collected_at
load_started_at
load_completed_at
rows_loaded
db_rows_inserted
db_rows_updated
status
error_code
error_message
```

## 2. `public.newsapi_articles`

Primary fact table for ingested articles.

Primary key:

```sql
uri
```

The article row stores:

- scalar article fields
- run metadata
- raw upstream JSONB payloads (`source`, `authors`, `categories`, `concepts`, etc.)
- `source_uri`, which points to the canonical source dimension

## 3. `public.newsapi_sources`

Canonical source dimension extracted from `newsapi_articles.source`.

## 4. `public.newsapi_concepts`

Canonical concept dimension extracted from `newsapi_articles.concepts`.

## 5. `public.newsapi_article_concepts`

Many-to-many bridge between articles and concepts.

## 6. `public.newsapi_categories`

Canonical category dimension extracted from `newsapi_articles.categories`.

## 7. `public.newsapi_article_categories`

Many-to-many bridge between articles and categories.

---

# Artifact Contract

Current contract version:

```text
newsapi-ai/v3
```

The artifact contract still carries the raw upstream fields as JSON strings in CSV / JSONL artifacts.

Key metadata fields:

```text
run_id
run_type
nth_run
ingestion_source
collected_at
```

Core article fields include:

```text
uri
url
title
body
date_time
date_time_published
lang
sentiment
event_uri
story_uri
source
authors
```

Enrichment fields include:

```text
categories
concepts
links
videos
shares
duplicate_list
extracted_dates
location
original_article
raw_article
```

The collector does not need to emit separate normalized-dimension artifacts. The loader derives normalized tables from the same raw artifact payload.

---

# Artifact Storage Layout

Artifacts are written to deterministic paths.

```text
ingestion_source=newsapi-ai/
  run_id=YYYY-MM-DDTHH-MM-SSZ/
    run_type=scheduled|backfill|seed/
      nth_run=N/
        articles.jsonl
        articles.csv
        manifest.json
        load_report.json
        load_reports/<invocation>.json
```

Benefits:

- prevents accidental overwrite
- supports multiple executions for the same logical run
- preserves replayability
- isolates run-level debugging

---

# Loader

The loader is triggered when a `manifest.json` file appears.

Loader responsibilities:

1. Download artifact files
2. Validate artifact contract version
3. Validate CSV header
4. Read `run_id`, `run_type`, and `nth_run`
5. `COPY` CSV into a temporary staging table
6. UPSERT `public.newsapi_articles`
7. Sync normalized dimensions from the temporary staging table
8. Update the exact `public.pipeline_runs` row
9. Write `load_report.json`

Loader guarantees:

- transactional safety
- idempotent artifact replay
- advisory-lock protection against duplicate concurrent loads
- deterministic article upserts
- normalized-dimension synchronization from the same raw artifact payload

---

# Normalized Dimension Sync Model

The loader keeps the raw JSONB payloads on `public.newsapi_articles`, but also projects selected dimensions into dedicated relational tables.

## Source sync

From staged article `source` JSON:

- UPSERT `public.newsapi_sources`
- set `public.newsapi_articles.source_uri`

## Concept sync

From staged article `concepts` JSON:

- UPSERT canonical `public.newsapi_concepts`
- replace article-level mappings in `public.newsapi_article_concepts`

## Category sync

From staged article `categories` JSON:

- UPSERT canonical `public.newsapi_categories`
- replace article-level mappings in `public.newsapi_article_categories`

This design keeps article rows debuggable while making source, concept, and category queries much faster and cleaner.

---

# Why Keep JSONB and Normalized Tables Together

Keeping both models is intentional.

## Raw JSONB on articles provides:

- exact upstream snapshot retention
- debugging and auditability
- compatibility with the artifact contract
- flexibility for rarely queried fields

## Normalized tables provide:

- faster joins
- simpler relational queries
- easier indexing
- better source/entity/category analytics

This hybrid model is the current recommended warehouse design for the MDG news pipeline.

---

# Category Hierarchy Note

`public.newsapi_categories.parent_uri` is stored as an upstream taxonomy reference string.

It is not currently enforced as a self-referencing foreign key because the upstream taxonomy does not always materialize every ancestor node. The hierarchy can still be traversed logically using `parent_uri`, but the warehouse does not yet require a fully closed taxonomy tree.

---

# Time Window Model

The pipeline uses a canonical daily ingestion boundary and environment-controlled backfill windowing.

Important environment variables include:

```text
LOOKBACK_DAYS
WINDOW_END_DAYS_AGO
RUN_TYPE
```

These support:

- scheduled daily ingestion
- seed runs
- controlled historical backfills

---

# Clustering Foundations

The warehouse supports downstream clustering and analysis using signals such as:

- article text and metadata
- story / event identifiers
- concepts
- categories
- location
- source / outlet
- timestamps

The normalized concept and category tables are especially important for topic clustering, faceting, anomaly detection, and source-comparison analysis.

---

# Query Patterns Enabled by the Current Model

Examples now supported cleanly:

- recent articles by source domain
- all articles linked to a concept URI
- top categories by article count
- source-level coverage comparisons
- concept faceting across runs or windows
- category-based clustering features
