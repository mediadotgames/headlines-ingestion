
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

```
EventRegistry API
        ↓
Collector Lambda
        ↓
S3 Artifacts
        ↓
Loader Lambda
        ↓
PostgreSQL Warehouse
```

### Collector Lambda

The collector Lambda performs the following tasks:

1. Determines the ingestion window based on Honolulu midnight
2. Fetches articles from EventRegistry using the REST API
3. Deduplicates by article URI
4. Writes ingestion artifacts to S3
5. Updates the `pipeline_runs` tracking table

Artifacts written to S3 include:

```
articles.jsonl
articles.csv
manifest.json
```

These artifacts represent the **contract** between the collector and loader.

---

### S3 Artifact Layout

Artifacts are stored using a deterministic prefix structure:

```
s3://<bucket>/<prefix>/
    ingestion_source=newsapi-ai/
        run_id=YYYY-MM-DDTHH-MM-SSZ/
            articles.csv
            articles.jsonl
            manifest.json
            load_report.json
```

This structure enables:

- deterministic replay
- debugging individual runs
- backfilling historical windows
- verifying ingestion integrity

---

# Loader Lambda

The loader Lambda is triggered when a `manifest.json` file appears in S3.

The loader performs:

1. Download artifact
2. Validate artifact contract version
3. Validate CSV header
4. COPY CSV into a temporary staging table
5. Perform transactional upsert into `newsapi_articles`
6. Produce a `load_report.json` diagnostic artifact

Key properties of the loader:

- Uses `ON COMMIT DROP` staging tables
- Executes within a single database transaction
- Supports idempotent replays
- Updates existing rows when enrichment fields change

---

# Artifact Contract

The artifact contract defines the schema shared between collector and loader.

Current version:

```
newsapi-ai/v2
```

Key columns include:

Core article fields:

```
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

Enrichment fields:

```
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

These fields are serialized as JSON in the artifact and loaded as JSONB in PostgreSQL.

---

# Database Schema

Primary table:

```
public.newsapi_articles
```

Primary key:

```
uri
```

Important metadata columns:

```
run_id
ingestion_source
collected_at
ingested_at
```

These fields allow tracking which ingestion run produced each article.

---

# Pipeline Runs Table

The table `pipeline_runs` tracks ingestion state.

Key columns:

```
run_id
ingestion_source
window_from
window_to
collected_at
load_started_at
load_completed_at
rows_loaded
db_rows_inserted
db_rows_updated
status
```

This enables:

- run monitoring
- failure detection
- debugging ingestion problems

---

# Time Window Model

The pipeline uses a deterministic time model based on **Honolulu midnight**.

This ensures that the entire U.S. news cycle is captured before a run executes.

Example schedule:

```
00:05 Pacific/Honolulu
```

Window controls:

```
LOOKBACK_DAYS
WINDOW_END_DAYS_AGO
```

These environment variables allow easy historical backfills.

---

# Enrichment Data Model

The enrichment fields convert articles into a structured knowledge graph.

### Concepts

Concepts represent normalized entities:

```
people
organizations
locations
topics
events
```

Example:

```
Donald Trump
United States Congress
Artificial Intelligence
Pentagon
```

Concepts enable semantic clustering and narrative analysis.

---

### Categories

Categories represent hierarchical topic classifications:

```
news/Politics
news/Technology
dmoz/Society/Law
```

These enable topic-level aggregation and trend analysis.

---

### Location

Location metadata identifies geographic context for events described in articles.

This enables geographic coverage analysis.

---

### Links

Links represent URLs referenced inside article bodies.

These can be used to build citation graphs between news articles.

---

# Clustering Foundations

The enrichment fields allow clustering articles into stories or topics.

Signals used for clustering include:

```
shared concepts
shared categories
event_uri
story_uri
publication timestamps
source relationships
```

Concept overlap is particularly powerful for clustering because articles discussing the same entities are often describing the same story.

---

# Analytical Use Cases

With the enriched dataset the system can support:

- topic trend analysis
- event clustering
- narrative tracking
- media coverage comparison
- coverage omission detection

Example questions the dataset can answer:

```
Which outlets covered a given political event?
Which outlets ignored a major story?
Which entities dominate the news cycle?
How does coverage differ across sources?
```

---

# Observability

Pipeline observability includes:

- CloudWatch logs for both Lambdas
- `pipeline_runs` status tracking
- `load_report.json` diagnostic artifacts
- artifact contract validation

This design ensures that ingestion failures are detectable and debuggable.

---

# Future Extensions

Planned enhancements include:

- normalized concept tables for faster clustering queries
- article similarity indexes
- event-level clustering services
- public-interest classification models
- coverage heatmap visualizations

These capabilities build on the structured enrichment data already present in the pipeline.

---

# Summary

The MDG News Pipeline transforms raw news articles into a structured dataset suitable for large-scale media analysis.

Through enrichment, artifact-based ingestion, and transactional loading, the system provides a reliable foundation for analyzing news narratives, detecting coverage patterns, and clustering related stories across multiple media outlets.
