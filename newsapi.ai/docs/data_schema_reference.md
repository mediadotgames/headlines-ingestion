# MDG Data Schema Reference

This document describes the current PostgreSQL schema for the MDG news ingestion pipeline.

The current model keeps the raw upstream EventRegistry / NewsAPI.ai payloads on `public.newsapi_articles` as JSONB, while also projecting high-value dimensions into normalized relational tables for faster querying and cleaner joins.

---

# Table: public.newsapi_articles

This is the primary warehouse table for ingested NewsAPI.ai articles.

## Primary Key

```sql
uri text primary key
```

Each row represents one canonical article identified by the upstream article URI.

## Core Article Fields

| Column | Type | Description |
| --- | --- | --- |
| uri | text | Stable upstream article identifier |
| url | text | Canonical article URL |
| title | text | Article headline |
| body | text | Article body text |
| date | date | Article publication date |
| time | time | Article publication time |
| date_time | timestamptz | Parsed article timestamp |
| date_time_published | timestamptz | Upstream published timestamp |
| lang | text | Language code |
| is_duplicate | boolean | Duplicate flag from upstream |
| data_type | text | Upstream article type (`news`, `blog`, `pr`) |
| sentiment | double precision | Upstream sentiment score |
| event_uri | text | Upstream event identifier |
| relevance | integer | Upstream relevance score |
| story_uri | text | Upstream story identifier |
| image | text | Article image URL |
| sim | double precision | Upstream similarity metric |
| wgt | bigint | Upstream weight |

## Raw Upstream JSONB Fields

These are preserved on the article row as the raw upstream snapshot.

| Column | Type | Description |
| --- | --- | --- |
| source | jsonb | Upstream source object |
| authors | jsonb | Upstream authors array/object |
| categories | jsonb | Upstream category array |
| concepts | jsonb | Upstream concept array |
| links | jsonb | URLs referenced inside the article |
| videos | jsonb | Videos extracted from the article |
| shares | jsonb | Social media share counts |
| duplicate_list | jsonb | URIs of duplicate articles |
| extracted_dates | jsonb | Dates detected in article text |
| location | jsonb | Geographic location extracted from article |
| original_article | jsonb | Original article metadata if duplicate |
| raw_article | jsonb | Full original EventRegistry payload |

These JSONB columns are retained for:
- auditability
- replay/debugging
- raw payload inspection
- compatibility with the artifact contract

## Relational Linkage Fields

| Column | Type | Description |
| --- | --- | --- |
| source_uri | text | Canonical source reference to `public.newsapi_sources.uri` |
| ingestion_source | text | Source of ingestion (`newsapi-ai`) |
| run_id | timestamptz | Logical ingestion run identifier |
| run_type | text | Execution mode (`scheduled`, `backfill`, `seed`) |
| nth_run | integer | Retry / execution ordinal for the same logical run |
| collected_at | timestamptz | Time collector retrieved the article |
| ingested_at | timestamptz | Time loader inserted the row |
| created_at | timestamptz | Row creation timestamp |
| updated_at | timestamptz | Row update timestamp |

---

# Table: public.newsapi_sources

Canonical source dimension for the upstream `source` object.

## Primary Key

```sql
uri text primary key
```

## Columns

| Column | Type | Description |
| --- | --- | --- |
| uri | text | Source URI/domain |
| title | text | Source display name |
| description | text | Upstream source description |
| social_media | jsonb | Social account metadata |
| ranking | jsonb | Upstream ranking metadata |
| location | jsonb | Upstream source geography |
| image | text | Source image/logo |
| thumb_image | text | Small source image/logo |
| created_at | timestamptz | Row creation timestamp |
| updated_at | timestamptz | Row update timestamp |

## Relationships

- one source → many `public.newsapi_articles`
- referenced by `public.newsapi_articles.source_uri`

---

# Table: public.newsapi_concepts

Canonical concept dimension extracted from article enrichment.

## Primary Key

```sql
uri text primary key
```

## Columns

| Column | Type | Description |
| --- | --- | --- |
| uri | text | Concept URI |
| type | text | Concept type (`person`, `loc`, `org`, `wiki`) |
| image | text | Concept image |
| label | jsonb | Localized labels |
| location | jsonb | Location metadata for geographic concepts |
| synonyms | jsonb | Localized synonyms |
| created_at | timestamptz | Row creation timestamp |
| updated_at | timestamptz | Row update timestamp |

## Relationships

- many concepts ↔ many `public.newsapi_articles`
- linked through `public.newsapi_article_concepts`

---

# Table: public.newsapi_article_concepts

Junction table between articles and canonical concepts.

## Primary Key

```sql
(article_uri, concept_uri)
```

## Columns

| Column | Type | Description |
| --- | --- | --- |
| article_uri | text | FK to `public.newsapi_articles.uri` |
| concept_uri | text | FK to `public.newsapi_concepts.uri` |
| created_at | timestamptz | Row creation timestamp |

---

# Table: public.newsapi_categories

Canonical category dimension extracted from article enrichment.

## Primary Key

```sql
uri text primary key
```

## Columns

| Column | Type | Description |
| --- | --- | --- |
| uri | text | Category URI |
| parent_uri | text | Upstream parent category URI |
| label | text | Category label |
| created_at | timestamptz | Row creation timestamp |
| updated_at | timestamptz | Row update timestamp |

## Relationships

- many categories ↔ many `public.newsapi_articles`
- linked through `public.newsapi_article_categories`

`parent_uri` is stored as a taxonomy reference but is not currently enforced as a self-referencing foreign key, because the upstream taxonomy does not reliably materialize every parent node.

---

# Table: public.newsapi_article_categories

Junction table between articles and canonical categories.

## Primary Key

```sql
(article_uri, category_uri)
```

## Columns

| Column | Type | Description |
| --- | --- | --- |
| article_uri | text | FK to `public.newsapi_articles.uri` |
| category_uri | text | FK to `public.newsapi_categories.uri` |
| created_at | timestamptz | Row creation timestamp |

---

# Table: public.pipeline_runs

Tracks the lifecycle of ingestion runs.

## Primary Key

```sql
(run_id, ingestion_source, run_type, nth_run)
```

## Columns

| Column | Type | Description |
| --- | --- | --- |
| run_id | timestamptz | Logical ingestion window identifier |
| ingestion_source | text | Pipeline source name |
| run_type | text | Execution mode |
| nth_run | integer | Retry / execution ordinal |
| window_from | timestamptz | Beginning of ingestion window |
| window_to | timestamptz | End of ingestion window |
| collected_at | timestamptz | Time collector completed |
| articles_fetched | integer | Raw articles retrieved from API |
| articles_deduped | integer | Unique articles retained |
| load_started_at | timestamptz | Time loader began processing |
| load_completed_at | timestamptz | Time loader finished |
| rows_loaded | integer | Rows inserted or updated in `newsapi_articles` |
| db_rows_inserted | integer | New article rows inserted |
| db_rows_updated | integer | Existing article rows updated |
| status | text | Run status (`started`, `collected`, `loaded`, `failed`) |
| error_code | text | Error classification |
| error_message | text | Error message from pipeline |
| created_at | timestamptz | Row creation timestamp |
| updated_at | timestamptz | Row update timestamp |

---

# Table: public.events

Canonical event dimension populated by the event ingestion pipeline. Each row represents one upstream EventRegistry event, identified by its event URI.

## Primary Key

```sql
uri text primary key
```

## Columns

| Column | Type | Nullable | Default | Description |
| --- | --- | --- | --- | --- |
| uri | text | not null | | Stable upstream event identifier |
| total_article_count | integer | | | Total articles associated with the event upstream |
| relevance | integer | | | Upstream relevance score |
| event_date | date | | | Date the event occurred |
| sentiment | double precision | | | Upstream sentiment score |
| social_score | double precision | | | Upstream social media score |
| article_counts | jsonb | | | Per-language article count breakdown |
| title | jsonb | | | Localized event titles |
| summary | jsonb | | | Localized event summaries |
| concepts | jsonb | | | Concepts associated with the event |
| categories | jsonb | | | Categories associated with the event |
| common_dates | jsonb | | | Frequently mentioned dates in the event |
| location | jsonb | | | Geographic location of the event |
| stories | jsonb | | | Story clusters within the event |
| images | jsonb | | | Images associated with the event |
| wgt | bigint | | | Upstream weight |
| raw_event | jsonb | not null | | Full original EventRegistry event payload |
| first_collected_at | timestamptz | not null | `now()` | First time this event was fetched |
| last_collected_at | timestamptz | not null | `now()` | Most recent time this event was fetched |
| created_at | timestamptz | not null | `now()` | Row creation timestamp |
| updated_at | timestamptz | not null | `now()` | Row update timestamp |

## Indexes

| Name | Definition |
| --- | --- |
| `events_pkey` | PRIMARY KEY btree (`uri`) |
| `idx_events_event_date` | btree (`event_date` DESC) |
| `idx_events_last_collected_at` | btree (`last_collected_at` DESC) |
| `idx_events_relevance` | btree (`relevance` DESC) |
| `idx_events_social_score` | btree (`social_score` DESC) |
| `idx_events_total_article_count` | btree (`total_article_count` DESC) |

## Triggers

- `set_updated_at` — BEFORE UPDATE, executes `tg_set_updated_at()`

## Relationships

- many events ↔ many `public.newsapi_articles` via `newsapi_articles.event_uri = events.uri`

---

# Table: public.pipeline_run_metrics

Per-stage metrics for pipeline runs. Each row is one metric measurement for a specific pipeline stage within a run.

## Primary Key

```sql
(run_id, ingestion_source, run_type, nth_run, stage, metric_name)
```

## Columns

| Column | Type | Nullable | Default | Description |
| --- | --- | --- | --- | --- |
| run_id | timestamptz | not null | | Logical ingestion run identifier |
| ingestion_source | text | not null | | Pipeline source name |
| run_type | text | not null | | Execution mode (`scheduled`, `backfill`, `seed`) |
| nth_run | integer | not null | | Retry / execution ordinal |
| stage | text | not null | `'article_load'` | Pipeline stage that produced the metric |
| metric_name | text | not null | | Name of the metric |
| metric_value | bigint | not null | | Numeric value of the metric |
| created_at | timestamptz | not null | `now()` | Row creation timestamp |

## Indexes

| Name | Definition |
| --- | --- |
| `pipeline_run_metrics_pkey` | PRIMARY KEY btree (`run_id`, `ingestion_source`, `run_type`, `nth_run`, `stage`, `metric_name`) |
| `idx_pipeline_run_metrics_metric_name` | btree (`metric_name`) |
| `idx_pipeline_run_metrics_run_lookup` | btree (`run_id` DESC, `ingestion_source`, `run_type`, `nth_run`, `stage`) |
| `idx_pipeline_run_metrics_stage_metric` | btree (`stage`, `metric_name`) |

## Check Constraints

| Name | Expression |
| --- | --- |
| `pipeline_run_metrics_nth_run_check` | `nth_run >= 1` |
| `pipeline_run_metrics_run_type_check` | `run_type IN ('scheduled', 'backfill', 'seed')` |
| `pipeline_run_metrics_stage_check` | `stage IN ('article_collect', 'article_load', 'event_collect', 'event_load')` |

---

# Key Relationships

```text
pipeline_runs
      ↓
newsapi_articles
      ├── source_uri → newsapi_sources
      ├── event_uri → events
      ├── article_concepts → newsapi_concepts
      └── article_categories → newsapi_categories

pipeline_run_metrics
      └── (run_id, ingestion_source, run_type, nth_run) → pipeline_runs
```

Each pipeline run produces many article rows. Articles retain the raw JSONB enrichment payloads while also syncing the canonical source, concept, and category dimensions. The event pipeline discovers event URIs from loaded articles and populates the `events` table. Pipeline run metrics track per-stage measurements across both the article and event pipelines.

---

# Example Useful Queries

## Articles per ingestion run

```sql
SELECT run_id, run_type, nth_run, COUNT(*)
FROM public.newsapi_articles
GROUP BY run_id, run_type, nth_run
ORDER BY run_id DESC, nth_run DESC;
```

## Top sources using normalized source dimension

```sql
SELECT
  s.title,
  a.source_uri,
  COUNT(*) AS articles
FROM public.newsapi_articles a
LEFT JOIN public.newsapi_sources s
  ON s.uri = a.source_uri
GROUP BY s.title, a.source_uri
ORDER BY articles DESC
LIMIT 25;
```

## Recent articles for a concept

```sql
SELECT a.uri, a.title, a.date_time_published
FROM public.newsapi_articles a
JOIN public.newsapi_article_concepts ac
  ON ac.article_uri = a.uri
WHERE ac.concept_uri = 'http://en.wikipedia.org/wiki/United_States'
ORDER BY a.date_time_published DESC
LIMIT 50;
```

## Top categories by article count

```sql
SELECT c.uri, c.label, COUNT(*) AS article_count
FROM public.newsapi_article_categories ac
JOIN public.newsapi_categories c
  ON c.uri = ac.category_uri
GROUP BY c.uri, c.label
ORDER BY article_count DESC
LIMIT 25;
```
