# Data Schema Reference – MDG News Pipeline

## Overview

This document describes the database schema used by the MDG News Pipeline. The primary purpose of the schema is to store enriched news articles along with ingestion metadata so the dataset can support:

* topic clustering
* event analysis
* coverage comparisons
* narrative tracking
* public-interest scoring

The two most important tables are:

* `public.newsapi_articles`
* `public.pipeline_runs`

---

# Table: public.newsapi_articles

This table stores every article ingested from EventRegistry.

## Primary Key

```
uri
```

This is the canonical EventRegistry article identifier.

---

# Core Article Fields

| Column              | Type             | Description                                        |
| ------------------- | ---------------- | -------------------------------------------------- |
| uri                 | text             | Unique article identifier from EventRegistry       |
| url                 | text             | Article URL                                        |
| title               | text             | Article headline                                   |
| body                | text             | Full article body text                             |
| date                | date             | Article publication date                           |
| time                | time             | Article publication time                           |
| date_time           | timestamptz      | Datetime when EventRegistry identified the article |
| date_time_published | timestamptz      | Datetime from article metadata                     |
| lang                | text             | Article language                                   |
| data_type           | text             | Article type (`news`, `blog`, `pr`)                |
| sentiment           | double precision | Sentiment score (-1 to 1)                          |
| image               | text             | Primary article image URL                          |

---

# Event & Story Fields

| Column    | Type             | Description                            |
| --------- | ---------------- | -------------------------------------- |
| event_uri | text             | EventRegistry event identifier         |
| story_uri | text             | EventRegistry story cluster identifier |
| relevance | integer          | EventRegistry relevance score          |
| sim       | double precision | Similarity to story centroid           |
| wgt       | bigint           | Internal EventRegistry weight value    |

These fields enable event-level clustering and story grouping.

---

# Source Metadata

| Column  | Type  | Description             |
| ------- | ----- | ----------------------- |
| source  | jsonb | Source metadata object  |
| authors | jsonb | List of article authors |

Example `source` JSON:

```json
{
  "uri": "reuters.com",
  "title": "Reuters",
  "description": "...",
  "location": {...},
  "ranking": {...}
}
```

---

# Enrichment Fields

These fields were added to support clustering and deeper analysis.

| Column           | Type  | Description                                |
| ---------------- | ----- | ------------------------------------------ |
| categories       | jsonb | Topic categories assigned to the article   |
| concepts         | jsonb | Detected entities and topics               |
| links            | jsonb | URLs referenced inside the article         |
| videos           | jsonb | Videos extracted from the article          |
| shares           | jsonb | Social media share counts                  |
| duplicate_list   | jsonb | URIs of duplicate articles                 |
| extracted_dates  | jsonb | Dates detected in article text             |
| location         | jsonb | Geographic location extracted from article |
| original_article | jsonb | Original article metadata if duplicate     |
| raw_article      | jsonb | Full original EventRegistry payload        |

These enrichment fields are the foundation for:

* semantic clustering
* event detection
* entity analysis
* geographic analysis

---

# Ingestion Metadata

| Column           | Type        | Description                          |
| ---------------- | ----------- | ------------------------------------ |
| ingestion_source | text        | Source of ingestion (`newsapi-ai`)   |
| run_id           | timestamptz | Identifier for the ingestion run     |
| collected_at     | timestamptz | Time collector retrieved the article |
| ingested_at      | timestamptz | Time loader inserted the row         |
| created_at       | timestamptz | Row creation timestamp               |
| updated_at       | timestamptz | Row update timestamp                 |

These columns allow articles to be traced back to the exact pipeline run that produced them.

---

# Table: public.pipeline_runs

This table tracks the lifecycle of ingestion runs.

## Primary Key

```
(run_id, ingestion_source)
```

---

# Run Metadata

| Column           | Type        | Description                         |
| ---------------- | ----------- | ----------------------------------- |
| run_id           | timestamptz | Identifier for the ingestion window |
| ingestion_source | text        | Pipeline source name                |
| window_from      | timestamptz | Beginning of ingestion window       |
| window_to        | timestamptz | End of ingestion window             |

---

# Collector Metadata

| Column           | Type        | Description                     |
| ---------------- | ----------- | ------------------------------- |
| collected_at     | timestamptz | Time collector completed        |
| articles_fetched | integer     | Raw articles retrieved from API |
| articles_deduped | integer     | Unique articles retained        |

---

# Loader Metadata

| Column            | Type        | Description                  |
| ----------------- | ----------- | ---------------------------- |
| load_started_at   | timestamptz | Time loader began processing |
| load_completed_at | timestamptz | Time loader finished         |
| rows_loaded       | integer     | Rows written during load     |
| db_rows_inserted  | integer     | Newly inserted rows          |
| db_rows_updated   | integer     | Existing rows updated        |

---

# Status Fields

| Column        | Type | Description                                             |
| ------------- | ---- | ------------------------------------------------------- |
| status        | text | Run status (`started`, `collected`, `loaded`, `failed`) |
| error_code    | text | Error classification                                    |
| error_message | text | Error message from pipeline                             |

These fields allow monitoring systems to detect failures and track ingestion health.

---

# Key Relationships

```
pipeline_runs
      ↓
newsapi_articles
      ↓
concepts / categories / locations
```

Each ingestion run generates many articles, and each article contains nested enrichment data.

---

# Example Useful Queries

## Articles per ingestion run

```sql
SELECT run_id, COUNT(*)
FROM public.newsapi_articles
GROUP BY run_id
ORDER BY run_id DESC;
```

---

## Top news sources

```sql
SELECT
  source->>'title' AS source,
  COUNT(*) AS articles
FROM public.newsapi_articles
GROUP BY source
ORDER BY articles DESC;
```

---

## Most frequent concepts

```sql
SELECT
  concept->'label'->>'eng' AS concept,
  COUNT(*) AS articles
FROM public.newsapi_articles,
LATERAL jsonb_array_elements(concepts) concept
GROUP BY concept
ORDER BY articles DESC
LIMIT 25;
```

---

# Summary

The MDG schema combines:

* structured relational metadata
* nested enrichment JSON
* ingestion run tracking

This hybrid structure allows the dataset to support both traditional SQL analysis and more advanced semantic clustering techniques.
