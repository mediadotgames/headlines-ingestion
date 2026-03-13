# News Pipeline Normalization Summary

## What we accomplished

We upgraded the pipeline from a raw-ingestion-only model to a hybrid model:

- `newsapi_articles` still preserves raw JSONB for `source`, `concepts`, and `categories`
- normalized relational tables now exist for:
  - `newsapi_sources`
  - `newsapi_concepts`
  - `newsapi_categories`
  - `newsapi_article_concepts`
  - `newsapi_article_categories`
- historical data was backfilled into the normalized tables
- enforceable foreign keys were added
- loaders were updated so all new runs populate the normalized tables automatically
- `pipeline_run_metrics` was added for run-level observability of normalized outputs

## Why this matters

### 1. Relationships are now first-class relational data

Before, concepts and categories were trapped inside article-level JSON arrays.

Now, article↔concept and article↔category relationships are explicit tables. This makes joins, aggregations, filtering, and faceting much easier and faster.

### 2. Analytics queries are simpler and faster

Questions about concept coverage, category usage, source behavior, and article relationships now use indexed joins instead of JSON expansion logic.

### 3. Raw truth was preserved

We kept the raw JSONB on `newsapi_articles`, which means we still have:

- original provider payloads
- reprocessing flexibility
- debugging support
- the ability to compare raw vs normalized outputs

### 4. Canonical dimensions now exist

We now have one canonical row per:

- source
- concept
- category

This reduces repetition and creates a clean foundation for future enrichment like source metadata, category quality tiers, and concept cleanup.

### 5. Run-level validation is now possible

With `pipeline_run_metrics`, we can track normalized outputs per run, including article counts, concept links, category links, and distinct dimensions touched.

## Core tables

### `newsapi_articles`
- **ID:** `uri`
- **Purpose:** core article fact table
- **Relationships:** many-to-one to `newsapi_sources`, many-to-many to `newsapi_concepts` and `newsapi_categories`

### `newsapi_sources`
- **ID:** `uri`
- **Purpose:** canonical upstream source dimension
- **Relationships:** one-to-many to `newsapi_articles`

### `newsapi_concepts`
- **ID:** `uri`
- **Purpose:** canonical concept/entity dimension
- **Relationships:** many-to-many to `newsapi_articles`

### `newsapi_categories`
- **ID:** `uri`
- **Purpose:** canonical category dimension
- **Relationships:** many-to-many to `newsapi_articles`; logical hierarchy via `parent_uri`

### `newsapi_article_concepts`
- **ID:** `(article_uri, concept_uri)`
- **Purpose:** article↔concept relationship table

### `newsapi_article_categories`
- **ID:** `(article_uri, category_uri)`
- **Purpose:** article↔category relationship table

### `pipeline_run_metrics`
- **ID:** `(run_id, ingestion_source, run_type, nth_run, metric_name)`
- **Purpose:** per-run observability for normalized pipeline outputs

## Practical benefits

- better clustering inputs from concept frequency, co-occurrence, and overlap
- better outlet analysis through normalized source relationships
- better anomaly detection around sparse enrichment, category behavior, and source shifts
- better dashboards and monitoring built on normalized joins instead of JSON extraction

## What we learned about the data

- concepts look relatively strong and useful
- `news/...` categories look reasonable as broad topical labels
- many `dmoz/...` categories appear noisy or over-attached

This suggests:

- concepts should likely be the stronger semantic feature
- categories should probably be quality-tiered before they influence important downstream logic

## Overall significance

This work turned the pipeline from a raw article ingestion store into:

- a raw ingestion store
- a relational analytics model
- a run-level observability system

That is foundational infrastructure work that improves clustering, dashboards, anomaly detection, outlet analysis, feature engineering, and public-interest evaluation.
