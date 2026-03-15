# MDG Data Schema Reference

Single source of truth for the PostgreSQL schema powering the MDG news pipeline. Covers ingestion, enrichment, analytics, and operational tables.

The architecture retains raw upstream EventRegistry / NewsAPI.ai payloads as JSONB on `newsapi_articles`, while projecting high-value dimensions into normalized relational tables. Enrichment stages (validation, clustering, public interest assessment) produce additional derived tables downstream.

---

## Table of Contents

1. [Ingestion Pipeline Tables](#ingestion-pipeline-tables)
   - [newsapi_articles](#table-publicnewsapi_articles)
   - [newsapi_sources](#table-publicnewsapi_sources)
   - [newsapi_concepts](#table-publicnewsapi_concepts)
   - [newsapi_categories](#table-publicnewsapi_categories)
   - [newsapi_article_concepts](#table-publicnewsapi_article_concepts)
   - [newsapi_article_categories](#table-publicnewsapi_article_categories)
   - [events](#table-publicevents)
2. [Enrichment Pipeline Tables](#enrichment-pipeline-tables)
   - [validation_outputs](#table-publicvalidation_outputs)
   - [topics](#table-publictopics)
   - [headline_topic_assignments](#table-publicheadline_topic_assignments)
   - [public_interest_assessments](#table-publicpublic_interest_assessments)
   - [scope_classifications](#table-publicscope_classifications)
   - [enrichment_pipeline_runs](#table-publicenrichment_pipeline_runs)
3. [Analytics Tables](#analytics-tables)
   - [outlet_bias_scores](#table-publicoutlet_bias_scores)
   - [enriched_headlines](#table-publicenriched_headlines)
4. [Analytics Views](#analytics-views)
   - [v_heatmap](#view-v_heatmap)
   - [v_heatmap_asymmetry](#view-v_heatmap_asymmetry)
5. [Operational Tables](#operational-tables)
   - [pipeline_runs](#table-publicpipeline_runs)
   - [pipeline_run_metrics](#table-publicpipeline_run_metrics)
   - [anomalies](#table-publicanomalies)
   - [outlets](#table-publicoutlets)
6. [PI Evaluation Tables](#pi-evaluation-tables)
   - [pi_prompt_versions](#table-publicpi_prompt_versions)
   - [pi_eval_runs](#table-publicpi_eval_runs)
   - [pi_eval_results](#table-publicpi_eval_results)
   - [pi_benchmark_labels](#table-publicpi_benchmark_labels)
7. [Legacy / Audit Tables](#legacy--audit-tables)
   - [headlines](#table-publicheadlines)
   - [topic_assignment_audit](#table-publictopic_assignment_audit)
8. [Entity Relationships](#entity-relationships)
9. [Example Queries](#example-queries)

---

# Ingestion Pipeline Tables

## Table: public.newsapi_articles

Primary warehouse table for ingested NewsAPI.ai articles. Each row represents one canonical article identified by its upstream article URI.

**Domain concept**: A **Story** — a single news article from NewsAPI.ai/EventRegistry.

### Primary Key

```sql
uri text primary key
```

### Core Article Fields

| Column | Type | Description |
| --- | --- | --- |
| uri | text | Stable upstream article identifier |
| url | text | Canonical article URL |
| title | text | Article headline |
| body | text | Article body text |
| date | date | Date when EventRegistry serialized the article (monotonically increasing; **not** the actual publish date) |
| time | time | Time (UTC) when EventRegistry serialized the article |
| date_time | timestamptz | Combined `date`+`time`; when EventRegistry serialized the article (monotonically increasing) |
| date_time_published | timestamptz | When the article was first discovered in RSS feeds; closer to the actual publication time but **not** monotonically increasing |
| lang | text | Language code |
| is_duplicate | boolean | Duplicate flag from upstream (unreliable — do not use for dedup logic) |
| data_type | text | Upstream article type (`news`, `blog`, `pr`) |
| sentiment | double precision | Upstream sentiment score |
| event_uri | text | Upstream event identifier |
| relevance | integer | Query-match quality score from upstream |
| story_uri | text | Upstream story identifier |
| image | text | Article image URL |
| sim | double precision | Cosine similarity to the assigned event centroid |
| wgt | bigint | Internal upstream sorting parameter; not for analytical use |

### Raw Upstream JSONB Fields

Preserved on the article row as the raw upstream snapshot for auditability, replay/debugging, and raw payload inspection.

| Column | Type | Description |
| --- | --- | --- |
| source | jsonb | Upstream source object (`{"uri": "bbc.com", "title": "BBC", ...}`) |
| authors | jsonb | Upstream authors array/object |
| categories | jsonb | Upstream category array (`[{"uri": "news/Politics", "wgt": 100}, ...]`) |
| concepts | jsonb | Upstream concept array (`[{"uri": "...", "label": {"eng": "..."}, "type": "..."}, ...]`) |
| links | jsonb | URLs referenced inside the article |
| videos | jsonb | Videos extracted from the article |
| shares | jsonb | Social platform share counts (facebook, googlePlus, pinterest, linkedIn) |
| duplicate_list | jsonb | URIs of duplicate articles |
| extracted_dates | jsonb | Dates detected in article text |
| location | jsonb | Geographic location extracted from article |
| original_article | jsonb | Original article metadata if duplicate |
| raw_article | jsonb | Full original EventRegistry payload |

> **Note**: Prefer normalized joins (`newsapi_sources`, `newsapi_concepts`, `newsapi_categories`) over JSONB extraction for analytics queries.

### Relational Linkage Fields

| Column | Type | Description |
| --- | --- | --- |
| source_uri | text | FK to `newsapi_sources.uri` |
| ingestion_source | text | Source of ingestion (`newsapi-ai`) |
| run_id | timestamptz | Logical ingestion run identifier |
| run_type | text | Execution mode (`scheduled`, `backfill`, `seed`) |
| nth_run | integer | Retry / execution ordinal for the same logical run |
| collected_at | timestamptz | Time collector retrieved the article |
| ingested_at | timestamptz | Time loader inserted the row |
| created_at | timestamptz | Row creation timestamp |
| updated_at | timestamptz | Row update timestamp |

### Relationships

- Story → belongs_to → Topic (via `headline_topic_assignments`)
- Story → has → ValidationOutput (via `validation_outputs`)
- Story → has → PublicInterestAssessment (via `public_interest_assessments`)
- Story → produced_by → NewsAPISource (via `source_uri` FK to `newsapi_sources`)
- Story → linked_to → Event (via `event_uri` FK to `events`)
- Story ↔ NewsAPIConcept (many-to-many via `newsapi_article_concepts`)
- Story ↔ NewsAPICategory (many-to-many via `newsapi_article_categories`)

---

## Table: public.newsapi_sources

Canonical source dimension. One row per upstream publisher.

### Primary Key

```sql
uri text primary key
```

### Columns

| Column | Type | Description |
| --- | --- | --- |
| uri | text | Source URI/domain (e.g. `bbc.com`) |
| title | text | Source display name (e.g. `BBC`) |
| description | text | Upstream source description |
| social_media | jsonb | Social account metadata |
| ranking | jsonb | Upstream ranking metadata |
| location | jsonb | Upstream source geography |
| image | text | Source image/logo |
| thumb_image | text | Small source image/logo |
| created_at | timestamptz | Row creation timestamp |
| updated_at | timestamptz | Row update timestamp |

### Relationships

- one source → many `newsapi_articles` (via `newsapi_articles.source_uri`)

---

## Table: public.newsapi_concepts

Canonical concept/entity dimension extracted from article enrichment. Covers people, organizations, locations, and wiki topics.

### Primary Key

```sql
uri text primary key
```

### Columns

| Column | Type | Description |
| --- | --- | --- |
| uri | text | Concept URI (e.g. wiki URL) |
| type | text | Concept type (`person`, `loc`, `org`, `wiki`) |
| label | jsonb | Localized labels |
| image | text | Concept image |
| location | jsonb | Location metadata for geographic concepts |
| synonyms | jsonb | Localized synonyms |
| created_at | timestamptz | Row creation timestamp |
| updated_at | timestamptz | Row update timestamp |

### Relationships

- many concepts ↔ many articles (via `newsapi_article_concepts`)

---

## Table: public.newsapi_categories

Canonical category dimension with hierarchical structure.

### Primary Key

```sql
uri text primary key
```

### Columns

| Column | Type | Description |
| --- | --- | --- |
| uri | text | Category URI (e.g. `news/Politics`, `dmoz/Society`) |
| parent_uri | text | Upstream parent category URI (not FK-enforced — upstream taxonomy is incomplete) |
| label | text | Category display label |
| created_at | timestamptz | Row creation timestamp |
| updated_at | timestamptz | Row update timestamp |

### Relationships

- many categories ↔ many articles (via `newsapi_article_categories`)

> **Data quality note**: `news/*` categories are reasonable broad topical labels. `dmoz/*` categories are noisy and over-attached — quality-tier before use in downstream logic.

---

## Table: public.newsapi_article_concepts

Junction table between articles and canonical concepts.

### Primary Key

```sql
(article_uri, concept_uri)
```

### Columns

| Column | Type | Description |
| --- | --- | --- |
| article_uri | text | FK to `newsapi_articles.uri` |
| concept_uri | text | FK to `newsapi_concepts.uri` |
| created_at | timestamptz | Row creation timestamp |

---

## Table: public.newsapi_article_categories

Junction table between articles and canonical categories.

### Primary Key

```sql
(article_uri, category_uri)
```

### Columns

| Column | Type | Description |
| --- | --- | --- |
| article_uri | text | FK to `newsapi_articles.uri` |
| category_uri | text | FK to `newsapi_categories.uri` |
| created_at | timestamptz | Row creation timestamp |

---

## Table: public.events

Canonical event dimension populated by the event ingestion pipeline. Each row represents one upstream EventRegistry event.

### Primary Key

```sql
uri text primary key
```

### Columns

| Column | Type | Nullable | Default | Description |
| --- | --- | --- | --- | --- |
| uri | text | not null | | Stable upstream event identifier |
| total_article_count | integer | | | Total articles associated with the event upstream |
| relevance | integer | | | Upstream relevance score |
| event_date | date | | | Date the event occurred |
| sentiment | double precision | | | Upstream sentiment score |
| social_score | double precision | | | Aggregate social media engagement metric |
| article_counts | jsonb | | | Per-language article count breakdown |
| title | jsonb | | | Localized event titles |
| summary | jsonb | | | Localized event summaries |
| concepts | jsonb | | | Concepts associated with the event |
| categories | jsonb | | | Categories associated with the event |
| common_dates | jsonb | | | Frequently mentioned dates in the event |
| location | jsonb | | | Geographic location of the event |
| stories | jsonb | | | Story clusters within the event |
| images | jsonb | | | Images associated with the event |
| wgt | bigint | | | Internal upstream sorting parameter; not for analytical use |
| raw_event | jsonb | not null | | Full original EventRegistry event payload |
| first_collected_at | timestamptz | not null | `now()` | First time this event was fetched |
| last_collected_at | timestamptz | not null | `now()` | Most recent time this event was fetched |
| created_at | timestamptz | not null | `now()` | Row creation timestamp |
| updated_at | timestamptz | not null | `now()` | Row update timestamp |

### Indexes

| Name | Definition |
| --- | --- |
| `events_pkey` | PRIMARY KEY btree (`uri`) |
| `idx_events_event_date` | btree (`event_date` DESC) |
| `idx_events_last_collected_at` | btree (`last_collected_at` DESC) |
| `idx_events_relevance` | btree (`relevance` DESC) |
| `idx_events_social_score` | btree (`social_score` DESC) |
| `idx_events_total_article_count` | btree (`total_article_count` DESC) |

### Triggers

- `set_updated_at` — BEFORE UPDATE, executes `tg_set_updated_at()`

### Relationships

- many events ↔ many `newsapi_articles` via `newsapi_articles.event_uri = events.uri`

---

# Enrichment Pipeline Tables

## Table: public.validation_outputs

Enrichment output from the validation stage. One row per story that has been validated.

### Primary Key

```sql
story_id text primary key
```

### Columns

| Column | Type | Description |
| --- | --- | --- |
| story_id | text | FK to `newsapi_articles.uri` |
| headline_clean | text | Normalized headline text |
| snippet_clean | text | Normalized snippet/description text |
| story_text_clean | text | Normalized body text |
| snippet_valid | boolean | Whether the snippet passes quality checks |
| body_valid | boolean | Passes both text cleanliness AND semantic consistency (headline-body cosine sim >= 0.3) |
| body_headline_similarity | real | Cosine similarity between headline and body[:500] embeddings |
| embedding | vector(384) | 384-dim vector (BAAI/bge-small-en-v1.5) |
| top_category | text | One of: `sports`, `politics`, `business`, `entertainment`, `technology`, `health`, `environment`, `science`, `general` |
| concept_labels | jsonb | Normalized concept labels array |
| category_labels | jsonb | Category URI paths array |
| named_entities | jsonb | spaCy NER entities array |
| dedup_status | text | `keep` or `duplicate` |
| dedup_reason | text | NULL or comma-separated rule names |
| dedup_kept_story_id | text | URI of the kept story if this is a duplicate |
| validation_version | text | Schema version (currently `v2.0`) |
| created_at | timestamptz | Row creation timestamp |
| updated_at | timestamptz | Row update timestamp |

---

## Table: public.topics

Represents a cluster of related stories.

### Primary Key

```sql
topic_id uuid primary key
```

### Columns

| Column | Type | Description |
| --- | --- | --- |
| topic_id | uuid | Unique cluster identifier |
| label | text | Human-readable label (medoid headline) |
| centroid | vector(384) | Cluster centroid embedding vector |
| story_count | integer | Number of stories in cluster |
| dominant_category | text | Most common `top_category` in cluster |
| label_source_story_id | text | Story URI used as the label source |
| medoid_id | text | Medoid story URI |
| cluster_version | text | Clustering algorithm version |
| earliest_published_at | timestamptz | Earliest publication time in cluster |
| latest_published_at | timestamptz | Latest publication time in cluster |
| concept_counts | jsonb | Concept frequency distribution (default `{}`) |
| category_counts | jsonb | Category frequency distribution (default `{}`) |
| entity_counts | jsonb | Entity frequency distribution (default `{}`) |
| created_at | timestamptz | Row creation timestamp |

### Relationships

- Topic → contains → Stories (via `headline_topic_assignments`)

---

## Table: public.headline_topic_assignments

Links stories to their assigned topic clusters.

### Columns

| Column | Type | Description |
| --- | --- | --- |
| story_id | text | FK to `newsapi_articles.uri` |
| topic_id | uuid | FK to `topics.topic_id` |
| composite_score | real | Overall assignment quality score |
| similarity_to_centroid | real | Cosine similarity to topic centroid |
| assignment_method | text | Algorithm used for assignment |
| pipeline_run_id | uuid | FK to enrichment pipeline run |
| assigned_at | timestamptz | When the assignment was made |

---

## Table: public.public_interest_assessments

LLM-based public interest evaluation result per story.

### Primary Key

```sql
story_id text primary key
```

### Columns

| Column | Type | Description |
| --- | --- | --- |
| story_id | text | FK to `newsapi_articles.uri` |
| is_public_interest | boolean | Boolean gate result |
| label | text | `High`, `Moderate`, `Low`, or `Not Public Interest` |
| met_count | integer | Number of criteria met (0–4) |
| confidence | real | Confidence score |
| material_impact | boolean | Material impact criterion |
| institutional_action | boolean | Institutional action criterion |
| scope_scale | boolean | Scope/scale criterion |
| new_information | boolean | New information criterion |
| assessment_json | jsonb | Full assessment payload (prompt version, model, raw response, reasoning) |
| evaluated_at | timestamptz | Evaluation timestamp |
| pipeline_run_id | uuid | FK to enrichment pipeline run |

---

## Table: public.scope_classifications

Scope gating for stories — determines whether a story is in-scope for downstream consumption.

### Primary Key

```sql
story_id text primary key
```

### Columns

| Column | Type | Description |
| --- | --- | --- |
| story_id | text | FK to `newsapi_articles.uri` |
| scope_status | text | `IN_SCOPE` or `OUT_OF_SCOPE` |

---

## Table: public.enrichment_pipeline_runs

Tracks enrichment pipeline execution runs. Each row represents one end-to-end enrichment pass over a batch of stories.

### Primary Key

```sql
run_id uuid primary key default gen_random_uuid()
```

### Columns

| Column | Type | Nullable | Default | Description |
| --- | --- | --- | --- | --- |
| run_id | uuid | not null | `gen_random_uuid()` | Unique run identifier |
| started_at | timestamptz | not null | `now()` | When the run began |
| finished_at | timestamptz | | | When the run completed (null if still running) |
| status | text | not null | `'running'` | Run status (`running`, `completed`, `failed`) |
| stories_selected | integer | not null | `0` | Stories picked for enrichment |
| stories_validated | integer | not null | `0` | Stories that passed validation stage |
| stories_clustered | integer | not null | `0` | Stories assigned to topic clusters |
| stories_judged | integer | not null | `0` | Stories evaluated for public interest |
| stories_scoped | integer | not null | `0` | Stories that received scope classification |
| error_count | integer | not null | `0` | Total errors encountered during run |
| error_details | jsonb | | | Structured error information |
| created_at | timestamptz | not null | `now()` | Row creation timestamp |

### Indexes

| Name | Definition |
| --- | --- |
| `enrichment_pipeline_runs_pkey` | PRIMARY KEY btree (`run_id`) |
| `idx_epr_started_at` | btree (`started_at` DESC) |
| `idx_epr_status` | btree (`status`) |

---

# Analytics Tables

## Table: public.outlet_bias_scores

Outlet-level bias scores from multiple rating services, used for political asymmetry analysis.

### Primary Key

```sql
outlet_domain text primary key
```

### Columns

| Column | Type | Description |
| --- | --- | --- |
| outlet_domain | text | Domain key (e.g. `reuters.com`) |
| source_name | text | Display name (UNIQUE); join key to `enriched_headlines.source_name` |
| allsides_bias_score | double precision | AllSides scale (−6 to +6) |
| adfontes_bias_score | double precision | Ad Fontes Media scale (−42 to +42) |
| adfontes_reliability_score | double precision | Ad Fontes reliability (0–64) |
| mbfc_bias_score | double precision | MBFC scale (−10 to +10) |
| mbfc_factual_rating | text | MBFC factual rating (`Very High`, `High`, `Mostly Factual`, `Mixed`, `Low`) |
| composite_bias_score | double precision | Normalized average on AllSides scale |
| composite_bias_label | text | `Left` / `Lean Left` / `Center` / `Lean Right` / `Right` |
| political_group | text | `Left` / `Center` / `Right` (for asymmetry analysis) |
| geo_group | text | `US` / `Non-US` (for asymmetry analysis) |
| bias_last_verified_date | date | Last verification date |

---

## Table: public.enriched_headlines

Denormalized pre-joined table for Grafana dashboards and frontend consumption. Refreshed via `TRUNCATE + INSERT` — not directly written by pipelines.

### Key Columns

| Column | Type | Description |
| --- | --- | --- |
| story_id | text | FK to `newsapi_articles.uri` |
| title | text | Original headline |
| description | text | Article description/snippet |
| content | text | Article content |
| url | text | Article URL |
| source_name | text | Publisher display name |
| published_at | timestamptz | Publication timestamp |
| headline_clean | text | Normalized headline from validation |
| body_valid | boolean | Body validity from validation |
| top_category | text | Category classification from validation |
| topic_id | uuid | Assigned topic cluster |
| topic_label | text | Topic cluster label |
| cluster_size | integer | Number of stories in the topic |
| composite_score | real | Topic assignment quality score |
| dominant_category | text | Most common category in topic |
| is_public_interest | boolean | Public interest gate |
| pi_label | text | Public interest label |
| met_count | integer | PI criteria met count |
| material_impact | boolean | PI criterion |
| institutional_action | boolean | PI criterion |
| scope_scale | boolean | PI criterion |
| new_information | boolean | PI criterion |
| scope_status | text | `IN_SCOPE` / `OUT_OF_SCOPE` |
| source_feed | text | Source pipeline identifier |
| enriched_at | timestamptz | When enrichment was applied |

---

# Analytics Views

## View: v_heatmap

Topic × outlet matrix for coverage analysis.

- **Rows**: topics ordered by `cluster_size` DESC
- **Columns**: outlets ordered by `adfontes_bias_score` ASC (left → right politically)
- **Cells**: `article_count`, `pi_count`, `noise_count`, `news_to_noise_ratio`
- **Filter**: `scope_status = 'IN_SCOPE'`

## View: v_heatmap_asymmetry

Per-topic asymmetry metrics.

- `political_skew`: normalized (left − right) difference [−1, +1]. Center outlets excluded.
- `geo_skew`: normalized (US − non-US) difference [−1, +1].
- **Filter**: `scope_status = 'IN_SCOPE'`

---

# Operational Tables

## Table: public.pipeline_runs

Tracks the lifecycle of ingestion runs.

### Primary Key

```sql
(run_id, ingestion_source, run_type, nth_run)
```

### Columns

| Column | Type | Description |
| --- | --- | --- |
| run_id | timestamptz | Logical ingestion window identifier |
| ingestion_source | text | Pipeline source name |
| run_type | text | Execution mode (`scheduled`, `backfill`, `seed`) |
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

## Table: public.pipeline_run_metrics

Per-stage metrics for pipeline runs. Each row is one metric measurement for a specific pipeline stage within a run.

### Primary Key

```sql
(run_id, ingestion_source, run_type, nth_run, stage, metric_name)
```

### Columns

| Column | Type | Nullable | Default | Description |
| --- | --- | --- | --- | --- |
| run_id | timestamptz | not null | | Logical ingestion run identifier |
| ingestion_source | text | not null | | Pipeline source name |
| run_type | text | not null | | Execution mode (`scheduled`, `backfill`, `seed`) |
| nth_run | integer | not null | | Retry / execution ordinal |
| stage | text | not null | `'article_load'` | Pipeline stage that produced the metric |
| metric_name | text | not null | | Name of the metric |
| metric_value | bigint | not null | | Numeric value of the metric |
| stage_started_at | timestamptz | | | When the stage began executing |
| stage_completed_at | timestamptz | | | When the stage finished executing |
| created_at | timestamptz | not null | `now()` | Row creation timestamp |

### Indexes

| Name | Definition |
| --- | --- |
| `pipeline_run_metrics_pkey` | PRIMARY KEY btree (`run_id`, `ingestion_source`, `run_type`, `nth_run`, `stage`, `metric_name`) |
| `idx_pipeline_run_metrics_metric_name` | btree (`metric_name`) |
| `idx_pipeline_run_metrics_run_lookup` | btree (`run_id` DESC, `ingestion_source`, `run_type`, `nth_run`, `stage`) |
| `idx_pipeline_run_metrics_stage_metric` | btree (`stage`, `metric_name`) |

### Check Constraints

| Name | Expression |
| --- | --- |
| `pipeline_run_metrics_nth_run_check` | `nth_run >= 1` |
| `pipeline_run_metrics_run_type_check` | `run_type IN ('scheduled', 'backfill', 'seed')` |
| `pipeline_run_metrics_stage_check` | `stage IN ('article_collect', 'article_load', 'event_collect', 'event_load')` |

---

## Table: public.anomalies

Tracks anomalous news patterns flagged by automated detection or manual reporting.

### Primary Key

```sql
anomaly_id uuid primary key default gen_random_uuid()
```

### Columns

| Column | Type | Description |
| --- | --- | --- |
| anomaly_id | uuid | Unique anomaly identifier (auto-generated) |
| summary | text | Human-readable description of the anomaly |
| report_time | timestamptz | When the anomaly was reported (UTC) |
| category | anomaly_category | `Saturation Event`, `Ignored Policy Proposals`, `Ignored Human Rights Violation` |
| severity | anomaly_severity | `Low`, `Medium`, `High`, `Critical` |
| reporter | text | Reporting agent (`bots@...` or username) |
| tags | text[] | Freeform tags |
| outlets | text[] | Outlet identifiers involved |
| resolution_status | anomaly_resolution_status | `Pending`, `Validated`, `Invalidated` |
| investigator | text | Assigned investigator (optional) |
| added_context | text | Community notes / additional context (optional) |
| created_at | timestamptz | Row creation timestamp |
| updated_at | timestamptz | Row update timestamp |

### Indexes

| Name | Definition |
| --- | --- |
| `idx_anomalies_report_time` | btree (`report_time`) |
| `idx_anomalies_resolution_status` | btree (`resolution_status`) |
| `idx_anomalies_tags_gin` | GIN (`tags`) |
| `idx_anomalies_outlets_gin` | GIN (`outlets`) |

---

## Table: public.outlets

Canonical outlet dimension tracking news organizations and their metadata.

### Primary Key

```sql
id text primary key
```

### Columns

| Column | Type | Description |
| --- | --- | --- |
| id | text | Outlet identifier |
| name | text | Display name (unique) |
| description | text | Outlet description |
| website | text | Outlet website URL (unique) |
| category | text | Outlet category |
| language | text | Primary language |
| country | text | Country of origin |
| list1 | boolean | Permanent White House Press Credentials |
| wh_2022 | boolean | White House credentials 2022 |
| wh_2025 | boolean | White House credentials 2025 |
| list2 | boolean | Global bureaus |
| wire | boolean | Wire service |
| global | boolean | Global outlet |
| state_funded | boolean | State-funded outlet |
| list3 | boolean | Shapes public opinion |
| api_access | api_access_type | `Free`, `Paid`, `No API`, `Pending` |
| paywall | paywall_type | `Hard`, `Soft`, `No Paywall`, `Pending` |
| active | boolean | Whether outlet is actively tracked |
| created_at | timestamptz | Row creation timestamp |
| updated_at | timestamptz | Row update timestamp |

---

# PI Evaluation Tables

Tables supporting the public interest prompt evaluation framework. Used for benchmarking and iterating on PI assessment prompts.

## Table: public.pi_prompt_versions

Stores versioned public interest assessment prompts for evaluation. Only one version may be active at a time (enforced by partial unique index).

### Primary Key

```sql
version_id text primary key
```

### Columns

| Column | Type | Nullable | Default | Description |
| --- | --- | --- | --- | --- |
| version_id | text | not null | | Version identifier (e.g. `v1.0`, `v2.1`) |
| system_prompt | text | not null | | System prompt sent to the LLM |
| user_template | text | not null | | User message template (with placeholders for story data) |
| description | text | | | Human-readable description of changes in this version |
| model | text | not null | `'gpt-4o-mini'` | LLM model used for evaluation |
| temperature | real | not null | `0.0` | Sampling temperature |
| is_active | boolean | not null | `false` | Whether this is the currently active prompt version |
| created_at | timestamptz | not null | `now()` | Row creation timestamp |

### Indexes

| Name | Definition |
| --- | --- |
| `pi_prompt_versions_pkey` | PRIMARY KEY btree (`version_id`) |
| `idx_ppv_active` | UNIQUE btree (`is_active`) WHERE `is_active = true` |

### Relationships

- Referenced by `pi_eval_runs.prompt_version`

---

## Table: public.pi_eval_runs

Tracks individual evaluation runs of PI prompts against benchmark data. Each run evaluates one prompt version against the full benchmark set and computes aggregate accuracy metrics.

### Primary Key

```sql
run_id uuid primary key default gen_random_uuid()
```

### Columns

| Column | Type | Nullable | Default | Description |
| --- | --- | --- | --- | --- |
| run_id | uuid | not null | `gen_random_uuid()` | Unique run identifier |
| prompt_version | text | not null | | FK to `pi_prompt_versions.version_id` |
| model | text | not null | | LLM model used for this run |
| temperature | real | not null | `0.0` | Sampling temperature used |
| benchmark_size | integer | not null | | Number of benchmark stories evaluated |
| started_at | timestamptz | not null | `now()` | When the evaluation began |
| finished_at | timestamptz | | | When the evaluation completed |
| accuracy | real | | | Overall accuracy (fraction correct) |
| precision_pi | real | | | Precision for public interest classification |
| recall_pi | real | | | Recall for public interest classification |
| f1_pi | real | | | F1 score for public interest classification |
| false_positive_rate | real | | | False positive rate |
| false_negative_rate | real | | | False negative rate |
| accuracy_material_impact | real | | | Per-criterion accuracy: material impact |
| accuracy_institutional_action | real | | | Per-criterion accuracy: institutional action |
| accuracy_scope_scale | real | | | Per-criterion accuracy: scope/scale |
| accuracy_new_information | real | | | Per-criterion accuracy: new information |
| notes | text | | | Free-text notes about the run |
| created_at | timestamptz | not null | `now()` | Row creation timestamp |

### Indexes

| Name | Definition |
| --- | --- |
| `pi_eval_runs_pkey` | PRIMARY KEY btree (`run_id`) |
| `idx_per_prompt_version` | btree (`prompt_version`, `started_at` DESC) |

### Foreign Keys

| Name | Definition |
| --- | --- |
| `pi_eval_runs_prompt_version_fkey` | `prompt_version` → `pi_prompt_versions(version_id)` |

### Relationships

- Referenced by `pi_eval_results.run_id`

---

## Table: public.pi_eval_results

Per-story evaluation results from PI prompt evaluation runs. Each row compares the LLM's assessment against human ground truth labels, recording both sides and match flags.

### Primary Key

```sql
id uuid primary key default gen_random_uuid()
```

### Columns

| Column | Type | Nullable | Default | Description |
| --- | --- | --- | --- | --- |
| id | uuid | not null | `gen_random_uuid()` | Unique result identifier |
| run_id | uuid | not null | | FK to `pi_eval_runs.run_id` |
| story_id | text | not null | | Story URI evaluated |
| llm_is_public_interest | boolean | not null | | LLM's public interest gate result |
| llm_label | text | not null | | LLM's label (High/Moderate/Low/Not Public Interest) |
| llm_met_count | integer | not null | | LLM's criteria met count |
| llm_material_impact | boolean | not null | | LLM's material impact criterion |
| llm_institutional_action | boolean | not null | | LLM's institutional action criterion |
| llm_scope_scale | boolean | not null | | LLM's scope/scale criterion |
| llm_new_information | boolean | not null | | LLM's new information criterion |
| llm_reasoning | text | | | LLM's reasoning text |
| human_is_public_interest | boolean | not null | | Human ground truth gate |
| human_label | text | not null | | Human ground truth label |
| human_material_impact | boolean | not null | | Human ground truth: material impact |
| human_institutional_action | boolean | not null | | Human ground truth: institutional action |
| human_scope_scale | boolean | not null | | Human ground truth: scope/scale |
| human_new_information | boolean | not null | | Human ground truth: new information |
| label_match | boolean | not null | | Whether LLM label matches human label |
| pi_match | boolean | not null | | Whether LLM gate matches human gate |
| criteria_match_count | integer | not null | | Number of criteria where LLM matches human (0–4) |
| created_at | timestamptz | not null | `now()` | Row creation timestamp |

### Indexes

| Name | Definition |
| --- | --- |
| `pi_eval_results_pkey` | PRIMARY KEY btree (`id`) |
| `idx_perr_run_id` | btree (`run_id`) |
| `idx_perr_story_id` | btree (`story_id`) |
| `idx_perr_disagreement` | btree (`run_id`) WHERE `label_match = false` |

### Foreign Keys

| Name | Definition |
| --- | --- |
| `pi_eval_results_run_id_fkey` | `run_id` → `pi_eval_runs(run_id)` |

---

## Table: public.pi_benchmark_labels

Human ground truth labels for benchmarking PI assessment accuracy. Each row is one reviewer's assessment of one story. The unique constraint on `(story_id, reviewer_id)` prevents duplicate labels.

### Primary Key

```sql
id uuid primary key default gen_random_uuid()
```

### Columns

| Column | Type | Nullable | Default | Description |
| --- | --- | --- | --- | --- |
| id | uuid | not null | `gen_random_uuid()` | Unique label identifier |
| story_id | text | not null | | Story URI being labeled |
| reviewer_id | text | not null | | Identifier of the human reviewer |
| is_public_interest | boolean | not null | | Reviewer's public interest gate |
| label | text | not null | | Reviewer's label (High/Moderate/Low/Not Public Interest) |
| material_impact | boolean | not null | | Reviewer's material impact criterion |
| institutional_action | boolean | not null | | Reviewer's institutional action criterion |
| scope_scale | boolean | not null | | Reviewer's scope/scale criterion |
| new_information | boolean | not null | | Reviewer's new information criterion |
| notes | text | | | Reviewer notes |
| confidence | text | | | Reviewer's self-reported confidence level |
| labeled_at | timestamptz | not null | `now()` | When the label was recorded |
| created_at | timestamptz | not null | `now()` | Row creation timestamp |

### Indexes

| Name | Definition |
| --- | --- |
| `pi_benchmark_labels_pkey` | PRIMARY KEY btree (`id`) |
| `idx_pbl_story_id` | btree (`story_id`) |
| `idx_pbl_reviewer_id` | btree (`reviewer_id`) |
| `idx_pbl_story_reviewer` | UNIQUE btree (`story_id`, `reviewer_id`) |

---

# Legacy / Audit Tables

## Table: public.headlines

Legacy headline table from the original NewsAPI.org ingestion pipeline. **Deprecated** — retained for historical reference only.

### Primary Key

```sql
url text primary key
```

### Columns

| Column | Type | Description |
| --- | --- | --- |
| id | text | Stable identifier (auto-generated UUID) |
| source_id | text | Upstream source identifier |
| source_name | text | Display name of the source |
| author | text | Article author |
| title | text | Headline text |
| description | text | Article description/snippet |
| url | text | Canonical article URL |
| url_to_image | text | Featured image URL |
| published_at | timestamptz | Article publication timestamp |
| content | text | Article content (99% truncated due to NewsAPI.org limits) |
| public_interest_score | integer | Computed public interest score |
| material_impact | boolean | Material impact flag |
| institutional_action | boolean | Institutional action flag |
| scope_scale | boolean | Scope/scale flag |
| new_information | boolean | New information flag |
| topic_id | uuid | Topic cluster identifier |
| run_id | timestamptz | Logical ingestion run identifier |
| collected_at | timestamptz | Time collector retrieved the article |
| ingested_at | timestamptz | Time loader inserted the row |
| ingestion_source | text | Source pipeline (`newsapi-org`, `rss`, `scraper`) |
| created_at | timestamptz | Row creation timestamp |
| updated_at | timestamptz | Row update timestamp |

---

## Table: public.topic_assignment_audit

Audit trail for topic assignment changes. Details TBD — table exists in live DB but is not yet fully documented.

> **TODO**: Capture column definitions from live DB.

---

# Entity Relationships

```text
                    pipeline_runs
                         ↓
                  newsapi_articles ──────────────── events
                   │   │   │   │
                   │   │   │   └─── source_uri → newsapi_sources
                   │   │   │
                   │   │   └─── newsapi_article_concepts → newsapi_concepts
                   │   │
                   │   └─── newsapi_article_categories → newsapi_categories
                   │
        ┌──────────┼──────────┬─────────────────────┐
        ↓          ↓          ↓                     ↓
validation_  headline_topic  public_interest_  scope_
 outputs     assignments     assessments       classifications
                 │
                 ↓
              topics

pipeline_run_metrics
   └── (run_id, ingestion_source, run_type, nth_run) → pipeline_runs

outlet_bias_scores ←──join──→ enriched_headlines (via source_name)

enriched_headlines = denormalized join of all the above
```

---

# Example Queries

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

## Stories with public interest assessment

```sql
SELECT
  a.title,
  vo.top_category,
  pia.label AS pi_label,
  pia.met_count,
  sc.scope_status
FROM public.newsapi_articles a
JOIN public.validation_outputs vo ON vo.story_id = a.uri
JOIN public.public_interest_assessments pia ON pia.story_id = a.uri
LEFT JOIN public.scope_classifications sc ON sc.story_id = a.uri
WHERE pia.is_public_interest = true
ORDER BY a.date_time_published DESC
LIMIT 25;
```

## Topic coverage by outlet bias

```sql
SELECT
  eh.topic_label,
  obs.composite_bias_label,
  COUNT(*) AS article_count
FROM public.enriched_headlines eh
JOIN public.outlet_bias_scores obs ON obs.source_name = eh.source_name
WHERE eh.scope_status = 'IN_SCOPE'
GROUP BY eh.topic_label, obs.composite_bias_label
ORDER BY article_count DESC
LIMIT 50;
```
