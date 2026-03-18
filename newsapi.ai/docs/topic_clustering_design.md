
# Topic Clustering Design – MDG News Pipeline

## Purpose

> **Note (2026-03-17):** This document captures the original design rationale for concept-based clustering. The production implementation has evolved to use **spaCy NER entities (PERSON, ORG, GPE)** instead of NewsAPI concepts as the overlap signal, along with an entity-veto gate and medoid-based centroids. For the current algorithm specification, see `context/core_specs_folder/TOPIC_CLUSTERING_ALGORITHM_SPEC.md` and `context/pipelines/clustering_pipeline.md`.

This document describes the design approach for clustering news articles into coherent **topics**, **stories**, and **events** using the enriched data collected by the MDG News Pipeline.

The clustering layer transforms individual articles into structured narratives that allow analysis of:

- story development
- coverage distribution across media outlets
- narrative propagation
- coverage omissions
- topic evolution over time

The design relies heavily on enrichment fields already present in the ingestion pipeline, particularly **concepts**, **categories**, **event_uri**, and **story_uri**.

---

# Key Signals for Clustering

The clustering system uses multiple signals extracted during ingestion.

### 1. Concepts

Concepts are normalized entities detected by EventRegistry.

Examples:

```
Donald Trump
Pentagon
Artificial Intelligence
United States Congress
Federal Reserve
```

Concepts are extremely powerful clustering signals because:

- different wording maps to the same entity
- they represent the semantic meaning of an article
- multiple concepts appearing together define a narrative context

Articles sharing many concepts are likely describing the same event.

---

### 2. Categories

Categories classify articles into topic domains such as:

```
news/Politics
news/Technology
news/Business
```

Categories help provide **coarse topic boundaries**. They can be used to filter candidate articles before deeper clustering.

Example use:

- cluster only within `news/Politics`
- compare clustering patterns across categories

---

### 3. Event URI

EventRegistry assigns an **event_uri** to many articles.

Example:

```
eng-11455335
```

These identifiers already represent machine-generated clusters created by EventRegistry.

They can be used in two ways:

- as a baseline clustering signal
- as a validation signal for custom clustering algorithms

---

### 4. Story URI

The `story_uri` field represents a smaller narrative grouping within EventRegistry.

Stories can represent:

- sub-events
- related narratives within a broader event

Story URIs can therefore be used to identify clusters at a **finer resolution**.

---

# Basic Concept Clustering Query

The simplest clustering approach uses **concept overlap**.

```
WITH article_concepts AS (
  SELECT
    uri,
    title,
    concept->>'uri' AS concept_uri
  FROM public.newsapi_articles
  CROSS JOIN LATERAL jsonb_array_elements(concepts) concept
)
SELECT
  a1.uri AS article_1,
  a2.uri AS article_2,
  COUNT(*) AS shared_concepts
FROM article_concepts a1
JOIN article_concepts a2
  ON a1.concept_uri = a2.concept_uri
 AND a1.uri < a2.uri
GROUP BY a1.uri, a2.uri
ORDER BY shared_concepts DESC
LIMIT 20;
```

This query identifies pairs of articles that share multiple concepts. A high number of shared concepts indicates strong narrative similarity.

In practice, a clustering system would use a threshold such as:

```
shared_concepts >= 3
```

to determine candidate clusters.

---

# Concept Frequency Filtering

Some concepts appear extremely frequently and provide little clustering value.

Examples:

```
United States
President
Government
```

These should be filtered using concept frequency thresholds.

Example query:

```
SELECT
  concept->>'uri' AS concept_uri,
  COUNT(*) AS article_count
FROM public.newsapi_articles,
LATERAL jsonb_array_elements(concepts) concept
GROUP BY concept_uri
ORDER BY article_count DESC;
```

Concepts appearing in a very large percentage of articles can be treated as **stop concepts**.

---

# Multi-Signal Clustering

The most reliable clusters combine multiple signals:

```
concept overlap
event_uri
story_uri
publication time proximity
category similarity
```

Example clustering heuristic:

```
same event_uri
OR
(shared_concepts >= 3 AND same_category)
AND publication_time within 24 hours
```

This multi-signal approach significantly reduces false positives.

---

# Event-Level Coverage Analysis

Once articles are clustered by event, the system can analyze coverage across sources.

Example query:

```
SELECT
  event_uri,
  source->>'title' AS source,
  COUNT(*) AS articles
FROM public.newsapi_articles
GROUP BY event_uri, source
ORDER BY event_uri;
```

This produces a table showing which outlets covered a particular event.

If a major outlet has **zero articles for an event cluster**, that indicates a potential **coverage omission**.

---

# Narrative Propagation Analysis

Clustering also allows analysis of how stories spread across media outlets.

Example query:

```
SELECT
  source->>'title' AS source,
  MIN(date_time_published) AS first_article
FROM public.newsapi_articles
WHERE event_uri = 'eng-11455335'
GROUP BY source
ORDER BY first_article;
```

This reveals the **chronology of reporting** for a specific event and can show which outlets break stories first.

---

# Topic Heatmap

Clusters can be used to generate coverage heatmaps across media outlets.

Example aggregation:

```
SELECT
  event_uri,
  source->>'title' AS source,
  COUNT(*) AS coverage
FROM public.newsapi_articles
GROUP BY event_uri, source;
```

This data can be visualized as:

```
Event        NYT   Reuters   Fox   AP   Politico
------------------------------------------------
Event A      12       10      8     9      7
Event B       0        5      2     4      3
Event C       9        8      6     7      5
```

Rows containing zeros represent **coverage gaps**.

---

# Normalized Concept Table (Future Optimization)

Querying JSON arrays repeatedly is computationally expensive.

A future optimization is to build a normalized table:

```
article_concepts
```

Schema:

```
article_uri
concept_uri
concept_label
concept_type
```

This allows clustering queries to run significantly faster and enables indexing.

Example index:

```
CREATE INDEX idx_article_concepts_concept
ON article_concepts(concept_uri);
```

---

# Public Interest Scoring

Concepts can also support public-interest scoring.

Entities associated with government, regulation, public safety, or major institutions may signal public-interest relevance.

Example:

```
Congress
Supreme Court
Pentagon
Federal Reserve
FDA
```

Stories containing these concepts are likely candidates for higher public-interest weighting.

---

# Future Extensions

The clustering framework can be extended with:

- vector embeddings
- graph-based clustering
- community detection algorithms
- temporal narrative tracking
- cross-source narrative comparison

These techniques can build on the structured concept graph already available in the dataset.

---

# Summary

Concepts, categories, event identifiers, and temporal data combine to form a rich set of signals for topic clustering. By leveraging these enrichment fields, the MDG News Pipeline can move beyond raw article ingestion to support advanced narrative analysis, event detection, and coverage comparison across media outlets.
