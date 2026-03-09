
# NewsAPI Enrichment Insights & Clustering Guide

## Overview

The ingestion pipeline enriches articles with structured metadata from EventRegistry, including **concepts**, **categories**, **locations**, **links**, **duplicate relationships**, and the full **raw article payload**. These fields allow the dataset to be used not only for storage but also for analytical exploration and automated topic clustering.

Concepts represent normalized entities such as people, organizations, locations, and abstract topics. Categories provide hierarchical topical classifications. When combined with existing fields like `event_uri`, `story_uri`, and source information, these enrichments enable sophisticated analysis of media coverage patterns, event clustering, and narrative tracking.

This document highlights example queries that illustrate how the enrichment data can be used to generate insights.

---

# 1. Coverage by Concept Across News Sources

```sql
SELECT
  source->>'title' AS source,
  COUNT(*) AS articles
FROM public.newsapi_articles,
LATERAL jsonb_array_elements(concepts) c
WHERE c->'label'->>'eng' = 'Donald Trump'
GROUP BY source
ORDER BY articles DESC;
```

This query counts how many articles mentioning a specific concept were published by each news source. By exploding the `concepts` JSON array, the query isolates articles referencing the entity **Donald Trump** and aggregates counts by publication.

The result provides a quick way to evaluate **coverage intensity** across outlets. Analysts can compare how frequently different sources write about a given entity or topic. When extended to multiple time windows, this approach can reveal editorial emphasis, emerging narratives, or shifts in attention across media organizations.

---

# 2. Discover the Most Discussed Entities

```sql
SELECT
  concept->'label'->>'eng' AS concept,
  COUNT(*) AS article_count
FROM public.newsapi_articles,
LATERAL jsonb_array_elements(concepts) concept
GROUP BY concept
ORDER BY article_count DESC
LIMIT 25;
```

This query identifies the entities that appear most frequently in the dataset. Because concepts are normalized by EventRegistry, different textual variations of an entity map to the same canonical identifier.

The output reveals the **dominant narratives in the news cycle**. High-frequency entities may represent political figures, major events, corporate actors, or emerging crises. Analysts can use this information to quickly understand what topics are driving the overall news ecosystem at any given moment.

---

# 3. Topic Distribution by Category

```sql
SELECT
  category->>'uri' AS category,
  COUNT(*) AS articles
FROM public.newsapi_articles,
LATERAL jsonb_array_elements(categories) category
GROUP BY category
ORDER BY articles DESC;
```

Categories classify articles into a hierarchical topic taxonomy such as `news/Politics`, `news/Technology`, or `dmoz/Society/Law`. This query counts how many articles fall into each category.

The results provide insight into **topic distribution across the dataset**. Analysts can determine which areas dominate coverage, track shifts in subject matter over time, and identify underrepresented topics. Combined with source-level aggregation, this approach can also reveal editorial specialization among outlets.

---

# 4. Detect Articles With Shared Concepts (Cluster Signals)

```sql
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

This query finds pairs of articles that share multiple concepts. By expanding the concept arrays into rows and joining on concept identifiers, the query identifies articles that reference similar entities.

Shared concepts are a strong signal that two articles belong to the same **topic cluster or news story**. Articles that reference the same people, organizations, and locations often describe the same event. This technique forms the foundation for automated clustering algorithms and can be used to build story groupings even when explicit event identifiers are unavailable.

---

# 5. Category + Concept Relationships

```sql
SELECT
  concept->'label'->>'eng' AS concept,
  category->>'uri' AS category,
  COUNT(*) AS article_count
FROM public.newsapi_articles,
LATERAL jsonb_array_elements(concepts) concept,
LATERAL jsonb_array_elements(categories) category
GROUP BY concept, category
ORDER BY article_count DESC
LIMIT 50;
```

This query examines how concepts and categories intersect. It counts how frequently specific entities appear within particular topic classifications.

The results reveal **semantic relationships** between entities and topics. For example, a concept like "Artificial Intelligence" may frequently appear in the `news/Technology` category but occasionally surface in `news/Politics`. Analysts can use this to identify cross-domain narratives and understand how entities move across thematic boundaries in media coverage.

---

# 6. Geographic Context from Article Locations

```sql
SELECT
  location->'label'->>'eng' AS location,
  COUNT(*) AS articles
FROM public.newsapi_articles
WHERE location IS NOT NULL
GROUP BY location
ORDER BY articles DESC
LIMIT 20;
```

Some articles contain extracted geographic metadata describing where an event occurred. This query aggregates articles by location label.

Geographic analysis helps reveal **regional concentration of events or reporting**. Analysts can identify which locations dominate global news coverage, track geographic trends in reporting, or correlate media attention with geopolitical developments.

---

# 7. Source-Level Topic Specialization

```sql
SELECT
  source->>'title' AS source,
  category->>'uri' AS category,
  COUNT(*) AS articles
FROM public.newsapi_articles,
LATERAL jsonb_array_elements(categories) category
GROUP BY source, category
ORDER BY articles DESC
LIMIT 50;
```

This query measures which outlets publish the most content in each topic category.

The output helps identify **editorial specialization**. Some outlets may focus heavily on politics, others on international affairs or technology. Understanding these patterns can help contextualize coverage differences across media organizations and improve downstream analysis such as bias detection or coverage balance studies.

---

# Conclusion

The enrichment fields transform the dataset from simple article storage into a structured knowledge graph of entities, topics, locations, and sources. Concepts enable semantic clustering, categories provide topical classification, and source metadata enables comparative media analysis.

Together, these features allow researchers to explore narrative formation, identify clusters of related stories, measure coverage patterns, and analyze how information propagates across the media landscape.
