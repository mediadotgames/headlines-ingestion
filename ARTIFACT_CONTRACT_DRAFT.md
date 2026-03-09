# ARTIFACT_CONTRACT.md

## Purpose

This document defines the artifact contract between the **collector** and the **loader** for the NewsAPI ingestion pipelines.

It exists to prevent drift between:

- collector output
- loader expectations
- target table schema
- pipeline lineage semantics

The collector and loader must be kept consistent with this document.

---

## Scope

This contract applies to artifact-based ingestion pipelines that follow this pattern:

1. collector fetches upstream articles
2. collector writes artifacts to S3 or local disk
3. loader reads artifacts
4. loader upserts rows into Aurora Postgres

Current pipelines using this pattern:

- `newsapi-org`
- `newsapi-ai`

---

## Artifact Set Per Run

Each run produces one artifact set under a single run prefix.

Typical files:

- `manifest.json`
- `articles.csv`
- `articles.jsonl`
- `load_report.json` (written by loader)

### S3 layout

Artifacts are stored under a run-specific prefix:

`<artifact_prefix>/ingestion_source=<INGESTION_SOURCE>/run_id=<RUN_ID>/`

Example:

`newsapi.ai/out/ingestion_source=newsapi-ai/run_id=2026-03-07T10-00-00.000Z/`

---

## Separation of Responsibilities

### Row-level payload
Belongs in `articles.csv` and `articles.jsonl`.

Examples:

- article identifier
- source identifier
- title
- url
- published timestamp
- content/body
- source metadata

### Run-level metadata
Belongs in `manifest.json`.

Examples:

- `ingestion_source`
- `run_id`
- `collected_at`
- `window_from`
- `window_to`
- scheduling metadata
- counts / page metadata

### Loader responsibility
The loader is the only component that merges:

- row-level payload from the artifact rows
- run-level metadata from `manifest.json`

into final database rows.

---

## Canonical Meaning of `run_id`

`run_id` means:

> the canonical timestamp identifier for the ingestion run boundary

It is not a freeform label.

It should remain stable, machine-readable, and castable to the target database type.

### Rules

- `run_id` must be valid for the target DB column type
- `run_id` must not be overloaded to mean "seed", "backfill", or any other human label
- if backfill/seed labeling is needed, it must be represented separately

### Current convention

For daily Honolulu-boundary runs:

- `run_id = window_to` as a UTC timestamp

Example:

- `run_id = 2026-03-07T10:00:00.000Z`

---

## Meaning of `collected_at`

`collected_at` means:

> the actual wall-clock time when the collector completed article collection for that run

This is distinct from `run_id`.

### Example

- `run_id`: logical run boundary
- `collected_at`: actual collection timestamp

Both should be preserved.

---

## Backfill Representation

Backfills must not change the meaning or type of `run_id`.

### Allowed backfill controls

Backfills may be represented by collector configuration such as:

- `LOOKBACK_DAYS`
- `WINDOW_END_DAYS_AGO`

### Recommended rule

Backfill runs use the same artifact contract as normal runs:

- same manifest shape
- same CSV shape
- same loader behavior

Only the window changes.

### Not allowed

Do not use:

- `run_id = "SEED"`
- other non-timestamp values in timestamp fields
- overloaded artifact paths based on ad hoc labels

If a backfill label is needed, add a separate manifest field such as:

- `seed_run: true`
- `run_type: "backfill"`

This metadata may be used for reporting, but must not redefine `run_id`.

---

## `manifest.json` Contract

`manifest.json` must contain the run-level metadata needed by the loader.

### Required fields

Required for all runs:

- `ingestion_source`
- `run_id`
- `collected_at`
- `window_from`
- `window_to`

### Typical optional fields

May include:

- `canonical_tz`
- `window_from_local`
- `window_to_local`
- `source_uris` or source list
- `articles_fetched`
- `articles_deduped`
- `page_size`
- `max_pages`
- pagination notes
- schedule metadata
- backfill metadata such as `seed_run`

### Loader dependency

The loader is allowed to rely on these required fields existing and being valid.

---

## `articles.csv` Contract

The CSV contract is explicit and must be stable.

### Rule

The collector and loader must share exactly the same:

- header names
- column order
- value formatting assumptions

### Enforcement

The loader should validate the header before COPY.

### Current `newsapi-ai` CSV columns

Current exact column order:

1. `uri`
2. `url`
3. `title`
4. `body`
5. `date`
6. `time`
7. `date_time`
8. `date_time_published`
9. `lang`
10. `is_duplicate`
11. `data_type`
12. `sentiment`
13. `event_uri`
14. `relevance`
15. `story_uri`
16. `image`
17. `source`
18. `authors`
19. `sim`
20. `wgt`

### Current `newsapi-org` CSV columns

Current exact column order:

1. `source_id`
2. `source_name`
3. `author`
4. `title`
5. `description`
6. `url`
7. `url_to_image`
8. `published_at`
9. `content`

### Important rule

Run-level metadata must not be mixed into CSV row payload unless that is explicitly part of the declared contract.

Examples of run-level metadata that do **not** belong in row CSV by default:

- `ingestion_source`
- `run_id`
- `window_from`
- `window_to`

These belong in `manifest.json` unless the contract explicitly says otherwise.

---

## `articles.jsonl` Contract

`articles.jsonl` is the row-level structured artifact.

It should contain one JSON object per article.

It is primarily used for:

- auditability
- debugging
- machine readability
- future loader alternatives

It should remain semantically consistent with `articles.csv`, even if field formatting differs.

---

## Loader Expectations

The loader is allowed to assume:

- `manifest.json` exists
- `articles.csv` exists
- the CSV header matches the declared contract
- row types are parseable according to the current loader implementation

The loader is not allowed to silently reinterpret a changed schema.

If the artifact contract does not match expectations, the loader should fail loudly.

---

## Temp Table / COPY Rules

The temp table and COPY statement must exactly match the artifact CSV contract.

### Safeguard requirements

Whenever CSV columns change, all of the following must be reviewed together:

1. migration / target table schema
2. shared artifact schema file
3. collector CSV writer
4. loader temp table definition
5. loader COPY column order
6. loader insert / upsert SQL
7. header validation logic

### Rule

Column order must match exactly.

A mismatch between:

- artifact CSV order
- temp table column order
- COPY column order

is a contract violation.

---

## Upsert Semantics

The loader performs the authoritative DB merge.

### The loader may mutate on insert

On insert, the loader may set:

- row payload columns
- `ingestion_source`
- `run_id`
- `collected_at`
- `ingested_at`

### The loader may mutate on update

On conflict/update, the loader may update:

- row payload columns that changed
- `ingestion_source`
- `run_id`
- `collected_at`
- `updated_at`

### The loader should not mutate on update

Unless explicitly intended, the loader should not update:

- `ingested_at`

`ingested_at` should normally represent first insert time.

---

## No-op Update Policy

The loader should skip no-op updates.

This means:

- if no meaningful tracked field changed
- do not update the row
- do not rewrite `updated_at`

The no-op comparison must include all fields that define meaningful row change, including lineage fields if those are intended to refresh.

Example lineage fields often included in the comparison:

- `ingestion_source`
- `run_id`
- `collected_at`

---

## Target Table Lineage Expectations

If the target table supports lineage, the loader should populate:

- `ingestion_source`
- `run_id`
- `collected_at`
- `ingested_at`

This enables:

- auditability
- Grafana run validation
- backfill inspection
- source-specific operational reporting

---

## Invariants

The following must remain true:

1. collector and loader agree on artifact schema
2. `run_id` retains one stable meaning
3. run-level metadata is defined in one place
4. temp table/COPY/upsert stay in sync with artifacts
5. loader fails loudly on contract mismatch
6. overlapping windows are safe because the loader upserts deterministically

---

## Change Management Checklist

Any time a new column is added to a target table that a loader writes to, review all of the following:

- DB migration
- shared artifact schema
- collector artifact writer
- manifest contract
- temp table definition
- COPY column order
- insert SQL
- upsert SQL
- no-op comparison
- Grafana / verification queries

No schema change is complete until all of these are reviewed together.

---

## Operational Principle

The artifact contract is part of the production interface.

Changing it is equivalent to changing an API.

Treat it with the same discipline.