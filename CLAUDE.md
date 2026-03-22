# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Deterministic ETL pipeline that ingests news articles and events from EventRegistry (NewsAPI.ai) and NewsAPI.org into a PostgreSQL warehouse. Clean separation between **collectors** (fetch data → versioned artifacts on S3/disk) and **loaders** (artifacts → PostgreSQL).

## Repository Structure

- `newsapi.ai/` — Primary ingestion pipeline (articles + events)
- `newsapi.org/` — Secondary ingestion source
- `migrations/` — PostgreSQL schema migrations (numbered SQL files)

Each pipeline source follows the same pattern: `src/articles/` and `src/events/` contain collector, loader, lambda wrapper, and shared artifact schema modules.

## Build & Deploy Commands

All commands run from `newsapi.ai/`:

```bash
# Build TypeScript
npm run build

# Package Lambda zip bundles (esbuild + archiver)
npm run package:lambda:collector:newsapi-ai
npm run package:lambda:loader:newsapi-ai
npm run package:lambda:collector:events
npm run package:lambda:loader:events

# Deploy to AWS Lambda
npm run deploy:lambda:collector:newsapi-ai

# Package + deploy in one step
npm run ship:lambda:collector:newsapi-ai
npm run ship:lambdas          # all four lambdas
```

## Running Locally

```bash
cd newsapi.ai
cp .env.example .env          # configure credentials
npx ts-node src/articles/newsapi-ai_collector.ts
npx ts-node src/articles/newsapi-ai_loader.ts
npx ts-node src/events/event_collector.ts
npx ts-node src/events/event_loader.ts
```

All configuration is via environment variables (see `.env.example` for full reference).

## Architecture

### Data Flow

```
EventRegistry API → Collector → Artifacts (S3 or /tmp) → Loader → PostgreSQL
```

- **Collectors** fetch from upstream APIs, deduplicate by URI, write deterministic CSV/JSONL artifacts with a manifest, and record `pipeline_runs` metadata.
- **Loaders** read artifacts, validate against the artifact contract version, bulk-load via `COPY`/upsert into PostgreSQL, and write a `load_report.json`.
- **Lambda wrappers** (`src/*/lambda/`) adapt the same collector/loader logic for AWS Lambda execution.

### S3 Event Chain (Lambda)

In production, the 4 Lambdas trigger each other automatically via S3 event notifications:

```
EventBridge (6h cron) → newsapi-ai_collector
  → writes manifest.json → triggers newsapi-ai_loader
    → writes load_report.json → triggers event_collector
      → writes event-manifest.json → triggers event-loader
```

Filter rules: prefix `newsapi.ai/out/ingestion_source=newsapi-ai/` + suffix `manifest.json`, `load_report.json`, or `event-manifest.json`.

### Code Duplication Warning

Collector logic is **duplicated** between standalone and Lambda versions:
- `src/articles/newsapi-ai_collector.ts` (local/ts-node)
- `src/articles/lambda/newsapi-ai_collector_lambda.ts` (AWS Lambda)

**Both files must be updated together** for any logic change. Same applies to loaders and event pipeline.

### Key Date/Time Distinction

EventRegistry articles have two timestamps:
- `dateTime` — **index time** (when EventRegistry indexed the article). This is what the API sorts by.
- `dateTimePub` — **publication time** (when the article was published). Can differ from `dateTime` by hours.

The collector uses `dateTime` for pagination early-stop (matching API sort order) and `dateTimePub` for article acceptance filtering.

### Artifact Contract

Versioned format (`newsapi-ai/v3` for articles, `newsapi-ai-events/v1` for events). Artifacts include `articles.csv`, `articles.jsonl`, and `manifest.json`. S3 layout: `<prefix>/ingestion_source=<SOURCE>/run_id=<RUN_ID>/`.

### Key Database Tables

- `public.newsapi_articles` — Main fact table, PK: `uri`, 60+ columns with JSONB raw payloads
- `public.events` — Event facts with freshness tracking
- `public.pipeline_runs` — Execution tracking, PK: `(run_id, ingestion_source, run_type, nth_run)`
- `public.newsapi_article_concepts`, `newsapi_article_categories` — Dimension tables

### Run Tracking

- `run_id`: Canonical ISO 8601 timestamp for run boundary
- `run_type`: `scheduled`, `backfill`, or `seed`
- `COLLECTOR_MODE`: `date_window` (normal) or `article_uri_list` (repair/backfill)
- `CYCLE_HOURS`: Controls sub-daily ingestion windows (4, 6, or 24 hours)

## Tech Stack

- **Runtime**: Node.js 20, TypeScript (ES2022/CommonJS)
- **Database**: PostgreSQL via `pg` + `pg-copy-streams` (no ORM)
- **Dates**: Luxon (Pacific/Honolulu timezone anchoring for cycles)
- **Cloud**: AWS Lambda + S3 (`@aws-sdk/client-s3` v3)
- **Bundler**: esbuild for Lambda packaging

## No Testing Framework

There is no automated test suite. Testing is done via local `ts-node` execution and Lambda log monitoring (`scripts/lambda/tail-lambdas.sh`).

## Operational Scripts

- `newsapi.ai/scripts/backfill_sources.sh` — Date-range backfill orchestration with operator review gates
- `newsapi.ai/scripts/lambda/tail-lambdas.sh` — Tail CloudWatch logs for all 4 Lambda functions
- `newsapi.ai/scripts/lambda/manifest.json` — Lambda function configuration

## Manual Lambda Backfill

To backfill a specific Honolulu day via Lambda:

```bash
# 1. Set backfill env vars
aws lambda update-function-configuration --function-name newsapi-ai_collector \
  --environment '{"Variables":{...existing vars..., "BACKFILL_LOCAL_DATE":"2026-03-06", "RUN_TYPE":"backfill"}}'

# 2. Invoke (S3 chain auto-fires loader → event_collector → event_loader)
aws lambda invoke --function-name newsapi-ai_collector --payload '{}' --cli-read-timeout 600 out.json

# 3. ALWAYS restore immediately after invoke returns
aws lambda update-function-configuration --function-name newsapi-ai_collector \
  --environment '{"Variables":{...existing vars..., "BACKFILL_LOCAL_DATE":"", "RUN_TYPE":"scheduled"}}'
```

**Important:** Forgetting to restore means the next scheduled 6h run will re-backfill instead of running normally. Lambda has 512MB ephemeral storage — large 24h backfills with all 38 sources can hit ENOSPC.

## Lambda Constraints

- **Timeout**: 300s (5 minutes)
- **Memory**: 3008 MB
- **Ephemeral storage**: 512 MB (can be limiting for full-day backfills)
- **VPC**: Runs in VPC with NAT gateway for internet + S3 VPC endpoint

## Windows Development

AWS CLI commands in Git Bash require:
```bash
PYTHONUTF8=1 MSYS_NO_PATHCONV=1 aws lambda ...
```

Production DB queries over SSM tunnel require:
```bash
NODE_TLS_REJECT_UNAUTHORIZED=0 node -e "..."
```

## Conventions

- Retry logic uses exponential backoff with jitter (base 1s, cap 15s) for 429/5xx and network errors
- All timestamps are UTC (`timestamptz`)
- Bulk loading uses PostgreSQL COPY protocol, not row-by-row INSERT
- Feature branches: `feature/<name>`, fixes: `fix/<name>`

## Documentation

- `newsapi.ai/docs/data_schema_reference.md` — Comprehensive table/column reference
- `newsapi.ai/docs/mdg_news_pipeline_architecture.md` — Pipeline architecture overview
- `newsapi.ai/.env.example` — Full environment variable reference (476 lines)
