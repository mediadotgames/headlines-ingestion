# Jira Ticket: Backfill Articles + Events (2026-02-14 → 2026-03-13)

Copy the sections below into Jira.

---

## Title

Backfill 14 new article sources and events for 2026-02-14 through 2026-03-13

## Type

Task

## Priority

High

## Labels

`backfill`, `data-pipeline`, `newsapi-ai`, `aws-lambda`

## Description

### Background

The headlines ingestion pipeline currently collects articles from 14 sources
(the original 11 plus dailywire.com, nationalreview.com, and nypost.com which
were backfilled independently). We are expanding coverage to 28 sources by
adding 14 new sources. Historical
data for the new sources needs to be backfilled from 2026-02-14 through
2026-03-13 (Honolulu local dates, 28 days).

Additionally, approximately half of the existing articles from the original
14 sources have `event_uri` values that do not yet have corresponding rows in
the `public.events` table. The backfill will also collect these missing events.

### New Sources (14)

axios.com, bloomberg.com, abcnews.com, cbsnews.com, us.cnn.com, latimes.com,
nbcnews.com, newsweek.com, theguardian.com, thehill.com, usatoday.com,
yahoo.com, rt.com, jpost.com

### Estimated Volume

~69,117 new articles across 14 sources over 28 days (~2,468 articles/day).

### Approach

A bash script that uses the AWS CLI to:

1. Snapshot current Lambda environment configurations
2. Loop through each date (2026-02-14 → 2026-03-13) sequentially:
   - Set `BACKFILL_LOCAL_DATE` and `EVENTREGISTRY_SOURCE_URIS` (14 new sources)
     on the article collector Lambda
   - Set `EVENT_DISCOVERY_SCOPE=time_window` on the event collector Lambda so
     it discovers event_uris from **all** articles on that date (both existing
     14-source and new 14-source), not just the current run
   - Invoke the article collector; the S3-triggered daisy chain handles the rest:
     article loader → event collector → event loader
   - Poll S3 for `event-load-report.json` to confirm pipeline completion
   - Display a results summary (article/event counts, errors)
   - **Pause for operator review** — the script blocks until the operator
     approves (`y`), retries the date (`r`), skips (`s`), or quits (`q`)
   - Only advances to the next date after explicit approval
3. After all 28 days complete:
   - Update `EVENTREGISTRY_SOURCE_URIS` to all 28 sources
   - Restore `EVENT_DISCOVERY_SCOPE=parent_run` (production default)
   - Clear `BACKFILL_LOCAL_DATE`, reset `RUN_TYPE=scheduled`

### Key Design Decisions

- **No code changes required** — uses existing env var knobs on the Lambdas
- **Sequential execution** — one date at a time, no parallelism
- **Operator review gate** — script pauses after each date for manual validation
  before proceeding (approve / retry / skip / quit)
- **`time_window` event discovery** — scans all articles in each day's window
  regardless of source, catching missing events from the original corpus
- **`EVENT_DISCOVERY_ONLY_MISSING=true`** — skips events already in the DB
- **Idempotent** — safe to re-run failed dates; articles and events upsert by URI

### Reference

See `newsapi.ai/docs/backfill_plan_2026-02-14_to_2026-03-13.md` for the full
technical plan.

---

## Acceptance Criteria

- [ ] Backfill script created and tested with a single date dry-run
- [ ] All 28 dates (2026-02-14 through 2026-03-13) processed successfully
- [ ] `newsapi_articles` contains articles from all 14 new sources for the date range
- [ ] `events` table populated for event_uris discovered across all 28 sources
- [ ] Production `EVENTREGISTRY_SOURCE_URIS` updated to include all 28 sources
- [ ] Lambda env vars restored to production defaults after backfill
- [ ] No impact on the scheduled daily pipeline during or after backfill
- [ ] Pipeline_runs table shows `status = 'loaded'` for all backfill runs
- [ ] Summary report generated with per-day article and event counts

---

## Subtasks

1. **Write backfill script** — Bash script using AWS CLI to drive the
   day-by-day backfill loop with env var management, invocation, polling,
   logging, and restoration
2. **Dry-run validation** — Run the script for a single date (e.g., 2026-03-13)
   and verify articles + events are correctly ingested
3. **Execute full backfill** — Run the script for all 28 dates, monitoring
   CloudWatch and pipeline_runs for errors
4. **Post-backfill validation** — Query article and event counts by source and
   date to confirm completeness
5. **Update production source list** — Confirm EVENTREGISTRY_SOURCE_URIS is set
   to all 28 sources and scheduled pipeline runs correctly

---

## Estimated Effort

- Script development: small
- Dry-run + validation: small
- Full backfill execution: ~28 sequential Lambda invocations, each taking
  2-5 minutes end-to-end for the full daisy chain. Total wall-clock time
  estimate: 1-3 hours (unattended after launch)
- Post-backfill validation: small
