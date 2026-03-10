03/10/2026
# 1. Documentation Plan

# Run Tracking Enhancement Plan

## Goal

Improve pipeline run tracking so the system can correctly represent:

- scheduled runs
- backfill runs
- seed runs
- retries or multiple executions of the same logical run

## Problem

The current `pipeline_runs` design assumes that a `run_id` uniquely identifies a single run. In practice:

- a scheduled run and a manual run may share the same `run_id`
- retries may occur
- seed and backfill runs may use the same window

This causes run records to overwrite each other.

## Design

Introduce two new columns:

```

run_type
nth_run

```

### run_type

Defines the purpose of the run.

Allowed values:

```

scheduled
backfill
seed

```

### nth_run

Counts how many executions have occurred for the same logical run.

Example:

```

run_id = 2026-03-09T10:00Z
scheduled nth_run=1
scheduled nth_run=2
backfill  nth_run=1

```

## New Primary Key

```

(run_id, ingestion_source, run_type, nth_run)

```

This uniquely identifies every execution.

## Articles Table Change

Add the same fields to `newsapi_articles`:

```

run_type
nth_run

```

This ties each article row to the exact pipeline execution that produced it.

## Manifest Contract Change

`manifest.json` will now include:

```

run_id
run_type
nth_run

```

The loader will use these values when updating `pipeline_runs`.

## Collector Changes

Remove:

```

SEED_RUN

```

Add:

```

RUN_TYPE

```

Supported values:

```

scheduled
backfill
seed

```

Collector workflow:

1. compute logical `run_id`
2. determine `run_type`
3. query max `nth_run`
4. insert new `pipeline_runs` row
5. write `run_type` and `nth_run` to manifest

## Loader Changes

Loader will:

1. read `run_id`, `run_type`, `nth_run` from `manifest.json`
2. update the matching `pipeline_runs` row
3. write those fields into `newsapi_articles`

## Historical Backfill

Existing rows will be backfilled:

- `run_id = 1900-01-01` → `seed`
- all others → `scheduled`
- `nth_run` assigned deterministically per `(run_id, ingestion_source, run_type)`
