#!/usr/bin/env bash
#
# backfill_sources.sh
#
# Backfill 14 new article sources and events for a date range.
# Runs one date at a time with an operator review gate between each.
#
# Usage:
#   ./scripts/backfill_sources.sh [START_DATE] [END_DATE]
#
# Defaults:
#   START_DATE = 2026-02-14
#   END_DATE   = 2026-03-13
#
# Prerequisites:
#   - AWS CLI v2 configured with appropriate permissions
#   - jq installed
#   - Lambda functions deployed: newsapi-ai_collector, newsapi-ai_loader,
#     event_collector, event-loader
#
set -euo pipefail

# ──────────────────────────────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────────────────────────────

START_DATE="${1:-2026-02-14}"
END_DATE="${2:-2026-03-13}"

ARTICLE_COLLECTOR_FN="newsapi-ai_collector"
EVENT_COLLECTOR_FN="event_collector"

# 14 new sources to backfill
NEW_SOURCES="axios.com,bloomberg.com,abcnews.com,cbsnews.com,us.cnn.com,latimes.com,nbcnews.com,newsweek.com,theguardian.com,thehill.com,usatoday.com,yahoo.com,rt.com,jpost.com"

# All 25 sources (post-backfill production)
ALL_SOURCES="aljazeera.com,apnews.com,axios.com,abcnews.com,bbc.com,bloomberg.com,cbsnews.com,foxnews.com,jpost.com,latimes.com,ms.now,nbcnews.com,newsweek.com,yahoo.com,nytimes.com,politico.com,reuters.com,rt.com,scmp.com,theguardian.com,thehill.com,us.cnn.com,usatoday.com,washingtonpost.com,wsj.com"

# Honolulu is UTC-10 with no DST
HONOLULU_OFFSET_HOURS=10

POLL_INTERVAL_SECONDS=30
POLL_TIMEOUT_SECONDS=900  # 15 minutes

SNAPSHOT_DIR="/tmp/backfill_snapshots_$(date +%s)"
LOG_FILE="/tmp/backfill_$(date +%Y%m%d_%H%M%S).log"

# ──────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────

log() {
  local msg="[$(date -u +%Y-%m-%dT%H:%M:%SZ)] $*"
  echo "$msg"
  echo "$msg" >> "$LOG_FILE"
}

die() {
  log "FATAL: $*"
  exit 1
}

# Advance a YYYY-MM-DD date by 1 day (portable: uses date -d on Linux)
next_date() {
  date -u -d "$1 + 1 day" +%Y-%m-%d
}

# Convert Honolulu local date to UTC start/end ISO timestamps
# Honolulu midnight = date + 10:00:00 UTC (no DST)
honolulu_to_utc_window() {
  local local_date="$1"
  local next_day
  next_day="$(next_date "$local_date")"
  echo "${local_date}T${HONOLULU_OFFSET_HOURS}:00:00Z"
  echo "${next_day}T${HONOLULU_OFFSET_HOURS}:00:00Z"
}

# Get current env vars for a Lambda as JSON object
get_lambda_env() {
  aws lambda get-function-configuration \
    --function-name "$1" \
    --query 'Environment.Variables' \
    --output json 2>/dev/null || echo '{}'
}

# Merge key=value pairs into a Lambda's env vars
# Usage: merge_lambda_env FUNCTION_NAME '{"KEY":"VAL",...}'
merge_lambda_env() {
  local fn="$1"
  local overrides="$2"
  local current
  current="$(get_lambda_env "$fn")"
  local merged
  merged="$(echo "$current" "$overrides" | jq -s '.[0] * .[1]')"

  aws lambda update-function-configuration \
    --function-name "$fn" \
    --environment "{\"Variables\": $merged}" \
    --output json > /dev/null

  wait_for_lambda_update "$fn"
}

# Poll until Lambda update finishes (replaces 'aws lambda wait' which can hang)
wait_for_lambda_update() {
  local fn="$1"
  local max_attempts=30   # 30 × 2s = 60s max
  local attempt=0
  while (( attempt < max_attempts )); do
    local status
    status="$(aws lambda get-function-configuration \
      --function-name "$fn" \
      --query 'LastUpdateStatus' --output text 2>/dev/null)"
    case "$status" in
      Successful) return 0 ;;
      Failed)
        log "ERROR: Lambda update failed for $fn"
        return 1 ;;
    esac
    (( attempt++ ))
    sleep 2
  done
  log "ERROR: Timed out waiting for $fn update (${max_attempts}×2s)"
  return 1
}

# Restore Lambda env from snapshot
restore_lambda_env() {
  local fn="$1"
  local snapshot_file="$2"
  if [[ -f "$snapshot_file" ]]; then
    local env_json
    env_json="$(cat "$snapshot_file")"
    aws lambda update-function-configuration \
      --function-name "$fn" \
      --environment "{\"Variables\": $env_json}" \
      --output json > /dev/null
    wait_for_lambda_update "$fn"
    log "Restored $fn env vars from snapshot"
  fi
}

# ──────────────────────────────────────────────────────────────────────
# Snapshot & Restore
# ──────────────────────────────────────────────────────────────────────

snapshot_configs() {
  mkdir -p "$SNAPSHOT_DIR"
  log "Snapshotting Lambda configurations to $SNAPSHOT_DIR"
  get_lambda_env "$ARTICLE_COLLECTOR_FN" > "$SNAPSHOT_DIR/article_collector.json"
  get_lambda_env "$EVENT_COLLECTOR_FN" > "$SNAPSHOT_DIR/event_collector.json"
  log "Snapshots saved"
}

restore_configs() {
  log "Restoring Lambda configurations from snapshots..."
  restore_lambda_env "$ARTICLE_COLLECTOR_FN" "$SNAPSHOT_DIR/article_collector.json"
  restore_lambda_env "$EVENT_COLLECTOR_FN" "$SNAPSHOT_DIR/event_collector.json"
  log "Lambda configurations restored to pre-backfill state"
}

# Trap to restore configs on exit/interrupt
cleanup() {
  local exit_code=$?
  echo ""
  log "Caught exit signal (code=$exit_code). Restoring Lambda configs..."
  restore_configs
  log "Backfill log: $LOG_FILE"
  exit "$exit_code"
}

# ──────────────────────────────────────────────────────────────────────
# Pipeline Polling
# ──────────────────────────────────────────────────────────────────────

# Find the current highest nth_run under a run_id/run_type prefix.
# Returns the number (e.g. "6"), or "0" if none exist yet.
get_max_nth_run() {
  local artifact_bucket="$1"
  local s3_prefix="$2"

  # List immediate "folders" under the prefix and extract nth_run values
  aws s3 ls "s3://${artifact_bucket}/${s3_prefix}" 2>/dev/null \
    | grep -oP 'nth_run=\K[0-9]+' \
    | sort -n \
    | tail -1 || echo "0"
}

# Poll S3 for load_report.json in a specific nth_run folder.
# We pass the expected nth_run so we don't match artifacts from previous runs.
poll_for_article_load_report() {
  local artifact_bucket="$1"
  local artifact_prefix="$2"
  local safe_run_id="$3"
  local expected_nth_run="$4"

  local s3_prefix="${artifact_prefix}/ingestion_source=newsapi-ai/run_id=${safe_run_id}/run_type=backfill/nth_run=${expected_nth_run}/"

  local elapsed=0
  while (( elapsed < POLL_TIMEOUT_SECONDS )); do
    local found
    found="$(aws s3 ls "s3://${artifact_bucket}/${s3_prefix}" --recursive 2>/dev/null \
      | grep 'load_report\.json$' || true)"

    if [[ -n "$found" ]]; then
      local key
      key="$(echo "$found" | awk '{print $NF}' | head -1)"
      echo "$key"
      return 0
    fi

    sleep "$POLL_INTERVAL_SECONDS"
    elapsed=$(( elapsed + POLL_INTERVAL_SECONDS ))
    log "  Polling article load_report (nth_run=${expected_nth_run})... (${elapsed}s / ${POLL_TIMEOUT_SECONDS}s)" >&2
  done

  log "  TIMEOUT waiting for article load_report.json" >&2
  return 1
}

# Poll for event-load-report or event loader load_reports/ directory
poll_for_event_completion() {
  local artifact_bucket="$1"
  local artifact_prefix="$2"
  local safe_run_id="$3"
  local expected_nth_run="$4"

  # Event artifacts go under:
  # ARTIFACT_PREFIX/ingestion_source=newsapi-ai-events/parent_run_id=<safe_run_id>/parent_run_type=backfill/nth_run=<N>/
  local s3_prefix="${artifact_prefix}/ingestion_source=newsapi-ai-events/parent_run_id=${safe_run_id}/parent_run_type=backfill/nth_run=${expected_nth_run}/"

  local elapsed=0
  while (( elapsed < POLL_TIMEOUT_SECONDS )); do
    # Look for event-manifest.json first (means event collector ran)
    local event_manifest
    event_manifest="$(aws s3 ls "s3://${artifact_bucket}/${s3_prefix}" --recursive 2>/dev/null \
      | grep 'event-manifest\.json$' || true)"

    if [[ -n "$event_manifest" ]]; then
      # Now look for load_reports/ (event loader output)
      local event_load_report
      event_load_report="$(aws s3 ls "s3://${artifact_bucket}/${s3_prefix}" --recursive 2>/dev/null \
        | grep 'load_reports/' || true)"

      if [[ -n "$event_load_report" ]]; then
        log "  Event pipeline complete" >&2
        return 0
      fi
    fi

    sleep "$POLL_INTERVAL_SECONDS"
    elapsed=$(( elapsed + POLL_INTERVAL_SECONDS ))
    log "  Polling event completion (nth_run=${expected_nth_run})... (${elapsed}s / ${POLL_TIMEOUT_SECONDS}s)" >&2
  done

  log "  TIMEOUT waiting for event pipeline completion" >&2
  return 1
}

# ──────────────────────────────────────────────────────────────────────
# Results Display
# ──────────────────────────────────────────────────────────────────────

display_results() {
  local artifact_bucket="$1"
  local load_report_key="$2"
  local current_date="$3"

  echo ""
  echo "═══════════════════════════════════════════════════════════════"
  echo "  RESULTS FOR: $current_date"
  echo "═══════════════════════════════════════════════════════════════"

  if [[ -n "$load_report_key" ]]; then
    local report
    report="$(aws s3 cp "s3://${artifact_bucket}/${load_report_key}" - 2>/dev/null || echo '{}')"

    if [[ -n "$report" && "$report" != "{}" ]]; then
      echo ""
      echo "  Article Load Report:"
      echo "  ─────────────────────────────────────────────────────────"
      echo "  Run ID:             $(echo "$report" | jq -r '.run_id // "N/A"')"
      echo "  Nth Run:            $(echo "$report" | jq -r '.nth_run // "N/A"')"
      echo "  Pipeline Status:    $(echo "$report" | jq -r '.pipeline_status // "N/A"')"
      echo "  Rows in Artifact:   $(echo "$report" | jq -r '.rows_in_artifact // "N/A"')"
      echo "  Rows Loaded:        $(echo "$report" | jq -r '.rows_loaded // "N/A"')"
      echo "  DB Rows Inserted:   $(echo "$report" | jq -r '.db_rows_inserted // "N/A"')"
      echo "  DB Rows Updated:    $(echo "$report" | jq -r '.db_rows_updated // "N/A"')"
      echo "  DB Rows Unchanged:  $(echo "$report" | jq -r '.db_rows_unchanged // "N/A"')"
      echo "  Duration:           $(echo "$report" | jq -r '.duration_ms // "N/A"')ms"
      echo "  Min Published:      $(echo "$report" | jq -r '.min_date_time_published // "N/A"')"
      echo "  Max Published:      $(echo "$report" | jq -r '.max_date_time_published // "N/A"')"
      echo ""

      # Show pipeline_run_metrics if present
      local metrics
      metrics="$(echo "$report" | jq -r '.pipeline_run_metrics // empty')"
      if [[ -n "$metrics" ]]; then
        echo "  Pipeline Run Metrics:"
        echo "  ─────────────────────────────────────────────────────────"
        echo "$metrics" | jq -r 'to_entries[] | "  \(.key): \(.value)"' 2>/dev/null || true
        echo ""
      fi
    fi
  else
    echo "  (load_report not found)"
  fi

  echo "═══════════════════════════════════════════════════════════════"
  echo ""
}

# ──────────────────────────────────────────────────────────────────────
# Operator Review Gate
# ──────────────────────────────────────────────────────────────────────

# Returns: "approve" | "retry" | "skip" | "quit"
review_gate() {
  local current_date="$1"
  local dates_remaining="$2"

  echo "  Dates remaining after this one: $dates_remaining" >&2
  echo "" >&2
  echo "  ┌─────────────────────────────────────────────────────────┐" >&2
  echo "  │  REVIEW GATE for $current_date                       │" >&2
  echo "  │                                                         │" >&2
  echo "  │  [y] Approve and continue to next date                  │" >&2
  echo "  │  [r] Retry this date                                    │" >&2
  echo "  │  [s] Skip this date and move to next                    │" >&2
  echo "  │  [q] Quit backfill (restore Lambda configs)             │" >&2
  echo "  └─────────────────────────────────────────────────────────┘" >&2
  echo "" >&2

  while true; do
    read -rp "  Decision for $current_date [y/r/s/q]: " choice
    case "${choice,,}" in
      y|yes)     echo "approve"; return ;;
      r|retry)   echo "retry";   return ;;
      s|skip)    echo "skip";    return ;;
      q|quit)    echo "quit";    return ;;
      *)         echo "  Invalid choice. Enter y, r, s, or q." >&2 ;;
    esac
  done
}

# ──────────────────────────────────────────────────────────────────────
# Pre-flight Checks
# ──────────────────────────────────────────────────────────────────────

preflight() {
  log "Starting pre-flight checks..."

  # Check AWS CLI
  if ! command -v aws &>/dev/null; then
    die "AWS CLI not found. Install it first."
  fi

  # Check jq
  if ! command -v jq &>/dev/null; then
    die "jq not found. Install it first."
  fi

  # Verify Lambda access
  local article_config
  article_config="$(aws lambda get-function-configuration \
    --function-name "$ARTICLE_COLLECTOR_FN" --output json 2>/dev/null)" \
    || die "Cannot access Lambda function: $ARTICLE_COLLECTOR_FN"

  local event_config
  event_config="$(aws lambda get-function-configuration \
    --function-name "$EVENT_COLLECTOR_FN" --output json 2>/dev/null)" \
    || die "Cannot access Lambda function: $EVENT_COLLECTOR_FN"

  # Extract ARTIFACT_BUCKET and ARTIFACT_PREFIX from current article collector config
  ARTIFACT_BUCKET="$(echo "$article_config" | jq -r '.Environment.Variables.ARTIFACT_BUCKET // empty')"
  ARTIFACT_PREFIX="$(echo "$article_config" | jq -r '.Environment.Variables.ARTIFACT_PREFIX // empty')"

  if [[ -z "$ARTIFACT_BUCKET" ]]; then
    die "ARTIFACT_BUCKET not set on $ARTICLE_COLLECTOR_FN"
  fi
  if [[ -z "$ARTIFACT_PREFIX" ]]; then
    die "ARTIFACT_PREFIX not set on $ARTICLE_COLLECTOR_FN"
  fi

  log "ARTIFACT_BUCKET: $ARTIFACT_BUCKET"
  log "ARTIFACT_PREFIX: $ARTIFACT_PREFIX"
  log "Pre-flight checks passed"
}

# ──────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────

main() {
  echo ""
  echo "╔═══════════════════════════════════════════════════════════════╗"
  echo "║           BACKFILL: 14 New Sources + Events                 ║"
  echo "║           Date range: $START_DATE → $END_DATE             ║"
  echo "╚═══════════════════════════════════════════════════════════════╝"
  echo ""

  log "Backfill starting: $START_DATE → $END_DATE"
  log "Log file: $LOG_FILE"

  # Pre-flight
  preflight

  # Snapshot current configs
  snapshot_configs

  # Set up trap for cleanup on exit/interrupt
  trap cleanup INT TERM

  # Build date list
  local dates=()
  local d="$START_DATE"
  while [[ "$d" < "$END_DATE" || "$d" == "$END_DATE" ]]; do
    dates+=("$d")
    d="$(next_date "$d")"
  done

  local total=${#dates[@]}
  log "Total dates to process: $total"

  # Summary of completed/skipped/failed dates
  local completed=0
  local skipped=0
  local failed=0
  local completed_dates=()
  local skipped_dates=()
  local failed_dates=()

  for (( i=0; i<total; i++ )); do
    local current_date="${dates[$i]}"
    local remaining=$(( total - i - 1 ))
    local attempt=1

    while true; do
      echo ""
      log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
      log "Processing date $((i+1))/$total: $current_date (attempt $attempt)"
      log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

      # 1. Compute UTC window
      local utc_window
      utc_window="$(honolulu_to_utc_window "$current_date")"
      local utc_start utc_end
      utc_start="$(echo "$utc_window" | head -1)"
      utc_end="$(echo "$utc_window" | tail -1)"
      log "UTC window: $utc_start → $utc_end"

      # safe_run_id: the end of the Honolulu day in UTC, with colons replaced
      # run_id = windowToUtc = utc_end (the end boundary)
      local safe_run_id
      safe_run_id="$(echo "$utc_end" | sed 's/:/-/g; s/Z$/.000Z/')"

      # 2. Update article collector env vars
      log "Updating $ARTICLE_COLLECTOR_FN env vars..."
      merge_lambda_env "$ARTICLE_COLLECTOR_FN" "$(cat <<ENVJSON
{
  "BACKFILL_LOCAL_DATE": "$current_date",
  "RUN_TYPE": "backfill",
  "EVENTREGISTRY_SOURCE_URIS": "$NEW_SOURCES"
}
ENVJSON
)"
      log "Article collector configured"

      # 3. Update event collector env vars
      log "Updating $EVENT_COLLECTOR_FN env vars..."
      merge_lambda_env "$EVENT_COLLECTOR_FN" "$(cat <<ENVJSON
{
  "EVENT_DISCOVERY_SCOPE": "time_window",
  "EVENT_DISCOVERY_TIME_COLUMN": "date_time_published",
  "EVENT_DISCOVERY_START": "$utc_start",
  "EVENT_DISCOVERY_END": "$utc_end",
  "EVENT_DISCOVERY_ONLY_MISSING": "true",
  "RUN_TYPE": "backfill"
}
ENVJSON
)"
      log "Event collector configured"

      # 4. Snapshot current max nth_run so we can poll for the new one
      local run_type_prefix="${ARTIFACT_PREFIX}/ingestion_source=newsapi-ai/run_id=${safe_run_id}/run_type=backfill/"
      local max_nth_run
      max_nth_run="$(get_max_nth_run "$ARTIFACT_BUCKET" "$run_type_prefix")"
      local expected_nth_run=$(( max_nth_run + 1 ))
      log "Current max nth_run=$max_nth_run, expecting nth_run=$expected_nth_run"

      # 5. Invoke article collector
      log "Invoking $ARTICLE_COLLECTOR_FN..."
      local invoke_response="/tmp/backfill_invoke_${current_date}.json"
      local invoke_result
      invoke_result="$(aws lambda invoke \
        --function-name "$ARTICLE_COLLECTOR_FN" \
        --payload '{}' \
        --cli-read-timeout 600 \
        "$invoke_response" \
        --output json 2>&1)"

      local function_error
      function_error="$(echo "$invoke_result" | jq -r '.FunctionError // empty' 2>/dev/null || true)"

      if [[ -n "$function_error" ]]; then
        log "ERROR: Lambda returned FunctionError: $function_error"
        log "Response: $(cat "$invoke_response" 2>/dev/null || echo 'N/A')"
        failed_dates+=("$current_date")
        # Still show review gate so operator can retry/skip/quit
        display_results "$ARTIFACT_BUCKET" "" "$current_date"
        echo "  *** ARTICLE COLLECTOR FAILED ***"
        echo "  FunctionError: $function_error"
        echo ""
      else
        log "Article collector invoked successfully"

        # 6. Poll for article load_report.json in the new nth_run
        log "Polling for article load_report.json (nth_run=${expected_nth_run})..."
        local load_report_key=""
        load_report_key="$(poll_for_article_load_report \
          "$ARTIFACT_BUCKET" "$ARTIFACT_PREFIX" "$safe_run_id" "$expected_nth_run")" || true

        if [[ -n "$load_report_key" ]]; then
          log "Article load_report found: $load_report_key"

          # 7. Poll for event pipeline completion
          log "Polling for event pipeline completion..."
          poll_for_event_completion \
            "$ARTIFACT_BUCKET" "$ARTIFACT_PREFIX" "$safe_run_id" "$expected_nth_run" || true
        else
          log "WARNING: Article load_report not found within timeout"
        fi

        # 7. Display results
        display_results "$ARTIFACT_BUCKET" "$load_report_key" "$current_date"
      fi

      # 8. Review gate
      local decision
      decision="$(review_gate "$current_date" "$remaining")"
      log "Operator decision for $current_date: $decision"

      case "$decision" in
        approve)
          completed=$(( completed + 1 ))
          completed_dates+=("$current_date")
          break
          ;;
        retry)
          attempt=$(( attempt + 1 ))
          log "Retrying $current_date (attempt $attempt)..."
          continue
          ;;
        skip)
          skipped=$(( skipped + 1 ))
          skipped_dates+=("$current_date")
          break
          ;;
        quit)
          log "Operator chose to quit at $current_date"
          echo ""
          echo "Restoring Lambda configurations..."
          restore_configs
          echo ""
          echo "╔═══════════════════════════════════════════════════════════════╗"
          echo "║  BACKFILL ABORTED                                           ║"
          echo "║  Completed: $completed  Skipped: $skipped  Remaining: $remaining  ║"
          echo "╚═══════════════════════════════════════════════════════════════╝"
          echo ""
          log "Backfill aborted. Completed=$completed Skipped=$skipped Remaining=$remaining"
          # Disable the trap since we already restored
          trap - INT TERM
          exit 0
          ;;
      esac
    done
  done

  # ──────────────────────────────────────────────────────────────────
  # Post-backfill
  # ──────────────────────────────────────────────────────────────────

  echo ""
  log "All dates processed. Starting post-backfill configuration..."

  # Ask operator whether to update production source list
  echo ""
  echo "  ┌─────────────────────────────────────────────────────────┐"
  echo "  │  POST-BACKFILL: Update production source list?          │"
  echo "  │                                                         │"
  echo "  │  This will set EVENTREGISTRY_SOURCE_URIS to all 25      │"
  echo "  │  sources and restore event collector to parent_run      │"
  echo "  │  scope for normal scheduled operation.                  │"
  echo "  │                                                         │"
  echo "  │  [y] Yes, update to 25 sources                          │"
  echo "  │  [n] No, just restore original configs                  │"
  echo "  └─────────────────────────────────────────────────────────┘"
  echo ""
  read -rp "  Update production sources? [y/n]: " update_choice

  if [[ "${update_choice,,}" == "y" || "${update_choice,,}" == "yes" ]]; then
    log "Updating production source list to all 25 sources..."
    merge_lambda_env "$ARTICLE_COLLECTOR_FN" "$(cat <<ENVJSON
{
  "EVENTREGISTRY_SOURCE_URIS": "$ALL_SOURCES",
  "BACKFILL_LOCAL_DATE": "",
  "RUN_TYPE": "scheduled"
}
ENVJSON
)"

    merge_lambda_env "$EVENT_COLLECTOR_FN" "$(cat <<ENVJSON
{
  "EVENT_DISCOVERY_SCOPE": "parent_run",
  "EVENT_DISCOVERY_TIME_COLUMN": "ingested_at",
  "EVENT_DISCOVERY_START": "",
  "EVENT_DISCOVERY_END": "",
  "EVENT_DISCOVERY_ONLY_MISSING": "false",
  "RUN_TYPE": "scheduled"
}
ENVJSON
)"
    log "Production source list updated to 25 sources"
    # Disable the trap since we've done our own update (not restoring snapshot)
    trap - INT TERM
  else
    log "Restoring original configs (not updating source list)..."
    restore_configs
    trap - INT TERM
  fi

  # Summary
  echo ""
  echo "╔═══════════════════════════════════════════════════════════════╗"
  echo "║  BACKFILL COMPLETE                                          ║"
  echo "╠═══════════════════════════════════════════════════════════════╣"
  echo "║  Completed: $completed                                          ║"
  echo "║  Skipped:   $skipped                                          ║"
  echo "║  Failed:    $failed                                          ║"
  echo "╚═══════════════════════════════════════════════════════════════╝"
  echo ""

  if [[ ${#completed_dates[@]} -gt 0 ]]; then
    echo "  Completed dates: ${completed_dates[*]}"
  fi
  if [[ ${#skipped_dates[@]} -gt 0 ]]; then
    echo "  Skipped dates:   ${skipped_dates[*]}"
  fi
  if [[ ${#failed_dates[@]} -gt 0 ]]; then
    echo "  Failed dates:    ${failed_dates[*]}"
  fi

  echo ""
  echo "  Log file: $LOG_FILE"
  echo "  Snapshots: $SNAPSHOT_DIR"
  echo ""
}

main
