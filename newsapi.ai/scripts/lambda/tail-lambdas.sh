#!/usr/bin/env bash
#
# tail-lambdas.sh — Open 4 Git Bash windows snapped to screen quadrants,
# each showing CloudWatch logs for a Lambda function (history + live tail).
#
# Usage: tail-lambdas.sh [days] [monitor]
#   days     Number of days of history to fetch (default: 1)
#   monitor  Monitor number to snap to (default: 1 = primary)
#
# Alias:
#   alias lambda-logs='~/dev/headlines-ingestion/newsapi.ai/scripts/lambda/tail-lambdas.sh'

set -euo pipefail

# Prevent Git Bash from mangling /aws/lambda/... into C:/Program Files/Git/...
export MSYS_NO_PATHCONV=1

# ── Config ──────────────────────────────────────────────────────────
REGION="us-east-2"
DAYS="${1:-1}"
MONITOR="${2:-1}"
SINCE="${DAYS}d"
MINTTY="$(command -v mintty 2>/dev/null || echo "/usr/bin/mintty")"
BASH_EXE="$(command -v bash)"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ── Dependency check ────────────────────────────────────────────────
if ! command -v aws &>/dev/null; then
  echo "Error: 'aws' CLI is not installed."
  echo "Install: https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html"
  exit 1
fi

# ── Snapshot existing mintty PIDs before launching ──────────────────
BEFORE_PIDS=$(powershell.exe -NoProfile -Command \
  "(Get-Process -Name mintty -ErrorAction SilentlyContinue).Id -join ','" 2>/dev/null | tr -d '\r')

# ── Launch helper ───────────────────────────────────────────────────
launch_pane() {
  local func_name="$1"
  local label="$2"
  local log_group="/aws/lambda/${func_name}"

  "$MINTTY" \
    --title "$label" \
    --exec "$BASH_EXE" -c "
      export MSYS_NO_PATHCONV=1
      echo '━━━ ${label} ━━━  (${log_group})  ━━━  last ${DAYS}d + live tail ━━━'
      echo ''
      aws logs tail '${log_group}' --region '${REGION}' --since '${SINCE}' --follow --format short
      echo ''
      echo '[stream ended — press Enter to close]'
      read
    " &
}

# ── Open 4 windows ─────────────────────────────────────────────────
launch_pane "newsapi-ai_collector" "Article Collector"
sleep 0.3
launch_pane "newsapi-ai_loader"    "Article Loader"
sleep 0.3
launch_pane "event_collector"      "Event Collector"
sleep 0.3
launch_pane "event-loader"         "Event Loader"

# Give last window time to fully appear
sleep 2

# ── Snap to quadrants via external PowerShell script ────────────────
powershell.exe -NoProfile -ExecutionPolicy Bypass \
  -File "$(cygpath -w "$SCRIPT_DIR/snap-windows.ps1")" \
  -ExcludePids "$BEFORE_PIDS" \
  -Monitor "$MONITOR" 2>/dev/null

echo "Lambda log viewer launched — 4 windows snapped to quadrants."
