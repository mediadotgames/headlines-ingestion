#!/usr/bin/env bash
# Run the cycle switch analysis against your PostgreSQL warehouse.
# Usage:
#   ./run_cycle_analysis.sh                          # uses DATABASE_URL from env
#   ./run_cycle_analysis.sh "postgres://user:pass@host:5432/db"
#   source ../../../.env && ./run_cycle_analysis.sh   # load from .env first

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DB_URL="${1:-${DATABASE_URL:-}}"

if [[ -z "$DB_URL" ]]; then
    echo "ERROR: No database URL provided."
    echo "  Set DATABASE_URL env var or pass as first argument."
    exit 1
fi

echo "Running ingestion cycle analysis..."
echo "Database: ${DB_URL%%@*}@***"
echo "Timestamp: $(date -u '+%Y-%m-%dT%H:%M:%SZ')"
echo "============================================="
echo ""

psql "$DB_URL" -f "$SCRIPT_DIR/analyze_cycle_switch.sql" \
    --expanded \
    --pset=footer=off \
    2>&1 | tee "$SCRIPT_DIR/analysis_output_$(date -u '+%Y%m%dT%H%M%SZ').txt"

echo ""
echo "Analysis complete. Output also saved to $SCRIPT_DIR/analysis_output_*.txt"
