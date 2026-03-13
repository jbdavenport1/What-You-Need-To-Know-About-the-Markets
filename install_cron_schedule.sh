#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_BIN="${PYTHON_BIN:-$(command -v python3)}"
CRON_SCHEDULE="${CRON_SCHEDULE:-30 6 * * 1}"
LOG_FILE="${LOG_FILE:-$SCRIPT_DIR/output/weekly_automation.log}"
CRON_LINE="$CRON_SCHEDULE cd $SCRIPT_DIR && $PYTHON_BIN run_weekly_market_automation.py >> $LOG_FILE 2>&1"

mkdir -p "$SCRIPT_DIR/output"
( crontab -l 2>/dev/null | grep -Fv "run_weekly_market_automation.py" || true; echo "$CRON_LINE" ) | crontab -

echo "Installed cron job:"
echo "$CRON_LINE"
