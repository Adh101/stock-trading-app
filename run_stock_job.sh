#!/usr/bin/env bash
set -euo pipefail

# --- PATHS ---
PROJECT_DIR="/Users/atishdhamala/stock-trading-app"
VENV_DIR="$PROJECT_DIR/pythonenv"
PY="$VENV_DIR/bin/python3"
SCRIPT="$PROJECT_DIR/script.py"      # <- your script
LOG_DIR="$PROJECT_DIR/logs"
MAIN_LOG="$LOG_DIR/run_script.log"
# -------------------------

mkdir -p "$LOG_DIR"
cd "$PROJECT_DIR"

timestamp() { date '+%F %T'; }

echo "$(timestamp) START script.py" >> "$MAIN_LOG"

set +e
"$PY" "$SCRIPT" >> "$MAIN_LOG" 2>&1
ec=$?
set -e

if [ $ec -eq 0 ]; then
  echo "$(timestamp) OK" >> "$MAIN_LOG"
else
  echo "$(timestamp) FAIL exit=$ec" >> "$MAIN_LOG"
  exit "$ec"
fi
