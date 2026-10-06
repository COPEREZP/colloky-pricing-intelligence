#!/usr/bin/env bash
# =====================================================================
#  COLLOKY SCRAPER - run in the background (macOS / Linux)
# ---------------------------------------------------------------------
#  Usage (from the folder that has colloky_scraper.py):
#     bash run_scraper.sh
#  The scraper keeps running even if you close the terminal.
#  Progress is written to the "logs" folder.
#  Follow it live with:  tail -f logs/colloky_scraper_*.log
# =====================================================================

cd "$(dirname "$0")" || exit 1

# PostgreSQL credentials (only if they are not already set):
# export POSTGRES_PASSWORD="your_password"

# Resume an interrupted snapshot (optional):
# export COLLOKY_REANUDAR_SNAPSHOT="2026-10-06 08:47:00"

export PYTHONIOENCODING=utf-8

nohup python3 colloky_scraper.py > /dev/null 2>&1 &

echo "Colloky scraper started in the background (PID $!)."
echo "Follow it with: tail -f logs/colloky_scraper_*.log"
