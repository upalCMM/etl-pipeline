#!/bin/bash
# Setup daily cron for BigQuery ETLs

echo "Setting up daily BigQuery ETL cron job..."
echo "========================================="

CRON_TIME="7 0 * * *"  # 7:00 AM daily
SCRIPT_PATH="$HOME/scripts/etl_pipeline/run_bigquery_daily.sh"
CRON_JOB="$CRON_TIME $SCRIPT_PATH"

echo "Cron job to add:"
echo "$CRON_JOB"
echo ""
echo "This will run at 7:00 AM daily"
echo ""

read -p "Add this cron job? (y/n): " CONFIRM
if [[ "$CONFIRM" != "y" && "$CONFIRM" != "Y" ]]; then
    echo "Cancelled."
    exit 0
fi

# Backup current crontab
BACKUP="$HOME/scripts/etl_pipeline/cron_backup_$(date +%Y%m%d_%H%M%S).txt"
crontab -l > "$BACKUP" 2>/dev/null || true
echo "✅ Crontab backed up to: $BACKUP"

# Add new job
(crontab -l 2>/dev/null | grep -v "$SCRIPT_PATH"; echo "$CRON_JOB") | crontab -

echo "✅ Cron job added!"
echo ""
echo "Current crontab:"
crontab -l | grep -A1 -B1 "run_bigquery_daily" || echo "(Not found in output)"
