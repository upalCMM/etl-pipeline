#!/bin/bash
# Setup cron jobs for Master ETL Scheduler

set -e

echo "Setting up Master ETL Scheduler Cron Jobs"
echo "=========================================="
echo ""

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MASTER_SCRIPT="$SCRIPT_DIR/master_etl_scheduler.sh"

if [[ ! -f "$MASTER_SCRIPT" ]]; then
    echo "❌ Master scheduler not found: $MASTER_SCRIPT"
    exit 1
fi

echo "✅ Found master scheduler: $MASTER_SCRIPT"
echo ""

echo "📅 Proposed Schedule:"
echo "   6:00 AM  - CRM ETLs (already scheduled separately)"
echo "   7:00 AM  - BigQuery ETLs (4 scripts)"
echo "   8:00 AM  - Pipedrive ETLs (7 scripts)"
echo "   9:00 AM  - GitHub ETL (1 script)"
echo ""

echo "This setup will create cron jobs for BigQuery, Pipedrive, and GitHub ETLs."
echo "CRM ETLs are assumed to already be scheduled at 6:00 AM."
echo ""

read -p "Continue with setup? (y/n): " CONFIRM
if [[ "$CONFIRM" != "y" && "$CONFIRM" != "Y" ]]; then
    echo "Cancelled."
    exit 0
fi

# Backup current crontab
BACKUP_FILE="$SCRIPT_DIR/crontab_backup_master_$(date +%Y%m%d_%H%M%S).txt"
crontab -l > "$BACKUP_FILE" 2>/dev/null || true
echo "✅ Crontab backed up to: $BACKUP_FILE"

# Create temp crontab
TEMP_CRON=$(mktemp)

# Keep existing entries except our ETL schedules
(crontab -l 2>/dev/null | grep -v "$MASTER_SCRIPT" | grep -v "schedule_bigquery\|schedule_crm\|run_bigquery" || true) > "$TEMP_CRON"

# Add master scheduler cron jobs
echo "# ==========================================================" >> "$TEMP_CRON"
echo "# MASTER ETL SCHEDULER - WSL" >> "$TEMP_CRON"
echo "# ==========================================================" >> "$TEMP_CRON"
echo "" >> "$TEMP_CRON"
echo "# BigQuery ETLs - 7:00 AM daily" >> "$TEMP_CRON"
echo "0 7 * * * cd $SCRIPT_DIR && $MASTER_SCRIPT --batch bigquery >> $SCRIPT_DIR/logs/master_cron.log 2>&1" >> "$TEMP_CRON"
echo "" >> "$TEMP_CRON"
echo "# Pipedrive ETLs - 8:00 AM daily" >> "$TEMP_CRON"
echo "0 8 * * * cd $SCRIPT_DIR && $MASTER_SCRIPT --batch pipedrive >> $SCRIPT_DIR/logs/master_cron.log 2>&1" >> "$TEMP_CRON"
echo "" >> "$TEMP_CRON"
echo "# GitHub ETL - 9:00 AM daily" >> "$TEMP_CRON"
echo "0 9 * * * cd $SCRIPT_DIR && $MASTER_SCRIPT --batch github >> $SCRIPT_DIR/logs/master_cron.log 2>&1" >> "$TEMP_CRON"
echo "" >> "$TEMP_CRON"
echo "# Full master run (for manual testing) - 10:00 AM daily" >> "$TEMP_CRON"
echo "0 10 * * * cd $SCRIPT_DIR && $MASTER_SCRIPT >> $SCRIPT_DIR/logs/master_full.log 2>&1" >> "$TEMP_CRON"

# Install new crontab
crontab "$TEMP_CRON"
rm -f "$TEMP_CRON"

echo ""
echo "✅ Cron jobs added successfully!"
echo ""

echo "📋 Current ETL cron jobs:"
echo "-------------------------"
crontab -l | grep -A2 -B2 "MASTER ETL\|BigQuery\|Pipedrive\|GitHub" || echo "(No matching entries found)"

echo ""
echo "📝 Setup Complete!"
echo ""
echo "✅ Scheduled Jobs:"
echo "   7:00 AM - BigQuery ETLs (4 scripts)"
echo "   8:00 AM - Pipedrive ETLs (7 scripts)"
echo "   9:00 AM - GitHub ETL (1 script)"
echo "   10:00 AM - Full master run (testing)"
echo ""
echo "📁 Logs will be saved to:"
echo "   - $SCRIPT_DIR/logs/master_schedule/ (detailed logs)"
echo "   - $SCRIPT_DIR/logs/master_cron.log (cron output)"
echo "   - $SCRIPT_DIR/logs/master_full.log (full run output)"
echo ""
echo "🔧 To modify schedule: crontab -e"
echo "🔍 To test immediately: ./master_etl_scheduler.sh"
echo ""
echo "Note: CRM ETLs are scheduled separately at 6:00 AM"
