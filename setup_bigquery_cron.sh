#!/bin/bash
# Setup cron job for BigQuery ETL schedule

set -e

echo "Setting up BigQuery ETL cron job..."
echo "=================================="

# Get current directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCHEDULE_SCRIPT="$SCRIPT_DIR/schedule_bigquery_etls.sh"

# Check if script exists
if [[ ! -f "$SCHEDULE_SCRIPT" ]]; then
    echo "❌ Schedule script not found: $SCHEDULE_SCRIPT"
    exit 1
fi

echo "✅ Found schedule script: $SCHEDULE_SCRIPT"

# Default time
RUN_TIME="07:00"

echo ""
echo "📅 Schedule Configuration:"
echo "   Script: $SCHEDULE_SCRIPT"
echo "   Time: $RUN_TIME daily (recommended: after CRM ETLs at 6:00 AM)"
echo "   User: $(whoami)"
echo ""

# Parse hour and minute
HOUR=$(echo $RUN_TIME | cut -d: -f1)
MINUTE=$(echo $RUN_TIME | cut -d: -f2)

# Create cron entry
CRON_ENTRY="$MINUTE $HOUR * * * cd $SCRIPT_DIR && $SCHEDULE_SCRIPT"

echo "Cron entry to be added:"
echo "   $CRON_ENTRY"
echo ""

# Ask for confirmation
read -p "Add this cron job? (y/n): " CONFIRM
if [[ $CONFIRM != "y" && $CONFIRM != "Y" ]]; then
    echo "Cancelled."
    exit 0
fi

# Backup current crontab
BACKUP_FILE="$SCRIPT_DIR/crontab_backup_bigquery_$(date +%Y%m%d_%H%M%S).txt"
crontab -l > "$BACKUP_FILE" 2>/dev/null || true
echo "✅ Current crontab backed up to: $BACKUP_FILE"

# Remove any existing BigQuery ETL entries
TEMP_CRON=$(mktemp)
(crontab -l 2>/dev/null | grep -v "schedule_bigquery_etls.sh" || true) > "$TEMP_CRON"

# Add new entry
echo "$CRON_ENTRY" >> "$TEMP_CRON"

# Install new crontab
crontab "$TEMP_CRON"
rm -f "$TEMP_CRON"

echo "✅ Cron job added successfully!"
echo ""

# Show current crontab
echo "Current crontab entries:"
echo "-----------------------"
crontab -l | grep -A1 -B1 "schedule_bigquery_etls.sh" || echo "No matching entries found"

echo ""
echo "📝 Notes:"
echo "1. Logs will be saved to: $SCRIPT_DIR/logs/bigquery_schedule/"
echo "2. To test immediately: ./schedule_bigquery_etls.sh"
echo "3. To remove: crontab -e and delete the line"
