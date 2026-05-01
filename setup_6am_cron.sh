#!/bin/bash
# Setup daily ETL pipeline at 6:00 AM

echo "╔══════════════════════════════════════════════════════════════════════╗"
echo "║                    ETL PIPELINE CRON JOB SETUP                       ║"
echo "╚══════════════════════════════════════════════════════════════════════╝"
echo ""

# Get the full path to the scheduler
SCRIPT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/etl_6am_master.sh"

echo "📁 Scheduler location: $SCRIPT_PATH"
echo ""

# Create the cron job
CRON_TIME="0 6 * * *"  # 6:00 AM daily
CRON_USER="$(whoami)"
CRON_COMMAND="cd $(dirname "$SCRIPT_PATH") && $SCRIPT_PATH"

echo "⏰ Scheduled time: $CRON_TIME (6:00 AM daily)"
echo "👤 Running as user: $CRON_USER"
echo ""

# Remove any existing ETL cron jobs
echo "Cleaning up existing ETL cron jobs..."
crontab -l 2>/dev/null | grep -v "etl_6am_master.sh" | crontab -

# Add new cron job
echo "Adding new cron job..."
(crontab -l 2>/dev/null; echo "# ETL Pipeline - Runs daily at 6:00 AM"; echo "$CRON_TIME $CRON_COMMAND >> logs/cron_schedule.log 2>&1") | crontab -

if [ $? -eq 0 ]; then
    echo ""
    echo "✅ CRON JOB SETUP COMPLETE!"
    echo ""
    echo "📋 Job Details:"
    echo "   • Time: 6:00 AM daily"
    echo "   • Command: $SCRIPT_PATH"
    echo "   • Logs: logs/cron_schedule.log"
    echo ""
    echo "📅 Next scheduled run: Tomorrow at 6:00 AM"
    echo ""
    echo "🔧 Management commands:"
    echo "   • View cron jobs: crontab -l"
    echo "   • Edit cron jobs: crontab -e"
    echo "   • Remove all jobs: crontab -r"
    echo "   • Test manually: ./etl_6am_master.sh"
else
    echo ""
    echo "❌ Failed to setup cron job"
    exit 1
fi

echo ""
echo "Current crontab entries:"
echo "════════════════════════════════════════"
crontab -l
