#!/bin/bash
# ETL Pipeline Monitor

echo "╔══════════════════════════════════════════════════════════════════════╗"
echo "║                      ETL PIPELINE MONITOR                           ║"
echo "╚══════════════════════════════════════════════════════════════════════╝"
echo ""

BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# 1. Check Cron Status
echo "📅 SCHEDULE STATUS"
echo "─────────────────"
if crontab -l 2>/dev/null | grep -q "etl_6am_master.sh"; then
    echo -e "✅ Scheduled: 6:00 AM daily"
    crontab -l | grep "etl_6am_master.sh" | sed 's/^/   /'
else
    echo -e "❌ Not scheduled"
    echo "   Run: ./setup_6am_cron.sh"
fi

echo ""
echo "📊 SCRIPT STATUS"
echo "────────────────"

TOTAL_COUNT=0
for category in pipedrive bigquery github crm; do
    count=$(find "$category" -name "*.py" 2>/dev/null | wc -l)
    ((TOTAL_COUNT+=count))
    echo "   $category/: $count scripts"
done
echo "   ─────────────────"
echo "   TOTAL: $TOTAL_COUNT scripts"

echo ""
echo "📈 RECENT EXECUTIONS"
echo "───────────────────"

if ls logs/etl_master_*.log 1>/dev/null 2>&1; then
    echo "Recent runs (newest first):"
    echo ""
    
    ls -t logs/etl_master_*.log | head -3 | while read logfile; do
        logname=$(basename "$logfile")
        logdate=$(echo "$logname" | sed 's/etl_master_\(.*\)\.log/\1/' | sed 's/_/ /')
        size=$(du -h "$logfile" | cut -f1)
        
        echo "📄 $logname ($size)"
        echo "   📅 $logdate"
        
        # Get success rate from log
        if grep -q "Success Rate:" "$logfile"; then
            success_rate=$(grep "Success Rate:" "$logfile" | tail -1 | awk '{print $NF}')
            duration=$(grep "Total Duration:" "$logfile" | tail -1 | awk '{print $NF}')
            echo "   ✅ Success Rate: $success_rate"
            echo "   ⏱️  Duration: $duration"
        fi
        
        # Get failed scripts if any
        if grep -q "❌ Failed:" "$logfile"; then
            failed=$(grep "❌ Failed:" "$logfile" | tail -1 | awk '{print $3}')
            if [ "$failed" -gt 0 ]; then
                echo "   ⚠️  $failed scripts failed"
                # Show failed script names
                grep "❌ Failed (" "$logfile" | sed 's/^/     • /' | head -3
            fi
        fi
        echo ""
    done
else
    echo "No execution logs found"
    echo "Run: ./etl_6am_master.sh"
fi

echo ""
echo "💾 DISK USAGE"
echo "─────────────"
du -sh logs/ 2>/dev/null || echo "Logs directory: 0 bytes"

echo ""
echo "🔧 QUICK ACTIONS"
echo "───────────────"
echo "1. Test all scripts:   ./etl_6am_master.sh"
echo "2. Schedule at 6AM:    ./setup_6am_cron.sh"
echo "3. View cron jobs:     crontab -l"
echo "4. Manual run:         ./etl_6am_master.sh"
echo "5. Check logs:         ls -lt logs/"
