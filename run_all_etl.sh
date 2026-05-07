#!/bin/bash
# ETL Master Scheduler - Runs working scripts

BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="$BASE_DIR/logs"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
MASTER_LOG="$LOG_DIR/etl_master_${TIMESTAMP}.log"

echo "Starting ETL Pipeline at $(date)" | tee -a "$MASTER_LOG"
echo "=================================" | tee -a "$MASTER_LOG"

source "$BASE_DIR/../venv/bin/activate"

run_script() {
    local group="$1"
    local script="$2"
    local script_log="$LOG_DIR/${script%.*}_${TIMESTAMP}.log"
    
    echo "[$(date '+%H:%M:%S')] Running: $group/$script" | tee -a "$MASTER_LOG"
    
    (cd "$BASE_DIR/$group" && python "$script") >> "$script_log" 2>&1
    
    if [ $? -eq 0 ]; then
        echo "   ✅ Success" | tee -a "$MASTER_LOG"
        return 0
    else
        echo "   ❌ Failed" | tee -a "$MASTER_LOG"
        # Show error
        tail -5 "$script_log" 2>/dev/null | sed 's/^/     /' | tee -a "$MASTER_LOG"
        return 1
    fi
}

echo "" | tee -a "$MASTER_LOG"
echo "=== Pipedrive ETLs ===" | tee -a "$MASTER_LOG"

# Script 1: RingCentral Sync
run_script "pipedrive" "sync_ringcentral_final.py"

# Script 2: Pipedrive Deals Sync
run_script "pipedrive" "sync_pipedrive_deals_final.py"

echo "" | tee -a "$MASTER_LOG"
echo "ETL Pipeline completed at $(date)" | tee -a "$MASTER_LOG"
echo "Log: $MASTER_LOG" | tee -a "$MASTER_LOG"
