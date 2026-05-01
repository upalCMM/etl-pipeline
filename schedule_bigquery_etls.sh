#!/bin/bash
# BigQuery ETL Master Scheduler for WSL
# Runs all 4 BigQuery ETL scripts in sequence

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Use the correct venv location (one level up)
PARENT_DIR="$(dirname "$SCRIPT_DIR")"
source "$PARENT_DIR/venv/bin/activate"

# Set WSL environment
export SSL_CERT_FILE=$(python -m certifi)
export REQUESTS_CA_BUNDLE=$(python -m certifi)

# Configuration
LOG_DIR="logs/bigquery_schedule"
mkdir -p "$LOG_DIR"

TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
DATE_TODAY=$(date +"%Y-%m-%d")
MASTER_LOG="$LOG_DIR/bigquery_master_${TIMESTAMP}.log"

# Script configuration
SCRIPTS=(
    "update_google_ads_data_final_schd.py"
    "gsc_daily_update_final_schd.py"
    "ga4_landing_pages_update_final_schd.py"
    "ga4_referrer_sessions_update_final_schd.py"
)

SCRIPT_NAMES=(
    "Google Ads ETL"
    "Google Search Console ETL"
    "GA4 Landing Pages ETL"
    "GA4 Referrer Sessions ETL"
)

# Function to log messages
log_message() {
    local message="$1"
    local level="${2:-INFO}"
    local timestamp=$(date +"%Y-%m-%d %H:%M:%S")
    echo "[$timestamp] [$level] $message" | tee -a "$MASTER_LOG"
}

# Function to run a single ETL script
run_etl_script() {
    local script_file="$1"
    local script_name="$2"
    local script_num="$3"
    local total_scripts="$4"
    
    local script_path="bigquery/$script_file"
    local script_log="$LOG_DIR/${script_file%.py}_${TIMESTAMP}.log"
    
    log_message "Starting script $script_num of $total_scripts: $script_name"
    log_message "Script: $script_file"
    
    if [[ ! -f "$script_path" ]]; then
        log_message "❌ Script not found: $script_path" "ERROR"
        return 1
    fi
    
    # Make sure script is executable
    chmod +x "$script_path" 2>/dev/null || true
    
    local start_time=$(date +%s)
    
    # Run the script with timeout (1 hour)
    log_message "Executing: python3 $script_file"
    
    timeout 3600 python3 "$script_path" 2>&1 | tee -a "$script_log" | tee -a "$MASTER_LOG"
    
    local exit_code=${PIPESTATUS[0]}
    local end_time=$(date +%s)
    local duration=$((end_time - start_time))
    
    # Check result
    if [ $exit_code -eq 0 ]; then
        log_message "✅ $script_name completed successfully in ${duration}s" "SUCCESS"
        
        # Extract stats from log if available
        local rows_processed=$(grep -o "rows.*[0-9,]*" "$script_log" | tail -1 | grep -o "[0-9,]*" | head -1 || echo "N/A")
        log_message "   Rows processed: $rows_processed"
        
        return 0
        
    elif [ $exit_code -eq 124 ]; then
        log_message "⏱️  $script_name TIMEOUT after ${duration}s (1 hour limit)" "WARNING"
        return 2
        
    else
        log_message "❌ $script_name FAILED after ${duration}s (exit code: $exit_code)" "ERROR"
        
        # Extract error details
        local last_error=$(tail -5 "$script_log" | grep -i "error\|exception\|traceback\|failed" | head -2 || echo "Unknown error")
        log_message "   Error details: $last_error"
        
        return 1
    fi
}

# Main execution
log_message "================================================" "HEADER"
log_message "🚀 BIGQUERY ETL MASTER SCHEDULER - STARTING" "HEADER"
log_message "Date: $DATE_TODAY" "HEADER"
log_message "Time: $(date +'%H:%M:%S')" "HEADER"
log_message "================================================" "HEADER"

START_TIME=$(date +%s)
declare -a RESULTS
declare -a DURATIONS

# Run each script
for i in "${!SCRIPTS[@]}"; do
    script_start=$(date +%s)
    
    run_etl_script "${SCRIPTS[$i]}" "${SCRIPT_NAMES[$i]}" "$((i+1))" "${#SCRIPTS[@]}"
    RESULTS[$i]=$?
    
    script_end=$(date +%s)
    DURATIONS[$i]=$((script_end - script_start))
    
    # Add a small delay between scripts
    if [ $i -lt $((${#SCRIPTS[@]} - 1)) ]; then
        log_message "Waiting 5 seconds before next script..."
        sleep 5
    fi
done

END_TIME=$(date +%s)
TOTAL_DURATION=$((END_TIME - START_TIME))

# Final summary
log_message "================================================" "HEADER"
log_message "📊 EXECUTION SUMMARY" "HEADER"
log_message "Total duration: ${TOTAL_DURATION}s" "HEADER"

success_count=0
for result in "${RESULTS[@]}"; do
    if [ $result -eq 0 ]; then
        ((success_count++))
    fi
done

log_message "Successful scripts: $success_count/${#SCRIPTS[@]}" "HEADER"

if [ $success_count -eq ${#SCRIPTS[@]} ]; then
    log_message "🎉 ALL SCRIPTS COMPLETED SUCCESSFULLY!" "SUCCESS"
elif [ $success_count -gt 0 ]; then
    log_message "⚠️  $success_count of ${#SCRIPTS[@]} scripts succeeded" "WARNING"
else
    log_message "❌ ALL SCRIPTS FAILED!" "ERROR"
fi

log_message "================================================" "HEADER"
log_message "Scheduler completed at $(date +'%H:%M:%S')" "INFO"

# Create simple summary file
SUMMARY_FILE="$LOG_DIR/summary_${TIMESTAMP}.txt"
{
    echo "BigQuery ETL Schedule Summary"
    echo "============================="
    echo "Date: $DATE_TODAY"
    echo "Start: $(date -d @$START_TIME +'%H:%M:%S')"
    echo "End: $(date -d @$END_TIME +'%H:%M:%S')"
    echo "Duration: ${TOTAL_DURATION}s"
    echo ""
    echo "Results:"
    for i in "${!SCRIPTS[@]}"; do
        local status=""
        if [ ${RESULTS[$i]} -eq 0 ]; then
            status="✅ SUCCESS"
        elif [ ${RESULTS[$i]} -eq 2 ]; then
            status="⏱️  TIMEOUT"
        else
            status="❌ FAILED"
        fi
        echo "- ${SCRIPT_NAMES[$i]}: $status (${DURATIONS[$i]}s)"
    done
    echo ""
    echo "Logs: $LOG_DIR/"
} > "$SUMMARY_FILE"

log_message "Summary saved to: $SUMMARY_FILE"

exit 0
