#!/bin/bash
# MASTER ETL SCHEDULER FOR WSL
# Runs all ETL scripts in proper sequence with comprehensive logging

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

source ../venv/bin/activate

# Set WSL environment
export SSL_CERT_FILE=$(python -m certifi)
export REQUESTS_CA_BUNDLE=$(python -m certifi)

# Configuration
LOG_DIR="logs/master_schedule"
mkdir -p "$LOG_DIR"

TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
DATE_TODAY=$(date +"%Y-%m-%d")
MASTER_LOG="$LOG_DIR/master_${TIMESTAMP}.log"

# Email alert configuration
ALERT_EMAIL=""
ALERT_ON_FAILURE=true

# ============================================================================
# ETL SCRIPT CONFIGURATION
# ============================================================================

# Note: CRM scripts are already scheduled separately at 6:00 AM
# This scheduler handles BigQuery, Pipedrive, and GitHub ETLs

# BigQuery ETLs (7:00 AM)
BIGQUERY_SCRIPTS=(
    "bigquery/update_google_ads_data_final_schd.py"
    "bigquery/gsc_daily_update_final_schd.py"
    "bigquery/ga4_landing_pages_update_final_schd.py"
    "bigquery/ga4_referrer_sessions_update_final_schd.py"
)

BIGQUERY_NAMES=(
    "Google Ads ETL"
    "Google Search Console ETL"
    "GA4 Landing Pages ETL"
    "GA4 Referrer Sessions ETL"
)

# Pipedrive ETLs (8:00 AM)
PIPEDRIVE_SCRIPTS=(
    "pipedrive/sync_pipedrive_pipelines_final.py"           # Reference data first
    "pipedrive/sync_pipedrive_org_deals_final.py"          # Dimension tables
    "pipedrive/sync_pipedrive_deals_final_schd.py"         # Core deals data
    "pipedrive/sync_pipedrive_deals_final.py"              # Deals with credit status
    "pipedrive/sync_pipedrive_activities_shorter_final.py" # Activities (largest)
    "pipedrive/sync_mailchimp_final.py"                    # Third-party
    "pipedrive/sync_ringcentral_final.py"                  # Third-party
)

PIPEDRIVE_NAMES=(
    "Pipedrive Pipelines"
    "Pipedrive Organizations & Deals"
    "Pipedrive Deals (Scheduled)"
    "Pipedrive Deals (Credit Status)"
    "Pipedrive Activities"
    "MailChimp Integration"
    "RingCentral Integration"
)

# GitHub ETL (9:00 AM)
GITHUB_SCRIPTS=(
    "github/update_etl_github_prs_final_schd.py"
)

GITHUB_NAMES=(
    "GitHub PRs ETL"
)

# ============================================================================
# LOGGING FUNCTIONS
# ============================================================================

log_message() {
    local message="$1"
    local level="${2:-INFO}"
    local timestamp=$(date +"%Y-%m-%d %H:%M:%S")
    local log_entry="[$timestamp] [$level] $message"
    
    echo "$log_entry" | tee -a "$MASTER_LOG"
    
    # Also write to daily log
    local daily_log="$LOG_DIR/master_$(date +"%Y%m%d").log"
    echo "$log_entry" >> "$daily_log"
}

log_section() {
    local title="$1"
    local width=70
    
    log_message ""
    log_message "$(printf '=%.0s' $(seq 1 $width))" "SECTION"
    log_message "  $title" "SECTION"
    log_message "$(printf '=%.0s' $(seq 1 $width))" "SECTION"
    log_message ""
}

log_result() {
    local script_name="$1"
    local status="$2"
    local duration="$3"
    local message="$4"
    
    case $status in
        "SUCCESS")
            log_message "✅ $script_name completed in ${duration}s" "SUCCESS"
            ;;
        "FAILED")
            log_message "❌ $script_name failed after ${duration}s" "ERROR"
            log_message "   Error: $message" "ERROR"
            ;;
        "TIMEOUT")
            log_message "⏱️  $script_name timed out after ${duration}s" "WARNING"
            ;;
        "SKIPPED")
            log_message "⏭️  $script_name skipped: $message" "WARNING"
            ;;
    esac
}

send_alert() {
    local subject="ETL Alert: $1"
    local message="$2"
    
    if [ -n "$ALERT_EMAIL" ] && [ "$ALERT_ON_FAILURE" = true ]; then
        echo "$message" | mail -s "$subject" "$ALERT_EMAIL" 2>/dev/null || true
        log_message "Alert sent to: $ALERT_EMAIL" "ALERT"
    fi
}

# ============================================================================
# ETL RUNNER FUNCTIONS
# ============================================================================

run_etl_batch() {
    local batch_name="$1"
    shift
    local scripts=("$@")
    shift ${#scripts[@]}
    local names=("$@")
    
    local total_scripts=${#scripts[@]}
    local batch_start=$(date +%s)
    local batch_passed=0
    local batch_failed=0
    
    log_section "STARTING $batch_name BATCH ($total_scripts scripts)"
    
    for i in "${!scripts[@]}"; do
        local script_path="${scripts[$i]}"
        local script_name="${names[$i]}"
        local script_file=$(basename "$script_path")
        local script_num=$((i + 1))
        
        log_message "[$script_num/$total_scripts] Starting: $script_name"
        log_message "  Script: $script_file"
        
        if [[ ! -f "$script_path" ]]; then
            log_result "$script_name" "SKIPPED" "0" "Script not found: $script_path"
            ((batch_failed++))
            continue
        fi
        
        # Ensure script is executable
        chmod +x "$script_path" 2>/dev/null || true
        
        # Create individual script log
        local script_log="$LOG_DIR/${script_file%.py}_${TIMESTAMP}.log"
        
        local script_start=$(date +%s)
        
        # Run with timeout (BigQuery: 2h, Pipedrive: 3h, GitHub: 4h)
        local timeout_seconds=7200  # Default 2 hours
        
        if [[ "$batch_name" == "PIPEDRIVE" ]]; then
            timeout_seconds=10800  # 3 hours for Pipedrive (Activities can be large)
        elif [[ "$batch_name" == "GITHUB" ]]; then
            timeout_seconds=14400  # 4 hours for GitHub (many API calls)
        fi
        
        timeout $timeout_seconds python3 "$script_path" 2>&1 | tee -a "$script_log" | tee -a "$MASTER_LOG"
        
        local exit_code=${PIPESTATUS[0]}
        local script_end=$(date +%s)
        local script_duration=$((script_end - script_start))
        
        # Check result
        if [ $exit_code -eq 0 ]; then
            log_result "$script_name" "SUCCESS" "$script_duration" ""
            ((batch_passed++))
            
            # Extract stats if available
            local rows=$(grep -o "rows.*[0-9,]*" "$script_log" 2>/dev/null | tail -1 | grep -o "[0-9,]*" | head -1 || echo "")
            if [ -n "$rows" ]; then
                log_message "    Rows processed: $rows" "INFO"
            fi
            
        elif [ $exit_code -eq 124 ]; then
            log_result "$script_name" "TIMEOUT" "$script_duration" "Timeout after ${timeout_seconds}s"
            send_alert "ETL Timeout" "$script_name timed out after ${timeout_seconds} seconds"
            ((batch_failed++))
            
        else
            # Extract error message
            local error_msg=$(grep -i "error\|exception\|traceback\|failed" "$script_log" 2>/dev/null | head -2 | tr '\n' ' ' || echo "Unknown error")
            log_result "$script_name" "FAILED" "$script_duration" "$error_msg"
            send_alert "ETL Failure" "$script_name failed: $error_msg"
            ((batch_failed++))
        fi
        
        # Delay between scripts (except last one)
        if [ $i -lt $((total_scripts - 1)) ]; then
            log_message "  Waiting 30 seconds before next script..." "INFO"
            sleep 30
        fi
    done
    
    local batch_end=$(date +%s)
    local batch_duration=$((batch_end - batch_start))
    
    log_message ""
    log_message "📊 $batch_name BATCH SUMMARY:" "SUMMARY"
    log_message "  Total scripts: $total_scripts" "SUMMARY"
    log_message "  ✅ Passed: $batch_passed" "SUMMARY"
    log_message "  ❌ Failed: $batch_failed" "SUMMARY"
    log_message "  ⏱️  Duration: ${batch_duration}s" "SUMMARY"
    log_message ""
    
    return $batch_failed
}

# ============================================================================
# MAIN EXECUTION
# ============================================================================

log_section "MASTER ETL SCHEDULER - STARTING"
log_message "Date: $DATE_TODAY"
log_message "Time: $(date +'%H:%M:%S')"
log_message "Host: $(hostname)"
log_message "User: $(whoami)"
log_message ""

OVERALL_START=$(date +%s)
OVERALL_FAILURES=0

# Run BigQuery ETLs (7:00 AM batch)
log_message "🕖 SCHEDULED: BigQuery ETLs (7:00 AM)" "SCHEDULE"
run_etl_batch "BIGQUERY" "${BIGQUERY_SCRIPTS[@]}" "${BIGQUERY_NAMES[@]}"
BIGQUERY_FAILURES=$?
OVERALL_FAILURES=$((OVERALL_FAILURES + BIGQUERY_FAILURES))

# Delay between batches (1 hour simulated - would be actual time in cron)
log_message ""
log_message "⏳ Waiting 1 hour before next batch (8:00 AM Pipedrive ETLs)..." "SCHEDULE"
log_message "   In production, this would be actual time delay via cron scheduling" "SCHEDULE"
# sleep 3600  # Actual 1 hour delay - commented for testing

# Run Pipedrive ETLs (8:00 AM batch)
log_message "🕗 SCHEDULED: Pipedrive ETLs (8:00 AM)" "SCHEDULE"
run_etl_batch "PIPEDRIVE" "${PIPEDRIVE_SCRIPTS[@]}" "${PIPEDRIVE_NAMES[@]}"
PIPEDRIVE_FAILURES=$?
OVERALL_FAILURES=$((OVERALL_FAILURES + PIPEDRIVE_FAILURES))

# Delay between batches (1 hour simulated)
log_message ""
log_message "⏳ Waiting 1 hour before next batch (9:00 AM GitHub ETL)..." "SCHEDULE"
# sleep 3600  # Actual 1 hour delay - commented for testing

# Run GitHub ETL (9:00 AM batch)
log_message "🕘 SCHEDULED: GitHub ETL (9:00 AM)" "SCHEDULE"
run_etl_batch "GITHUB" "${GITHUB_SCRIPTS[@]}" "${GITHUB_NAMES[@]}"
GITHUB_FAILURES=$?
OVERALL_FAILURES=$((OVERALL_FAILURES + GITHUB_FAILURES))

# ============================================================================
# FINAL SUMMARY
# ============================================================================

OVERALL_END=$(date +%s)
OVERALL_DURATION=$((OVERALL_END - OVERALL_START))

log_section "MASTER ETL SCHEDULER - COMPLETE"

log_message "📈 OVERALL EXECUTION SUMMARY" "SUMMARY"
log_message "  Total duration: ${OVERALL_DURATION}s ($((OVERALL_DURATION / 60)) minutes)" "SUMMARY"
log_message "" "SUMMARY"

log_message "📊 BATCH RESULTS:" "SUMMARY"
log_message "  BigQuery ETLs:  ${#BIGQUERY_SCRIPTS[@]} scripts, $BIGQUERY_FAILURES failures" "SUMMARY"
log_message "  Pipedrive ETLs: ${#PIPEDRIVE_SCRIPTS[@]} scripts, $PIPEDRIVE_FAILURES failures" "SUMMARY"
log_message "  GitHub ETL:     ${#GITHUB_SCRIPTS[@]} scripts, $GITHUB_FAILURES failures" "SUMMARY"
log_message "" "SUMMARY"

TOTAL_SCRIPTS=$((${#BIGQUERY_SCRIPTS[@]} + ${#PIPEDRIVE_SCRIPTS[@]} + ${#GITHUB_SCRIPTS[@]}))
TOTAL_SUCCESS=$((TOTAL_SCRIPTS - OVERALL_FAILURES))

log_message "🎯 FINAL RESULTS:" "SUMMARY"
log_message "  Total scripts: $TOTAL_SCRIPTS" "SUMMARY"
log_message "  ✅ Successful: $TOTAL_SUCCESS" "SUMMARY"
log_message "  ❌ Failed: $OVERALL_FAILURES" "SUMMARY"
log_message "" "SUMMARY"

if [ $OVERALL_FAILURES -eq 0 ]; then
    log_message "🎉 ALL ETL SCRIPTS COMPLETED SUCCESSFULLY!" "SUCCESS"
    send_alert "ETL Success - All Scripts Complete" "All $TOTAL_SCRIPTS ETL scripts completed successfully in ${OVERALL_DURATION}s"
elif [ $TOTAL_SUCCESS -eq 0 ]; then
    log_message "💥 ALL ETL SCRIPTS FAILED!" "ERROR"
    send_alert "ETL Complete Failure" "All $TOTAL_SCRIPTS ETL scripts failed!"
else
    log_message "⚠️  $TOTAL_SUCCESS/$TOTAL_SCRIPTS scripts succeeded" "WARNING"
    send_alert "ETL Partial Success" "$TOTAL_SUCCESS of $TOTAL_SCRIPTS ETL scripts succeeded"
fi

# Generate HTML report
generate_html_report() {
    local report_file="$LOG_DIR/report_${TIMESTAMP}.html"
    
    cat > "$report_file" << HTML_REPORT
<!DOCTYPE html>
<html>
<head>
    <title>ETL Schedule Report - $DATE_TODAY</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        h1 { color: #333; }
        .success { color: green; }
        .error { color: red; }
        .warning { color: orange; }
        table { border-collapse: collapse; width: 100%; margin: 20px 0; }
        th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
        th { background-color: #f2f2f2; }
        .summary { background-color: #f9f9f9; padding: 15px; border-radius: 5px; }
    </style>
</head>
<body>
    <h1>ETL Schedule Report</h1>
    <p><strong>Date:</strong> $DATE_TODAY</p>
    <p><strong>Start Time:</strong> $(date -d @$OVERALL_START +'%H:%M:%S')</p>
    <p><strong>End Time:</strong> $(date -d @$OVERALL_END +'%H:%M:%S')</p>
    <p><strong>Duration:</strong> ${OVERALL_DURATION}s ($((OVERALL_DURATION / 60)) minutes)</p>
    
    <div class="summary">
        <h2>Summary</h2>
        <p>Total Scripts: $TOTAL_SCRIPTS</p>
        <p class="success">Successful: $TOTAL_SUCCESS</p>
        <p class="error">Failed: $OVERALL_FAILURES</p>
    </div>
    
    <h2>Log Files</h2>
    <ul>
        <li><a href="master_${TIMESTAMP}.log">Master Log</a></li>
        <li><a href="master_$(date +"%Y%m%d").log">Daily Log</a></li>
    </ul>
    
    <p><em>Report generated automatically by Master ETL Scheduler</em></p>
</body>
</html>
HTML_REPORT
    
    log_message "📄 HTML report generated: $report_file" "INFO"
}

generate_html_report

log_message ""
log_message "📁 Logs available in: $LOG_DIR/" "INFO"
log_message "   - Master log: $MASTER_LOG" "INFO"
log_message "   - Individual script logs: $LOG_DIR/*.log" "INFO"
log_message "   - HTML report: $LOG_DIR/report_${TIMESTAMP}.html" "INFO"

log_section "SCHEDULER COMPLETED"

exit $OVERALL_FAILURES
