# TEST CHANGE - Git branch learning
#!/bin/bash
# COMPLETE ETL MASTER SCHEDULER
# Runs ALL 19 ETL scripts sequentially at 6:00 AM daily

BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="$BASE_DIR/logs"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
MASTER_LOG="$LOG_DIR/etl_master_${TIMESTAMP}.log"
START_TIME=$(date +%s)

# Color codes for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo "====================================================================" | tee -a "$MASTER_LOG"
echo "?? STARTING COMPLETE ETL PIPELINE - $(date)" | tee -a "$MASTER_LOG"
echo "====================================================================" | tee -a "$MASTER_LOG"

# Activate virtual environment
# Load environment variables from .env (needed for cron/env -i)
PYTHON_BIN="/home/upalcmm/scripts/venv/bin/python"

if [ -x "$PYTHON_BIN" ]; then
    echo "Using venv python: $PYTHON_BIN" | tee -a "$MASTER_LOG"
else
    echo "ERROR: venv python not found at $PYTHON_BIN" | tee -a "$MASTER_LOG"
    exit 2
fi



# Function to run ETL script with timing and error handling
run_etl() {
    local category="$1"
    local script="$2"
    local full_path="$BASE_DIR/$category/$script"
    local script_log="$LOG_DIR/${script%.*}_${TIMESTAMP}.log"
    
    echo "" | tee -a "$MASTER_LOG"
    echo "[$(date '+%H:%M:%S')] ?? $category/$script" | tee -a "$MASTER_LOG"
    echo "--------------------------------------------------" | tee -a "$MASTER_LOG"
    
    # Check if script exists
    if [ ! -f "$full_path" ]; then
        echo -e "${RED}   ? ERROR: Script not found${NC}" | tee -a "$MASTER_LOG"
        return 2
    fi
    
    # Start timing
    local start_time=$(date +%s)
    
    # Run the script
    cd "$BASE_DIR/$category"
    "$PYTHON_BIN" "$script" >> "$script_log" 2>&1
    local exit_code=$?
    
    # End timing
    local end_time=$(date +%s)
    local duration=$((end_time - start_time))
    
    # Report results
    if [ $exit_code -eq 0 ]; then
        echo -e "${GREEN}   ? SUCCESS (${duration}s)${NC}" | tee -a "$MASTER_LOG"
        return 0
    else
        echo -e "${RED}   ? FAILED (${duration}s)${NC}" | tee -a "$MASTER_LOG"
        echo "   Last 3 lines of error:" | tee -a "$MASTER_LOG"
        tail -3 "$script_log" 2>/dev/null | sed 's/^/      /' | tee -a "$MASTER_LOG"
        return 1
    fi
}

# Initialize counters
TOTAL_SCRIPTS=0
SUCCESS_COUNT=0
FAILED_COUNT=0

echo "" | tee -a "$MASTER_LOG"
echo "?? EXECUTION SUMMARY" | tee -a "$MASTER_LOG"
echo "===================" | tee -a "$MASTER_LOG"

# ============================
# PHASE 1: PIPEDRIVE ETLs (7 scripts)
# ============================
echo "" | tee -a "$MASTER_LOG"
echo -e "${BLUE}?? PHASE 1: PIPEDRIVE ETLs${NC}" | tee -a "$MASTER_LOG"
echo "--------------------------" | tee -a "$MASTER_LOG"

PIPEDRIVE_SCRIPTS=(
    "sync_pipedrive_org_deals_final.py"
    "sync_pipedrive_deals_final_schd.py"
    "sync_pipedrive_activities_shorter_final.py"
    "sync_ringcentral_final.py"
    "sync_mailchimp_final.py"
    "sync_pipedrive_pipelines_final.py"
    "sync_pipedrive_deals_final.py"
)

for script in "${PIPEDRIVE_SCRIPTS[@]}"; do
    ((TOTAL_SCRIPTS++))
    if run_etl "pipedrive" "$script"; then
        ((SUCCESS_COUNT++))
    else
        ((FAILED_COUNT++))
    fi
done

# ============================
# PHASE 2: BIGQUERY ETLs (4 scripts)
# ============================
echo "" | tee -a "$MASTER_LOG"
echo -e "${BLUE}?? PHASE 2: BIGQUERY ETLs${NC}" | tee -a "$MASTER_LOG"
echo "-------------------------" | tee -a "$MASTER_LOG"

BIGQUERY_SCRIPTS=(
    "ga4_referrer_sessions_update_final_schd.py"
    "ga4_landing_pages_update_final_schd.py"
    "gsc_daily_update_final_schd.py"
    "update_google_ads_data_final_schd.py"
)

for script in "${BIGQUERY_SCRIPTS[@]}"; do
    ((TOTAL_SCRIPTS++))
    if run_etl "bigquery" "$script"; then
        ((SUCCESS_COUNT++))
    else
        ((FAILED_COUNT++))
    fi
done

# ============================
# PHASE 3: GITHUB ETLs (2 scripts)
# ============================
echo "" | tee -a "$MASTER_LOG"
echo -e "${BLUE}?? PHASE 3: GITHUB ETLs${NC}" | tee -a "$MASTER_LOG"
echo "------------------------" | tee -a "$MASTER_LOG"

GITHUB_SCRIPTS=(
    "update_etl_github_prs_final_schd.py"
    "update_etl_github_user_final_schd.py"
)

for script in "${GITHUB_SCRIPTS[@]}"; do
    ((TOTAL_SCRIPTS++))
    if run_etl "github" "$script"; then
        ((SUCCESS_COUNT++))
    else
        ((FAILED_COUNT++))
    fi
done

# ============================
# PHASE 4: CRM ETLs (7 scripts)
# ============================
echo "" | tee -a "$MASTER_LOG"
echo -e "${BLUE}?? PHASE 4: CRM ETLs${NC}" | tee -a "$MASTER_LOG"
echo "---------------------" | tee -a "$MASTER_LOG"

CRM_SCRIPTS=(
    "etl_crm_leads_final_schd.py"
    "etl_crm_partner_credit_status_final_schd.py"
    "etl_crm_partner_logs_master_final_schd.py"
    "etl_crm_partner_master_final_schd.py"
    "etl_crm_leads_company_final_schd.py"
    "etl_crm_signups_final_schd.py"
    "facebook_daily_update_final_schd.py"
    "msads_etl_working_final.py"   # <-- Add this line
)
for script in "${CRM_SCRIPTS[@]}"; do
    ((TOTAL_SCRIPTS++))
    if run_etl "crm" "$script"; then
        ((SUCCESS_COUNT++))
    else
        ((FAILED_COUNT++))
    fi
done

# ============================
# FINAL SUMMARY
# ============================
echo "" | tee -a "$MASTER_LOG"
echo "====================================================================" | tee -a "$MASTER_LOG"
echo "?? EXECUTION COMPLETE - $(date)" | tee -a "$MASTER_LOG"
echo "====================================================================" | tee -a "$MASTER_LOG"

# Calculate success rate
if [ $TOTAL_SCRIPTS -gt 0 ]; then
    SUCCESS_RATE=$((SUCCESS_COUNT * 100 / TOTAL_SCRIPTS))
else
    SUCCESS_RATE=0
fi

# Color-coded summary
if [ $FAILED_COUNT -eq 0 ]; then
    SUMMARY_COLOR=$GREEN
    SUMMARY_ICON="?"
elif [ $SUCCESS_RATE -ge 80 ]; then
    SUMMARY_COLOR=$YELLOW
    SUMMARY_ICON="?? "
else
    SUMMARY_COLOR=$RED
    SUMMARY_ICON="?"
fi

echo -e "${SUMMARY_COLOR}" | tee -a "$MASTER_LOG"
echo "   $SUMMARY_ICON TOTAL SCRIPTS: $TOTAL_SCRIPTS" | tee -a "$MASTER_LOG"
echo "   ? SUCCESSFUL: $SUCCESS_COUNT" | tee -a "$MASTER_LOG"
echo "   ? FAILED: $FAILED_COUNT" | tee -a "$MASTER_LOG"
echo "   ?? SUCCESS RATE: $SUCCESS_RATE%" | tee -a "$MASTER_LOG"
echo -e "${NC}" | tee -a "$MASTER_LOG"

echo "" | tee -a "$MASTER_LOG"
echo "?? LOG FILES:" | tee -a "$MASTER_LOG"
echo "   Master log: $MASTER_LOG" | tee -a "$MASTER_LOG"
find "$LOG_DIR" -name "*_${TIMESTAMP}.log" -type f 2>/dev/null | while read log; do
    echo "   Individual: $(basename "$log")" | tee -a "$MASTER_LOG"
done

END_TIME=$(date +%s)
TOTAL_DURATION=$((END_TIME - START_TIME))

echo "" | tee -a "$MASTER_LOG"
echo "??  TOTAL DURATION: ${TOTAL_DURATION} seconds" | tee -a "$MASTER_LOG"
echo "====================================================================" | tee -a "$MASTER_LOG"

# Return appropriate exit code
if [ $FAILED_COUNT -eq 0 ]; then
    exit 0
else
    exit 1
fi
