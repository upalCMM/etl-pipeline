#!/bin/bash
# ULTIMATE ETL MASTER SCHEDULER
# Runs ALL 18 ETL scripts daily at 6:00 AM
# Version: 3.0 - Production Ready

BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="$BASE_DIR/logs"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
MASTER_LOG="$LOG_DIR/etl_master_${TIMESTAMP}.log"
START_TIME=$(date +%s)

# Create logs directory
mkdir -p "$LOG_DIR"

# Load environment variables
if [ -f "$BASE_DIR/.env" ]; then
    export $(grep -v '^#' "$BASE_DIR/.env" | xargs)
    echo "Loaded environment variables from .env" | tee -a "$MASTER_LOG"
fi

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

# Header
echo "╔══════════════════════════════════════════════════════════════════════╗" | tee -a "$MASTER_LOG"
echo "║                         ETL MASTER PIPELINE                          ║" | tee -a "$MASTER_LOG"
echo "║                    Daily Execution - 6:00 AM Schedule                ║" | tee -a "$MASTER_LOG"
echo "╠══════════════════════════════════════════════════════════════════════╣" | tee -a "$MASTER_LOG"
echo "║ Start Time: $(date)                                                  ║" | tee -a "$MASTER_LOG"
echo "║ Log File: $MASTER_LOG                                                ║" | tee -a "$MASTER_LOG"
echo "╚══════════════════════════════════════════════════════════════════════╝" | tee -a "$MASTER_LOG"

# Activate virtual environment
if [ -f "$BASE_DIR/../venv/bin/activate" ]; then
    source "$BASE_DIR/../venv/bin/activate"
    echo "✅ Virtual environment activated" | tee -a "$MASTER_LOG"
else
    echo -e "${YELLOW}⚠️  Virtual environment not found${NC}" | tee -a "$MASTER_LOG"
fi

# Function to run single ETL script
run_etl_script() {
    local category="$1"
    local script="$2"
    local script_dir="$BASE_DIR/$category"
    local script_path="$script_dir/$script"
    local script_log="$LOG_DIR/${script%.*}_${TIMESTAMP}.log"
    
    echo "" | tee -a "$MASTER_LOG"
    echo -e "${CYAN}▶ [$(date '+%H:%M:%S')] $category/$script${NC}" | tee -a "$MASTER_LOG"
    echo "────────────────────────────────────────────────────" | tee -a "$MASTER_LOG"
    
    # Check if script exists
    if [ ! -f "$script_path" ]; then
        echo -e "${RED}❌ Script not found${NC}" | tee -a "$MASTER_LOG"
        return 1
    fi
    
    # Start timer
    local start_time=$(date +%s)
    
    # Run the script
    cd "$script_dir"
    python3 "$script" >> "$script_log" 2>&1
    local exit_code=$?
    
    # End timer
    local end_time=$(date +%s)
    local duration=$((end_time - start_time))
    
    # Report results
    if [ $exit_code -eq 0 ]; then
        echo -e "${GREEN}✅ Success (${duration}s)${NC}" | tee -a "$MASTER_LOG"
        
        # Show last line of log for success
        if [ -f "$script_log" ]; then
            tail -1 "$script_log" | sed 's/^/    /' | tee -a "$MASTER_LOG"
        fi
        return 0
    else
        echo -e "${RED}❌ Failed (${duration}s)${NC}" | tee -a "$MASTER_LOG"
        
        # Show error summary
        if [ -f "$script_log" ]; then
            echo "    Error summary:" | tee -a "$MASTER_LOG"
            grep -E "(ERROR|Error:|Traceback|Exception:|failed|Failed)" "$script_log" | head -3 | sed 's/^/      • /' | tee -a "$MASTER_LOG"
        fi
        return 1
    fi
}

# Initialize counters
TOTAL_SCRIPTS=0
SUCCESS_COUNT=0
FAILED_COUNT=0

echo "" | tee -a "$MASTER_LOG"
echo -e "${BLUE}📊 EXECUTION PROGRESS${NC}" | tee -a "$MASTER_LOG"
echo "════════════════════════════════════════" | tee -a "$MASTER_LOG"

# ============================================
# PHASE 1: PIPEDRIVE ETLs (7 scripts)
# ============================================
echo "" | tee -a "$MASTER_LOG"
echo -e "${YELLOW}🔷 PHASE 1: PIPEDRIVE ETLs${NC}" | tee -a "$MASTER_LOG"
echo "────────────────────────────────────" | tee -a "$MASTER_LOG"

PIPEDRIVE_SCRIPTS=(
    "sync_pipedrive_org_deals_final.py"
    "sync_pipedrive_deals_final_schd.py"
    "sync_pipedrive_activities_final.py"
    "sync_ringcentral_final.py"
    "sync_mailchimp_final.py"
    "sync_pipedrive_pipelines_final.py"
    "sync_pipedrive_deals_final.py"
)

for script in "${PIPEDRIVE_SCRIPTS[@]}"; do
    ((TOTAL_SCRIPTS++))
    if run_etl_script "pipedrive" "$script"; then
        ((SUCCESS_COUNT++))
    else
        ((FAILED_COUNT++))
        # Optional: Continue on failure
        echo -e "${YELLOW}⚠️  Continuing with next script...${NC}" | tee -a "$MASTER_LOG"
    fi
done

# ============================================
# PHASE 2: BIGQUERY ETLs (4 scripts)
# ============================================
echo "" | tee -a "$MASTER_LOG"
echo -e "${YELLOW}🔷 PHASE 2: BIGQUERY ETLs${NC}" | tee -a "$MASTER_LOG"
echo "────────────────────────────────────" | tee -a "$MASTER_LOG"

BIGQUERY_SCRIPTS=(
    "ga4_referrer_sessions_update_final_schd.py"
    "ga4_landing_pages_update_final_schd.py"
    "gsc_daily_update_final_schd.py"
    "update_google_ads_data_final_schd.py"
)

for script in "${BIGQUERY_SCRIPTS[@]}"; do
    ((TOTAL_SCRIPTS++))
    if run_etl_script "bigquery" "$script"; then
        ((SUCCESS_COUNT++))
    else
        ((FAILED_COUNT++))
        echo -e "${YELLOW}⚠️  Continuing with next script...${NC}" | tee -a "$MASTER_LOG"
    fi
done

# ============================================
# PHASE 3: GITHUB ETLs (1 script)
# ============================================
echo "" | tee -a "$MASTER_LOG"
echo -e "${YELLOW}🔷 PHASE 3: GITHUB ETLs${NC}" | tee -a "$MASTER_LOG"
echo "────────────────────────────────────" | tee -a "$MASTER_LOG"

((TOTAL_SCRIPTS++))
if run_etl_script "github" "update_etl_github_prs_final_schd.py"; then
    ((SUCCESS_COUNT++))
else
    ((FAILED_COUNT++))
fi

# ============================================
# PHASE 4: CRM ETLs (5 scripts)
# ============================================
echo "" | tee -a "$MASTER_LOG"
echo -e "${YELLOW}🔷 PHASE 4: CRM ETLs${NC}" | tee -a "$MASTER_LOG"
echo "────────────────────────────────────" | tee -a "$MASTER_LOG"

CRM_SCRIPTS=(
    "etl_crm_leads_final_schd.py"
    "etl_crm_partner_credit_status_final_schd.py"
    "etl_crm_partner_logs_master_final_schd.py"
    "etl_crm_partner_master_final_schd.py"
    "etl_crm_leads_company_final_schd.py"
)

for script in "${CRM_SCRIPTS[@]}"; do
    ((TOTAL_SCRIPTS++))
    if run_etl_script "crm" "$script"; then
        ((SUCCESS_COUNT++))
    else
        ((FAILED_COUNT++))
        echo -e "${YELLOW}⚠️  Continuing with next script...${NC}" | tee -a "$MASTER_LOG"
    fi
done

# ============================================
# FINAL SUMMARY
# ============================================
END_TIME=$(date +%s)
TOTAL_DURATION=$((END_TIME - START_TIME))

echo "" | tee -a "$MASTER_LOG"
echo "╔══════════════════════════════════════════════════════════════════════╗" | tee -a "$MASTER_LOG"
echo "║                           EXECUTION SUMMARY                          ║" | tee -a "$MASTER_LOG"
echo "╠══════════════════════════════════════════════════════════════════════╣" | tee -a "$MASTER_LOG"

# Calculate success rate
if [ $TOTAL_SCRIPTS -gt 0 ]; then
    SUCCESS_RATE=$((SUCCESS_COUNT * 100 / TOTAL_SCRIPTS))
else
    SUCCESS_RATE=0
fi

# Color-coded summary
if [ $FAILED_COUNT -eq 0 ]; then
    SUMMARY_COLOR=$GREEN
    SUMMARY_ICON="🎉"
elif [ $SUCCESS_RATE -ge 80 ]; then
    SUMMARY_COLOR=$YELLOW
    SUMMARY_ICON="⚠️ "
else
    SUMMARY_COLOR=$RED
    SUMMARY_ICON="❌"
fi

echo -e "${SUMMARY_COLOR}║ ${SUMMARY_ICON} Total Scripts: $TOTAL_SCRIPTS                                     ║${NC}" | tee -a "$MASTER_LOG"
echo -e "${GREEN}║ ✅ Successful: $SUCCESS_COUNT                                            ║${NC}" | tee -a "$MASTER_LOG"
echo -e "${RED}║ ❌ Failed: $FAILED_COUNT                                                ║${NC}" | tee -a "$MASTER_LOG"
echo -e "${SUMMARY_COLOR}║ 📊 Success Rate: $SUCCESS_RATE%                                        ║${NC}" | tee -a "$MASTER_LOG"
echo -e "${CYAN}║ ⏱️  Total Duration: ${TOTAL_DURATION}s                                    ║${NC}" | tee -a "$MASTER_LOG"
echo "╠══════════════════════════════════════════════════════════════════════╣" | tee -a "$MASTER_LOG"
echo "║ 📋 Log Files Generated:                                              ║" | tee -a "$MASTER_LOG"
echo "║    • Master log: $(basename "$MASTER_LOG")" | tee -a "$MASTER_LOG"
echo "║    • Individual script logs in: $LOG_DIR/" | tee -a "$MASTER_LOG"
echo "╚══════════════════════════════════════════════════════════════════════╝" | tee -a "$MASTER_LOG"

# Send notification (optional)
if [ $FAILED_COUNT -gt 0 ]; then
    echo "" | tee -a "$MASTER_LOG"
    echo -e "${YELLOW}⚠️  $FAILED_COUNT scripts failed. Check individual logs for details.${NC}" | tee -a "$MASTER_LOG"
fi

# Return appropriate exit code
if [ $FAILED_COUNT -eq 0 ]; then
    exit 0
else
    exit 1
fi
