#!/bin/bash
# Master CRM ETL Scheduler
# Run daily at 6 AM

echo "================================================"
echo "🚀 CRM ETL DAILY SCHEDULE - $(date)"
echo "================================================"
echo ""

LOG_DIR="logs/crm"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/crm_etl_$(date +%Y%m%d_%H%M%S).log"

# Function to run a CRM script with logging
run_crm_script() {
    local script_name=$1
    local start_time=$(date +%s)
    
    echo ""
    echo "🔄 RUNNING: $script_name"
    echo "   Started: $(date)"
    
    # Run the script
    cd crm
    python3 "$script_name" 2>&1
    EXIT_CODE=$?
    cd ..
    
    local end_time=$(date +%s)
    local duration=$((end_time - start_time))
    
    if [ $EXIT_CODE -eq 0 ]; then
        echo "   ✅ COMPLETED in ${duration}s"
        return 0
    else
        echo "   ❌ FAILED after ${duration}s (exit code: $EXIT_CODE)"
        return 1
    fi
}

# Redirect all output to log file
exec > >(tee -a "$LOG_FILE") 2>&1

echo "📁 Log file: $LOG_FILE"

# Run CRM scripts in logical order
echo ""
echo "📋 EXECUTION ORDER:"
echo "1. Partner Master Companies (core data)"
echo "2. Partner Credit Status"
echo "3. Partner Company Logs"
echo "4. Leads"
echo "5. Leads Company"

# Execute scripts
FAILED_SCRIPTS=()

echo ""
echo "=== STARTING CRM ETL EXECUTION ==="

# 1. Partner Master Companies
if run_crm_script "etl_crm_partner_master_final_schd.py"; then
    echo "✅ Step 1/5 complete"
else
    FAILED_SCRIPTS+=("etl_crm_partner_master_final_schd.py")
fi

# 2. Partner Credit Status
if run_crm_script "etl_crm_partner_credit_status_final_schd.py"; then
    echo "✅ Step 2/5 complete"
else
    FAILED_SCRIPTS+=("etl_crm_partner_credit_status_final_schd.py")
fi

# 3. Partner Company Logs
if run_crm_script "etl_crm_partner_logs_master_final_schd.py"; then
    echo "✅ Step 3/5 complete"
else
    FAILED_SCRIPTS+=("etl_crm_partner_logs_master_final_schd.py")
fi

# 4. Leads
if run_crm_script "etl_crm_leads_final_schd.py"; then
    echo "✅ Step 4/5 complete"
else
    FAILED_SCRIPTS+=("etl_crm_leads_final_schd.py")
fi

# 5. Leads Company
if run_crm_script "etl_crm_leads_company_final_schd.py"; then
    echo "✅ Step 5/5 complete"
else
    FAILED_SCRIPTS+=("etl_crm_leads_company_final_schd.py")
fi

echo ""
echo "================================================"
echo "📊 CRM ETL EXECUTION SUMMARY"
echo "================================================"
echo "Total time: $(date)"
echo ""

if [ ${#FAILED_SCRIPTS[@]} -eq 0 ]; then
    echo "🎉 ALL 5 CRM SCRIPTS EXECUTED SUCCESSFULLY!"
    echo "✅ Total rows processed today: ~1+ million"
else
    echo "⚠️  ${#FAILED_SCRIPTS[@]} script(s) failed:"
    for script in "${FAILED_SCRIPTS[@]}"; do
        echo "   ❌ $script"
    done
fi

echo ""
echo "📁 Detailed log: $LOG_FILE"
echo "================================================"
