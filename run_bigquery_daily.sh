#!/bin/bash
# Simple daily runner for BigQuery ETL scripts

set -e

echo "======================================="
echo "BigQuery Daily ETL Runner"
echo "Date: $(date)"
echo "======================================="

cd ~/scripts/etl_pipeline
source ../venv/bin/activate

# Set WSL environment
export SSL_CERT_FILE=$(python -m certifi)
export REQUESTS_CA_BUNDLE=$(python -m certifi)

# Create log directory
LOG_DIR="logs/bigquery_daily"
mkdir -p "$LOG_DIR"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")

# Run each script
SCRIPTS=(
    "Google Ads:bigquery/update_google_ads_data_final_schd.py"
    "GSC:bigquery/gsc_daily_update_final_schd.py"
    "GA4 Landing Pages:bigquery/ga4_landing_pages_update_final_schd.py"
    "GA4 Referrer Sessions:bigquery/ga4_referrer_sessions_update_final_schd.py"
)

TOTAL_START=$(date +%s)

for script_entry in "${SCRIPTS[@]}"; do
    IFS=':' read -r script_name script_path <<< "$script_entry"
    
    echo ""
    echo "🚀 Running: $script_name"
    echo "---------------------------------------"
    
    SCRIPT_START=$(date +%s)
    
    # Run the script
    python3 "$script_path" 2>&1 | tee "$LOG_DIR/${script_name// /_}_${TIMESTAMP}.log"
    
    SCRIPT_EXIT=$?
    SCRIPT_END=$(date +%s)
    SCRIPT_DURATION=$((SCRIPT_END - SCRIPT_START))
    
    if [ $SCRIPT_EXIT -eq 0 ]; then
        echo "✅ $script_name completed in ${SCRIPT_DURATION}s"
    else
        echo "❌ $script_name failed after ${SCRIPT_DURATION}s (exit: $SCRIPT_EXIT)"
    fi
    
    # Small delay between scripts
    sleep 5
done

TOTAL_END=$(date +%s)
TOTAL_DURATION=$((TOTAL_END - TOTAL_START))

echo ""
echo "======================================="
echo "✅ All BigQuery ETL scripts completed!"
echo "Total time: ${TOTAL_DURATION}s"
echo "Logs: $LOG_DIR/"
echo "======================================="
