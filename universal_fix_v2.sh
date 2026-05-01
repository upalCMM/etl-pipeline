#!/bin/bash
# Universal fix v2 - Works for all scripts

echo "Applying universal fix to all ETL scripts..."
echo "==========================================="

TOTAL_FIXED=0

for category in pipedrive bigquery github crm; do
    echo ""
    echo "Processing $category/..."
    
    for script in "$category"/*.py; do
        if [ -f "$script" ]; then
            script_name=$(basename "$script")
            echo "  Fixing: $script_name"
            
            # Create backup
            cp "$script" "${script}.backup.$(date +%s)"
            
            # Read the entire script
            script_content=$(cat "$script")
            
            # Create new script with proper imports
            cat > "$script" << 'NEWHEADER'
#!/usr/bin/env python3
"""
ETL Script - Auto-fixed version
"""
import os
import sys

# AUTO-FIX: Add project root to Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
if project_root not in sys.path:
    sys.path.insert(0, project_root)
NEWHEADER
            
            # Add the original content (skip shebang if present)
            if echo "$script_content" | head -1 | grep -q "^#!/"; then
                # Keep the original shebang
                echo "$script_content" | head -1 >> "$script"
                echo "$script_content" | tail -n +2 >> "$script"
            else
                echo "$script_content" >> "$script"
            fi
            
            ((TOTAL_FIXED++))
            echo "    ✅ Fixed"
        fi
    done
done

echo ""
echo "==========================================="
echo "✅ Fixed $TOTAL_FIXED scripts"
echo ""
echo "Testing a few scripts to verify..."
echo ""

# Test a few scripts
echo "1. Testing pipedrive/sync_ringcentral_final.py..."
cd pipedrive
python3 sync_ringcentral_final.py 2>&1 | grep -E "(INFO|ERROR|Traceback)" | head -5
cd ..

echo ""
echo "2. Testing github/update_etl_github_prs_final_schd.py..."
cd github
python3 update_etl_github_prs_final_schd.py --help 2>&1 | head -5
cd ..

echo ""
echo "If you see 'INFO' or '--help' output, the imports are working!"
echo "Database connection errors are expected if database isn't set up."
