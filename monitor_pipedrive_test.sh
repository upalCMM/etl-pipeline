#!/bin/bash
# Monitor the Pipedrive test progress

TEST_DIR=$(ls -td logs/pipedrive_test_* 2>/dev/null | head -1)

if [ -z "$TEST_DIR" ]; then
    echo "No test directory found. Is test_pipedrive_scripts.sh running?"
    exit 1
fi

echo "Monitoring Pipedrive test in: $TEST_DIR"
echo "Press Ctrl+C to stop monitoring"
echo ""

# Show initial files
echo "Test logs found:"
ls -la "$TEST_DIR/" | grep -v "^total"

echo ""
echo "=== Current Status ==="

# Check which scripts have been tested
for script in pipedrive/*.py; do
    script_name=$(basename "$script")
    log_file="$TEST_DIR/${script_name%.py}.log"
    
    if [ -f "$log_file" ]; then
        if grep -q "PASSED\|SUCCESS\|complete" "$log_file" 2>/dev/null; then
            echo "✅ $(basename $script_name) - TESTED (PASS)"
        elif grep -q "FAILED\|ERROR\|Traceback" "$log_file" 2>/dev/null; then
            echo "❌ $(basename $script_name) - TESTED (FAIL)"
        else
            echo "⏳ $(basename $script_name) - IN PROGRESS"
        fi
    else
        echo "📝 $(basename $script_name) - PENDING"
    fi
done

echo ""
echo "=== Recent Log Entries ==="
# Show tail of most recent log
recent_log=$(ls -t "$TEST_DIR/"*.log 2>/dev/null | head -1)
if [ -n "$recent_log" ]; then
    echo "Most recent: $(basename $recent_log)"
    tail -10 "$recent_log"
fi
