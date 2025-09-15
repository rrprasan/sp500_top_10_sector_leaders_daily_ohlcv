#!/bin/bash

# Incremental OHLCV Pipeline Scheduler Script with Email Notifications
# This script sets up the environment and runs the incremental pipeline
# Designed to be run via cron job every Saturday at 7 AM PT
# Includes success/failure email notifications

# Set script directory and change to it
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Set up logging
LOG_FILE="$SCRIPT_DIR/cron_incremental_pipeline.log"
DATE_TIME=$(date '+%Y-%m-%d %H:%M:%S')
START_TIME=$(date +%s)

echo "========================================" >> "$LOG_FILE"
echo "[$DATE_TIME] Starting scheduled incremental pipeline with email notifications" >> "$LOG_FILE"
echo "========================================" >> "$LOG_FILE"

# Environment setup
export PATH="/usr/local/bin:/usr/bin:/bin:$PATH"

# Ensure POLYGON_API_KEY is set (you may need to adjust this path)
if [ -f "$HOME/.bashrc" ]; then
    source "$HOME/.bashrc"
fi

if [ -f "$HOME/.zshrc" ]; then
    source "$HOME/.zshrc"
fi

# Check if POLYGON_API_KEY is set
if [ -z "$POLYGON_API_KEY" ]; then
    echo "[$DATE_TIME] ERROR: POLYGON_API_KEY environment variable not set" >> "$LOG_FILE"
    echo "[$DATE_TIME] Please ensure POLYGON_API_KEY is exported in your shell profile" >> "$LOG_FILE"
    exit 1
fi

# Check if Python is available
if ! command -v python3 &> /dev/null; then
    echo "[$DATE_TIME] ERROR: python3 not found in PATH" >> "$LOG_FILE"
    exit 1
fi

# Check if the incremental pipeline script exists
if [ ! -f "$SCRIPT_DIR/incremental_ohlcv_pipeline.py" ]; then
    echo "[$DATE_TIME] ERROR: incremental_ohlcv_pipeline.py not found" >> "$LOG_FILE"
    exit 1
fi

# Function to send email notifications
send_email_notification() {
    local notification_type=$1
    local stats_or_error=$2
    
    echo "[$DATE_TIME] Sending $notification_type email notification..." >> "$LOG_FILE"
    
    # Create temporary Python script for email notification
    cat > "$SCRIPT_DIR/temp_email_notification.py" << EOF
#!/usr/bin/env python3
import sys
import os
import json
from datetime import datetime

# Add current directory to path
sys.path.insert(0, '$SCRIPT_DIR')

try:
    from email_notifier import PipelineEmailNotifier
    
    notifier = PipelineEmailNotifier()
    
    if '$notification_type' == 'success':
        # Parse statistics from log file
        stats = $stats_or_error
        success = notifier.send_success_notification(stats)
    else:
        # Parse error details
        error_details = $stats_or_error
        success = notifier.send_failure_notification(error_details)
    
    sys.exit(0 if success else 1)
    
except Exception as e:
    print(f"Email notification failed: {str(e)}")
    sys.exit(1)
EOF
    
    # Run the email notification
    python3 "$SCRIPT_DIR/temp_email_notification.py" >> "$LOG_FILE" 2>&1
    EMAIL_EXIT_CODE=$?
    
    # Clean up temporary file
    rm -f "$SCRIPT_DIR/temp_email_notification.py"
    
    if [ $EMAIL_EXIT_CODE -eq 0 ]; then
        echo "[$DATE_TIME] ✅ Email notification sent successfully" >> "$LOG_FILE"
    else
        echo "[$DATE_TIME] ⚠️ Email notification failed (exit code: $EMAIL_EXIT_CODE)" >> "$LOG_FILE"
    fi
}

# Run the incremental pipeline
echo "[$DATE_TIME] Starting incremental OHLCV pipeline..." >> "$LOG_FILE"

# Capture pipeline output to parse statistics
PIPELINE_OUTPUT_FILE="$SCRIPT_DIR/temp_pipeline_output.log"
python3 "$SCRIPT_DIR/incremental_ohlcv_pipeline.py" > "$PIPELINE_OUTPUT_FILE" 2>&1
EXIT_CODE=$?

# Append pipeline output to main log
cat "$PIPELINE_OUTPUT_FILE" >> "$LOG_FILE"

# Calculate execution time
END_TIME=$(date +%s)
EXECUTION_TIME=$((END_TIME - START_TIME))
EXECUTION_TIME_FORMATTED=$(printf '%02d:%02d:%02d' $((EXECUTION_TIME/3600)) $((EXECUTION_TIME%3600/60)) $((EXECUTION_TIME%60)))

# Log completion status and send notifications
if [ $EXIT_CODE -eq 0 ]; then
    echo "[$DATE_TIME] ✅ Incremental pipeline completed successfully" >> "$LOG_FILE"
    
    # Extract statistics from pipeline output
    TOTAL_TICKERS=$(grep -o "Total tickers analyzed: [0-9]*" "$PIPELINE_OUTPUT_FILE" | grep -o "[0-9]*" | tail -1)
    SUCCESSFUL_TICKERS=$(grep -o "Successfully processed: [0-9]*" "$PIPELINE_OUTPUT_FILE" | grep -o "[0-9]*" | tail -1)
    FAILED_TICKERS=$(grep -o "Failed: [0-9]*" "$PIPELINE_OUTPUT_FILE" | grep -o "[0-9]*" | tail -1)
    DATE_RANGE=$(grep -o "from [0-9-]* to [0-9-]*" "$PIPELINE_OUTPUT_FILE" | head -1)
    
    # Default values if parsing fails
    TOTAL_TICKERS=${TOTAL_TICKERS:-0}
    SUCCESSFUL_TICKERS=${SUCCESSFUL_TICKERS:-0}
    FAILED_TICKERS=${FAILED_TICKERS:-0}
    DATE_RANGE=${DATE_RANGE:-"Unknown"}
    FILES_UPLOADED=$SUCCESSFUL_TICKERS  # Assuming 1 file per successful ticker
    
    # Create statistics JSON
    STATS_JSON="{\"total_tickers\": $TOTAL_TICKERS, \"successful_tickers\": $SUCCESSFUL_TICKERS, \"failed_tickers\": $FAILED_TICKERS, \"execution_time\": \"$EXECUTION_TIME_FORMATTED\", \"date_range\": \"$DATE_RANGE\", \"files_uploaded\": $FILES_UPLOADED}"
    
    # Send success notification
    send_email_notification "success" "$STATS_JSON"
    
else
    echo "[$DATE_TIME] ❌ Incremental pipeline failed with exit code: $EXIT_CODE" >> "$LOG_FILE"
    
    # Extract error information from pipeline output
    ERROR_MESSAGE=$(tail -20 "$PIPELINE_OUTPUT_FILE" | sed 's/"/\\"/g' | tr '\n' ' ')
    
    # Create error details JSON
    ERROR_JSON="{\"exit_code\": $EXIT_CODE, \"error_message\": \"$ERROR_MESSAGE\", \"execution_time\": \"$EXECUTION_TIME_FORMATTED\", \"failed_step\": \"Pipeline Execution\"}"
    
    # Send failure notification
    send_email_notification "failure" "$ERROR_JSON"
fi

# Clean up temporary files
rm -f "$PIPELINE_OUTPUT_FILE"

echo "[$DATE_TIME] Scheduled run completed (execution time: $EXECUTION_TIME_FORMATTED)" >> "$LOG_FILE"
echo "" >> "$LOG_FILE"

exit $EXIT_CODE
